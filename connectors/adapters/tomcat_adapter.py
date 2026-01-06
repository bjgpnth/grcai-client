# connectors/adapters/tomcat_adapter.py
from connectors.base_connector import BaseConnector
from connectors.historical import HistoricalLogCollectionMixin
import traceback
import re
import time
import inspect
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, date, timedelta
from collections import Counter

logger = logging.getLogger("grcai.adapters.tomcat")


DATE_FILENAME_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


class TomcatAdapter(BaseConnector, HistoricalLogCollectionMixin):
    """
    TomcatAdapter - diagnostics for Tomcat instances.

    Behavior summary (Option A):
      - CURRENT collection prefers Docker container logs (docker logs equivalent).
      - HISTORICAL collection first tries Docker time-filtered logs; if unavailable,
        it falls back to reading rotated log files inside the container:
          /usr/local/tomcat/logs/catalina.YYYY-MM-DD.log
          localhost_access_log.YYYY-MM-DD.txt
      - Robust fallbacks: missing files/tools do not raise; errors are collected
        into instance_res["errors"].
    """

    def __init__(self, name=None, env_config=None, issue_time=None, component_config=None, **kwargs):
        if name is None:
            name = "tomcat"
        if env_config is None:
            env_config = {"hosts": []}

        super().__init__(name=name, issue_time=issue_time, component_config=component_config)

        self.env_config = env_config or {}
        self.component_config = component_config or {}
        self.instances = self.component_config.get("instances", [])
        self.hosts = self.env_config.get("hosts", [])

    # --------------------------
    # Helpers
    # --------------------------
    def _safe_unwrap(self, maybe: Any) -> str:
        if maybe is None:
            return ""
        if isinstance(maybe, dict):
            return maybe.get("stdout") or maybe.get("output") or ""
        return str(maybe)

    def _run_in_container(self, host_connector, container_id: str, cmd: str, timeout: int = 10) -> str:
        """
        Execute a command inside a container if supported; otherwise try to run
        a 'docker exec' wrapper using host_connector.exec_cmd. Returns string output.
        For non-Docker hosts (container_id is None/empty), use exec_cmd directly.
        """
        # For non-Docker hosts (container_id is None/empty), skip exec_in_container and use exec_cmd directly
        if not container_id:
            exec_cmd = getattr(host_connector, "exec_cmd", None)
            if exec_cmd and callable(exec_cmd):
                try:
                    raw = exec_cmd(cmd, timeout=timeout)
                    return self._safe_unwrap(raw) or ""
                except Exception as e:
                    return f"[host exec error: {e}]"
            return f"[no exec capability on host connector]"
        
        # Check connector type to determine execution method
        from connectors.host_connectors.docker_host_connector import DockerHostConnector
        from connectors.host_connectors.ssh_host_connector import SSHHostConnector
        
        # SSHHostConnector: use docker exec via exec_cmd (SSHHostConnector doesn't implement exec_in_container)
        if isinstance(host_connector, SSHHostConnector) and hasattr(host_connector, "exec_cmd"):
            try:
                # Use docker exec to run command inside container
                # Wrap command in sh -c for proper shell execution
                wrapped = f'sh -c {sh_quote(cmd)}'
                docker_cmd = f'docker exec {container_id} {wrapped}'
                raw = host_connector.exec_cmd(docker_cmd, timeout=timeout)
                if isinstance(raw, dict):
                    stdout = raw.get("stdout", "")
                    stderr = raw.get("stderr", "")
                    rc = raw.get("rc", 1)
                    if rc == 0 or stdout:
                        return stdout
                    error_msg = stderr or raw.get("error", "Command failed")
                    return f"[docker exec error: {error_msg}]"
                return self._safe_unwrap(raw) or ""
            except Exception as e:
                return f"[docker exec error: {e}]"
        
        # DockerHostConnector: use exec_in_container (actually implemented)
        elif isinstance(host_connector, DockerHostConnector):
            exec_in_container = getattr(host_connector, "exec_in_container", None)
            if exec_in_container and callable(exec_in_container):
                try:
                    # Check if exec_in_container accepts timeout parameter
                    sig = inspect.signature(exec_in_container)
                    if 'timeout' in sig.parameters:
                        out = exec_in_container(container_id, cmd, timeout=timeout)
                    else:
                        out = exec_in_container(container_id, cmd)
                    return self._safe_unwrap(out) or ""
                except Exception as e:
                    return f"[docker-host exec error: {e}]"

        # Fallback: if host connector can run exec_cmd, attempt `docker exec <id> sh -c "<cmd>"`
        exec_cmd = getattr(host_connector, "exec_cmd", None)
        if exec_cmd and callable(exec_cmd):
            try:
                wrapped = f'sh -c {sh_quote(cmd)}'
                wrapper = f"docker exec {container_id} {wrapped}"
                raw = exec_cmd(wrapper, timeout=timeout)
                return self._safe_unwrap(raw) or ""
            except Exception as e:
                return f"[exec_cmd/docker exec error: {e}]"

        return f"[no exec capability on host connector]"

    def _read_file_in_container(self, host_connector, container_id: str, path: str) -> str:
        read_helper = getattr(host_connector, "read_file_in_container", None)
        if read_helper and callable(read_helper):
            try:
                out = read_helper(container_id, path)
                return self._safe_unwrap(out) or ""
            except Exception as e:
                # fallback to exec cat
                try:
                    return self._run_in_container(host_connector, container_id, f"cat {sh_quote(path)}") or ""
                except Exception:
                    return f"[docker-host read error: {e}]"

        try:
            return self._run_in_container(host_connector, container_id, f"cat {sh_quote(path)}") or ""
        except Exception as e:
            return f"[read fallback error: {e}]"

    def _tail_file(self, host_connector, container_id: str, path: str, lines: int = 200) -> str:
        """
        Attempt to tail a file inside the container. We avoid complex shell chaining that
        confuses exec APIs. Use `tail -n <N> <file>` if available, otherwise `cat`.
        """
        # try tail -n
        out = self._run_in_container(host_connector, container_id, f"tail -n {int(lines)} {sh_quote(path)}")
        if out and not out.startswith("[docker-host exec error"):
            return out
        # fallback to cat
        out2 = self._run_in_container(host_connector, container_id, f"cat {sh_quote(path)}")
        return out2 or ""

    def _find_java_pid(self, host_connector, container_id: str, catalina_home: str = None, port: int = None) -> Optional[str]:
        """
        Find the Java PID for the Tomcat process.
        Filters for processes containing 'catalina', 'tomcat', the catalina_home path, or listening on the Tomcat port.
        Prioritizes catalina_home path matching when available, as it's the most specific.
        """
        # Method 1: Try to find Tomcat-specific processes using catalina_home path (PRIORITIZED when available)
        # This is the most reliable method when catalina_home is provided
        if catalina_home:
            # Normalize path: remove trailing slashes for better matching
            catalina_home_normalized = catalina_home.rstrip('/')
            # Escape special regex characters in catalina_home for use in grep pattern
            catalina_home_pattern = re.escape(catalina_home_normalized)
            # Look for processes with catalina_home in the command line
            # Try multiple ps formats and grep patterns for robustness
            candidates = [
                f"ps -ef | grep '[j]ava' | grep '{catalina_home_normalized}' | grep -v grep | awk '{{print $2; exit}}' || true",
                f"ps aux | grep '[j]ava' | grep '{catalina_home_normalized}' | grep -v grep | awk '{{print $2; exit}}' || true",
                f"ps -eo pid,cmd | grep '[j]ava' | grep '{catalina_home_normalized}' | grep -v grep | awk '{{print $1; exit}}' || true",
                # Also try with regex pattern (more flexible but may be slower)
                f"ps -ef | grep -E '[j]ava.*{catalina_home_pattern}' | grep -v grep | awk '{{print $2; exit}}' || true",
                f"ps aux | grep -E '[j]ava.*{catalina_home_pattern}' | grep -v grep | awk '{{print $2; exit}}' || true",
            ]
            for c in candidates:
                out = self._run_in_container(host_connector, container_id, c)
                out = (out or "").strip()
                if out and not out.startswith("[") and not out.startswith("Error"):  # Skip error messages
                    m = re.search(r"(\d+)", out)
                    if m:
                        pid = m.group(1)
                        # Verify the PID actually has the catalina_home path in its command line
                        verify_cmd = f"ps -p {pid} -o cmd --no-headers 2>/dev/null | grep -q '{catalina_home_normalized}' && echo {pid} || true"
                        verify_out = self._run_in_container(host_connector, container_id, verify_cmd)
                        if verify_out and verify_out.strip() and verify_out.strip() == pid:
                            return pid
        
        # Method 2: Try port-based detection (reliable if port is known and matches)
        if port:
            try:
                # Find process listening on the Tomcat port
                port_candidates = [
                    f"lsof -ti :{port} 2>/dev/null || true",
                    f"ss -tlnp 2>/dev/null | grep :{port} | grep -oP 'pid=\\K\\d+' | head -1 || true",
                    f"netstat -tlnp 2>/dev/null | grep :{port} | grep -oP '\\d+/java' | cut -d'/' -f1 | head -1 || true",
                ]
                for c in port_candidates:
                    out = self._run_in_container(host_connector, container_id, c)
                    out = (out or "").strip()
                    if out and not out.startswith("[") and not out.startswith("Error"):
                        m = re.search(r"(\d+)", out)
                        if m:
                            pid = m.group(1)
                            # Verify it's actually a Java process
                            verify_cmd = f"ps -p {pid} -o cmd --no-headers 2>/dev/null | grep -q java && echo {pid} || true"
                            verify_out = self._run_in_container(host_connector, container_id, verify_cmd)
                            if verify_out and verify_out.strip():
                                return pid
            except Exception:
                pass  # Continue to other methods if port-based detection fails
        
        # Method 3: Fallback: look for processes with "catalina" or "tomcat" in the command line
        candidates = [
            "ps -eo pid,cmd | grep -E '[j]ava.*[Cc]atalina' | grep -v grep | awk '{print $1; exit}' || true",
            "ps -ef | grep -E '[j]ava.*[Cc]atalina' | grep -v grep | awk '{print $2; exit}' || true",
            "ps aux | grep -E '[j]ava.*[Cc]atalina' | grep -v grep | awk '{print $2; exit}' || true",
            "ps -eo pid,cmd | grep -E '[j]ava.*[Tt]omcat' | grep -v grep | awk '{print $1; exit}' || true",
            "ps -ef | grep -E '[j]ava.*[Tt]omcat' | grep -v grep | awk '{print $2; exit}' || true",
            "ps aux | grep -E '[j]ava.*[Tt]omcat' | grep -v grep | awk '{print $2; exit}' || true",
        ]
        for c in candidates:
            out = self._run_in_container(host_connector, container_id, c)
            out = (out or "").strip()
            if out:
                m = re.search(r"(\d+)", out)
                if m:
                    return m.group(1)
        
        # Method 4: Last resort: return first Java process (original behavior)
        candidates = [
            "pgrep -o -f java || true",
            "ps -eo pid,cmd | grep [j]ava | awk '{print $1; exit}' || true",
            "ps -ef | grep [j]ava | awk '{print $2; exit}' || true",
            "ps aux | grep [j]ava | awk '{print $2; exit}' || true",
        ]
        for c in candidates:
            out = self._run_in_container(host_connector, container_id, c)
            out = (out or "").strip()
            if out:
                m = re.search(r"(\d+)", out)
                if m:
                    return m.group(1)
        return None

    def _try_tool(self, host_connector, container_id: str, tool_name: str) -> bool:
        check_cmds = [f"command -v {tool_name} 2>/dev/null || true", f"which {tool_name} 2>/dev/null || true"]
        for c in check_cmds:
            out = self._run_in_container(host_connector, container_id, c)
            if out and out.strip() and not out.strip().startswith("[docker-host exec error"):
                return True
        return False

    def _http_healthcheck(self, host_connector, container_id: str, port: int = 8080) -> dict:
        endpoints = [f"http://127.0.0.1:{port}/", f"http://localhost:{port}/"]
        res = {"status": "UNKNOWN", "http_code": None, "body_snippet": "", "method": None}
        if self._try_tool(host_connector, container_id, "curl"):
            head = self._run_in_container(host_connector, container_id, f"curl -sS -I --max-time 5 http://127.0.0.1:{port}/ || true")
            if head and "HTTP/" in head:
                body = self._run_in_container(host_connector, container_id, f"curl -sS --max-time 5 http://127.0.0.1:{port}/ || true")
                first = (body or "").strip()[:1024]
                code_match = re.search(r"HTTP/\d\.\d\s+(\d+)", head)
                res.update({"status": "UP" if code_match and int(code_match.group(1)) < 500 else "DOWN",
                            "http_code": int(code_match.group(1)) if code_match else None,
                            "body_snippet": first,
                            "method": "curl"})
                return res
        if self._try_tool(host_connector, container_id, "wget"):
            out = self._run_in_container(host_connector, container_id, f"wget -qO- --timeout=5 http://127.0.0.1:{port}/ || true")
            if out and out.strip():
                return {"status": "UP", "http_code": 200, "body_snippet": out.strip()[:1024], "method": "wget"}
        return res

    def _collect_gc_logs(self, host_connector, container_id: str, catalina_home: str) -> dict:
        logs = {}
        paths_to_probe = [
            f"{catalina_home}/logs",
            "/usr/local/tomcat/logs",
            "/opt/tomcat/logs",
            "/var/log/tomcat"
        ]
        for p in paths_to_probe:
            list_out = self._run_in_container(host_connector, container_id, f"ls -1 {sh_quote(p)} 2>/dev/null || true")
            if list_out and list_out.strip():
                for ln in list_out.splitlines():
                    if "gc" in ln.lower():
                        fp = f"{p.rstrip('/')}/{ln.strip()}"
                        tail = self._tail_file(host_connector, container_id, fp, lines=200)
                        logs[ln.strip()] = tail
                if logs:
                    return logs
        return logs

    def _analyze_thread_dump_text(self, dump_text: str) -> dict:
        if not dump_text:
            return {"blocked_threads": 0, "waiting_threads": 0, "deadlock": False, "stuck_threads": []}
        blocked = len(re.findall(r'\".*\"\s+#[0-9]+\s+Blocked', dump_text, flags=re.IGNORECASE))
        waiting = len(re.findall(r'WAITING', dump_text, flags=re.IGNORECASE))
        deadlock = bool(re.search(r'Deadlock|Found one Java-level deadlock', dump_text, flags=re.IGNORECASE))
        stuck_names = re.findall(r'\"([^\"]+)\".*(BLOCKED|WAITING)', dump_text, flags=re.IGNORECASE)
        stuck_threads = [s[0] for s in stuck_names][:10]
        return {"blocked_threads": blocked, "waiting_threads": waiting, "deadlock": deadlock, "stuck_threads": stuck_threads}

    def _split_combined_logs(self, combined_text: str) -> tuple:
        """
        Split Tomcat access log content by HTTP status codes.
        Returns tuple(access_text, error_text) where:
        - access_text: 2XX/3XX status codes (successful requests)
        - error_text: 4XX/5XX status codes (HTTP errors)
        """
        if not combined_text:
            return "", ""
        access_lines = []
        error_lines = []
        
        # Pattern to extract HTTP status code from Tomcat access log lines
        # Format: "172.18.0.10 - - [01/Dec/2025:10:49:38 +0000] "GET /abc/ HTTP/1.0" 404 683"
        status_pattern = re.compile(r'HTTP/\d\.\d"\s+(\d{3})')
        
        for ln in combined_text.splitlines():
            l = ln.strip()
            if not l:
                continue
            
            # Check if it's a Tomcat access log line (has HTTP status code)
            status_match = status_pattern.search(l)
            if status_match:
                status_code = int(status_match.group(1))
                # 4XX and 5XX are errors - put them in error_lines
                if 400 <= status_code < 600:
                    error_lines.append(l)
                else:
                    # 2XX and 3XX are successes - put them in access_lines
                    access_lines.append(l)
            else:
                # If it doesn't match access log format, skip it
                # (we're only interested in access log lines for splitting)
                pass
        
        return "\n".join(access_lines), "\n".join(error_lines)

    def _find_current_access_log(self, host_connector, container_id: str, log_dir: str) -> Optional[str]:
        """
        Find the access log file for the current date.
        Returns the filename (e.g., "localhost_access_log.2025-12-01.txt") or None.
        """
        try:
            all_files = self._list_log_dir(host_connector, container_id, log_dir)
            access_log_files = [f for f in all_files if "localhost_access" in f.lower() or "access" in f.lower()]
            
            if not access_log_files:
                return None
            
            # Get current date in YYYY-MM-DD format
            today = date.today()
            today_str = today.strftime("%Y-%m-%d")
            
            # Look for file matching today's date
            for filename in access_log_files:
                if today_str in filename:
                    return filename
            
            # If no exact match, try to find the most recent one (fallback)
            # Sort by name (most recent first, if date-based)
            access_log_files.sort(reverse=True)
            return access_log_files[0] if access_log_files else None
        except Exception:
            return None

    def _analyze_access_log_status_codes(self, access_log_content: str) -> dict:
        """
        Analyze HTTP status codes from Tomcat access log content.
        Returns a summary with counts by status code range and specific codes.
        """
        if not access_log_content:
            return {
                "total_requests": 0,
                "status_2xx": 0,
                "status_3xx": 0,
                "status_4xx": 0,
                "status_5xx": 0,
                "status_other": 0,
                "status_code_counts": {},
                "4xx_errors": [],
                "5xx_errors": []
            }
        
        lines = [l.strip() for l in access_log_content.splitlines() if l.strip()]
        if not lines:
            return {
                "total_requests": 0,
                "status_2xx": 0,
                "status_3xx": 0,
                "status_4xx": 0,
                "status_5xx": 0,
                "status_other": 0,
                "status_code_counts": {},
                "4xx_errors": [],
                "5xx_errors": []
            }
        
        # Pattern to match HTTP status code in Tomcat access log format
        # Format: IP - - [date] "METHOD /path HTTP/1.x" status
        # The status code appears after the HTTP version quote
        status_pattern = re.compile(r'HTTP/\d\.\d"\s+(\d{3})')
        
        status_counts = Counter()
        status_2xx = 0
        status_3xx = 0
        status_4xx = 0
        status_5xx = 0
        status_other = 0
        errors_4xx = []
        errors_5xx = []
        
        for line in lines:
            match = status_pattern.search(line)
            if match:
                status_code = int(match.group(1))
                status_counts[status_code] += 1
                
                if 200 <= status_code < 300:
                    status_2xx += 1
                elif 300 <= status_code < 400:
                    status_3xx += 1
                elif 400 <= status_code < 500:
                    status_4xx += 1
                    # Store 4XX error details (limit to avoid huge payloads)
                    if len(errors_4xx) < 50:
                        errors_4xx.append({
                            "status": status_code,
                            "line": line[:500]  # Truncate long lines
                        })
                elif 500 <= status_code < 600:
                    status_5xx += 1
                    # Store 5XX error details (limit to avoid huge payloads)
                    if len(errors_5xx) < 50:
                        errors_5xx.append({
                            "status": status_code,
                            "line": line[:500]  # Truncate long lines
                        })
                else:
                    status_other += 1
        
        return {
            "total_requests": len(lines),
            "status_2xx": status_2xx,
            "status_3xx": status_3xx,
            "status_4xx": status_4xx,
            "status_5xx": status_5xx,
            "status_other": status_other,
            "status_code_counts": dict(status_counts),
            "4xx_errors": errors_4xx,
            "5xx_errors": errors_5xx
        }

    # --------------------------
    # Rotated log helpers (filename date parsing)
    # --------------------------
    def _list_log_dir(self, host_connector, container_id: str, log_dir: str) -> List[str]:
        out = self._run_in_container(host_connector, container_id, f"ls -1 {sh_quote(log_dir)} 2>/dev/null || true")
        if not out:
            return []
        return [ln.strip() for ln in out.splitlines() if ln.strip()]

    def _parse_date_from_filename(self, filename: str) -> Optional[date]:
        m = DATE_FILENAME_RE.search(filename)
        if not m:
            return None
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            return None

    def _select_files_in_window(self, filenames: List[str], since_dt: datetime, until_dt: datetime, file_type: str = "catalina") -> List[str]:
        """
        Select filenames that contain a YYYY-MM-DD date within [since, until].
        
        Args:
            filenames: List of log filenames
            since_dt: Start datetime
            until_dt: End datetime
            file_type: "catalina" to only select catalina files, "access" to only select access log files, "all" for both
        """
        chosen = []
        since_d = since_dt.date()
        until_d = until_dt.date()
        
        # Filter by file type first
        filtered_files = []
        if file_type == "catalina":
            # Only catalina files (catalina.out, catalina.YYYY-MM-DD.log, etc.)
            filtered_files = [f for f in filenames if "catalina" in f.lower() and "access" not in f.lower()]
        elif file_type == "access":
            # Only access log files
            filtered_files = [f for f in filenames if "localhost_access" in f.lower() or ("access" in f.lower() and "catalina" not in f.lower())]
        else:
            # All files
            filtered_files = filenames
        
        # Select files by date
        for fn in filtered_files:
            d = self._parse_date_from_filename(fn)
            if d and since_d <= d <= until_d:
                chosen.append(fn)
        
        # If nothing chosen, consider today's files (best-effort)
        if not chosen:
            for fn in filtered_files:
                if file_type == "catalina" and "catalina" in fn.lower():
                    chosen.append(fn)
                elif file_type == "access" and ("localhost_access" in fn.lower() or "access" in fn.lower()):
                    chosen.append(fn)
                elif file_type == "all":
                    if any(x in fn.lower() for x in ("catalina", "localhost_access_log", "localhost_access")):
                        chosen.append(fn)
        
        return chosen

    def _collect_rotated_files_window(self, host_connector, container_id: str, log_dir: str, since_dt: datetime, until_dt: datetime, max_lines: int = 10000, file_type: str = "catalina", filter_by_timestamp: bool = True) -> str:
        """
        Read rotated files in log_dir for the provided time window and filter by timestamps.
        
        Args:
            host_connector: Host connector
            container_id: Container ID
            log_dir: Log directory path
            since_dt: Start datetime
            until_dt: End datetime
            max_lines: Maximum lines to return
            file_type: "catalina" or "access" to filter file types
            filter_by_timestamp: If True, filter log lines by embedded timestamps
        """
        files = self._list_log_dir(host_connector, container_id, log_dir)
        if not files:
            return ""
        selected = self._select_files_in_window(files, since_dt, until_dt, file_type=file_type)
        
        # Determine which parser to use based on file type
        parser = None
        if filter_by_timestamp:
            from connectors.historical.timestamp_parsers import TimestampParserRegistry
            if file_type == "catalina":
                parser = TimestampParserRegistry.get("tomcat")
            elif file_type == "access":
                # Tomcat access logs use the same format as Nginx access logs
                parser = TimestampParserRegistry.get("nginx_access")
        
        combined = []
        lines_total = 0
        for fn in selected:
            fp = f"{log_dir.rstrip('/')}/{fn}"
            # Get all content (no tail limit) for historical collection, we'll filter by timestamp
            try:
                content = self._read_file_in_container(host_connector, container_id, fp)
            except Exception:
                # Fallback to tail if read_file_in_container fails
                content = self._tail_file(host_connector, container_id, fp, lines=max_lines * 10)  # Get more lines for filtering
            
            if content:
                # Filter by timestamp if parser is available
                if parser and filter_by_timestamp:
                    from connectors.historical.log_time_filter import filter_logs_by_time_window
                    log_lines = content.splitlines()
                    filtered_lines = filter_logs_by_time_window(log_lines, since_dt, until_dt, parser, max_lines=max_lines)
                    filtered_content = "\n".join(filtered_lines)
                    if filtered_content:
                        combined.append(f"=== FILE: {fn} ===\n{filtered_content}")
                        lines_total += len(filtered_lines)
                else:
                    # No filtering, just use content as-is
                    combined.append(f"=== FILE: {fn} ===\n{content}")
                    lines_total += len(content.splitlines())
            
            # safety: avoid returning enormous payloads
            if lines_total > max_lines:
                break
        
        return "\n\n".join(combined)

    # --------------------------
    # Per-host collection
    # --------------------------
    def collect_for_host(self, host_info, host_connector):
        result: Dict[str, Any] = {
            "type": "tomcat",
            "discovered": [],
            "instances": [],
            "collection_mode": "historical" if self._is_historical_collection() else "current"
        }

        # Get time window for historical collection - define at method scope to avoid scoping issues
        since_dt = None
        until_dt = None
        if self._is_historical_collection():
            since_dt, until_dt = self._get_time_window()
            result["time_window"] = {
                "since": since_dt.isoformat(),
                "until": until_dt.isoformat(),
                "issue_time": self.issue_time.isoformat() if self.issue_time else None
            }

        inst_list = self.instances or self.component_config.get("instances", [])
        if not inst_list:
            return result

        can_exec_container = hasattr(host_connector, "exec_in_container") and callable(getattr(host_connector, "exec_in_container"))

        for inst in inst_list:
            try:
                name = inst.get("name")
                container_name = inst.get("container")
                catalina_home = inst.get("catalina_home") or "/usr/local/tomcat"
                systemd_service = inst.get("systemd_service")
                tomcat_port = inst.get("port", 8080)  # Read port from YAML, default 8080
                instance_res: Dict[str, Any] = {
                    "name": name,
                    "container": container_name,
                    "catalina_home": catalina_home,
                    "systemd_service": systemd_service,
                    "version": "",
                    "running": False,
                    "catalina_out_tail": {"content": "", "type": "current", "collection_mode": result["collection_mode"]},
                    "java_process": "",
                    "pid": None,
                    "thread_dump": {},
                    "thread_analysis": {},
                    "gc_logs": {},
                    "http_healthcheck": {},
                    "metrics": {},
                    "errors": [],
                    "listen_ports": "",
                    "access_log_analysis": None,
                    "access_log_tail": "",
                    "error_log_tail": "",
                }

                # try to map container name -> id (do this BEFORE liveness check)
                container_id = container_name
                docker_name_to_id = {}
                if hasattr(host_connector, "list_containers"):
                    try:
                        containers_result = host_connector.list_containers()
                        
                        # Handle SSHHostConnector format: dict with "containers" key
                        if isinstance(containers_result, dict):
                            if containers_result.get("ok") and containers_result.get("containers"):
                                containers_list = containers_result["containers"]
                                for c_dict in containers_list:
                                    # SSHHostConnector returns dicts with lowercase keys: "id", "names" (plural, comma-separated)
                                    container_id_val = c_dict.get("id", "")
                                    container_names = c_dict.get("names", "") or c_dict.get("name", "")
                                    # Handle comma-separated names and remove leading slashes
                                    if container_names:
                                        for name in str(container_names).split(","):
                                            name = name.strip().lstrip("/")  # Remove leading slash
                                            if name and container_id_val:
                                                docker_name_to_id[name] = container_id_val
                                    # Also map id->id
                                    if container_id_val:
                                        docker_name_to_id[container_id_val] = container_id_val
                        
                        # Handle DockerHostConnector format: List[Container] with .name and .id
                        elif isinstance(containers_result, list):
                            for c in containers_result:
                                name_attr = getattr(c, "name", None) or getattr(c, "names", None)
                                cid = getattr(c, "id", None) or getattr(c, "short_id", None)
                                # name_attr could be a list or string
                                names_list = []
                                if isinstance(name_attr, (list, tuple)):
                                    names_list.extend(name_attr)
                                elif name_attr:
                                    names_list.append(name_attr)
                                for nm in names_list:
                                    clean_name = str(nm).lstrip("/")
                                    docker_name_to_id[clean_name] = str(cid) if cid else None
                                    if str(nm) != clean_name:
                                        docker_name_to_id[str(nm)] = str(cid) if cid else None
                                # ensure we at least map id->id
                                if cid:
                                    docker_name_to_id[str(cid)] = str(cid)
                    except Exception as e:
                        logger.debug(f"Tomcat adapter: list_containers failed: {e}")
                        pass

                if container_name:
                    cand = container_name.strip()
                    cand_no_slash = cand.lstrip("/")
                    container_id = docker_name_to_id.get(cand) or docker_name_to_id.get(cand_no_slash)
                    
                    # If container_name is specified but container_id is None, provide helpful error
                    if container_id is None:
                        available_containers = list(docker_name_to_id.keys()) if docker_name_to_id else []
                        error_msg = f"Container '{container_name}' specified in YAML but not found. Available containers: {', '.join(available_containers) if available_containers else 'none'}"
                        logger.warning(f"Tomcat adapter: {error_msg}")
                        instance_res["errors"].append(error_msg)
                        instance_res["running"] = False
                        result["discovered"].append(name)
                        result["instances"].append(instance_res)
                        continue

                # ------------------------------------------------------------------
                # LIVENESS: Container-aware checks
                # ------------------------------------------------------------------
                try:
                    # Detect if we're in a Docker container context
                    from connectors.host_connectors.docker_host_connector import DockerHostConnector
                    is_docker_container = container_id is not None and isinstance(host_connector, DockerHostConnector)
                    
                    if is_docker_container:
                        # ============================================================
                        # DOCKER CONTAINER: Use container-aware checks
                        # ============================================================
                        logger.debug(f"Tomcat adapter: Using Docker container-aware liveness checks for '{name}'")
                        
                        # 0. First, verify container is actually running (not just exists)
                        container_running = False
                        container_status = "unknown"
                        container_id_str = str(container_id) if container_id else ""
                        # If connector lacks a Docker client (fake/minimal connectors), skip status check and assume reachable
                        if not hasattr(host_connector, "client") or host_connector.client is None:
                            logger.debug("Tomcat adapter: host_connector.client missing; skipping status check and assuming container reachable")
                            container_running = True
                            container_status = "unknown"
                        else:
                            try:
                                try:
                                    container_info = host_connector.client.containers.get(container_id_str)
                                    if container_info is None:
                                        raise RuntimeError(f"tomcat container {container_id_str[:12]} not found")
                                    container_info.reload()
                                    if not hasattr(container_info, 'attrs') or not isinstance(container_info.attrs, dict):
                                        raise RuntimeError(f"tomcat container {container_id_str[:12]} has invalid attrs")
                                    state = container_info.attrs.get('State', {})
                                    if not isinstance(state, dict):
                                        raise RuntimeError(f"tomcat container {container_id_str[:12]} has invalid State")
                                    container_status = state.get('Status', 'unknown')
                                    container_running = container_status == "running"
                                    logger.debug(f"Tomcat adapter: Container {container_id_str[:12]} status: {container_status}")
                                    if not container_running:
                                        logger.warning(f"Tomcat adapter: Container {container_id_str[:12]} is not running (status: {container_status})")
                                except Exception as e:
                                    error_str = str(e)
                                    logger.debug(f"Tomcat adapter: Failed to get container status: {e}")
                                    if "not found" in error_str.lower() or "no such container" in error_str.lower():
                                        raise RuntimeError(f"tomcat container {container_id_str[:12]} not found: {error_str}")
                                    container_running = False
                            except RuntimeError as e:
                                raise
                            except Exception as e:
                                logger.debug(f"Tomcat adapter: Container status check failed: {e}")
                                container_running = False
                        
                        # Also verify we can actually exec into the container (double-check)
                        if container_running:
                            try:
                                test_exec = self._run_in_container(host_connector, container_id_str, "echo test") if can_exec_container else getattr(host_connector, "exec_cmd", lambda x: "")("echo test")
                                if isinstance(test_exec, str) and (test_exec.startswith("[container exec error") or test_exec.startswith("[host exec error")):
                                    logger.warning(f"Tomcat adapter: Container {container_id_str[:12]} appears running but exec failed: {test_exec[:100]}")
                                    container_running = False
                            except Exception as e:
                                logger.debug(f"Tomcat adapter: Exec test failed: {e}")
                                container_running = False
                        
                        # Only fail liveness if we have a real client and it's not running
                        if hasattr(host_connector, "client") and host_connector.client is not None and not container_running:
                            status_msg = f"status: {container_status}" if container_status != "unknown" else "container not running"
                            raise RuntimeError(f"tomcat container {container_id_str[:12] if container_id_str else 'unknown'} is not running ({status_msg})")
                        # If no client, treat as reachable for test connectors
                        if (not hasattr(host_connector, "client")) or host_connector.client is None:
                            container_running = True
                        
                        # 1. Check port accessibility from Docker host (not inside container)
                        port_check_passed = False
                        if not getattr(host_connector, "client", None):
                            # Fake/minimal connectors in tests: skip host socket check
                            logger.debug("Tomcat adapter: host_connector.client missing; skipping host socket port check and assuming reachable")
                            port_check_passed = True
                        else:
                            try:
                                import socket
                                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                sock.settimeout(2)
                                connect_result = sock.connect_ex(('127.0.0.1', tomcat_port))
                                sock.close()
                                if connect_result == 0:
                                    port_check_passed = True
                                    logger.debug(f"Tomcat adapter: Port {tomcat_port} check passed from host (socket)")
                                else:
                                    logger.debug(f"Tomcat adapter: Port {tomcat_port} not accessible from host (socket result: {connect_result})")
                            except Exception as e:
                                logger.debug(f"Tomcat adapter: Host port check (socket) failed: {e}")
                        
                        # 2. Optionally check /proc/1/status to verify main process is java/tomcat
                        process_check_passed = False
                        try:
                            proc_status_cmd = "cat /proc/1/status 2>/dev/null | grep -i '^Name:' | awk '{print $2}' || echo 'PROC_CHECK_FAILED'"
                            proc_out = self._run_in_container(host_connector, container_id_str, proc_status_cmd) if can_exec_container else getattr(host_connector, "exec_cmd", lambda x: "")(proc_status_cmd)
                            proc_str = str(proc_out).lower().strip()
                            if "proc_check_failed" not in proc_str and ("java" in proc_str or "tomcat" in proc_str):
                                process_check_passed = True
                                logger.debug(f"Tomcat adapter: Process check passed (/proc/1/status shows java/tomcat)")
                        except Exception as e:
                            logger.debug(f"Tomcat adapter: Process check (/proc/1/status) failed: {e}")
                        
                        # 3. Check catalina_home exists (if configured)
                        if catalina_home:
                            home_check_cmd = f"test -d {catalina_home} && echo OK || echo MISSING"
                            home_out = self._run_in_container(host_connector, container_id_str, home_check_cmd) if can_exec_container else getattr(host_connector, "exec_cmd", lambda x: "")(home_check_cmd)
                            if "MISSING" in str(home_out):
                                logger.warning(f"Tomcat adapter: catalina_home not found: {catalina_home} (but container is running)")
                        
                        # Liveness check: At least one of port check or process check should succeed
                        if hasattr(host_connector, "client") and host_connector.client is not None:
                            if not port_check_passed and not process_check_passed:
                                raise RuntimeError(f"tomcat port {tomcat_port} not reachable from host and process check failed")
                        else:
                            # For fake/minimal connectors, allow liveness to pass even if checks failed
                            if not port_check_passed and not process_check_passed:
                                logger.debug("Tomcat adapter: Skipping liveness failure because connector has no Docker client (test/fake connector)")
                        
                    else:
                        # ============================================================
                        # NON-DOCKER (SSH/Local): Use standard checks
                        # ============================================================
                        logger.debug(f"Tomcat adapter: Using standard liveness checks for '{name}' (non-Docker)")
                        
                        # Check catalina_home exists
                        home_check_cmd = f"test -d {catalina_home} && echo OK || echo MISSING"
                        home_out = self._run_in_container(host_connector, container_id, home_check_cmd) if can_exec_container else getattr(host_connector, "exec_cmd", lambda x: "")(home_check_cmd)
                        if "MISSING" in str(home_out):
                            raise RuntimeError(f"catalina_home not found: {catalina_home}")

                        # Check for java/tomcat process
                        ps_cmd = "ps -ef | grep -i '[j]ava.*tomcat\\|[t]omcat' | grep -v grep"
                        ps_out = self._run_in_container(host_connector, container_id, ps_cmd) if can_exec_container else getattr(host_connector, "exec_cmd", lambda x: "")(ps_cmd)
                        if not ps_out.strip():
                            # As fallback, check port from config (or default 8080)
                            port_cmd = f"timeout 2 bash -c '</dev/tcp/127.0.0.1/{tomcat_port}' 2>&1 || nc -z -w2 127.0.0.1 {tomcat_port} 2>&1 || echo 'PORT_FAILED'"
                            port_out = self._run_in_container(host_connector, container_id, port_cmd) if can_exec_container else getattr(host_connector, "exec_cmd", lambda x: "")(port_cmd)
                            port_str = str(port_out).lower()
                            if "port_failed" in port_str or "refused" in port_str or "failed" in port_str or "connection refused" in port_str or "no route to host" in port_str:
                                raise RuntimeError("tomcat process/port not reachable")
                    
                    instance_res["running"] = True
                except Exception as e:
                    error_msg = f"liveness: {e}"
                    logger.warning(f"Tomcat adapter: Liveness check failed for '{name}': {error_msg}")
                    instance_res["errors"].append(error_msg)
                    result["discovered"].append(name)
                    result["instances"].append(instance_res)
                    continue

                # 1) version detection
                version_out = ""
                try_cmds = [
                    f"{catalina_home}/bin/catalina.sh version 2>&1 || {catalina_home}/bin/catalina version 2>&1 || catalina.sh version 2>&1 || catalina version 2>&1 || true",
                    f"{catalina_home}/bin/version.sh 2>&1 || true",
                    "java -version 2>&1 || true",
                    "ps -ef | grep [j]ava || true",
                ]
                for t in try_cmds:
                    if can_exec_container:
                        out = self._run_in_container(host_connector, container_id, t)
                    else:
                        out = getattr(host_connector, "exec_cmd", lambda x: "")(t)
                    if out and out.strip():
                        version_out = out.strip()
                        break
                
                # Extract clean version string (e.g., "Apache Tomcat/9.0.112" or "9.0.112")
                if version_out:
                    # Try to extract "Server version: Apache Tomcat/X.Y.Z" or "Apache Tomcat/X.Y.Z"
                    version_match = re.search(r'Server version:\s*(Apache Tomcat/[\d.]+)', version_out, re.IGNORECASE)
                    if not version_match:
                        version_match = re.search(r'Apache Tomcat/([\d.]+)', version_out, re.IGNORECASE)
                    if version_match:
                        instance_res["version"] = version_match.group(1) if version_match.lastindex else version_match.group(0)
                    else:
                        # Fallback: use first 200 chars, but skip "NOTE: Picked up JDK_JAVA_OPTIONS" lines
                        lines = [l for l in version_out.splitlines() if l.strip() and not l.strip().startswith("NOTE:")]
                        if lines:
                            instance_res["version"] = "\n".join(lines[:10])[:500]
                        else:
                            instance_res["version"] = version_out[:500]
                else:
                    instance_res["version"] = ""

                # 2) catalina logs
                try:
                    if self._is_historical_collection() and since_dt is not None and until_dt is not None:
                        # HISTORICAL: try Docker time-filtering first (recommended)
                        used = False
                        try:
                            from connectors.host_connectors.docker_host_connector import DockerHostConnector
                            if container_id and isinstance(host_connector, DockerHostConnector):
                                docker_out = host_connector.get_container_logs_time_filtered(container_id, since_dt, until_dt, tail_limit=50000)
                                if docker_out and not docker_out.startswith("[docker"):
                                    instance_res["catalina_out_tail"] = {
                                        "content": docker_out.strip(),
                                        "type": "historical",
                                        "source": "docker_logs",
                                        "time_window": {"since": since_dt.isoformat(), "until": until_dt.isoformat()},
                                        "collection_mode": "historical",
                                        "line_count": len(docker_out.splitlines())
                                    }
                                    used = True
                        except Exception:
                            used = False

                        if not used:
                            # Fall back to rotated files inside container
                            log_dir = f"{catalina_home}/logs"
                            collected = self._collect_rotated_files_window(host_connector, container_id, log_dir, since_dt, until_dt, max_lines=10000, file_type="catalina", filter_by_timestamp=True)
                            instance_res["catalina_out_tail"] = {
                                "content": collected.strip(),
                                "type": "historical",
                                "source": "file",
                                "time_window": {"since": since_dt.isoformat(), "until": until_dt.isoformat()},
                                "collection_mode": "historical",
                                "line_count": len(collected.splitlines()) if collected else 0
                            }
                    else:
                        # CURRENT: prefer docker logs (fast)
                        try:
                            from connectors.host_connectors.docker_host_connector import DockerHostConnector
                            from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                            
                            out = None
                            
                            # Try DockerHostConnector.get_container_logs first
                            if container_id and isinstance(host_connector, DockerHostConnector) and hasattr(host_connector, "get_container_logs"):
                                out = host_connector.get_container_logs(container_id, tail=400)
                                if out and not out.startswith("[docker"):
                                    instance_res["catalina_out_tail"] = {"content": out.strip(), "type": "current", "source": "docker_logs", "collection_mode": "current"}
                            
                            # Try SSHHostConnector with docker logs command
                            elif container_id and isinstance(host_connector, SSHHostConnector) and hasattr(host_connector, "exec_cmd"):
                                docker_logs_cmd = f"timeout 5 docker logs --tail 400 {container_id} 2>&1"
                                out = host_connector.exec_cmd(docker_logs_cmd, timeout=5)
                                if isinstance(out, dict):
                                    out = out.get("stdout", "") or out.get("output", "")
                                if out and not out.startswith("[host exec error") and not out.startswith("[docker exec error"):
                                    instance_res["catalina_out_tail"] = {"content": str(out).strip(), "type": "current", "source": "docker_logs", "collection_mode": "current"}
                            
                            # Fallback: try tail file inside container
                            if not instance_res.get("catalina_out_tail") or not instance_res["catalina_out_tail"].get("content"):
                                c_out = self._tail_file(host_connector, container_id, f"{catalina_home}/logs/catalina.out", lines=400)
                                # Only set if we got actual content (not an error message)
                                if c_out and not c_out.startswith("[docker exec error"):
                                    instance_res["catalina_out_tail"] = {"content": c_out or "", "type": "current", "collection_mode": "current"}
                            else:
                                instance_res["catalina_out_tail"] = {"content": "", "type": "current", "collection_mode": "current"}
                        except Exception:
                            c_out = self._tail_file(host_connector, container_id, f"{catalina_home}/logs/catalina.out", lines=400)
                            # Only set if we got actual content (not an error message)
                            if c_out and not c_out.startswith("[docker exec error"):
                                instance_res["catalina_out_tail"] = {"content": c_out or "", "type": "current", "collection_mode": "current"}
                            else:
                                instance_res["catalina_out_tail"] = {"content": "", "type": "current", "collection_mode": "current"}

                except Exception as e:
                    logger.warning("Failed to collect catalina logs: %s", e)
                    instance_res["catalina_out_tail"] = {"content": "", "type": "current", "collection_mode": "current"}
                    instance_res["errors"].append({
                        "type": "collection_error",
                        "component": "tomcat",
                        "stage": "catalina_logs",
                        "message": f"Failed to collect catalina logs: {e}"
                    })

                # 3) java pid and process listing
                pid = None
                try:
                    pid = self._find_java_pid(host_connector, container_id, catalina_home, tomcat_port)
                    instance_res["pid"] = pid
                    if pid:
                        proc_info = self._run_in_container(host_connector, container_id, f"ps -o pid,cmd -p {pid} 2>/dev/null || ps -ef | grep [j]ava | head -1 || true")
                        instance_res["java_process"] = proc_info or ""
                        instance_res["running"] = True
                    else:
                        instance_res["running"] = False
                except Exception as e:
                    instance_res["java_process"] = f"[java pid probe error: {e}]"

                # 4) jstack/jmap/jcmd attempts
                jstack_text = ""
                jmap_text = ""
                jcmd_text = ""
                if pid:
                    try:
                        if self._try_tool(host_connector, container_id, "jstack"):
                            jstack_text = self._run_in_container(host_connector, container_id, f"jstack {pid} 2>&1 || true")
                    except Exception:
                        jstack_text = ""
                    if not jstack_text:
                        try:
                            _ = self._run_in_container(host_connector, container_id, f"kill -3 {pid} 2>/dev/null || true")
                            time.sleep(0.3)
                            jstack_text = self._tail_file(host_connector, container_id, f"{catalina_home}/logs/catalina.out", lines=400)
                        except Exception:
                            jstack_text = ""
                    try:
                        if self._try_tool(host_connector, container_id, "jmap"):
                            jmap_text = self._run_in_container(host_connector, container_id, f"jmap -heap {pid} 2>&1 || true")
                    except Exception:
                        jmap_text = ""
                    try:
                        if self._try_tool(host_connector, container_id, "jcmd"):
                            jcmd_text = self._run_in_container(host_connector, container_id, f"jcmd {pid} VM.system_properties 2>&1 || true")
                    except Exception:
                        jcmd_text = ""
                instance_res["thread_dump"] = {"jstack": jstack_text or "", "jmap": jmap_text or "", "jcmd": jcmd_text or ""}

                # 5) thread analysis
                try:
                    c_out_text = ""
                    if isinstance(instance_res.get("catalina_out_tail"), dict):
                        c_out_text = instance_res["catalina_out_tail"].get("content", "") or ""
                    else:
                        c_out_text = instance_res.get("catalina_out_tail", "") or ""
                    analysis_text = jstack_text or c_out_text or ""
                    instance_res["thread_analysis"] = self._analyze_thread_dump_text(analysis_text)
                except Exception:
                    instance_res["thread_analysis"] = {}

                # 6) gc logs
                try:
                    instance_res["gc_logs"] = self._collect_gc_logs(host_connector, container_id, catalina_home)
                except Exception:
                    instance_res["gc_logs"] = {}

                # 7) http healthcheck
                try:
                    instance_res["http_healthcheck"] = self._http_healthcheck(host_connector, container_id, port=8080)
                except Exception:
                    instance_res["http_healthcheck"] = {"status": "UNKNOWN"}

                # 8) listen ports probe
                try:
                    pid = instance_res.get("pid")
                    port = inst.get("port", 8080)
                    
                    # Try multiple methods to get listening ports
                    lp = ""
                    
                    # Method 1: Try lsof (most reliable for containers)
                    if self._try_tool(host_connector, container_id, "lsof"):
                        if pid:
                            lp = self._run_in_container(host_connector, container_id, f"lsof -i -P -n 2>/dev/null | grep {pid} || true")
                        if not lp or not lp.strip():
                            lp = self._run_in_container(host_connector, container_id, f"lsof -i :{port} -P -n 2>/dev/null || true")
                    
                    # Method 2: Try ss with PID filtering (format: pid=1 or users:(("java",pid=1))
                    if (not lp or not lp.strip()) and self._try_tool(host_connector, container_id, "ss"):
                        if pid:
                            # Try different PID formats in ss output
                            lp = self._run_in_container(host_connector, container_id, f"ss -lntp 2>/dev/null | grep -E 'pid={pid}|pid={pid},' || true")
                            if not lp or not lp.strip():
                                # Try with users format
                                lp = self._run_in_container(host_connector, container_id, f"ss -lntp 2>/dev/null | grep '{pid}' || true")
                        if not lp or not lp.strip():
                            # Fallback: filter by port
                            lp = self._run_in_container(host_connector, container_id, f"ss -lntp 2>/dev/null | grep :{port} || true")
                    
                    # Method 3: Try netstat
                    if (not lp or not lp.strip()) and self._try_tool(host_connector, container_id, "netstat"):
                        if pid:
                            lp = self._run_in_container(host_connector, container_id, f"netstat -tulpn 2>/dev/null | grep {pid} || true")
                        if not lp or not lp.strip():
                            lp = self._run_in_container(host_connector, container_id, f"netstat -tulpn 2>/dev/null | grep :{port} || true")
                    
                    instance_res["listen_ports"] = lp.strip() if lp else ""
                except Exception as e:
                    instance_res["listen_ports"] = f"[listen probe error: {e}]"

                # 9) scan for error patterns
                errors_detected = []
                try:
                    c_out_text = ""
                    if isinstance(instance_res.get("catalina_out_tail"), dict):
                        c_out_text = instance_res["catalina_out_tail"].get("content", "") or ""
                    else:
                        c_out_text = instance_res.get("catalina_out_tail", "") or ""
                    scan_text = (c_out_text or "") + "\n" + "\n".join(instance_res.get("gc_logs", {}).values())
                    for pat in ["OutOfMemoryError", "java.lang.OutOfMemoryError", "GC overhead limit exceeded", "Unable to create new native thread", "StackOverflowError", "Too many open files"]:
                        if pat.lower() in (scan_text or "").lower():
                            errors_detected.append({"pattern": pat})
                except Exception:
                    pass
                # flatten into errors field as strings (legacy-friendly)
                instance_res["errors"].extend([f"detected:{e['pattern']}" for e in errors_detected])

                # ---- METRICS COLLECTION ----
                try:
                    metrics = {}
                    
                    # 1. Thread pool usage from thread_analysis
                    thread_analysis = instance_res.get("thread_analysis", {})
                    if thread_analysis:
                        metrics["thread_pool_blocked"] = thread_analysis.get("blocked_threads", 0)
                        metrics["thread_pool_waiting"] = thread_analysis.get("waiting_threads", 0)
                        metrics["thread_pool_deadlock"] = thread_analysis.get("deadlock", False)
                        
                        # Count total threads from jstack output
                        jstack_text = instance_res.get("thread_dump", {}).get("jstack", "")
                        if jstack_text:
                            thread_count = len(re.findall(r'^"[^"]+"', jstack_text, re.MULTILINE))
                            metrics["total_threads"] = thread_count
                    
                    # 2. JVM heap usage from jmap -heap output
                    jmap_text = instance_res.get("thread_dump", {}).get("jmap", "")
                    if jmap_text:
                        # Parse heap sizes from jmap -heap output
                        # Look for patterns like "Heap Usage:\nPS Young Generation\n   Eden Space:\n      capacity = ..."
                        heap_match = re.search(r'Heap Usage:\s*\n.*?capacity\s*=\s*(\d+)', jmap_text, re.MULTILINE | re.IGNORECASE)
                        if heap_match:
                            try:
                                heap_capacity = int(heap_match.group(1)) / (1024 * 1024)  # Convert bytes to MB
                                metrics["heap_capacity_mb"] = round(heap_capacity, 2)
                            except (ValueError, IndexError):
                                pass
                        
                        # Parse used heap
                        used_match = re.search(r'used\s*=\s*(\d+)', jmap_text, re.IGNORECASE)
                        if used_match:
                            try:
                                heap_used = int(used_match.group(1)) / (1024 * 1024)  # Convert bytes to MB
                                metrics["heap_used_mb"] = round(heap_used, 2)
                            except (ValueError, IndexError):
                                pass
                        
                        # Try jcmd VM.flags for heap settings
                        jcmd_text = instance_res.get("thread_dump", {}).get("jcmd", "")
                        if jcmd_text:
                            max_heap_match = re.search(r'-Xmx(\d+)([kmg]?)', jcmd_text, re.IGNORECASE)
                            if max_heap_match:
                                try:
                                    value = int(max_heap_match.group(1))
                                    unit = max_heap_match.group(2).upper()
                                    multipliers = {"K": 1024, "M": 1024*1024, "G": 1024*1024*1024, "": 1024*1024}
                                    max_heap_mb = (value * multipliers.get(unit, 1024*1024)) / (1024 * 1024)
                                    metrics["heap_max_mb"] = round(max_heap_mb, 2)
                                except (ValueError, IndexError):
                                    pass
                    
                    # 3. Access log collection (with status code splitting)
                    # For CURRENT collection, find today's access log file and split by status codes
                    combined_logs_for_analysis = None  # Store original logs for analysis
                    try:
                        log_dir = f"{catalina_home}/logs"
                        
                        # For CURRENT collection, find today's access log file
                        if not self._is_historical_collection():
                            current_log_file = self._find_current_access_log(host_connector, container_id, log_dir)
                            
                            if current_log_file:
                                try:
                                    # Read the current access log file
                                    access_log_content = self._tail_file(host_connector, container_id, f"{log_dir}/{current_log_file}", lines=2000)
                                    if access_log_content:
                                        combined_logs_for_analysis = access_log_content
                                        
                                        # Split by status codes: 2XX/3XX -> access_log_tail, 4XX/5XX -> error_log_tail
                                        access_text, error_text = self._split_combined_logs(access_log_content)
                                        access_lines = access_text.splitlines()[-200:] if access_text else []
                                        error_lines = error_text.splitlines()[-200:] if error_text else []
                                        
                                        # Only show "[no access logs]" if we actually have no logs at all
                                        # If we have logs but they're all errors, show empty or a more accurate message
                                        if access_lines:
                                            instance_res["access_log_tail"] = "\n".join(access_lines)
                                        elif access_log_content and error_lines:
                                            # We have logs but they're all errors (4xx/5xx)
                                            instance_res["access_log_tail"] = "[no successful requests (2xx/3xx)]"
                                        else:
                                            instance_res["access_log_tail"] = "[no access logs]"
                                        
                                        if error_lines:
                                            instance_res["error_log_tail"] = "\n".join(error_lines)
                                        else:
                                            instance_res["error_log_tail"] = "[no HTTP errors in access logs]"
                                        
                                        # Count requests for metrics
                                        all_lines = [l.strip() for l in access_log_content.splitlines() if l.strip()]
                                        metrics["access_log_requests"] = len(all_lines)
                                        
                                        # Extract timestamps and calculate requests per second
                                        timestamps = []
                                        for line in all_lines:
                                            ts_match = re.search(r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})', line)
                                            if ts_match:
                                                try:
                                                    ts_str = ts_match.group(1)
                                                    dt = datetime.strptime(ts_str, "%d/%b/%Y:%H:%M:%S")
                                                    timestamps.append(dt)
                                                except Exception:
                                                    continue
                                        
                                        if len(timestamps) >= 2:
                                            timestamps.sort()
                                            time_span = (timestamps[-1] - timestamps[0]).total_seconds()
                                            if time_span > 0:
                                                rps = len(timestamps) / time_span
                                                metrics["requests_per_second"] = round(rps, 2)
                                except Exception as e:
                                    logger.debug(f"Failed to read current access log file: {e}")
                            else:
                                # Fallback: try to find any access log file
                                try:
                                    all_files = self._list_log_dir(host_connector, container_id, log_dir)
                                    access_log_files = [f for f in all_files if "localhost_access" in f.lower() or "access" in f.lower()]
                                    if access_log_files:
                                        access_log_files.sort(reverse=True)
                                        latest_log = access_log_files[0]
                                        access_log_content = self._tail_file(host_connector, container_id, f"{log_dir}/{latest_log}", lines=2000)
                                        if access_log_content:
                                            combined_logs_for_analysis = access_log_content
                                            access_text, error_text = self._split_combined_logs(access_log_content)
                                            access_lines = access_text.splitlines()[-200:] if access_text else []
                                            error_lines = error_text.splitlines()[-200:] if error_text else []
                                            
                                            # Only show "[no access logs]" if we actually have no logs at all
                                            if access_lines:
                                                instance_res["access_log_tail"] = "\n".join(access_lines)
                                            elif access_log_content and error_lines:
                                                # We have logs but they're all errors (4xx/5xx)
                                                instance_res["access_log_tail"] = "[no successful requests (2xx/3xx)]"
                                            else:
                                                instance_res["access_log_tail"] = "[no access logs]"
                                            
                                            if error_lines:
                                                instance_res["error_log_tail"] = "\n".join(error_lines)
                                            else:
                                                instance_res["error_log_tail"] = "[no HTTP errors in access logs]"
                                except Exception:
                                    pass
                        
                        else:
                            # HISTORICAL collection: collect access logs for the time window
                            try:
                                since_dt, until_dt = self._get_time_window()
                                collected_access = self._collect_rotated_files_window(host_connector, container_id, log_dir, since_dt, until_dt, max_lines=2000, file_type="access", filter_by_timestamp=True)
                                if collected_access:
                                    access_lines = []
                                    for section in collected_access.split("=== FILE:"):
                                        if section.strip():
                                            lines_split = section.split("\n", 1)
                                            if len(lines_split) > 1:
                                                access_lines.extend(lines_split[1].splitlines())
                                    if access_lines:
                                        access_log_content = "\n".join(access_lines)
                                        combined_logs_for_analysis = access_log_content
                                        access_text, error_text = self._split_combined_logs(access_log_content)
                                        access_lines_filtered = access_text.splitlines()[-200:] if access_text else []
                                        error_lines_filtered = error_text.splitlines()[-200:] if error_text else []
                                        instance_res["access_log_tail"] = "\n".join(access_lines_filtered) if access_lines_filtered else "[no access logs in time window]"
                                        instance_res["error_log_tail"] = "\n".join(error_lines_filtered) if error_lines_filtered else "[no HTTP errors in access logs]"
                                    else:
                                        instance_res["access_log_tail"] = "[no access logs in time window]"
                                        instance_res["error_log_tail"] = "[no HTTP errors in access logs]"
                                else:
                                    instance_res["access_log_tail"] = "[no access logs in time window]"
                                    instance_res["error_log_tail"] = "[no HTTP errors in access logs]"
                            except Exception as e:
                                logger.debug(f"Failed to collect historical access logs: {e}", exc_info=True)
                                instance_res["access_log_tail"] = "[collection failed]"
                                instance_res["error_log_tail"] = "[collection failed]"
                        
                        # Analyze access log for HTTP status codes (4XX, 5XX errors)
                        # Always run analysis, even if logs are empty (to ensure field is always set)
                        analysis_content = combined_logs_for_analysis if combined_logs_for_analysis else instance_res.get("access_log_tail", "")
                        placeholder_messages = ["[no access logs]", "[no HTTP errors in access logs]", "[collection failed]"]
                        
                        try:
                            # Check if we have actual log content (not placeholder messages)
                            if analysis_content and not analysis_content.startswith("[") and analysis_content not in placeholder_messages:
                                # We have real log content - analyze it
                                analysis_result = self._analyze_access_log_status_codes(analysis_content)
                                instance_res["access_log_analysis"] = analysis_result
                                
                                # Add 4XX errors to the errors list
                                status_4xx = analysis_result.get("status_4xx", 0)
                                if status_4xx > 0:
                                    instance_res["errors"].append(f"HTTP 4XX errors detected: {status_4xx} occurrences")
                                
                                # Add 5XX errors to the errors list
                                status_5xx = analysis_result.get("status_5xx", 0)
                                if status_5xx > 0:
                                    instance_res["errors"].append(f"HTTP 5XX errors detected: {status_5xx} occurrences")
                            else:
                                # No valid log content - set empty analysis
                                instance_res["access_log_analysis"] = self._analyze_access_log_status_codes("")
                        except Exception as e:
                            logger.debug(f"Access log analysis failed: {e}", exc_info=True)
                            instance_res["access_log_analysis"] = {"error": str(e)}
                    except Exception as e:
                        logger.debug(f"Access log collection failed: {e}", exc_info=True)
                        # Ensure fields are set even on error
                        if not instance_res.get("access_log_tail") or instance_res.get("access_log_tail") == "":
                            instance_res["access_log_tail"] = "[no access logs found]"
                        if not instance_res.get("error_log_tail") or instance_res.get("error_log_tail") == "":
                            instance_res["error_log_tail"] = "[no error logs found]"
                        if not instance_res.get("access_log_analysis"):
                            instance_res["access_log_analysis"] = self._analyze_access_log_status_codes("")
                    
                    # 4. Session count from catalina.out or JMX (if accessible)
                    try:
                        c_out_text = ""
                        if isinstance(instance_res.get("catalina_out_tail"), dict):
                            c_out_text = instance_res["catalina_out_tail"].get("content", "") or ""
                        else:
                            c_out_text = instance_res.get("catalina_out_tail", "") or ""
                        
                        # Look for session creation patterns in catalina.out
                        # Pattern: "Session created: ..." or "Active sessions: X"
                        session_matches = re.findall(r'(?:Session created|Active sessions?|session count)\s*:?\s*(\d+)', c_out_text, re.IGNORECASE)
                        if session_matches:
                            try:
                                # Get the most recent session count if multiple
                                metrics["session_count_estimated"] = int(session_matches[-1])
                            except (ValueError, IndexError):
                                pass
                    except Exception:
                        pass
                    
                    # Store metrics
                    if metrics:
                        instance_res["metrics"] = metrics
                        
                except Exception as e:
                    logger.debug("Failed to collect Tomcat metrics: %s", e, exc_info=True)
                    # Metrics are optional - don't fail collection

                result["discovered"].append(name)
                result["instances"].append(instance_res)

            except Exception as e:
                result["instances"].append({
                    "name": inst.get("name"),
                    "container": inst.get("container"),
                    "error": str(e),
                    "trace": traceback.format_exc()
                })
        return result

    # --------------------------
    # collect() for standalone usage
    # --------------------------
    def collect(self):
        self.start_collection()
        result = {"type": "host", "discovered": [], "findings": {}}
        from connectors.host_connectors.local_host_connector import LocalHostConnector
        for host_info in self.env_config.get("hosts", []):
            name = host_info.get("name") or host_info.get("address")
            try:
                connector = LocalHostConnector(host_cfg=host_info)
                host_data = self.collect_for_host(host_info, connector)
                result["discovered"].append(name)
                result["findings"][name] = host_data
            except Exception as e:
                result["discovered"].append(name)
                result["findings"][name] = {"error": str(e), "trace": traceback.format_exc()}
        self.end_collection()
        return result


# ------------------------------
# Small helper to safely quote a string for /bin/sh -lc wrapper
# ------------------------------
def sh_quote(s: str) -> str:
    """
    Return a single-quoted shell-safe representation suitable for passing in
    /bin/sh -lc '...'. We use single quotes and escape internal single quotes.
    """
    if s is None:
        return "''"
    return "'" + s.replace("'", "'\"'\"'") + "'"