# connectors/adapters/nginx_adapter.py
"""
Refactored NginxAdapter - safer log handling and docker-log aware.

Key behavior changes:
 - Detects when configured log files are symlinks to /dev/stdout or /dev/stderr
 - If so, uses DockerHostConnector.get_container_logs / get_container_logs_time_filtered
   instead of reading the file (prevents hangs on /dev/stdout)
 - Tries to preserve historical collection using time-filtered docker logs
 - Falls back to file reads or `tail` only when safe
"""

from connectors.base_connector import BaseConnector
from connectors.historical import HistoricalLogCollectionMixin
import traceback
import shlex
import re
from collections import Counter
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger("grcai.adapters.nginx")

DEFAULT_CONF_PATH = "/etc/nginx/nginx.conf"
DEFAULT_ACCESS_LOG = "/var/log/nginx/access.log"
DEFAULT_ERROR_LOG = "/var/log/nginx/error.log"


class NginxAdapter(BaseConnector, HistoricalLogCollectionMixin):

    def __init__(self, name=None, env_config=None, issue_time=None, component_config=None, **kwargs):
        super().__init__(name or "nginx", issue_time, component_config)
        self.env_config = env_config or {}
        self.component_config = component_config or {}
        self.instances_cfg = self.component_config.get("instances", [])
        self.hosts = self.env_config.get("hosts", [])

        safe_hosts = [h.get("name") for h in (self.env_config.get("hosts") or [])]
        logger.debug(f"NginxAdapter.__init__: component_config={component_config}")
        logger.debug(f"NginxAdapter.__init__: instances_cfg={self.instances_cfg}")
        logger.info("NginxAdapter initialized; instances=%d hosts=%s",
                    len(self.instances_cfg), safe_hosts)

    # ---------------------------
    # Host connector selection
    # ---------------------------
    def _make_host_connector(self, host_info):
        from connectors.host_connectors.docker_host_connector import DockerHostConnector
        from connectors.host_connectors.ssh_host_connector import SSHHostConnector
        from connectors.host_connectors.local_host_connector import LocalHostConnector

        access = self.env_config.get("access", {})

        if host_info.get("docker"):
            return DockerHostConnector(host_info=host_info, global_access=access)
        if host_info.get("ssh"):
            return SSHHostConnector(host_info=host_info, global_access=access)
        return LocalHostConnector(host_info=host_info)

    # ---------------------------
    # Shell wrap and exec helpers
    # ---------------------------
    def _sh_wrap(self, cmd: str) -> str:
        safe = cmd.replace('"', '\\"')
        return f'sh -c "{safe}"'

    def _exec(self, host_connector, container_id, cmd, timeout=None):
        """
        Execute a command: prefer container exec, fallback to host exec.
        Returns stdout string or raises RuntimeError with normalized message.
        """
        wrapped = self._sh_wrap(cmd)
        logger.debug("exec cmd=%s container_id=%s", cmd, container_id)

        # If we have a container_id, try container execution methods
        if container_id:
            # Check connector type to determine execution method
            from connectors.host_connectors.docker_host_connector import DockerHostConnector
            from connectors.host_connectors.ssh_host_connector import SSHHostConnector
            
            # SSHHostConnector: use docker exec via exec_cmd (SSHHostConnector doesn't implement exec_in_container)
            if isinstance(host_connector, SSHHostConnector) and hasattr(host_connector, "exec_cmd"):
                try:
                    # Use docker exec to run command inside container
                    docker_cmd = f'docker exec {container_id} {wrapped}'
                    out = host_connector.exec_cmd(docker_cmd, timeout=timeout)
                    if isinstance(out, dict):
                        stdout = out.get("stdout", "")
                        stderr = out.get("stderr", "")
                        rc = out.get("rc", 1)
                        # Check if command succeeded (rc=0) or if we got output
                        if rc == 0 or stdout:
                            return stdout
                        # If failed, return error message
                        error_msg = stderr or out.get("error", "Command failed")
                        error_preview = error_msg[:100] if error_msg else "None"
                        logger.debug(f"Nginx adapter: docker exec failed: rc={rc}, stderr='{error_preview}'")
                        return f"[docker exec error: {error_msg}]"
                    return str(out or "")
                except Exception as e:
                    logger.warning(f"Nginx adapter: docker exec exception: {e}", exc_info=True)
                    return f"[docker exec error: {e}]"
            
            # DockerHostConnector: use exec_in_container (actually implemented)
            elif isinstance(host_connector, DockerHostConnector) and hasattr(host_connector, "exec_in_container"):
                try:
                    # Check if exec_in_container supports timeout parameter
                    import inspect
                    sig = inspect.signature(host_connector.exec_in_container)
                    if 'timeout' in sig.parameters:
                        out = host_connector.exec_in_container(container_id, wrapped, timeout=timeout)
                    else:
                        out = host_connector.exec_in_container(container_id, wrapped)
                    if isinstance(out, dict):
                        return out.get("stdout", out.get("output", "")) or ""
                    return str(out or "")
                except Exception as e:
                    logger.debug("exec_in_container error: %s", e, exc_info=True)
                    error_str = str(e).strip() or f"{type(e).__name__} (no message)"
                    raise RuntimeError(f"[docker-host exec error: {error_str}]")

            # Fallback: if host connector can run exec_cmd, attempt `docker exec <id> sh -c "<cmd>"`
            elif hasattr(host_connector, "exec_cmd"):
                try:
                    docker_cmd = f"docker exec {container_id} {wrapped}"
                    out = host_connector.exec_cmd(docker_cmd, timeout=timeout)
                    if isinstance(out, dict):
                        return out.get("stdout", out.get("output", "")) or ""
                    return str(out or "")
                except Exception as e:
                    logger.debug("exec_cmd/docker exec error: %s", e, exc_info=True)
                    error_str = str(e).strip() or f"{type(e).__name__} (no message)"
                    raise RuntimeError(f"[exec_cmd/docker exec error: {error_str}]")

        # Fallback host exec (if no container_id or no container-specific exec method)
        if hasattr(host_connector, "exec_cmd"):
            try:
                # Check if exec_cmd supports timeout parameter
                import inspect
                sig = inspect.signature(host_connector.exec_cmd)
                if 'timeout' in sig.parameters:
                    out = host_connector.exec_cmd(cmd, timeout=timeout)
                else:
                    out = host_connector.exec_cmd(cmd)
                if isinstance(out, dict):
                    return out.get("stdout", out.get("output", "")) or ""
                return str(out or "")
            except Exception as e:
                logger.debug("exec_cmd error: %s", e, exc_info=True)
                error_str = str(e).strip() or f"{type(e).__name__} (no message)"
                raise RuntimeError(f"[host exec error: {error_str}]")

        raise RuntimeError("No execution method on host connector")

    def _read_file(self, host_connector, container_id, path):
        """
        Read a file from container or host. For containers prefer read_file_in_container
        which uses exec safely. If those methods are not available, fallback to exec(cat).
        """
        if container_id and hasattr(host_connector, "read_file_in_container"):
            try:
                out = host_connector.read_file_in_container(container_id, path)
                if isinstance(out, dict):
                    return out.get("stdout", out.get("output", "")) or ""
                return str(out or "")
            except Exception as e:
                logger.debug("read_file_in_container error: %s", e, exc_info=True)
                error_str = str(e).strip() or f"{type(e).__name__} (no message)"
                raise RuntimeError(f"[docker-host read error: {error_str}]")

        if hasattr(host_connector, "read_file"):
            try:
                out = host_connector.read_file(path)
                if isinstance(out, dict):
                    return out.get("stdout", out.get("output", "")) or ""
                return str(out or "")
            except Exception as e:
                logger.debug("read_file error: %s", e, exc_info=True)
                error_str = str(e).strip() or f"{type(e).__name__} (no message)"
                raise RuntimeError(f"[host read error: {error_str}]")

        # last resort: exec cat (may hang if path is /dev/stdout; check for symlink first)
        # Check for symlink to stdout/stderr first to prevent hangs
        try:
            if container_id and self._is_path_symlink_to_stdout(host_connector, container_id, path):
                raise RuntimeError(f"[read error: path {path} is symlink to stdout/stderr - cannot read directly, use Docker logs instead]")
        except Exception:
            # Detection failed - be cautious but continue (might fail, but won't hang)
            pass
        
        try:
            return self._exec(host_connector, container_id, f"cat {shlex.quote(path)}", timeout=5)
        except Exception as e:
            raise RuntimeError(f"[cat fallback error: {e}]")

    # ---------------------------
    # Container os probe
    # ---------------------------
    def _probe_container_type(self, host_connector, container_id):
        probe = {"alpine": False, "busybox": False, "os": None}
        try:
            txt = ""
            try:
                txt = self._read_file(host_connector, container_id, "/etc/os-release")
            except Exception:
                txt = ""

            if txt:
                low = txt.lower()
                probe["os"] = txt.strip().splitlines()[0] if txt.strip() else None
                probe["alpine"] = "alpine" in low
                probe["busybox"] = "busybox" in low

            if not probe["busybox"]:
                try:
                    out = self._exec(host_connector, container_id, "which busybox || true")
                    if "busybox" in out:
                        probe["busybox"] = True
                except Exception:
                    pass

            if not probe["os"]:
                try:
                    u = self._exec(host_connector, container_id, "uname -a")
                    probe["os"] = u.strip()
                except Exception:
                    pass
        except Exception:
            pass

        logger.debug("probe: alpine=%s busybox=%s os=%s",
                     probe["alpine"], probe["busybox"], probe["os"])
        return probe

    # ---------------------------
    # Config parsing / server block parsing
    # ---------------------------
    def _parse_server_blocks(self, text):
        if not text:
            return []
        blocks = []
        server_re = re.compile(r"server\s*\{", re.IGNORECASE)
        for m in server_re.finditer(text):
            idx = m.start()
            open_idx = text.find("{", idx)
            if open_idx == -1:
                continue
            depth = 0
            end_idx = None
            for j in range(open_idx, len(text)):
                if text[j] == "{":
                    depth += 1
                elif text[j] == "}":
                    depth -= 1
                    if depth == 0:
                        end_idx = j
                        break
            if end_idx:
                raw = text[m.start(): end_idx+1]
                listen = re.findall(r"listen\s+([^;]+);", raw)
                snames = re.findall(r"server_name\s+([^;]+);", raw)
                listens = []
                for l in listen:
                    listens.extend([x.strip() for x in re.split(r"\s+", l) if x.strip()])
                server_names = []
                for s in snames:
                    server_names.extend([x.strip() for x in s.split() if x.strip()])
                directives = {}
                for key in ("root", "index", "access_log", "error_log"):
                    vals = re.findall(rf"{key}\s+([^;]+);", raw)
                    if vals:
                        directives[key] = [v.strip() for v in vals]
                blocks.append({
                    "raw": raw.strip(),
                    "listen": listens,
                    "server_name": server_names,
                    "directives": directives
                })
        return blocks

    def _summarize_error_log(self, text):
        """Summarize error log with counts by severity level."""
        if not text:
            return {
                "errors": [],
                "total_lines": 0,
                "error_count": 0,
                "warn_count": 0,
                "crit_count": 0
            }
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        interesting = [l for l in lines if re.search(r"\berror\b|\bwarn\b|\bcrit\b", l, re.IGNORECASE)]
        c = Counter(interesting)
        
        # Count by severity
        error_count = sum(1 for line in lines if re.search(r"\berror\b", line, re.IGNORECASE))
        warn_count = sum(1 for line in lines if re.search(r"\bwarn\b", line, re.IGNORECASE))
        crit_count = sum(1 for line in lines if re.search(r"\bcrit\b", line, re.IGNORECASE))
        
        return {
            "errors": [{"line": line, "count": cnt} for line, cnt in c.most_common()],
            "total_lines": len(lines),
            "error_count": error_count,
            "warn_count": warn_count,
            "crit_count": crit_count
        }

    def _parse_config_metrics(self, config_text):
        """Extract worker_connections and worker_processes from nginx config."""
        metrics = {}
        if not config_text:
            return metrics
        
        # Look for worker_connections in events block
        worker_conn_match = re.search(r"worker_connections\s+(\d+);", config_text, re.IGNORECASE)
        if worker_conn_match:
            metrics["worker_connections"] = int(worker_conn_match.group(1))
        
        # Look for worker_processes
        worker_proc_match = re.search(r"worker_processes\s+(\d+|auto);", config_text, re.IGNORECASE)
        if worker_proc_match:
            val = worker_proc_match.group(1).lower()
            if val == "auto":
                metrics["worker_processes"] = "auto"
            else:
                metrics["worker_processes"] = int(val)
        
        return metrics

    def _get_nginx_stub_status(self, host_connector, container_id):
        """
        Try to fetch nginx stub_status metrics if available.
        Looks for stub_status endpoints on common ports (80, 8080) and paths (/status, /nginx_status, /stub_status).
        Returns dict with metrics or None if unavailable.
        """
        # Try common stub_status locations
        status_paths = ["/status", "/nginx_status", "/stub_status"]
        ports = [80, 8080]  # Common nginx ports
        
        for port in ports:
            for path in status_paths:
                try:
                    # Try curl or wget
                    cmd = f"curl -s http://localhost:{port}{path} 2>/dev/null || wget -qO- http://localhost:{port}{path} 2>/dev/null || true"
                    out = self._exec(host_connector, container_id, cmd, timeout=2)
                    if out and "Active connections" in out:
                        # Parse stub_status output
                        # Format: Active connections: 1\nserver accepts handled requests\n 1 1 1\nReading: 0 Writing: 1 Waiting: 0
                        metrics = {}
                        for line in out.splitlines():
                            if "Active connections:" in line:
                                match = re.search(r"Active connections:\s*(\d+)", line)
                                if match:
                                    metrics["active_connections"] = int(match.group(1))
                            elif "server accepts handled requests" in line:
                                # Next line has the numbers
                                continue
                            elif re.match(r"^\s*\d+\s+\d+\s+\d+\s*$", line):
                                parts = line.strip().split()
                                if len(parts) >= 3:
                                    metrics["accepted_requests"] = int(parts[0])
                                    metrics["handled_requests"] = int(parts[1])
                                    metrics["total_requests"] = int(parts[2])
                            elif "Reading:" in line:
                                match = re.search(r"Reading:\s*(\d+)\s+Writing:\s*(\d+)\s+Waiting:\s*(\d+)", line)
                                if match:
                                    metrics["reading"] = int(match.group(1))
                                    metrics["writing"] = int(match.group(2))
                                    metrics["waiting"] = int(match.group(3))
                        if metrics:
                            return metrics
                except Exception:
                    continue
        return None

    def _analyze_access_log_status_codes(self, access_log_content):
        """
        Analyze HTTP status codes from nginx access log content.
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
        
        # Pattern to match HTTP status code in nginx access log format
        # Format: IP - - [date] "METHOD /path HTTP/1.x" status size
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

    def _calculate_requests_per_second(self, access_log_content):
        """Calculate requests per second from access log content."""
        if not access_log_content:
            return None
        
        # Parse access log timestamps and count requests
        lines = [l.strip() for l in access_log_content.splitlines() if l.strip()]
        if not lines:
            return None
        
        # Extract timestamps from access log lines
        # Format: IP - - [27/Nov/2025:08:09:34 +0000] "GET / HTTP/1.1" 200 287
        timestamps = []
        for line in lines:
            # Match timestamp in format [DD/Mon/YYYY:HH:MM:SS +TZ]
            match = re.search(r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})', line)
            if match:
                try:
                    from datetime import datetime
                    ts_str = match.group(1)
                    # Parse format: 27/Nov/2025:08:09:34
                    dt = datetime.strptime(ts_str, "%d/%b/%Y:%H:%M:%S")
                    timestamps.append(dt)
                except Exception:
                    continue
        
        if len(timestamps) < 2:
            return None
        
        # Calculate time span and requests per second
        timestamps.sort()
        time_span = (timestamps[-1] - timestamps[0]).total_seconds()
        if time_span > 0:
            rps = len(timestamps) / time_span
            return round(rps, 2)
        return None

    def _get_process_uptime(self, host_connector, container_id, process_output):
        """Extract process uptime from ps output or /proc/PID/stat."""
        if not process_output:
            return None
        
        try:
            # Try to extract PID from process output
            pid_match = re.search(r'^\s*(\d+)', process_output, re.MULTILINE)
            if pid_match:
                pid = pid_match.group(1)
                # Try to read /proc/PID/stat for start time
                try:
                    stat_cmd = f"cat /proc/{pid}/stat 2>/dev/null | awk '{{print $22}}' || true"
                    stat_out = self._exec(host_connector, container_id, stat_cmd, timeout=2)
                    if stat_out and stat_out.strip().isdigit():
                        # Calculate uptime from start time (clock ticks since boot)
                        # This is complex, so let's try a simpler approach
                        uptime_cmd = f"ps -o etime= -p {pid} 2>/dev/null || ps -p {pid} -o etime= 2>/dev/null || true"
                        uptime_out = self._exec(host_connector, container_id, uptime_cmd, timeout=2)
                        if uptime_out and uptime_out.strip():
                            return uptime_out.strip()
                except Exception:
                    pass
        except Exception:
            pass
        return None

    # ---------------------------
    # Helpers to detect symlink -> /dev/stdout
    # ---------------------------
    def _resolve_link_target(self, host_connector, container_id, path) -> Optional[str]:
        """
        Try to resolve symlink of `path` inside the container (or host).
        Returns the resolved path string or None if cannot determine.
        Uses explicit timeout to prevent hangs.
        """
        # Use shorter timeout for symlink detection (2 seconds max)
        # Prefer container readlink if available via exec
        try:
            out = self._exec(host_connector, container_id, f"timeout 2 readlink -f {shlex.quote(path)} 2>/dev/null || readlink -f {shlex.quote(path)} 2>/dev/null || true", timeout=2)
            out = out.strip()
            if out and not out.startswith("[") and "error" not in out.lower():
                return out
        except Exception as e:
            logger.debug("readlink failed for %s: %s", path, e)
            pass

        # Try ls -l parsing as fallback (also with timeout)
        try:
            out = self._exec(host_connector, container_id, f"timeout 2 ls -l {shlex.quote(path)} 2>/dev/null || ls -l {shlex.quote(path)} 2>/dev/null || true", timeout=2)
            out = out.strip()
            # example: lrwxrwxrwx 1 root root 11 Nov 18 02:21 /var/log/nginx/access.log -> /dev/stdout
            if "->" in out and not out.startswith("["):
                parts = out.split("->")
                if len(parts) >= 2:
                    target = parts[-1].strip()
                    return target
        except Exception as e:
            logger.debug("ls -l failed for %s: %s", path, e)
            pass

        return None

    def _is_path_symlink_to_stdout(self, host_connector, container_id, path) -> bool:
        """
        Return True if `path` resolves to /dev/stdout or /dev/stderr (or similar).
        Uses safe container execs (readlink / ls) with timeouts, avoiding blocking reads.
        Fails fast (returns False) if detection takes too long to prevent hangs.
        """
        try:
            # Quick heuristic: if path contains common log patterns and we're in a container,
            # assume it might be symlinked and be cautious
            if not container_id:
                return False
                
            target = self._resolve_link_target(host_connector, container_id, path)
            if not target:
                return False
            target = target.lower()
            if "/dev/stdout" in target or "/dev/stderr" in target or "stdout" in target or "stderr" in target:
                logger.debug("Path %s resolves to stdout/stderr target=%s", path, target)
                return True
        except Exception as e:
            logger.debug("Symlink detection failed for %s: %s", path, e)
            pass
        return False

    # ---------------------------
    # Heuristic split of combined docker logs into access vs error
    # ---------------------------
    def _split_combined_logs(self, combined_text):
        """
        Heuristically split Docker logs into nginx access logs vs error logs.
        
        Nginx access log format: IP - - [date] "METHOD /path HTTP/1.x" status size ...
        Nginx error log format: typically contains timestamps, log levels (error, warn, crit), and messages
        Docker-entrypoint.sh logs: start with "/docker-entrypoint.sh:" or similar patterns
        
        Returns tuple(access_text, error_text).
        """
        if not combined_text:
            return "", ""
        access_lines = []
        error_lines = []
        
        # Patterns to identify nginx access logs (common format)
        # Examples: "192.168.65.1 - - [27/Nov/2025:08:09:34 +0000] "GET / HTTP/1.1" 200 287"
        access_pattern = re.compile(
            r'^\S+\s+-\s+-\s+\[.*?\]\s+"(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+.*?\s+HTTP/\d\.\d"\s+\d{3}', 
            re.IGNORECASE
        )
        
        # Pattern to extract HTTP status code from access log lines
        status_pattern = re.compile(r'HTTP/\d\.\d"\s+(\d{3})')
        
        # Patterns to identify docker-entrypoint.sh or initialization logs
        init_pattern = re.compile(r'^(?:/docker-entrypoint\.sh:|/docker-entrypoint\.d/|\.sh:\s+info:)', re.IGNORECASE)
        
        # Patterns for nginx error logs (may have timestamps, log levels, or nginx-specific error messages)
        error_keywords = re.compile(r'\b(?:error|warn|crit|alert|emerg|fatal)\b', re.IGNORECASE)
        nginx_error_prefix = re.compile(r'^\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+\[(?:error|warn|crit|alert|emerg)\]', re.IGNORECASE)
        
        for ln in combined_text.splitlines():
            l = ln.strip()
            if not l:
                continue
            
            # Skip docker-entrypoint.sh and initialization logs entirely
            if init_pattern.search(l):
                continue
            
            # Check if it matches nginx access log format
            if access_pattern.search(l):
                # Extract status code to determine if it's an error (4XX, 5XX) or success (2XX, 3XX)
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
                    # Can't determine status - treat as access log
                    access_lines.append(l)
            # Check if it's clearly an error log line (nginx error log, not access log)
            elif nginx_error_prefix.search(l) or error_keywords.search(l):
                error_lines.append(l)
            # Default: if it doesn't match access log format but has error keywords, treat as error
            # Otherwise, we can't confidently classify it - skip it to avoid polluting access logs
            else:
                # If it looks like it might be an error (has error keywords), classify as error
                if error_keywords.search(l):
                    error_lines.append(l)
                # If it's unclear, skip it (don't add to access logs to avoid polluting)
                # This prevents docker-entrypoint.sh logs from appearing in access logs
        
        return ("\n".join(access_lines), "\n".join(error_lines))

    # ---------------------------
    # Core collection per host
    # ---------------------------
    def collect_for_host(self, host_info, host_connector):
        logger.info(f"Nginx adapter: collect_for_host called for host={host_info.get('name')}, instances_cfg={self.instances_cfg}")
        findings = {
            "type": "nginx",
            "discovered": [],
            "instances": [],
            "collection_mode": "historical" if self._is_historical_collection() else "current"
        }

        if self._is_historical_collection():
            since, until = self._get_time_window()
            findings["time_window"] = {
                "since": since.isoformat(),
                "until": until.isoformat(),
                "issue_time": self.issue_time.isoformat() if self.issue_time else None
            }

        # Instances from YAML or a single synthesized instance per host
        instances_cfg = self.instances_cfg or []
        logger.info(f"Nginx adapter: instances_cfg from self.instances_cfg: {instances_cfg}")
        if not instances_cfg:
            fallback_name = host_info.get("name") or host_info.get("address")
            logger.warning(f"Nginx adapter: No instances_cfg found, using fallback name: {fallback_name}")
            instances_cfg = [{"name": fallback_name}]

        # Build a local map of container name -> id if connector supports it
        # Make this non-blocking - if it hangs, we'll skip it and resolve container_id directly
        docker_name_to_id = {}
        if hasattr(host_connector, "list_containers"):
            try:
                # Try to list containers, but don't let it block forever
                # DockerHostConnector already has timeout protection, but we add extra safety
                containers_result = host_connector.list_containers()
                logger.info(f"Nginx adapter: list_containers returned type: {type(containers_result).__name__}")
                
                # Handle SSHHostConnector format: dict with "containers" key
                if isinstance(containers_result, dict):
                    logger.info(f"Nginx adapter: list_containers dict keys: {list(containers_result.keys())}")
                    logger.info(f"Nginx adapter: list_containers ok={containers_result.get('ok')}, containers count={len(containers_result.get('containers', []))}")
                    if containers_result.get("ok") and containers_result.get("containers"):
                        containers_list = containers_result["containers"]
                        logger.info(f"Nginx adapter: Processing {len(containers_list)} containers from SSHHostConnector")
                        for idx, c_dict in enumerate(containers_list):
                            logger.debug(f"Nginx adapter: Container {idx}: keys={list(c_dict.keys()) if isinstance(c_dict, dict) else 'not a dict'}")
                            # SSHHostConnector returns dicts with lowercase keys: "id", "names" (plural, comma-separated)
                            container_id = c_dict.get("id", "")
                            container_names = c_dict.get("names", "") or c_dict.get("name", "")
                            logger.debug(f"Nginx adapter: Container {idx}: id={container_id}, names={container_names}")
                            # Handle comma-separated names and remove leading slashes
                            if container_names:
                                for name in str(container_names).split(","):
                                    name = name.strip().lstrip("/")  # Remove leading slash
                                    if name and container_id:
                                        docker_name_to_id[name] = container_id
                                        logger.info(f"Nginx adapter: Mapped container '{name}' -> {container_id[:12] if container_id else 'None'}")
                            # Also map id->id
                            if container_id:
                                docker_name_to_id[container_id] = container_id
                    elif not containers_result.get("ok"):
                        error = containers_result.get("error", "Unknown error")
                        logger.warning(f"Nginx adapter: list_containers returned ok=False: {error}")
                    elif not containers_result.get("ok"):
                        error = containers_result.get("error", "Unknown error")
                        logger.warning(f"Nginx adapter: list_containers returned ok=False: {error}")
                
                # Handle DockerHostConnector format: List[Container] with .name and .id
                elif isinstance(containers_result, list):
                    logger.debug(f"Nginx adapter: Processing {len(containers_result)} containers from DockerHostConnector")
                    for c in containers_result:
                        try:
                            name_attr = getattr(c, "name", None) or getattr(c, "names", None)
                            cid = getattr(c, "id", None) or getattr(c, "short_id", None)
                            # name_attr could be a list or string
                            names_list = []
                            if isinstance(name_attr, (list, tuple)):
                                names_list.extend(name_attr)
                            elif name_attr:
                                names_list.append(name_attr)
                            for nm in names_list:
                                if nm and cid:
                                    clean_name = str(nm).lstrip("/")
                                    docker_name_to_id[clean_name] = str(cid)
                                    if str(nm) != clean_name:
                                        docker_name_to_id[str(nm)] = str(cid)
                            # ensure we at least map id->id
                            if cid:
                                docker_name_to_id[str(cid)] = str(cid)
                        except Exception:
                            # Skip this container if we can't process it
                            continue
                else:
                    logger.warning(f"Nginx adapter: list_containers returned unexpected type: {type(containers_result)}")
                
                logger.debug(f"Nginx adapter: Container map built with {len(docker_name_to_id)} entries: {list(docker_name_to_id.keys())}")
            except Exception as e:
                logger.debug(f"Nginx adapter: list_containers failed: {e}")
                # Continue without container mapping - we'll try to resolve container_id directly later
                pass

        for inst in instances_cfg:
            name = inst.get("name") or inst.get("container") or "<unnamed>"
            container_name = inst.get("container")
            logger.info(f"Nginx adapter: Processing instance '{name}' from config: {inst}")
            
            # Robust container id resolution
            container_id = None
            if container_name and docker_name_to_id:
                # direct lookup
                container_id = docker_name_to_id.get(container_name)
                # try common variants if direct lookup failed
                if not container_id:
                    cand = (container_name or "").strip()
                    cand_no_slash = cand.lstrip("/")
                    container_id = docker_name_to_id.get(cand) or docker_name_to_id.get(cand_no_slash)
                    if not container_id:
                        # suffix or substring match (best-effort)
                        for cname, cid in docker_name_to_id.items():
                            if cname == cand or cname.endswith(cand) or cand in cname:
                                container_id = cid
                                break

            # If container_name is specified but container_id is None, provide helpful error
            if container_name and container_id is None:
                available_containers = list(docker_name_to_id.keys()) if docker_name_to_id else []
                error_msg = f"Container '{container_name}' specified in YAML but not found. Available containers: {', '.join(available_containers) if available_containers else 'none'}"
                logger.warning(f"Nginx adapter: {error_msg}")
                entry = {
                    "name": name,
                    "container": container_name,
                    "running": False,
                    "version": None,
                    "process": None,
                    "listen_ports": None,
                    "config_preview": None,
                    "raw_config": None,
                    "server_blocks": [],
                    "config_test": None,
                    "access_log_tail": None,
                    "access_log_analysis": None,
                    "error_log_tail": None,
                    "error_summary": None,
                    "errors": [error_msg],
                    "metrics": {},
                    "logs": {}
                }
                findings["discovered"].append(name)
                findings["instances"].append(entry)
                continue

            conf_path = None
            if inst.get("conf_paths"):
                arr = inst["conf_paths"]
                conf_path = arr[0] if isinstance(arr, (list, tuple)) else arr
            conf_path = conf_path or DEFAULT_CONF_PATH

            access_log = inst.get("access_log", DEFAULT_ACCESS_LOG)
            error_log = inst.get("error_log", DEFAULT_ERROR_LOG)
            nginx_port = inst.get("port", 80)  # Read port from YAML, default 80

            entry = {
                "name": name,
                "container": container_name,
                "running": False,
                "version": None,
                "process": None,
                "listen_ports": None,
                "config_preview": None,
                "raw_config": None,
                "server_blocks": [],
                "config_test": None,
                "access_log_tail": None,
                "access_log_analysis": None,
                "error_log_tail": None,
                "error_summary": None,
                "errors": [],
                "metrics": {},  # For orchestrator to detect data presence
                "logs": {}      # For orchestrator to detect data presence
            }

            logger.debug(f"Nginx adapter: ===== ABOUT TO START LIVENESS CHECK for instance '{name}' =====")
            logger.debug(f"Nginx adapter: Entry created: {entry.get('name')}, container: {entry.get('container')}")
            try:
                # ------------------------------------------------------------------
                # LIVENESS: Container-aware checks
                # ------------------------------------------------------------------
                logger.debug(f"Nginx adapter: ===== STARTING LIVENESS CHECK for instance '{name}' (container_id={container_id}) =====")
                logger.debug(f"Nginx adapter: host_connector type: {type(host_connector).__name__}")
                logger.debug(f"Nginx adapter: host_info: {host_info}")
                try:
                    # Detect if we're in a Docker container context
                    from connectors.host_connectors.docker_host_connector import DockerHostConnector
                    # Check if this is a Docker container (either DockerHostConnector or SSHHostConnector with container_id)
                    from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                    is_docker_container = container_id is not None and (
                        isinstance(host_connector, DockerHostConnector) or 
                        isinstance(host_connector, SSHHostConnector)
                    )
                    logger.info(f"Nginx adapter: is_docker_container={is_docker_container} (container_id={container_id}, connector_type={type(host_connector).__name__})")
                    
                    if is_docker_container:
                        # ============================================================
                        # DOCKER CONTAINER: Use container-aware checks
                        # ============================================================
                        logger.debug(f"Nginx adapter: Using Docker container-aware liveness checks for '{name}'")
                        
                        # 0. First, verify container is actually running (not just exists)
                        container_running = False
                        container_status = "unknown"
                        container_id_str = str(container_id) if container_id else ""
                        try:
                            # DockerHostConnector: use Docker SDK
                            if isinstance(host_connector, DockerHostConnector) and hasattr(host_connector, "client") and host_connector.client:
                                try:
                                    container_info = host_connector.client.containers.get(container_id_str)
                                    if container_info is None:
                                        raise RuntimeError(f"nginx container {container_id_str[:12]} not found")
                                    container_info.reload()
                                    if not hasattr(container_info, 'attrs') or not isinstance(container_info.attrs, dict):
                                        raise RuntimeError(f"nginx container {container_id_str[:12]} has invalid attrs")
                                    state = container_info.attrs.get('State', {})
                                    if not isinstance(state, dict):
                                        raise RuntimeError(f"nginx container {container_id_str[:12]} has invalid State")
                                    container_status = state.get('Status', 'unknown')
                                    container_running = container_status == "running"
                                    logger.debug(f"Nginx adapter: Container {container_id_str[:12]} status: {container_status}")
                                    if not container_running:
                                        logger.warning(f"Nginx adapter: Container {container_id_str[:12]} is not running (status: {container_status})")
                                except Exception as e:
                                    error_str = str(e)
                                    logger.debug(f"Nginx adapter: Failed to get container status: {e}")
                                    if "not found" in error_str.lower() or "no such container" in error_str.lower():
                                        raise RuntimeError(f"nginx container {container_id_str[:12]} not found: {error_str}")
                                    container_running = False
                            # SSHHostConnector or other: use docker inspect command
                            elif isinstance(host_connector, SSHHostConnector) and hasattr(host_connector, "exec_cmd"):
                                try:
                                    inspect_cmd = f"docker inspect --format '{{{{.State.Status}}}}' {container_id_str} 2>/dev/null || echo 'NOT_FOUND'"
                                    out = host_connector.exec_cmd(inspect_cmd)
                                    if isinstance(out, dict):
                                        status_output = out.get("stdout", "").strip()
                                    else:
                                        status_output = str(out).strip()
                                    
                                    if status_output == "NOT_FOUND" or not status_output:
                                        raise RuntimeError(f"nginx container {container_id_str[:12]} not found")
                                    
                                    container_status = status_output.lower()
                                    container_running = container_status == "running"
                                    logger.debug(f"Nginx adapter: Container {container_id_str[:12]} status: {container_status}")
                                    if not container_running:
                                        logger.warning(f"Nginx adapter: Container {container_id_str[:12]} is not running (status: {container_status})")
                                except RuntimeError:
                                    raise
                                except Exception as e:
                                    logger.debug(f"Nginx adapter: Container status check (docker inspect) failed: {e}")
                                    container_running = False
                            else:
                                raise RuntimeError(f"nginx Docker client or exec_cmd not available")
                        except RuntimeError as e:
                            # Re-raise RuntimeError as-is (these are our liveness failures)
                            raise
                        except Exception as e:
                            logger.debug(f"Nginx adapter: Container status check failed: {e}")
                            container_running = False
                        
                        # Also verify we can actually exec into the container (double-check)
                        if container_running:
                            try:
                                test_exec = self._exec(host_connector, container_id_str, "echo test", timeout=2)
                                if test_exec.startswith("[container exec error") or test_exec.startswith("[host exec error"):
                                    logger.warning(f"Nginx adapter: Container {container_id_str[:12]} appears running but exec failed: {test_exec[:100]}")
                                    container_running = False
                            except Exception as e:
                                logger.debug(f"Nginx adapter: Exec test failed: {e}")
                                container_running = False
                        
                        if not container_running:
                            status_msg = f"status: {container_status}" if container_status != "unknown" else "container not running"
                            raise RuntimeError(f"nginx container {container_id_str[:12] if container_id_str else 'unknown'} is not running ({status_msg})")
                        
                        # 1. Check port accessibility from Docker host (not inside container)
                        port_check_passed = False
                        if isinstance(host_connector, SSHHostConnector):
                            # For SSH-accessed containers, check port on remote host (use localhost on remote host)
                            try:
                                port_cmd = f"timeout 2 bash -c '</dev/tcp/127.0.0.1/{nginx_port}' 2>&1 || nc -z -w2 127.0.0.1 {nginx_port} 2>&1 || echo 'PORT_CHECK_FAILED'"
                                port_out = host_connector.exec_cmd(port_cmd)
                                if isinstance(port_out, dict):
                                    port_str = port_out.get("stdout", "").strip()
                                else:
                                    port_str = str(port_out).strip()
                                
                                if "PORT_CHECK_FAILED" not in port_str and port_str:
                                    port_check_passed = True
                                    logger.debug(f"Nginx adapter: Port {nginx_port} check passed from remote host")
                                else:
                                    logger.debug(f"Nginx adapter: Port {nginx_port} not accessible from remote host: {port_str[:100]}")
                            except Exception as e:
                                logger.debug(f"Nginx adapter: Remote host port check failed: {e}")
                        else:
                            # DockerHostConnector: check port on local host
                            try:
                                import socket
                                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                sock.settimeout(2)
                                connect_result = sock.connect_ex(('127.0.0.1', nginx_port))
                                sock.close()
                                if connect_result == 0:
                                    port_check_passed = True
                                    logger.debug(f"Nginx adapter: Port {nginx_port} check passed from host (socket)")
                                else:
                                    logger.debug(f"Nginx adapter: Port {nginx_port} not accessible from host (socket result: {connect_result})")
                            except Exception as e:
                                logger.debug(f"Nginx adapter: Host port check (socket) failed: {e}")
                        
                        # 2. Optionally check /proc/1/status to verify main process is nginx
                        process_check_passed = False
                        try:
                            proc_status_cmd = "cat /proc/1/status 2>/dev/null | grep -i '^Name:' | awk '{print $2}' || echo 'PROC_CHECK_FAILED'"
                            proc_out = self._exec(host_connector, container_id_str, proc_status_cmd)
                            proc_str = str(proc_out).lower().strip()
                            if "proc_check_failed" not in proc_str and "nginx" in proc_str:
                                process_check_passed = True
                                logger.debug(f"Nginx adapter: Process check passed (/proc/1/status shows nginx)")
                        except Exception as e:
                            logger.debug(f"Nginx adapter: Process check (/proc/1/status) failed: {e}")
                        
                        # Liveness check: At least one of port check or process check should succeed
                        if not port_check_passed and not process_check_passed:
                            raise RuntimeError(f"nginx port {nginx_port} not reachable from host and process check failed")
                        
                    else:
                        # ============================================================
                        # NON-DOCKER (SSH/Local): Use standard checks
                        # ============================================================
                        logger.info(f"Nginx adapter: Using standard liveness checks for '{name}' (non-Docker)")
                        
                        # Check nginx binary presence
                        logger.info(f"Nginx adapter: Checking for nginx binary...")
                        which_out = self._exec(host_connector, container_id, "command -v nginx || which nginx || echo 'NOT_FOUND'", timeout=5)
                        which_lower = str(which_out).lower().strip()
                        logger.info(f"Nginx adapter: Binary check result: '{which_out}' (lower: '{which_lower}')")
                        # Raise if: no output, exec errors, or binary not reported as a path
                        if (not which_out or 
                            which_out.startswith("[host exec error") or 
                            which_out.startswith("[container exec error") or
                            which_out.startswith("[no exec method") or
                            "not found" in which_lower or
                            "not_found" in which_lower or
                            not which_lower.startswith("/")):
                            error_msg = f"nginx binary not found or not in PATH (command output: '{which_out}')"
                            logger.warning(f"Nginx adapter: {error_msg}")
                            raise RuntimeError(error_msg)
                        logger.info(f"Nginx adapter: Binary check PASSED, nginx found at: {which_out}")

                        # Check process - use [n]ginx pattern to avoid grep in output
                        ps_cmds = [
                            "ps -ef | grep '[n]ginx'",  # [n]ginx matches nginx but not grep nginx
                            "ps aux | grep '[n]ginx'",
                            "pgrep -a nginx 2>/dev/null || true"
                        ]
                        ps_out = ""
                        ps_check_failed_reasons = []
                        for ps_cmd in ps_cmds:
                            try:
                                ps_result = self._exec(host_connector, container_id, ps_cmd, timeout=5)
                                ps_result_str = str(ps_result) if ps_result else ""
                                # Check if result is valid (not an error and contains nginx)
                                if (ps_result_str and 
                                    ps_result_str.strip() and 
                                    not ps_result_str.startswith("[host exec error") and
                                    not ps_result_str.startswith("[container exec error") and
                                    not ps_result_str.startswith("[no exec method") and
                                    ("nginx" in ps_result_str.lower() or len(ps_result_str.strip()) > 0)):
                                    ps_out = ps_result_str
                                    logger.info(f"Nginx adapter: Process check PASSED with command: {ps_cmd}, output: {ps_result_str[:200]}")
                                    break
                                else:
                                    reason = f"command '{ps_cmd}' returned invalid result: {ps_result_str[:100]}"
                                    ps_check_failed_reasons.append(reason)
                                    logger.debug(f"Nginx adapter: Process check {reason}")
                            except RuntimeError as e:
                                reason = f"command '{ps_cmd}' raised RuntimeError: {e}"
                                ps_check_failed_reasons.append(reason)
                                logger.debug(f"Nginx adapter: Process check {reason}")
                                continue
                            except Exception as e:
                                reason = f"command '{ps_cmd}' failed with exception: {e}"
                                ps_check_failed_reasons.append(reason)
                                logger.debug(f"Nginx adapter: Process check {reason}")
                                continue

                        if not ps_out.strip():
                            # Fallback to port check (from config or default 80)
                            logger.info(f"Nginx adapter: Process check failed (reasons: {ps_check_failed_reasons}), trying port check on port {nginx_port}")
                            port_cmd = f"timeout 2 bash -c '</dev/tcp/127.0.0.1/{nginx_port}' 2>&1 || nc -z -w2 127.0.0.1 {nginx_port} 2>&1 || echo 'PORT_CHECK_FAILED'"
                            try:
                                port_out = self._exec(host_connector, container_id, port_cmd, timeout=5)
                                port_str = str(port_out).lower()
                                if "port_check_failed" in port_str or "refused" in port_str or "failed" in port_str or "connection refused" in port_str or "no route to host" in port_str:
                                    error_msg = f"nginx process/port {nginx_port} not reachable (process check failed: {', '.join(ps_check_failed_reasons[:2])}, port check failed: {port_str[:100]})"
                                    logger.warning(f"Nginx adapter: {error_msg}")
                                    raise RuntimeError(error_msg)
                                logger.info(f"Nginx adapter: Port check PASSED on port {nginx_port}, output: {port_str[:100]}")
                            except RuntimeError:
                                raise
                            except Exception as e:
                                error_msg = f"nginx process/port {nginx_port} not reachable (process check failed: {', '.join(ps_check_failed_reasons[:2])}, port check exception: {e})"
                                logger.warning(f"Nginx adapter: {error_msg}")
                                raise RuntimeError(error_msg)
                        else:
                            logger.info(f"Nginx adapter: Process check succeeded, nginx is running")
                    
                    # Set running flag and log success - this should always execute if we reach here
                    logger.debug(f"Nginx adapter: ===== REACHED LINE 903, about to set entry['running'] = True =====")
                    entry["running"] = True
                    logger.debug(f"Nginx adapter: ===== entry['running'] SET, about to populate metrics =====")
                    # Immediately populate metrics to indicate service is running (so orchestrator doesn't mark as missing)
                    entry["metrics"]["status"] = "running"
                    entry["metrics"]["liveness_check"] = "passed"
                    logger.debug(f"Nginx adapter: ===== Metrics populated: {entry['metrics']} =====")
                    logger.info(f"Nginx adapter: Liveness check PASSED, proceeding with collection for '{name}'")
                    logger.debug(f"Nginx adapter: ===== About to exit liveness try block =====")
                except Exception as le:
                    error_msg = f"liveness: {le}"
                    logger.warning(f"Nginx adapter: ===== LIVENESS CHECK FAILED for '{name}': {error_msg} =====")
                    logger.debug(f"Nginx adapter: Exception type: {type(le).__name__}, Exception: {le}")
                    import traceback
                    logger.error(f"Nginx adapter: Traceback:\n{traceback.format_exc()}")
                    entry["errors"].append(error_msg)
                    findings["discovered"].append(name)
                    findings["instances"].append(entry)
                    continue

                logger.info(f"Nginx adapter: ===== STARTING COLLECTION for nginx instance: '{name}' =====")
                # 1) version - must wrap with sh -c to capture stderr in container exec
                try:
                    logger.debug(f"Collecting version for {name}")
                    version_cmd = "nginx -v 2>&1"
                    v = self._exec(host_connector, container_id, version_cmd, timeout=5)
                    entry["version"] = (v or "").strip()
                except Exception as e:
                    entry["errors"].append(f"version: {e}")
                    logger.debug(f"Version collection failed for {name}: {e}")

                # 2) process listing
                try:
                    ps_cmd = "ps aux | grep [n]ginx || ps -ef | grep [n]ginx || true"
                    p = self._exec(host_connector, container_id, ps_cmd, timeout=5)
                    entry["process"] = (p or "").strip()
                    entry["running"] = bool(entry["process"])
                except Exception as e:
                    entry["errors"].append(f"process: {e}")

                # 3) listen ports - filter for nginx only
                try:
                    # Get nginx PIDs first to filter ports accurately
                    nginx_pids = []
                    if entry.get("process"):
                        # Extract PIDs from process output (format: "root 699 ... nginx: master process")
                        pid_matches = re.findall(r'\b(\d+)\b.*nginx', entry["process"], re.IGNORECASE)
                        nginx_pids = [pid for pid in pid_matches if pid]
                    
                    # Try multiple methods to get only nginx ports
                    # Method 1: Use lsof to get nginx ports directly (most reliable)
                    # Method 2: Use ss/netstat and filter for nginx process name
                    # Method 3: Use ss/netstat and filter by nginx PIDs
                    # Method 4: Fallback to ports 80/443 if process filtering fails
                    if nginx_pids:
                        pid_filter = "|".join(nginx_pids)
                        listen_cmd = (
                            f"lsof -i -P -n 2>/dev/null | grep -E 'nginx|{pid_filter}' | grep LISTEN || "
                            f"ss -tulnp 2>/dev/null | grep -E 'nginx|pid={pid_filter}' || "
                            f"netstat -tulnp 2>/dev/null | grep -E 'nginx|{pid_filter}' || "
                            f"ss -tuln 2>/dev/null | grep -E ':(80|443) ' || "
                            f"netstat -tuln 2>/dev/null | grep -E ':(80|443) ' || "
                            f"echo ''"
                        )
                    else:
                        listen_cmd = (
                            "lsof -i -P -n 2>/dev/null | grep -i nginx | grep LISTEN || "
                            "ss -tulnp 2>/dev/null | grep -i nginx || "
                            "netstat -tulnp 2>/dev/null | grep -i nginx || "
                            "ss -tuln 2>/dev/null | grep -E ':(80|443) ' || "
                            "netstat -tuln 2>/dev/null | grep -E ':(80|443) ' || "
                            "echo ''"
                        )
                    
                    lp = self._exec(host_connector, container_id, listen_cmd, timeout=5)
                    lp_filtered = (lp or "").strip()
                    
                    # Clean up the output - remove empty lines and ensure it's nginx-related
                    if lp_filtered:
                        lines = [line for line in lp_filtered.split('\n') if line.strip()]
                        # Additional filter to ensure nginx relevance
                        filtered_lines = []
                        for line in lines:
                            line_lower = line.lower()
                            # Include if it mentions nginx, or is port 80/443, or matches nginx PID
                            if ('nginx' in line_lower or 
                                ':80 ' in line or ':443 ' in line or 
                                any(f'pid={pid}' in line or f' {pid}/' in line for pid in nginx_pids)):
                                filtered_lines.append(line)
                        
                        if filtered_lines:
                            entry["listen_ports"] = "\n".join(filtered_lines)
                        else:
                            entry["listen_ports"] = ""
                    else:
                        entry["listen_ports"] = ""
                except Exception as e:
                    entry["errors"].append(f"listen_ports: {e}")
                    entry["listen_ports"] = None

                # 4) config preview (with timeout protection via _read_file's internal timeout)
                try:
                    # Try to read config - _read_file already has timeout protection via _exec (timeout=5)
                    # If it takes too long, it will timeout and we'll catch the exception
                    # For non-Docker hosts, use exec_cmd directly with explicit timeout
                    if not container_id:
                        # For SSH hosts, use exec_cmd with explicit timeout to prevent hangs
                        cfg_cmd = f"timeout 5 cat {shlex.quote(conf_path)} 2>/dev/null || cat {shlex.quote(conf_path)} 2>/dev/null || echo ''"
                        cfg_out = self._exec(host_connector, container_id, cfg_cmd, timeout=6)
                        cfg_txt = cfg_out if cfg_out and not cfg_out.startswith("[host exec error") else None
                    else:
                        cfg_txt = self._read_file(host_connector, container_id, conf_path)
                    
                    entry["raw_config"] = cfg_txt if cfg_txt is not None else ""
                    if cfg_txt is None:
                        cfg_preview = "[missing]"
                    else:
                        cfg_preview = "\n".join((cfg_txt or "").splitlines()[:50])
                    entry["config_preview"] = cfg_preview
                    
                    # Parse server blocks
                    try:
                        server_blocks = self._parse_server_blocks(entry.get("raw_config") or entry.get("config_preview") or "")
                        entry["server_blocks"] = server_blocks
                    except Exception as e:
                        entry["errors"].append(f"parse_server_blocks: {e}")
                except Exception as e:
                    # Config read failed or timed out - skip it gracefully
                    logger.debug(f"Config read failed for {name}: {e}")
                    entry["errors"].append(f"config read: {e}")
                    entry["config_preview"] = "[config read skipped - timeout or error]"
                    entry["raw_config"] = ""

                # 5) access log tail - with timeout protection
                combined_logs_for_analysis = None  # Store original combined logs for analysis
                try:
                    # Debug: Check collection mode
                    is_historical = self._is_historical_collection()
                    logger.debug(f"Collecting access logs for {name} - Historical mode: {is_historical}, Issue time: {self.issue_time}")
                    
                    # For non-Docker hosts (runtime: host), skip symlink check and use file-based collection
                    # Symlink checks are only relevant for Docker containers
                    # For SSHHostConnector, always assume logs go to stdout/stderr (Docker best practice)
                    from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                    is_symlinked = False
                    if container_id:
                        if isinstance(host_connector, SSHHostConnector):
                            # For SSHHostConnector, assume logs are symlinked (Docker containers typically log to stdout/stderr)
                            is_symlinked = True
                            logger.debug(f"Access log: Assuming symlinked for SSHHostConnector (Docker best practice)")
                        else:
                            try:
                                # Only check symlink for Docker containers, with timeout
                                is_symlinked = self._is_path_symlink_to_stdout(host_connector, container_id, access_log)
                                logger.debug(f"Access log symlink check for {name}: is_symlinked={is_symlinked}")
                            except Exception as e:
                                logger.debug(f"Symlink check failed: {e}")
                                pass
                    
                    if is_historical:
                        since, until = self._get_time_window()
                        logger.debug(f"Historical collection time window: {since} to {until}")
                    
                    if is_symlinked:
                        # Logs are symlinked to stdout/stderr - USE docker logs (it's the only way to get them)
                        logger.info(f"✓ Access log is symlinked to stdout/stderr for {name}, using docker logs")
                        from connectors.host_connectors.docker_host_connector import DockerHostConnector
                        from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                        
                        if container_id and isinstance(host_connector, DockerHostConnector) and hasattr(host_connector, "get_container_logs"):
                            try:
                                # Check if historical collection - use time-filtered logs if so
                                if self._is_historical_collection():
                                    since, until = self._get_time_window()
                                    logger.info(f"✓ Historical collection detected for {name} - time window: {since} to {until}")
                                    # Check if host_connector supports time-filtered logs (must be DockerHostConnector)
                                    from connectors.host_connectors.docker_host_connector import DockerHostConnector
                                    logger.debug(f"Host connector type: {type(host_connector)}, is DockerHostConnector: {isinstance(host_connector, DockerHostConnector)}")
                                    if isinstance(host_connector, DockerHostConnector):
                                        # Strategy: Get a large tail of logs, then filter by embedded timestamps
                                        # Docker's time filtering may not work if container was restarted or logs rotated
                                        # So we get more logs and filter by the actual timestamps in the log content
                                        logger.info(f"✓ Getting ALL logs (no tail limit) for {name} for historical filtering by embedded timestamps")
                                        # For historical collection, get ALL logs (no tail limit) since we'll filter by timestamp
                                        # This ensures we capture logs from the requested time window even if they're not in the recent tail
                                        combined_logs = host_connector.get_container_logs(container_id, tail=None)
                                        logger.debug(f"Raw get_container_logs return type: {type(combined_logs)}, length: {len(str(combined_logs)) if combined_logs else 0}")
                                        
                                        if isinstance(combined_logs, dict):
                                            combined_logs = combined_logs.get("stdout", combined_logs.get("output", "")) or ""
                                        combined_logs = str(combined_logs or "")
                                        
                                        logger.info(f"✓ Retrieved {len(combined_logs)} characters from Docker logs for {name}")
                                        
                                        # Check if we got an error message
                                        if combined_logs.startswith("[docker"):
                                            logger.warning(f"✗ Docker logs returned error: {combined_logs}")
                                            combined_logs = ""
                                        else:
                                            # ALWAYS post-process: Filter logs by embedded timestamps
                                            # This is the reliable way to filter by the actual log timestamps
                                            from connectors.historical.timestamp_parsers import TimestampParserRegistry
                                            from connectors.historical.log_time_filter import filter_logs_by_time_window
                                            
                                            # parser = TimestampParserRegistry.get("nginx_access")
                                            parser = (
                                                TimestampParserRegistry.get("nginx") or
                                                TimestampParserRegistry.get("nginx_access") or
                                                TimestampParserRegistry.get("nginx_access_log") or
                                                TimestampParserRegistry.get("common_log")
                                            )
                                            logger.info(f"✓ Parser found: {parser is not None}, parser name: {parser.__name__ if parser and hasattr(parser, '__name__') else 'None'}")
                                            
                                            if parser and combined_logs:
                                                log_lines = combined_logs.splitlines()
                                                logger.info(f"✓ Got {len(log_lines)} log lines from Docker, filtering by time window {since} to {until}")
                                                filtered_lines = filter_logs_by_time_window(log_lines, since, until, parser, max_lines=2000)
                                                combined_logs = "\n".join(filtered_lines)
                                                logger.info(f"✓ Filtered to {len(filtered_lines)} log lines within time window")
                                                if len(filtered_lines) == 0 and len(log_lines) > 0:
                                                    # Got logs from Docker but none matched time window - log sample with timestamps
                                                    sample_lines = log_lines[:5]
                                                    logger.warning(f"✗ No logs matched time window {since} to {until}. Sample lines (first 5):")
                                                    for line in sample_lines:
                                                        ts = parser(line)
                                                        logger.warning(f"  Line timestamp: {ts}, Time window: {since} to {until}, In window: {since <= ts <= until if ts else 'N/A'}, Line: {line[:100]}")
                                            elif not combined_logs:
                                                logger.warning(f"✗ No logs returned from Docker - container may not have logs or logs may be rotated")
                                            else:
                                                logger.warning(f"✗ Nginx access log parser not found - cannot filter by timestamps")
                                    else:
                                        logger.warning(f"✗ Host connector is not DockerHostConnector (type: {type(host_connector).__name__}), cannot get historical logs for symlinked access log")
                                        combined_logs = ""
                                else:
                                    # CURRENT collection - use regular docker logs
                                    logger.debug(f"Fetching docker logs with tail=2000 for {name} (current, symlinked logs)")
                                    combined_logs = host_connector.get_container_logs(container_id, tail=2000)
                                    if isinstance(combined_logs, dict):
                                        combined_logs = combined_logs.get("stdout", combined_logs.get("output", "")) or ""
                                    combined_logs = str(combined_logs or "")
                                
                                combined_logs_for_analysis = combined_logs  # Store for analysis
                                # Split combined docker logs: 2XX/3XX -> access, 4XX/5XX -> errors
                                access_text, error_text = self._split_combined_logs(combined_logs)
                                access_lines = access_text.splitlines()[-200:] if access_text else []
                                error_lines = error_text.splitlines()[-200:] if error_text else []
                                entry["access_log_tail"] = "\n".join(access_lines) if access_lines else "[no access logs found in docker logs]"
                                # Also populate error_log_tail with 4XX/5XX errors from access logs
                                if error_lines:
                                    entry["error_log_tail"] = "\n".join(error_lines)
                                else:
                                    # No 4XX/5XX errors found, but ensure error_log_tail is set to empty string, not None
                                    if "error_log_tail" not in entry or entry.get("error_log_tail") is None:
                                        entry["error_log_tail"] = "[no HTTP errors in access logs]"
                            except Exception as e:
                                logger.warning(f"Docker logs failed for symlinked logs: {e}", exc_info=True)
                                entry["access_log_tail"] = "[docker logs unavailable]"
                                entry["errors"].append(f"docker logs failed: {e}")
                        elif container_id and isinstance(host_connector, SSHHostConnector) and hasattr(host_connector, "exec_cmd"):
                            # For SSH-accessed containers, use docker logs command
                            try:
                                logger.info(f"Collecting Nginx logs from Docker (via SSH) for container {container_id}")
                                docker_logs_cmd = f"timeout 5 docker logs --tail 2000 {container_id} 2>&1"
                                combined_logs = self._exec(host_connector, None, docker_logs_cmd, timeout=5)
                                logger.info(f"Nginx adapter: docker logs command returned: type={type(combined_logs).__name__}, length={len(str(combined_logs)) if combined_logs else 0}")
                                logger.info(f"Nginx adapter: docker logs command output (first 500 chars): {str(combined_logs)[:500] if combined_logs else 'None'}")
                                
                                if combined_logs and not combined_logs.startswith("[host exec error") and not combined_logs.startswith("[container exec error"):
                                    combined_logs = str(combined_logs).strip()
                                    logger.info(f"Nginx adapter: Processing {len(combined_logs)} chars of logs, {len(combined_logs.splitlines())} lines")
                                    
                                    if not combined_logs:
                                        logger.warning(f"Nginx adapter: docker logs returned empty output - container may have no logs yet")
                                        entry["access_log_tail"] = "[no logs in container yet - container may be new or no requests received]"
                                        entry["error_log_tail"] = "[no logs in container yet]"
                                    else:
                                        # Split combined docker logs: 2XX/3XX -> access, 4XX/5XX -> errors
                                        access_text, error_text = self._split_combined_logs(combined_logs)
                                        logger.info(f"Nginx adapter: Split logs - access: {len(access_text)} chars ({len(access_text.splitlines())} lines), error: {len(error_text)} chars ({len(error_text.splitlines())} lines)")
                                        access_lines = access_text.splitlines()[-200:] if access_text else []
                                        error_lines = error_text.splitlines()[-200:] if error_text else []
                                        
                                        # If no access logs found but we have combined_logs, check if it's just initialization logs
                                        if not access_lines and combined_logs:
                                            # Check if logs are only Docker entrypoint initialization logs
                                            init_only = True
                                            for line in combined_logs.splitlines():
                                                line_lower = line.lower().strip()
                                                # If we find any line that's not an entrypoint log, it's not just initialization
                                                if line_lower and not any(pattern in line_lower for pattern in [
                                                    '/docker-entrypoint.sh',
                                                    'docker-entrypoint.d',
                                                    'listen-on-ipv6',
                                                    'local-resolvers',
                                                    'envsubst-on-templates',
                                                    'tune-worker-processes',
                                                    'configuration complete',
                                                    'ready for start up'
                                                ]):
                                                    init_only = False
                                                    break
                                            
                                            if init_only:
                                                logger.info(f"Nginx adapter: Only Docker entrypoint initialization logs found (no HTTP requests yet)")
                                                entry["access_log_tail"] = "[no HTTP requests logged yet - container initialized but no traffic received]"
                                            else:
                                                logger.warning(f"Nginx adapter: No access log patterns found in docker logs. Raw logs (first 500 chars): {combined_logs[:500]}")
                                                # Store raw logs as access logs if no pattern match (might be different format)
                                                entry["access_log_tail"] = "\n".join(combined_logs.splitlines()[-200:])
                                        else:
                                            entry["access_log_tail"] = "\n".join(access_lines) if access_lines else "[no access logs found in docker logs]"
                                        
                                        if error_lines:
                                            entry["error_log_tail"] = "\n".join(error_lines)
                                        else:
                                            entry["error_log_tail"] = "[no HTTP errors in access logs]"
                                        logger.info(f"✅ Successfully collected Nginx logs from Docker via SSH ({len(combined_logs)} chars, {len(combined_logs.splitlines())} lines)")
                                else:
                                    logger.warning(f"Nginx adapter: docker logs command failed or returned error: {combined_logs[:200] if combined_logs else 'None'}")
                                    entry["access_log_tail"] = "[docker logs unavailable via SSH]"
                                    entry["error_log_tail"] = "[docker logs unavailable via SSH]"
                            except Exception as e:
                                logger.warning(f"Nginx adapter: docker logs via SSH failed: {e}", exc_info=True)
                                entry["access_log_tail"] = "[docker logs unavailable via SSH]"
                                entry["error_log_tail"] = "[docker logs unavailable via SSH]"
                                entry["errors"].append(f"docker logs via SSH failed: {e}")
                        else:
                            entry["access_log_tail"] = "[docker logs not available - cannot read symlinked logs]"
                            entry["errors"].append("docker logs not available - cannot read symlinked logs")
                    elif container_id and isinstance(host_connector, SSHHostConnector) and hasattr(host_connector, "exec_cmd"):
                        # For SSHHostConnector, always use docker logs (even if symlink detection failed)
                        # Docker containers typically log to stdout/stderr anyway
                        try:
                            logger.info(f"Collecting Nginx logs from Docker (via SSH, fallback) for container {container_id}")
                            docker_logs_cmd = f"timeout 5 docker logs --tail 2000 {container_id} 2>&1"
                            combined_logs = self._exec(host_connector, None, docker_logs_cmd, timeout=5)
                            logger.debug(f"Nginx adapter: docker logs command output (first 200 chars): {str(combined_logs)[:200] if combined_logs else 'None'}")
                            
                            if combined_logs and not combined_logs.startswith("[host exec error") and not combined_logs.startswith("[container exec error"):
                                combined_logs = str(combined_logs)
                                # Split combined docker logs: 2XX/3XX -> access, 4XX/5XX -> errors
                                access_text, error_text = self._split_combined_logs(combined_logs)
                                access_lines = access_text.splitlines()[-200:] if access_text else []
                                error_lines = error_text.splitlines()[-200:] if error_text else []
                                entry["access_log_tail"] = "\n".join(access_lines) if access_lines else "[no access logs found in docker logs]"
                                if error_lines:
                                    entry["error_log_tail"] = "\n".join(error_lines)
                                else:
                                    entry["error_log_tail"] = "[no HTTP errors in access logs]"
                                logger.info(f"✅ Successfully collected Nginx logs from Docker via SSH (fallback) ({len(combined_logs)} chars, {len(combined_logs.splitlines())} lines)")
                            else:
                                logger.warning(f"Nginx adapter: docker logs command failed or returned error: {combined_logs[:100] if combined_logs else 'None'}")
                                entry["access_log_tail"] = "[docker logs unavailable via SSH]"
                                entry["error_log_tail"] = "[docker logs unavailable via SSH]"
                        except Exception as e:
                            logger.warning(f"Nginx adapter: docker logs via SSH (fallback) failed: {e}", exc_info=True)
                            entry["access_log_tail"] = "[docker logs unavailable via SSH]"
                            entry["error_log_tail"] = "[docker logs unavailable via SSH]"
                            entry["errors"].append(f"docker logs via SSH (fallback) failed: {e}")
                    elif container_id and hasattr(host_connector, "get_container_logs"):
                        # Try docker logs for non-symlinked logs (fallback option)
                        try:
                            # Check if historical collection - use time-filtered logs if so
                            if self._is_historical_collection():
                                since, until = self._get_time_window()
                                logger.debug(f"Fetching time-filtered docker logs for {name} (historical, non-symlinked logs): {since} to {until}")
                                from connectors.host_connectors.docker_host_connector import DockerHostConnector
                                if isinstance(host_connector, DockerHostConnector):
                                    # Strategy: Get ALL logs (no tail limit) for historical collection, then filter by embedded timestamps
                                    logger.info(f"✓ Getting ALL logs (no tail limit) for {name} for historical filtering by embedded timestamps")
                                    combined_logs = host_connector.get_container_logs(container_id, tail=None)
                                    if isinstance(combined_logs, dict):
                                        combined_logs = combined_logs.get("stdout", combined_logs.get("output", "")) or ""
                                    combined_logs = str(combined_logs or "")
                                    
                                    # Post-process: Filter logs by embedded timestamps
                                    if not combined_logs.startswith("[docker"):
                                        from connectors.historical.timestamp_parsers import TimestampParserRegistry
                                        from connectors.historical.log_time_filter import filter_logs_by_time_window
                                        # parser = TimestampParserRegistry.get("nginx_access")
                                        parser = (
                                            TimestampParserRegistry.get("nginx") or
                                            TimestampParserRegistry.get("nginx_access") or
                                            TimestampParserRegistry.get("nginx_access_log") or
                                            TimestampParserRegistry.get("common_log")
                                        )
                                        if parser and combined_logs:
                                            log_lines = combined_logs.splitlines()
                                            logger.info(f"Got {len(log_lines)} log lines from Docker, filtering by time window {since} to {until}")
                                            filtered_lines = filter_logs_by_time_window(log_lines, since, until, parser, max_lines=2000)
                                            combined_logs = "\n".join(filtered_lines)
                                            logger.info(f"Filtered to {len(filtered_lines)} log lines within time window")
                                        else:
                                            logger.warning("No parser available or no logs to filter")
                                    else:
                                        logger.warning(f"Docker logs returned error: {combined_logs}")
                                        combined_logs = ""
                                else:
                                    logger.warning(f"Host connector is not DockerHostConnector, cannot get historical logs")
                                    combined_logs = ""
                            else:
                                # CURRENT collection - use regular docker logs
                                logger.debug(f"Trying docker logs with tail=50 for {name} (current, non-symlinked logs)")
                                combined_logs = host_connector.get_container_logs(container_id, tail=50)
                                if isinstance(combined_logs, dict):
                                    combined_logs = combined_logs.get("stdout", combined_logs.get("output", "")) or ""
                                combined_logs = str(combined_logs or "")
                            
                            combined_logs_for_analysis = combined_logs  # Store for analysis
                            # Split combined docker logs: 2XX/3XX -> access, 4XX/5XX -> errors
                            access_text, error_text = self._split_combined_logs(combined_logs)
                            access_lines = access_text.splitlines()[-200:] if access_text else []
                            error_lines = error_text.splitlines()[-200:] if error_text else []
                            entry["access_log_tail"] = "\n".join(access_lines) if access_lines else "[no access logs]"
                            # Also populate error_log_tail with 4XX/5XX errors from access logs
                            if error_lines:
                                entry["error_log_tail"] = "\n".join(error_lines)
                            else:
                                # No 4XX/5XX errors found, but ensure error_log_tail is set
                                if "error_log_tail" not in entry or entry.get("error_log_tail") is None:
                                    entry["error_log_tail"] = "[no HTTP errors in access logs]"
                        except Exception as e:
                            logger.debug(f"Docker logs failed: {e}")
                            entry["access_log_tail"] = "[docker logs unavailable]"
                    else:
                        # Use tail command for regular files (not symlinked)
                        # Check if historical collection - use historical log collection mixin if so
                        if self._is_historical_collection():
                            since, until = self._get_time_window()
                            logger.debug(f"Collecting historical file-based logs for {name}: {since} to {until}")
                            try:
                                # Use historical log collection mixin for file-based logs
                                # Note: This can be slow for large log files, but we rely on individual command timeouts
                                access_log_content = self._collect_historical_logs(
                                    host_connector=host_connector,
                                    container_id=container_id,
                                    log_paths=[access_log],
                                    parser_name="nginx_access",
                                    since=since,
                                    until=until,
                                    max_lines=2000
                                )
                                combined_logs_for_analysis = access_log_content
                                # Split by status code: 2XX/3XX -> access, 4XX/5XX -> errors
                                if access_log_content:
                                    access_text, error_text = self._split_combined_logs(access_log_content)
                                    access_lines = access_text.splitlines()[-200:] if access_text else []
                                    error_lines = error_text.splitlines()[-200:] if error_text else []
                                    entry["access_log_tail"] = "\n".join(access_lines) if access_lines else "[no access logs in time window]"
                                    if error_lines:
                                        entry["error_log_tail"] = "\n".join(error_lines)
                                    else:
                                        if "error_log_tail" not in entry or entry.get("error_log_tail") is None:
                                            entry["error_log_tail"] = "[no HTTP errors in access logs]"
                                else:
                                    entry["access_log_tail"] = "[no access logs in time window]"
                                    if "error_log_tail" not in entry or entry.get("error_log_tail") is None:
                                        entry["error_log_tail"] = "[no HTTP errors in access logs]"
                            except Exception as e:
                                logger.warning(f"Historical file-based log collection failed: {e}, falling back to tail")
                                # Fallback to tail
                                tail_cmd = "tail -n 200"
                                al_out = self._exec(host_connector, container_id, f"{tail_cmd} {shlex.quote(access_log)} 2>/dev/null || true", timeout=3)
                                access_log_content = (al_out or "").strip()
                                combined_logs_for_analysis = access_log_content if access_log_content and not access_log_content.startswith("[") else None
                                if access_log_content and re.search(r'HTTP/\d\.\d"\s+\d{3}', access_log_content):
                                    access_text, error_text = self._split_combined_logs(access_log_content)
                                    access_lines = access_text.splitlines()[-200:] if access_text else []
                                    error_lines = error_text.splitlines()[-200:] if error_text else []
                                    entry["access_log_tail"] = "\n".join(access_lines) if access_lines else "[no access logs]"
                                    if error_lines:
                                        entry["error_log_tail"] = "\n".join(error_lines)
                                else:
                                    entry["access_log_tail"] = access_log_content or "[no access logs]"
                        else:
                            # CURRENT collection - use tail command
                            # Use timeout wrapper and handle permission errors gracefully
                            tail_cmd = "timeout 3 tail -n 200"
                            al_out = self._exec(host_connector, container_id, f"{tail_cmd} {shlex.quote(access_log)} 2>/dev/null || echo ''", timeout=4)
                            access_log_content = (al_out or "").strip()
                            # Even for file-based logs, split by status code: 2XX/3XX -> access, 4XX/5XX -> errors
                            placeholder_messages = ["[no access logs]", "[docker logs unavailable]", "[collection failed]"]
                            if access_log_content and not access_log_content.startswith("[") and access_log_content not in placeholder_messages:
                                # Check if it looks like access log format (has HTTP status codes)
                                if re.search(r'HTTP/\d\.\d"\s+\d{3}', access_log_content):
                                    access_text, error_text = self._split_combined_logs(access_log_content)
                                    access_lines = access_text.splitlines()[-200:] if access_text else []
                                    error_lines = error_text.splitlines()[-200:] if error_text else []
                                    entry["access_log_tail"] = "\n".join(access_lines) if access_lines else "[no access logs]"
                                    # Also populate error_log_tail with 4XX/5XX errors
                                    if error_lines:
                                        entry["error_log_tail"] = "\n".join(error_lines)
                                    else:
                                        # No 4XX/5XX errors found, but ensure error_log_tail is set
                                        if "error_log_tail" not in entry or entry.get("error_log_tail") is None:
                                            entry["error_log_tail"] = "[no HTTP errors in access logs]"
                                    combined_logs_for_analysis = access_log_content  # Store for analysis
                                else:
                                    # Doesn't look like access log format - use as-is
                                    entry["access_log_tail"] = access_log_content
                                    combined_logs_for_analysis = access_log_content
                            else:
                                entry["access_log_tail"] = access_log_content
                                combined_logs_for_analysis = access_log_content if access_log_content and not access_log_content.startswith("[") else None
                except Exception as e:
                    logger.debug(f"Access log collection failed: {e}")
                    entry["errors"].append(f"access_log: {e}")
                    entry["access_log_tail"] = "[collection failed]"
                
                # POST-PROCESSING: Ensure access_log_tail is properly split by status code
                # This is a safety net in case split didn't happen during collection
                # Check if access_log_tail contains 4XX/5XX errors that should be in error_log_tail
                current_access_tail = entry.get("access_log_tail", "")
                placeholder_messages = ["[no access logs]", "[no access logs found in docker logs]", "[docker logs unavailable]", "[docker logs not available - cannot read symlinked logs]", "[collection failed]"]
                
                if current_access_tail and not current_access_tail.startswith("[") and current_access_tail not in placeholder_messages:
                    # Check if it contains HTTP status codes
                    if re.search(r'HTTP/\d\.\d"\s+\d{3}', current_access_tail):
                        # Check if it contains 4XX/5XX errors (should be split)
                        if re.search(r'HTTP/\d\.\d"\s+[45]\d{2}', current_access_tail):
                            # Contains 4XX/5XX errors - need to split!
                            logger.debug(f"Post-processing: Splitting access_log_tail that contains 4XX/5XX errors")
                            access_text, error_text = self._split_combined_logs(current_access_tail)
                            access_lines = access_text.splitlines()[-200:] if access_text else []
                            error_lines = error_text.splitlines()[-200:] if error_text else []
                            entry["access_log_tail"] = "\n".join(access_lines) if access_lines else "[no access logs]"
                            # Set error_log_tail with 4XX/5XX errors (append if already exists)
                            if error_lines:
                                existing_errors = entry.get("error_log_tail", "")
                                if existing_errors and not existing_errors.startswith("["):
                                    entry["error_log_tail"] = existing_errors + "\n" + "\n".join(error_lines)
                                else:
                                    entry["error_log_tail"] = "\n".join(error_lines)
                            # Use original content for analysis
                            combined_logs_for_analysis = current_access_tail
                
                # Analyze access log for HTTP status codes (4XX, 5XX errors)
                # Always run analysis, even if logs are empty (to ensure field is always set)
                # This is OUTSIDE the collection try block so it always runs
                # Use original combined logs if available (before splitting), otherwise use access_log_tail
                analysis_content = combined_logs_for_analysis if combined_logs_for_analysis else entry.get("access_log_tail", "")
                placeholder_messages = ["[no access logs]", "[no access logs found in docker logs]", "[docker logs unavailable]", "[docker logs not available - cannot read symlinked logs]", "[collection failed]"]
                
                try:
                    # Check if we have actual log content (not placeholder messages)
                    if analysis_content and not analysis_content.startswith("[") and analysis_content not in placeholder_messages:
                        # We have real log content - analyze it (use combined logs to get all 4XX/5XX errors)
                        analysis_result = self._analyze_access_log_status_codes(analysis_content)
                        entry["access_log_analysis"] = analysis_result
                        
                        # Add 4XX errors to the errors list
                        status_4xx = analysis_result.get("status_4xx", 0)
                        if status_4xx > 0:
                            entry["errors"].append(f"HTTP 4XX errors detected: {status_4xx} occurrences")
                        
                        # Add 5XX errors to the errors list
                        status_5xx = analysis_result.get("status_5xx", 0)
                        if status_5xx > 0:
                            entry["errors"].append(f"HTTP 5XX errors detected: {status_5xx} occurrences")
                    else:
                        # No valid log content - set empty analysis
                        entry["access_log_analysis"] = self._analyze_access_log_status_codes("")
                except Exception as e:
                    logger.warning(f"Access log analysis failed for {name}: {e}", exc_info=True)
                    entry["access_log_analysis"] = {"error": str(e)}

                # 6) error log tail
                # Collect nginx error logs from the actual error log file
                # This is separate from HTTP 4XX/5XX errors extracted from access logs
                # We'll append actual error log content to error_log_tail if it already contains HTTP errors
                # Always try to read the error log file, even if error_log_tail already has HTTP errors
                    try:
                        logger.debug(f"Collecting nginx error logs for {name}")
                        # Check if logs are symlinked to stdout/stderr
                        # For SSHHostConnector, always assume logs go to stdout/stderr (Docker best practice)
                        from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                        is_symlinked = False
                        if container_id:
                            if isinstance(host_connector, SSHHostConnector):
                                # For SSHHostConnector, assume logs are symlinked (Docker containers typically log to stdout/stderr)
                                is_symlinked = True
                                logger.debug(f"Error log: Assuming symlinked for SSHHostConnector (Docker best practice)")
                            else:
                                try:
                                    is_symlinked = self._is_path_symlink_to_stdout(host_connector, container_id, error_log)
                                except Exception:
                                    pass
                        
                        if is_symlinked:
                            # Logs are symlinked to stdout/stderr - USE docker logs (it's the only way to get them)
                            logger.debug(f"Error log is symlinked to stdout/stderr, using docker logs")
                            from connectors.host_connectors.docker_host_connector import DockerHostConnector
                            from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                            
                            if container_id and isinstance(host_connector, DockerHostConnector) and hasattr(host_connector, "get_container_logs"):
                                try:
                                    # Check if historical collection - use time-filtered logs if so
                                    if self._is_historical_collection():
                                        since, until = self._get_time_window()
                                        logger.debug(f"Fetching time-filtered docker logs for error log (historical, symlinked logs): {since} to {until}")
                                        # Check if host_connector supports time-filtered logs (must be DockerHostConnector)
                                        from connectors.host_connectors.docker_host_connector import DockerHostConnector
                                        if isinstance(host_connector, DockerHostConnector):
                                            # Strategy: Get ALL logs (no tail limit) for historical collection, then filter by embedded timestamps
                                            logger.info(f"✓ Getting ALL error logs (no tail limit) for {name} for historical filtering by embedded timestamps")
                                            combined_logs = host_connector.get_container_logs(container_id, tail=None)
                                            if isinstance(combined_logs, dict):
                                                combined_logs = combined_logs.get("stdout", combined_logs.get("output", "")) or ""
                                            combined_logs = str(combined_logs or "")
                                            
                                            # Check if we got an error message
                                            if combined_logs.startswith("[docker"):
                                                logger.warning(f"Docker error logs returned error: {combined_logs}")
                                                combined_logs = ""
                                            else:
                                                # ALWAYS post-process: Filter logs by embedded timestamps
                                                from connectors.historical.timestamp_parsers import TimestampParserRegistry
                                                from connectors.historical.log_time_filter import filter_logs_by_time_window
                                                
                                                # Try nginx_access parser first (error logs might also have access log format)
                                                # parser = TimestampParserRegistry.get("nginx_access")
                                                parser = (
                                                    TimestampParserRegistry.get("nginx") or
                                                    TimestampParserRegistry.get("nginx_access") or
                                                    TimestampParserRegistry.get("nginx_access_log") or
                                                    TimestampParserRegistry.get("common_log")
                                                )
                                                if not parser or not combined_logs:
                                                    # Try nginx_error parser as fallback
                                                    parser = TimestampParserRegistry.get("nginx_error")
                                                
                                                if parser and combined_logs:
                                                    log_lines = combined_logs.splitlines()
                                                    logger.info(f"Got {len(log_lines)} error log lines from Docker, filtering by time window {since} to {until}")
                                                    filtered_lines = filter_logs_by_time_window(log_lines, since, until, parser, max_lines=2000)
                                                    combined_logs = "\n".join(filtered_lines)
                                                    logger.info(f"Filtered to {len(filtered_lines)} error log lines within time window")
                                                elif not combined_logs:
                                                    logger.warning(f"No error logs returned from Docker - container may not have logs or logs may be rotated")
                                                else:
                                                    logger.warning(f"Nginx error log parser not found - cannot filter by timestamps")
                                        else:
                                            logger.warning(f"Host connector doesn't support time-filtered logs for error logs")
                                            # Don't fall back to current logs in historical mode
                                            combined_logs = ""
                                    else:
                                        # CURRENT collection - use regular docker logs
                                        logger.debug(f"Fetching docker logs with tail=2000 for error log (current, symlinked logs)")
                                        combined_logs = host_connector.get_container_logs(container_id, tail=2000)
                                        if isinstance(combined_logs, dict):
                                            combined_logs = combined_logs.get("stdout", combined_logs.get("output", "")) or ""
                                        combined_logs = str(combined_logs or "")
                                    
                                    # Split combined docker logs - get nginx error logs (not HTTP errors)
                                    _, error_text = self._split_combined_logs(combined_logs)
                                    error_lines = error_text.splitlines()[-200:] if error_text else []
                                    # Only set if we have nginx error logs (not HTTP 4XX/5XX which are already set)
                                    if error_lines:
                                        existing_errors = entry.get("error_log_tail", "")
                                        if existing_errors and not existing_errors.startswith("["):
                                            entry["error_log_tail"] = existing_errors + "\n" + "\n".join(error_lines)
                                        else:
                                            entry["error_log_tail"] = "\n".join(error_lines)
                                    elif not entry.get("error_log_tail"):
                                        entry["error_log_tail"] = "[no error logs found in docker logs]"
                                except Exception as e:
                                    logger.debug(f"Docker logs failed for symlinked error logs: {e}")
                                    if not entry.get("error_log_tail"):
                                        entry["error_log_tail"] = "[docker logs unavailable]"
                            elif container_id and isinstance(host_connector, SSHHostConnector) and hasattr(host_connector, "exec_cmd"):
                                # For SSH-accessed containers, use docker logs command
                                try:
                                    logger.info(f"Collecting Nginx error logs from Docker (via SSH) for container {container_id}")
                                    docker_logs_cmd = f"timeout 5 docker logs --tail 2000 {container_id} 2>&1"
                                    combined_logs = self._exec(host_connector, None, docker_logs_cmd, timeout=5)
                                    logger.debug(f"Nginx adapter: docker logs command output for error logs (first 200 chars): {str(combined_logs)[:200] if combined_logs else 'None'}")
                                    
                                    if combined_logs and not combined_logs.startswith("[host exec error") and not combined_logs.startswith("[container exec error"):
                                        combined_logs = str(combined_logs)
                                        # Split combined docker logs - get nginx error logs (not HTTP errors)
                                        _, error_text = self._split_combined_logs(combined_logs)
                                        error_lines = error_text.splitlines()[-200:] if error_text else []
                                        if error_lines:
                                            # Append to existing error_log_tail if it has HTTP errors, otherwise replace
                                            existing_error_log = entry.get("error_log_tail", "")
                                            if existing_error_log and existing_error_log != "[no HTTP errors in access logs]":
                                                entry["error_log_tail"] = existing_error_log + "\n--- Nginx Error Logs ---\n" + "\n".join(error_lines)
                                            else:
                                                entry["error_log_tail"] = "\n".join(error_lines)
                                        elif not entry.get("error_log_tail"):
                                            entry["error_log_tail"] = "[no error logs found in docker logs]"
                                        logger.info(f"✅ Successfully collected Nginx error logs from Docker via SSH ({len(combined_logs)} chars, {len(combined_logs.splitlines())} lines)")
                                    else:
                                        logger.warning(f"Nginx adapter: docker logs command for error logs failed or returned error: {combined_logs[:100] if combined_logs else 'None'}")
                                        if not entry.get("error_log_tail"):
                                            entry["error_log_tail"] = "[docker logs unavailable via SSH]"
                                except Exception as e:
                                    logger.warning(f"Nginx adapter: docker logs via SSH for error logs failed: {e}", exc_info=True)
                                    if not entry.get("error_log_tail"):
                                        entry["error_log_tail"] = "[docker logs unavailable via SSH]"
                            else:
                                if not entry.get("error_log_tail"):
                                    entry["error_log_tail"] = "[docker logs not available - cannot read symlinked logs]"
                        elif container_id and hasattr(host_connector, "get_container_logs"):
                            # Try docker logs for non-symlinked logs (fallback option)
                            try:
                                # Check if historical collection - use time-filtered logs if so
                                if self._is_historical_collection():
                                    since, until = self._get_time_window()
                                    logger.debug(f"Fetching time-filtered docker logs for error log (historical, non-symlinked logs): {since} to {until}")
                                    from connectors.host_connectors.docker_host_connector import DockerHostConnector
                                    if isinstance(host_connector, DockerHostConnector):
                                        # Strategy: Get ALL logs (no tail limit) for historical collection, then filter by embedded timestamps
                                        logger.info(f"✓ Getting ALL error logs (no tail limit) for {name} for historical filtering by embedded timestamps")
                                        combined_logs = host_connector.get_container_logs(container_id, tail=None)
                                        if isinstance(combined_logs, dict):
                                            combined_logs = combined_logs.get("stdout", combined_logs.get("output", "")) or ""
                                        combined_logs = str(combined_logs or "")
                                        
                                        # Post-process: Filter logs by embedded timestamps
                                        if not combined_logs.startswith("[docker"):
                                            from connectors.historical.timestamp_parsers import TimestampParserRegistry
                                            from connectors.historical.log_time_filter import filter_logs_by_time_window
                                            # parser = TimestampParserRegistry.get("nginx_access") 
                                            parser = (
                                                TimestampParserRegistry.get("nginx") or
                                                TimestampParserRegistry.get("nginx_access") or
                                                TimestampParserRegistry.get("nginx_access_log") or
                                                TimestampParserRegistry.get("common_log")
                                            ) or TimestampParserRegistry.get("nginx_error")
                                            if parser and combined_logs:
                                                log_lines = combined_logs.splitlines()
                                                logger.info(f"Got {len(log_lines)} error log lines from Docker, filtering by time window {since} to {until}")
                                                filtered_lines = filter_logs_by_time_window(log_lines, since, until, parser, max_lines=2000)
                                                combined_logs = "\n".join(filtered_lines)
                                                logger.info(f"Filtered to {len(filtered_lines)} error log lines within time window")
                                        else:
                                            logger.warning(f"Docker error logs returned error: {combined_logs}")
                                            combined_logs = ""
                                    else:
                                        logger.warning(f"Host connector is not DockerHostConnector, cannot get historical error logs")
                                        combined_logs = ""
                                else:
                                    # CURRENT collection - use regular docker logs
                                    logger.debug(f"Trying docker logs with tail=50 for error log (current, non-symlinked logs)")
                                    combined_logs = host_connector.get_container_logs(container_id, tail=50)
                                    if isinstance(combined_logs, dict):
                                        combined_logs = combined_logs.get("stdout", combined_logs.get("output", "")) or ""
                                    combined_logs = str(combined_logs or "")
                                
                                _, error_text = self._split_combined_logs(combined_logs)
                                error_lines = error_text.splitlines()[-200:] if error_text else []
                                if error_lines:
                                    existing_errors = entry.get("error_log_tail", "")
                                    if existing_errors and not existing_errors.startswith("["):
                                        entry["error_log_tail"] = existing_errors + "\n" + "\n".join(error_lines)
                                    else:
                                        entry["error_log_tail"] = "\n".join(error_lines)
                                elif not entry.get("error_log_tail"):
                                    entry["error_log_tail"] = "[no error logs]"
                            except Exception as e:
                                logger.debug(f"Docker logs failed for error log: {e}")
                                if not entry.get("error_log_tail"):
                                    entry["error_log_tail"] = "[docker logs unavailable]"
                        else:
                            # Use tail command for regular files (not symlinked)
                            # Check if historical collection - use historical log collection mixin if so
                            if self._is_historical_collection():
                                since, until = self._get_time_window()
                                logger.debug(f"Collecting historical file-based error logs for {name}: {since} to {until}")
                                try:
                                    # Use historical log collection mixin for file-based logs
                                    error_content = self._collect_historical_logs(
                                        host_connector=host_connector,
                                        container_id=container_id,
                                        log_paths=[error_log],
                                        parser_name="nginx_error",  # Use nginx_error parser for error logs
                                        since=since,
                                        until=until,
                                        max_lines=2000
                                    )
                                    if error_content:
                                        existing_errors = entry.get("error_log_tail", "")
                                        if existing_errors and not existing_errors.startswith("["):
                                            entry["error_log_tail"] = existing_errors + "\n" + error_content
                                        else:
                                            entry["error_log_tail"] = error_content
                                    elif not entry.get("error_log_tail"):
                                        entry["error_log_tail"] = "[no error logs in time window]"
                                except Exception as e:
                                    logger.warning(f"Historical file-based error log collection failed: {e}, falling back to tail")
                                    # Fallback to tail
                                    tail_cmd = "tail -n 200"
                                    el_out = self._exec(host_connector, container_id, f"{tail_cmd} {shlex.quote(error_log)} 2>/dev/null || true", timeout=3)
                                    error_content = (el_out or "").strip()
                                    if error_content:
                                        existing_errors = entry.get("error_log_tail", "")
                                        if existing_errors and not existing_errors.startswith("["):
                                            entry["error_log_tail"] = existing_errors + "\n" + error_content
                                        else:
                                            entry["error_log_tail"] = error_content
                                    elif not entry.get("error_log_tail"):
                                        entry["error_log_tail"] = "[no error logs]"
                            else:
                                # CURRENT collection - use tail command
                                # Use timeout wrapper and handle permission errors gracefully
                                tail_cmd = "timeout 3 tail -n 200"
                                el_out = self._exec(host_connector, container_id, f"{tail_cmd} {shlex.quote(error_log)} 2>/dev/null || echo ''", timeout=4)
                                error_content = (el_out or "").strip()
                                if error_content:
                                    existing_errors = entry.get("error_log_tail", "")
                                    if existing_errors and not existing_errors.startswith("["):
                                        entry["error_log_tail"] = existing_errors + "\n" + error_content
                                    else:
                                        entry["error_log_tail"] = error_content
                                elif not entry.get("error_log_tail"):
                                    entry["error_log_tail"] = "[no error logs]"
                    except Exception as e:
                        logger.debug(f"Error log collection failed: {e}")
                    # Only set error_log_tail to failure message if it wasn't already set (e.g., from HTTP errors)
                    if not entry.get("error_log_tail") or entry.get("error_log_tail") in ["[no HTTP errors in access logs]", "[no error logs]"]:
                        entry["error_log_tail"] = "[error log collection failed]"
                    
                    # derive a best-effort error summary
                    try:
                        entry["error_summary"] = self._summarize_error_log(entry["error_log_tail"])
                    except Exception:
                        entry["error_summary"] = {"errors": [], "total_lines": 0}
                
                # Populate metrics and logs for orchestrator detection
                # Metrics: version, process, listen_ports, config_test, access_log_analysis
                # Always populate at least one metric to indicate data was collected (even if empty)
                if entry.get("version"):
                    entry["metrics"]["version"] = entry["version"]
                if entry.get("process"):
                    entry["metrics"]["process"] = entry["process"]
                if entry.get("listen_ports"):
                    entry["metrics"]["listen_ports"] = entry["listen_ports"]
                if entry.get("config_test"):
                    entry["metrics"]["config_test"] = entry["config_test"]
                if entry.get("access_log_analysis"):
                    entry["metrics"]["access_log_analysis"] = entry["access_log_analysis"]
                # If no metrics collected, at least mark that liveness passed and service is running
                if not entry["metrics"] and entry.get("running"):
                    entry["metrics"]["status"] = "running"
                    entry["metrics"]["liveness_check"] = "passed"
                
                # Logs: access_log_tail, error_log_tail
                # Format logs as dictionaries with proper structure for UI display
                if entry.get("access_log_tail") and entry["access_log_tail"] not in ["[no access logs]", "[docker logs unavailable]", "[collection failed]"]:
                    access_log_content = entry["access_log_tail"]
                    entry["logs"]["access_log"] = {
                        "content": access_log_content,
                        "type": "current",
                        "collection_mode": "current",
                        "source": "file",
                        "line_count": len(access_log_content.splitlines()) if access_log_content else 0
                    }
                if entry.get("error_log_tail") and entry["error_log_tail"] not in ["[no error logs]", "[no HTTP errors in access logs]", "[docker logs unavailable]", "[collection failed]"]:
                    error_log_content = entry["error_log_tail"]
                    entry["logs"]["error_log"] = {
                        "content": error_log_content,
                        "type": "current",
                        "collection_mode": "current",
                        "source": "file",
                        "line_count": len(error_log_content.splitlines()) if error_log_content else 0
                    }
                
                logger.info(f"Nginx adapter: Collection completed for '{name}'. Metrics keys: {list(entry['metrics'].keys())}, Logs keys: {list(entry['logs'].keys())}")

            except Exception as e:
                # catch-all for instance-level unexpected errors
                entry["errors"].append(f"fatal: {e}")
                entry["errors"].append(traceback.format_exc())

            findings["discovered"].append(name)
            findings["instances"].append(entry)

        return findings

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