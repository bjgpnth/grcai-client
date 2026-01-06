# connectors/adapters/docker_adapter.py

import logging
import traceback
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from connectors.base_connector import BaseConnector

logger = logging.getLogger("grcai.adapters.docker")


def _parse_docker_inspect(inspect_data: dict) -> dict:
    """
    Parse docker inspect output into structured data.
    Extracts key information about container state, resources, and health.
    """
    if not inspect_data:
        return {}
    
    state = inspect_data.get("State", {})
    config = inspect_data.get("Config", {})
    host_config = inspect_data.get("HostConfig", {})
    
    # Extract restart count
    restart_count = inspect_data.get("RestartCount", 0)
    
    # Extract state information
    status = state.get("Status", "unknown")
    running = state.get("Running", False)
    paused = state.get("Paused", False)
    restarting = state.get("Restarting", False)
    exit_code = state.get("ExitCode", None)
    started_at = state.get("StartedAt", "")
    finished_at = state.get("FinishedAt", "")
    
    # Extract OOM kill information
    oom_killed = state.get("OOMKilled", False)
    
    # Extract health check status
    health = state.get("Health", {})
    health_status = health.get("Status", "none") if health else "none"
    health_failing_streak = health.get("FailingStreak", 0) if health else 0
    
    # Extract resource limits
    memory_limit = host_config.get("Memory", 0)  # in bytes
    cpu_quota = host_config.get("CpuQuota", 0)
    cpu_period = host_config.get("CpuPeriod", 0)
    cpu_shares = host_config.get("CpuShares", 0)
    
    # Calculate CPU limit percentage (if quota/period set)
    cpu_limit_percent = None
    if cpu_quota > 0 and cpu_period > 0:
        cpu_limit_percent = (cpu_quota / cpu_period) * 100
    
    # Extract restart policy
    restart_policy = host_config.get("RestartPolicy", {})
    restart_policy_name = restart_policy.get("Name", "no")
    restart_policy_maximum_retry_count = restart_policy.get("MaximumRetryCount", 0)
    
    # Extract image information
    image = inspect_data.get("Image", "")
    
    # Extract labels
    labels = config.get("Labels", {})
    
    return {
        "restart_count": restart_count,
        "status": status,
        "running": running,
        "paused": paused,
        "restarting": restarting,
        "exit_code": exit_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "oom_killed": oom_killed,
        "health_status": health_status,
        "health_failing_streak": health_failing_streak,
        "resource_limits": {
            "memory_bytes": memory_limit,
            "memory_mb": round(memory_limit / (1024 * 1024), 2) if memory_limit > 0 else None,
            "cpu_quota": cpu_quota,
            "cpu_period": cpu_period,
            "cpu_shares": cpu_shares,
            "cpu_limit_percent": round(cpu_limit_percent, 2) if cpu_limit_percent else None
        },
        "restart_policy": {
            "name": restart_policy_name,
            "maximum_retry_count": restart_policy_maximum_retry_count
        },
        "image": image,
        "labels": labels
    }


def _get_container_stats(connector, container_id: str) -> Optional[dict]:
    """
    Get current container resource usage using docker stats.
    Returns dict with memory_usage, cpu_percent, etc.
    """
    try:
        # Try Docker API first (if available)
        if hasattr(connector, "client") and connector.client:
            try:
                container = connector.client.containers.get(container_id)
                stats = container.stats(stream=False)
                
                # Parse Docker stats API response
                cpu_delta = stats.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0) - \
                           stats.get("precpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
                system_delta = stats.get("cpu_stats", {}).get("system_cpu_usage", 0) - \
                              stats.get("precpu_stats", {}).get("system_cpu_usage", 0)
                
                cpu_percent = 0.0
                if system_delta > 0:
                    num_cores = len(stats.get("cpu_stats", {}).get("cpu_usage", {}).get("percpu_usage", []))
                    if num_cores == 0:
                        num_cores = 1
                    cpu_percent = (cpu_delta / system_delta) * num_cores * 100.0
                
                memory_stats = stats.get("memory_stats", {})
                memory_usage = memory_stats.get("usage", 0)
                memory_limit = memory_stats.get("limit", 0)
                
                return {
                    "memory_usage_bytes": memory_usage,
                    "memory_limit_bytes": memory_limit,
                    "memory_used_mb": round(memory_usage / (1024 * 1024), 2) if memory_usage > 0 else 0,
                    "memory_limit_mb": round(memory_limit / (1024 * 1024), 2) if memory_limit > 0 else None,
                    "cpu_percent": round(cpu_percent, 2),
                    "source": "docker_api"
                }
            except Exception as e:
                logger.debug(f"Docker API stats failed, trying CLI: {e}")
        
        # Fallback to docker CLI
        if hasattr(connector, "exec_cmd"):
            cmd = f"docker stats --no-stream --format '{{{{json .}}}}' {container_id} 2>/dev/null || true"
            out = connector.exec_cmd(cmd)
            if isinstance(out, dict):
                stdout = out.get("stdout", "")
            else:
                stdout = str(out or "")
            
            if stdout and stdout.strip():
                try:
                    stats_data = json.loads(stdout.strip())
                    # Parse memory usage (format: "123.45MiB / 512MiB")
                    memory_str = stats_data.get("MemUsage", "")
                    memory_parts = memory_str.split(" / ")
                    memory_used_str = memory_parts[0] if len(memory_parts) > 0 else ""
                    memory_limit_str = memory_parts[1] if len(memory_parts) > 1 else ""
                    
                    # Parse CPU percentage (format: "12.34%")
                    cpu_str = stats_data.get("CPUPerc", "0%")
                    cpu_percent = float(cpu_str.rstrip("%")) if cpu_str != "0%" else 0.0
                    
                    return {
                        "memory_usage": memory_str,
                        "memory_used_str": memory_used_str,
                        "memory_limit_str": memory_limit_str,
                        "cpu_percent": cpu_percent,
                        "net_io": stats_data.get("NetIO", ""),
                        "block_io": stats_data.get("BlockIO", ""),
                        "source": "docker_cli"
                    }
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logger.debug(f"Failed to parse docker stats JSON: {e}")
                    return None
    except Exception as e:
        logger.debug(f"Failed to get container stats: {e}")
        return None
    
    return None


def _analyze_container_issues(container_data: dict, stats: Optional[dict] = None) -> List[str]:
    """
    Analyze container data for issues and return list of error messages.
    """
    issues = []
    
    restart_count = container_data.get("restart_count", 0)
    status = container_data.get("status", "")
    exit_code = container_data.get("exit_code")
    oom_killed = container_data.get("oom_killed", False)
    health_status = container_data.get("health_status", "none")
    health_failing_streak = container_data.get("health_failing_streak", 0)
    restarting = container_data.get("restarting", False)
    
    # Check for excessive restarts
    if restart_count > 0:
        if restart_count >= 5:
            issues.append(f"Container has restarted {restart_count} times (possible crash loop)")
        elif restart_count >= 3:
            issues.append(f"Container has restarted {restart_count} times")
    
    # Check for OOM kills
    if oom_killed:
        issues.append("Container was killed by OOM (Out of Memory)")
    
    # Check exit code (137 = OOM kill, 143 = SIGTERM, non-zero = error)
    if exit_code is not None and exit_code != 0:
        if exit_code == 137:
            issues.append(f"Container exited with code 137 (OOM kill)")
        elif exit_code == 143:
            issues.append(f"Container exited with code 143 (SIGTERM)")
        else:
            issues.append(f"Container exited with non-zero code {exit_code}")
    
    # Check health check status
    if health_status == "unhealthy":
        issues.append(f"Container health check is unhealthy (failing streak: {health_failing_streak})")
    elif health_status == "starting" and health_failing_streak > 0:
        issues.append(f"Container health check is starting but has {health_failing_streak} failures")
    
    # Check if container is stuck restarting
    if restarting:
        issues.append("Container is currently restarting (may be stuck)")
    
        # Check resource limits vs usage
        if stats:
            cpu_percent = stats.get("cpu_percent", 0)
            
            # Check memory usage (handle both API and CLI formats)
            memory_usage_percent = None
            if stats.get("source") == "docker_api":
                # Docker API format
                memory_used_bytes = stats.get("memory_usage_bytes", 0)
                memory_limit_bytes = stats.get("memory_limit_bytes", 0)
                if memory_limit_bytes > 0 and memory_used_bytes > 0:
                    memory_usage_percent = (memory_used_bytes / memory_limit_bytes) * 100
            else:
                # CLI format - parse strings
                memory_limit_str = stats.get("memory_limit_str", "")
                memory_used_str = stats.get("memory_used_str", "")
                if memory_limit_str and memory_used_str:
                    try:
                        # Extract numbers from strings like "512MiB" or "1.2GiB"
                        def parse_memory(s):
                            s = s.strip().upper()
                            if "GIB" in s:
                                num = float(re.search(r'[\d.]+', s).group())
                                return num * 1024  # Convert to MB
                            elif "MIB" in s:
                                return float(re.search(r'[\d.]+', s).group())
                            elif "KIB" in s:
                                num = float(re.search(r'[\d.]+', s).group())
                                return num / 1024  # Convert to MB
                            return None
                        
                        used_mb = parse_memory(memory_used_str)
                        limit_mb = parse_memory(memory_limit_str)
                        
                        if used_mb and limit_mb and used_mb > 0 and limit_mb > 0:
                            memory_usage_percent = (used_mb / limit_mb) * 100
                    except Exception:
                        pass  # Ignore parsing errors
            
            if memory_usage_percent and memory_usage_percent > 90:
                if stats.get("source") == "docker_api":
                    issues.append(f"Container memory usage is {memory_usage_percent:.1f}% of limit ({stats.get('memory_used_mb', 0)}MB / {stats.get('memory_limit_mb', 0)}MB)")
                else:
                    issues.append(f"Container memory usage is {memory_usage_percent:.1f}% of limit ({stats.get('memory_used_str', '')} / {stats.get('memory_limit_str', '')})")
            
            if cpu_percent > 90:
                issues.append(f"Container CPU usage is {cpu_percent:.1f}% (very high)")
    
    return issues


class DockerAdapter(BaseConnector):
    """
    DockerAdapter - Collects Docker container-level diagnostics.
    
    Focuses on:
    - Container lifecycle (restarts, exits, status)
    - Resource limits vs usage
    - Health check status
    - OOM kills
    - Docker daemon health
    
    This complements service-specific adapters by providing infrastructure-level
    context that may not be visible in application logs.
    """
    
    def __init__(self, name=None, env_config=None, issue_time=None, component_config=None, **kwargs):
        super().__init__(name or "docker", issue_time, component_config)
        self.env_config = env_config or {}
        self.component_config = component_config or {}
        
        # Optional: filter containers by name pattern or labels
        self.container_filters = self.component_config.get("container_filters", {})
        self.include_all_containers = self.component_config.get("include_all_containers", True)
        
        logger.info(
            "DockerAdapter initialized; filters=%s, include_all=%s",
            self.container_filters, self.include_all_containers
        )
    
    def _make_host_connector(self, host_info):
        """Create appropriate host connector for Docker access."""
        from connectors.host_connectors.docker_host_connector import DockerHostConnector
        from connectors.host_connectors.ssh_host_connector import SSHHostConnector
        from connectors.host_connectors.local_host_connector import LocalHostConnector
        
        access = self.env_config.get("access", {})
        
        if host_info.get("docker"):
            return DockerHostConnector(host_info=host_info, global_access=access)
        if host_info.get("ssh"):
            return SSHHostConnector(host_info=host_info, global_access=access)
        return LocalHostConnector(host_info=host_info)
    
    def _exec(self, connector, cmd: str) -> str:
        """Execute command on host (not in container)."""
        if hasattr(connector, "exec_cmd"):
            try:
                out = connector.exec_cmd(cmd)
                if isinstance(out, dict):
                    # SSH connector returns dict with stdout, stderr, rc, error
                    stdout = out.get("stdout", "") or out.get("output", "")
                    stderr = out.get("stderr", "")
                    error = out.get("error")
                    rc = out.get("rc", 0)
                    
                    # If there's an error or non-zero return code, log it but still return stdout if available
                    if error or (rc != 0 and rc is not None):
                        logger.debug(f"Docker adapter _exec: Command '{cmd[:50]}...' returned rc={rc}, error={error}, stderr={stderr[:100] if stderr else 'none'}")
                    
                    return stdout
                return str(out or "")
            except Exception as e:
                logger.debug(f"Docker adapter _exec: Exception executing '{cmd[:50]}...': {e}")
                return f"[host exec error: {e}]"
        return "[no exec method]"
    
    def _get_docker_inspect(self, connector, container_id: str) -> Optional[dict]:
        """Get docker inspect output as dict."""
        instance_appended = False
        try:
            # Try using Docker API first (if available)
            if hasattr(connector, "client") and connector.client:
                try:
                    container = connector.client.containers.get(container_id)
                    return container.attrs
                except Exception:
                    pass
            
            # Fallback to docker CLI
            cmd = f"docker inspect {container_id} 2>/dev/null"
            out = self._exec(connector, cmd)
            if out and not out.startswith("["):
                try:
                    inspect_list = json.loads(out)
                    if inspect_list and len(inspect_list) > 0:
                        return inspect_list[0]
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            logger.debug(f"Failed to get docker inspect for {container_id}: {e}")
        
        return None
    
    def _get_docker_events(self, connector, since_minutes: int = 60) -> List[dict]:
        """Get Docker events from the last N minutes."""
        events = []
        try:
            since_time = datetime.utcnow() - timedelta(minutes=since_minutes)
            since_timestamp = int(since_time.timestamp())
            
            # Use docker events API
            cmd = f"docker events --since {since_timestamp} --format '{{{{json .}}}}' 2>/dev/null | head -100 || true"
            out = self._exec(connector, cmd)
            
            if out:
                for line in out.splitlines():
                    line = line.strip()
                    if line:
                        try:
                            event = json.loads(line)
                            events.append(event)
                        except json.JSONDecodeError:
                            pass
        except Exception as e:
            logger.debug(f"Failed to get docker events: {e}")
        
        return events
    
    def _get_docker_daemon_logs(self, connector) -> str:
        """
        Get Docker daemon logs (systemd journal, log file, or Docker Desktop logs).
        
        Supports:
        - Linux: systemd journal, /var/log/docker.log, /var/log/docker/daemon.log
        - macOS (Docker Desktop): ~/Library/Containers/com.docker.docker/Data/log/
        
        Note: For macOS Docker Desktop, logs are read directly from the local filesystem
        (not via connector) since Docker Desktop runs in a VM and logs are stored locally.
        """
        import platform
        import os
        
        logs = []
        is_macos = platform.system() == "Darwin"
        
        # Check if we're running locally (for macOS Docker Desktop logs)
        # Docker Desktop logs are always on the local macOS machine, not on remote hosts
        is_local = True  # Default to True, we'll check connector type
        try:
            from connectors.host_connectors.local_host_connector import LocalHostConnector
            from connectors.host_connectors.docker_host_connector import DockerHostConnector
            # If it's a LocalHostConnector or DockerHostConnector (which connects to local Docker),
            # we're on the local machine
            if isinstance(connector, LocalHostConnector) or isinstance(connector, DockerHostConnector):
                is_local = True
            else:
                # SSH or other remote connector - logs won't be on local macOS
                is_local = False
        except Exception:
            # If we can't determine, assume local (safer for macOS)
            is_local = True
        
        # Try systemd journal (Linux only, via connector)
        if not is_macos:
            cmd = "journalctl -u docker.service --no-pager -n 100 2>/dev/null || true"
            out = self._exec(connector, cmd)
            
            # Debug: Log raw output details
            out_len = len(out) if out else 0
            out_starts_with_bracket = out.startswith("[") if out else False
            out_first_chars = repr(out[:200]) if out and len(out) > 0 else "None"
            logger.debug(f"Docker adapter: journalctl raw output - length: {out_len}, starts with '[': {out_starts_with_bracket}, first 200 chars: {out_first_chars}")
            
            # More lenient check - only filter if it's clearly an error message
            # Error messages from _exec start with "[host exec error:" or "[no exec method]"
            if out:
                # Check if it's a real error message (not just output that happens to start with [)
                is_error = (
                    out.startswith("[host exec error:") or 
                    out.startswith("[container exec error:") or 
                    out.startswith("[no exec method]")
                )
                
                if is_error:
                    logger.warning(f"Docker adapter: journalctl returned error: {out[:200]}")
                elif out.strip():
                    # Valid output - add it
                    logs.append(out)
                    logger.info(f"Docker adapter: Collected {len(out)} characters from journalctl")
                else:
                    logger.debug("Docker adapter: journalctl output filtered - only whitespace")
            else:
                logger.debug("Docker adapter: journalctl returned empty/None output")
        
        # Try Linux log files (via connector)
        if not is_macos:
            for log_path in ["/var/log/docker.log", "/var/log/docker/daemon.log"]:
                cmd = f"tail -n 100 {log_path} 2>/dev/null || true"
                out = self._exec(connector, cmd)
                if out and not out.startswith("[") and out.strip():
                    logs.append(out)
                    break
        
        # Try macOS Docker Desktop log locations (read directly from local filesystem)
        # Note: Docker Desktop logs are always on the local macOS machine, even when
        # connecting to Docker via DockerHostConnector
        # However, if running inside a container, the macOS host filesystem may not be accessible
        if is_macos and is_local:
            # Docker Desktop stores logs in ~/Library/Containers/com.docker.docker/Data/log/
            # Subdirectories: host/ and vm/
            docker_desktop_log_base = os.path.expanduser("~/Library/Containers/com.docker.docker/Data/log")
            
            # Check if we're running inside a container (can't access host macOS filesystem)
            is_inside_container = os.path.exists("/.dockerenv") or os.path.exists("/.dockerinit")
            
            if is_inside_container:
                logger.debug("Running inside container - Docker Desktop logs on macOS host are not accessible")
                # Could add a note here, but for now just skip
            elif os.path.exists(docker_desktop_log_base) and os.path.isdir(docker_desktop_log_base):
                # Try to read from host/ and vm/ subdirectories
                for subdir in ["host", "vm"]:
                    log_dir = os.path.join(docker_desktop_log_base, subdir)
                    if os.path.exists(log_dir) and os.path.isdir(log_dir):
                        try:
                            # List log files in the directory
                            log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
                            if log_files:
                                # Get the most recent log file
                                log_files.sort(key=lambda f: os.path.getmtime(os.path.join(log_dir, f)), reverse=True)
                                log_file = os.path.join(log_dir, log_files[0])
                                
                                # Read last 100 lines
                                try:
                                    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                                        lines = f.readlines()
                                        if lines:
                                            # Get last 100 lines
                                            recent_lines = lines[-100:] if len(lines) > 100 else lines
                                            log_content = "".join(recent_lines)
                                            if log_content.strip():
                                                logs.append(f"=== Docker Desktop {subdir} logs ({log_files[0]}) ===\n{log_content}")
                                                logger.info(f"Successfully read Docker Desktop logs from {log_file}")
                                                break  # Found logs, no need to check other subdirs
                                except Exception as e:
                                    logger.debug(f"Failed to read Docker Desktop log file {log_file}: {e}")
                        except Exception as e:
                            logger.debug(f"Failed to access Docker Desktop log directory {log_dir}: {e}")
            else:
                logger.debug(f"Docker Desktop log directory not found: {docker_desktop_log_base}")
        
        result = "\n".join(logs) if logs else ""
        logger.debug(f"Docker adapter: _get_docker_daemon_logs returning {len(result)} characters, {len(logs)} log sources")
        return result
    
    def collect_for_host(self, host_info, connector):
        """
        Collect Docker container diagnostics for a host.
        
        Returns:
            dict with keys:
            - type: "docker"
            - discovered: list of host names
            - containers: list of container data
            - daemon: Docker daemon information
            - findings: aggregate findings
            - errors: list of errors
        """
        findings = {
            "type": "docker",
            "discovered": [],
            "containers": [],
            "daemon": {},
            "findings": {},
            "errors": [],
            # Signal daemon reachability even if zero containers
            "instances": []
        }
        
        host_name = host_info.get("name") or host_info.get("address") or "unknown"
        findings["discovered"].append(host_name)
        
        try:
            # Quick daemon reachability check up front; don't throw, just record.
            daemon_reachable = False
            if hasattr(connector, "client") and connector.client:
                try:
                    connector.client.ping()
                    daemon_reachable = True
                except Exception as e:
                    findings["errors"].append(f"Docker ping failed: {e}")
            else:
                # For SSH connectors, check reachability via CLI
                if hasattr(connector, "exec_cmd"):
                    try:
                        # Try docker version as a reachability test
                        cmd = "docker version --format '{{.Server.Version}}' 2>/dev/null || true"
                        out = self._exec(connector, cmd)
                        if out and not out.startswith("[") and out.strip():
                            daemon_reachable = True
                            logger.debug(f"Docker adapter: Daemon reachable via CLI (version: {out.strip()})")
                    except Exception:
                        pass
                if not daemon_reachable:
                    # Only add this as an error if we can't reach via CLI either
                    findings["errors"].append("Docker client not available on connector")
            
            # Get list of containers
            containers = []
            if hasattr(connector, "list_containers"):
                try:
                    container_list = connector.list_containers()
                    if isinstance(container_list, list):
                        containers = container_list
                    elif isinstance(container_list, dict):
                        # Check for error status from SSH connector
                        if not container_list.get("ok", True):
                            error_msg = container_list.get("error", "Unknown error listing containers")
                            findings["errors"].append(f"Failed to list containers: {error_msg}")
                            logger.warning(f"Docker adapter: list_containers returned error: {error_msg}")
                        containers = container_list.get("containers", [])
                        # If we successfully got containers, daemon is reachable
                        if containers:
                            daemon_reachable = True
                except Exception as e:
                    findings["errors"].append(f"Failed to list containers: {e}")
                    logger.warning(f"Docker adapter: Exception in list_containers: {e}", exc_info=True)
            
            # Fallback: Try docker ps directly if list_containers failed or returned empty
            if not containers and hasattr(connector, "exec_cmd"):
                try:
                    logger.debug("Docker adapter: Trying fallback docker ps command")
                    cmd = "docker ps --format '{{json .}}' 2>/dev/null || true"
                    out = self._exec(connector, cmd)
                    if out and not out.startswith("[") and out.strip():
                        # json is already imported at module level
                        for line in out.strip().splitlines():
                            line = line.strip()
                            if line:
                                try:
                                    container_data = json.loads(line)
                                    containers.append(container_data)
                                except json.JSONDecodeError:
                                    continue
                    if containers:
                        logger.info(f"Docker adapter: Found {len(containers)} containers via fallback docker ps")
                except Exception as e:
                    logger.debug(f"Docker adapter: Fallback docker ps failed: {e}")
            
            # Filter containers if needed
            if not self.include_all_containers and self.container_filters:
                filtered = []
                for c in containers:
                    # Get container name
                    name = None
                    if hasattr(c, "name"):
                        name = c.name
                    elif isinstance(c, dict):
                        name = c.get("name") or c.get("Names", [""])[0] if c.get("Names") else None
                    
                    # Apply filters
                    name_pattern = self.container_filters.get("name_pattern")
                    if name_pattern and name:
                        if re.search(name_pattern, name):
                            filtered.append(c)
                    else:
                        filtered.append(c)
                containers = filtered
            
            # Collect data for each container
            container_data_list = []
            all_issues = []
            containers_with_restarts = []
            oom_kills = []
            unhealthy_containers = []
            
            for c in containers:
                try:
                    # Get container ID and name
                    container_id = None
                    container_name = None
                    
                    if hasattr(c, "id"):
                        container_id = c.id
                    elif isinstance(c, dict):
                        container_id = c.get("id") or c.get("Id")
                    
                    if hasattr(c, "name"):
                        container_name = c.name
                    elif isinstance(c, dict):
                        names = c.get("name") or c.get("Names")
                        if isinstance(names, list) and names:
                            container_name = names[0].lstrip("/")
                        elif isinstance(names, str):
                            container_name = names.lstrip("/")
                    
                    if not container_id:
                        continue
                    
                    # Get docker inspect data
                    inspect_data = self._get_docker_inspect(connector, container_id)
                    if not inspect_data:
                        continue
                    
                    # Parse inspect data
                    parsed = _parse_docker_inspect(inspect_data)
                    
                    # Get current stats
                    stats = _get_container_stats(connector, container_id)
                    
                    # Analyze for issues
                    issues = _analyze_container_issues(parsed, stats)
                    
                    # Build container entry
                    container_entry = {
                        "name": container_name or container_id[:12],
                        "container_id": container_id[:12],  # Short ID
                        "full_container_id": container_id,
                        **parsed,
                        "stats": stats,
                        "issues": issues
                    }
                    
                    container_data_list.append(container_entry)
                    
                    # Aggregate findings
                    if parsed.get("restart_count", 0) > 0:
                        containers_with_restarts.append(container_name or container_id[:12])
                    
                    if parsed.get("oom_killed", False):
                        oom_kills.append(container_name or container_id[:12])
                    
                    if parsed.get("health_status") == "unhealthy":
                        unhealthy_containers.append(container_name or container_id[:12])
                    
                    all_issues.extend(issues)
                    
                except Exception as e:
                    logger.warning(f"Failed to collect data for container {c}: {e}", exc_info=True)
                    findings["errors"].append(f"Container collection error: {e}")
            
            findings["containers"] = container_data_list
            
            # Get Docker daemon information
            # Initialize daemon dict first to preserve logs even if other collection fails
            findings["daemon"] = {}
            
            try:
                # Get Docker version
                version_out = self._exec(connector, "docker version --format '{{.Server.Version}}' 2>/dev/null || true")
                docker_version = version_out.strip() if version_out and not version_out.startswith("[") else None
                findings["daemon"]["version"] = docker_version
            except Exception as e:
                logger.debug(f"Failed to get Docker version: {e}")
                findings["daemon"]["version"] = None
                
            try:
                # Get Docker info (comprehensive daemon information)
                info_out = self._exec(connector, "docker info --format '{{json .}}' 2>/dev/null || true")
                docker_info = {}
                if info_out and not info_out.startswith("["):
                    try:
                        # Strip whitespace before parsing
                        info_out_stripped = info_out.strip()
                        docker_info = json.loads(info_out_stripped)
                    except json.JSONDecodeError as e:
                        logger.debug(f"Failed to parse docker info as JSON: {e}, raw output: {info_out[:200]}")
                findings["daemon"]["info"] = docker_info
            except Exception as e:
                logger.debug(f"Failed to get Docker info: {e}")
                findings["daemon"]["info"] = {}
            
            try:
                # Get additional Docker system info
                system_df_out = self._exec(connector, "docker system df --format '{{json .}}' 2>/dev/null || true")
                system_df = {}
                if system_df_out and not system_df_out.startswith("["):
                    try:
                        system_df = json.loads(system_df_out)
                    except json.JSONDecodeError:
                        pass
                findings["daemon"]["system_df"] = system_df
            except Exception as e:
                logger.debug(f"Failed to get Docker system df: {e}")
                findings["daemon"]["system_df"] = {}
                
            # Get daemon logs - do this separately so it doesn't get lost if other collection fails
            try:
                daemon_logs = self._get_docker_daemon_logs(connector)
                
                # Log if we got logs or not (for debugging)
                if daemon_logs:
                    logger.info(f"Docker adapter: Collected {len(daemon_logs)} characters of Docker daemon logs")
                    findings["daemon"]["logs"] = daemon_logs[:10000]  # Limit log size
                else:
                    logger.warning("Docker adapter: No Docker daemon logs collected - this may indicate an issue with log collection")
                    findings["daemon"]["logs"] = ""
                
                # Debug: Log what we stored
                stored_logs = findings["daemon"].get("logs", "")
                logger.debug(f"Docker adapter: Stored {len(stored_logs)} characters in findings['daemon']['logs'], type: {type(stored_logs)}")
            except Exception as e:
                logger.error(f"Docker adapter: Exception collecting daemon logs: {e}", exc_info=True)
                findings["daemon"]["logs"] = ""
            
            # Aggregate findings
            findings["findings"] = {
                "total_containers": len(container_data_list),
                "containers_with_restarts": containers_with_restarts,
                "restart_count_total": sum(c.get("restart_count", 0) for c in container_data_list),
                "oom_kills": oom_kills,
                "unhealthy_containers": unhealthy_containers,
                "total_issues": len(all_issues)
            }
            
            # Add aggregate issues to errors
            if containers_with_restarts:
                findings["errors"].append(
                    f"Containers with restarts: {', '.join(containers_with_restarts)}"
                )
            if oom_kills:
                findings["errors"].append(
                    f"Containers killed by OOM: {', '.join(oom_kills)}"
                )
            if unhealthy_containers:
                findings["errors"].append(
                    f"Unhealthy containers: {', '.join(unhealthy_containers)}"
                )

            # Get container counts using CLI (more reliable than parsing container_data_list)
            containers_total_all = len(containers)
            containers_running = 0
            containers_stopped = 0
            
            if hasattr(connector, "exec_cmd"):
                try:
                    # Get total containers (including stopped)
                    cmd = "docker ps -a --format '{{.ID}}' 2>/dev/null | wc -l || echo '0'"
                    out = self._exec(connector, cmd)
                    if out and not out.startswith("[") and out.strip().isdigit():
                        containers_total_all = int(out.strip())
                    
                    # Get running containers count
                    cmd = "docker ps --format '{{.ID}}' 2>/dev/null | wc -l || echo '0'"
                    out = self._exec(connector, cmd)
                    if out and not out.startswith("[") and out.strip().isdigit():
                        containers_running = int(out.strip())
                    
                    # Calculate stopped containers
                    containers_stopped = containers_total_all - containers_running
                except Exception as e:
                    logger.debug(f"Failed to get container counts via CLI: {e}")
                    # Fallback: use container_data_list if available
                    containers_running = len([c for c in container_data_list if c.get("running", False)])
                    containers_stopped = len(container_data_list) - containers_running
            
            # Determine final status: if we have containers or can reach daemon, mark as reachable
            final_status = "unknown"
            if daemon_reachable or containers_total_all > 0:
                final_status = "reachable"
            
            # Extract images_total from daemon info (handle different possible formats)
            images_total = None
            daemon_info = findings.get("daemon", {}).get("info", {})
            if isinstance(daemon_info, dict):
                # Try different possible keys for images count
                images_total = daemon_info.get("Images") or daemon_info.get("images") or daemon_info.get("ImageCount")
                # If it's a string, try to convert to int
                if isinstance(images_total, str) and images_total.isdigit():
                    images_total = int(images_total)
            
            # Fallback: try to get images count directly if not in daemon info
            if images_total is None and hasattr(connector, "exec_cmd"):
                try:
                    cmd = "docker images --format '{{.ID}}' 2>/dev/null | wc -l || echo '0'"
                    out = self._exec(connector, cmd)
                    if out and not out.startswith("[") and out.strip().isdigit():
                        images_total = int(out.strip())
                except Exception:
                    pass
            
            # Add a summary instance so orchestrator treats daemon as reachable
            inst_metrics = {
                "containers_total": containers_total_all,
                "containers_running": containers_running,
                "containers_stopped": containers_stopped,
                "images_total": images_total
            }
            # Extract daemon logs for instance
            daemon_logs_snippet = ""
            inst_logs = {}
            try:
                # Get logs from findings["daemon"]["logs"]
                daemon_data = findings.get("daemon", {})
                daemon_logs_raw = daemon_data.get("logs", "") if isinstance(daemon_data, dict) else ""
                
                logger.debug(f"Docker adapter: Extracting logs - daemon_data type: {type(daemon_data)}, has logs key: {'logs' in daemon_data if isinstance(daemon_data, dict) else False}")
                logger.debug(f"Docker adapter: daemon_logs_raw - length: {len(daemon_logs_raw) if daemon_logs_raw else 0}, type: {type(daemon_logs_raw)}, truthy: {bool(daemon_logs_raw)}")
                
                if daemon_logs_raw:
                    daemon_logs_snippet = daemon_logs_raw[:2000]  # Limit to 2000 chars for instance
                    inst_logs["daemon_logs"] = daemon_logs_snippet
                    logger.info(f"Docker adapter: Added {len(daemon_logs_snippet)} characters to instance logs")
                else:
                    logger.warning(f"Docker adapter: No daemon logs to add - daemon_logs_raw is empty. daemon_data keys: {list(daemon_data.keys()) if isinstance(daemon_data, dict) else 'not a dict'}")
            except Exception as e:
                logger.error(f"Docker adapter: Exception extracting daemon logs: {e}", exc_info=True)
                daemon_logs_snippet = ""
            findings["instances"].append({
                "name": host_name,
                "status": final_status,
                "containers_total": containers_total_all,
                "errors": findings.get("errors", []),
                "metrics": inst_metrics,
                "logs": inst_logs
            })
            instance_appended = True
            
        except Exception as e:
            logger.error(f"Docker adapter collection failed: {e}", exc_info=True)
            findings["errors"].append(f"Fatal error: {e}")
            findings["errors"].append(traceback.format_exc())
        finally:
            # Ensure at least one instance entry so orchestrator treats component as present
            if not instance_appended:
                daemon_logs_snippet = ""
                try:
                    daemon_logs_snippet = (findings.get("daemon", {}).get("logs") or "")[:2000]
                except Exception:
                    daemon_logs_snippet = ""
                inst_logs = {}
                if daemon_logs_snippet:
                    inst_logs["daemon_logs"] = daemon_logs_snippet
                findings["instances"].append({
                    "name": host_name,
                    "status": "unknown",
                    "containers_total": len(findings.get("containers") or []),
                    "errors": findings.get("errors", []),
                    "metrics": {"containers_total": len(findings.get("containers") or [])},
                    "logs": inst_logs
                })
        
        return findings
    
    def collect(self):
        """Standalone collection method."""
        self.start_collection()
        result = {"type": "docker", "discovered": [], "findings": {}}
        
        for host_info in self.env_config.get("hosts", []):
            name = host_info.get("name") or host_info.get("address")
            try:
                connector = self._make_host_connector(host_info)
                host_data = self.collect_for_host(host_info, connector)
                result["discovered"].append(name)
                result["findings"][name] = host_data
            except Exception as e:
                result["discovered"].append(name)
                result["findings"][name] = {"error": str(e), "trace": traceback.format_exc()}
        
        self.end_collection()
        return result

