# connectors/adapters/redis_adapter.py (UPDATED)

import logging
import re
import traceback
from connectors.base_connector import BaseConnector

logger = logging.getLogger("grcai.adapters.redis")

DEFAULT_REDIS_CLI = "redis-cli"


def _parse_redis_info(text: str):
    """
    Parse `redis-cli INFO` text into a dict of sections -> {key: value}
    Includes special handling for Keyspace section:
    db0:keys=10,expires=0,avg_ttl=0  → { "db0": {"keys": "10", ...} }
    
    Handles multiple section header formats:
    - # Server
    - #Server (no space)
    - Server: (colon format, less common)
    """
    out = {}
    if not text:
        return out

    section = None

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Section header - try multiple formats
        # Format 1: "# Server" or "#Server"
        if line.startswith("#"):
            # Remove # and any following space
            section_name = line[1:].strip().lower()
            if section_name:
                section = section_name
                out.setdefault(section, {})
                continue
        
        # Format 2: "Server:" (colon format, less common)
        if line.endswith(":") and not "=" in line:
            section_name = line[:-1].strip().lower()
            if section_name:
                section = section_name
                out.setdefault(section, {})
                continue

        # Skip lines that don't have a section yet (unless they're comments)
        if section is None:
            continue

        # ---- KEYSPACE SPECIAL CASE ----
        # Keyspace format: db0:keys=10,expires=0,avg_ttl=0
        # Note: Keyspace uses = for values, not :
        if section == "keyspace" and ":" in line and "=" in line:
            # Example: db0:keys=10,expires=0,avg_ttl=0
            try:
                db, rest = line.split(":", 1)
                metrics = {}
                for part in rest.split(","):
                    if "=" in part:
                        k, v = part.split("=", 1)
                        metrics[k.strip()] = v.strip()
                if db and metrics:
                    out["keyspace"][db] = metrics
            except Exception:
                # If keyspace parsing fails, try to parse as regular key:value
                pass
            continue

        # ---- Standard key:value or key=value ----
        # Redis 8.4.0 uses : for key-value pairs (e.g., redis_version:8.4.0)
        # Older versions might use =, so support both
        if section is not None and not line.startswith("#"):
            # Try colon first (Redis 8.4.0 format)
            # Only parse as key:value if it's NOT a keyspace line (keyspace already handled above)
            if ":" in line:
                # Skip if this looks like a keyspace line (has both : and =, and we're in keyspace section)
                # But if we're not in keyspace section, parse it as key:value
                if section == "keyspace" and "=" in line:
                    # Already handled above, skip
                    continue
                try:
                    k, v = line.split(":", 1)
                    k = k.strip()
                    v = v.strip()
                    if k:  # Only add non-empty keys
                        out[section][k] = v
                except Exception:
                    # If colon split fails, try equals
                    pass
            # Fallback to equals format (older Redis versions or special cases)
            elif "=" in line:
                try:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip()
                    if k:  # Only add non-empty keys
                        out[section][k] = v
                except Exception:
                    # Skip malformed lines
                    continue

    return out


class RedisAdapter(BaseConnector):
    def __init__(self, name=None, env_config=None, issue_time=None, component_config=None, **kwargs):
        super().__init__(name or "redis", issue_time, component_config)
        self.env_config = env_config or {}
        self.component_config = component_config or {}
        self.instances_cfg = self.component_config.get("instances", [])
        self.hosts = self.env_config.get("hosts", [])

        logger.info(
            "RedisAdapter initialized; instances=%d hosts=%s",
            len(self.instances_cfg), [h.get("name") for h in (self.hosts or [])]
        )

    # ----------------------------------------------------
    # Host connector selection
    # ----------------------------------------------------
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

    # ----------------------------------------------------
    # Exec helpers
    # ----------------------------------------------------
    def _sh_wrap(self, cmd: str) -> str:
        safe = cmd.replace('"', '\\"')
        return f'sh -c "{safe}"'

    def _exec(self, connector, container_id, cmd, timeout=None):
        wrapped = self._sh_wrap(cmd)

        # If we have a container_id, try container execution methods
        if container_id:
            # Check connector type to determine execution method
            from connectors.host_connectors.docker_host_connector import DockerHostConnector
            from connectors.host_connectors.ssh_host_connector import SSHHostConnector
            
            # SSHHostConnector: use docker exec via exec_cmd (SSHHostConnector doesn't implement exec_in_container)
            if isinstance(connector, SSHHostConnector) and hasattr(connector, "exec_cmd"):
                try:
                    # Use docker exec to run command inside container
                    docker_cmd = f'docker exec {container_id} {wrapped}'
                    out = connector.exec_cmd(docker_cmd, timeout=timeout)
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
                        logger.debug(f"Redis adapter: docker exec failed: rc={rc}, stderr='{error_preview}'")
                        return f"[docker exec error: {error_msg}]"
                    return str(out or "")
                except Exception as e:
                    logger.warning(f"Redis adapter: docker exec exception: {e}", exc_info=True)
                    return f"[docker exec error: {e}]"
            
            # DockerHostConnector: use exec_in_container (actually implemented)
            elif isinstance(connector, DockerHostConnector) and hasattr(connector, "exec_in_container"):
                try:
                    # Check if exec_in_container supports timeout parameter
                    import inspect
                    sig = inspect.signature(connector.exec_in_container)
                    if 'timeout' in sig.parameters:
                        out = connector.exec_in_container(container_id, wrapped, timeout=timeout)
                    else:
                        out = connector.exec_in_container(container_id, wrapped)
                    if isinstance(out, dict):
                        result = out.get("stdout") or out.get("output") or ""
                        # Ensure result is always a string
                        return str(result) if result is not None else ""
                    # Ensure out is always a string
                    return str(out) if out is not None else ""
                except Exception as e:
                    return f"[container exec error: {e}]"

        # Fallback host exec
        if hasattr(connector, "exec_cmd"):
            try:
                out = connector.exec_cmd(cmd, timeout=timeout)
                if isinstance(out, dict):
                    return out.get("stdout") or out.get("output") or ""
                return str(out or "")
            except Exception as e:
                return f"[host exec error: {e}]"

        return "[no exec method]"

    # ----------------------------------------------------
    # Per-host collection
    # ----------------------------------------------------
    def collect_for_host(self, host_info, connector):
        findings = {
            "type": "redis",
            "discovered": [],
            "instances": [],
            "collection_mode": "current"  # Redis adapter currently only supports current mode
        }

        # Map docker containers (handle both SSHHostConnector dict format and DockerHostConnector list format)
        docker_map = {}
        if hasattr(connector, "list_containers"):
            try:
                containers_result = connector.list_containers()
                logger.info(f"Redis adapter: list_containers returned: type={type(containers_result).__name__}")
                if isinstance(containers_result, dict):
                    logger.info(f"Redis adapter: list_containers dict keys: {list(containers_result.keys())}")
                    logger.info(f"Redis adapter: list_containers ok={containers_result.get('ok')}, containers count={len(containers_result.get('containers', []))}")
                
                # Handle SSHHostConnector format: dict with "containers" key and "ok" key
                if isinstance(containers_result, dict):
                    if "containers" in containers_result:
                        containers_list = containers_result["containers"]
                        ok = containers_result.get("ok", True)
                        if not ok:
                            error = containers_result.get("error", "Unknown error")
                            logger.warning(f"Redis adapter: list_containers returned ok=False: {error}")
                        logger.info(f"Redis adapter: Processing {len(containers_list)} containers from list_containers (ok={ok})")
                        for idx, c_dict in enumerate(containers_list):
                            logger.debug(f"Redis adapter: Container {idx}: keys={list(c_dict.keys()) if isinstance(c_dict, dict) else 'not a dict'}")
                            # SSHHostConnector returns lowercase keys: "id", "names" (plural, comma-separated)
                            cid = c_dict.get("id")
                            names = c_dict.get("names") or c_dict.get("name")  # Try both "names" and "name"
                            logger.debug(f"Redis adapter: Container {idx}: id={cid}, names={names}")
                            if names and cid:
                                # Handle comma-separated names
                                name_list = [n.strip() for n in str(names).split(",")]
                                for name in name_list:
                                    if name:
                                        clean_name = name.lstrip('/')
                                        docker_map[clean_name] = cid
                                        if name != clean_name:
                                            docker_map[name] = cid
                                        logger.info(f"Redis adapter: Mapped container '{clean_name}' (from '{name}') -> {cid[:12]}")
                            else:
                                logger.warning(f"Redis adapter: Container {idx} missing id or names: id={cid}, names={names}")
                    else:
                        logger.warning(f"Redis adapter: list_containers returned dict without 'containers' key. Keys: {list(containers_result.keys())}")
                
                # Handle DockerHostConnector format: List[Container] with .name and .id
                elif isinstance(containers_result, list):
                    logger.info(f"Redis adapter: list_containers returned {len(containers_result)} containers (type: list)")
                    for c in containers_result:
                        if hasattr(c, "name") and hasattr(c, "id"):
                            name = c.name
                            cid = str(c.id)
                            clean_name = name.lstrip('/')
                            docker_map[clean_name] = cid
                            if name != clean_name:
                                docker_map[name] = cid
                            logger.info(f"Redis adapter: Mapped container '{clean_name}' (from '{name}') -> {cid[:12]}")
                else:
                    logger.warning(f"Redis adapter: list_containers returned unexpected type: {type(containers_result)}")
                
                logger.info(f"Redis adapter: Container map built with {len(docker_map)} entries: {list(docker_map.keys())}")
            except Exception as e:
                logger.warning(f"Redis adapter: Failed to map containers: {e}", exc_info=True)

        # Default instance when none configured
        instances_cfg = self.instances_cfg or [{"name": host_info.get("name"), "container": None}]

        for inst in instances_cfg:
            name = inst.get("name")
            container_name = inst.get("container")
            container_id = docker_map.get(container_name) if container_name else None
            
            # If container_name is specified but container_id is None, provide helpful error
            if container_name and container_id is None:
                available_containers = list(docker_map.keys()) if docker_map else []
                error_msg = f"Container '{container_name}' specified in YAML but not found. Available containers: {', '.join(available_containers) if available_containers else 'none'}"
                logger.warning(f"Redis adapter: {error_msg}")
                entry = {
                    "name": name,
                    "container": container_name,
                    "ping": None,
                    "version": None,
                    "server": {},
                    "memory": {},
                    "clients": {},
                    "keyspace": {},
                    "persistence": {},
                    "replication": {},
                    "cpu": {},
                    "metrics": {},
                    "errors": [error_msg]
                }
                findings["discovered"].append(name)
                findings["instances"].append(entry)
                continue
            
            # Debug logging for container mapping
            if container_name:
                if container_id:
                    logger.info(f"Redis adapter: Found container '{container_name}' -> {container_id[:12]}")

            entry = {
                "name": name,
                "container": container_name,
                "ping": None,
                "version": None,
                "server": {},
                "memory": {},
                "clients": {},
                "keyspace": {},
                "persistence": {},
                "replication": {},
                "cpu": {},
                "metrics": {},
                "errors": []
            }

            redis_port = inst.get("port", 6379)  # Read port from YAML, default 6379

            def run_redis_cli(args: str):
                # Use -p flag if port is specified (even if default)
                cmd = f"{DEFAULT_REDIS_CLI} -p {redis_port} {args}"
                return self._exec(connector, container_id, cmd).strip()

            logger.debug(f"Redis adapter: Starting liveness check for instance '{name}' on port {redis_port}")

            # ------------------------------------------------------------------
            # LIVENESS: Container-aware checks
            # ------------------------------------------------------------------
            try:
                # Detect if we're in a Docker container context
                # For SSHHostConnector, we can still have containers (Docker on remote VM)
                from connectors.host_connectors.docker_host_connector import DockerHostConnector
                from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                is_docker_container = container_id is not None and isinstance(connector, DockerHostConnector)
                is_ssh_with_container = container_id is not None and isinstance(connector, SSHHostConnector)
                
                if is_docker_container or is_ssh_with_container:
                    # ============================================================
                    # DOCKER CONTAINER: Use container-aware checks
                    # ============================================================
                    logger.debug(f"Redis adapter: Using Docker container-aware liveness checks for '{name}' (connector: {type(connector).__name__})")
                    
                    # 1. Check port accessibility
                    port_check_passed = False
                    if is_ssh_with_container:
                        # For SSH-accessed containers, check port on remote host
                        try:
                            port_cmd = f"timeout 2 bash -c '</dev/tcp/127.0.0.1/{redis_port}' 2>&1 || nc -z -w2 127.0.0.1 {redis_port} 2>&1 || echo 'PORT_CHECK_FAILED'"
                            port_out = connector.exec_cmd(port_cmd)
                            if isinstance(port_out, dict):
                                port_str = port_out.get("stdout", "").strip()
                                port_rc = port_out.get("rc", 1)
                                logger.debug(f"Redis adapter: Port {redis_port} check command output: stdout='{port_str[:200]}', rc={port_rc}")
                            else:
                                port_str = str(port_out).strip()
                                logger.debug(f"Redis adapter: Port {redis_port} check command output: '{port_str[:200]}'")
                            
                            port_str_lower = port_str.lower()
                            # Empty output or no error indicators = success
                            if port_str == "" or (port_str_lower != "" and "port_check_failed" not in port_str_lower and "refused" not in port_str_lower and "failed" not in port_str_lower):
                                port_check_passed = True
                                logger.debug(f"Redis adapter: Port {redis_port} check passed on remote host")
                            else:
                                logger.debug(f"Redis adapter: Port {redis_port} not accessible on remote host: {port_str[:100]}")
                        except Exception as e:
                            logger.debug(f"Redis adapter: Remote port check (SSH) failed: {e}")
                    else:
                        # For local DockerHostConnector, use socket check
                        try:
                            import socket
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.settimeout(2)
                            result = sock.connect_ex(('127.0.0.1', redis_port))
                            sock.close()
                            if result == 0:
                                port_check_passed = True
                                logger.debug(f"Redis adapter: Port {redis_port} check passed from host (socket)")
                            else:
                                logger.debug(f"Redis adapter: Port {redis_port} not accessible from host (socket result: {result})")
                        except Exception as e:
                            logger.debug(f"Redis adapter: Host port check (socket) failed: {e}")
                    
                    # 2. Optionally check /proc/1/status to verify main process is redis
                    process_check_passed = False
                    try:
                        proc_status_cmd = "cat /proc/1/status 2>/dev/null | grep -i '^Name:' | awk '{print $2}' || echo 'PROC_CHECK_FAILED'"
                        proc_out = self._exec(connector, container_id, proc_status_cmd)
                        proc_str = str(proc_out).lower().strip()
                        if "proc_check_failed" not in proc_str and "redis" in proc_str:
                            process_check_passed = True
                            logger.debug(f"Redis adapter: Process check passed (/proc/1/status shows redis)")
                    except Exception as e:
                        logger.debug(f"Redis adapter: Process check (/proc/1/status) failed: {e}")
                    
                    # 3. Try PING directly (redis-cli should be available in Redis containers)
                    entry["ping"] = run_redis_cli("PING")
                    ping_str = str(entry["ping"] or "").lower().strip()
                    ping_succeeded = "pong" in ping_str
                    
                    # Liveness check: At least one of port check, process check, or PING should succeed
                    if not port_check_passed and not process_check_passed and not ping_succeeded:
                        # All checks failed - determine the best error message
                        if "connection refused" in ping_str or "could not connect" in ping_str:
                            raise RuntimeError(f"redis port {redis_port} not reachable from host and PING failed")
                        elif ping_str.startswith("[container exec error") or ping_str.startswith("[host exec error"):
                            raise RuntimeError(f"redis-cli not available in container and port {redis_port} not accessible from host")
                        else:
                            raise RuntimeError(f"redis liveness check failed: port={port_check_passed}, process={process_check_passed}, ping={ping_succeeded}")
                    
                    # If PING failed but port/process checks passed, log warning but continue
                    if not ping_succeeded:
                        logger.warning(f"Redis adapter: PING failed but port/process checks passed for '{name}', continuing anyway")
                    
                else:
                    # ============================================================
                    # NON-DOCKER (SSH/Local): Use standard checks
                    # ============================================================
                    logger.debug(f"Redis adapter: Using standard liveness checks for '{name}' (non-Docker)")
                    
                    # Check redis-cli presence - use which as fallback if command -v fails
                    which_out = self._exec(connector, container_id, f"command -v {DEFAULT_REDIS_CLI} 2>&1 || which {DEFAULT_REDIS_CLI} 2>&1 || echo 'NOT_FOUND'")
                    which_str = str(which_out).strip()
                    which_lower = which_str.lower()
                    
                    # Check for various failure indicators
                    if (not which_str or 
                        which_str == "NOT_FOUND" or
                        which_out.startswith("[host exec error") or 
                        which_out.startswith("[container exec error") or
                        which_out.startswith("[no exec method") or
                        "not found" in which_lower or
                        "no exec method" in which_lower or
                        len(which_str) == 0 or
                        (not which_str.startswith("/") and not which_str.startswith("[") and len(which_str) < 3)):
                        # Binary not found - check if redis process exists as fallback
                        ps_cmd = "ps aux | grep -i '[r]edis' | grep -v grep"
                        ps_out = self._exec(connector, container_id, ps_cmd)
                        if not ps_out.strip() or ps_out.startswith("[host exec error") or ps_out.startswith("[container exec error"):
                            raise RuntimeError(f"{DEFAULT_REDIS_CLI} not installed or not in PATH and no redis process found")
                        else:
                            # Process exists but binary not found - might be a path issue
                            raise RuntimeError(f"{DEFAULT_REDIS_CLI} not found in PATH (redis process exists)")
                    
                    # Try PING with port
                    entry["ping"] = run_redis_cli("PING")
                    ping_str = str(entry["ping"] or "").lower().strip()
                    
                    # Check if ping succeeded
                    if "pong" not in ping_str:
                        # Check for connection errors
                        if ("connection refused" in ping_str or 
                            "could not connect" in ping_str or 
                            "connection error" in ping_str or
                            "no connection" in ping_str or
                            "refused" in ping_str or
                            "error connecting" in ping_str):
                            # Port might be wrong or service not running - try port check
                            port_cmd = f"timeout 2 bash -c 'echo > /dev/tcp/127.0.0.1/{redis_port}' 2>&1 || nc -z -w2 127.0.0.1 {redis_port} 2>&1 || echo 'PORT_CHECK_FAILED'"
                            port_out = self._exec(connector, container_id, port_cmd)
                            port_str = str(port_out).lower()
                            if "port_check_failed" in port_str or "refused" in port_str or "failed" in port_str:
                                # Check if redis process exists
                                ps_cmd = "ps aux | grep -i '[r]edis' | grep -v grep"
                                ps_out = self._exec(connector, container_id, ps_cmd)
                                if not ps_out.strip() or ps_out.startswith("[host exec error") or ps_out.startswith("[container exec error"):
                                    raise RuntimeError(f"redis port {redis_port} not reachable and no redis process found")
                                else:
                                    raise RuntimeError(f"redis port {redis_port} not reachable (process exists but port closed)")
                            raise RuntimeError(f"redis port {redis_port} not reachable: {entry['ping']}")
                        # Other error - might be auth or other issue, but still report it
                        raise RuntimeError(f"redis ping failed: {entry['ping']}")
                        
            except Exception as e:
                error_msg = f"liveness: {e}"
                logger.warning(f"Redis adapter: Liveness check failed for '{name}': {error_msg}")
                entry["errors"].append(error_msg)
                findings["discovered"].append(name)
                findings["instances"].append(entry)
                continue

            try:
                # PING
                # Already pinged in liveness; proceed with INFO
                for section in ["server", "memory", "clients", "keyspace", "persistence", "replication", "cpu", "stats"]:
                    try:
                        logger.debug(f"Redis adapter: Executing INFO {section} command")
                        out = run_redis_cli(f"INFO {section}")
                        logger.info(f"Redis adapter: INFO {section} returned {len(out) if out else 0} chars. First 200: {out[:200] if out else 'None'}")
                        # Check if output is an error message
                        if out and not out.startswith("[") and "error" not in out.lower()[:50]:
                            # Log raw output for debugging (first 200 chars)
                            logger.debug(f"Redis adapter: INFO {section} raw output (first 200 chars): {out[:200]}")
                        parsed = _parse_redis_info(out)
                        parsed_section = parsed.get(section, {})
                        entry[section] = parsed_section
                        logger.info(f"Redis adapter: INFO {section} parsed {len(parsed_section)} fields: {list(parsed_section.keys())[:5]}")
                        if section == "server":
                            entry["version"] = entry[section].get("redis_version")
                        else:
                            # Only log warnings if parsing failed (empty section) or output is an error
                            if not parsed_section:
                                if out and out.startswith("["):
                                    logger.warning(f"Redis adapter: INFO {section} returned error: {out[:200]}")
                                elif not out or len(out.strip()) == 0:
                                    logger.warning(f"Redis adapter: INFO {section} returned empty output")
                                else:
                                    logger.warning(f"Redis adapter: INFO {section} returned non-error but parsing resulted in empty section. Output: {out[:200]}")
                    except Exception as e:
                        logger.warning(f"Redis adapter: Error collecting INFO {section}: {e}", exc_info=True)
                        entry["errors"].append(f"{section} info: {e}")
                
                # Fallback: Extract version from logs if not found in INFO
                if not entry.get("version"):
                    try:
                        # Check if we have logs already collected (they might be collected later, but try anyway)
                        logs_dict = entry.get("logs", {})
                        if isinstance(logs_dict, dict):
                            # Check redis-server.log first (most likely to have version info)
                            for log_name in ["redis-server.log", "docker_logs"]:
                                log_entry = logs_dict.get(log_name)
                                if isinstance(log_entry, dict):
                                    log_content = log_entry.get("content", "")
                                elif isinstance(log_entry, str):
                                    log_content = log_entry
                                else:
                                    continue
                                
                                if log_content:
                                    # Look for "Redis version=X.Y.Z" pattern
                                    version_match = re.search(r'Redis\s+version[=\s]+(\d+\.\d+\.\d+)', log_content, re.IGNORECASE)
                                    if version_match:
                                        entry["version"] = version_match.group(1)
                                        logger.debug(f"Redis adapter: Extracted version '{entry['version']}' from {log_name} logs")
                                        break
                    except Exception as e:
                        logger.debug(f"Redis adapter: Failed to extract version from logs: {e}")
                
                # Fallback 2: Try redis-cli --version command
                if not entry.get("version"):
                    try:
                        version_out = run_redis_cli("--version")
                        if version_out and not version_out.startswith("[") and "redis" in version_out.lower():
                            version_match = re.search(r'redis[_-]?cli\s+.*?(\d+\.\d+\.\d+)', version_out, re.IGNORECASE)
                            if version_match:
                                entry["version"] = version_match.group(1)
                                logger.debug(f"Redis adapter: Extracted version '{entry['version']}' from redis-cli --version")
                    except Exception as e:
                        logger.debug(f"Redis adapter: Failed to extract version from redis-cli --version: {e}")

                # ---- METRICS COLLECTION ----
                try:
                    metrics = {}
                    
                    # 1. Used memory (from memory section)
                    memory = entry.get("memory", {})
                    if memory:
                        used_memory = memory.get("used_memory")
                        used_memory_human = memory.get("used_memory_human")
                        if used_memory:
                            try:
                                # Convert bytes to MB
                                metrics["used_memory_mb"] = round(int(used_memory) / (1024 * 1024), 2)
                            except (ValueError, TypeError):
                                pass
                        if used_memory_human:
                            metrics["used_memory_human"] = used_memory_human
                        
                        # Memory stats
                        max_memory = memory.get("maxmemory")
                        if max_memory and max_memory != "0":
                            try:
                                metrics["max_memory_mb"] = round(int(max_memory) / (1024 * 1024), 2)
                                if used_memory:
                                    usage_percent = (int(used_memory) / int(max_memory)) * 100
                                    metrics["memory_usage_percent"] = round(usage_percent, 2)
                            except (ValueError, TypeError):
                                pass
                    
                    # 2. Evicted keys (from stats section)
                    stats = entry.get("stats", {})
                    if stats:
                        evicted_keys = stats.get("evicted_keys")
                        if evicted_keys:
                            try:
                                metrics["evicted_keys"] = int(evicted_keys)
                            except (ValueError, TypeError):
                                pass
                        
                        # 3. Hit/miss ratio (from stats section)
                        keyspace_hits = stats.get("keyspace_hits")
                        keyspace_misses = stats.get("keyspace_misses")
                        if keyspace_hits is not None and keyspace_misses is not None:
                            try:
                                hits = int(keyspace_hits)
                                misses = int(keyspace_misses)
                                total = hits + misses
                                if total > 0:
                                    metrics["hit_ratio"] = round((hits / total) * 100, 2)
                                    metrics["miss_ratio"] = round((misses / total) * 100, 2)
                                metrics["keyspace_hits"] = hits
                                metrics["keyspace_misses"] = misses
                            except (ValueError, TypeError):
                                pass
                        
                        # Additional useful stats
                        total_commands = stats.get("total_commands_processed")
                        if total_commands:
                            try:
                                metrics["total_commands_processed"] = int(total_commands)
                            except (ValueError, TypeError):
                                pass
                    
                    # 4. Connected clients
                    clients = entry.get("clients", {})
                    if clients:
                        connected_clients = clients.get("connected_clients")
                        if connected_clients:
                            try:
                                metrics["connected_clients"] = int(connected_clients)
                            except (ValueError, TypeError):
                                pass
                    
                    # 5. Keyspace size (total keys)
                    keyspace = entry.get("keyspace", {})
                    if keyspace:
                        total_keys = 0
                        for db_name, db_data in keyspace.items():
                            if isinstance(db_data, dict):
                                keys = db_data.get("keys")
                                if keys:
                                    try:
                                        total_keys += int(keys)
                                    except (ValueError, TypeError):
                                        pass
                        if total_keys > 0:
                            metrics["total_keys"] = total_keys
                    
                    # Store metrics
                    if metrics:
                        entry["metrics"] = metrics
                        
                except Exception as e:
                    logger.debug("Failed to collect Redis metrics: %s", e, exc_info=True)
                    # Metrics are optional - don't fail collection

                # ---- LOG COLLECTION ----
                # Redis typically logs to stdout/stderr (Docker logs)
                # Also check for log files if configured
                logs_dict = {}
                try:
                    # First, try to get Docker logs (most common for Redis)
                    if container_id:
                        from connectors.host_connectors.docker_host_connector import DockerHostConnector
                        from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                        
                        if isinstance(connector, DockerHostConnector):
                            logger.info(f"Collecting Redis logs from Docker for container {container_id}")
                            docker_logs = connector.get_container_logs(container_id, tail=200)
                            if docker_logs and not docker_logs.startswith("[docker"):
                                logs_dict["docker_logs"] = {
                                    "content": docker_logs,
                                    "type": "current",
                                    "collection_mode": "current",
                                    "source": "docker_logs",
                                    "line_count": len(docker_logs.splitlines())
                                }
                                logger.info(f"✅ Successfully collected Redis logs from Docker ({len(docker_logs)} chars, {len(docker_logs.splitlines())} lines)")
                            else:
                                logger.debug(f"Docker logs returned error or empty: {docker_logs[:100] if docker_logs else 'None'}")
                        elif isinstance(connector, SSHHostConnector):
                            # For SSH-accessed containers, use docker logs command
                            # Try container_id first, fallback to container_name if container_id is None
                            container_to_use = container_id or container_name
                            if container_to_use:
                                logger.info(f"Collecting Redis logs from Docker (via SSH) for container {container_to_use}")
                                try:
                                    docker_logs_cmd = f"timeout 5 docker logs --tail 200 {container_to_use} 2>&1"
                                    docker_logs_out = self._exec(connector, None, docker_logs_cmd, timeout=5)
                                    logger.debug(f"Redis adapter: docker logs command output (first 200 chars): {str(docker_logs_out)[:200] if docker_logs_out else 'None'}")
                                    if docker_logs_out and not docker_logs_out.startswith("[host exec error") and not docker_logs_out.startswith("[container exec error"):
                                        logs_dict["docker_logs"] = {
                                            "content": docker_logs_out,
                                            "type": "current",
                                            "collection_mode": "current",
                                            "source": "docker_logs",
                                            "line_count": len(docker_logs_out.splitlines())
                                        }
                                        logger.info(f"✅ Successfully collected Redis logs from Docker via SSH ({len(docker_logs_out)} chars, {len(docker_logs_out.splitlines())} lines)")
                                    else:
                                        logger.warning(f"Redis adapter: Docker logs via SSH returned error or empty. Output: {docker_logs_out[:200] if docker_logs_out else 'None'}")
                                except Exception as e:
                                    logger.warning(f"Redis adapter: Failed to collect Docker logs via SSH: {e}", exc_info=True)
                            else:
                                logger.warning(f"Redis adapter: Cannot collect Docker logs - both container_id and container_name are None")
                    
                    # Also try to find Redis log files (if configured)
                    log_paths = [
                        "/var/log/redis/redis-server.log",
                        "/var/log/redis.log",
                        "/usr/local/var/log/redis.log",
                        "/opt/redis/logs/redis.log"
                    ]
                    
                    for log_path in log_paths:
                        try:
                            # Check if file exists
                            file_check = self._exec(
                                connector, container_id,
                                f"test -f {log_path} && echo 'exists' || echo 'missing'"
                            )
                            if "missing" in (file_check or ""):
                                continue
                            
                            # Tail the log file
                            tail_out = self._exec(
                                connector, container_id,
                                f"tail -n 200 {log_path} 2>/dev/null || true"
                            )
                            
                            # Check if we got valid output (not error message, not empty)
                            is_error = tail_out and (
                                tail_out.startswith("[container exec error") or
                                tail_out.startswith("[host exec error") or
                                tail_out.startswith("[docker-host exec error") or
                                tail_out.startswith("[docker not available") or
                                tail_out.startswith("[no exec method]")
                            )
                            
                            if tail_out and not is_error and tail_out.strip():
                                log_name = log_path.split("/")[-1]  # Get filename
                                logs_dict[log_name] = {
                                    "content": tail_out,
                                    "type": "current",
                                    "collection_mode": "current",
                                    "source": "file",
                                    "line_count": len(tail_out.splitlines())
                                }
                                logger.info(f"✅ Successfully collected Redis logs from {log_path} ({len(tail_out)} chars, {len(tail_out.splitlines())} lines)")
                                break  # Found a log file, no need to check others
                        except Exception as e:
                            logger.debug(f"Exception while checking {log_path}: {e}")
                            continue
                    
                    # Set logs dict (UI expects logs to be a dict where each key is a log name)
                    entry["logs"] = logs_dict
                    
                    # Post-processing: Try to extract version from logs if still not found
                    if not entry.get("version") and logs_dict:
                        try:
                            import re
                            # Check redis-server.log first (most likely to have version info)
                            for log_name in ["redis-server.log", "docker_logs"]:
                                log_entry = logs_dict.get(log_name)
                                if isinstance(log_entry, dict):
                                    log_content = log_entry.get("content", "")
                                elif isinstance(log_entry, str):
                                    log_content = log_entry
                                else:
                                    continue
                                
                                if log_content:
                                    # Look for "Redis version=X.Y.Z" pattern
                                    version_match = re.search(r'Redis\s+version[=\s]+(\d+\.\d+\.\d+)', log_content, re.IGNORECASE)
                                    if version_match:
                                        entry["version"] = version_match.group(1)
                                        logger.debug(f"Redis adapter: Extracted version '{entry['version']}' from {log_name} logs (post-processing)")
                                        break
                        except Exception as e:
                            logger.debug(f"Redis adapter: Failed to extract version from logs in post-processing: {e}")
                    
                    if not logs_dict:
                        logger.warning(f"No Redis logs collected for instance {name} (container_id={container_id})")
                        
                except Exception as e:
                    logger.warning(f"Failed to collect Redis logs: {e}")
                    entry["logs"] = {}

            except Exception as e:
                entry["errors"].append(f"fatal: {e}")
                entry["errors"].append(traceback.format_exc())

            findings["discovered"].append(name)
            findings["instances"].append(entry)

        return findings

    # ----------------------------------------------------
    # Backwards-compatible collect()
    # ----------------------------------------------------
    def collect(self):
        results = {}
        for h in (self.hosts or []):
            name = h.get("name", "unknown-host")
            connector = self._make_host_connector(h)
            results[name] = self.collect_for_host(h, connector)
        return results