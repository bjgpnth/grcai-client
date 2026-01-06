# connectors/adapters/nodejs_adapter.py

import logging
import traceback
import json
import re
import os
from connectors.base_connector import BaseConnector
from connectors.historical import HistoricalLogCollectionMixin

logger = logging.getLogger("grcai.adapters.nodejs")

DEFAULT_APP_DIR = "/app"
DEFAULT_NODE_BIN = "node"
DEFAULT_NPM_BIN = "npm"


class NodeJSAdapter(BaseConnector, HistoricalLogCollectionMixin):

    def __init__(self, name=None, env_config=None, issue_time=None, component_config=None, **kwargs):
        super().__init__(name or "nodejs", issue_time, component_config)

        self.env_config = env_config or {}
        self.component_config = component_config or {}

        # Get instances from component config (host-centric: no hosts list in component_config)
        self.instances_cfg = component_config.get("instances", [])

        logger.info(
            "NodeJSAdapter initialized; instances=%d",
            len(self.instances_cfg)
        )

    # ------------------------------------------------------------
    # Build correct host connector (docker, ssh, local)
    # ------------------------------------------------------------
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

    # ------------------------------------------------------------
    # Execute inside container or host
    # ------------------------------------------------------------
    def _exec(self, connector, container_id, cmd, timeout=None):
        """
        Execute a command: prefer container exec, fallback to host exec.
        Returns stdout string or error message string.
        """
        escaped = cmd.replace('"', '\\"')
        wrapped = f'sh -c "{escaped}"'

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
                        return f"[docker exec error: {error_msg}]"
                    return str(out or "")
                except Exception as e:
                    logger.debug(f"NodeJS adapter: docker exec exception: {e}", exc_info=True)
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
                        return out.get("stdout") or out.get("output") or ""
                    return str(out or "")
                except Exception as e:
                    return f"[container exec error: {e}]"

            # Fallback: if host connector can run exec_cmd, attempt `docker exec <id> sh -c "<cmd>"`
            elif hasattr(connector, "exec_cmd"):
                try:
                    docker_cmd = f"docker exec {container_id} {wrapped}"
                    out = connector.exec_cmd(docker_cmd, timeout=timeout)
                    if isinstance(out, dict):
                        return out.get("stdout") or out.get("output") or ""
                    return str(out or "")
                except Exception as e:
                    return f"[docker exec error: {e}]"

        # No container_id: execute on host
        if hasattr(connector, "exec_cmd"):
            try:
                out = connector.exec_cmd(cmd, timeout=timeout)
                if isinstance(out, dict):
                    return out.get("stdout") or out.get("output") or ""
                return str(out or "")
            except Exception as e:
                return f"[host exec error: {e}]"

        return "[no exec method]"

    # ------------------------------------------------------------
    # Read file from container or host
    # ------------------------------------------------------------
    def _read_file(self, connector, container_id, path):
        """Read a file from container or host"""
        if container_id and hasattr(connector, "read_file_in_container"):
            try:
                out = connector.read_file_in_container(container_id, path)
                if isinstance(out, dict):
                    return out.get("stdout", out.get("output", "")) or ""
                return str(out or "")
            except Exception as e:
                logger.debug("read_file_in_container error: %s", e)
                # Fallback to exec cat
                return self._exec(connector, container_id, f"cat {path}")

        if hasattr(connector, "read_file"):
            try:
                out = connector.read_file(path)
                if isinstance(out, dict):
                    return out.get("stdout", out.get("output", "")) or ""
                return str(out or "")
            except Exception as e:
                logger.debug("read_file error: %s", e)
                return self._exec(connector, container_id, f"cat {path}")

        return self._exec(connector, container_id, f"cat {path}")

    # ------------------------------------------------------------
    # Main collection per host
    # ------------------------------------------------------------
    def collect_for_host(self, host_info, connector):
        findings = {
            "type": "nodejs",
            "discovered": [],
            "instances": [],
            "collection_mode": "historical" if self._is_historical_collection() else "current"
        }
        
        # Add time window metadata if historical collection
        if self._is_historical_collection():
            since, until = self._get_time_window()
            findings["time_window"] = {
                "since": since.isoformat(),
                "until": until.isoformat(),
                "issue_time": self.issue_time.isoformat() if self.issue_time else None
            }

        # Build mapping container-name → id
        docker_map = {}
        if hasattr(connector, "list_containers"):
            try:
                containers_result = connector.list_containers()
                
                # Handle SSHHostConnector format: dict with "containers" key
                if isinstance(containers_result, dict):
                    if containers_result.get("ok") and containers_result.get("containers"):
                        containers_list = containers_result["containers"]
                        for c_dict in containers_list:
                            # SSHHostConnector returns dicts with lowercase keys: "id", "names" (plural, comma-separated)
                            container_id = c_dict.get("id", "")
                            container_names = c_dict.get("names", "") or c_dict.get("name", "")
                            # Handle comma-separated names and remove leading slashes
                            if container_names:
                                for name in str(container_names).split(","):
                                    name = name.strip().lstrip("/")  # Remove leading slash
                                    if name and container_id:
                                        docker_map[name] = container_id
                
                # Handle DockerHostConnector format: List[Container] with .name and .id
                elif isinstance(containers_result, list):
                    for c in containers_result:
                        if hasattr(c, "name") and hasattr(c, "id"):
                            clean_name = str(c.name).lstrip("/")
                            docker_map[clean_name] = str(c.id)
                            if str(c.name) != clean_name:
                                docker_map[str(c.name)] = str(c.id)
            except Exception:
                pass

        for inst in (self.instances_cfg or []):
            inst_name = inst.get("name", "nodejs")
            container_name = inst.get("container")
            container_id = docker_map.get(container_name) if container_name else None

            # If container_name is specified but container_id is None, fail early
            if container_name and container_id is None:
                available_containers = list(docker_map.keys()) if docker_map else []
                error_msg = f"Container '{container_name}' specified in YAML but not found. Available containers: {', '.join(available_containers) if available_containers else 'none'}"
                logger.warning(f"NodeJS adapter: {error_msg}")
                entry = {
                    "name": inst_name,
                    "container": container_name,
                    "version": None,
                    "npm_version": None,
                    "processes": [],
                    "package_json": {},
                    "dependencies": {},
                    "env_vars": {},
                    "listening_ports": [],
                    "memory_usage": None,
                    "cpu_usage": None,
                    "logs": {},
                    "error_logs": {},
                    "metrics": {},
                    "errors": [error_msg]
                }
                findings["discovered"].append(inst_name)
                findings["instances"].append(entry)
                continue

            entry = {
                "name": inst_name,
                "container": container_name,
                "version": None,
                "npm_version": None,
                "processes": [],
                "package_json": {},
                "dependencies": {},
                "env_vars": {},
                "listening_ports": [],
                "memory_usage": None,
                "cpu_usage": None,
                "logs": {},
                "error_logs": {},
                "metrics": {},
                "errors": []
            }

            # ------------------------------------------------------------------
            # LIVENESS: Container-aware checks
            # ------------------------------------------------------------------
            try:
                # Detect if we're in a Docker container context
                from connectors.host_connectors.docker_host_connector import DockerHostConnector
                from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                # Check if this is a Docker container (either DockerHostConnector or SSHHostConnector with container_id)
                is_docker_container = container_id is not None and (
                    isinstance(connector, DockerHostConnector) or 
                    isinstance(connector, SSHHostConnector)
                )
                logger.debug(f"NodeJS adapter: is_docker_container={is_docker_container} (container_id={container_id}, connector_type={type(connector).__name__})")
                
                if is_docker_container:
                    # ============================================================
                    # DOCKER CONTAINER: Use container-aware checks
                    # ============================================================
                    logger.debug(f"NodeJS adapter: Using Docker container-aware liveness checks for '{inst_name}'")
                    
                    # 0. First, verify container is actually running (not just exists)
                    container_running = False
                    container_status = "unknown"
                    container_id_str = str(container_id) if container_id else ""
                    try:
                        # DockerHostConnector: use Docker SDK
                        if isinstance(connector, DockerHostConnector) and hasattr(connector, "client") and connector.client:
                            try:
                                container_info = connector.client.containers.get(container_id_str)
                                if container_info is None:
                                    raise RuntimeError(f"nodejs container {container_id_str[:12]} not found")
                                container_info.reload()
                                if not hasattr(container_info, 'attrs') or not isinstance(container_info.attrs, dict):
                                    raise RuntimeError(f"nodejs container {container_id_str[:12]} has invalid attrs")
                                state = container_info.attrs.get('State', {})
                                if not isinstance(state, dict):
                                    raise RuntimeError(f"nodejs container {container_id_str[:12]} has invalid State")
                                container_status = state.get('Status', 'unknown')
                                container_running = container_status == "running"
                                logger.debug(f"NodeJS adapter: Container {container_id_str[:12]} status: {container_status}")
                                if not container_running:
                                    logger.warning(f"NodeJS adapter: Container {container_id_str[:12]} is not running (status: {container_status})")
                            except Exception as e:
                                error_str = str(e)
                                logger.debug(f"NodeJS adapter: Failed to get container status: {e}")
                                if "not found" in error_str.lower() or "no such container" in error_str.lower():
                                    raise RuntimeError(f"nodejs container {container_id_str[:12]} not found: {error_str}")
                                container_running = False
                        # SSHHostConnector or other: use docker inspect command
                        elif isinstance(connector, SSHHostConnector) and hasattr(connector, "exec_cmd"):
                            try:
                                inspect_cmd = f"docker inspect --format '{{{{.State.Status}}}}' {container_id_str} 2>/dev/null || echo 'NOT_FOUND'"
                                status_output = connector.exec_cmd(inspect_cmd)
                                if isinstance(status_output, dict):
                                    status_output = status_output.get("stdout", "")
                                status_output = str(status_output).strip().lower()
                                
                                if "not_found" in status_output or not status_output:
                                    raise RuntimeError(f"nodejs container {container_id_str[:12]} not found")
                                
                                container_status = status_output
                                container_running = container_status == "running"
                                logger.debug(f"NodeJS adapter: Container {container_id_str[:12]} status: {container_status}")
                                if not container_running:
                                    logger.warning(f"NodeJS adapter: Container {container_id_str[:12]} is not running (status: {container_status})")
                            except RuntimeError as e:
                                raise
                            except Exception as e:
                                logger.debug(f"NodeJS adapter: Container status check (docker inspect) failed: {e}")
                                container_running = False
                        else:
                            raise RuntimeError(f"nodejs Docker client not available")
                    except RuntimeError as e:
                        raise
                    except Exception as e:
                        logger.debug(f"NodeJS adapter: Container status check failed: {e}")
                        container_running = False
                    
                    # Also verify we can actually exec into the container (double-check)
                    if container_running:
                        try:
                            test_exec = self._exec(connector, container_id_str, "echo test", timeout=2)
                            if test_exec.startswith("[container exec error") or test_exec.startswith("[host exec error"):
                                logger.warning(f"NodeJS adapter: Container {container_id_str[:12]} appears running but exec failed: {test_exec[:100]}")
                                container_running = False
                        except Exception as e:
                            logger.debug(f"NodeJS adapter: Exec test failed: {e}")
                            container_running = False
                    
                    if not container_running:
                        status_msg = f"status: {container_status}" if container_status != "unknown" else "container not running"
                        raise RuntimeError(f"nodejs container {container_id_str[:12] if container_id_str else 'unknown'} is not running ({status_msg})")
                    
                    # 1. Check port accessibility from Docker host (if ports configured)
                    expected_ports = inst.get("ports") or []
                    if expected_ports:
                        port_check_passed = False
                        for p in expected_ports:
                            try:
                                if isinstance(connector, SSHHostConnector):
                                    # For SSH-accessed containers, check port on remote host (use localhost on remote host)
                                    port_cmd = f"timeout 2 bash -c '</dev/tcp/127.0.0.1/{p}' 2>&1 || nc -z -w2 127.0.0.1 {p} 2>&1 || echo 'PORT_CHECK_FAILED'"
                                    port_out = connector.exec_cmd(port_cmd)
                                    if isinstance(port_out, dict):
                                        port_str = port_out.get("stdout", "").strip()
                                    else:
                                        port_str = str(port_out).strip()
                                    
                                    if "PORT_CHECK_FAILED" not in port_str and port_str:
                                        port_check_passed = True
                                        logger.debug(f"NodeJS adapter: Port {p} check passed from remote host")
                                        break  # At least one port is reachable
                                    else:
                                        logger.debug(f"NodeJS adapter: Port {p} not accessible from remote host: {port_str[:100]}")
                                else:
                                    # DockerHostConnector: check port on local host
                                    import socket
                                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    sock.settimeout(2)
                                    connect_result = sock.connect_ex(('127.0.0.1', p))
                                    sock.close()
                                    if connect_result == 0:
                                        port_check_passed = True
                                        logger.debug(f"NodeJS adapter: Port {p} check passed from host (socket)")
                                        break  # At least one port is reachable
                                    else:
                                        logger.debug(f"NodeJS adapter: Port {p} not accessible from host (socket result: {connect_result})")
                            except Exception as e:
                                logger.debug(f"NodeJS adapter: Port check failed for port {p}: {e}")
                        
                        if not port_check_passed:
                            raise RuntimeError(f"nodejs ports {expected_ports} not reachable from host")
                    
                    # 2. Optionally check /proc/1/status to verify main process is node
                    process_check_passed = False
                    try:
                        proc_status_cmd = "cat /proc/1/status 2>/dev/null | grep -i '^Name:' | awk '{print $2}' || echo 'PROC_CHECK_FAILED'"
                        proc_out = self._exec(connector, container_id_str, proc_status_cmd)
                        proc_str = str(proc_out).lower().strip()
                        if "proc_check_failed" not in proc_str and ("node" in proc_str or "nodejs" in proc_str):
                            process_check_passed = True
                            logger.debug(f"NodeJS adapter: Process check passed (/proc/1/status shows node/nodejs)")
                    except Exception as e:
                        logger.debug(f"NodeJS adapter: Process check (/proc/1/status) failed: {e}")
                    
                    # If no ports configured and process check failed, that's OK (binary might exist but no app running)
                    # We only fail if ports are configured and none are reachable
                    
                else:
                    # ============================================================
                    # NON-DOCKER (SSH/Local): Use standard checks
                    # ============================================================
                    logger.debug(f"NodeJS adapter: Using standard liveness checks for '{inst_name}' (non-Docker)")
                    
                    # Check node binary presence - try both 'node' and 'nodejs'
                    which_out = self._exec(connector, container_id, "command -v node 2>&1 || command -v nodejs 2>&1 || echo 'NOT_FOUND'", timeout=5)
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
                        # Binary not found - check if node process exists as fallback
                        ps_cmd = "ps aux | grep -i '[n]ode' | grep -v grep"
                        ps_out = self._exec(connector, container_id, ps_cmd, timeout=5)
                        if not ps_out.strip() or ps_out.startswith("[host exec error") or ps_out.startswith("[container exec error"):
                            raise RuntimeError("node binary not found and no node processes found")
                        else:
                            # Process exists but binary not found - might be a path issue
                            raise RuntimeError("node binary not found in PATH (node process exists)")
                    
                    # Binary found - now check if applications are running (optional check)
                    # If ports specified, verify they are listening
                    expected_ports = inst.get("ports") or []
                    if expected_ports:
                        for p in expected_ports:
                            port_cmd = f"timeout 2 bash -c 'echo > /dev/tcp/127.0.0.1/{p}' 2>&1 || nc -z -w2 127.0.0.1 {p} 2>&1 || echo 'PORT_CHECK_FAILED'"
                            port_out = self._exec(connector, container_id, port_cmd, timeout=5)
                            port_str = str(port_out).lower()
                            if "port_check_failed" in port_str or "refused" in port_str or "failed" in port_str or "connection refused" in port_str:
                                raise RuntimeError(f"node port {p} not reachable")
                    # If no ports specified, we don't require processes to be running
                    # (binary exists is sufficient - apps might not be running yet)
                    
            except Exception as e:
                error_msg = f"liveness: {e}"
                logger.warning(f"NodeJS adapter: Liveness check failed for '{inst_name}': {error_msg}")
                entry["errors"].append(error_msg)
                findings["discovered"].append(inst_name)
                findings["instances"].append(entry)
                continue

            app_dir = inst.get("app_dir", DEFAULT_APP_DIR)
            node_bin = inst.get("node_bin_path", DEFAULT_NODE_BIN)
            npm_bin = inst.get("npm_bin_path", DEFAULT_NPM_BIN)
            log_paths = inst.get("log_paths", [])
            package_json_path = inst.get("package_json_path") or f"{app_dir}/package.json"

            try:
                # --- Node.js Version ---
                try:
                    out = self._exec(connector, container_id, f"{node_bin} --version", timeout=10)
                    if out and not out.startswith("["):
                        entry["version"] = out.strip()
                except Exception as e:
                    entry["errors"].append(f"node version: {e}")

                # --- NPM Version ---
                try:
                    out = self._exec(connector, container_id, f"{npm_bin} --version", timeout=10)
                    if out and not out.startswith("["):
                        entry["npm_version"] = out.strip()
                except Exception as e:
                    entry["errors"].append(f"npm version: {e}")

                # --- Running Processes ---
                try:
                    # Find node processes - try multiple approaches
                    processes = []
                    
                    # Method 1: ps aux (standard format)
                    out = self._exec(connector, container_id, "ps aux | grep -E '[n]ode|[n]pm' | grep -v grep | head -20", timeout=10)
                    if out and not out.startswith("["):
                        for line in out.strip().splitlines():
                            if line.strip():
                                parts = line.split()
                                # ps aux format: USER PID %CPU %MEM VSZ RSS TTY STAT START TIME COMMAND
                                # Minimum 11 fields, but be flexible
                                if len(parts) >= 11:
                                    try:
                                        processes.append({
                                            "user": parts[0],
                                            "pid": parts[1],
                                            "cpu": parts[2],
                                            "mem": parts[3],
                                            "command": " ".join(parts[10:])[:200]  # Truncate long commands
                                        })
                                    except (IndexError, ValueError) as e:
                                        logger.debug(f"NodeJS adapter: Failed to parse ps aux line: {line[:100]}, error: {e}")
                                        continue
                    
                    # Method 2: If ps aux didn't work, try ps -ef (alternative format)
                    if not processes:
                        out = self._exec(connector, container_id, "ps -ef | grep -E '[n]ode|[n]pm' | grep -v grep | head -20", timeout=10)
                        if out and not out.startswith("["):
                            for line in out.strip().splitlines():
                                if line.strip():
                                    parts = line.split()
                                    # ps -ef format: UID PID PPID C STIME TTY TIME CMD
                                    # Minimum 8 fields
                                    if len(parts) >= 8:
                                        try:
                                            processes.append({
                                                "user": parts[0],
                                                "pid": parts[1],
                                                "cpu": "N/A",
                                                "mem": "N/A",
                                                "command": " ".join(parts[7:])[:200]
                                            })
                                        except (IndexError, ValueError) as e:
                                            logger.debug(f"NodeJS adapter: Failed to parse ps -ef line: {line[:100]}, error: {e}")
                                            continue
                    
                    # Method 3: Extract PIDs from listening ports as fallback
                    if not processes:
                        # We'll populate this after listening ports are collected
                        # For now, just log that we'll try this fallback
                        logger.debug("NodeJS adapter: No processes found via ps, will try extracting from listening ports")
                    
                        entry["processes"] = processes
                except Exception as e:
                    logger.debug(f"NodeJS adapter: Process collection error: {e}", exc_info=True)
                    entry["errors"].append(f"processes: {e}")

                # --- Package.json ---
                try:
                    # Try multiple locations for package.json
                    package_json_paths = [package_json_path]
                    
                    # Also try to find package.json from running processes
                    for proc in entry.get("processes", []):
                        cmd = proc.get("command", "")
                        # Extract working directory from command if possible
                        # For commands like "node /path/to/app.js", try /path/to/package.json
                        if cmd and ("node" in cmd.lower() or "npm" in cmd.lower()):
                            # Try to extract path from command
                            parts = cmd.split()
                            for part in parts:
                                if part.endswith(".js") and "/" in part:
                                    # Found a JS file path, try its directory
                                    # Use shell dirname command instead of os.path
                                    dirname_cmd = f"dirname {part} 2>/dev/null || echo '.'"
                                    dir_out = self._exec(connector, container_id, dirname_cmd, timeout=5)
                                    if dir_out and not dir_out.startswith("[") and dir_out.strip() and dir_out.strip() != ".":
                                        dir_path = dir_out.strip()
                                        potential_path = f"{dir_path}/package.json"
                                        if potential_path not in package_json_paths:
                                            package_json_paths.append(potential_path)
                    
                    # Also try common locations
                    common_paths = [
                        f"{app_dir}/package.json",
                        "/app/package.json",
                        "/var/www/package.json",
                        "./package.json"
                    ]
                    for path in common_paths:
                        if path not in package_json_paths:
                            package_json_paths.append(path)
                    
                    pkg_content = None
                    pkg_content_stripped = ""
                    found_path = None
                    
                    # Try each path until we find a valid package.json
                    for pkg_path in package_json_paths:
                        try:
                            pkg_content = self._read_file(connector, container_id, pkg_path)
                            pkg_content_stripped = pkg_content.strip() if pkg_content else ""
                            
                            if pkg_content and not pkg_content.startswith("[") and pkg_content_stripped:
                                found_path = pkg_path
                                break
                        except Exception:
                            continue
                    
                    if pkg_content and found_path and pkg_content_stripped:
                        try:
                            # Try to parse the JSON
                            pkg_data = json.loads(pkg_content_stripped)
                            entry["package_json"] = {
                                "name": pkg_data.get("name", ""),
                                "version": pkg_data.get("version", ""),
                                "description": pkg_data.get("description", ""),
                                "main": pkg_data.get("main", ""),
                                "scripts": pkg_data.get("scripts", {}),
                                "source_path": found_path  # Include where we found it
                            }
                            # Extract dependencies
                            deps = {}
                            deps.update(pkg_data.get("dependencies", {}))
                            deps.update(pkg_data.get("devDependencies", {}))
                            entry["dependencies"] = {k: v for k, v in list(deps.items())[:50]}  # Limit to 50
                            logger.debug(f"NodeJS adapter: Found package.json at {found_path}")
                        except json.JSONDecodeError as e:
                            # Check if this is the "empty file" error
                            error_str = str(e)
                            is_empty_error = "Expecting value: line 1 column 1 (char 0)" in error_str
                            
                            # Only log parse errors if:
                            # 1. Content exists (not empty/whitespace)
                            # 2. It's NOT the "empty file" error
                            if pkg_content_stripped and not is_empty_error:
                                logger.debug(f"package.json parse error for {inst_name} at {found_path}: {e} (content: {pkg_content_stripped[:50]})")
                                entry["errors"].append(f"package.json parse: {e}")
                            # If empty or whitespace-only, silently skip (not an error)
                    # If file doesn't exist or is empty, leave package_json as {} (not an error)
                    # This is normal for some Node.js applications
                except Exception as e:
                    # Only log as error if it's a real error (not just file not found)
                    error_msg = str(e).lower()
                    if "no such file" not in error_msg and "not found" not in error_msg:
                        logger.debug(f"package.json read error for {inst_name}: {e}")
                        entry["errors"].append(f"package.json: {e}")
                    # If file doesn't exist, that's fine - leave package_json as {}

                # --- Environment Variables ---
                try:
                    # Get environment variables from Node.js processes
                    # Try to get env from the first Node.js process
                    node_pids = [proc.get("pid") for proc in entry.get("processes", []) if proc.get("pid")]
                    env_vars = {}
                    
                    if node_pids:
                        # Try to get environment from /proc/<pid>/environ (Linux)
                        first_pid = node_pids[0]
                        try:
                            # Try to get all env vars from process, then filter
                            env_cmd = f"cat /proc/{first_pid}/environ 2>/dev/null | tr '\\0' '\\n' | head -50"
                            out = self._exec(connector, container_id, env_cmd, timeout=10)
                            if out and not out.startswith("["):
                                for line in out.strip().splitlines():
                                    if "=" in line:
                                        key, value = line.split("=", 1)
                                        # Filter for Node.js related or common env vars
                                        key_upper = key.upper()
                                        if (any(prefix in key_upper for prefix in ["NODE", "NPM", "PATH", "HOME", "USER", "PWD", "SHELL", "LANG", "TZ"]) or
                                            key_upper.startswith("NODE_") or key_upper.startswith("NPM_")):
                                            # Don't expose sensitive values
                                            if any(sensitive in key_upper for sensitive in ["PASSWORD", "SECRET", "KEY", "TOKEN"]):
                                                env_vars[key] = "***REDACTED***"
                                            else:
                                                env_vars[key] = value[:200]  # Truncate long values
                        except Exception as e:
                            logger.debug(f"Failed to get env from /proc/{first_pid}/environ: {e}")
                    
                    # Fallback: try global env if process-specific failed
                    if not env_vars:
                        out = self._exec(connector, container_id, "env | grep -E '^(NODE|npm|PATH)=' | head -20", timeout=10)
                        if out and not out.startswith("["):
                            for line in out.strip().splitlines():
                                if "=" in line:
                                    key, value = line.split("=", 1)
                                if any(sensitive in key.upper() for sensitive in ["PASSWORD", "SECRET", "KEY", "TOKEN"]):
                                    env_vars[key] = "***REDACTED***"
                                else:
                                        env_vars[key] = value[:200]
                    
                        entry["env_vars"] = env_vars
                except Exception as e:
                    entry["errors"].append(f"env_vars: {e}")

                # --- Listening Ports ---
                try:
                    # First, get Node.js process PIDs
                    node_pids = set()
                    for proc in entry.get("processes", []):
                        pid = proc.get("pid")
                        if pid:
                            node_pids.add(str(pid))
                    
                    # Find listening ports and filter by Node.js PIDs
                    out = self._exec(connector, container_id, "ss -tlnp 2>/dev/null | grep LISTEN || netstat -tlnp 2>/dev/null | grep LISTEN", timeout=10)
                    if out and not out.startswith("["):
                        ports = []
                        extracted_pids = set()  # Track PIDs found from port lines
                        for line in out.strip().splitlines():
                            if "LISTEN" in line:
                                # Check if this port belongs to a Node.js process
                                # Look for PID in the line (format: pid=1234 or users:(("process",pid=1234))
                                is_node_port = False
                                found_pid = None
                                
                                if node_pids:
                                    # Check if any Node.js PID appears in the line
                                    for pid in node_pids:
                                        if f"pid={pid}" in line or f",pid={pid}," in line or f",pid={pid})" in line:
                                            is_node_port = True
                                            found_pid = pid
                                            break
                                else:
                                    # Fallback: check if "node" appears in the line
                                    is_node_port = "node" in line.lower()
                                    # Try to extract PID from the line - handle multiple formats
                                    # Format 1: pid=1234 (from netstat)
                                    pid_match = re.search(r'pid=(\d+)', line)
                                    if not pid_match:
                                        # Format 2: 1234/node (from ss output)
                                        pid_match = re.search(r'(\d+)/node', line)
                                    if not pid_match:
                                        # Format 3: Just a number before /node or space
                                        pid_match = re.search(r'\b(\d+)\s*[/\s]node', line)
                                    if pid_match:
                                        found_pid = pid_match.group(1)
                                        extracted_pids.add(found_pid)
                                        logger.debug(f"NodeJS adapter: Extracted PID {found_pid} from listening port line: {line[:80]}")
                                
                                if is_node_port:
                                    # Extract port number
                                    port_match = re.search(r':(\d+)', line)
                                    if port_match:
                                        port = port_match.group(1)
                                        if port not in [p.get("port") for p in ports]:
                                            ports.append({
                                                "port": port,
                                                "line": line[:100]  # Truncate
                                            })
                        
                        entry["listening_ports"] = ports[:20]  # Limit to 20
                        
                        # Fallback: If we found PIDs from listening ports but no processes, try to get process info
                        if (not entry.get("processes") or len(entry.get("processes", [])) == 0) and extracted_pids:
                            logger.debug(f"NodeJS adapter: Found PIDs from listening ports: {extracted_pids}, attempting to get process info")
                            # Ensure processes list exists
                            if "processes" not in entry:
                                entry["processes"] = []
                            
                            for pid in list(extracted_pids)[:5]:  # Limit to 5 PIDs
                                try:
                                    # BusyBox ps doesn't support -p, so use ps aux and filter by PID
                                    # Try ps aux first (most common)
                                    proc_cmd = f"ps aux | grep -E '^[^ ]+[ ]+{pid}[ ]' | grep -v grep"
                                    proc_out = self._exec(connector, container_id, proc_cmd, timeout=5)
                                    
                                    # If ps aux doesn't work, try ps -o (BusyBox format)
                                    if not proc_out or proc_out.startswith("[") or not proc_out.strip():
                                        proc_cmd = f"ps -o pid,user,comm,args | grep -E '^[ ]*{pid}[ ]' | grep -v grep"
                                        proc_out = self._exec(connector, container_id, proc_cmd, timeout=5)
                                    
                                    if proc_out and not proc_out.startswith("[") and proc_out.strip():
                                        # Handle different ps output formats
                                        lines = proc_out.strip().splitlines()
                                        if lines:
                                            # Get the first matching line
                                            proc_line = lines[0].strip()
                                            parts = proc_line.split(None, 10)  # Allow more parts for flexibility
                                            
                                            if len(parts) >= 2:
                                                # For ps aux: USER PID %CPU %MEM VSZ RSS TTY STAT START TIME COMMAND
                                                # For ps -o: PID USER COMMAND ARGS
                                                # Try to find PID in the parts
                                                proc_pid = None
                                                for i, part in enumerate(parts):
                                                    if part == str(pid):
                                                        proc_pid = part
                                                        break
                                                
                                                if proc_pid or len(parts) >= 2:
                                                    # Determine format based on number of parts
                                                    if len(parts) >= 11:
                                                        # ps aux format
                                                        proc_info = {
                                                            "user": parts[0] if len(parts) > 0 else "unknown",
                                                            "pid": proc_pid or (parts[1] if len(parts) > 1 else pid),
                                                            "cpu": parts[2] if len(parts) > 2 and parts[2].replace(".", "").replace("-", "").isdigit() else "N/A",
                                                            "mem": parts[3] if len(parts) > 3 and parts[3].replace(".", "").replace("-", "").isdigit() else "N/A",
                                                            "command": " ".join(parts[10:]) if len(parts) > 10 else (" ".join(parts[4:]) if len(parts) > 4 else "node")
                                                        }
                                                    else:
                                                        # ps -o format (BusyBox) or minimal format
                                                        # For BusyBox: PID USER COMMAND ARGS
                                                        # The command might be in parts[2] and args in parts[3:]
                                                        command_parts = parts[2:] if len(parts) > 2 else ["node"]
                                                        # Remove duplicate "node" if command starts with it
                                                        if len(command_parts) > 1 and command_parts[0] == "node" and command_parts[1] == "node":
                                                            command_parts = command_parts[1:]
                                                        proc_info = {
                                                            "user": parts[1] if len(parts) > 1 else "unknown",
                                                            "pid": proc_pid or (parts[0] if len(parts) > 0 else pid),
                                                            "cpu": "N/A",  # BusyBox ps doesn't show CPU
                                                            "mem": "N/A",  # BusyBox ps doesn't show memory
                                                            "command": " ".join(command_parts)
                                                        }
                                                    entry["processes"].append(proc_info)
                                                    logger.debug(f"NodeJS adapter: Successfully extracted process info for PID {pid}: {proc_info}")
                                except Exception as e:
                                    logger.debug(f"NodeJS adapter: Failed to get process info for PID {pid}: {e}", exc_info=True)
                except Exception as e:
                    entry["errors"].append(f"listening_ports: {e}")

                # --- Memory/CPU Usage ---
                try:
                    # Try to get memory and CPU from ps aux first (standard Linux)
                    out = self._exec(connector, container_id, "ps aux | grep -E '[n]ode' | grep -v grep | awk '{sum+=$4} END {print sum}'", timeout=10)
                    if out and not out.startswith("[") and out.strip():
                        try:
                            mem_val = float(out.strip())
                            if mem_val > 0:
                                entry["memory_usage"] = mem_val
                        except (ValueError, TypeError):
                            pass
                    
                    out = self._exec(connector, container_id, "ps aux | grep -E '[n]ode' | grep -v grep | awk '{sum+=$3} END {print sum}'", timeout=10)
                    if out and not out.startswith("[") and out.strip():
                        try:
                            cpu_val = float(out.strip())
                            if cpu_val > 0:
                                entry["cpu_usage"] = cpu_val
                        except (ValueError, TypeError):
                            pass
                    
                    # Fallback: Try to get memory from /proc/<pid>/status for each process
                    if entry.get("memory_usage") is None:
                        node_pids = [proc.get("pid") for proc in entry.get("processes", []) if proc.get("pid")]
                        total_mem_kb = 0
                        for pid in node_pids:
                            try:
                                # Get VmRSS (resident set size) from /proc/<pid>/status
                                mem_cmd = f"grep -E '^VmRSS:' /proc/{pid}/status 2>/dev/null | awk '{{print $2}}'"
                                mem_out = self._exec(connector, container_id, mem_cmd, timeout=5)
                                if mem_out and not mem_out.startswith("[") and mem_out.strip().isdigit():
                                    total_mem_kb += int(mem_out.strip())
                            except Exception:
                                continue
                        
                        if total_mem_kb > 0:
                            # Convert KB to MB for consistency
                            entry["memory_usage"] = round(total_mem_kb / 1024, 2)
                except Exception as e:
                    logger.debug(f"NodeJS adapter: Memory/CPU calculation error: {e}", exc_info=True)
                    # Don't add to errors - this is optional

                # --- Logs Collection (historical or current) ---
                # Node.js logs are stored as a dictionary where keys are log file names
                # (e.g., "docker_logs", "app.log") and values contain log metadata
                logs_dict = {}
                try:
                    if self._is_historical_collection():
                        # Historical collection
                        since, until = self._get_time_window()
                        
                        # Strategy 1: Try Docker logs with time filtering (for containers)
                        if container_id:
                            try:
                                from connectors.host_connectors.docker_host_connector import DockerHostConnector
                                if isinstance(connector, DockerHostConnector):
                                    docker_logs = connector.get_container_logs_time_filtered(container_id, since, until)
                                    if docker_logs and not docker_logs.startswith("[docker"):
                                        logs_dict["docker_logs"] = {
                                            "content": docker_logs.strip(),
                                            "type": "historical",
                                            "source": "docker_logs",
                                            "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                                            "collection_mode": "historical",
                                            "line_count": len(docker_logs.splitlines())
                                        }
                                    else:
                                        raise Exception("Docker log filtering failed")
                                else:
                                    raise Exception("Docker log filtering not available")
                            except Exception as e:
                                logger.debug("Docker log filtering failed, using file-based: %s", e)
                                # Fall back to file-based historical collection
                                if log_paths:
                                    node_logs = self._collect_historical_logs(
                                        host_connector=connector,
                                        container_id=container_id,
                                        log_paths=log_paths,
                                        parser_name="syslog",  # Use syslog parser as fallback
                                        since=since,
                                        until=until,
                                        max_lines=10000
                                    )
                                    if node_logs:
                                        # Use the first log path as the key, or "node.log" as default
                                        log_key = log_paths[0].split("/")[-1] if log_paths and isinstance(log_paths, list) else "node.log"
                                        logs_dict[log_key] = {
                                        "content": node_logs.strip(),
                                        "type": "historical",
                                        "source": "file",
                                        "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                                        "collection_mode": "historical",
                                            "line_count": len(node_logs.splitlines())
                                    }
                        else:
                            # Host-based: use file-based historical collection
                            if log_paths:
                                node_logs = self._collect_historical_logs(
                                    host_connector=connector,
                                    container_id=None,
                                    log_paths=log_paths,
                                    parser_name="syslog",
                                    since=since,
                                    until=until,
                                    max_lines=10000
                                )
                                if node_logs:
                                    log_key = log_paths[0].split("/")[-1] if log_paths and isinstance(log_paths, list) else "node.log"
                                    logs_dict[log_key] = {
                                    "content": node_logs.strip(),
                                    "type": "historical",
                                    "source": "file",
                                    "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                                    "collection_mode": "historical",
                                        "line_count": len(node_logs.splitlines())
                                }
                    else:
                        # Current collection
                        if container_id:
                            # Try to get recent docker logs
                            try:
                                from connectors.host_connectors.docker_host_connector import DockerHostConnector
                                if isinstance(connector, DockerHostConnector):
                                    logger.info(f"Collecting Node.js logs from Docker for container {container_id}")
                                    # Get last 200 lines from docker logs
                                    docker_logs = connector.get_container_logs(container_id, tail=200)
                                    if docker_logs and not docker_logs.startswith("[docker"):
                                        logs_dict["docker_logs"] = {
                                            "content": docker_logs.strip(),
                                            "type": "current",
                                            "source": "docker_logs",
                                            "collection_mode": "current",
                                            "line_count": len(docker_logs.splitlines())
                                        }
                                    else:
                                        raise Exception("Docker logs unavailable")
                                else:
                                    raise Exception("Docker logs not available")
                            except Exception as e:
                                logger.debug("Docker logs failed, trying file-based: %s", e)
                                # Fallback to file-based logs
                                if log_paths:
                                    # Try to tail the first log file
                                    first_log = log_paths[0] if isinstance(log_paths, list) else log_paths
                                    log_content = self._exec(connector, container_id, f"tail -n 200 {first_log} 2>/dev/null || echo ''", timeout=10)
                                    if log_content and not log_content.startswith("["):
                                        log_key = first_log.split("/")[-1] if "/" in first_log else first_log
                                        logs_dict[log_key] = {
                                        "content": log_content.strip(),
                                        "type": "current",
                                        "source": "file",
                                        "collection_mode": "current",
                                            "line_count": len(log_content.splitlines())
                                    }
                        else:
                            # Host-based: read from log files
                            if log_paths:
                                for log_path in (log_paths if isinstance(log_paths, list) else [log_paths]):
                                    log_content = self._exec(connector, None, f"tail -n 200 {log_path} 2>/dev/null || echo ''", timeout=10)
                                    if log_content and not log_content.startswith("["):
                                        log_key = log_path.split("/")[-1] if "/" in log_path else log_path
                                        logs_dict[log_key] = {
                                            "content": log_content.strip(),
                                    "type": "current",
                                    "source": "file",
                                    "collection_mode": "current",
                                            "line_count": len(log_content.splitlines())
                                }
                except Exception as e:
                    logger.warning("Failed to collect Node.js logs: %s", e)
                
                # Store logs dictionary (empty if collection failed)
                entry["logs"] = logs_dict

                # --- Error Logs (separate from console logs if configured) ---
                error_log_paths = inst.get("error_log_paths", [])
                if error_log_paths:
                    try:
                        if self._is_historical_collection():
                            since, until = self._get_time_window()
                            error_logs = self._collect_historical_logs(
                                host_connector=connector,
                                container_id=container_id,
                                log_paths=error_log_paths,
                                parser_name="syslog",
                                since=since,
                                until=until,
                                max_lines=5000
                            )
                            entry["error_logs"] = {
                                "content": error_logs.strip(),
                                "type": "historical",
                                "source": "file",
                                "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                                "collection_mode": "historical"
                            }
                        else:
                            error_contents = []
                            for err_path in (error_log_paths if isinstance(error_log_paths, list) else [error_log_paths]):
                                err_content = self._exec(connector, container_id, f"tail -n 100 {err_path} 2>/dev/null || echo ''", timeout=10)
                                if err_content and not err_content.startswith("["):
                                    error_contents.append(f"=== {err_path} ===\n{err_content}")
                            entry["error_logs"] = {
                                "content": "\n\n".join(error_contents).strip(),
                                "type": "current",
                                "source": "file",
                                "collection_mode": "current"
                            }
                    except Exception as e:
                        logger.warning("Failed to collect error logs: %s", e)
                        entry["error_logs"] = {
                            "content": "",
                            "type": "current",
                            "collection_mode": "current"
                        }

            except Exception as e:
                entry["errors"].append(f"fatal: {e}")
                entry["errors"].append(traceback.format_exc())

            # ---- METRICS COLLECTION ----
            try:
                metrics = {}
                
                # Process count
                processes = entry.get("processes", [])
                if processes:
                    metrics["process_count"] = len(processes)
                
                # Memory usage
                memory_usage = entry.get("memory_usage")
                if memory_usage is not None:
                    metrics["memory_usage_percent"] = round(memory_usage, 2)
                
                # CPU usage
                cpu_usage = entry.get("cpu_usage")
                if cpu_usage is not None:
                    metrics["cpu_usage_percent"] = round(cpu_usage, 2)
                
                # Listening ports count
                listening_ports = entry.get("listening_ports", [])
                if listening_ports:
                    metrics["listening_ports_count"] = len(listening_ports)
                
                # Dependencies count
                dependencies = entry.get("dependencies", {})
                if dependencies:
                    metrics["dependencies_count"] = len(dependencies)
                
                # Store metrics
                if metrics:
                    entry["metrics"] = metrics
                    
            except Exception as e:
                logger.debug("Failed to collect Node.js metrics: %s", e, exc_info=True)
                # Metrics are optional - don't fail collection

            findings["discovered"].append(inst_name)
            findings["instances"].append(entry)

        return findings

    # ------------------------------------------------------------
    # Legacy `.collect()` wrapper (deprecated - orchestrator uses collect_for_host directly)
    # ------------------------------------------------------------
    def collect(self):
        """
        Legacy method - orchestrator now calls collect_for_host() directly.
        This method is kept for backward compatibility with some tests.
        """
        results = {}
        
        # In host-centric structure, find which hosts have nodejs by checking their services
        all_hosts = self.env_config.get("hosts", [])
        
        for host in all_hosts:
            host_name = host.get("name")
            host_services = host.get("services", {})
            
            # Check if this host has nodejs service
            if "nodejs" not in host_services:
                continue
            
            # Get nodejs service config for this host
            nodejs_cfg = host_services["nodejs"]
            
            # Update component_config to this host's nodejs config
            original_instances = self.instances_cfg
            self.instances_cfg = nodejs_cfg.get("instances", [])
            
            try:
                connector = self._make_host_connector(host)
                host_result = self.collect_for_host(host, connector)
                results[host_name] = host_result
                
                try:
                    connector.close()
                except Exception:
                    pass
            finally:
                # Restore original instances config
                self.instances_cfg = original_instances

        return results

