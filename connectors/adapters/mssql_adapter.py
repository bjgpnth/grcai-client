# connectors/adapters/mssql_adapter.py

import logging
import traceback
from connectors.base_connector import BaseConnector
from connectors.historical import HistoricalLogCollectionMixin

logger = logging.getLogger("grcai.adapters.mssql")

# Try to import pyodbc, fallback to pymssql
try:
    import pyodbc
    HAS_PYODBC = True
    HAS_PYMSSQL = False
except ImportError:
    HAS_PYODBC = False
    try:
        import pymssql
        HAS_PYMSSQL = True
    except ImportError:
        HAS_PYMSSQL = False
        logger.warning("Neither pyodbc nor pymssql available. MSSQL adapter will have limited functionality.")


class MSSQLAdapter(BaseConnector, HistoricalLogCollectionMixin):

    def __init__(self, name=None, env_config=None, issue_time=None, component_config=None, **kwargs):
        super().__init__(name or "mssql", issue_time, component_config)

        self.env_config = env_config or {}
        self.component_config = component_config or {}

        # Get instances from component config (host-centric: no hosts list in component_config)
        self.instances_cfg = component_config.get("instances", [])

        logger.info(
            "MSSQLAdapter initialized; instances=%d",
            len(self.instances_cfg)
        )

    # ------------------------------------------------------------
    # Build connection string from instance config
    # ------------------------------------------------------------
    def _build_connection_string(self, inst):
        """Build connection string for MSSQL"""
        # Check if libraries are available first
        if not HAS_PYODBC and not HAS_PYMSSQL:
            raise RuntimeError("Neither pyodbc nor pymssql is available. Please install one: pip install pyodbc or pip install pymssql")
        
        server = inst.get("server") or inst.get("host") or "localhost"
        port = inst.get("port", 1433)
        database = inst.get("database") or inst.get("db") or "master"
        user = inst.get("user") or inst.get("username")
        password = inst.get("password")
        trust_server_certificate = inst.get("trust_server_certificate", True)  # Default True for Docker
        connection_timeout = inst.get("connection_timeout", 30)
        
        # Authentication method
        auth_method = inst.get("auth_method", "sql")  # "sql" or "windows"
        
        if HAS_PYODBC:
            # pyodbc connection string
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};"
            if auth_method == "windows":
                conn_str += "Trusted_Connection=yes;"
            else:
                if user and password:
                    conn_str += f"UID={user};PWD={password};"
            if trust_server_certificate:
                conn_str += "TrustServerCertificate=yes;"
            conn_str += f"Connection Timeout={connection_timeout};"
            return conn_str
        elif HAS_PYMSSQL:
            # pymssql uses separate parameters
            return {
                "server": server,
                "port": port,
                "database": database,
                "user": user,
                "password": password,
                "timeout": connection_timeout,
                "login_timeout": connection_timeout
            }

    # ------------------------------------------------------------
    # Execute SQL query using Python library
    # ------------------------------------------------------------
    def _execute_sql(self, inst, sql, connector, container_id):
        """
        Execute SQL query. Connects from host to container's exposed port.
        Returns list of rows (each row is a dict).
        
        Note: For Docker containers, we connect from the host to the container's
        exposed port. The container must have its SQL Server port (1433) exposed.
        """
        conn_str_or_params = self._build_connection_string(inst)
        
        # Debug: Log connection details
        server = inst.get("server") or inst.get("host") or "localhost"
        port = inst.get("port", 1433)
        logger.info(f"MSSQL adapter: SQL connection attempt - server: {server}, port: {port}, connection_string: {conn_str_or_params[:100] if isinstance(conn_str_or_params, str) else conn_str_or_params}")
        
        # Connect from host to container's exposed port
        # This is the preferred approach since we're using Python libraries
        try:
            if HAS_PYODBC:
                conn = pyodbc.connect(conn_str_or_params)
                cursor = conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Convert to list of dicts, handling bytes objects for JSON serialization
                result = []
                for row in rows:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        value = row[i]
                        # Convert bytes to string for JSON serialization
                        if isinstance(value, bytes):
                            try:
                                value = value.decode('utf-8', errors='replace')
                            except Exception:
                                value = value.decode('latin-1', errors='replace')
                        # Convert other non-serializable types
                        elif value is not None and not isinstance(value, (str, int, float, bool, type(None))):
                            # Handle datetime, Decimal, etc.
                            value = str(value)
                        row_dict[col] = value
                    result.append(row_dict)
                conn.close()
                return result
            elif HAS_PYMSSQL:
                params = conn_str_or_params
                conn = pymssql.connect(**params)
                cursor = conn.cursor(as_dict=True)
                cursor.execute(sql)
                rows = cursor.fetchall()
                conn.close()
                # pymssql returns dicts, but we need to ensure bytes are converted
                result = []
                for row in rows:
                    if isinstance(row, dict):
                        cleaned_row = {}
                        for key, value in row.items():
                            # Convert bytes to string for JSON serialization
                            if isinstance(value, bytes):
                                try:
                                    value = value.decode('utf-8', errors='replace')
                                except Exception:
                                    value = value.decode('latin-1', errors='replace')
                            # Convert other non-serializable types
                            elif value is not None and not isinstance(value, (str, int, float, bool, type(None))):
                                value = str(value)
                            cleaned_row[key] = value
                        result.append(cleaned_row)
                    else:
                        result.append(row)
                return result
            else:
                raise RuntimeError("Neither pyodbc nor pymssql is available. Please install one: pip install pyodbc or pip install pymssql")
        except Exception as e:
            error_msg = f"SQL execution error: {str(e)}"
            logger.warning(error_msg)
            raise RuntimeError(error_msg)

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
    def _exec(self, connector, container_id, cmd):
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
                    out = connector.exec_cmd(docker_cmd)
                    if isinstance(out, dict):
                        stdout = out.get("stdout", "")
                        stderr = out.get("stderr", "")
                        rc = out.get("rc", 1)
                        # Check if command succeeded (rc=0) or if we got output
                        if rc == 0 or stdout:
                            return stdout
                        # If failed, return error message
                        error_msg = stderr or out.get("error", "Command failed")
                        logger.debug(f"MSSQL adapter: docker exec failed: rc={rc}, stderr='{error_msg[:100]}'")
                        return f"[docker exec error: {error_msg}]"
                    return str(out or "")
                except Exception as e:
                    logger.warning(f"MSSQL adapter: docker exec exception: {e}", exc_info=True)
                    return f"[docker exec error: {e}]"
            
            # DockerHostConnector: use exec_in_container (actually implemented)
            elif isinstance(connector, DockerHostConnector):
                try:
                    out = connector.exec_in_container(container_id, wrapped)
                    if isinstance(out, dict):
                        return out.get("stdout") or out.get("output") or ""
                    return str(out or "")
                except Exception as e:
                    return f"[container exec error: {e}]"

        # No container_id or container execution failed: execute on host
        if hasattr(connector, "exec_cmd"):
            try:
                out = connector.exec_cmd(cmd)
                if isinstance(out, dict):
                    return out.get("stdout") or out.get("output") or ""
                return str(out or "")
            except Exception as e:
                return f"[host exec error: {e}]"

        return "[no exec method]"

    # ------------------------------------------------------------
    # Main collection per host
    # ------------------------------------------------------------
    def collect_for_host(self, host_info, connector):
        findings = {
            "type": "mssql",
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
        # Handle both SSHHostConnector format (dict) and DockerHostConnector format (list)
        docker_map = {}
        
        # Log connector type for debugging
        connector_type = type(connector).__name__
        logger.info(f"MSSQL adapter: Using connector type: {connector_type}")
        
        # Check if DockerHostConnector has a valid client
        if hasattr(connector, "client"):
            if connector.client is None:
                logger.warning(f"MSSQL adapter: DockerHostConnector has no Docker client (client=None). This usually means Docker is not available locally or connection failed.")
                if hasattr(connector, "_last_err"):
                    logger.warning(f"MSSQL adapter: DockerHostConnector error: {connector._last_err}")
            else:
                logger.info(f"MSSQL adapter: DockerHostConnector has valid Docker client")
        
        if hasattr(connector, "list_containers"):
            try:
                containers_result = connector.list_containers()
                logger.info(f"MSSQL adapter: list_containers returned {len(containers_result) if containers_result else 0} containers (type: {type(containers_result).__name__})")
                
                # Handle SSHHostConnector format: {"ok": bool, "containers": [dict, ...]}
                if isinstance(containers_result, dict):
                    if containers_result.get("ok") and containers_result.get("containers"):
                        for c in containers_result["containers"]:
                            # SSHHostConnector returns dicts with lowercase keys
                            container_id = c.get("id", "")
                            container_names = c.get("names", "")
                            # Handle comma-separated names and remove leading slashes
                            if container_names:
                                for name in container_names.split(","):
                                    name = name.strip().lstrip("/")  # Remove leading slash
                                    if name:
                                        docker_map[name] = container_id
                # Handle DockerHostConnector format: List[Container] with .name and .id
                elif isinstance(containers_result, list):
                    for c in containers_result:
                        # Docker SDK container objects have:
                        # - .name (property, returns container name, may have leading '/')
                        # - .id (property, returns full container ID)
                        # - .attrs (dict, contains full container attributes including Names[])
                        name = None
                        cid = None
                        
                        # Try to get name - use same robust logic as Kafka adapter
                        if hasattr(c, "name"):
                            try:
                                name = c.name
                            except Exception as e:
                                logger.debug(f"MSSQL adapter: Failed to get container.name: {e}")
                        elif hasattr(c, "attrs") and isinstance(c.attrs, dict):
                            # Fallback: get from attrs
                            names = c.attrs.get("Names", [])
                            # Ensure names is a list before subscripting
                            if names and isinstance(names, (list, tuple)) and len(names) > 0:
                                name = names[0] if isinstance(names[0], str) else str(names[0])
                        
                        # Try to get ID
                        if hasattr(c, "id"):
                            try:
                                cid = c.id
                            except Exception as e:
                                logger.debug(f"MSSQL adapter: Failed to get container.id: {e}")
                        elif hasattr(c, "attrs") and isinstance(c.attrs, dict):
                            cid = c.attrs.get("Id")
                        
                        # Always convert cid to string immediately
                        if cid is not None:
                            cid = str(cid)
                        
                        if name and cid:
                            # Strip leading '/' from name if present (Docker SDK returns names with leading slash)
                            clean_name = name.lstrip('/')
                            # Store both with and without leading '/' for flexibility
                            docker_map[clean_name] = cid
                            if name != clean_name:
                                docker_map[name] = cid
                            logger.info(f"MSSQL adapter: Mapped container '{clean_name}' (from '{name}') -> {cid[:12]}")
                        else:
                            logger.warning(f"MSSQL adapter: Container missing name or ID: name={name}, id={cid[:12] if cid else None}")
            except Exception as e:
                logger.warning(f"MSSQL adapter: Failed to list containers: {e}", exc_info=True)
        
        logger.info(f"MSSQL adapter: Container map built with {len(docker_map)} entries: {list(docker_map.keys())}")

        for inst in (self.instances_cfg or []):
            inst_name = inst.get("name", "mssql")
            container_name = inst.get("container")
            container_id = docker_map.get(container_name) if container_name else None

            # If container_name is specified but container_id is None, provide helpful error
            if container_name and container_id is None:
                available_containers = list(docker_map.keys())
                error_msg = f"Container '{container_name}' specified in YAML but not found. Available containers: {', '.join(available_containers) if available_containers else 'none'}"
                logger.warning(f"MSSQL adapter: {error_msg}")
                entry = {
                    "name": inst_name,
                    "container": container_name,
                    "status": "service_not_running",
                    "message": error_msg,
                    "metrics": {},
                    "logs": {}
                }
                findings["discovered"].append(inst_name)
                findings["instances"].append(entry)
                continue

            entry = {
                "name": inst_name,
                "container": container_name,
                "version": None,
                "databases": [],
                "connections_total": None,
                "connections_by_state": [],
                "tables": [],
                "query_performance": [],
                "wait_stats": [],
                "blocking_info": [],
                "deadlock_info": [],
                "server_config": {},
                "database_health": {
                    "file_sizes": [],
                    "index_fragmentation": [],
                    "missing_indexes": [],
                    "long_running_queries": [],
                    "buffer_pool_stats": {},
                    "io_stats": [],
                    "transaction_log_stats": [],
                    "page_life_expectancy": None
                },
                "error_logs": {},
                "metrics": {},
                "errors": []
            }

            # ------------------------------------------------------------------
            # LIVENESS: Container-aware checks
            # ------------------------------------------------------------------
            try:
                container_port = inst.get("port", 1433)  # Container port from YAML
                
                # Detect if we're in a Docker container context
                # Works with both DockerHostConnector and SSHHostConnector (via docker exec)
                from connectors.host_connectors.docker_host_connector import DockerHostConnector
                from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                is_docker_container = container_id is not None
                
                # Determine target host for port checks
                # For SSH-accessed containers, use remote host address; for local, use localhost
                if isinstance(connector, SSHHostConnector):
                    target_host = connector.address or host_info.get("address", "127.0.0.1")
                else:
                    target_host = inst.get("address", "127.0.0.1")  # Use instance address or localhost
                
                # For SSH-accessed containers, get the host port mapping
                # Container port 1433 might be mapped to host port 11433
                host_port = container_port  # Default to container port
                if is_docker_container and isinstance(connector, SSHHostConnector):
                    try:
                        # Use docker port command which is simpler and more reliable
                        # Output format: "0.0.0.0:11433" or "[::]:11433" - we extract the port number
                        port_cmd = f"docker port {container_id} {container_port}/tcp 2>/dev/null | head -1 | cut -d: -f2 | tr -d ' ' || echo ''"
                        port_out = connector.exec_cmd(port_cmd)
                        if isinstance(port_out, dict):
                            port_str = port_out.get("stdout", "").strip()
                        else:
                            port_str = str(port_out).strip()
                        
                        # Extract just the port number (remove any trailing newlines or spaces)
                        if port_str:
                            port_str = port_str.split()[0] if port_str.split() else port_str
                            # Remove any non-digit characters except the port number itself
                            port_digits = ''.join(filter(str.isdigit, port_str))
                            if port_digits and port_digits.isdigit():
                                host_port = int(port_digits)
                                logger.info(f"MSSQL adapter: Container port {container_port} mapped to host port {host_port}")
                            else:
                                logger.debug(f"MSSQL adapter: Could not extract host port from '{port_str}', using container port {container_port}")
                        else:
                            logger.debug(f"MSSQL adapter: docker port returned empty, using container port {container_port}")
                    except Exception as e:
                        logger.debug(f"MSSQL adapter: Failed to get host port mapping: {e}, using container port {container_port}")
                
                port = host_port  # Use host port for checks
                
                if is_docker_container:
                    # ============================================================
                    # DOCKER CONTAINER: Use container-aware checks
                    # ============================================================
                    logger.debug(f"MSSQL adapter: Using Docker container-aware liveness checks for '{inst_name}'")
                    
                    # 0. First, verify container is actually running (not just exists)
                    container_running = False
                    container_status = "unknown"
                    container_id_str = str(container_id) if container_id else ""
                    try:
                        # DockerHostConnector: use Docker SDK
                        if hasattr(connector, "client") and connector.client:
                            try:
                                container_info = connector.client.containers.get(container_id_str)
                                if container_info is None:
                                    raise RuntimeError(f"mssql container {container_id_str[:12]} not found")
                                container_info.reload()
                                if not hasattr(container_info, 'attrs') or not isinstance(container_info.attrs, dict):
                                    raise RuntimeError(f"mssql container {container_id_str[:12]} has invalid attrs")
                                state = container_info.attrs.get('State', {})
                                if not isinstance(state, dict):
                                    raise RuntimeError(f"mssql container {container_id_str[:12]} has invalid State")
                                container_status = state.get('Status', 'unknown')
                                container_running = container_status == "running"
                                logger.debug(f"MSSQL adapter: Container {container_id_str[:12]} status: {container_status}")
                                if not container_running:
                                    logger.warning(f"MSSQL adapter: Container {container_id_str[:12]} is not running (status: {container_status})")
                            except Exception as e:
                                error_str = str(e)
                                logger.debug(f"MSSQL adapter: Failed to get container status: {e}")
                                if "not found" in error_str.lower() or "no such container" in error_str.lower():
                                    raise RuntimeError(f"mssql container {container_id_str[:12]} not found: {error_str}")
                                container_running = False
                        # SSHHostConnector or other: use docker inspect command
                        elif hasattr(connector, "exec_cmd"):
                            try:
                                inspect_cmd = f"docker inspect --format '{{{{.State.Status}}}}' {container_id_str} 2>/dev/null || echo 'NOT_FOUND'"
                                out = connector.exec_cmd(inspect_cmd)
                                if isinstance(out, dict):
                                    status_output = out.get("stdout", "").strip()
                                else:
                                    status_output = str(out).strip()
                                
                                if status_output == "NOT_FOUND" or not status_output:
                                    raise RuntimeError(f"mssql container {container_id_str[:12]} not found")
                                
                                container_status = status_output.lower()
                                container_running = container_status == "running"
                                logger.debug(f"MSSQL adapter: Container {container_id_str[:12]} status: {container_status}")
                                if not container_running:
                                    logger.warning(f"MSSQL adapter: Container {container_id_str[:12]} is not running (status: {container_status})")
                            except RuntimeError:
                                raise
                            except Exception as e:
                                logger.debug(f"MSSQL adapter: Container status check (docker inspect) failed: {e}")
                                container_running = False
                        else:
                            raise RuntimeError(f"mssql Docker client or exec_cmd not available")
                    except RuntimeError as e:
                        raise
                    except Exception as e:
                        logger.debug(f"MSSQL adapter: Container status check failed: {e}")
                        container_running = False
                    
                    # Also verify we can actually exec into the container (double-check)
                    # Track exec_check_passed separately so we can use it as a liveness signal.
                    exec_check_passed = False
                    if container_running:
                        try:
                            test_exec = self._exec(connector, container_id_str, "echo test", timeout=2)
                            if not (isinstance(test_exec, str) and (test_exec.startswith("[container exec error") or test_exec.startswith("[host exec error"))):
                                exec_check_passed = True
                            else:
                                logger.warning(f"MSSQL adapter: Container {container_id_str[:12]} appears running but exec failed: {test_exec[:100]}")
                        except Exception as e:
                            logger.debug(f"MSSQL adapter: Exec test failed: {e}")
                    
                    # Only fail liveness if container_running is False (works for both DockerHostConnector and SSHHostConnector)
                    if not container_running:
                        status_msg = f"status: {container_status}" if container_status != "unknown" else "container not running"
                        raise RuntimeError(f"mssql container {container_id_str[:12] if container_id_str else 'unknown'} is not running ({status_msg})")
                    
                    # 1. Check port accessibility from Docker host (not inside container)
                    port_check_passed = False
                    if isinstance(connector, SSHHostConnector):
                        # For SSH-accessed containers, check port on remote host (use localhost on remote host)
                        try:
                            port_cmd = f"timeout 2 bash -c '</dev/tcp/127.0.0.1/{port}' 2>&1 || nc -z -w2 127.0.0.1 {port} 2>&1 || echo 'PORT_CHECK_FAILED'"
                            port_out = connector.exec_cmd(port_cmd)
                            if isinstance(port_out, dict):
                                port_str = port_out.get("stdout", "").strip()
                                port_stderr = port_out.get("stderr", "").strip()
                                port_rc = port_out.get("rc", 1)
                                logger.info(f"MSSQL adapter: Port {port} check command output: stdout='{port_str[:200]}', stderr='{port_stderr[:200]}', rc={port_rc}")
                            else:
                                port_str = str(port_out).strip()
                                logger.info(f"MSSQL adapter: Port {port} check command output: '{port_str[:200]}'")
                            
                            port_str_lower = port_str.lower()
                            # Check for success indicators (empty output or connection succeeded)
                            # nc -z returns empty on success, /dev/tcp also returns empty on success
                            if port_str == "" or port_str_lower == "":
                                port_check_passed = True
                                logger.info(f"MSSQL adapter: Port {port} check passed on remote host (empty output = success)")
                            elif "port_check_failed" not in port_str_lower and "refused" not in port_str_lower and "failed" not in port_str_lower and "connection refused" not in port_str_lower:
                                # If we get here and output is not empty, it might still be success (nc might output something)
                                # Check if it's actually an error message
                                if "no route" not in port_str_lower and "timeout" not in port_str_lower:
                                    port_check_passed = True
                                    logger.info(f"MSSQL adapter: Port {port} check passed on remote host (no error indicators)")
                                else:
                                    logger.debug(f"MSSQL adapter: Port {port} not accessible on remote host: {port_str[:100]}")
                            else:
                                logger.debug(f"MSSQL adapter: Port {port} not accessible on remote host: {port_str[:100]}")
                        except Exception as e:
                            logger.warning(f"MSSQL adapter: Remote port check (SSH) failed: {e}", exc_info=True)
                    else:
                        # For local DockerHostConnector, use socket check
                        try:
                            import socket
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.settimeout(2)
                            connect_result = sock.connect_ex((target_host, port))
                            sock.close()
                            if connect_result == 0:
                                port_check_passed = True
                                logger.debug(f"MSSQL adapter: Port {port} check passed from host (socket)")
                            else:
                                logger.debug(f"MSSQL adapter: Port {port} not accessible from host (socket result: {connect_result})")
                        except Exception as e:
                            logger.debug(f"MSSQL adapter: Host port check (socket) failed: {e}")
                    
                    # 2. Optionally check /proc/1/status to verify main process is sqlservr
                    process_check_passed = False
                    try:
                        proc_status_cmd = "cat /proc/1/status 2>/dev/null | grep -i '^Name:' | awk '{print $2}' || echo 'PROC_CHECK_FAILED'"
                        proc_out = self._exec(connector, container_id_str, proc_status_cmd)
                        proc_str = str(proc_out).lower().strip()
                        logger.info(f"MSSQL adapter: Process check (/proc/1/status) output: '{proc_str}'")
                        if "proc_check_failed" not in proc_str and ("sqlservr" in proc_str or "mssql" in proc_str):
                            process_check_passed = True
                            logger.info(f"MSSQL adapter: Process check passed (/proc/1/status shows sqlservr/mssql)")
                        else:
                            logger.debug(f"MSSQL adapter: Process check (/proc/1/status) did not find sqlservr/mssql")
                    except Exception as e:
                        logger.warning(f"MSSQL adapter: Process check (/proc/1/status) failed: {e}", exc_info=True)
                    # Fallback: try ps aux in container to detect sqlservr
                    if not process_check_passed:
                        try:
                            ps_cmd = "ps aux | grep -i '[s]qlservr\\|[m]ssql' | grep -v grep"
                            ps_out = self._exec(connector, container_id_str, ps_cmd)
                            logger.info(f"MSSQL adapter: Process check fallback (ps aux) output: '{str(ps_out)[:200]}'")
                            if ps_out and not ps_out.startswith("[") and not ps_out.startswith("host exec error") and not ps_out.startswith("container exec error"):
                                process_check_passed = True
                                logger.info("MSSQL adapter: Process check fallback (ps aux) passed")
                            else:
                                logger.debug(f"MSSQL adapter: Process check fallback (ps aux) failed or returned error")
                        except Exception as e:
                            logger.warning(f"MSSQL adapter: Process check fallback (ps aux) failed: {e}", exc_info=True)
                    
                    # Log summary of all checks
                    logger.info(f"MSSQL adapter: Liveness checks summary - port_check: {port_check_passed}, process_check: {process_check_passed}, exec_check: {exec_check_passed}")
                    
                    # Liveness check: pass if any of port/process/exec checks succeed
                    if not (port_check_passed or process_check_passed or exec_check_passed):
                        error_details = []
                        if not port_check_passed:
                            error_details.append(f"port {port} not reachable")
                        if not process_check_passed:
                            error_details.append("process check failed")
                        if not exec_check_passed:
                            error_details.append("exec check failed")
                        raise RuntimeError(f"mssql liveness check failed: {', '.join(error_details)}")
                    
                    # For SSH-accessed containers, update instance config to use host port for SQL connections
                    # SQL connections are made from collector machine to remote host, so use host port
                    if isinstance(connector, SSHHostConnector) and host_port != container_port:
                        inst = inst.copy()  # Don't modify original
                        inst["port"] = host_port
                        # Use remote host address for SQL connections
                        # Note: The collector machine must be able to reach this address:port
                        if not inst.get("server") and not inst.get("host"):
                            inst["server"] = connector.address or target_host
                        logger.info(f"MSSQL adapter: Using host port {host_port} (mapped from container port {container_port}) for SQL connections")
                        logger.info(f"MSSQL adapter: SQL connection will be made from collector to {inst.get('server')}:{inst.get('port')}")
                        
                        # Verify connectivity from collector machine before attempting SQL queries
                        try:
                            import socket
                            test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            test_sock.settimeout(5)
                            test_host = inst.get("server") or inst.get("host") or "localhost"
                            test_port = inst.get("port", 1433)
                            test_result = test_sock.connect_ex((test_host, test_port))
                            test_sock.close()
                            if test_result != 0:
                                logger.warning(f"MSSQL adapter: Cannot reach {test_host}:{test_port} from collector machine (socket error: {test_result}). SQL queries will fail.")
                                logger.warning(f"MSSQL adapter: This may be due to firewall rules or network configuration. Port {test_port} must be accessible from the collector machine.")
                            else:
                                logger.info(f"MSSQL adapter: Connectivity test passed - {test_host}:{test_port} is reachable from collector")
                        except Exception as e:
                            logger.warning(f"MSSQL adapter: Connectivity test failed: {e}. SQL queries may fail.")
                    
                    # Mark as successful since liveness checks passed
                    entry["status"] = "successful"
                    
                else:
                    # ============================================================
                    # NON-DOCKER (SSH/Local): Use standard checks
                    # ============================================================
                    logger.debug(f"MSSQL adapter: Using standard liveness checks for '{inst_name}' (non-Docker)")
                    
                    # Check port open (localhost or remote) - try multiple methods
                    port_cmd = f"timeout 2 bash -c '</dev/tcp/{target_host}/{port}' 2>&1 || nc -z -w2 {target_host} {port} 2>&1 || echo 'PORT_CHECK_FAILED'"
                    port_out = self._exec(connector, container_id, port_cmd)
                    port_str = str(port_out).lower()
                    # Check for various failure indicators
                    if "port_check_failed" in port_str or "refused" in port_str or "failed" in port_str or "connection refused" in port_str or "no route to host" in port_str:
                        # If port check failed, try alternative: check if sqlservr process is running
                        ps_cmd = "ps aux | grep -i '[s]qlservr\\|[m]ssql' | grep -v grep"
                        ps_out = self._exec(connector, container_id, ps_cmd)
                        if not ps_out.strip() or ps_out.startswith("[host exec error") or ps_out.startswith("[container exec error"):
                            raise RuntimeError(f"mssql port {port} not reachable and no sqlservr process found")
                        else:
                            raise RuntimeError(f"mssql port {port} not reachable (process exists but port closed)")
                    # Also check if exec itself failed
                    if port_out.startswith("[host exec error") or port_out.startswith("[container exec error"):
                        raise RuntimeError(f"mssql port {port} check command failed: {port_out}")
                    
                    # Mark as successful since liveness check passed
                    entry["status"] = "successful"
                        
            except Exception as e:
                error_msg = f"liveness: {e}"
                logger.warning(f"MSSQL adapter: Liveness check failed for '{inst_name}': {error_msg}")
                entry["errors"].append(error_msg)
                findings["discovered"].append(inst_name)
                findings["instances"].append(entry)
                continue

            def run_sql(sql):
                try:
                    return self._execute_sql(inst, sql, connector, container_id)
                except Exception as e:
                    entry["errors"].append(f"SQL error: {str(e)}")
                    return []

            # Note: Log collection can happen even without SQL libraries
            # SQL queries require pyodbc or pymssql, but logs can be collected from Docker/files

            # Check if libraries are available for SQL queries
            sql_available = HAS_PYODBC or HAS_PYMSSQL
            
            if not sql_available:
                entry["version"] = "Library not available (pyodbc/pymssql required)"
                entry["note"] = "MSSQL adapter requires pyodbc or pymssql for SQL queries. Install with: pip install pyodbc. Logs will still be collected."
            else:
                try:
                    # --- Version ---
                    try:
                        rows = run_sql("SELECT @@VERSION as version")
                        if rows:
                            if isinstance(rows[0], dict):
                                entry["version"] = rows[0].get("version", "")
                            else:
                                entry["version"] = str(rows[0][0]) if rows[0] else ""
                    except Exception as e:
                        entry["errors"].append(f"version: {e}")

                    # --- Databases ---
                    try:
                        sql = """
                            SELECT 
                                name,
                                database_id,
                                create_date,
                                compatibility_level,
                                state_desc,
                                (SELECT SUM(size) * 8 / 1024 FROM sys.master_files WHERE database_id = db.database_id) as size_mb
                            FROM sys.databases db
                            ORDER BY name
                        """
                        rows = run_sql(sql)
                        dbs = []
                        for row in rows:
                            if isinstance(row, dict):
                                dbs.append({
                                    "name": row.get("name", ""),
                                    "database_id": row.get("database_id"),
                                    "create_date": str(row.get("create_date", "")),
                                    "compatibility_level": row.get("compatibility_level"),
                                    "state": row.get("state_desc", ""),
                                    "size_mb": row.get("size_mb")
                                })
                            else:
                                dbs.append({
                                    "name": str(row[0]) if len(row) > 0 else "",
                                    "database_id": row[1] if len(row) > 1 else None,
                                    "create_date": str(row[2]) if len(row) > 2 else "",
                                    "compatibility_level": row[3] if len(row) > 3 else None,
                                    "state": str(row[4]) if len(row) > 4 else "",
                                    "size_mb": row[5] if len(row) > 5 else None
                                })
                        entry["databases"] = dbs
                    except Exception as e:
                        entry["errors"].append(f"databases: {e}")

                    # --- Total connections ---
                    try:
                        rows = run_sql("SELECT COUNT(*) as total FROM sys.dm_exec_sessions WHERE is_user_process = 1")
                        if rows:
                            if isinstance(rows[0], dict):
                                entry["connections_total"] = rows[0].get("total", 0)
                            else:
                                entry["connections_total"] = rows[0][0] if rows[0] else 0
                    except Exception as e:
                        entry["errors"].append(f"connections_total: {e}")

                    # --- Connections by state ---
                    try:
                        sql = """
                            SELECT 
                                status,
                                COUNT(*) as count
                            FROM sys.dm_exec_sessions
                            WHERE is_user_process = 1
                            GROUP BY status
                            ORDER BY count DESC
                        """
                        rows = run_sql(sql)
                        states = []
                        for row in rows:
                            if isinstance(row, dict):
                                states.append({
                                    "status": row.get("status", ""),
                                    "count": row.get("count", 0)
                                })
                            else:
                                states.append({
                                    "status": str(row[0]) if len(row) > 0 else "",
                                    "count": row[1] if len(row) > 1 else 0
                                })
                        entry["connections_by_state"] = states
                    except Exception as e:
                        entry["errors"].append(f"connections_by_state: {e}")

                    # --- Tables metadata ---
                    try:
                        sql = """
                            SELECT TOP 50
                                TABLE_SCHEMA,
                                TABLE_NAME,
                                TABLE_TYPE
                            FROM INFORMATION_SCHEMA.TABLES
                            ORDER BY TABLE_SCHEMA, TABLE_NAME
                        """
                        rows = run_sql(sql)
                        tables = []
                        for row in rows:
                            if isinstance(row, dict):
                                tables.append({
                                    "schema": row.get("TABLE_SCHEMA", ""),
                                    "name": row.get("TABLE_NAME", ""),
                                    "type": row.get("TABLE_TYPE", "")
                                })
                            else:
                                tables.append({
                                    "schema": str(row[0]) if len(row) > 0 else "",
                                    "name": str(row[1]) if len(row) > 1 else "",
                                    "type": str(row[2]) if len(row) > 2 else ""
                                })
                        entry["tables"] = tables
                    except Exception as e:
                        entry["errors"].append(f"tables: {e}")

                    # --- Query Performance (top queries by CPU/IO) ---
                    try:
                        sql = """
                            SELECT TOP 10
                                qs.execution_count,
                                qs.total_worker_time / 1000 as total_cpu_ms,
                                qs.total_logical_reads as total_logical_reads,
                                qs.total_elapsed_time / 1000 as total_elapsed_ms,
                                SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
                                    ((CASE qs.statement_end_offset
                                        WHEN -1 THEN DATALENGTH(qt.text)
                                        ELSE qs.statement_end_offset
                                    END - qs.statement_start_offset)/2)+1) as query_text
                            FROM sys.dm_exec_query_stats qs
                            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
                            ORDER BY qs.total_worker_time DESC
                        """
                        rows = run_sql(sql)
                        perf = []
                        for row in rows:
                            if isinstance(row, dict):
                                perf.append({
                                    "execution_count": row.get("execution_count", 0),
                                    "total_cpu_ms": row.get("total_cpu_ms", 0),
                                    "total_logical_reads": row.get("total_logical_reads", 0),
                                    "total_elapsed_ms": row.get("total_elapsed_ms", 0),
                                    "query_text": row.get("query_text", "")[:500]  # Truncate long queries
                                })
                            else:
                                perf.append({
                                    "execution_count": row[0] if len(row) > 0 else 0,
                                    "total_cpu_ms": row[1] if len(row) > 1 else 0,
                                    "total_logical_reads": row[2] if len(row) > 2 else 0,
                                    "total_elapsed_ms": row[3] if len(row) > 3 else 0,
                                    "query_text": (str(row[4]) if len(row) > 4 else "")[:500]
                                })
                        entry["query_performance"] = perf
                    except Exception as e:
                        entry["errors"].append(f"query_performance: {e}")

                    # --- Wait Stats ---
                    try:
                        sql = """
                            SELECT TOP 20
                                wait_type,
                                waiting_tasks_count,
                                wait_time_ms,
                                max_wait_time_ms,
                                signal_wait_time_ms
                            FROM sys.dm_os_wait_stats
                            WHERE wait_type NOT IN ('CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE',
                                'SLEEP_TASK', 'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR',
                                'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH',
                                'XE_TIMER_EVENT', 'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT',
                                'CLR_AUTO_EVENT', 'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
                                'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP')
                            ORDER BY wait_time_ms DESC
                        """
                        rows = run_sql(sql)
                        waits = []
                        for row in rows:
                            if isinstance(row, dict):
                                waits.append({
                                    "wait_type": row.get("wait_type", ""),
                                    "waiting_tasks_count": row.get("waiting_tasks_count", 0),
                                    "wait_time_ms": row.get("wait_time_ms", 0),
                                    "max_wait_time_ms": row.get("max_wait_time_ms", 0),
                                    "signal_wait_time_ms": row.get("signal_wait_time_ms", 0)
                                })
                            else:
                                waits.append({
                                    "wait_type": str(row[0]) if len(row) > 0 else "",
                                    "waiting_tasks_count": row[1] if len(row) > 1 else 0,
                                    "wait_time_ms": row[2] if len(row) > 2 else 0,
                                    "max_wait_time_ms": row[3] if len(row) > 3 else 0,
                                    "signal_wait_time_ms": row[4] if len(row) > 4 else 0
                                })
                        entry["wait_stats"] = waits
                    except Exception as e:
                        entry["errors"].append(f"wait_stats: {e}")

                    # --- Blocking Info ---
                    try:
                        sql = """
                            SELECT 
                                blocking.session_id as blocking_session_id,
                                blocked.session_id as blocked_session_id,
                                blocked.wait_type,
                                blocked.wait_time,
                                blocked.wait_resource,
                                DB_NAME(blocked.database_id) as database_name
                            FROM sys.dm_exec_requests blocked
                            INNER JOIN sys.dm_exec_sessions blocking ON blocked.blocking_session_id = blocking.session_id
                            WHERE blocked.blocking_session_id > 0
                        """
                        rows = run_sql(sql)
                        blocking = []
                        for row in rows:
                            if isinstance(row, dict):
                                blocking.append({
                                    "blocking_session_id": row.get("blocking_session_id"),
                                    "blocked_session_id": row.get("blocked_session_id"),
                                    "wait_type": row.get("wait_type", ""),
                                    "wait_time": row.get("wait_time", 0),
                                    "wait_resource": row.get("wait_resource", ""),
                                    "database_name": row.get("database_name", "")
                                })
                            else:
                                blocking.append({
                                    "blocking_session_id": row[0] if len(row) > 0 else None,
                                    "blocked_session_id": row[1] if len(row) > 1 else None,
                                    "wait_type": str(row[2]) if len(row) > 2 else "",
                                    "wait_time": row[3] if len(row) > 3 else 0,
                                    "wait_resource": str(row[4]) if len(row) > 4 else "",
                                    "database_name": str(row[5]) if len(row) > 5 else ""
                                })
                        entry["blocking_info"] = blocking
                    except Exception as e:
                        entry["errors"].append(f"blocking_info: {e}")

                    # --- Deadlock Info (from error log) ---
                    try:
                        sql = """
                            SELECT TOP 10
                                CAST(target_data AS XML).value('(//@timestamp)[1]', 'datetime') as timestamp,
                                CAST(target_data AS XML).value('(//@id)[1]', 'int') as id,
                                CAST(target_data AS XML).value('(//@type)[1]', 'varchar(100)') as type,
                                CAST(target_data AS XML).value('(//@message)[1]', 'varchar(max)') as message
                            FROM sys.dm_xe_session_targets xet
                            INNER JOIN sys.dm_xe_sessions xes ON xes.address = xet.event_session_address
                            WHERE xes.name = 'system_health'
                            AND CAST(target_data AS XML).value('(//@type)[1]', 'varchar(100)') = 'xml_deadlock_report'
                            ORDER BY timestamp DESC
                        """
                        rows = run_sql(sql)
                        deadlocks = []
                        for row in rows:
                            if isinstance(row, dict):
                                deadlocks.append({
                                    "timestamp": str(row.get("timestamp", "")),
                                    "id": row.get("id"),
                                    "type": row.get("type", ""),
                                    "message": str(row.get("message", ""))[:1000]  # Truncate
                                })
                            else:
                                deadlocks.append({
                                    "timestamp": str(row[0]) if len(row) > 0 else "",
                                    "id": row[1] if len(row) > 1 else None,
                                    "type": str(row[2]) if len(row) > 2 else "",
                                    "message": (str(row[3]) if len(row) > 3 else "")[:1000]
                                })
                        entry["deadlock_info"] = deadlocks
                    except Exception as e:
                        # Deadlock info may not be available - not critical
                        entry["errors"].append(f"deadlock_info: {e}")

                    # --- Server Configuration ---
                    try:
                        sql = """
                            SELECT 
                                name,
                                value,
                                value_in_use,
                                description
                            FROM sys.configurations
                            WHERE name IN (
                                'max server memory (MB)',
                                'min server memory (MB)',
                                'max degree of parallelism',
                                'cost threshold for parallelism',
                                'optimize for ad hoc workloads',
                                'backup compression default',
                                'remote access',
                                'remote login timeout (s)'
                            )
                            ORDER BY name
                        """
                        rows = run_sql(sql)
                        config = {}
                        for row in rows:
                            def convert_config_value(val):
                                """Convert SQL Server config value (bytes, string, or int) to appropriate type"""
                                if val is None:
                                    return None
                                # If it's bytes, convert to int
                                if isinstance(val, bytes):
                                    try:
                                        # SQL Server stores config values as binary (little-endian int)
                                        return int.from_bytes(val, byteorder='little', signed=False)
                                    except Exception:
                                        return str(val)
                                # If it's a string that looks like binary data (Unicode escape sequences), try to parse
                                if isinstance(val, str):
                                    # Check if it's a string representation of binary data
                                    # SQL Server config values are typically integers
                                    try:
                                        # Try to convert string to int if it's numeric
                                        if val.strip().isdigit() or (val.strip().startswith('-') and val.strip()[1:].isdigit()):
                                            return int(val)
                                        # If it contains Unicode replacement characters (\ufffd), it might be binary data
                                        # that was incorrectly decoded. Try to reconstruct the bytes.
                                        if '\ufffd' in val or any(ord(c) > 127 for c in val):
                                            # Special case: pattern \ufffd\ufffd\ufffd\u007f often represents 0xFF 0xFF 0xFF 0x7F
                                            # which is 2147483647 (max 32-bit signed int, common for "max server memory")
                                            if val == '\ufffd\ufffd\ufffd\u007f' or val == '\xff\xff\xff\x7f':
                                                return 2147483647
                                            # Try to encode back to bytes and interpret as little-endian int
                                            try:
                                                # Encode as latin-1 to preserve byte values, then convert
                                                byte_data = val.encode('latin-1')
                                                # Handle common cases: 4-byte ints (max memory often 0x7FFFFFFF = 2147483647)
                                                if len(byte_data) == 4:
                                                    result = int.from_bytes(byte_data, byteorder='little', signed=False)
                                                    # Sanity check: if it's a reasonable config value (0 to 2^31-1), return it
                                                    if 0 <= result <= 2147483647:
                                                        return result
                                                elif len(byte_data) > 0 and len(byte_data) <= 8:
                                                    # Try as little-endian unsigned int
                                                    result = int.from_bytes(byte_data, byteorder='little', signed=False)
                                                    if 0 <= result <= 2147483647:
                                                        return result
                                            except Exception:
                                                pass
                                        # If it contains only null bytes or control chars, it might be binary data
                                        if all(ord(c) < 32 for c in val if c):
                                            # Try to interpret as bytes and convert
                                            try:
                                                return int.from_bytes(val.encode('latin-1'), byteorder='little', signed=False)
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass
                                    return val
                                # If it's already an int/float, return as-is
                                if isinstance(val, (int, float, bool)):
                                    return val
                                # Try to convert to int if possible
                                try:
                                    return int(val)
                                except (ValueError, TypeError):
                                    return str(val)
                            
                            if isinstance(row, dict):
                                name = row.get("name", "")
                                config[name] = {
                                    "value": convert_config_value(row.get("value")),
                                    "value_in_use": convert_config_value(row.get("value_in_use")),
                                    "description": row.get("description", "")
                                }
                            else:
                                name = str(row[0]) if len(row) > 0 else ""
                                config[name] = {
                                    "value": convert_config_value(row[1] if len(row) > 1 else None),
                                    "value_in_use": convert_config_value(row[2] if len(row) > 2 else None),
                                    "description": str(row[3]) if len(row) > 3 else ""
                                }
                        entry["server_config"] = config
                    except Exception as e:
                        entry["errors"].append(f"server_config: {e}")

                except Exception as e:
                    entry["errors"].append(f"SQL collection: {e}")

            # --- Database Health Statistics (only if SQL libraries available) ---
            if sql_available:
                try:
                    # 1. Database File Sizes
                    try:
                        sql = """
                            SELECT 
                                DB_NAME(database_id) as database_name,
                                name as logical_name,
                                type_desc,
                                physical_name,
                                size * 8.0 / 1024 as size_mb,
                                max_size,
                                growth
                            FROM sys.master_files
                            ORDER BY database_id, type_desc
                        """
                        rows = run_sql(sql)
                        file_sizes = []
                        for row in rows:
                            if isinstance(row, dict):
                                size_mb_val = row.get("size_mb", 0)
                                # Convert to float if it's a string or other type
                                try:
                                    size_mb_val = float(size_mb_val) if size_mb_val is not None else 0
                                except (ValueError, TypeError):
                                    size_mb_val = 0
                                file_sizes.append({
                                    "database_name": row.get("database_name", ""),
                                    "logical_name": row.get("logical_name", ""),
                                    "type": row.get("type_desc", ""),
                                    "physical_name": row.get("physical_name", ""),
                                    "size_mb": round(size_mb_val, 2),
                                    "max_size": row.get("max_size"),
                                    "growth": row.get("growth")
                                })
                            else:
                                file_sizes.append({
                                    "database_name": str(row[0]) if len(row) > 0 else "",
                                    "logical_name": str(row[1]) if len(row) > 1 else "",
                                    "type": str(row[2]) if len(row) > 2 else "",
                                    "physical_name": str(row[3]) if len(row) > 3 else "",
                                    "size_mb": round(float(row[4]) if len(row) > 4 and row[4] else 0, 2),
                                    "max_size": row[5] if len(row) > 5 else None,
                                    "growth": row[6] if len(row) > 6 else None
                                })
                        entry["database_health"]["file_sizes"] = file_sizes
                    except Exception as e:
                        entry["errors"].append(f"file_sizes: {e}")

                    # 2. Index Fragmentation
                    try:
                        sql = """
                            SELECT TOP 50
                                DB_NAME(ps.database_id) as database_name,
                                OBJECT_SCHEMA_NAME(ps.object_id, ps.database_id) as schema_name,
                                OBJECT_NAME(ps.object_id, ps.database_id) as table_name,
                                i.name as index_name,
                                ps.avg_fragmentation_in_percent,
                                ps.page_count,
                                ps.avg_page_space_used_in_percent
                            FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'LIMITED') ps
                            INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
                            WHERE ps.avg_fragmentation_in_percent > 10
                            AND ps.page_count > 100
                            ORDER BY ps.avg_fragmentation_in_percent DESC
                        """
                        rows = run_sql(sql)
                        frag = []
                        for row in rows:
                            if isinstance(row, dict):
                                frag.append({
                                    "database_name": row.get("database_name", ""),
                                    "schema_name": row.get("schema_name", ""),
                                    "table_name": row.get("table_name", ""),
                                    "index_name": row.get("index_name", ""),
                                    "fragmentation_percent": round(row.get("avg_fragmentation_in_percent", 0), 2),
                                    "page_count": row.get("page_count", 0),
                                    "page_space_used_percent": round(row.get("avg_page_space_used_in_percent", 0), 2)
                                })
                            else:
                                frag.append({
                                    "database_name": str(row[0]) if len(row) > 0 else "",
                                    "schema_name": str(row[1]) if len(row) > 1 else "",
                                    "table_name": str(row[2]) if len(row) > 2 else "",
                                    "index_name": str(row[3]) if len(row) > 3 else "",
                                    "fragmentation_percent": round(float(row[4]) if len(row) > 4 and row[4] else 0, 2),
                                    "page_count": row[5] if len(row) > 5 else 0,
                                    "page_space_used_percent": round(float(row[6]) if len(row) > 6 and row[6] else 0, 2)
                                })
                        entry["database_health"]["index_fragmentation"] = frag
                    except Exception as e:
                        entry["errors"].append(f"index_fragmentation: {e}")

                    # 3. Missing Indexes (suggestions)
                    try:
                        sql = """
                            SELECT TOP 20
                                DB_NAME(database_id) as database_name,
                                OBJECT_SCHEMA_NAME(object_id, database_id) as schema_name,
                                OBJECT_NAME(object_id, database_id) as table_name,
                                equality_columns,
                                inequality_columns,
                                included_columns,
                                user_seeks,
                                user_scans,
                                avg_total_user_cost,
                                avg_user_impact
                            FROM sys.dm_db_missing_index_details mid
                            INNER JOIN sys.dm_db_missing_index_groups mig ON mid.index_handle = mig.index_handle
                            INNER JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
                            WHERE database_id > 4  -- Exclude system databases
                            ORDER BY migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) DESC
                        """
                        rows = run_sql(sql)
                        missing = []
                        for row in rows:
                            if isinstance(row, dict):
                                missing.append({
                                    "database_name": row.get("database_name", ""),
                                    "schema_name": row.get("schema_name", ""),
                                    "table_name": row.get("table_name", ""),
                                    "equality_columns": row.get("equality_columns", ""),
                                    "inequality_columns": row.get("inequality_columns", ""),
                                    "included_columns": row.get("included_columns", ""),
                                    "user_seeks": row.get("user_seeks", 0),
                                    "user_scans": row.get("user_scans", 0),
                                    "avg_total_user_cost": round(row.get("avg_total_user_cost", 0), 2),
                                    "avg_user_impact": round(row.get("avg_user_impact", 0), 2)
                                })
                            else:
                                missing.append({
                                    "database_name": str(row[0]) if len(row) > 0 else "",
                                    "schema_name": str(row[1]) if len(row) > 1 else "",
                                    "table_name": str(row[2]) if len(row) > 2 else "",
                                    "equality_columns": str(row[3]) if len(row) > 3 else "",
                                    "inequality_columns": str(row[4]) if len(row) > 4 else "",
                                    "included_columns": str(row[5]) if len(row) > 5 else "",
                                    "user_seeks": row[6] if len(row) > 6 else 0,
                                    "user_scans": row[7] if len(row) > 7 else 0,
                                    "avg_total_user_cost": round(float(row[8]) if len(row) > 8 and row[8] else 0, 2),
                                    "avg_user_impact": round(float(row[9]) if len(row) > 9 and row[9] else 0, 2)
                                })
                        entry["database_health"]["missing_indexes"] = missing
                    except Exception as e:
                        entry["errors"].append(f"missing_indexes: {e}")

                    # 4. Long-Running Active Queries
                    try:
                        sql = """
                            SELECT TOP 20
                                session_id,
                                DB_NAME(database_id) as database_name,
                                start_time,
                                status,
                                command,
                                wait_type,
                                wait_time,
                                cpu_time,
                                total_elapsed_time / 1000 as elapsed_seconds,
                                SUBSTRING(text, (statement_start_offset/2)+1,
                                    ((CASE statement_end_offset
                                        WHEN -1 THEN DATALENGTH(text)
                                        ELSE statement_end_offset
                                    END - statement_start_offset)/2)+1) as query_text
                            FROM sys.dm_exec_requests r
                            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
                            WHERE r.database_id > 4  -- Exclude system databases
                            AND r.status != 'sleeping'
                            AND r.total_elapsed_time > 5000  -- More than 5 seconds
                            ORDER BY r.total_elapsed_time DESC
                        """
                        rows = run_sql(sql)
                        long_running = []
                        for row in rows:
                            if isinstance(row, dict):
                                long_running.append({
                                    "session_id": row.get("session_id"),
                                    "database_name": row.get("database_name", ""),
                                    "start_time": str(row.get("start_time", "")),
                                    "status": row.get("status", ""),
                                    "command": row.get("command", ""),
                                    "wait_type": row.get("wait_type", ""),
                                    "wait_time": row.get("wait_time", 0),
                                    "cpu_time": row.get("cpu_time", 0),
                                    "elapsed_seconds": round(row.get("elapsed_seconds", 0), 2),
                                    "query_text": (row.get("query_text", "") or "")[:500]  # Truncate
                                })
                            else:
                                long_running.append({
                                    "session_id": row[0] if len(row) > 0 else None,
                                    "database_name": str(row[1]) if len(row) > 1 else "",
                                    "start_time": str(row[2]) if len(row) > 2 else "",
                                    "status": str(row[3]) if len(row) > 3 else "",
                                    "command": str(row[4]) if len(row) > 4 else "",
                                    "wait_type": str(row[5]) if len(row) > 5 else "",
                                    "wait_time": row[6] if len(row) > 6 else 0,
                                    "cpu_time": row[7] if len(row) > 7 else 0,
                                    "elapsed_seconds": round(float(row[8]) if len(row) > 8 and row[8] else 0, 2),
                                    "query_text": (str(row[9]) if len(row) > 9 else "")[:500]
                                })
                        entry["database_health"]["long_running_queries"] = long_running
                    except Exception as e:
                        entry["errors"].append(f"long_running_queries: {e}")

                    # 5. Buffer Pool Statistics
                    try:
                        sql = """
                            SELECT 
                                (SELECT TOP 1 cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Page life expectancy' AND instance_name = '') as page_life_expectancy,
                                (SELECT TOP 1 cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Buffer cache hit ratio' AND instance_name = '') as buffer_cache_hit_ratio,
                                (SELECT TOP 1 cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Buffer cache hit ratio base' AND instance_name = '') as buffer_cache_hit_ratio_base,
                                (SELECT TOP 1 cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Page reads/sec' AND instance_name = '') as page_reads_per_sec,
                                (SELECT TOP 1 cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Page writes/sec' AND instance_name = '') as page_writes_per_sec
                        """
                        rows = run_sql(sql)
                        if rows:
                            row = rows[0]
                            if isinstance(row, dict):
                                entry["database_health"]["buffer_pool_stats"] = {
                                    "page_life_expectancy": row.get("page_life_expectancy"),
                                    "buffer_cache_hit_ratio": row.get("buffer_cache_hit_ratio"),
                                    "buffer_cache_hit_ratio_base": row.get("buffer_cache_hit_ratio_base"),
                                    "page_reads_per_sec": row.get("page_reads_per_sec"),
                                    "page_writes_per_sec": row.get("page_writes_per_sec")
                                }
                                # Calculate actual hit ratio percentage
                                hit_ratio = row.get("buffer_cache_hit_ratio")
                                hit_ratio_base = row.get("buffer_cache_hit_ratio_base")
                                if hit_ratio and hit_ratio_base and hit_ratio_base > 0:
                                    entry["database_health"]["buffer_pool_stats"]["buffer_cache_hit_ratio_percent"] = round((hit_ratio / hit_ratio_base) * 100, 2)
                            else:
                                entry["database_health"]["buffer_pool_stats"] = {
                                    "page_life_expectancy": row[0] if len(row) > 0 else None,
                                    "buffer_cache_hit_ratio": row[1] if len(row) > 1 else None,
                                    "buffer_cache_hit_ratio_base": row[2] if len(row) > 2 else None,
                                    "page_reads_per_sec": row[3] if len(row) > 3 else None,
                                    "page_writes_per_sec": row[4] if len(row) > 4 else None
                                }
                                # Calculate actual hit ratio percentage
                                hit_ratio = row[1] if len(row) > 1 else None
                                hit_ratio_base = row[2] if len(row) > 2 else None
                                if hit_ratio and hit_ratio_base and hit_ratio_base > 0:
                                    entry["database_health"]["buffer_pool_stats"]["buffer_cache_hit_ratio_percent"] = round((hit_ratio / hit_ratio_base) * 100, 2)
                        entry["database_health"]["page_life_expectancy"] = entry["database_health"]["buffer_pool_stats"].get("page_life_expectancy")
                    except Exception as e:
                        entry["errors"].append(f"buffer_pool_stats: {e}")

                    # 6. I/O Statistics by Database
                    try:
                        sql = """
                            SELECT TOP 20
                                DB_NAME(database_id) as database_name,
                                file_id,
                                num_of_reads,
                                num_of_writes,
                                io_stall_read_ms,
                                io_stall_write_ms,
                                io_stall,
                                num_of_bytes_read,
                                num_of_bytes_written
                            FROM sys.dm_io_virtual_file_stats(NULL, NULL)
                            WHERE database_id > 4  -- Exclude system databases
                            ORDER BY io_stall DESC
                        """
                        rows = run_sql(sql)
                        io_stats = []
                        for row in rows:
                            if isinstance(row, dict):
                                io_stats.append({
                                    "database_name": row.get("database_name", ""),
                                    "file_id": row.get("file_id"),
                                    "num_of_reads": row.get("num_of_reads", 0),
                                    "num_of_writes": row.get("num_of_writes", 0),
                                    "io_stall_read_ms": row.get("io_stall_read_ms", 0),
                                    "io_stall_write_ms": row.get("io_stall_write_ms", 0),
                                    "io_stall_ms": row.get("io_stall", 0),
                                    "bytes_read": row.get("num_of_bytes_read", 0),
                                    "bytes_written": row.get("num_of_bytes_written", 0)
                                })
                            else:
                                io_stats.append({
                                    "database_name": str(row[0]) if len(row) > 0 else "",
                                    "file_id": row[1] if len(row) > 1 else None,
                                    "num_of_reads": row[2] if len(row) > 2 else 0,
                                    "num_of_writes": row[3] if len(row) > 3 else 0,
                                    "io_stall_read_ms": row[4] if len(row) > 4 else 0,
                                    "io_stall_write_ms": row[5] if len(row) > 5 else 0,
                                    "io_stall_ms": row[6] if len(row) > 6 else 0,
                                    "bytes_read": row[7] if len(row) > 7 else 0,
                                    "bytes_written": row[8] if len(row) > 8 else 0
                                })
                        entry["database_health"]["io_stats"] = io_stats
                    except Exception as e:
                        entry["errors"].append(f"io_stats: {e}")

                    # 7. Transaction Log Statistics
                    try:
                        sql = """
                            SELECT 
                                DB_NAME(database_id) as database_name,
                                name as logical_name,
                                physical_name,
                                (size * 8.0 / 1024) as size_mb,
                                ((size - CAST(FILEPROPERTY(name, 'SpaceUsed') AS int)) * 8.0 / 1024) as free_space_mb,
                                CAST(FILEPROPERTY(name, 'SpaceUsed') AS int) * 8.0 / 1024 as used_space_mb,
                                (CAST(FILEPROPERTY(name, 'SpaceUsed') AS float) / size * 100) as percent_used
                            FROM sys.master_files
                            WHERE type_desc = 'LOG'
                            AND database_id > 4  -- Exclude system databases
                            ORDER BY database_id
                        """
                        rows = run_sql(sql)
                        tlog_stats = []
                        for row in rows:
                            if isinstance(row, dict):
                                tlog_stats.append({
                                    "database_name": row.get("database_name", ""),
                                    "logical_name": row.get("logical_name", ""),
                                    "physical_name": row.get("physical_name", ""),
                                    "size_mb": round(row.get("size_mb", 0), 2),
                                    "free_space_mb": round(row.get("free_space_mb", 0), 2),
                                    "used_space_mb": round(row.get("used_space_mb", 0), 2),
                                    "percent_used": round(row.get("percent_used", 0), 2)
                                })
                            else:
                                tlog_stats.append({
                                    "database_name": str(row[0]) if len(row) > 0 else "",
                                    "logical_name": str(row[1]) if len(row) > 1 else "",
                                    "physical_name": str(row[2]) if len(row) > 2 else "",
                                    "size_mb": round(float(row[3]) if len(row) > 3 and row[3] else 0, 2),
                                    "free_space_mb": round(float(row[4]) if len(row) > 4 and row[4] else 0, 2),
                                    "used_space_mb": round(float(row[5]) if len(row) > 5 and row[5] else 0, 2),
                                    "percent_used": round(float(row[6]) if len(row) > 6 and row[6] else 0, 2)
                                })
                        entry["database_health"]["transaction_log_stats"] = tlog_stats
                    except Exception as e:
                        entry["errors"].append(f"transaction_log_stats: {e}")

                except Exception as e:
                    entry["errors"].append(f"database_health: {e}")

            # --- Error Logs (historical or current) ---
            # Log collection works even without SQL libraries
            # MSSQL logs are stored as a dictionary where keys are log file names
            # (e.g., "docker_logs", "errorlog") and values contain log metadata
            error_logs_dict = {}
            try:
                    if self._is_historical_collection():
                        # Historical collection
                        since, until = self._get_time_window()
                        data_dir = inst.get("data_dir") or "/var/opt/mssql"
                        
                        # Common MSSQL log locations
                        log_paths = [
                            f"{data_dir}/log/errorlog",
                            f"{data_dir}/log/errorlog.*",
                            "/var/opt/mssql/log/errorlog",
                            "/var/opt/mssql/log/errorlog.*",
                        ]
                        
                        try:
                            # Try Docker log time filtering first (if container)
                            from connectors.host_connectors.docker_host_connector import DockerHostConnector
                            if container_id and isinstance(connector, DockerHostConnector):
                                logger.info(f"Collecting MSSQL logs from Docker for container {container_id}")
                                mssql_logs = connector.get_container_logs_time_filtered(container_id, since, until)
                                if mssql_logs and not mssql_logs.startswith("[docker"):  # Success if not error message
                                    error_logs_dict["docker_logs"] = {
                                        "content": mssql_logs.strip(),
                                        "type": "historical",
                                        "source": "docker_logs",
                                        "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                                        "collection_mode": "historical",
                                        "line_count": len(mssql_logs.splitlines())
                                    }
                                else:
                                    raise Exception("Docker log filtering failed")
                            elif container_id and hasattr(connector, "exec_cmd"):
                                # SSHHostConnector: use docker logs command with time filtering
                                logger.info(f"Collecting MSSQL logs from Docker (via SSH) for container {container_id}")
                                # Note: docker logs doesn't support time filtering directly, so we get all logs
                                # For historical collection, get all logs and filter by time if needed
                                docker_logs_cmd = f"docker logs {container_id} 2>&1"
                                out = connector.exec_cmd(docker_logs_cmd)
                                if isinstance(out, dict):
                                    docker_logs = out.get("stdout", "").strip()
                                else:
                                    docker_logs = str(out).strip()
                                
                                if docker_logs and not docker_logs.startswith("[") and "error" not in docker_logs.lower()[:50]:
                                    # Filter logs by time window if needed (basic filtering)
                                    # For now, just collect all logs for historical mode
                                    error_logs_dict["docker_logs"] = {
                                        "content": docker_logs,
                                        "type": "historical",
                                        "source": "docker_logs",
                                        "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                                        "collection_mode": "historical",
                                        "line_count": len(docker_logs.splitlines())
                                    }
                                else:
                                    raise Exception("Docker logs unavailable via SSH")
                            else:
                                raise Exception("Docker log filtering not available")
                        except Exception as e:
                            logger.debug("Docker log filtering failed, using file-based: %s", e)
                            # Fall back to file-based historical collection
                            actual_log_paths = []
                            for pattern in log_paths:
                                if "*" not in pattern:
                                    actual_log_paths.append(pattern)
                                else:
                                    base_path = pattern.replace(".*", "")
                                    actual_log_paths.append(base_path)
                            
                            mssql_logs = self._collect_historical_logs(
                                host_connector=connector,
                                container_id=container_id,
                                log_paths=actual_log_paths,
                                parser_name="syslog",  # Use syslog parser as fallback
                                since=since,
                                until=until,
                                max_lines=10000
                            )
                            if mssql_logs:
                                error_logs_dict["errorlog"] = {
                                "content": mssql_logs.strip(),
                                "type": "historical",
                                "source": "file",
                                "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                                "collection_mode": "historical",
                                    "line_count": len(mssql_logs.splitlines())
                                }
                            
                            # Also try to collect SQL Agent logs (historical)
                            agent_log_paths = [
                                f"{data_dir}/log/sqlagentstartup.log",
                                "/var/opt/mssql/log/sqlagentstartup.log"
                            ]
                            agent_logs = self._collect_historical_logs(
                                host_connector=connector,
                                container_id=container_id,
                                log_paths=agent_log_paths,
                                parser_name="syslog",
                                since=since,
                                until=until,
                                max_lines=1000
                            )
                            if agent_logs:
                                error_logs_dict["sqlagentstartup.log"] = {
                                    "content": agent_logs.strip(),
                                    "type": "historical",
                                    "source": "file",
                                    "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                                    "collection_mode": "historical",
                                    "line_count": len(agent_logs.splitlines())
                            }
                    else:
                        # Current collection - try Docker logs first, then file-based
                        docker_logs_collected = False
                        try:
                            from connectors.host_connectors.docker_host_connector import DockerHostConnector
                            if container_id and isinstance(connector, DockerHostConnector):
                                logger.info(f"Collecting MSSQL logs from Docker for container {container_id}")
                                docker_logs = connector.get_container_logs(container_id, tail=200)
                                if docker_logs and not docker_logs.startswith("[docker"):
                                    error_logs_dict["docker_logs"] = {
                                        "content": docker_logs.strip(),
                                        "type": "current",
                                        "source": "docker_logs",
                                        "collection_mode": "current",
                                        "line_count": len(docker_logs.splitlines())
                                    }
                                    docker_logs_collected = True
                                else:
                                    raise Exception("Docker logs unavailable")
                            elif container_id and hasattr(connector, "exec_cmd"):
                                # SSHHostConnector: use docker logs command
                                logger.info(f"Collecting MSSQL logs from Docker (via SSH) for container {container_id}")
                                docker_logs_cmd = f"docker logs --tail 200 {container_id} 2>&1"
                                out = connector.exec_cmd(docker_logs_cmd)
                                if isinstance(out, dict):
                                    docker_logs = out.get("stdout", "").strip()
                                else:
                                    docker_logs = str(out).strip()
                                
                                if docker_logs and not docker_logs.startswith("[") and "error" not in docker_logs.lower()[:50]:
                                    error_logs_dict["docker_logs"] = {
                                        "content": docker_logs,
                                        "type": "current",
                                        "source": "docker_logs",
                                        "collection_mode": "current",
                                        "line_count": len(docker_logs.splitlines())
                                    }
                                    docker_logs_collected = True
                                else:
                                    raise Exception("Docker logs unavailable via SSH")
                            else:
                                raise Exception("Docker logs not available")
                        except Exception as e:
                            logger.debug("Docker logs failed, trying file-based: %s", e)
                            # Fallback to file-based logs
                            data_dir = inst.get("data_dir") or "/var/opt/mssql"
                            log_cmd = f"tail -n 200 {data_dir}/log/errorlog 2>/dev/null || tail -n 200 /var/opt/mssql/log/errorlog 2>/dev/null || echo ''"
                            mssql_logs = self._exec(connector, container_id, log_cmd)
                            if mssql_logs and not mssql_logs.startswith("["):
                                error_logs_dict["errorlog"] = {
                                    "content": mssql_logs.strip(),
                                    "type": "current",
                                    "source": "file",
                                    "collection_mode": "current",
                                    "line_count": len(mssql_logs.splitlines())
                                }
                        
                        # Always try to collect SQL Agent logs from files (even if Docker logs were collected)
                        # SQL Agent logs are not typically in Docker stdout/stderr
                        data_dir = inst.get("data_dir") or "/var/opt/mssql"
                        agent_log_cmd = f"tail -n 100 {data_dir}/log/sqlagentstartup.log 2>/dev/null || tail -n 100 /var/opt/mssql/log/sqlagentstartup.log 2>/dev/null || echo ''"
                        agent_logs = self._exec(connector, container_id, agent_log_cmd)
                        if agent_logs and not agent_logs.startswith("[") and agent_logs.strip():
                            error_logs_dict["sqlagentstartup.log"] = {
                                "content": agent_logs.strip(),
                        "type": "current",
                                "source": "file",
                                "collection_mode": "current",
                                "line_count": len(agent_logs.splitlines())
                            }
                        
                        # Check for trace files and extended events files (note their presence)
                        # These are binary/XML formats that require special tools to read
                        trace_check_cmd = f"ls -1 {data_dir}/log/*.trc 2>/dev/null | head -5 || ls -1 /var/opt/mssql/log/*.trc 2>/dev/null | head -5 || echo ''"
                        trace_files = self._exec(connector, container_id, trace_check_cmd)
                        if trace_files and not trace_files.startswith("[") and trace_files.strip():
                            trace_list = [f.strip() for f in trace_files.splitlines() if f.strip()]
                            if trace_list:
                                entry["trace_files_available"] = trace_list
                                entry["note_trace_files"] = "Trace files (.trc) are binary format and require SQL Server tools to read. Files available but not collected."
                        
                        xel_check_cmd = f"ls -1 {data_dir}/log/*.xel 2>/dev/null | head -5 || ls -1 /var/opt/mssql/log/*.xel 2>/dev/null | head -5 || echo ''"
                        xel_files = self._exec(connector, container_id, xel_check_cmd)
                        if xel_files and not xel_files.startswith("[") and xel_files.strip():
                            xel_list = [f.strip() for f in xel_files.splitlines() if f.strip()]
                            if xel_list:
                                entry["extended_events_files_available"] = xel_list
                                entry["note_xel_files"] = "Extended Events files (.xel) are XML format and can be large. Files available but not collected. Use SQL Server tools to read."
            except Exception as e:
                logger.warning("Failed to collect MSSQL error logs: %s", e)
            
            # Store error logs dictionary (empty if collection failed)
            entry["error_logs"] = error_logs_dict
            # Also store in "logs" field so orchestrator can detect data presence
            entry["logs"] = error_logs_dict

            # ---- METRICS COLLECTION ----
            try:
                metrics = {}
                
                # 1. Active connections summary
                connections_total = entry.get("connections_total")
                if connections_total:
                    try:
                        metrics["active_connections"] = int(connections_total)
                    except (ValueError, TypeError):
                        pass
                
                # Parse connections_by_state for detailed breakdown
                connections_by_state = entry.get("connections_by_state", [])
                if connections_by_state:
                    for conn_state in connections_by_state:
                        if isinstance(conn_state, dict):
                            state = conn_state.get("status", "").strip()
                            count = conn_state.get("count", 0)
                            if state:
                                metrics[f"connections_{state.lower()}"] = int(count) if count else 0
                
                # 2. Database count
                databases = entry.get("databases", [])
                if databases:
                    metrics["database_count"] = len(databases)
                
                # 3. Table count
                tables = entry.get("tables", [])
                if tables:
                    metrics["table_count"] = len(tables)
                
                # 4. Blocking sessions count
                blocking_info = entry.get("blocking_info", [])
                if blocking_info:
                    metrics["blocking_sessions"] = len(blocking_info)
                
                # 5. Deadlock count
                deadlock_info = entry.get("deadlock_info", [])
                if deadlock_info:
                    metrics["deadlock_count"] = len(deadlock_info)
                
                # 6. Top wait time (from wait stats)
                wait_stats = entry.get("wait_stats", [])
                if wait_stats:
                    total_wait_time = sum(w.get("wait_time_ms", 0) if isinstance(w, dict) else 0 for w in wait_stats)
                    metrics["total_wait_time_ms"] = total_wait_time
                    if wait_stats:
                        top_wait = wait_stats[0]
                        if isinstance(top_wait, dict):
                            metrics["top_wait_type"] = top_wait.get("wait_type", "")
                            metrics["top_wait_time_ms"] = top_wait.get("wait_time_ms", 0)
                
                # 7. Database Health Metrics
                db_health = entry.get("database_health", {})
                
                # Page Life Expectancy (critical for buffer pool health)
                page_life_expectancy = db_health.get("page_life_expectancy")
                if page_life_expectancy:
                    try:
                        metrics["page_life_expectancy"] = int(page_life_expectancy)
                    except (ValueError, TypeError):
                        pass
                
                # Buffer Pool Hit Ratio
                buffer_pool_stats = db_health.get("buffer_pool_stats", {})
                if buffer_pool_stats:
                    hit_ratio_percent = buffer_pool_stats.get("buffer_cache_hit_ratio_percent")
                    if hit_ratio_percent:
                        try:
                            metrics["buffer_cache_hit_ratio_percent"] = round(float(hit_ratio_percent), 2)
                        except (ValueError, TypeError):
                            pass
                
                # Fragmented Indexes Count
                index_frag = db_health.get("index_fragmentation", [])
                if index_frag:
                    metrics["fragmented_indexes_count"] = len(index_frag)
                    # Count high fragmentation (>30%)
                    high_frag = [idx for idx in index_frag if isinstance(idx, dict) and idx.get("fragmentation_percent", 0) > 30]
                    if high_frag:
                        metrics["highly_fragmented_indexes_count"] = len(high_frag)
                
                # Missing Indexes Count
                missing_indexes = db_health.get("missing_indexes", [])
                if missing_indexes:
                    metrics["missing_indexes_count"] = len(missing_indexes)
                
                # Long-Running Queries Count
                long_running = db_health.get("long_running_queries", [])
                if long_running:
                    metrics["long_running_queries_count"] = len(long_running)
                    # Average elapsed time
                    if long_running:
                        avg_elapsed = sum(q.get("elapsed_seconds", 0) if isinstance(q, dict) else 0 for q in long_running) / len(long_running)
                        metrics["avg_long_running_query_seconds"] = round(avg_elapsed, 2)
                
                # Total Database Size (from file_sizes)
                file_sizes = db_health.get("file_sizes", [])
                if file_sizes:
                    total_size_mb = sum(f.get("size_mb", 0) if isinstance(f, dict) else 0 for f in file_sizes)
                    metrics["total_database_size_mb"] = round(total_size_mb, 2)
                
                # Transaction Log Usage
                tlog_stats = db_health.get("transaction_log_stats", [])
                if tlog_stats:
                    # Find databases with high log usage (>80%)
                    high_usage = [tlog for tlog in tlog_stats if isinstance(tlog, dict) and tlog.get("percent_used", 0) > 80]
                    if high_usage:
                        metrics["databases_with_high_log_usage"] = len(high_usage)
                        metrics["high_log_usage_databases"] = [tlog.get("database_name", "") for tlog in high_usage if isinstance(tlog, dict)]
                
                # Store metrics
                if metrics:
                    entry["metrics"] = metrics
                    
            except Exception as e:
                logger.debug("Failed to collect MSSQL metrics: %s", e, exc_info=True)
                # Metrics are optional - don't fail collection

            findings["discovered"].append(inst_name)
            findings["instances"].append(entry)
            
            # Debug: Log entry summary for troubleshooting
            has_logs = bool(entry.get("logs", {}))
            has_metrics = bool(entry.get("metrics", {}))
            entry_status = entry.get("status", "not_set")
            logger.info(f"MSSQL adapter: Entry '{inst_name}' added to findings - status: {entry_status}, has_logs: {has_logs}, has_metrics: {has_metrics}")

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
        
        # In host-centric structure, find which hosts have mssql by checking their services
        all_hosts = self.env_config.get("hosts", [])
        
        for host in all_hosts:
            host_name = host.get("name")
            host_services = host.get("services", {})
            
            # Check if this host has mssql service
            if "mssql" not in host_services:
                continue
            
            # Get mssql service config for this host
            mssql_cfg = host_services["mssql"]
            
            # Update component_config to this host's mssql config
            original_instances = self.instances_cfg
            self.instances_cfg = mssql_cfg.get("instances", [])
            
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

