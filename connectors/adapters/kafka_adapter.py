# connectors/adapters/kafka_adapter.py

import logging
import traceback
import re
import shlex
from connectors.base_connector import BaseConnector
from connectors.historical import HistoricalLogCollectionMixin

logger = logging.getLogger("grcai.adapters.kafka")

DEFAULT_KAFKA_HOME = "/opt/kafka"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


def _parse_kafka_properties(text: str):
    """
    Parse Kafka properties file (key=value format) into a dict.
    Handles comments and empty lines.
    """
    out = {}
    if not text:
        return out
    
    # Handle string representation of bytes (like "b'...'")
    if isinstance(text, str) and (text.startswith("b'") or text.startswith('b"')):
        try:
            import ast
            if text.startswith("b'") and text.endswith("'"):
                bytes_obj = ast.literal_eval(text)
                if isinstance(bytes_obj, bytes):
                    text = bytes_obj.decode('utf-8', errors='replace')
            elif text.startswith('b"') and text.endswith('"'):
                bytes_obj = ast.literal_eval(text)
                if isinstance(bytes_obj, bytes):
                    text = bytes_obj.decode('utf-8', errors='replace')
        except (ValueError, SyntaxError):
            # If evaluation fails, try to extract content manually
            if text.startswith("b'") and text.endswith("'"):
                text = text[2:-1].replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r")
            elif text.startswith('b"') and text.endswith('"'):
                text = text[2:-1].replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r")
    
    # Split into lines and parse
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        
        if "=" in line:
            parts = line.split("=", 1)
            key = parts[0].strip()
            value = parts[1].strip()
            if key:  # Only add non-empty keys
                out[key] = value
    
    return out


def _parse_topic_describe(text: str):
    """
    Parse output from `kafka-topics.sh --describe` into structured data.
    Example output:
    Topic: my-topic	PartitionCount: 3	ReplicationFactor: 2	Configs: segment.ms=1000
    	Topic: my-topic	Partition: 0	Leader: 1	Replicas: 1,2	Isr: 1,2
    	Topic: my-topic	Partition: 1	Leader: 2	Replicas: 2,1	Isr: 2,1
    	Topic: my-topic	Partition: 2	Leader: 1	Replicas: 1,2	Isr: 1,2
    """
    topics = {}
    current_topic = None
    
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        
        # Topic header line
        if line.startswith("Topic:") and "PartitionCount:" in line:
            # Extract topic name and metadata
            topic_match = re.search(r'Topic:\s+(\S+)', line)
            partition_match = re.search(r'PartitionCount:\s+(\d+)', line)
            replication_match = re.search(r'ReplicationFactor:\s+(\d+)', line)
            configs_match = re.search(r'Configs:\s+(.+)', line)
            
            if topic_match:
                topic_name = topic_match.group(1)
                current_topic = topic_name
                topics[topic_name] = {
                    "name": topic_name,
                    "partition_count": int(partition_match.group(1)) if partition_match else 0,
                    "replication_factor": int(replication_match.group(1)) if replication_match else 0,
                    "configs": configs_match.group(1) if configs_match else "",
                    "partitions": []
                }
        
        # Partition detail line
        elif line.startswith("Topic:") and "Partition:" in line and current_topic:
            partition_match = re.search(r'Partition:\s+(\d+)', line)
            leader_match = re.search(r'Leader:\s+(\d+)', line)
            replicas_match = re.search(r'Replicas:\s+([\d,]+)', line)
            isr_match = re.search(r'Isr:\s+([\d,]+)', line)
            
            if partition_match:
                partition_info = {
                    "partition": int(partition_match.group(1)),
                    "leader": int(leader_match.group(1)) if leader_match else None,
                    "replicas": [int(x) for x in replicas_match.group(1).split(",")] if replicas_match else [],
                    "isr": [int(x) for x in isr_match.group(1).split(",")] if isr_match else []
                }
                topics[current_topic]["partitions"].append(partition_info)
    
    return topics


def _parse_consumer_groups_describe(text: str):
    """
    Parse output from `kafka-consumer-groups.sh --describe --group <group>`.
    Example output:
    GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
    my-group        my-topic        0          100              200             100             consumer-1      /127.0.0.1      consumer-1
    """
    groups = {}
    lines = text.splitlines()
    
    if len(lines) < 2:
        return groups
    
    # Skip header line
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split()
        if len(parts) >= 6:
            group = parts[0]
            topic = parts[1]
            partition = parts[2]
            current_offset = parts[3]
            log_end_offset = parts[4]
            lag = parts[5]
            consumer_id = parts[6] if len(parts) > 6 else ""
            host = parts[7] if len(parts) > 7 else ""
            client_id = parts[8] if len(parts) > 8 else ""
            
            if group not in groups:
                groups[group] = {
                    "group_id": group,
                    "topics": {},
                    "total_lag": 0
                }
            
            if topic not in groups[group]["topics"]:
                groups[group]["topics"][topic] = []
            
            try:
                lag_int = int(lag)
                groups[group]["total_lag"] += lag_int
            except (ValueError, TypeError):
                lag_int = 0
            
            groups[group]["topics"][topic].append({
                "partition": int(partition),
                "current_offset": current_offset,
                "log_end_offset": log_end_offset,
                "lag": lag_int,
                "consumer_id": consumer_id,
                "host": host,
                "client_id": client_id
            })
    
    return groups


class KafkaAdapter(BaseConnector, HistoricalLogCollectionMixin):
    """
    KafkaAdapter - diagnostics for Kafka broker instances.
    
    Collects:
    - Broker version and API information
    - Topics and partition details
    - Consumer groups and lag information
    - Server configuration
    - Logs (with historical support)
    - Metrics
    """
    
    def __init__(self, name=None, env_config=None, issue_time=None, component_config=None, **kwargs):
        if name is None:
            name = "kafka"
        if env_config is None:
            env_config = {"hosts": []}
        
        super().__init__(name=name, issue_time=issue_time, component_config=component_config)
        
        self.env_config = env_config or {}
        self.component_config = component_config or {}
        self.instances_cfg = self.component_config.get("instances", [])
        self.hosts = self.env_config.get("hosts", [])
        
        logger.info(
            "KafkaAdapter initialized; instances=%d hosts=%s",
            len(self.instances_cfg), [h.get("name") for h in (self.hosts or [])]
        )
    
    # --------------------------
    # Host connector selection
    # --------------------------
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
    
    # --------------------------
    # Exec helpers
    # --------------------------
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
                        error_preview = error_msg[:100] if error_msg else "None"
                        logger.debug(f"Kafka adapter: docker exec failed: rc={rc}, stderr='{error_preview}'")
                        return f"[docker exec error: {error_msg}]"
                    return str(out or "")
                except Exception as e:
                    logger.warning(f"Kafka adapter: docker exec exception: {e}", exc_info=True)
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
        
        # No container_id or container execution failed: execute on host
        if hasattr(connector, "exec_cmd"):
            try:
                out = connector.exec_cmd(cmd)
                if isinstance(out, dict):
                    result = out.get("stdout") or out.get("output") or ""
                    # Ensure result is always a string
                    return str(result) if result is not None else ""
                # Ensure out is always a string
                return str(out) if out is not None else ""
            except Exception as e:
                return f"[host exec error: {e}]"
        
        return "[no exec method]"
    
    def _read_file(self, connector, container_id, path):
        """Read a file from container or host."""
        def _decode_content(content):
            """Helper to decode bytes or handle string representations of bytes."""
            if content is None:
                return ""
            # If it's bytes, decode it
            if isinstance(content, bytes):
                return content.decode('utf-8', errors='replace')
            # If it's a string, check if it's a string representation of bytes
            if isinstance(content, str):
                # Handle string representation of bytes like "b'...'" or 'b"...'
                if (content.startswith("b'") and content.endswith("'")) or \
                   (content.startswith('b"') and content.endswith('"')):
                    try:
                        # Use ast.literal_eval to safely evaluate the bytes literal
                        import ast
                        bytes_obj = ast.literal_eval(content)
                        if isinstance(bytes_obj, bytes):
                            return bytes_obj.decode('utf-8', errors='replace')
                    except (ValueError, SyntaxError):
                        # If evaluation fails, try manual extraction
                        if content.startswith("b'") and content.endswith("'"):
                            # Remove b' and ', then unescape
                            inner = content[2:-1]
                            return inner.encode('utf-8').decode('unicode_escape').encode('latin1').decode('utf-8', errors='replace')
                        elif content.startswith('b"') and content.endswith('"'):
                            inner = content[2:-1]
                            return inner.encode('utf-8').decode('unicode_escape').encode('latin1').decode('utf-8', errors='replace')
                return content
            # Otherwise, convert to string
            return str(content)
        
        if container_id and hasattr(connector, "read_file_in_container"):
            try:
                out = connector.read_file_in_container(container_id, path)
                if isinstance(out, dict):
                    result = out.get("stdout") or out.get("output") or ""
                    return _decode_content(result)
                return _decode_content(out)
            except Exception as e:
                return f"[docker-host read error: {e}]"
        
        if hasattr(connector, "read_file"):
            try:
                out = connector.read_file(path)
                if isinstance(out, dict):
                    result = out.get("stdout") or out.get("output") or ""
                    return _decode_content(result)
                return _decode_content(out)
            except Exception as e:
                return f"[host read error: {e}]"
        
        # Fallback: use cat with timeout
        try:
            return self._exec(connector, container_id, f"timeout 2 cat {shlex.quote(path)} 2>/dev/null || true", timeout=2)
        except Exception as e:
            return f"[cat fallback error: {e}]"
    
    # --------------------------
    # Kafka CLI helpers
    # --------------------------
    def _get_kafka_home(self, inst):
        """Get Kafka home directory from instance config or use default."""
        return inst.get("kafka_home") or DEFAULT_KAFKA_HOME
    
    def _get_bootstrap_servers(self, inst):
        """Get bootstrap servers from instance config or use default."""
        return inst.get("bootstrap_servers") or DEFAULT_BOOTSTRAP_SERVERS
    
    def _run_kafka_cmd(self, connector, container_id, inst, script_name, args=""):
        """
        Run a Kafka CLI script.
        
        Args:
            connector: Host connector
            container_id: Container ID (if applicable)
            inst: Instance config dict
            script_name: Name of Kafka script (e.g., "kafka-topics.sh")
            args: Arguments to pass to the script
        """
        kafka_home = self._get_kafka_home(inst)
        bootstrap_servers = self._get_bootstrap_servers(inst)
        
        # Try common script locations
        script_paths = [
            f"{kafka_home}/bin/{script_name}",
            f"{kafka_home}/bin/kafka-run-class.sh org.apache.kafka.tools.{script_name.replace('.sh', '')}",
            f"/usr/bin/{script_name}",
            script_name  # Try in PATH
        ]
        
        for script_path in script_paths:
            # Build command with bootstrap servers if needed
            if "--bootstrap-server" not in args and "--zookeeper" not in args:
                if script_name in ["kafka-topics.sh", "kafka-consumer-groups.sh", "kafka-broker-api-versions.sh"]:
                    args = f"--bootstrap-server {bootstrap_servers} {args}"
            
            # Wrap command with timeout at shell level for extra safety
            # This ensures commands fail fast even if _exec timeout doesn't work
            # Use very short timeout (2s) to avoid hitting orchestrator's 20s timeout
            # Multiple commands can run, so each needs to be very fast
            cmd = f"timeout 2 {script_path} {args}"
            out = self._exec(connector, container_id, cmd, timeout=2)
            
            # Ensure out is always a string to prevent 'int' object is not subscriptable
            if not isinstance(out, str):
                out = str(out) if out is not None else ""
            
            # Check for timeout indicators
            if "timeout" in out.lower() or "timed out" in out.lower():
                logger.debug(f"Kafka adapter: Command timed out: {script_name}")
                continue  # Try next script path
            
            # If we got output and it's not an error, return it
            if out and not out.startswith("[") and "error" not in out.lower()[:50]:
                return out
        
        return ""
    
    # --------------------------
    # Per-host collection
    # --------------------------
    def collect_for_host(self, host_info, connector):
        result = {
            "type": "kafka",
            "discovered": [],
            "instances": [],
            "collection_mode": "historical" if self._is_historical_collection() else "current"
        }
        
        if self._is_historical_collection():
            since, until = self._get_time_window()
            result["time_window"] = {
                "since": since.isoformat(),
                "until": until.isoformat(),
                "issue_time": self.issue_time.isoformat() if self.issue_time else None
            }
        
        # Map docker containers
        # Handle both SSHHostConnector format (dict) and DockerHostConnector format (list)
        docker_map = {}
        if hasattr(connector, "list_containers"):
            try:
                containers_result = connector.list_containers()
                logger.info(f"Kafka adapter: list_containers returned {len(containers_result) if containers_result else 0} containers (type: {type(containers_result).__name__})")
                
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
                                        logger.info(f"Kafka adapter: Mapped container '{name}' -> {container_id[:12]}")
                
                # Handle DockerHostConnector format: List[Container] with .name and .id
                elif isinstance(containers_result, list):
                    for c in containers_result:
                        # Docker SDK container objects have:
                        # - .name (property, returns container name, may have leading '/')
                        # - .id (property, returns full container ID)
                        name = None
                        cid = None
                        
                        # Try to get name
                        if hasattr(c, "name"):
                            name = c.name
                        elif hasattr(c, "attrs") and isinstance(c.attrs, dict):
                            # Fallback: get from attrs
                            names = c.attrs.get("Names", [])
                            # Ensure names is a list before subscripting
                            if names and isinstance(names, (list, tuple)) and len(names) > 0:
                                name = names[0] if isinstance(names[0], str) else str(names[0])
                        
                        # Try to get ID
                        if hasattr(c, "id"):
                            cid = c.id
                        elif hasattr(c, "attrs") and isinstance(c.attrs, dict):
                            cid = c.attrs.get("Id")
                        
                        # Always convert cid to string immediately
                        if cid is not None:
                            cid = str(cid)
                        
                        if name and cid:
                            # Strip leading '/' from name if present
                            clean_name = name.lstrip('/')
                            # Store both with and without leading '/' for flexibility
                            docker_map[clean_name] = cid
                            if name != clean_name:
                                docker_map[name] = cid
                            logger.info(f"Kafka adapter: Mapped container '{clean_name}' (from '{name}') -> {cid[:12]}")
                        else:
                            logger.warning(f"Kafka adapter: Container missing name or ID: name={name}, id={cid[:12] if cid else None}")
            except Exception as e:
                logger.warning(f"Kafka adapter: Failed to list containers: {e}", exc_info=True)
        
        logger.info(f"Kafka adapter: Container map built with {len(docker_map)} entries: {list(docker_map.keys())}")
        
        # Default instance when none configured
        instances_cfg = self.instances_cfg or [{"name": host_info.get("name"), "container": None}]
        
        for inst in instances_cfg:
            # CRITICAL: Ensure result is still a dict at the start of each iteration
            # This prevents 'int' object has no attribute 'get' errors
            if not isinstance(result, dict):
                logger.error(f"Kafka adapter: result is not a dict at start of instance loop, got {type(result)}. Re-initializing.")
                result = {
                    "type": "kafka",
                    "discovered": [],
                    "instances": [],
                    "collection_mode": "historical" if self._is_historical_collection() else "current"
                }
                if self._is_historical_collection():
                    since, until = self._get_time_window()
                    result["time_window"] = {
                        "since": since.isoformat(),
                        "until": until.isoformat(),
                        "issue_time": self.issue_time.isoformat() if self.issue_time else None
                    }
            name = inst.get("name")
            container_name = inst.get("container")
            container_id_raw = docker_map.get(container_name) if container_name else None
            
            # CRITICAL: Convert container_id to string IMMEDIATELY to prevent 'int' object is not subscriptable errors
            # This must happen before any slicing operations ([:12]) or Docker SDK calls
            container_id = str(container_id_raw) if container_id_raw is not None else None
            
            # If container_name is specified but container_id is None, provide helpful error
            if container_name and container_id is None:
                available_containers = list(docker_map.keys()) if docker_map else []
                error_msg = f"Container '{container_name}' specified in YAML but not found. Available containers: {', '.join(available_containers) if available_containers else 'none'}"
                logger.warning(f"Kafka adapter: {error_msg}")
                instance_res = {
                    "name": name,
                    "container": container_name,
                    "kafka_home": self._get_kafka_home(inst),
                    "bootstrap_servers": self._get_bootstrap_servers(inst),
                    "version": None,
                    "broker_id": None,
                    "broker_info": {},
                    "topics": [],
                    "topics_summary": {},
                    "consumer_groups": [],
                    "consumer_groups_summary": {},
                    "server_properties": {},
                    "logs": {},
                    "metrics": {},
                    "errors": [error_msg]
                }
                result["discovered"].append(name)
                result["instances"].append(instance_res)
                continue
            
            # Log container mapping for debugging
            if container_name:
                if container_id:
                    logger.info(f"Kafka adapter: Found container '{container_name}' -> {container_id[:12]}")
                else:
                    logger.debug(f"Kafka adapter: No container name specified for instance '{name}'")
            else:
                logger.debug(f"Kafka adapter: No container name specified for instance '{name}'")
            
            # Initialize instance_res with all required fields
            # CRITICAL: errors must be a list, not an integer or other type
            instance_res = {
                "name": name,
                "container": container_name,
                "kafka_home": self._get_kafka_home(inst),
                "bootstrap_servers": self._get_bootstrap_servers(inst),
                "version": None,
                "broker_id": None,
                "broker_info": {},
                "topics": [],
                "topics_summary": {},
                "consumer_groups": [],
                "consumer_groups_summary": {},
                "server_properties": {},
                "logs": {},
                "metrics": {},
                "errors": []  # MUST be a list, never an integer
            }
            
            # Defensive check: ensure errors is always a list
            if not isinstance(instance_res.get("errors"), list):
                logger.error(f"Kafka adapter: instance_res['errors'] is not a list for '{name}', got {type(instance_res.get('errors'))}")
                instance_res["errors"] = []

            # ------------------------------------------------------------------
            # LIVENESS: Container-aware checks
            # ------------------------------------------------------------------
            try:
                container_port = inst.get("port", 9092)  # Container port from YAML
                
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
                # Container port 9092 might be mapped to host port 19092
                host_port = container_port  # Default to container port
                if is_docker_container and isinstance(connector, SSHHostConnector):
                    try:
                        # Use docker port command which is simpler and more reliable
                        # Output format: "0.0.0.0:19092" or "[::]:19092" - we extract the port number
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
                                logger.info(f"Kafka adapter: Container port {container_port} mapped to host port {host_port}")
                            else:
                                logger.debug(f"Kafka adapter: Could not extract host port from '{port_str}', using container port {container_port}")
                        else:
                            logger.debug(f"Kafka adapter: docker port returned empty, using container port {container_port}")
                    except Exception as e:
                        logger.debug(f"Kafka adapter: Failed to get host port mapping: {e}, using container port {container_port}")
                
                port = host_port  # Use host port for checks
                
                if is_docker_container:
                    # ============================================================
                    # DOCKER CONTAINER: Use container-aware checks
                    # ============================================================
                    logger.debug(f"Kafka adapter: Using Docker container-aware liveness checks for '{name}'")
                
                    # 0. First, verify container is actually running (not just exists)
                    # Ensure container_id is a string (it should already be from earlier, but be safe)
                    if not container_id:
                        raise RuntimeError(f"kafka container ID is None or empty")
                    
                    container_id_str = str(container_id) if container_id else ""
                    if not container_id_str:
                        raise RuntimeError(f"kafka container ID could not be converted to string")
                    
                    container_running = False
                    container_status = "unknown"
                    try:
                        # DockerHostConnector: use Docker SDK
                        if isinstance(connector, DockerHostConnector) and hasattr(connector, "client") and connector.client:
                            try:
                                container_info = connector.client.containers.get(container_id_str)
                                if container_info is None:
                                    raise RuntimeError(f"kafka container {container_id_str[:12]} not found")
                                
                                # Docker SDK: check status via attrs['State']['Status']
                                container_info.reload()
                                if not hasattr(container_info, 'attrs') or not isinstance(container_info.attrs, dict):
                                    raise RuntimeError(f"kafka container {container_id_str[:12]} has invalid attrs")
                                
                                state = container_info.attrs.get('State', {})
                                if not isinstance(state, dict):
                                    raise RuntimeError(f"kafka container {container_id_str[:12]} has invalid State")
                                
                                container_status = state.get('Status', 'unknown')
                                container_running = container_status == "running"
                                logger.debug(f"Kafka adapter: Container {container_id_str[:12]} status: {container_status}")
                                if not container_running:
                                    logger.warning(f"Kafka adapter: Container {container_id_str[:12]} is not running (status: {container_status})")
                            except Exception as e:
                                error_str = str(e)
                                logger.debug(f"Kafka adapter: Failed to get container status: {e}")
                                # If it's a "not found" error, container doesn't exist
                                if "not found" in error_str.lower() or "no such container" in error_str.lower():
                                    raise RuntimeError(f"kafka container {container_id_str[:12]} not found: {error_str}")
                                container_running = False
                        # SSHHostConnector or other: use docker inspect command
                        elif isinstance(connector, SSHHostConnector) and hasattr(connector, "exec_cmd"):
                            try:
                                inspect_cmd = f"docker inspect --format '{{{{.State.Status}}}}' {container_id_str} 2>/dev/null || echo 'NOT_FOUND'"
                                out = connector.exec_cmd(inspect_cmd)
                                if isinstance(out, dict):
                                    status_output = out.get("stdout", "").strip()
                                else:
                                    status_output = str(out).strip()
                                
                                if status_output == "NOT_FOUND" or not status_output:
                                    raise RuntimeError(f"kafka container {container_id_str[:12]} not found")
                                
                                container_status = status_output.lower()
                                container_running = container_status == "running"
                                logger.debug(f"Kafka adapter: Container {container_id_str[:12]} status: {container_status}")
                                if not container_running:
                                    logger.warning(f"Kafka adapter: Container {container_id_str[:12]} is not running (status: {container_status})")
                            except RuntimeError:
                                raise
                            except Exception as e:
                                logger.debug(f"Kafka adapter: Container status check (docker inspect) failed: {e}")
                                container_running = False
                        else:
                            raise RuntimeError(f"kafka Docker client or exec_cmd not available")
                    except RuntimeError as e:
                        # Re-raise RuntimeError as-is (these are our liveness failures)
                        raise
                    except Exception as e:
                        logger.debug(f"Kafka adapter: Container status check failed: {e}")
                        container_running = False
                    
                    # Also verify we can actually exec into the container (double-check)
                    # Only do this if container appears running from status check
                    if container_running:
                        try:
                            # Try a simple exec to verify container is actually running
                            # Ensure container_id_str is a string before passing to _exec
                            if not isinstance(container_id_str, str):
                                container_id_str = str(container_id_str) if container_id_str else ""
                            test_exec = self._exec(connector, container_id_str, "echo test", timeout=2)
                            # Ensure test_exec is a string before string operations
                            if not isinstance(test_exec, str):
                                test_exec = str(test_exec) if test_exec is not None else ""
                            if test_exec.startswith("[container exec error") or test_exec.startswith("[host exec error"):
                                logger.warning(f"Kafka adapter: Container {container_id_str[:12]} appears running but exec failed: {test_exec[:100] if len(test_exec) > 100 else test_exec}")
                                container_running = False
                        except Exception as e:
                            logger.debug(f"Kafka adapter: Exec test failed: {e}")
                            container_running = False
                    
                    # CRITICAL: If container is not running, fail liveness check immediately
                    # This must happen before any collection operations
                    if not container_running:
                        status_msg = f"status: {container_status}" if container_status != "unknown" else "container not running"
                        raise RuntimeError(f"kafka container {container_id_str[:12] if container_id_str else 'unknown'} is not running ({status_msg})")
                    
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
                                logger.debug(f"Kafka adapter: Port {port} check command output: stdout='{port_str[:200]}', stderr='{port_stderr[:200]}', rc={port_rc}")
                            else:
                                port_str = str(port_out).strip()
                                logger.debug(f"Kafka adapter: Port {port} check command output: '{port_str[:200]}'")
                            
                            port_str_lower = port_str.lower()
                            # Check for success indicators (empty output or connection succeeded)
                            if port_str == "" or port_str_lower == "":
                                port_check_passed = True
                                logger.debug(f"Kafka adapter: Port {port} check passed on remote host (empty output = success)")
                            elif "port_check_failed" not in port_str_lower and "refused" not in port_str_lower and "failed" not in port_str_lower and "connection refused" not in port_str_lower:
                                if "no route" not in port_str_lower and "timeout" not in port_str_lower:
                                    port_check_passed = True
                                    logger.debug(f"Kafka adapter: Port {port} check passed on remote host (no error indicators)")
                                else:
                                    logger.debug(f"Kafka adapter: Port {port} not accessible on remote host: {port_str[:100]}")
                            else:
                                logger.debug(f"Kafka adapter: Port {port} not accessible on remote host: {port_str[:100]}")
                        except Exception as e:
                            logger.warning(f"Kafka adapter: Remote port check (SSH) failed: {e}", exc_info=True)
                    else:
                        # For local DockerHostConnector, use socket check
                        try:
                            import socket
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.settimeout(2)
                            # CRITICAL: Use a different variable name to avoid shadowing the outer 'result' dict
                            connect_result = sock.connect_ex(('127.0.0.1', port))
                            sock.close()
                            if connect_result == 0:
                                port_check_passed = True
                                logger.debug(f"Kafka adapter: Port {port} check passed from host (socket)")
                            else:
                                logger.debug(f"Kafka adapter: Port {port} not accessible from host (socket result: {connect_result})")
                        except Exception as e:
                            logger.debug(f"Kafka adapter: Host port check (socket) failed: {e}")
                    
                    # 2. Optionally check /proc/1/status to verify main process is kafka
                    process_check_passed = False
                    try:
                        proc_status_cmd = "cat /proc/1/status 2>/dev/null | grep -i '^Name:' | awk '{print $2}' || echo 'PROC_CHECK_FAILED'"
                        proc_out = self._exec(connector, container_id_str, proc_status_cmd)
                        proc_str = str(proc_out).lower().strip()
                        if "proc_check_failed" not in proc_str and "kafka" in proc_str:
                            process_check_passed = True
                            logger.debug(f"Kafka adapter: Process check passed (/proc/1/status shows kafka)")
                    except Exception as e:
                        logger.debug(f"Kafka adapter: Process check (/proc/1/status) failed: {e}")
                    
                    # 3. Try a simple Kafka command (broker API versions) as connectivity test
                    # This is more reliable than PING for Kafka
                    kafka_check_passed = False
                    test_cmd = None
                    try:
                        bootstrap_servers = self._get_bootstrap_servers(inst)
                        # Try a lightweight Kafka command to verify connectivity
                        test_cmd = self._run_kafka_cmd(
                            connector, container_id_str, inst,
                            "kafka-broker-api-versions.sh",
                            f"--bootstrap-server {bootstrap_servers}"
                            )
                        # More strict check: must have actual output, not empty, not error indicators
                        if (test_cmd and 
                            test_cmd.strip() and 
                            not test_cmd.startswith("[") and 
                            "error" not in test_cmd.lower()[:100] and
                            "connection refused" not in test_cmd.lower() and
                            "could not connect" not in test_cmd.lower() and
                            "timeout" not in test_cmd.lower()[:100]):
                            kafka_check_passed = True
                            logger.debug(f"Kafka adapter: Kafka command check passed (broker API versions)")
                        else:
                            logger.debug(f"Kafka adapter: Kafka command check failed: {test_cmd[:100] if test_cmd else 'None'}")
                    except Exception as e:
                        logger.debug(f"Kafka adapter: Kafka command check failed: {e}")
                    
                    # Liveness check: At least one of port check, process check, or Kafka command should succeed
                    if not port_check_passed and not process_check_passed and not kafka_check_passed:
                        # All checks failed - determine the best error message
                        test_cmd_str = str(test_cmd or "").lower()
                        if "connection refused" in test_cmd_str or "could not connect" in test_cmd_str:
                            raise RuntimeError(f"kafka port {port} not reachable from host and Kafka command failed")
                        elif test_cmd and (test_cmd.startswith("[container exec error") or test_cmd.startswith("[host exec error")):
                            raise RuntimeError(f"kafka tools not available in container and port {port} not accessible from host")
                        else:
                            raise RuntimeError(f"kafka liveness check failed: port={port_check_passed}, process={process_check_passed}, kafka_cmd={kafka_check_passed}")
                    
                    # If Kafka command failed but port/process checks passed, log warning but continue
                    if not kafka_check_passed:
                        logger.warning(f"Kafka adapter: Kafka command failed but port/process checks passed for '{name}', continuing anyway")
                    
                    # For SSH-accessed containers, update instance config with host port for external connections
                    # Note: Kafka commands inside container use container port (9092), but external connections use host port (19092)
                    if isinstance(connector, SSHHostConnector) and host_port != container_port:
                        inst = inst.copy()  # Don't modify original
                        inst["port"] = host_port
                        logger.info(f"Kafka adapter: Using host port {host_port} (mapped from container port {container_port}) for external connections")
                        logger.info(f"Kafka adapter: Kafka commands inside container will use container port {container_port}, external connections use host port {host_port}")
                    
                    # Mark as successful since liveness checks passed
                    instance_res["status"] = "successful"
                    
                else:
                    # ============================================================
                    # NON-DOCKER (SSH/Local): Use standard checks
                    # ============================================================
                    logger.debug(f"Kafka adapter: Using standard liveness checks for '{name}' (non-Docker)")
                    
                    # Check port accessibility using standard tools
                    port_cmd = f"timeout 2 bash -c '</dev/tcp/127.0.0.1/{port}' 2>&1 || nc -z -w2 127.0.0.1 {port} 2>&1 || echo 'PORT_CHECK_FAILED'"
                    port_out = self._exec(connector, container_id, port_cmd)
                    port_str = str(port_out).lower()
                    
                    # Check for various failure indicators
                    if "port_check_failed" in port_str or "refused" in port_str or "failed" in port_str or "connection refused" in port_str or "no route to host" in port_str:
                        # If port check failed, try alternative: check if kafka process is running
                        ps_cmd = "ps aux | grep -i '[k]afka' | grep -v grep"
                        ps_out = self._exec(connector, container_id, ps_cmd)
                        if not ps_out.strip() or ps_out.startswith("[host exec error") or ps_out.startswith("[container exec error"):
                            raise RuntimeError(f"kafka port {port} not reachable and no kafka process found")
                        else:
                            raise RuntimeError(f"kafka port {port} not reachable (process exists but port closed)")
                    # Also check if exec itself failed
                    if port_out.startswith("[host exec error") or port_out.startswith("[container exec error"):
                        raise RuntimeError(f"kafka port {port} check command failed: {port_out}")
                    
                    # Mark as successful since liveness check passed
                    instance_res["status"] = "successful"
                        
            except Exception as e:
                error_msg = f"liveness: {e}"
                logger.warning(f"Kafka adapter: Liveness check failed for '{name}': {error_msg}")
                # Ensure errors is always a list before appending
                if not isinstance(instance_res.get("errors"), list):
                    instance_res["errors"] = []
                instance_res["errors"].append(error_msg)
                # CRITICAL: Ensure result is still a dict before accessing it
                if not isinstance(result, dict):
                    logger.error(f"Kafka adapter: result is not a dict in liveness error handler for '{name}', got {type(result)}. Re-initializing.")
                    result = {
                        "type": "kafka",
                        "discovered": [],
                        "instances": [],
                        "collection_mode": "historical" if self._is_historical_collection() else "current"
                    }
                    if self._is_historical_collection():
                        since, until = self._get_time_window()
                        result["time_window"] = {
                            "since": since.isoformat(),
                            "until": until.isoformat(),
                            "issue_time": self.issue_time.isoformat() if self.issue_time else None
                        }
                result["discovered"].append(name)
                result["instances"].append(instance_res)
                continue  # Skip collection, return early with liveness error

            try:
                # CRITICAL: Ensure instance_res is still a dict at the start of collection
                # This must be checked BEFORE any operations that might corrupt it
                if not isinstance(instance_res, dict):
                    logger.error(f"Kafka adapter: instance_res is not a dict at start of collection for '{name}', got {type(instance_res)}. Value: {instance_res}. Re-initializing.")
                    instance_res = {
                        "name": name,
                        "container": container_name,
                        "kafka_home": self._get_kafka_home(inst),
                        "bootstrap_servers": self._get_bootstrap_servers(inst),
                        "version": None,
                        "broker_id": None,
                        "broker_info": {},
                        "topics": [],
                        "topics_summary": {},
                        "consumer_groups": [],
                        "consumer_groups_summary": {},
                        "server_properties": {},
                        "logs": {},
                        "metrics": {},
                        "errors": []
                    }
                
                # Ensure container_id is a string before any operations (defensive check)
                if container_id is not None:
                    container_id = str(container_id)
                
                # Additional defensive check: ensure instance_res is still a dict after container_id conversion
                if not isinstance(instance_res, dict):
                    logger.error(f"Kafka adapter: instance_res became non-dict after container_id conversion for '{name}', got {type(instance_res)}. Re-initializing.")
                    instance_res = {
                        "name": name,
                        "container": container_name,
                        "kafka_home": self._get_kafka_home(inst),
                        "bootstrap_servers": self._get_bootstrap_servers(inst),
                        "version": None,
                        "broker_id": None,
                        "broker_info": {},
                        "topics": [],
                        "topics_summary": {},
                        "consumer_groups": [],
                        "consumer_groups_summary": {},
                        "server_properties": {},
                        "logs": {},
                        "metrics": {},
                        "errors": []
                    }
                
                # PRIORITY: Collect fast operations first (logs, properties) before slow Kafka CLI commands
                # This ensures we get at least some data even if collection times out
                
                # 0) Server properties (fast - file read, or extract from env vars for KRaft)
                try:
                    kafka_home = self._get_kafka_home(inst)
                    props_paths = [
                        f"{kafka_home}/config/server.properties",
                        "/etc/kafka/server.properties",
                        "/opt/kafka/config/server.properties"
                    ]
                    
                    props_found = False
                    for props_path in props_paths:
                        try:
                            # Use timeout wrapper for file reads to avoid hanging
                            props_content = self._read_file(connector, container_id, props_path)
                            if props_content and not props_content.startswith("["):
                                instance_res["server_properties"] = _parse_kafka_properties(props_content)
                                # Extract broker.id if available
                                broker_id = instance_res["server_properties"].get("broker.id")
                                if broker_id:
                                    instance_res["broker_id"] = broker_id
                                    props_found = True
                                break
                        except Exception:
                            # Continue to next path if this one fails or times out
                            continue
                    
                    # If no server.properties file found (KRaft mode), we'll try to extract from logs later
                    # (after logs are collected in post-processing step)
                    if not props_found:
                        logger.debug(f"Kafka adapter: No server.properties found for '{name}' (KRaft mode likely), will try to extract from logs in post-processing")
                            
                except Exception as e:
                    logger.debug(f"Kafka adapter: Error reading server.properties for '{name}': {e}")
                    instance_res["errors"].append(f"server_properties: {e}")
                
                # 1) Logs collection (fast - file reads, should complete quickly)
                # Moved before Kafka CLI commands to ensure we get logs even if CLI commands hang
                try:
                    log_dirs = inst.get("logs") or inst.get("log_dirs") or []
                    if not log_dirs:
                        # Default log directories
                        kafka_home = self._get_kafka_home(inst)
                        log_dirs = [
                            f"{kafka_home}/logs",
                            "/var/log/kafka",
                            "/opt/kafka/logs"
                        ]
                    
                    if self._is_historical_collection():
                        # Historical log collection
                        since, until = self._get_time_window()
                        try:
                            kafka_logs = self._collect_historical_logs(
                                connector=connector,
                                container_id=container_id,
                                log_paths=log_dirs,
                                parser_name="syslog",  # Use syslog parser as fallback
                                since=since,
                                until=until,
                                max_lines=10000,
                                parser_context={"year": since.year if since else None}
                            )
                        except Exception as parse_error:
                            logger.debug("Historical log parsing failed, using fallback: %s", parse_error)
                            # Fall back to Docker logs for historical collection
                            kafka_logs = ""
                            try:
                                from connectors.host_connectors.docker_host_connector import DockerHostConnector
                                if isinstance(connector, DockerHostConnector) and container_id:
                                    docker_logs = connector.get_container_logs_time_filtered(
                                        container_id, since=since, until=until, tail_limit=10000
                                    )
                                    if docker_logs and not docker_logs.startswith("[docker"):
                                        kafka_logs = docker_logs
                            except Exception as e:
                                logger.debug(f"Failed to get Docker logs for historical collection: {e}")
                        instance_res["logs"] = {
                                "content": kafka_logs.strip(),
                                "type": "historical",
                                "source": "file",
                                "time_window": {
                                    "since": since.isoformat(),
                                    "until": until.isoformat()
                                },
                                "collection_mode": "historical",
                                "line_count": len(kafka_logs.splitlines()) if kafka_logs else 0
                            }
                    else:
                        # Current collection - prefer Docker logs for containers (much faster than file reads)
                        logs_dict = {}
                        logger.info(f"Kafka log collection: container_id={container_id}, log_dirs={log_dirs}")
                        
                        # For Docker containers, try docker logs first (fast and reliable)
                        if container_id:
                            try:
                                from connectors.host_connectors.docker_host_connector import DockerHostConnector
                                from connectors.host_connectors.ssh_host_connector import SSHHostConnector
                                
                                if isinstance(connector, DockerHostConnector):
                                    # Use Docker SDK to get logs (fast)
                                    docker_logs = connector.get_container_logs(container_id, tail=100)
                                    if docker_logs and not docker_logs.startswith("[docker"):
                                        logs_dict["docker_logs"] = {
                                            "content": docker_logs,
                                            "type": "current",
                                            "collection_mode": "current",
                                            "source": "docker_logs",
                                            "line_count": len(docker_logs.splitlines())
                                        }
                                        logger.info(f"Kafka adapter: Collected {len(docker_logs.splitlines())} lines from Docker logs")
                                elif isinstance(connector, SSHHostConnector):
                                    # Use docker logs command via SSH (fast)
                                    docker_logs_cmd = f"timeout 2 docker logs --tail 100 {container_id} 2>&1"
                                    docker_logs = self._exec(connector, None, docker_logs_cmd, timeout=2)
                                    if docker_logs and not docker_logs.startswith("[") and docker_logs.strip():
                                        logs_dict["docker_logs"] = {
                                            "content": docker_logs,
                                            "type": "current",
                                            "collection_mode": "current",
                                            "source": "docker_logs",
                                            "line_count": len(docker_logs.splitlines())
                                        }
                                        logger.info(f"Kafka adapter: Collected {len(docker_logs.splitlines())} lines from Docker logs via SSH")
                            except Exception as e:
                                logger.debug(f"Kafka adapter: Docker logs collection failed, falling back to file reads: {e}")
                        
                        # Fallback to file reads only if Docker logs didn't work
                        if not logs_dict:
                            logger.info("Kafka adapter: Falling back to file-based log collection")
                        for log_dir in log_dirs:
                            try:
                                dir_check = self._exec(
                                    connector, container_id,
                                    f"timeout 1 test -d {shlex.quote(log_dir)} && echo 'exists' || echo 'missing'",
                                    timeout=1
                                )
                                if "missing" in (dir_check or ""):
                                    continue
                            except Exception:
                                continue
                            
                            # Only try to read one log file (server.log) to save time
                            log_path = f"{log_dir}/server.log"
                            try:
                                file_check = self._exec(
                                    connector, container_id,
                                    f"timeout 1 test -f {shlex.quote(log_path)} && echo 'exists' || echo 'missing'",
                                    timeout=1
                                )
                                if "missing" in (file_check or ""):
                                    continue
                                
                                # Use timeout and reduce lines to make log collection faster
                                tail_out = self._exec(
                                    connector, container_id,
                                    f"timeout 2 tail -n 50 {shlex.quote(log_path)} 2>/dev/null || true",
                                    timeout=2
                                )
                                
                                is_error = tail_out and (
                                    tail_out.startswith("[container exec error") or
                                    tail_out.startswith("[host exec error") or
                                    tail_out.startswith("[docker-host exec error") or
                                    tail_out.startswith("[docker not available") or
                                    tail_out.startswith("[no exec method]")
                                )
                                
                                if tail_out and not is_error and tail_out.strip():
                                    logs_dict["server.log"] = {
                                        "content": tail_out,
                                        "type": "current",
                                        "collection_mode": "current",
                                        "source": "file",
                                        "line_count": len(tail_out.splitlines())
                                    }
                                    logger.info(f"Kafka adapter: Collected {len(tail_out.splitlines())} lines from {log_path}")
                                    break  # Only collect one log file as fallback
                            except Exception:
                                pass
                        
                        instance_res["logs"] = logs_dict
                except Exception as e:
                    logger.error(f"Kafka adapter: Error collecting logs for '{name}': {e}", exc_info=True)
                    instance_res["errors"].append(f"logs: {e}")
                
                # Now try Kafka CLI commands (slow, but optional - we already have logs and properties)
                # Wrap all CLI commands in a single try/except so we can return partial results
                # if any command hangs or times out
                try:
                    # 1) Broker version and API information
                    broker_info_out = self._run_kafka_cmd(
                        connector, container_id, inst,
                        "kafka-broker-api-versions.sh",
                        "--bootstrap-server " + self._get_bootstrap_servers(inst)
                    )
                    # Ensure broker_info_out is a string before string operations
                    if not isinstance(broker_info_out, str):
                        broker_info_out = str(broker_info_out) if broker_info_out is not None else ""
                    if broker_info_out and not broker_info_out.startswith("["):
                        instance_res["broker_info"]["api_versions"] = broker_info_out.strip()[:5000]
                        # Try to extract version from output - look for metadata.version pattern first
                        # Example: "metadata.version: 3.9-IV0" -> extract "3.9"
                        metadata_version_match = re.search(r'metadata\.version[:\s]+(\d+)\.(\d+)', broker_info_out, re.IGNORECASE)
                        if metadata_version_match:
                            # Construct version from metadata.version (e.g., 3.9-IV0 -> 3.9.0)
                            major = metadata_version_match.group(1)
                            minor = metadata_version_match.group(2)
                            instance_res["version"] = f"{major}.{minor}.0"
                        else:
                            # Fallback: look for standard version pattern (x.y.z)
                            version_match = re.search(r'(\d+\.\d+\.\d+)', broker_info_out)
                            if version_match:
                                instance_res["version"] = version_match.group(1)
                        
                        # Try to extract broker_id from broker_info output
                        # Example: "grc-dev-tc-app-1:29092 (id: 1 rack: null)"
                        broker_id_match = re.search(r'\(id:\s*(\d+)', broker_info_out)
                        if broker_id_match and not instance_res.get("broker_id"):
                            instance_res["broker_id"] = broker_id_match.group(1)
                    
                    # Fallback 1: Extract version from kafka_home path if not found in broker_info
                    # Example: "/app/kafka/kafka_2.13-3.9.0" -> "3.9.0"
                    if not instance_res.get("version"):
                        kafka_home = self._get_kafka_home(inst)
                        # Pattern: kafka_<scala_version>-<kafka_version> (e.g., kafka_2.13-3.9.0)
                        kafka_home_match = re.search(r'kafka[_-]\d+\.\d+[_-](\d+\.\d+\.\d+)', kafka_home)
                        if kafka_home_match:
                            instance_res["version"] = kafka_home_match.group(1)
                            logger.debug(f"Kafka adapter: Extracted version '{instance_res['version']}' from kafka_home path")
                    
                    # Fallback 2: Try to extract version from logs if still not found
                    # Look for "metadata.version to 3.9-IV0" pattern in logs
                    if not instance_res.get("version"):
                        try:
                            # Check if we have logs already collected (they might be collected later, but try anyway)
                            logs_dict = instance_res.get("logs", {})
                            if isinstance(logs_dict, dict):
                                # Check controller.log first (most likely to have version info)
                                for log_name in ["controller.log", "server.log"]:
                                    log_entry = logs_dict.get(log_name)
                                    if isinstance(log_entry, dict):
                                        log_content = log_entry.get("content", "")
                                    elif isinstance(log_entry, str):
                                        log_content = log_entry
                                    else:
                                        continue
                                    
                                    if log_content:
                                        # Look for "metadata.version to X.Y-IV0" pattern
                                        log_version_match = re.search(r'metadata\.version\s+to\s+(\d+)\.(\d+)-', log_content, re.IGNORECASE)
                                        if log_version_match:
                                            major = log_version_match.group(1)
                                            minor = log_version_match.group(2)
                                            instance_res["version"] = f"{major}.{minor}.0"
                                            logger.debug(f"Kafka adapter: Extracted version '{instance_res['version']}' from {log_name} logs")
                                            break
                        except Exception as e:
                            logger.debug(f"Kafka adapter: Failed to extract version from logs: {e}")
                        
                    # Fallback 3: Try to read version from a version file or run kafka version command
                    if not instance_res.get("version"):
                        try:
                            kafka_home = self._get_kafka_home(inst)
                            # Try to read version from build.properties or similar
                            version_file_paths = [
                                f"{kafka_home}/libs/kafka-clients-*.jar",  # JAR name might contain version
                                f"{kafka_home}/libs/kafka_*.jar",
                            ]
                            # Try to get version from JAR file names
                            for pattern in version_file_paths:
                                try:
                                    # List JAR files and extract version from name
                                    list_cmd = f"ls {pattern} 2>/dev/null | head -1"
                                    jar_path = self._exec(connector, container_id, list_cmd)
                                    if jar_path and not jar_path.startswith("[") and jar_path.strip():
                                        # Extract version from JAR name: kafka-clients-3.9.0.jar -> 3.9.0
                                        jar_version_match = re.search(r'kafka[_-]clients[_-](\d+\.\d+\.\d+)', jar_path)
                                        if jar_version_match:
                                            instance_res["version"] = jar_version_match.group(1)
                                            logger.debug(f"Kafka adapter: Extracted version '{instance_res['version']}' from JAR file name")
                                            break
                                except Exception:
                                    continue
                        except Exception as e:
                            logger.debug(f"Kafka adapter: Failed to extract version from JAR files: {e}")
                            
                except Exception as e:
                    # Kafka CLI commands may fail due to advertised listener configuration issues
                    # This is not a fatal error - we can still collect logs and properties
                    error_str = str(e)
                    if "19092" in error_str or "advertised" in error_str.lower() or "disconnect" in error_str.lower():
                        logger.warning(f"Kafka adapter: broker_info failed due to advertised listener config issue for '{name}': {e}")
                        instance_res["errors"].append(f"broker_info: Kafka CLI commands failed (likely due to advertised listener configuration - this is non-fatal)")
                    else:
                        logger.error(f"Kafka adapter: Error in broker_info for '{name}': {e}", exc_info=True)
                        instance_res["errors"].append(f"broker_info: {e}")
                
                # Only continue with more Kafka CLI commands if broker_info succeeded
                # This prevents hanging on multiple slow commands
                broker_info_success = bool(
                    instance_res.get("broker_info", {}).get("api_versions") or 
                    instance_res.get("version")
                )
                
                if broker_info_success:
                    # 2) Topics listing (only if broker_info worked)
                    try:
                        topics_list_out = self._run_kafka_cmd(
                            connector, container_id, inst,
                            "kafka-topics.sh",
                            "--list"
                        )
                        # Ensure topics_list_out is a string before string operations
                        if not isinstance(topics_list_out, str):
                            topics_list_out = str(topics_list_out) if topics_list_out is not None else ""
                        if topics_list_out and not topics_list_out.startswith("["):
                            topic_names = [t.strip() for t in topics_list_out.splitlines() if t.strip()]
                            instance_res["topics_summary"]["topic_names"] = topic_names
                            instance_res["topics_summary"]["topic_count"] = len(topic_names)
                    except Exception as e:
                        logger.debug(f"Kafka adapter: Error in topics_list for '{name}': {e}")
                        instance_res["errors"].append(f"topics_list: {e}")
                    
                    # 3) Topics detailed description (only if topics list worked)
                    if instance_res.get("topics_summary", {}).get("topic_names"):
                        try:
                            topics_describe_out = self._run_kafka_cmd(
                                connector, container_id, inst,
                                "kafka-topics.sh",
                                "--describe"
                            )
                            # Ensure topics_describe_out is a string before string operations
                            if not isinstance(topics_describe_out, str):
                                topics_describe_out = str(topics_describe_out) if topics_describe_out is not None else ""
                            if topics_describe_out and not topics_describe_out.startswith("["):
                                topics_dict = _parse_topic_describe(topics_describe_out)
                                instance_res["topics"] = list(topics_dict.values())
                        except Exception as e:
                            logger.debug(f"Kafka adapter: Error in topics_describe for '{name}': {e}")
                            instance_res["errors"].append(f"topics_describe: {e}")
                    
                    # 4) Consumer groups listing (only if broker_info worked)
                    try:
                        cg_list_out = self._run_kafka_cmd(
                            connector, container_id, inst,
                            "kafka-consumer-groups.sh",
                            "--list"
                        )
                        # Ensure cg_list_out is a string before string operations
                        if not isinstance(cg_list_out, str):
                            cg_list_out = str(cg_list_out) if cg_list_out is not None else ""
                        if cg_list_out and not cg_list_out.startswith("["):
                            cg_names = [cg.strip() for cg in cg_list_out.splitlines() if cg.strip()]
                            instance_res["consumer_groups_summary"]["group_names"] = cg_names
                            instance_res["consumer_groups_summary"]["group_count"] = len(cg_names)
                    except Exception as e:
                        logger.debug(f"Kafka adapter: Error in consumer_groups_list for '{name}': {e}")
                        instance_res["errors"].append(f"consumer_groups_list: {e}")
                    
                    # 5) Consumer groups detailed description (with lag) - only if we have groups
                    if instance_res.get("consumer_groups_summary", {}).get("group_names"):
                        try:
                            # Ensure consumer_groups_summary is a dict and group_names is a list
                            cg_summary = instance_res.get("consumer_groups_summary", {})
                            if not isinstance(cg_summary, dict):
                                cg_summary = {}
                                instance_res["consumer_groups_summary"] = cg_summary
                            
                            cg_names_raw = cg_summary.get("group_names", [])
                            # Ensure cg_names is always a list to prevent 'int' object is not subscriptable
                            if not isinstance(cg_names_raw, (list, tuple)):
                                cg_names = []
                            else:
                                cg_names = list(cg_names_raw)
                            
                            all_cg_details = {}
                            total_lag = 0
                            
                            # Limit to 3 groups to avoid timeout - orchestrator has 20s total timeout
                            # Each describe command takes ~2s, so 3 groups = 6s, leaving room for other commands
                            for cg_name in cg_names[:3]:  # Limit to 3 groups to avoid timeout
                                try:
                                    # Ensure cg_name is a string
                                    if not isinstance(cg_name, str):
                                        cg_name = str(cg_name) if cg_name is not None else ""
                                    cg_describe_out = self._run_kafka_cmd(
                                        connector, container_id, inst,
                                        "kafka-consumer-groups.sh",
                                        f"--describe --group {cg_name}"
                                    )
                                    # Ensure cg_describe_out is a string before string operations
                                    if not isinstance(cg_describe_out, str):
                                        cg_describe_out = str(cg_describe_out) if cg_describe_out is not None else ""
                                    if cg_describe_out and not cg_describe_out.startswith("["):
                                        cg_details = _parse_consumer_groups_describe(cg_describe_out)
                                        all_cg_details.update(cg_details)
                                        if cg_name in cg_details:
                                            total_lag += cg_details[cg_name].get("total_lag", 0)
                                except Exception as e:
                                    logger.debug(f"Kafka adapter: Error in consumer_group_describe for '{cg_name}': {e}")
                                    instance_res["errors"].append(f"consumer_group_describe_{cg_name}: {e}")
                            
                            instance_res["consumer_groups"] = list(all_cg_details.values())
                            instance_res["consumer_groups_summary"]["total_lag"] = total_lag
                        except Exception as e:
                            logger.debug(f"Kafka adapter: Error in consumer_groups_describe for '{name}': {e}")
                            instance_res["errors"].append(f"consumer_groups_describe: {e}")
                else:
                    logger.info(f"Kafka adapter: Skipping additional CLI commands for '{name}' - broker_info did not succeed")
                
                # Note: Server properties and logs are collected FIRST (before Kafka CLI commands)
                # to ensure we get at least some data even if CLI commands hang/timeout
                
                # Post-processing: Extract additional info from logs
                if instance_res.get("logs"):
                    try:
                        logs_dict = instance_res.get("logs", {})
                        if isinstance(logs_dict, dict):
                            # Get log content (try docker_logs first, then specific log files)
                            log_content = ""
                            for log_key in ["docker_logs", "server.log", "controller.log"]:
                                log_entry = logs_dict.get(log_key)
                                if isinstance(log_entry, dict):
                                    log_content = log_entry.get("content", "")
                                elif isinstance(log_entry, str):
                                    log_content = log_entry
                                if log_content:
                                    break
                            
                            if log_content:
                                # Extract server_properties from KafkaConfig dump if not already found
                                if not instance_res.get("server_properties") or not isinstance(instance_res.get("server_properties"), dict) or len(instance_res.get("server_properties", {})) == 0:
                                    props_from_logs = {}
                                    for line in log_content.split('\n'):
                                        # Look for tab-separated key=value pairs (KafkaConfig dump format: \tkey = value)
                                        if line.startswith('\t') and '=' in line:
                                            try:
                                                # Remove leading tab and split on '=' (first occurrence)
                                                clean_line = line.lstrip('\t').strip()
                                                if '=' in clean_line:
                                                    key, value = clean_line.split('=', 1)
                                                    key = key.strip()
                                                    value = value.strip()
                                                    # Only add non-null values and skip zookeeper configs (KRaft mode)
                                                    if value and value != 'null' and not key.startswith('zookeeper.'):
                                                        props_from_logs[key] = value
                                            except Exception:
                                                continue
                                    
                                    if props_from_logs:
                                        instance_res["server_properties"] = props_from_logs
                                        logger.debug(f"Kafka adapter: Extracted {len(props_from_logs)} properties from logs (post-processing)")
                                        # Extract broker.id if available and not already set
                                        if not instance_res.get("broker_id"):
                                            broker_id = props_from_logs.get("broker.id") or props_from_logs.get("node.id")
                                            if broker_id:
                                                instance_res["broker_id"] = broker_id
                                                logger.debug(f"Kafka adapter: Extracted broker_id '{broker_id}' from logs (post-processing)")
                                
                                # Extract version if not already found
                                if not instance_res.get("version"):
                                    # Look for "Kafka version: X.Y.Z" pattern
                                    version_match = re.search(r'Kafka version:\s*(\d+\.\d+\.\d+)', log_content, re.IGNORECASE)
                                    if version_match:
                                        instance_res["version"] = version_match.group(1)
                                        logger.debug(f"Kafka adapter: Extracted version '{instance_res['version']}' from logs (post-processing)")
                                    else:
                                        # Look for "metadata.version to X.Y-IV0" pattern
                                        log_version_match = re.search(r'metadata\.version\s+to\s+(\d+)\.(\d+)-', log_content, re.IGNORECASE)
                                        if log_version_match:
                                            major = log_version_match.group(1)
                                            minor = log_version_match.group(2)
                                            instance_res["version"] = f"{major}.{minor}.0"
                                            logger.debug(f"Kafka adapter: Extracted version '{instance_res['version']}' from logs (post-processing)")
                                
                                # Extract broker_id/nodeId from logs if not already found
                                if not instance_res.get("broker_id"):
                                    # Look for "nodeId=1" or "id=1" patterns
                                    node_id_match = re.search(r'(?:nodeId|node\.id|broker\.id|id)\s*[=:]\s*(\d+)', log_content, re.IGNORECASE)
                                    if node_id_match:
                                        instance_res["broker_id"] = node_id_match.group(1)
                                        logger.debug(f"Kafka adapter: Extracted broker_id '{instance_res['broker_id']}' from logs (post-processing)")
                    except Exception as e:
                        logger.debug(f"Kafka adapter: Failed to extract info from logs in post-processing: {e}")
                
                # 8) Metrics collection
                try:
                    metrics = {}
                    
                    # Basic metrics (always available)
                    if instance_res.get("version"):
                        metrics["version"] = instance_res["version"]
                    if instance_res.get("broker_id"):
                        metrics["broker_id"] = instance_res["broker_id"]
                    
                    # Topic metrics
                    topics = instance_res.get("topics", [])
                    # Ensure topics is a list to prevent subscript errors
                    if not isinstance(topics, (list, tuple)):
                        topics = []
                    
                    topics_summary = instance_res.get("topics_summary", {})
                    if isinstance(topics_summary, dict) and topics_summary.get("topic_count") is not None:
                        metrics["total_topics"] = topics_summary["topic_count"]
                    elif topics:
                        metrics["total_topics"] = len(topics)
                    
                    # Calculate partition metrics from topics list (if available)
                    if topics:
                        total_partitions = sum(t.get("partition_count", 0) if isinstance(t, dict) else 0 for t in topics)
                        if total_partitions > 0:
                            metrics["total_partitions"] = total_partitions
                        avg_replication = sum(t.get("replication_factor", 0) if isinstance(t, dict) else 0 for t in topics) / len(topics) if topics else 0
                        if avg_replication > 0:
                            metrics["avg_replication_factor"] = round(avg_replication, 2)
                    
                    # Consumer group metrics
                    consumer_groups = instance_res.get("consumer_groups", [])
                    # Ensure consumer_groups is a list
                    if not isinstance(consumer_groups, (list, tuple)):
                        consumer_groups = []
                    
                    cg_summary = instance_res.get("consumer_groups_summary", {})
                    if isinstance(cg_summary, dict):
                        if cg_summary.get("group_count") is not None:
                            metrics["total_consumer_groups"] = cg_summary["group_count"]
                        if cg_summary.get("total_lag") is not None:
                            metrics["total_consumer_lag"] = cg_summary["total_lag"]
                    
                    if consumer_groups:
                        if "total_consumer_groups" not in metrics:
                            metrics["total_consumer_groups"] = len(consumer_groups)
                        total_lag = sum(cg.get("total_lag", 0) if isinstance(cg, dict) else 0 for cg in consumer_groups)
                        if total_lag > 0:
                            metrics["total_consumer_lag"] = total_lag
                            metrics["avg_consumer_lag"] = round(total_lag / len(consumer_groups), 2) if consumer_groups else 0
                    
                    # Extract additional metrics from server_properties if available
                    server_props = instance_res.get("server_properties", {})
                    if isinstance(server_props, dict) and server_props:
                        # Extract useful config values as metrics
                        if "log.dirs" in server_props:
                            metrics["log_dirs"] = server_props["log.dirs"]
                        if "num.network.threads" in server_props:
                            metrics["network_threads"] = server_props["num.network.threads"]
                        if "num.io.threads" in server_props:
                            metrics["io_threads"] = server_props["num.io.threads"]
                    
                    # If metrics are missing, try to extract from instance config (YAML) first
                    # This is especially important for Docker/KRaft mode where server.properties might not have these
                    # Priority: YAML config > server_properties (already checked) > skip (to avoid timeout)
                    if "log_dirs" not in metrics:
                        # Try YAML config (check data_dir for KRaft, then logs, then log_dirs)
                        log_dirs = inst.get("data_dir") or inst.get("log_dirs") or inst.get("logs")
                        if log_dirs:
                            if isinstance(log_dirs, list):
                                metrics["log_dirs"] = ",".join(log_dirs)
                            else:
                                metrics["log_dirs"] = str(log_dirs)
                    
                    # Note: network_threads and io_threads are skipped if not in server_properties
                    # to avoid timeouts from parsing large log files. These are optional metrics.
                    
                    # Extract additional metrics from logs if available
                    logs_dict = instance_res.get("logs", {})
                    log_content = ""
                    if isinstance(logs_dict, dict):
                        # Get log content (only if not already extracted above)
                        for log_key in ["docker_logs", "server.log", "controller.log"]:
                            log_entry = logs_dict.get(log_key)
                            if isinstance(log_entry, dict):
                                log_content = log_entry.get("content", "")
                            elif isinstance(log_entry, str):
                                log_content = log_entry
                            if log_content:
                                break
                    
                    if log_content:
                            # Extract mode (KRaft vs Zookeeper)
                            if "KRaft" in log_content or "KafkaRaftServer" in log_content:
                                metrics["mode"] = "KRaft"
                            elif "zookeeper" in log_content.lower():
                                metrics["mode"] = "Zookeeper"
                            
                            # Extract process roles if available
                            process_roles_match = re.search(r'PROCESS_ROLES[=:]\s*([^\s,]+)', log_content, re.IGNORECASE)
                            if process_roles_match:
                                metrics["process_roles"] = process_roles_match.group(1)
                    
                    # Store metrics (even if empty, to show we tried)
                    instance_res["metrics"] = metrics
                        
                except Exception as e:
                    logger.debug("Failed to collect Kafka metrics: %s", e, exc_info=True)
                    # Metrics are optional - don't fail collection
                
            except Exception as e:
                error_msg = f"fatal: {e}"
                error_trace = traceback.format_exc()
                logger.error(f"Kafka adapter: Fatal error during collection for '{name}': {error_msg}")
                logger.error(f"Kafka adapter: Traceback:\n{error_trace}")
                # CRITICAL: Ensure instance_res is a dict before accessing it
                if not isinstance(instance_res, dict):
                    logger.error(f"Kafka adapter: instance_res is not a dict in exception handler for '{name}', got {type(instance_res)}. Re-initializing.")
                    instance_res = {
                        "name": name,
                        "container": container_name,
                        "kafka_home": self._get_kafka_home(inst) if inst else None,
                        "bootstrap_servers": self._get_bootstrap_servers(inst) if inst else None,
                        "version": None,
                        "broker_id": None,
                        "broker_info": {},
                        "topics": [],
                        "topics_summary": {},
                        "consumer_groups": [],
                        "consumer_groups_summary": {},
                        "server_properties": {},
                        "logs": {},
                        "metrics": {},
                        "errors": []
                    }
                # Ensure errors is a list before appending
                if not isinstance(instance_res.get("errors"), list):
                    instance_res["errors"] = []
                instance_res["errors"].append(error_msg)
                instance_res["errors"].append(error_trace)
            
            # Ensure instance_res is properly structured before adding to result
            if not isinstance(instance_res, dict):
                logger.error(f"Kafka adapter: instance_res is not a dict for '{name}', got {type(instance_res)}")
                instance_res = {"name": name, "errors": [f"Internal error: instance_res is {type(instance_res)}"]}
            
            # CRITICAL: Ensure metrics and logs are always dicts, never integers or other types
            # This prevents 'int' object is not subscriptable errors in the orchestrator
            if "metrics" in instance_res:
                if not isinstance(instance_res["metrics"], dict):
                    logger.warning(f"Kafka adapter: instance_res['metrics'] is not a dict for '{name}', got {type(instance_res['metrics'])}. Value: {instance_res['metrics']}")
                    instance_res["metrics"] = {}
            else:
                instance_res["metrics"] = {}
            
            if "logs" in instance_res:
                if not isinstance(instance_res["logs"], dict):
                    logger.warning(f"Kafka adapter: instance_res['logs'] is not a dict for '{name}', got {type(instance_res['logs'])}. Value: {instance_res['logs']}")
                    instance_res["logs"] = {}
            else:
                instance_res["logs"] = {}
            
            # Ensure errors is always a list
            if "errors" in instance_res:
                if not isinstance(instance_res["errors"], list):
                    logger.warning(f"Kafka adapter: instance_res['errors'] is not a list for '{name}', got {type(instance_res['errors'])}. Value: {instance_res['errors']}")
                    instance_res["errors"] = [str(instance_res["errors"])] if instance_res["errors"] else []
            else:
                instance_res["errors"] = []
            
            # CRITICAL: Ensure result is still a dict before accessing it
            # This prevents 'int' object has no attribute 'get' errors
            if not isinstance(result, dict):
                logger.error(f"Kafka adapter: result is not a dict before final check for '{name}', got {type(result)}. Value: {result}")
                # Re-initialize result if it got corrupted
                result = {
                    "type": "kafka",
                    "discovered": [],
                    "instances": [],
                    "collection_mode": "historical" if self._is_historical_collection() else "current"
                }
                if self._is_historical_collection():
                    since, until = self._get_time_window()
                    result["time_window"] = {
                        "since": since.isoformat(),
                        "until": until.isoformat(),
                        "issue_time": self.issue_time.isoformat() if self.issue_time else None
                    }
            
            # Ensure result structures are lists
            if not isinstance(result.get("discovered"), list):
                result["discovered"] = []
            if not isinstance(result.get("instances"), list):
                result["instances"] = []
            
            result["discovered"].append(name)
            result["instances"].append(instance_res)
        
        # Final safety check: ensure result is properly structured
        if not isinstance(result, dict):
            logger.error(f"Kafka adapter: result is not a dict, got {type(result)}")
            return {"type": "kafka", "discovered": [], "instances": [], "errors": ["Internal error: result is not a dict"]}
        
        # Ensure all required fields exist
        if "discovered" not in result or not isinstance(result["discovered"], list):
            result["discovered"] = []
        if "instances" not in result or not isinstance(result["instances"], list):
            result["instances"] = []
        
        return result
    
    # --------------------------
    # collect() for standalone usage
    # --------------------------
    def collect(self):
        self.start_collection()
        result = {"type": "host", "discovered": [], "findings": {}}
        
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

