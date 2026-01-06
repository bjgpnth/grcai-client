# orchestrator/session_orchestrator.py
import os
import sys
import logging
import logging.handlers
import traceback
from datetime import datetime
from pathlib import Path

# Ensure repo root is on Python path for utils imports
_REPO_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT_STR = str(_REPO_ROOT.resolve())
if _REPO_ROOT_STR not in sys.path:
    sys.path.insert(0, _REPO_ROOT_STR)
# Also add current working directory as fallback
if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

from evidence_store.evidence_store import EvidenceStore
from utils.print_utils import divider
from client.rca_client import get_reasoning_client

from connectors.registry import CONNECTOR_REGISTRY
from config.config_loader import ConfigLoader


# ================================================================
# ARCHITECTURAL BOUNDARY ENFORCEMENT
# ================================================================
# This orchestrator may sequence execution, but must NEVER interpret evidence or infer meaning.
#
# The orchestrator MAY:
#   - Trigger adapter execution
#   - Collect results
#   - Forward evidence to Central
#
# The orchestrator MUST NOT:
#   - Evaluate thresholds
#   - Classify failures
#   - Decide severity or cause
#   - Apply heuristics to evidence
#   - Make decisions about what evidence is "important"
#
# All reasoning, interpretation, and decision-making belongs in Central.
# ================================================================


# ================================================================
# >>>> Central logging setup
# ================================================================
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "grcai.log"

def configure_root_logger(level=None):
    level = level or logging.getLevelName(os.environ.get("GRCAI_LOG_LEVEL", "INFO"))
    logger = logging.getLogger("grcai")

    if logger.handlers:
        return logger        # Already set up

    logger.setLevel(level)
    fmt = "%(asctime)s %(levelname)-5s %(name)s %(message)s"
    formatter = logging.Formatter(fmt)

    # File handler
    fh = logging.handlers.RotatingFileHandler(
        filename=str(LOG_FILE),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    fh.setFormatter(formatter)
    fh.setLevel(level)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setFormatter(formatter)
    ch.setLevel(level)
    logger.addHandler(ch)

    logger.propagate = False
    return logger


# Sanitize host info to avoid logging sensitive fields
def sanitize_host_for_logging(host_info: dict) -> dict:
    if not host_info:
        return {}
    return {
        "name": host_info.get("name"),
        "type": host_info.get("type"),
        "tags": host_info.get("tags", []),
    }



class SessionOrchestrator:
    """
    Multi-host, environment-aware orchestrator (A5 version).
    """

    def __init__(self, environment=None, sessions_dir="grcai_sessions", env_path=None):
        # >>>> initialize logger
        self.logger = configure_root_logger()
        self.logger.info("SessionOrchestrator starting")

        self.issue_time = None
        self.components = []
        self.observations = ""
        self.environment = environment
        self.env_path = env_path
        self.config = {}

        self.store = EvidenceStore(base_dir=sessions_dir)

    # ----------------------------------------------------------------------
    def run_non_interactive(self, issue_time, components, observations, environment):
        self.issue_time = issue_time
        self.components = components
        self.observations = observations
        self.environment = environment

        self._load_environment_config()

        # Fast host pre-check (parallel, short timeout) before collection
        precheck = self._precheck_hosts(timeout=5)
        self._precheck_results = precheck
        self._precheck_unreachable = {h for h, r in precheck.items() if not r.get("accessible")}
        self._precheck_errors = {h: r.get("error") or "unreachable" for h, r in precheck.items() if not r.get("accessible")}
        self._precheck_available_hosts = {h for h, r in precheck.items() if r.get("accessible")}

        # If no hosts are reachable, fail fast
        if not self._precheck_available_hosts:
            msg = "All hosts unreachable during pre-check.\n" + "\n".join(
                [f"  - {h}: {err}" for h, err in self._precheck_errors.items()]
            )
            raise RuntimeError(msg)

        # Track collection start time for metadata
        from datetime import timezone
        collected_at = datetime.now(timezone.utc)

        # Check if any requested components exist in registry
        valid_components = [comp for comp in self.components if comp in CONNECTOR_REGISTRY]
        missing_components = [comp for comp in self.components if comp not in CONNECTOR_REGISTRY]
        if missing_components:
            self.logger.warning(f"Components not found in registry: {missing_components}")

        results = self._run_collectors()

        # Check if we have any actual evidence collected (not just empty structures)
        has_evidence = False
        has_error_entries = False  # Track if we have error entries (timeouts, collection errors)
        
        for comp_name, comp_data in results.get("host", {}).items():
            instances = comp_data.get("instances", [])
            findings = comp_data.get("findings", {})
            
            # Check instances for actual data
            for inst in instances:
                # Check for error entries (timeouts, collection errors) - these count as evidence
                if inst.get("errors"):
                    has_error_entries = True
                
                metrics = inst.get("metrics", {})
                logs = inst.get("logs", {})
                
                # Check if metrics have actual content
                if metrics:
                    for metric_name, metric_data in metrics.items():
                        if isinstance(metric_data, dict):
                            stdout = metric_data.get("stdout", "")
                            output = metric_data.get("output", "")
                            if (stdout and str(stdout).strip()) or (output and str(output).strip()):
                                has_evidence = True
                                break
                        elif isinstance(metric_data, str) and metric_data.strip():
                            has_evidence = True
                            break
                    if has_evidence:
                        break
                
                # Check if logs have actual content
                if logs:
                    for log_name, log_data in logs.items():
                        if isinstance(log_data, dict):
                            content = log_data.get("content", "")
                            if content and str(content).strip():
                                has_evidence = True
                                break
                        elif isinstance(log_data, str) and log_data.strip():
                            has_evidence = True
                            break
                    if has_evidence:
                        break
            
            # Also check findings for actual data (legacy format)
            if not has_evidence and findings:
                for host_name, finding_data in findings.items():
                    if isinstance(finding_data, dict):
                        # Check for actual content in findings (not just error messages)
                        for key, value in finding_data.items():
                            if key == "error":
                                # Error entries count as evidence (timeouts, collection errors)
                                if value:
                                    has_error_entries = True
                                continue  # Skip error fields for content check
                            if isinstance(value, dict):
                                if value.get("stdout") or value.get("content") or value.get("output"):
                                    content_val = value.get("stdout") or value.get("content") or value.get("output", "")
                                    if content_val and str(content_val).strip():
                                        has_evidence = True
                                        break
                            elif isinstance(value, str) and value.strip():
                                has_evidence = True
                                break
                        if has_evidence:
                            break
            
            if has_evidence:
                break

        connection_failures = results.get("connection_failures", {})
        
        # If all requested components are missing from registry, don't raise "All hosts unreachable"
        # Instead, return empty results (components will have empty data)
        if not valid_components and missing_components:
            self.logger.warning(f"All requested components missing from registry: {missing_components}. Returning empty results.")
            # Don't raise error - return empty results gracefully
        
        # If all hosts failed and no evidence collected (including error entries), raise an exception
        # Error entries (timeouts, collection errors) are considered evidence and should be returned
        # Only raise if we have valid components to collect (not if all components are missing)
        elif not has_evidence and not has_error_entries and connection_failures and valid_components:
            failed_hosts = list(connection_failures.keys())
            error_msg = f"All hosts unreachable. Failed hosts: {', '.join(failed_hosts)}"
            for host_name, error in connection_failures.items():
                error_msg += f"\n  - {host_name}: {error}"
            raise RuntimeError(error_msg)

        context = {
            "issue_time": self.issue_time.isoformat(),
            "components": self.components,
            "observations": self.observations,
        }

        path = self.store.save_session(
            context=context,
            host=results["host"],
            containers=results["containers"],
            os_nodes=results["os_nodes"],
            environment=self.environment,
            collected_at=collected_at,
            host_collection_status=results.get("host_collection_status", {})
        )

        results["evidence_file"] = path
        return results

    # ----------------------------------------------------------------------
    def _load_environment_config(self):
        loader = ConfigLoader()

        if self.env_path:
            self.config = loader.load_environment(self.environment, explicit_path=self.env_path) or {}
        else:
            self.config = loader.load_environment(self.environment) or {}

        self.logger.info(f"Loaded environment '{self.environment}'")

    # ----------------------------------------------------------------------
    # HOST PRE-CHECK (fast connectivity probe)
    # ----------------------------------------------------------------------
    def _precheck_hosts(self, timeout: int = 5) -> dict:
        """
        Fast, parallel connectivity pre-check.
        - SSH: short-timeout connect attempt
        - Docker: client init + ping
        - Local: assumed reachable
        Returns:
            {host_name: {"accessible": bool, "error": str | None, "connector": str}}
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from connectors.host_connectors.ssh_host_connector import SSHHostConnector
        from connectors.host_connectors.docker_host_connector import DockerHostConnector
        from connectors.host_connectors.local_host_connector import LocalHostConnector

        env = self.config
        hosts = env.get("hosts", []) or []
        global_access = env.get("access", {}) or {}

        results: dict[str, dict] = {}

        def _merge_access(host: dict) -> tuple[dict, dict]:
            host_access = host.get("access", {}) or {}
            merged_ssh = {}
            merged_ssh.update(global_access.get("ssh", {}) or {})
            merged_ssh.update(host_access.get("ssh", {}) or {})
            merged_ssh.update(host.get("ssh", {}) or {})
            if merged_ssh and "timeout" not in merged_ssh:
                merged_ssh["timeout"] = timeout

            merged_docker = {}
            merged_docker.update(global_access.get("docker", {}) or {})
            merged_docker.update(host_access.get("docker", {}) or {})
            merged_docker.update(host.get("docker", {}) or {})
            return merged_ssh, merged_docker

        def check_host(host: dict):
            host = host or {}
            host_name = host.get("name") or host.get("address") or "unknown"
            host_type = (host.get("type") or "").lower()
            merged_ssh, merged_docker = _merge_access(host)

            host_copy = dict(host)
            # Ensure we always set a short timeout for SSH checks on VMs
            if host_type == "vm":
                ssh_cfg = dict(merged_ssh)
                if "timeout" not in ssh_cfg:
                    ssh_cfg["timeout"] = timeout
                host_copy["ssh"] = ssh_cfg
            elif merged_ssh:
                host_copy["ssh"] = merged_ssh
            if merged_docker:
                host_copy["docker"] = merged_docker

            try:
                # For VM hosts, prefer SSH pre-check even if docker config exists
                if host_type == "vm":
                    conn = SSHHostConnector(host_info=host_copy, global_access=global_access)
                    try:
                        if conn._client:
                            conn.close()
                            return host_name, True, None, "ssh"
                        err = conn._connection_error or "SSH connection failed"
                        conn.close()
                        return host_name, False, err, "ssh"
                    except Exception as e:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        return host_name, False, f"SSH connection failed: {e}", "ssh"

                # Docker pre-check (only if no SSH config - SSH takes precedence)
                # If SSH config exists, we'll use SSHHostConnector which can execute Docker commands remotely
                if (host_type == "docker-host" or merged_docker.get("use_local_socket") or merged_docker.get("base_url")) and not merged_ssh:
                    conn = DockerHostConnector(host_info=host_copy, global_access=global_access, docker_timeout=timeout)
                    try:
                        if not getattr(conn, "client", None):
                            raise RuntimeError(conn._last_err or "Docker client not initialized")
                        conn.client.ping()
                        conn.close()
                        return host_name, True, None, "docker"
                    except Exception as e:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        return host_name, False, f"Docker connection failed: {e}", "docker"

                # SSH pre-check (explicit SSH on non-VM hosts)
                if merged_ssh:
                    conn = SSHHostConnector(host_info=host_copy, global_access=global_access)
                    try:
                        if conn._client:
                            conn.close()
                            return host_name, True, None, "ssh"
                        err = conn._connection_error or "SSH connection failed"
                        conn.close()
                        return host_name, False, err, "ssh"
                    except Exception as e:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        return host_name, False, f"SSH connection failed: {e}", "ssh"

                # Local hosts: assume reachable
                return host_name, True, None, "local"

            except Exception as e:
                return host_name, False, str(e), "unknown"

        max_workers = min(len(hosts), 8) or 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(check_host, host): host for host in hosts}
            for fut in as_completed(future_map):
                host_name, ok, err, connector_type = fut.result()
                results[host_name] = {
                    "accessible": ok,
                    "error": err,
                    "connector": connector_type,
                }

        return results

    # ----------------------------------------------------------------------
    # MAIN PIPELINE (A5)
    # ----------------------------------------------------------------------
    def _run_collectors(self):
        """
        Host-first collection pipeline.
        NEW: Iterates through hosts, then services on each host.
        """

        logger = self.logger          # shortcut

        results = {
            "host": {},
            "containers": {},
            "os_nodes": {},
        }

        env = self.config
        all_hosts = env.get("hosts", [])
        precheck_unreachable = getattr(self, "_precheck_unreachable", set())
        precheck_errors = getattr(self, "_precheck_errors", {})
        precheck_available = getattr(self, "_precheck_available_hosts", set())
        precheck_results = getattr(self, "_precheck_results", {})

        # Initialize component evidence containers upfront
        for comp in self.components:
            results["host"][comp] = {
                "type": "host",
                "discovered": [],
                "instances": [],  # Normalized structure
                "findings": {}  # Legacy structure for backward compatibility
            }

        from connectors.host_connectors.local_host_connector import LocalHostConnector
        from connectors.host_connectors.ssh_host_connector import SSHHostConnector
        from connectors.host_connectors.docker_host_connector import DockerHostConnector

        # Connector selector helper
        def _make_host_connector(host_info: dict, comp_cfg: dict | None = None):
            global_access = (env.get("access", {}) or {})
            host_info = host_info or {}
            host_type = host_info.get("type", "")

            # Ensure short SSH timeout for VMs if not provided
            if host_type == "vm":
                host_info = dict(host_info)
                ssh_cfg = dict(host_info.get("ssh") or {})
                if "timeout" not in ssh_cfg:
                    ssh_cfg["timeout"] = 5
                if ssh_cfg:
                    host_info["ssh"] = ssh_cfg

            # 1) Explicit SSH config takes highest priority
            if host_info.get("ssh"):
                return SSHHostConnector(host_info=host_info, global_access=global_access)

            # 2) Check if component instances require Docker (container-based services)
            # This should come BEFORE VM type check to handle Docker containers correctly
            # Even if host is marked as "vm", if services use Docker containers, use DockerHostConnector
            if comp_cfg:
                for inst in comp_cfg.get("instances", []):
                    if inst.get("container") or inst.get("runtime") == "docker":
                        try:
                            return DockerHostConnector(host_info=host_info, global_access=global_access)
                        except Exception:
                            return LocalHostConnector(host_info=host_info)

            # 3) Check for Docker host: type == "docker-host" OR explicit docker config
            docker_cfg = host_info.get("docker") or {}
            if host_info.get("type") == "docker-host":
                try:
                    return DockerHostConnector(host_info=host_info, global_access=global_access)
                except Exception:
                    return LocalHostConnector(host_info=host_info)
            elif docker_cfg.get("use_local_socket") or docker_cfg.get("base_url"):
                # Host has explicit docker configuration
                try:
                    return DockerHostConnector(host_info=host_info, global_access=global_access)
                except Exception:
                    return LocalHostConnector(host_info=host_info)

            # 4) For VM type hosts, prefer SSH connector (even if SSH config not explicit)
            # This ensures we try to connect remotely and report connection failures properly
            # BUT only if we haven't already determined Docker is needed above
            if host_type == "vm":
                # Try SSH first - it will report connection failures if host is unreachable
                return SSHHostConnector(host_info=host_info, global_access=global_access)

            # 5) Global SSH access config
            if global_access.get("ssh"):
                return SSHHostConnector(host_info=host_info, global_access=global_access)

            # 6) Default to local connector
            return LocalHostConnector(host_info=host_info)

        # Track connection failures for summary (seed with pre-check failures)
        connection_failures = dict(precheck_errors)  # {host_name: error_message}
        
        # Track per-host collection status: {host_name: {requested: [...], collected: [...], missing: [...], unavailable: [...]}}
        host_collection_status = {}
        
        # Helper to check if host is completely unreachable
        def _is_connection_failure(errors_list):
            """Check if all errors are connection-related failures."""
            if not errors_list:
                return False
            connection_keywords = [
                "SSH connection failed", "connection failed", "timed out", 
                "Connection refused", "ConnectionError", "Connection timeout",
                "connect failed", "unreachable", "No route to host"
            ]
            for error in errors_list:
                error_msg = error.get("message", "") if isinstance(error, dict) else str(error)
                error_msg_lower = error_msg.lower()
                if not any(keyword.lower() in error_msg_lower for keyword in connection_keywords):
                    return False
            return True

        # ================================================================
        # HOST-FIRST LOOP (NEW APPROACH)
        # ================================================================
        for host in all_hosts:
            host_name = host.get("name") or host.get("address")
            host_services = host.get("services", {})

            # If pre-check marked this host unreachable, skip collection
            if host_name in precheck_unreachable:
                logger.warning(f"Skipping host '{host_name}' due to pre-check failure: {connection_failures.get(host_name, 'unreachable')}")
                # Track that this host was requested but unreachable
                host_collection_status[host_name] = {
                    "requested": [comp for comp in self.components if comp in host_services],
                    "collected": [],
                    "missing": [],
                    "unavailable": [comp for comp in self.components if comp in host_services],
                    "unreachable": True
                }
                continue

            if not host_services:
                logger.debug(f"Host '{host_name}' has no services configured")
                # Track that this host has no services configured
                host_collection_status[host_name] = {
                    "requested": [],
                    "collected": [],
                    "missing": [],
                    "unavailable": [],
                    "unreachable": False
                }
                continue

            safe_host = sanitize_host_for_logging(host)
            logger.info(f"Processing host={safe_host}")

            # Track if this host has any successful collection
            host_has_data = False
            host_connection_error = None
            
            # Initialize tracking for this host
            host_requested_components = [comp for comp in self.components if comp in host_services]
            host_collection_status[host_name] = {
                "requested": host_requested_components,
                "collected": [],
                "missing": [],
                "unavailable": [],
                "unreachable": False
            }

            # For each requested component
            for comp in self.components:
                # If we already know this host is unreachable and have no data, skip remaining components
                if host_connection_error and not host_has_data:
                    logger.warning(f"Host '{host_name}' already unreachable; skipping remaining components.")
                    break

                # Skip if this host doesn't have this component
                if comp not in host_services:
                    # Component not configured for this host - don't track it
                    continue

                divider()
                print(f"🔹 Component: {comp} on {host_name}")

                logger.info(f"Running collector for component={comp} on host={safe_host}")

                ConnectorCls = CONNECTOR_REGISTRY.get(comp)
                if not ConnectorCls:
                    print(f"⚠️ No connector found for: {comp}")
                    logger.warning(f"No adapter for component: {comp}")
                    continue

                # Get service config (just instances, no hosts list)
                comp_cfg = host_services[comp]

                # Get component evidence container
                comp_evidence = results["host"][comp]

                # Create adapter for this component
                # Pass full env_config (includes access, hosts, metadata, etc.)
                adapter = ConnectorCls(
                    name=comp,
                    env_config=env,  # Full environment config
                    issue_time=self.issue_time,
                    component_config=comp_cfg  # Just the service instances config
                )

                # We already have host_info from the loop
                host_info = host

                connector = _make_host_connector(host_info, comp_cfg)
                
                # Early connection check for SSH connectors - if connection already failed, skip immediately
                if isinstance(connector, SSHHostConnector):
                    if hasattr(connector, '_connection_error') and connector._connection_error:
                        # Connection failed during initialization - skip this component
                        if not host_connection_error:
                            host_connection_error = connector._connection_error
                        print(f"⚠️ Host '{host_name}' connection failed during init: {connector._connection_error}")
                        logger.warning(f"Host '{host_name}' connection failed during init, skipping from evidence")
                        try:
                            connector.close()
                        except Exception:
                            pass
                        continue  # Skip this component for this host

                # Docker: run with timeout protection (even though DockerHostConnector has internal timeouts)
                if isinstance(connector, DockerHostConnector):
                    logger.debug(f"Using DockerHostConnector for host={safe_host}")
                    import threading, traceback as _tb
                    result_box = {"ok": False, "value": None, "exc": None}
                    
                    def _run_collect_docker():
                        try:
                            res = adapter.collect_for_host(host_info, connector)
                            result_box["ok"] = True
                            result_box["value"] = res
                        except Exception as e:
                            result_box["exc"] = (str(e), _tb.format_exc())
                    
                    worker = threading.Thread(target=_run_collect_docker, daemon=True)
                    worker.start()
                    worker.join(timeout=30)  # 30 second timeout for Docker operations
                    
                    if worker.is_alive():
                        logger.error(f"Timeout collecting Docker host={safe_host}")
                        comp_evidence["discovered"].append(host_name)
                        error_entry = {
                            "name": host_name,
                            "errors": [{
                                "type": "collection_error",
                                "component": comp,
                                "stage": "host_collection",
                                "message": "collect_for_host timed out after 30s (DockerHostConnector)"
                            }]
                        }
                        comp_evidence["instances"].append(error_entry)
                        # For nginx/tomcat, validator expects instances in findings
                        timeout_findings = {
                            "error": f"collect_for_host timed out after 30s"
                        }
                        if comp in ("nginx", "tomcat"):
                            timeout_findings["instances"] = [error_entry]
                        comp_evidence["findings"][host_name] = timeout_findings
                        # Timeout is a collection error, not service unavailable - mark as collected (with error)
                        if comp not in host_collection_status[host_name]["collected"]:
                            host_collection_status[host_name]["collected"].append(comp)
                    else:
                        if result_box["ok"]:
                            res = result_box["value"]
                            
                            # Check if this is a connection failure (all errors are connection-related)
                            if isinstance(res, dict):
                                errors = res.get("errors", [])
                                
                                # Check if we have any actual data (not just empty metrics/logs)
                                metrics = res.get("metrics", {})
                                logs = res.get("logs", {})
                                
                                # Check if metrics have actual content (not just empty dicts/strings)
                                has_metrics = False
                                if metrics:
                                    for metric_name, metric_data in metrics.items():
                                        if isinstance(metric_data, dict):
                                            stdout = metric_data.get("stdout", "")
                                            output = metric_data.get("output", "")
                                            if (stdout and str(stdout).strip()) or (output and str(output).strip()):
                                                has_metrics = True
                                                break
                                        elif isinstance(metric_data, str) and metric_data.strip():
                                            has_metrics = True
                                            break
                                
                                # Check if logs have actual content
                                has_logs = False
                                if logs:
                                    for log_name, log_data in logs.items():
                                        if isinstance(log_data, dict):
                                            content = log_data.get("content", "")
                                            if content and str(content).strip():
                                                has_logs = True
                                                break
                                        elif isinstance(log_data, str) and log_data.strip():
                                            has_logs = True
                                            break
                                
                            # If all errors are connection failures AND no actual data collected
                            if errors and _is_connection_failure(errors) and not has_metrics and not has_logs:
                                # Host is completely unreachable - don't add to evidence
                                if not host_connection_error:
                                    host_connection_error = errors[0].get("message", "Connection failed")
                                print(f"⚠️ Host '{host_name}' is unreachable: {host_connection_error}")
                                logger.warning(f"Host '{host_name}' unreachable, skipping from evidence")
                                # Mark component as unavailable due to connection failure
                                if comp not in host_collection_status[host_name]["unavailable"]:
                                    host_collection_status[host_name]["unavailable"].append(comp)
                                # Skip adding to evidence - close connector and continue to next component
                                try:
                                    connector.close()
                                except Exception:
                                    pass
                                # Skip this component for this host - don't add any evidence
                                continue  # This continues the inner loop (for comp in self.components)
                            
                            # Check if result indicates service not running (no instances found, but no connection error)
                            # This happens when adapter runs successfully but finds no service/containers
                            # OR when instances exist but only have liveness errors (service not running)
                            service_not_found = False
                            if isinstance(res, dict):
                                instances = res.get("instances", [])
                                # If no instances and no errors (or only "not found" type errors), service is not running
                                if not instances and not errors:
                                    service_not_found = True
                                elif not instances and errors:
                                    # Check if errors are "not found" type (service not running) vs actual errors
                                    not_found_keywords = ["not found", "no containers", "no instances", "service not running"]
                                    all_not_found = all(
                                        any(keyword in str(err.get("message", "")).lower() for keyword in not_found_keywords)
                                        for err in errors
                                    )
                                    if all_not_found:
                                        service_not_found = True
                                elif instances:
                                    # Check if all instances have only liveness errors (service not running)
                                    # Liveness errors indicate the service is not running, not a collection error
                                    liveness_keywords = ["liveness:", "not running", "container not running", "not found", "not installed", "not in path", "port not reachable", "binary not found"]
                                    all_liveness_errors = True
                                    has_any_data = False
                                    for inst in instances:
                                        # Check if instance has actual data (non-empty metrics or logs)
                                        inst_metrics = inst.get("metrics", {})
                                        inst_logs = inst.get("logs", {})
                                        if inst_metrics and isinstance(inst_metrics, dict) and inst_metrics:
                                            has_any_data = True
                                        if inst_logs and isinstance(inst_logs, dict) and inst_logs:
                                            has_any_data = True
                                        
                                        inst_errors = inst.get("errors", [])
                                        if not inst_errors:
                                            # If instance has no errors but also no data, it might be a valid empty instance
                                            # Only mark as liveness error if there's no data
                                            if not has_any_data:
                                                continue  # Check next instance
                                            else:
                                                all_liveness_errors = False
                                                break
                                        # Check if all errors in this instance are liveness errors
                                        inst_has_non_liveness = False
                                        for err in inst_errors:
                                            err_str = str(err).lower() if isinstance(err, str) else str(err.get("message", "")).lower() if isinstance(err, dict) else ""
                                            # If error doesn't contain liveness keywords, it's a real collection error
                                            if not any(keyword in err_str for keyword in liveness_keywords):
                                                inst_has_non_liveness = True
                                                break
                                        if inst_has_non_liveness:
                                            all_liveness_errors = False
                                            break
                                    # Only mark as service_not_found if all instances have liveness errors AND no actual data
                                    if all_liveness_errors and not has_any_data:
                                        logger.debug(f"Orchestrator: Detected service not running for {comp} on {host_name} (all instances have liveness errors)")
                                        service_not_found = True
                            
                            if service_not_found:
                                # Service is not running - mark as missing (not an error, just unavailable)
                                if comp not in host_collection_status[host_name]["missing"]:
                                    host_collection_status[host_name]["missing"].append(comp)
                                # Still add to evidence with a note that service is not running
                                comp_evidence["discovered"].append(host_name)
                                no_service_entry = {
                                    "name": host_name,
                                    "status": "service_not_running",
                                    "message": f"{comp} service is not running or not found on this host",
                                    "metrics": {},
                                    "logs": {}
                                }
                                comp_evidence["instances"].append(no_service_entry)
                                comp_evidence["findings"][host_name] = {
                                    "status": "service_not_running",
                                    "message": f"{comp} service is not running or not found"
                                }
                                continue
                            
                            if has_metrics or has_logs:
                                host_has_data = True
                            
                            comp_evidence["discovered"].append(host_name)
                            
                            # Mark component as collected for this host (has actual data)
                            if comp not in host_collection_status[host_name]["collected"]:
                                host_collection_status[host_name]["collected"].append(comp)
                            
                            # ALL adapter results should be normalized:
                            # 1. Always add to instances array (structured format)
                            # 2. Always flatten minimal fields into findings (backward compatibility)
                            
                            # Check if result has structured format (new adapters return this)
                            if isinstance(res, dict):
                                # If result has instances array, it's already structured
                                if "instances" in res and isinstance(res["instances"], list):
                                    # Result is component-level structure - extract instances
                                    instances_list = []
                                    for inst in res["instances"]:
                                        if "name" not in inst:
                                            inst["name"] = inst.get("name") or host_name
                                        comp_evidence["instances"].append(inst)
                                        instances_list.append(inst)
                                    
                                    # Flatten for backward compatibility
                                    legacy_findings = {
                                        **res.get("metrics", {}),
                                        **res.get("logs", {})
                                    }
                                    for key in ["name", "errors", "commands", "collection_mode", "time_window", "type"]:
                                        if key in res:
                                            legacy_findings[key] = res[key]
                                    # For nginx/tomcat, validator expects instances in findings
                                    if comp in ("nginx", "tomcat"):
                                        legacy_findings["instances"] = instances_list
                                    comp_evidence["findings"][host_name] = legacy_findings
                                elif "name" in res or "metrics" in res or "logs" in res:
                                    # Result is instance-level structure
                                    if "name" not in res:
                                        res["name"] = host_name
                                    comp_evidence["instances"].append(res)
                                    
                                    # Flatten for backward compatibility
                                    legacy_findings = {
                                        **res.get("metrics", {}),
                                        **res.get("logs", {})
                                    }
                                    for key in ["name", "errors", "commands", "collection_mode", "time_window"]:
                                        if key in res:
                                            legacy_findings[key] = res[key]
                                    # For nginx/tomcat, validator expects instances in findings
                                    if comp in ("nginx", "tomcat"):
                                        legacy_findings["instances"] = [res]
                                    comp_evidence["findings"][host_name] = legacy_findings
                                else:
                                    # Legacy format - store in both places for compatibility
                                    comp_evidence["findings"][host_name] = res
                                    # Create minimal instance entry
                                    comp_evidence["instances"].append({
                                        "name": host_name,
                                        "raw_findings": res
                                    })
                            else:
                                # Non-dict result - store as-is in findings only
                                comp_evidence["findings"][host_name] = res
                        elif result_box["exc"]:
                            # Exception occurred in thread
                            err_msg, err_tb = result_box["exc"]
                            
                            # Distinguish between connection failures (unreachable hosts) and collection errors (timeouts, etc.)
                            # Connection failures: SSH connection failed, connection refused, unreachable, etc.
                            # Collection errors: timeouts during collection, adapter errors, etc. - should be captured in evidence
                            is_connection_failure = (
                                "ssh connection failed" in err_msg.lower() or
                                "connection refused" in err_msg.lower() or
                                "connection timeout" in err_msg.lower() or
                                "unreachable" in err_msg.lower() or
                                "no route to host" in err_msg.lower() or
                                "connect failed" in err_msg.lower()
                            )
                            
                            if is_connection_failure:
                                # Host is unreachable - skip from evidence
                                if not host_connection_error:
                                    host_connection_error = err_msg
                                print(f"⚠️ Host '{host_name}' connection failed: {err_msg}")
                                logger.warning(f"Host '{host_name}' connection failed, skipping from evidence")
                                continue  # Skip this component for this host
                            
                            # Collection error (timeout, adapter error, etc.) - capture in evidence
                            comp_evidence["discovered"].append(host_name)
                            error_entry = {
                                "name": host_name,
                                "errors": [{
                                    "type": "collection_error",
                                    "component": comp,
                                    "stage": "host_collection",
                                    "message": err_msg,
                                    "traceback": err_tb
                                }]
                            }
                            comp_evidence["instances"].append(error_entry)
                            comp_evidence["findings"][host_name] = {"error": err_msg}
                            logger.exception(f"collector failed for host={safe_host}")

                    try:
                        connector.close()
                    except Exception:
                        pass

                    # Evidence already added to comp_evidence, continue to next component
                    continue

                # Non-docker: threaded mode with retry logic
                import threading, traceback as _tb, time
                
                # Timeout configuration: increased for cold starts, with retry support
                HOST_COLLECT_TIMEOUT = 35  # Increased from 20s to handle cold starts
                MAX_RETRIES = 1  # Retry once on timeout
                RETRY_DELAY = 2  # Wait 2s before retry
                
                collection_succeeded = False
                final_result_box = None
                final_exc = None
                attempt_count = 0
                last_timeout_error = None
                
                for retry_attempt in range(MAX_RETRIES + 1):
                    attempt_count = retry_attempt + 1
                    result_box = {"ok": False, "value": None, "exc": None}

                    def _run_collect():
                        try:
                            res = adapter.collect_for_host(host_info, connector)
                            result_box["ok"] = True
                            result_box["value"] = res
                        except Exception as e:
                            result_box["exc"] = (str(e), _tb.format_exc())

                    worker = threading.Thread(target=_run_collect, daemon=True)
                    worker.start()
                    worker.join(timeout=HOST_COLLECT_TIMEOUT)

                    if worker.is_alive():
                        # Timeout occurred
                        timeout_msg = f"collect_for_host timed out after {HOST_COLLECT_TIMEOUT}s"
                        last_timeout_error = timeout_msg
                        
                        if retry_attempt < MAX_RETRIES:
                            # Log retry attempt and wait before retrying
                            logger.warning(
                                f"Timeout collecting for host={safe_host}, component={comp} "
                                f"(attempt {attempt_count}/{MAX_RETRIES + 1}). Retrying after {RETRY_DELAY}s delay..."
                            )
                            time.sleep(RETRY_DELAY)
                            continue  # Retry
                        else:
                            # Max retries reached - log final failure
                            logger.error(
                                f"Timeout collecting for host={safe_host}, component={comp} "
                                f"after {MAX_RETRIES + 1} attempts"
                            )
                            collection_succeeded = False
                            break
                    else:
                        # Worker completed (either success or exception)
                        collection_succeeded = True
                        final_result_box = result_box
                        if result_box["exc"]:
                            final_exc = result_box["exc"]
                        break  # Success or non-timeout error - no retry needed
                
                # Handle final result
                if not collection_succeeded:
                    # Final timeout after retries
                    comp_evidence["discovered"].append(host_name)
                    error_entry = {
                        "name": host_name,
                        "errors": [{
                            "type": "collection_error",
                            "component": comp,
                            "stage": "host_collection",
                            "message": f"{last_timeout_error} (after {attempt_count} attempt(s))"
                        }]
                    }
                    comp_evidence["instances"].append(error_entry)
                    # For nginx/tomcat, validator expects instances in findings
                    timeout_findings = {
                        "error": f"{last_timeout_error} (after {attempt_count} attempt(s))"
                    }
                    if comp in ("nginx", "tomcat"):
                        timeout_findings["instances"] = [error_entry]
                    comp_evidence["findings"][host_name] = timeout_findings
                    # Timeout is a collection error, not service unavailable - mark as collected (with error)
                    if comp not in host_collection_status[host_name]["collected"]:
                        host_collection_status[host_name]["collected"].append(comp)
                    
                    try:
                        connector.close()
                    except Exception:
                        pass
                    continue  # Continue to next component
                elif final_exc:
                    # Non-timeout exception occurred - handle as before
                    err_msg, err_tb = final_exc
                    
                    # Distinguish between connection failures (unreachable hosts) and collection errors (timeouts, etc.)
                    # Connection failures: SSH connection failed, connection refused, unreachable, etc.
                    # Collection errors: timeouts during collection, adapter errors, etc. - should be captured in evidence
                    is_connection_failure = (
                        "ssh connection failed" in err_msg.lower() or
                        "connection refused" in err_msg.lower() or
                        "connection timeout" in err_msg.lower() or
                        "unreachable" in err_msg.lower() or
                        "no route to host" in err_msg.lower() or
                        "connect failed" in err_msg.lower()
                    )
                    
                    if is_connection_failure:
                        # Host is unreachable - skip from evidence
                        if not host_connection_error:
                            host_connection_error = err_msg
                        print(f"⚠️ Host '{host_name}' connection failed: {err_msg}")
                        logger.warning(f"Host '{host_name}' connection failed, skipping from evidence")
                        continue  # Skip this component for this host
                    
                    # Collection error (timeout, adapter error, etc.) - capture in evidence
                    comp_evidence["discovered"].append(host_name)
                    error_entry = {
                        "name": host_name,
                        "errors": [{
                            "type": "collection_error",
                            "component": comp,
                            "stage": "host_collection",
                            "message": err_msg,
                            "traceback": err_tb
                        }]
                    }
                    comp_evidence["instances"].append(error_entry)
                    comp_evidence["findings"][host_name] = {"error": err_msg}
                    logger.exception(f"collector failed for host={safe_host}")
                    
                    try:
                        connector.close()
                    except Exception:
                        pass
                    continue  # Continue to next component
                else:
                    # Collection succeeded
                    if final_result_box and final_result_box["ok"]:
                        res = final_result_box["value"]
                        
                        # Check if this is a connection failure BEFORE adding any evidence
                        should_skip = False
                        has_actual_data = False
                        if isinstance(res, dict):
                            errors = res.get("errors", [])
                            
                            # Check if we have any actual data (not just empty metrics/logs)
                            metrics = res.get("metrics", {})
                            logs = res.get("logs", {})
                            
                            # Check if metrics have actual content (not just empty dicts/strings)
                            has_metrics = False
                            if metrics:
                                for metric_name, metric_data in metrics.items():
                                    if isinstance(metric_data, dict):
                                        stdout = metric_data.get("stdout", "")
                                        output = metric_data.get("output", "")
                                        if (stdout and str(stdout).strip()) or (output and str(output).strip()):
                                            has_metrics = True
                                            break
                                    elif isinstance(metric_data, str) and metric_data.strip():
                                        has_metrics = True
                                        break
                            
                            # Check if logs have actual content
                            has_logs = False
                            if logs:
                                for log_name, log_data in logs.items():
                                    if isinstance(log_data, dict):
                                        content = log_data.get("content", "")
                                        if content and str(content).strip():
                                            has_logs = True
                                            break
                                    elif isinstance(log_data, str) and log_data.strip():
                                        has_logs = True
                                        break
                            
                            # If all errors are connection failures AND no actual data collected
                            if errors and _is_connection_failure(errors) and not has_metrics and not has_logs:
                                # Host is completely unreachable - don't add to evidence
                                should_skip = True
                                if not host_connection_error:
                                    host_connection_error = errors[0].get("message", "Connection failed")
                                print(f"⚠️ Host '{host_name}' is unreachable: {host_connection_error}")
                                logger.warning(f"Host '{host_name}' unreachable, skipping from evidence")
                                # Mark component as unavailable due to connection failure
                                if comp not in host_collection_status[host_name]["unavailable"]:
                                    host_collection_status[host_name]["unavailable"].append(comp)
                            else:
                                has_actual_data = has_metrics or has_logs
                            
                            if has_metrics or has_logs:
                                host_has_data = True
                        
                        # Skip adding to evidence if connection failure detected
                        if should_skip:
                            try:
                                connector.close()
                            except Exception:
                                pass
                            continue  # Skip this component for this host - don't add any evidence
                        
                        # Check if result indicates service not running (no instances found, but no connection error)
                        # OR when instances exist but only have liveness errors (service not running)
                        service_not_found = False
                        if isinstance(res, dict):
                            instances = res.get("instances", [])
                            errors = res.get("errors", [])
                            
                            # If no instances and no actual data (metrics/logs), check if service is not running
                            if not instances and not has_actual_data:
                                # If no errors, or only "not found" type errors, service is not running
                                if not errors:
                                    service_not_found = True
                                else:
                                    # Check if errors are "not found" type (service not running) vs actual errors
                                    not_found_keywords = ["not found", "no containers", "no instances", "service not running", "container not found"]
                                    all_not_found = all(
                                        any(keyword in str(err.get("message", "")).lower() for keyword in not_found_keywords)
                                        for err in errors
                                    )
                                    if all_not_found:
                                        service_not_found = True
                            elif instances and not has_actual_data:
                                # Check if all instances have only liveness errors (service not running)
                                # Liveness errors indicate the service is not running, not a collection error
                                liveness_keywords = ["liveness:", "not running", "container not running", "not found", "not installed", "not in path", "port not reachable", "binary not found"]
                                all_liveness_errors = True
                                has_any_data = False
                                for inst in instances:
                                    # Check if instance has actual data (non-empty metrics or logs)
                                    inst_metrics = inst.get("metrics", {})
                                    inst_logs = inst.get("logs", {})
                                    if inst_metrics and isinstance(inst_metrics, dict) and inst_metrics:
                                        has_any_data = True
                                    if inst_logs and isinstance(inst_logs, dict) and inst_logs:
                                        has_any_data = True
                                    
                                    inst_errors = inst.get("errors", [])
                                    if not inst_errors:
                                        # If instance has no errors but also no data, it might be a valid empty instance
                                        # Only mark as liveness error if there's no data
                                        if not has_any_data:
                                            continue  # Check next instance
                                        else:
                                            all_liveness_errors = False
                                            break
                                    # Check if all errors in this instance are liveness errors
                                    inst_has_non_liveness = False
                                    for err in inst_errors:
                                        err_str = str(err).lower() if isinstance(err, str) else str(err.get("message", "")).lower() if isinstance(err, dict) else ""
                                        # If error doesn't contain liveness keywords, it's a real collection error
                                        if not any(keyword in err_str for keyword in liveness_keywords):
                                            inst_has_non_liveness = True
                                            break
                                    if inst_has_non_liveness:
                                        all_liveness_errors = False
                                        break
                                # Only mark as service_not_found if all instances have liveness errors AND no actual data
                                if all_liveness_errors and not has_any_data:
                                    logger.debug(f"Orchestrator: Detected service not running for {comp} on {host_name} (all instances have liveness errors)")
                                    service_not_found = True
                        
                        if service_not_found:
                            # Service is not running - mark as missing (not an error, just unavailable)
                            if comp not in host_collection_status[host_name]["missing"]:
                                host_collection_status[host_name]["missing"].append(comp)
                            # Still add to evidence with a note that service is not running
                            comp_evidence["discovered"].append(host_name)
                            no_service_entry = {
                                "name": host_name,
                                "status": "service_not_running",
                                "message": f"{comp} service is not running or not found on this host",
                                "metrics": {},
                                "logs": {}
                            }
                            comp_evidence["instances"].append(no_service_entry)
                            comp_evidence["findings"][host_name] = {
                                "status": "service_not_running",
                                "message": f"{comp} service is not running or not found"
                            }
                            continue
                        
                        # Only add to evidence if we didn't skip due to connection failure
                        comp_evidence["discovered"].append(host_name)
                        
                        # Mark component as collected for this host (has actual data or error entries)
                        if comp not in host_collection_status[host_name]["collected"]:
                            host_collection_status[host_name]["collected"].append(comp)
                        
                        # ALL adapter results should be normalized (same logic as Docker path above)
                        if isinstance(res, dict):
                            # If result has instances array, it's already structured
                            if "instances" in res and isinstance(res["instances"], list):
                                # Result is component-level structure - extract instances
                                instances_list = []
                                for inst in res["instances"]:
                                    if "name" not in inst:
                                        inst["name"] = inst.get("name") or host_name
                                    comp_evidence["instances"].append(inst)
                                    instances_list.append(inst)
                                
                                # Flatten for backward compatibility
                                legacy_findings = {
                                    **res.get("metrics", {}),
                                    **res.get("logs", {})
                                }
                                for key in ["name", "errors", "commands", "collection_mode", "time_window", "type"]:
                                    if key in res:
                                        legacy_findings[key] = res[key]
                                # For nginx/tomcat, validator expects instances in findings
                                if comp in ("nginx", "tomcat"):
                                    legacy_findings["instances"] = instances_list
                                comp_evidence["findings"][host_name] = legacy_findings
                            elif "name" in res or "metrics" in res or "logs" in res:
                                # Result is instance-level structure
                                if "name" not in res:
                                    res["name"] = host_name
                                comp_evidence["instances"].append(res)
                                
                                # Flatten for backward compatibility
                                legacy_findings = {
                                    **res.get("metrics", {}),
                                    **res.get("logs", {})
                                }
                                for key in ["name", "errors", "commands", "collection_mode", "time_window"]:
                                    if key in res:
                                        legacy_findings[key] = res[key]
                                # For nginx/tomcat, validator expects instances in findings
                                if comp in ("nginx", "tomcat"):
                                    legacy_findings["instances"] = [res]
                                comp_evidence["findings"][host_name] = legacy_findings
                            else:
                                # Legacy format - store in both places for compatibility
                                comp_evidence["findings"][host_name] = res
                                # Create minimal instance entry
                                comp_evidence["instances"].append({
                                    "name": host_name,
                                    "raw_findings": res
                                })
                        else:
                            # Non-dict result - store as-is in findings only
                            comp_evidence["findings"][host_name] = res
                    elif result_box["exc"]:
                            # Exception occurred in thread
                            err_msg, err_tb = result_box["exc"]
                            
                            # Distinguish between connection failures (unreachable hosts) and collection errors (timeouts, etc.)
                            # Connection failures: SSH connection failed, connection refused, unreachable, etc.
                            # Collection errors: timeouts during collection, adapter errors, etc. - should be captured in evidence
                            is_connection_failure = (
                                "ssh connection failed" in err_msg.lower() or
                                "connection refused" in err_msg.lower() or
                                "connection timeout" in err_msg.lower() or
                                "unreachable" in err_msg.lower() or
                                "no route to host" in err_msg.lower() or
                                "connect failed" in err_msg.lower()
                            )
                            
                            if is_connection_failure:
                                # Host is unreachable - skip from evidence
                                if not host_connection_error:
                                    host_connection_error = err_msg
                                print(f"⚠️ Host '{host_name}' connection failed: {err_msg}")
                                logger.warning(f"Host '{host_name}' connection failed, skipping from evidence")
                                # Mark component as unavailable due to connection failure
                                if comp not in host_collection_status[host_name]["unavailable"]:
                                    host_collection_status[host_name]["unavailable"].append(comp)
                                continue  # Skip this component for this host
                            
                            # Collection error (timeout, adapter error, etc.) - capture in evidence
                            comp_evidence["discovered"].append(host_name)
                            error_entry = {
                                "name": host_name,
                                "errors": [{
                                    "type": "collection_error",
                                    "component": comp,
                                    "stage": "host_collection",
                                    "message": err_msg,
                                    "traceback": err_tb
                                }]
                            }
                            comp_evidence["instances"].append(error_entry)
                            comp_evidence["findings"][host_name] = {"error": err_msg}
                            logger.error(f"collect_for_host error for host={safe_host}: {err_msg}")
                            # Collection error is still considered "collected" (with error) - mark as collected
                            if comp not in host_collection_status[host_name]["collected"]:
                                host_collection_status[host_name]["collected"].append(comp)

                try:
                    connector.close()
                except Exception:
                    pass

                # Evidence already added to comp_evidence, continue to next component
            
            # After processing all components for this host, check if host was completely unreachable
            if host_connection_error and not host_has_data:
                # Host had connection failures and no successful data collection
                connection_failures[host_name] = host_connection_error
                print(f"⚠️ Removing unreachable host '{host_name}' from evidence (connection failed, no data collected)")
                logger.warning(f"Removing unreachable host '{host_name}' from evidence")
                
                # Remove any partial evidence for this host (cleanup)
                for comp in self.components:
                    if comp in results["host"]:
                        comp_evidence = results["host"][comp]
                        # Remove from discovered if present
                        discovered_list = comp_evidence.get("discovered", [])
                        if host_name in discovered_list:
                            discovered_list.remove(host_name)
                            comp_evidence["discovered"] = discovered_list
                        # Remove from findings
                        findings_dict = comp_evidence.get("findings", {})
                        if host_name in findings_dict:
                            del findings_dict[host_name]
                        # Remove from instances
                        instances_list = comp_evidence.get("instances", [])
                        comp_evidence["instances"] = [
                            inst for inst in instances_list
                            if inst.get("name") != host_name
                        ]
                        logger.debug(f"Cleaned up evidence for host '{host_name}' in component '{comp}'")

        # Add connection failures summary to results
        if connection_failures:
            results["connection_failures"] = connection_failures
            print(f"\n⚠️ Connection Failures Summary:")
            for host_name, error_msg in connection_failures.items():
                print(f"   - {host_name}: {error_msg}")
            logger.warning(f"Connection failures for hosts: {list(connection_failures.keys())}")

        # Add per-host collection status to results (for UI summary)
        results["host_collection_status"] = host_collection_status
        logger.info(f"Host collection status: {host_collection_status}")

        # Surface pre-check results for UI/consumers
        results["precheck"] = getattr(self, "_precheck_results", {})

        return results
    
    # ----------------------------------------------------------------------
    # Run RCA on existing evidence file
    # ----------------------------------------------------------------------
    def run_rca_on_file(self, evidence_file: str, api_key: str, model: str = "gpt-4o-mini"):
        """
        Run RCA analysis on an existing evidence file.
        
        Args:
            evidence_file: Path to evidence JSON file
            api_key: OpenAI API key
            model: Model name (default: gpt-4o-mini)
        
        Returns:
            Tuple of (rca_text, tasks)
        """
        p = Path(evidence_file)
        if not p.exists():
            raise FileNotFoundError(f"Evidence file not found: {evidence_file}")
        
        # Extract environment from evidence file if available
        environment = None
        try:
            import json
            with open(evidence_file, 'r') as f:
                evidence_data = json.load(f)
                environment = evidence_data.get("metadata", {}).get("environment") or evidence_data.get("environment")
        except:
            pass
        
        # Use orchestrator's environment if available
        if not environment:
            environment = self.environment
        
        # Use feature flag to get appropriate client (RCAClient or LLMReasoner)
        reasoner = get_reasoning_client(api_key=api_key, model=model, environment=environment)
        rca_text = reasoner.analyze(str(p))
        
        try:
            tasks = reasoner.generate_tasks_from_rca(rca_text)
        except Exception:
            tasks = []
        
        return rca_text, tasks