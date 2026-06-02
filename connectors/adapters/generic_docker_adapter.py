# connectors/adapters/generic_docker_adapter.py
"""
Generic Docker Adapter – collects logs and status for Docker services in config
that do not have a dedicated adapter. Execution-only; no log interpretation.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from connectors.base_connector import BaseConnector
from connectors.adapters.docker_adapter import _parse_docker_inspect

logger = logging.getLogger("grcai.adapters.generic_docker")

# Max characters to store per container log (avoid huge evidence)
DEFAULT_LOG_TAIL_LINES = 2000
MAX_LOG_CHARS = 50 * 1024


def _is_connector_error(out: str) -> bool:
    """Return True for adapter/connector sentinel errors, not valid JSON/log lines."""
    return out.startswith((
        "[host exec error:",
        "[docker-host exec error:",
        "[docker-host logs error:",
        "[docker-host error:",
        "[docker-host timeout",
        "[docker not available]",
        "[no exec method]",
    ))


class GenericDockerAdapter(BaseConnector):
    """
    Collects container metadata, resource usage, and logs for every Docker
    service in the host's config that does not have a dedicated adapter.
    Reports facts only; no interpretation.
    """

    def __init__(self, name=None, env_config=None, issue_time=None, component_config=None, **kwargs):
        super().__init__(name or "generic_docker", issue_time, component_config)
        self.env_config = env_config or {}
        self.component_config = component_config or {}

    def _exec(self, connector, cmd: str) -> str:
        """Execute command on host (not in container)."""
        if hasattr(connector, "exec_cmd"):
            try:
                out = connector.exec_cmd(cmd)
                if isinstance(out, dict):
                    stdout = out.get("stdout", "") or out.get("output", "")
                    return stdout
                return str(out or "")
            except Exception as e:
                logger.debug("Generic docker adapter _exec: %s", e)
                return f"[host exec error: {e}]"
        return "[no exec method]"

    def _get_docker_inspect(self, connector, container_id: str) -> Optional[dict]:
        """Get docker inspect output as dict."""
        try:
            if hasattr(connector, "client") and connector.client:
                try:
                    container = connector.client.containers.get(container_id)
                    return container.attrs
                except Exception:
                    pass
            cmd = f"docker inspect {container_id} 2>/dev/null"
            out = self._exec(connector, cmd)
            if out and not _is_connector_error(out):
                try:
                    inspect_list = json.loads(out)
                    if inspect_list and len(inspect_list) > 0:
                        return inspect_list[0]
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            logger.debug("Failed to get docker inspect for %s: %s", container_id, e)
        return None

    def _get_container_logs(self, connector, container_id: str, tail: int = DEFAULT_LOG_TAIL_LINES) -> str:
        """Get container logs (stdout+stderr). Returns string or error message."""
        try:
            if hasattr(connector, "get_container_logs"):
                try:
                    out = connector.get_container_logs(container_id, tail=tail)
                    if isinstance(out, dict):
                        out = out.get("stdout", out.get("output", "")) or ""
                    out = str(out or "")
                    if out and not _is_connector_error(out):
                        if len(out) > MAX_LOG_CHARS:
                            out = out[-MAX_LOG_CHARS:]
                        return out
                except NotImplementedError:
                    pass
            if hasattr(connector, "exec_cmd"):
                cmd = f"timeout 15 docker logs --tail {tail} {container_id} 2>&1 || true"
                out = self._exec(connector, cmd)
                if out and not _is_connector_error(out):
                    if len(out) > MAX_LOG_CHARS:
                        out = out[-MAX_LOG_CHARS:]
                    return out
        except Exception as e:
            logger.debug("Failed to get container logs for %s: %s", container_id, e)
        return ""

    def collect_for_host(self, host_info: dict, connector) -> dict:
        """
        Collect logs and status for all unclaimed Docker services on this host.
        Returns: type, discovered, instances, errors (same shape as other adapters).
        """
        host_name = host_info.get("name") or host_info.get("address") or "unknown"
        findings = {
            "type": "generic_docker",
            "discovered": [host_name],
            "instances": [],
            "errors": [],
        }

        host_services = self.component_config.get("_host_services")
        if not host_services or not isinstance(host_services, dict):
            logger.debug("Generic docker adapter: no _host_services, returning empty")
            findings["instances"].append({
                "name": host_name,
                "status": "no_services",
                "metrics": {},
                "logs": {},
                "errors": [],
            })
            return findings

        log_tail = self.component_config.get("log_tail_lines", DEFAULT_LOG_TAIL_LINES)
        collect_only = self.component_config.get("collect_only")
        if isinstance(collect_only, str):
            collect_only = {collect_only}
        elif isinstance(collect_only, (list, tuple, set)):
            collect_only = set(collect_only)
        else:
            collect_only = None
        collected_any = False

        # Lazy import to avoid circular import (registry imports this module)
        from connectors.registry import CONNECTOR_REGISTRY

        for svc_name, svc_cfg in host_services.items():
            if svc_name in CONNECTOR_REGISTRY or svc_name == "generic_docker":
                continue
            if collect_only is not None and svc_name not in collect_only:
                continue
            instances_cfg = svc_cfg.get("instances", []) if isinstance(svc_cfg, dict) else []
            for inst in instances_cfg:
                if not isinstance(inst, dict):
                    continue
                if not (inst.get("container") or inst.get("runtime") == "docker"):
                    continue
                container_id_or_name = inst.get("container") or inst.get("name")
                if not container_id_or_name:
                    continue
                inst_name = inst.get("name") or svc_name or container_id_or_name
                port = inst.get("port")
                entry = {
                    "name": inst_name,
                    "metrics": {},
                    "logs": {},
                    "errors": [],
                }
                if port is not None:
                    entry["port"] = port

                try:
                    inspect_data = self._get_docker_inspect(connector, container_id_or_name)
                    if inspect_data:
                        parsed = _parse_docker_inspect(inspect_data)
                        entry["metrics"]["container_metadata"] = {
                            "status": parsed.get("status", "unknown"),
                            "running": parsed.get("running", False),
                            "restart_count": parsed.get("restart_count", 0),
                            "image": parsed.get("image", ""),
                            "oom_killed": parsed.get("oom_killed", False),
                            "health_status": parsed.get("health_status", "none"),
                        }
                        entry["status"] = parsed.get("status", "unknown")
                        entry["container_id"] = inspect_data.get("Id", "")[:12] if inspect_data.get("Id") else None
                    else:
                        entry["errors"].append("Could not get container inspect")
                        entry["status"] = "unknown"

                    logs_content = self._get_container_logs(connector, container_id_or_name, tail=log_tail)
                    if logs_content:
                        entry["logs"]["container_logs"] = logs_content
                except Exception as e:
                    logger.warning("Generic docker adapter: error collecting %s: %s", inst_name, e)
                    entry["errors"].append(str(e))

                findings["instances"].append(entry)
                collected_any = True

        if not collected_any:
            findings["instances"].append({
                "name": host_name,
                "status": "no_unclaimed_containers",
                "metrics": {},
                "logs": {},
                "errors": [],
            })

        return findings
