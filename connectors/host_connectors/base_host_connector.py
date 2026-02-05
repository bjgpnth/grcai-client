# connectors/host_connectors/base_host_connector.py

import logging
from typing import Optional

logger = logging.getLogger("grcai.connectors.host")

class BaseHostConnector:
    """
    Base class for all host connectors.

    A HostConnector represents *one host/node* in the environment.
    (VM, docker-host, baremetal, db-host, etc.)

    Phase A: Only LocalHostConnector implements functionality.
    Other connectors remain stubs for Phase B.
    """

    def __init__(self, host_info: dict, query_timezone_immediately: bool = True):
        """
        host_info = {
            "name": "dev-node-1",
            "type": "docker-host",
            "address": "localhost",
            "ssh": {...},
            "docker": {...},
            "timezone": "Asia/Kolkata"  # Optional fallback
        }
        
        query_timezone_immediately: If False, timezone query will be deferred
        (useful for SSH connectors that need connection first)
        """
        self.host = host_info or {}
        self.host_name = self.host.get("name")
        self.host_type = self.host.get("type")
        self.address = self.host.get("address")
        
        # Timezone support (Phase 2)
        # Priority: 1) Runtime query, 2) Config fallback, 3) None (will default to UTC)
        self.timezone = None
        if query_timezone_immediately:
            self._query_host_timezone()
    
    def _query_host_timezone(self) -> Optional[str]:
        """
        Query host timezone and return IANA timezone string.
        
        This method should be overridden by subclasses to implement
        actual timezone query logic. Falls back to config if query fails.
        
        Returns:
            IANA timezone string (e.g., 'Asia/Kolkata') or None if query fails
        """
        host_name = self.host_name or "unknown"
        
        # Try runtime query first (implemented by subclasses)
        tz = self._query_timezone_runtime()
        if tz:
            self.timezone = tz
            logger.info(f"🌍 Host '{host_name}': Timezone queried successfully: {tz}")
            return tz
        
        # Fallback to config
        config_tz = self.host.get("timezone")
        if config_tz:
            self.timezone = config_tz
            logger.info(f"🌍 Host '{host_name}': Using timezone from config: {config_tz}")
            return config_tz
        
        # No timezone available - will default to UTC in parsers
        self.timezone = None
        logger.warning(f"🌍 Host '{host_name}': No timezone available (runtime query failed, no config fallback). Logs will be parsed as UTC.")
        return None
    
    def _query_timezone_runtime(self) -> Optional[str]:
        """
        Query timezone at runtime from the host.
        
        This method should be overridden by subclasses to implement
        actual timezone query (e.g., via SSH, local system, etc.).
        
        Returns:
            IANA timezone string or None if query fails
        """
        # Base implementation returns None
        # Subclasses should override this
        return None

    # ------------------------------------------------------------
    # OS commands (to be overridden)
    # ------------------------------------------------------------
    def exec_cmd(self, cmd: str):
        """Execute command on this host (local or remote)."""
        raise NotImplementedError()

    # ------------------------------------------------------------
    # Docker capabilities (to be overridden)
    # ------------------------------------------------------------
    def list_containers(self, filters=None):
        raise NotImplementedError()

    def get_container_logs(self, container_id, **kwargs):
        raise NotImplementedError()

    def exec_in_container(self, container_id, cmd: str):
        raise NotImplementedError()

    def read_file(self, path: str):
        raise NotImplementedError()