# connectors/host_connectors/base_host_connector.py

class BaseHostConnector:
    """
    Base class for all host connectors.

    A HostConnector represents *one host/node* in the environment.
    (VM, docker-host, baremetal, db-host, etc.)

    Phase A: Only LocalHostConnector implements functionality.
    Other connectors remain stubs for Phase B.
    """

    def __init__(self, host_info: dict):
        """
        host_info = {
            "name": "dev-node-1",
            "type": "docker-host",
            "address": "localhost",
            "ssh": {...},
            "docker": {...}
        }
        """
        self.host = host_info or {}
        self.host_name = self.host.get("name")
        self.host_type = self.host.get("type")
        self.address = self.host.get("address")

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