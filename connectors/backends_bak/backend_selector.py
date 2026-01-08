# connectors/backends/backend_selector.py

from connectors.backends.ssh_backend import SSHBackend
from connectors.backends.docker_backend import DockerBackend
from connectors.backends.docker_os_backend import DockerOSBackend

def select_backend(host_info=None, instance_config=None, docker_enabled=False):
    """
    Central backend selection logic for all adapters.

    host_info: dict from environment YAML:
        {
            "name": "...",
            "type": "linux" | "docker-host" | ...
            "address": "10.0.0.5"
        }

    instance_config: dict from services.yaml:
        {
            "name": "...",
            "container": "container_name"
        }
    """

    htype = (host_info or {}).get("type")
    address = (host_info or {}).get("address")

    # ----------------------------------------------------------
    # Case 1: OS-level adapter (no instance_config)
    # ----------------------------------------------------------
    if instance_config is None:
        if htype == "docker-host":
            # The host itself is a Docker machine
            return DockerOSBackend()

        # fall back to SSH
        return SSHBackend(
            host=address,
            user=host_info.get("ssh_user"),
            key_path=host_info.get("ssh_key"),
        )

    # ----------------------------------------------------------
    # Case 2: Specific instance (Tomcat) — may run inside docker
    # ----------------------------------------------------------
    container_name = instance_config.get("container")

    if docker_enabled and container_name:
        return DockerBackend(container_name=container_name)

    # fallback to SSH
    return SSHBackend(
        host=address,
        user=host_info.get("ssh_user"),
        key_path=host_info.get("ssh_key"),
    )