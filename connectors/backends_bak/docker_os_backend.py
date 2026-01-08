# connectors/backends/docker_os_backend.py

import docker
import traceback

class DockerOSBackend:
    """
    Provides OS-level metrics for a Docker host.

    This backend executes commands on the *Docker host* by launching
    ephemeral containers (alpine) and running commands inside them.
    """
    def __init__(self, base_url=None):
        try:
            if base_url:
                self.client = docker.DockerClient(base_url=base_url)
            else:
                self.client = docker.from_env()

            self.client.ping()
            self.connected = True
        except Exception as e:
            self.client = None
            self.connected = False
            self.init_error = str(e)

    # ----------------------------------------------
    # Internal helper for running commands
    # ----------------------------------------------
    def _run(self, cmd):
        if not self.connected:
            return {
                "cmd": cmd,
                "stdout": "",
                "stderr": "",
                "error": f"Docker not connected: {self.init_error}",
            }

        try:
            output = self.client.containers.run(
                image="alpine:latest",
                command=["sh", "-c", cmd],
                remove=True,
                stdout=True,
                stderr=True,
            )
            text = output.decode("utf-8", errors="ignore")

            return {
                "cmd": cmd,
                "stdout": text,
                "stderr": "",
                "error": None,
            }

        except Exception as e:
            return {
                "cmd": cmd,
                "stdout": "",
                "stderr": "",
                "error": f"[docker-host error: {str(e)}]",
                "trace": traceback.format_exc(),
            }

    # ----------------------------------------------
    # Public OS-level API
    # ----------------------------------------------
    def get_cpu(self):
        return self._run("top -bn1 | head -5 || ps")

    def get_memory(self):
        return self._run("free -m || cat /proc/meminfo")

    def get_disk(self):
        return self._run("df -h")

    def get_kernel(self):
        return self._run("uname -a")