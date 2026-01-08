# connectors/backends/docker_backend.py

import docker

class DockerBackend:
    """
    Backend for executing commands and reading files inside a Docker container.
    """

    def __init__(self, container_name, host=None):
        """
        container_name: name or id of the container
        host: optional (not used yet)
        """
        self.host = host

        try:
            self.client = docker.from_env()
        except Exception as e:
            raise RuntimeError(f"Docker client initialization failed: {e}")

        try:
            # This is the critical line you are missing
            self.container = self.client.containers.get(container_name)
        except Exception as e:
            raise RuntimeError(f"Failed to load Docker container '{container_name}': {e}")

    # ----------------------------------------------------------------------
    # EXEC inside container
    # ----------------------------------------------------------------------
    def safe_exec(self, cmd: str, timeout: int = 10) -> str:
        """
        Execute shell inside the container.
        """
        try:
            full = f"sh -c \"{cmd}\""
            res = self.container.exec_run(full, stdout=True, stderr=True)
            out = res.output if hasattr(res, "output") else res[1]

            if isinstance(out, bytes):
                return out.decode("utf-8", errors="ignore")

            return str(out)
        except Exception as e:
            return f"[docker exec error: {e}]"

    # ----------------------------------------------------------------------
    # FILE READ
    # ----------------------------------------------------------------------
    def safe_read_file(self, path: str, max_bytes=150000) -> str:
        """
        Read a file from inside the container using `cat`.
        """
        try:
            cmd = f"cat {path} 2>/dev/null"
            full = f"sh -c \"{cmd}\""
            res = self.container.exec_run(full, stdout=True, stderr=True)
            out = res.output if hasattr(res, "output") else res[1]

            if isinstance(out, bytes):
                text = out.decode("utf-8", errors="ignore")
            else:
                text = str(out)

            if len(text) > max_bytes:
                return text[:max_bytes] + "\n...[truncated]..."

            return text
        except Exception as e:
            return f"[docker read error: {e}]"