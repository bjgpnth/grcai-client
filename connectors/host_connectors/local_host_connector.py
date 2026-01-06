# connectors/host_connectors/local_host_connector.py

import subprocess
import docker
from connectors.host_connectors.base_host_connector import BaseHostConnector


class LocalHostConnector(BaseHostConnector):
    """
    Local-only implementation.

    Tests use: LocalHostConnector(host_cfg={...})
    Runtime uses: LocalHostConnector(host_info={...})
    Both are supported.
    """

    def __init__(self, host_info=None, host_cfg=None):
        # Allow both naming conventions
        resolved = host_info or host_cfg or {}
        super().__init__(host_info=resolved)

        # Initialize docker client if possible
        try:
            self.docker = docker.from_env()
        except Exception:
            self.docker = None

    # ------------------------------------------------------------
    # OS COMMANDS
    # ------------------------------------------------------------
    def exec_cmd(self, cmd: str):
        """
        Tests expect return format:
        {
            "stdout": "...",
            "stderr": "",
            "ok": True
        }
        """

        try:
            out = subprocess.check_output(
                cmd,
                shell=True,
                stderr=subprocess.STDOUT
            )
            return {
                "stdout": out.decode("utf-8", errors="ignore"),
                "stderr": "",
                "ok": True
            }
        except subprocess.CalledProcessError as e:
            return {
                "stdout": e.output.decode("utf-8", errors="ignore"),
                "stderr": "",
                "ok": False
            }
        except Exception as e:
            return {
                "stdout": "",
                "stderr": str(e),
                "ok": False
            }

    # ------------------------------------------------------------
    # DOCKER COMMANDS
    # ------------------------------------------------------------
    def list_containers(self, filters=None):
        """
        Tests expect:
        {
            "ok": True,
            "containers": [...]
        }
        """
        if not self.docker:
            return {
                "ok": True,
                "containers": []
            }

        try:
            containers = self.docker.containers.list(
                all=True,
                filters=filters or {}
            )
            return {
                "ok": True,
                "containers": containers
            }
        except Exception as e:
            return {
                "ok": False,
                "containers": [],
                "error": str(e)
            }

    def get_container_logs(self, container_id, tail=5000):
        if not self.docker:
            return {"ok": False, "stdout": "", "stderr": "docker unavailable"}

        try:
            c = self.docker.containers.get(container_id)
            logs = c.logs(tail=tail).decode("utf-8", errors="ignore")
            return {"ok": True, "stdout": logs, "stderr": ""}
        except Exception as e:
            return {"ok": False, "stdout": "", "stderr": str(e)}

    def exec_in_container(self, container_id, cmd: str):
        if not self.docker:
            return {"ok": False, "stdout": "", "stderr": "docker unavailable"}

        try:
            c = self.docker.containers.get(container_id)
            exit_code, output = c.exec_run(cmd)
            return {
                "ok": exit_code == 0,
                "stdout": output.decode("utf-8", errors="ignore"),
                "stderr": ""
            }
        except Exception as e:
            return {"ok": False, "stdout": "", "stderr": str(e)}

    def read_file(self, path: str):
        try:
            with open(path, "r", errors="ignore") as f:
                return {"ok": True, "stdout": f.read(), "stderr": ""}
        except Exception as e:
            return {"ok": False, "stdout": "", "stderr": str(e)}