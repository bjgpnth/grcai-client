# connectors/host_connectors/local_host_connector.py

import subprocess
import docker
from datetime import datetime, timezone
from typing import Optional
from connectors.host_connectors.base_host_connector import BaseHostConnector

try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None


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
    
    # ------------------------------------------------------------
    # TIMEZONE QUERY (Phase 2)
    # ------------------------------------------------------------
    def _query_timezone_runtime(self) -> Optional[str]:
        """
        Query local system timezone.
        
        Tries multiple methods:
        1. timedatectl (most reliable on Linux)
        2. /etc/timezone (Debian/Ubuntu)
        3. System timezone via datetime (fallback)
        """
        # Method 1: timedatectl (Linux)
        try:
            result = self.exec_cmd("timedatectl | grep 'Time zone' | awk '{print $3}'")
            if result.get("ok") and result.get("stdout"):
                tz_str = result["stdout"].strip()
                if tz_str:
                    return tz_str
        except Exception:
            pass
        
        # Method 2: /etc/timezone (Debian/Ubuntu)
        try:
            result = self.read_file("/etc/timezone")
            if result.get("ok") and result.get("stdout"):
                tz_str = result["stdout"].strip()
                if tz_str:
                    return tz_str
        except Exception:
            pass
        
        # Method 3: System timezone via datetime (fallback)
        try:
            if ZoneInfo:
                # Get local timezone
                local_tz = datetime.now(timezone.utc).astimezone().tzinfo
                if hasattr(local_tz, 'key'):
                    return local_tz.key
                # Try to get from time.tzname
                import time
                tz_name = time.tzname[0]
                # Map common abbreviations to IANA
                tz_map = {
                    "IST": "Asia/Kolkata",
                    "EST": "America/New_York",
                    "EDT": "America/New_York",
                    "PST": "America/Los_Angeles",
                    "PDT": "America/Los_Angeles",
                    "UTC": "UTC",
                    "GMT": "Europe/London",
                }
                return tz_map.get(tz_name)
        except Exception:
            pass
        
        return None