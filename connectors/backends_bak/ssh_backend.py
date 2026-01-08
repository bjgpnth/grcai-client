# connectors/backends/ssh_backend.py

import paramiko
import socket
import traceback


class SSHBackend:
    """
    SSH Backend
    - Key-based auth only (no passwords)
    - Executes commands remotely
    - Returns structured results safely
    """

    name = "ssh"

    def __init__(self, host: str, user: str, key_path: str, timeout: int = 10):
        self.host = host
        self.user = user
        self.key_path = key_path
        self.timeout = timeout
        self.client = None

    # ------------------------------------------------------------------
    # Connection Handling
    # ------------------------------------------------------------------
    def _connect(self):
        """Connect lazily — only once."""

        if self.client:
            return

        try:
            key = paramiko.RSAKey.from_private_key_file(self.key_path)

            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            self.client.connect(
                hostname=self.host,
                username=self.user,
                pkey=key,
                timeout=self.timeout,
                banner_timeout=self.timeout,
                auth_timeout=self.timeout,
            )

        except Exception as e:
            # Mark as failed but don't crash
            self.client = None
            raise RuntimeError(f"SSH connect failed for {self.user}@{self.host}: {e}")

    # ------------------------------------------------------------------
    # Command execution (low-level)
    # ------------------------------------------------------------------
    def exec_raw(self, cmd: str):
        """Execute a command on the remote host (may raise errors)."""

        if not self.client:
            self._connect()

        try:
            stdin, stdout, stderr = self.client.exec_command(cmd, timeout=self.timeout)
            out = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
            return {"stdout": out, "stderr": err}
        except Exception as e:
            raise RuntimeError(f"SSH exec failed: {e}")

    # ------------------------------------------------------------------
    # Safe Execution Wrapper (no exceptions)
    # ------------------------------------------------------------------
    def safe_exec(self, cmd: str):
        """
        Wrapper used by adapters.
        ALWAYS returns a dict:
            { "cmd": "...", "stdout": "...", "stderr": "...", "error": None or "..." }
        """

        try:
            result = self.exec_raw(cmd)
            result["cmd"] = cmd
            result["error"] = None
            return result

        except Exception as e:
            return {
                "cmd": cmd,
                "stdout": "",
                "stderr": "",
                "error": str(e)
            }

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def close(self):
        if self.client:
            try:
                self.client.close()
            except:
                pass