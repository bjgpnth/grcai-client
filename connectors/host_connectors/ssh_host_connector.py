# connectors/host_connectors/ssh_host_connector.py
"""
SSH-backed HostConnector.

Behavior:
 - Prefer paramiko if available (HAS_PARAMIKO flag).
 - All exec/read operations return structured dict:
     {"cmd": str, "stdout": str, "stderr": str, "rc": int, "error": Optional[str]}
 - list_containers() returns {"ok": bool, "containers": [dict,...]} (test expectations)
"""

from __future__ import annotations

import io
import os
import json
import shlex
import subprocess
from typing import Optional

# paramiko may or may not be present; tests monkeypatch this module path when needed.
try:
    import paramiko  # type: ignore
    from paramiko.ssh_exception import SSHException, AuthenticationException  # type: ignore
    HAS_PARAMIKO = True
except Exception:
    paramiko = None  # type: ignore
    SSHException = Exception  # fallback
    AuthenticationException = Exception  # fallback
    HAS_PARAMIKO = False

from connectors.host_connectors.base_host_connector import BaseHostConnector


DEFAULT_TIMEOUT = 5


def _mk_result(cmd: str, stdout: str = "", stderr: str = "", rc: int = 0, error: Optional[str] = None) -> dict:
    return {"cmd": cmd, "stdout": stdout or "", "stderr": stderr or "", "rc": int(rc or 0), "error": error}


class SSHHostConnector(BaseHostConnector):
    def __init__(self, host_info: dict, global_access: dict | None = None):
        resolved = host_info or {}
        super().__init__(host_info=resolved)

        self.global_access = global_access or {}

        ssh_cfg = {}
        ssh_cfg.update(self.global_access.get("ssh", {}) or {})
        # Check host_info.access.ssh (YAML format) before host_info.ssh (direct format)
        ssh_cfg.update(resolved.get("access", {}).get("ssh", {}) or {})
        ssh_cfg.update(resolved.get("ssh", {}) or {})

        # normalize keys (tests provide 'user' and 'key_path')
        self.address = ssh_cfg.get("host") or resolved.get("address") or ssh_cfg.get("hostname") or "127.0.0.1"
        self.port = int(ssh_cfg.get("port", 22) or 22)
        self.username = ssh_cfg.get("username") or ssh_cfg.get("user") or os.environ.get("USER", "unknown")
        self.auth_method = ssh_cfg.get("auth_method") or ssh_cfg.get("method") or "key"
        self.key_path = os.path.expanduser(ssh_cfg.get("key_path") or ssh_cfg.get("key") or "~/.ssh/id_rsa")
        self.password = ssh_cfg.get("password")
        self.timeout = int(ssh_cfg.get("timeout", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
        self.allow_agent = bool(ssh_cfg.get("allow_agent") or ssh_cfg.get("use_agent", False))

        # underlying client objects
        self._client = None
        self._sftp = None
        self.use_paramiko = HAS_PARAMIKO
        self._connection_error = None  # Store connection errors for reporting

        # attempt to connect; tests may monkeypatch _connect to a no-op
        try:
            self._connect()
        except Exception as e:
            # Store connection error instead of silently swallowing
            # This allows us to report connection failures properly
            self._connection_error = str(e)
            self._client = None
            self._sftp = None
            # For remote hosts (VM), we should report connection failures
            # For local hosts or test scenarios, connection might not be required

    # -----------------------
    # Internal connection
    # -----------------------
    def _connect(self):
        """
        Establish a connection using paramiko if available.
        Raises RuntimeError on failure.
        """
        if self.use_paramiko and paramiko:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                # prefer key auth if requested
                if self.auth_method and str(self.auth_method).lower() in ("key", "pubkey", "ssh-key"):
                    p = os.path.expanduser(self.key_path) if self.key_path else None
                    try:
                        if p and os.path.exists(p):
                            client.connect(
                                hostname=self.address,
                                port=self.port,
                                username=self.username,
                                key_filename=p,
                                timeout=self.timeout,
                                banner_timeout=self.timeout,
                                auth_timeout=self.timeout,
                                allow_agent=self.allow_agent,
                                look_for_keys=False,
                            )
                        else:
                            client.connect(
                                hostname=self.address,
                                port=self.port,
                                username=self.username,
                                timeout=self.timeout,
                                banner_timeout=self.timeout,
                                auth_timeout=self.timeout,
                                allow_agent=self.allow_agent,
                                look_for_keys=False,
                            )
                    except AuthenticationException:
                        if self.password:
                            client.connect(
                                hostname=self.address,
                                port=self.port,
                                username=self.username,
                                password=self.password,
                                timeout=self.timeout,
                                banner_timeout=self.timeout,
                                auth_timeout=self.timeout,
                                allow_agent=self.allow_agent,
                                look_for_keys=False,
                            )
                        else:
                            raise
                else:
                    # password-based or default
                    if self.password:
                        client.connect(
                            hostname=self.address,
                            port=self.port,
                            username=self.username,
                            password=self.password,
                            timeout=self.timeout,
                            banner_timeout=self.timeout,
                            auth_timeout=self.timeout,
                            allow_agent=self.allow_agent,
                            look_for_keys=False,
                        )
                    else:
                        client.connect(
                            hostname=self.address,
                            port=self.port,
                            username=self.username,
                            timeout=self.timeout,
                            banner_timeout=self.timeout,
                            auth_timeout=self.timeout,
                            allow_agent=self.allow_agent,
                            look_for_keys=False,
                        )

                self._client = client
                try:
                    self._sftp = client.open_sftp()
                except Exception:
                    self._sftp = None
                return
            except Exception as e:
                raise RuntimeError(f"SSH connect failed for {self.address}: {e}")
        else:
            # No paramiko: nothing to "connect" for subprocess mode.
            self._client = None
            self._sftp = None
            self.use_paramiko = False

    # -----------------------
    # exec_cmd: structured result expected by orchestrator/tests
    # -----------------------
    def exec_cmd(self, cmd: str, timeout: Optional[int] = None) -> dict:
        """
        Execute a command and return a dict:
        {"cmd", "stdout", "stderr", "rc", "error"}.
        Uses paramiko when connected; otherwise falls back to local subprocess (tests monkeypatch subprocess).
        """
        timeout = timeout or self.timeout
        cmd_str = str(cmd)

        # Check if this is a remote host (VM) and connection failed
        # For remote hosts, we should NOT fall back to local execution
        is_remote_host = self.host_type == "vm"
        
        if is_remote_host and not self._client:
            # Remote host but connection failed - report error instead of falling back to local
            error_msg = "SSH connection not established"
            if self._connection_error:
                error_msg = f"SSH connection failed: {self._connection_error}"
            return _mk_result(
                cmd_str,
                stdout="",
                stderr="",
                rc=1,
                error=error_msg
            )

        # If paramiko client is present, use it
        if self._client and self.use_paramiko:
            try:
                stdin, stdout_f, stderr_f = self._client.exec_command(cmd_str, timeout=timeout)
                try:
                    out_bytes = stdout_f.read()
                except Exception:
                    out_bytes = b""
                try:
                    err_bytes = stderr_f.read()
                except Exception:
                    err_bytes = b""
                stdout = out_bytes.decode("utf-8", errors="ignore") if isinstance(out_bytes, (bytes, bytearray)) else str(out_bytes or "")
                stderr = err_bytes.decode("utf-8", errors="ignore") if isinstance(err_bytes, (bytes, bytearray)) else str(err_bytes or "")
                try:
                    rc = stdout_f.channel.recv_exit_status()
                except Exception:
                    rc = 0
                return _mk_result(cmd_str, stdout=stdout, stderr=stderr, rc=rc, error=None)
            except Exception as e:
                return _mk_result(cmd_str, stdout="", stderr="", rc=1, error=str(e))

        # Otherwise, use subprocess (local execution). 
        # Only allow this for local hosts or when paramiko is not available and it's not a remote VM.
        # Tests monkeypatch subprocess.Popen for coverage.
        try:
            proc = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)
            out_b, err_b = proc.communicate(timeout=timeout)
            stdout = out_b.decode("utf-8", errors="ignore") if isinstance(out_b, (bytes, bytearray)) else str(out_b or "")
            stderr = err_b.decode("utf-8", errors="ignore") if isinstance(err_b, (bytes, bytearray)) else str(err_b or "")
            rc = proc.returncode if proc.returncode is not None else 0
            return _mk_result(cmd_str, stdout=stdout, stderr=stderr, rc=rc, error=None)
        except subprocess.TimeoutExpired:
            return _mk_result(cmd_str, stdout="", stderr="", rc=124, error=f"timeout after {timeout}s")
        except Exception as e:
            return _mk_result(cmd_str, stdout="", stderr="", rc=1, error=str(e))

    # -----------------------
    # read_file: prefer SFTP; fallback to remote cat via exec_cmd
    # returns structured dict (same shape as exec_cmd)
    # -----------------------
    def read_file(self, path: str, timeout: Optional[int] = None) -> dict:
        timeout = timeout or self.timeout
        cmd = f"cat {shlex.quote(path)}"
        
        # Check if this is a remote host (VM) and connection failed
        is_remote_host = self.host_type == "vm"
        if is_remote_host and not self._client:
            # Remote host but connection failed - report error
            error_msg = "SSH connection not established"
            if self._connection_error:
                error_msg = f"SSH connection failed: {self._connection_error}"
            return _mk_result(
                cmd,
                stdout="",
                stderr="",
                rc=1,
                error=error_msg
            )
        
        # Try SFTP read
        if self._sftp:
            try:
                with self._sftp.open(path, "r") as fh:
                    data = fh.read()
                    return _mk_result(cmd, stdout=str(data or ""), stderr="", rc=0, error=None)
            except Exception:
                pass

        # fallback
        return self.exec_cmd(cmd, timeout=timeout)

    # -----------------------
    # file_exists: try sftp.stat else `test -f` via exec_cmd
    # -----------------------
    def file_exists(self, path: str, timeout: Optional[int] = None) -> bool:
        timeout = timeout or self.timeout
        if self._sftp:
            try:
                self._sftp.stat(path)
                return True
            except Exception:
                return False
        res = self.exec_cmd(f"test -f {shlex.quote(path)} && echo yes || echo no", timeout=timeout)
        return "yes" in (res.get("stdout") or "")

    # -----------------------
    # list_containers: used by tests expecting {'ok': True, 'containers': [...]}
    # -----------------------
    def list_containers(self, filters: dict | None = None) -> dict:
        """
        Run `docker ps --format '{{json .}}'` on the remote host and parse JSON lines.
        Returns {"ok": bool, "containers": [dict,...]}.
        Normalize keys to lowercase for consistency (ID -> id, Names -> names).
        """
        try:
            res = self.exec_cmd("docker ps --format '{{json .}}'")
            if res.get("rc", 1) != 0:
                error_msg = res.get("error", "Command failed")
                return {"ok": False, "containers": [], "error": error_msg}
            lines = (res.get("stdout") or "").strip().splitlines()
            containers = []
            for line in lines:
                try:
                    j = json.loads(line)
                    # normalize keys to lowercase
                    normalized = {}
                    for k, v in j.items():
                        normalized[k.lower()] = v
                    # also coerce 'names' into singular string (some engines return multiple names comma-separated)
                    if "names" in normalized and isinstance(normalized["names"], list):
                        normalized["names"] = ",".join(normalized["names"])
                    containers.append(normalized)
                except Exception:
                    continue
            return {"ok": True, "containers": containers}
        except Exception as e:
            return {"ok": False, "containers": [], "error": str(e)}

    # -----------------------
    # close
    # -----------------------
    def close(self) -> None:
        try:
            if self._sftp:
                try:
                    self._sftp.close()
                except Exception:
                    pass
                self._sftp = None
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    pass
                self._client = None
        except Exception:
            pass