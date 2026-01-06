# connectors/host_connectors/docker_host_connector.py
"""
Final stable DockerHostConnector (macOS + Linux + Python 3.14):

 - No ThreadPoolExecutor
 - No multiprocessing
 - No pickling
 - No daemon threads
 - Timeout handled with POSIX signal.alarm
 - Docker calls run synchronously and safely
 - exec_in_container patched to avoid blocking (tty=False, stdin=False)
"""

from __future__ import annotations

import docker
import signal
from contextlib import contextmanager
from connectors.host_connectors.base_host_connector import BaseHostConnector
from typing import Any


DEFAULT_DOCKER_TIMEOUT = 5


# -------------------------------------------------
#                   TIMEOUT
# -------------------------------------------------
class TimeoutError(Exception):
    pass


@contextmanager
def timeout(seconds):
    """Timeout context manager using signal.alarm.

    Falls back to no-timeout if not in main thread (signals unavailable).
    """
    import threading

    is_main = threading.current_thread() is threading.main_thread()

    if not is_main:
        # No alarm possible -> run without timeout
        yield
        return

    def handler(signum, frame):
        raise TimeoutError()

    try:
        old = signal.signal(signal.SIGALRM, handler)
        signal.alarm(seconds)
        yield
    finally:
        signal.alarm(0)
        try:
            signal.signal(signal.SIGALRM, old)
        except Exception:
            pass


# -------------------------------------------------
#             DOCKER HOST CONNECTOR
# -------------------------------------------------
class DockerHostConnector(BaseHostConnector):
    def __init__(self, host_info, global_access=None, docker_timeout=None):
        super().__init__(host_info=host_info or {})
        self.global_access = global_access or {}
        self.timeout = docker_timeout or DEFAULT_DOCKER_TIMEOUT

        cfg = host_info.get("docker", {}) or {}
        acc = self.global_access.get("docker", {}) or {}

        base_url = cfg.get("base_url") or acc.get("base_url")
        use_local = cfg.get("use_local_socket")
        if use_local is None:
            use_local = acc.get("use_local_socket", True)

        # Try initialize docker client
        try:
            if base_url:
                self.client = docker.DockerClient(base_url=base_url)
            else:
                self.client = docker.from_env()
            self._last_err = None
        except Exception as e:
            self.client = None
            self._last_err = str(e)

        try:
            print(
                f"DEBUG DockerHostConnector.__init__: "
                f"host={host_info.get('name')}, client_present={bool(self.client)}, last_err={self._last_err}"
            )
        except Exception:
            pass

    # -------------------------------------------------
    #                SAFE DOCKER CALL
    # -------------------------------------------------
    def _safe(self, fn):
        if not self.client:
            raise RuntimeError("[docker not available]")

        try:
            with timeout(self.timeout):
                return fn()
        except TimeoutError:
            raise RuntimeError(f"[docker-host timeout after {self.timeout}s]")
        except Exception as e:
            raise RuntimeError(f"[docker-host error: {e}]")

    # -------------------------------------------------
    #               LIST CONTAINERS
    # -------------------------------------------------
    def list_containers(self, filters=None):
        if not self.client:
            return []
        try:
            return self._safe(lambda: self.client.containers.list(all=True, filters=filters or {}))
        except RuntimeError:
            return []

    # -------------------------------------------------
    #                     LOGS
    # -------------------------------------------------
    def get_container_logs(self, container_id, tail=5000, timeout_override=None):
        """
        Get container logs (both stdout and stderr).
        For symlinked logs (access.log -> /dev/stdout, error.log -> /dev/stderr),
        we need both streams to capture all logs.
        
        Args:
            container_id: Container ID or name
            tail: Number of lines to get. If None, gets all logs (use with caution for large log volumes)
            timeout_override: Optional timeout override in seconds (for large log volumes)
        """
        if not self.client:
            return "[docker not available]"

        def fn():
            c = self.client.containers.get(container_id)
            # IMPORTANT: Get both stdout and stderr to capture symlinked logs
            # access.log -> /dev/stdout and error.log -> /dev/stderr both appear in docker logs
            if tail is None:
                # Get all logs (no tail limit) - use for historical collection
                out = c.logs(stdout=True, stderr=True)
            else:
                out = c.logs(tail=tail, stdout=True, stderr=True)
            return out.decode("utf-8", errors="ignore")

        # Use longer timeout for getting all logs (can be very large)
        timeout_to_use = timeout_override if timeout_override is not None else (60 if tail is None else self.timeout)

        try:
            # Temporarily override timeout for this call
            original_timeout = self.timeout
            self.timeout = timeout_to_use
            try:
                return self._safe(fn)
            finally:
                self.timeout = original_timeout
        except RuntimeError as e:
            return f"[docker-host exec error: {e}]"

    def get_container_logs_time_filtered(self, container_id, since=None, until=None, tail_limit=50000):
        """Docker API time window log retrieval."""
        if not self.client:
            return "[docker not available]"

        def fn():
            from datetime import datetime

            c = self.client.containers.get(container_id)

            since_ts = int(since.timestamp()) if since and hasattr(since, "timestamp") else since
            until_ts = int(until.timestamp()) if until and hasattr(until, "timestamp") else until

            out = c.logs(
                since=since_ts,
                until=until_ts,
                timestamps=True,
                tail=tail_limit,
                stdout=True,
                stderr=True
            )
            if isinstance(out, bytes):
                return out.decode("utf-8", errors="ignore")
            return str(out or "")

        try:
            return self._safe(fn)
        except RuntimeError as e:
            return f"[docker-host logs error: {e}]"

    # -------------------------------------------------
    #          **PATCHED** EXEC IN CONTAINER
    # -------------------------------------------------
    def exec_in_container(self, container_id, cmd, timeout=None):
        """
        Non-blocking exec:
         - tty=False (critical)
         - stdin=False (critical)
         - stdout/stderr=True
        Prevents blocking on /dev/stdout or misbehaving commands.
        """
        if not self.client:
            return "[docker not available]"

        def fn():
            c = self.client.containers.get(container_id)

            try:
                # stable, safe execution (no tty!)
                res = c.exec_run(
                    cmd,
                    stdout=True,
                    stderr=True,
                    tty=False,      # IMPORTANT — prevents blocking
                    stdin=False     # IMPORTANT — prevents hanging
                )
            except TypeError:
                # SDK fallback (different signature)
                res = c.exec_run(cmd)

            # Extract output robustly
            if hasattr(res, "output"):
                out = res.output
            elif isinstance(res, tuple) and len(res) >= 2:
                out = res[1]
            else:
                out = res

            if isinstance(out, bytes):
                return out.decode("utf-8", errors="ignore")
            return str(out or "")

        try:
            return self._safe(fn)
        except RuntimeError as e:
            return f"[docker-host exec error: {e}]"

    # -------------------------------------------------
    #                READ FILE
    # -------------------------------------------------
    def read_file_in_container(self, container_id, path):
        return self.exec_in_container(container_id, f"cat {path}")

    # -------------------------------------------------
    #                  CLOSE CLIENT
    # -------------------------------------------------
    def close(self):
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass