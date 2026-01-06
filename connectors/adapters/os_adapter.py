# connectors/adapters/os_adapter.py
from connectors.base_connector import BaseConnector
from connectors.historical import HistoricalLogCollectionMixin
import traceback
import logging

logger = logging.getLogger("grcai.adapters.os")


class OSAdapter(BaseConnector, HistoricalLogCollectionMixin):
    """
    Collects OS-level metrics on ALL hosts in the environment.

    Host list comes from env_config["hosts"].
    """

    def __init__(self, name=None, env_config=None, issue_time=None, component_config=None, **kwargs):
        """
        Support two modes:

        1. Real orchestrator:
            OSAdapter(name, env_config, issue_time, component_config)

        2. Test mode:
            OSAdapter(issue_time=..., component_config={})
        """

        # --- TEST MODE (pytest + sanity suite) -------------------------
        # Tests never pass 'name' or 'env_config'
        if name is None and env_config is None:
            name = "os"
            env_config = {"hosts": []}       # minimal safe default

        super().__init__(
            name=name,
            issue_time=issue_time,
            component_config=component_config,
        )

        self.env_config = env_config or {}

        # IMPORTANT: load ALL hosts
        self.hosts = self.env_config.get("hosts", [])

    # ----------------------
    # Helpers
    # ----------------------
    def _normalize_exec_result(self, out):
        """
        Normalize return value of host_connector.exec_cmd into a dict:
          { "stdout": str, "stderr": str, "error": optional, "rc": optional }
        Accepts:
          - string (stdout)
          - dict with keys stdout/stderr/error/rc
          - nested dicts coming from docker/ssh wrappers
        """
        if out is None:
            return {"stdout": "", "stderr": "", "error": None, "rc": None}

        # If host connector returned a dict-like object
        if isinstance(out, dict):
            # many connectors use {"stdout": "...", "stderr": "", "ok": True}
            stdout = out.get("stdout", "")
            stderr = out.get("stderr", "")

            # some implementations nest stdout as a dict: {"stdout": {"stdout": "...", "stderr": ""}, ...}
            if isinstance(stdout, dict):
                inner = stdout
                stdout = inner.get("stdout", "") or ""
                stderr = inner.get("stderr", "") or stderr

            # error/rc fields
            err = out.get("error", None)
            rc = out.get("rc", None)
            # some use 'ok' boolean; map to rc if helpful
            if rc is None and "ok" in out:
                rc = 0 if out.get("ok") else 1

            # finally, ensure strings
            stdout = stdout if stdout is not None else ""
            stderr = stderr if stderr is not None else ""
            return {"stdout": str(stdout), "stderr": str(stderr), "error": err, "rc": rc}

        # If it's already a string (most simple LocalHostConnector returns string)
        if isinstance(out, str):
            return {"stdout": out, "stderr": "", "error": None, "rc": 0}

        # Fallback: coerce to string
        try:
            s = str(out)
        except Exception:
            s = ""
        return {"stdout": s, "stderr": "", "error": None, "rc": None}

    def _first_meaningful(self, text, max_lines=5):
        """Return first non-empty line(s) of text (up to max_lines) or empty string."""
        if not text:
            return ""
        lines = [line for line in str(text).splitlines() if line.strip()]
        if not lines:
            # if there are blank lines but no meaningful lines, return first raw line
            raw_lines = str(text).splitlines()
            return raw_lines[0] if raw_lines else ""
        return "\n".join(lines[:max_lines])

    # ----------------------------------------------------------------------
    def collect_for_host(self, host_info, host_connector):
        """
        Cross-platform OS metrics collector that works on:
        - BusyBox containers
        - Alpine Linux
        - Debian/Ubuntu
        - RHEL/CentOS
        - Real Linux servers
        - Docker ephemeral exec shells
        
        Returns:
            dict with keys:
            - name: host name
            - metrics: dict with cpu, memory, disk, kernel data
            - logs: dict with syslog, auth_log data
            - commands: list of raw command outputs (for debugging)
            - errors: list of structured error entries
        """
        from datetime import datetime, timezone

        host_name = host_info.get("name") or host_info.get("address") or "unknown"
        data = {
            "name": host_name,
            "metrics": {},
            "logs": {},
            "commands": [],
            "errors": []
        }

        def run_and_normalize(cmd):
            """Execute cmd using host_connector and normalize result."""
            cmd_result = {"cmd": cmd, "stdout": "", "stderr": "", "rc": None}
            try:
                out_raw = host_connector.exec_cmd(cmd)
                normalized = self._normalize_exec_result(out_raw)
                cmd_result.update(normalized)
                # Store raw command output for debugging
                data["commands"].append(cmd_result.copy())
                
                # If there's an error, convert to structured error format
                if normalized.get("error"):
                    error_msg = normalized["error"] or "Command execution failed"
                    data["errors"].append({
                        "type": "collection_error",
                        "component": "os",
                        "stage": "command_execution",
                        "message": error_msg,
                        "detail": f"Command: {cmd}, RC: {normalized.get('rc')}",
                        "code": f"ECMD_{normalized.get('rc', 'UNKNOWN')}" if normalized.get("rc") else None
                    })
            except Exception as e:
                cmd_result.update({"stdout": "", "stderr": "", "error": str(e), "rc": None})
                data["commands"].append(cmd_result.copy())
                error_msg = str(e) or type(e).__name__ or "Unknown error"
                data["errors"].append({
                    "type": "collection_error",
                    "component": "os",
                    "stage": "command_execution",
                    "message": error_msg,
                    "detail": f"Command: {cmd}",
                    "traceback": traceback.format_exc()
                })
            
            return cmd_result

        # ---------------------------------------------------------
        # CPU — try multiple fallbacks
        # ---------------------------------------------------------
        cpu_cmds = [
            "cat /proc/stat",       # universal on Linux
            "top -b -n1",           # GNU top (batch)
            "top -n1",              # BusyBox / alternative
            "top",                  # minimal top
            "ps aux",               # fallback
        ]

        cpu_result = None
        for cmd in cpu_cmds:
            r = run_and_normalize(cmd)
            if r.get("stdout"):
                cpu_result = {
                    "cmd": cmd,
                    "stdout": self._first_meaningful(r.get("stdout"), max_lines=5),
                    "stderr": r.get("stderr", ""),
                }
                break

        if cpu_result is None:
            cpu_result = {"cmd": cpu_cmds[0], "stdout": "", "stderr": ""}
            data["errors"].append({
                "type": "collection_error",
                "component": "os",
                "stage": "cpu_metrics",
                "message": "CPU metrics unavailable",
                "detail": "All CPU command attempts failed"
            })

        data["metrics"]["cpu"] = cpu_result

        # ---------------------------------------------------------
        # Memory — /proc/meminfo first, then free
        # ---------------------------------------------------------
        mem_cmds = [
            "cat /proc/meminfo",
            "free -m",
            "free",
        ]

        mem_result = None
        for cmd in mem_cmds:
            r = run_and_normalize(cmd)
            if r.get("stdout"):
                mem_result = {
                    "cmd": cmd,
                    "stdout": self._first_meaningful(r.get("stdout"), max_lines=10),
                    "stderr": r.get("stderr", ""),
                }
                break

        if mem_result is None:
            mem_result = {"cmd": mem_cmds[0], "stdout": "", "stderr": ""}
            data["errors"].append({
                "type": "collection_error",
                "component": "os",
                "stage": "memory_metrics",
                "message": "Memory metrics unavailable",
                "detail": "All memory command attempts failed"
            })

        data["metrics"]["memory"] = mem_result

        # ---------------------------------------------------------
        # Disk — df -h is generally available
        # ---------------------------------------------------------
        try:
            r = run_and_normalize("df -h")
            data["metrics"]["disk"] = {
                "cmd": "df -h",
                "stdout": r.get("stdout", ""),
                "stderr": r.get("stderr", ""),
            }
        except Exception as e:
            data["metrics"]["disk"] = {"cmd": "df -h", "stdout": "", "stderr": ""}
            data["errors"].append({
                "type": "collection_error",
                "component": "os",
                "stage": "disk_metrics",
                "message": str(e),
                "detail": "df -h command failed",
                "traceback": traceback.format_exc()
            })

        # ---------------------------------------------------------
        # Kernel — uname -a
        # ---------------------------------------------------------
        try:
            r = run_and_normalize("uname -a")
            data["metrics"]["kernel"] = {
                "cmd": "uname -a",
                "stdout": r.get("stdout", ""),
                "stderr": r.get("stderr", ""),
            }
        except Exception as e:
            data["metrics"]["kernel"] = {"cmd": "uname -a", "stdout": "", "stderr": ""}
            data["errors"].append({
                "type": "collection_error",
                "component": "os",
                "stage": "kernel_info",
                "message": str(e),
                "detail": "uname -a command failed",
                "traceback": traceback.format_exc()
            })

        # ---------------------------------------------------------
        # OS Logs (syslog/messages, auth.log, journalctl) - historical or current
        # ---------------------------------------------------------
        try:
            if self._is_historical_collection():
                # Historical collection
                since, until = self._get_time_window()
                
                # Try journalctl first (if available) - it has native time filtering
                # Format: journalctl --since "2025-11-23 10:00:00" --until "2025-11-23 11:00:00"
                since_str = since.strftime("%Y-%m-%d %H:%M:%S")
                until_str = until.strftime("%Y-%m-%d %H:%M:%S")
                journalctl_cmd = f"journalctl --since '{since_str}' --until '{until_str}' --no-pager 2>/dev/null || true"
                journalctl_out = run_and_normalize(journalctl_cmd)
                
                # Check if journalctl worked: use return code (rc == 0) and non-empty output
                # This is more reliable than checking for error strings, as journalctl may print warnings
                # but still return successfully
                if journalctl_out.get("rc") == 0 and journalctl_out.get("stdout", "").strip():
                    journalctl_stdout = journalctl_out.get("stdout", "")
                    # journalctl worked and returned logs
                    data["logs"]["syslog"] = {
                        "content": journalctl_stdout[:50000],  # Limit to 50KB
                        "type": "historical",
                        "source": "journalctl",
                        "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                        "collection_mode": "historical",
                        "line_count": len(journalctl_stdout.splitlines())
                    }
                else:
                    # Fall back to file-based syslog/messages collection
                    try:
                        # Try syslog first (Debian/Ubuntu), then messages (RHEL/CentOS)
                        # Order: Debian/Ubuntu syslog, then RHEL/CentOS messages
                        syslog_paths = ["/var/log/syslog", "/var/log/messages"]
                        syslog_content = self._collect_historical_logs(
                            host_connector=host_connector,
                            container_id=None,  # OS logs are on the host
                            log_paths=syslog_paths,
                            parser_name="syslog",
                            since=since,
                            until=until,
                            max_lines=10000,
                            parser_context={"year": self.issue_time.year if self.issue_time else None}
                        )
                        data["logs"]["syslog"] = {
                            "content": syslog_content.strip(),
                            "type": "historical",
                            "source": "file",
                            "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                            "collection_mode": "historical",
                            "line_count": len(syslog_content.splitlines()) if syslog_content else 0
                        }
                    except Exception as e:
                        logger.debug("Failed to collect syslog historically: %s", e)
                        data["logs"]["syslog"] = {
                            "content": "",
                            "type": "historical",
                            "source": "file",
                            "collection_mode": "historical"
                        }
                        data["errors"].append({
                            "type": "collection_error",
                            "component": "os",
                            "stage": "syslog_collection",
                            "message": f"Failed to collect syslog historically: {e}",
                            "detail": str(e)
                        })
                
                # Collect auth.log historically
                # Try /var/log/auth.log (Debian/Ubuntu) first, then /var/log/secure (RHEL/CentOS)
                try:
                    auth_log_content = self._collect_historical_logs(
                        host_connector=host_connector,
                        container_id=None,
                        log_paths=["/var/log/auth.log", "/var/log/secure"],
                        parser_name="auth",
                        since=since,
                        until=until,
                        max_lines=5000,
                        parser_context={"year": self.issue_time.year if self.issue_time else None}
                    )
                    data["logs"]["auth_log"] = {
                        "content": auth_log_content.strip(),
                        "type": "historical",
                        "source": "file",
                        "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                        "collection_mode": "historical",
                        "line_count": len(auth_log_content.splitlines()) if auth_log_content else 0
                    }
                except Exception as e:
                    logger.debug("Failed to collect auth.log historically: %s", e)
                    data["logs"]["auth_log"] = {
                        "content": "",
                        "type": "historical",
                        "source": "file",
                        "collection_mode": "historical"
                    }
                    data["errors"].append({
                        "type": "collection_error",
                        "component": "os",
                        "stage": "auth_log_collection",
                        "message": f"Failed to collect auth.log historically: {e}",
                        "detail": str(e)
                    })
                
                # Collect kernel logs historically
                # Try journalctl first (if available) for kernel logs with time filtering
                try:
                    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
                    until_str = until.strftime("%Y-%m-%d %H:%M:%S")
                    kernel_journalctl_cmd = f"journalctl -k --since '{since_str}' --until '{until_str}' --no-pager 2>/dev/null || true"
                    kernel_journalctl_out = run_and_normalize(kernel_journalctl_cmd)
                    
                    # Check if journalctl worked
                    if kernel_journalctl_out.get("rc") == 0 and kernel_journalctl_out.get("stdout", "").strip():
                        kernel_content = kernel_journalctl_out.get("stdout", "")[:50000]
                        data["logs"]["kernel"] = {
                            "content": kernel_content,
                            "type": "historical",
                            "source": "journalctl",
                            "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                            "collection_mode": "historical",
                            "line_count": len(kernel_content.splitlines()) if kernel_content else 0
                        }
                    else:
                        # Fall back to file-based kernel log collection
                        kernel_log_paths = ["/var/log/kern.log"]
                        kernel_content = self._collect_historical_logs(
                            host_connector=host_connector,
                            container_id=None,
                            log_paths=kernel_log_paths,
                            parser_name="syslog",  # Use syslog parser for kernel logs
                            since=since,
                            until=until,
                            max_lines=5000,
                            parser_context={"year": self.issue_time.year if self.issue_time else None}
                        )
                        data["logs"]["kernel"] = {
                            "content": kernel_content.strip(),
                            "type": "historical",
                            "source": "file",
                            "time_window": {"since": since.isoformat(), "until": until.isoformat()},
                            "collection_mode": "historical",
                            "line_count": len(kernel_content.splitlines()) if kernel_content else 0
                        }
                except Exception as e:
                    logger.debug("Failed to collect kernel logs historically: %s", e)
                    data["logs"]["kernel"] = {
                        "content": "",
                        "type": "historical",
                        "source": "file",
                        "collection_mode": "historical"
                    }
                    data["errors"].append({
                        "type": "collection_error",
                        "component": "os",
                        "stage": "kernel_log_collection",
                        "message": f"Failed to collect kernel logs historically: {e}",
                        "detail": str(e)
                    })
            else:
                # Current collection - tail recent logs
                # Try syslog sources individually to detect which one worked
                syslog_content = ""
                syslog_source = "unknown"
                # Try /var/log/syslog first (Debian/Ubuntu)
                try:
                    syslog_out = run_and_normalize("tail -n 200 /var/log/syslog 2>/dev/null")
                    if syslog_out.get("stdout", "").strip():
                        syslog_content = syslog_out.get("stdout", "")[:20000]
                        syslog_source = "file"
                except Exception:
                    pass
                
                # If syslog didn't work, try /var/log/messages (RHEL/CentOS)
                if not syslog_content:
                    try:
                        messages_out = run_and_normalize("tail -n 200 /var/log/messages 2>/dev/null")
                        if messages_out.get("stdout", "").strip():
                            syslog_content = messages_out.get("stdout", "")[:20000]
                            syslog_source = "file"
                    except Exception:
                        pass
                
                # If file-based didn't work, try journalctl
                if not syslog_content:
                    try:
                        journalctl_out = run_and_normalize("journalctl -n 200 --no-pager 2>/dev/null")
                        if journalctl_out.get("stdout", "").strip():
                            syslog_content = journalctl_out.get("stdout", "")[:20000]
                            syslog_source = "journalctl"
                    except Exception:
                        pass
                
                data["logs"]["syslog"] = {
                    "content": syslog_content,
                    "type": "current",
                    "collection_mode": "current",
                    "source": syslog_source,
                    "line_count": len(syslog_content.splitlines()) if syslog_content else 0
                }
                
                # Try auth.log sources individually
                auth_log_content = ""
                auth_log_source = "file"
                # Try /var/log/auth.log first (Debian/Ubuntu)
                try:
                    auth_out = run_and_normalize("tail -n 200 /var/log/auth.log 2>/dev/null")
                    if auth_out.get("stdout", "").strip():
                        auth_log_content = auth_out.get("stdout", "")[:20000]
                        auth_log_source = "file"
                except Exception:
                    pass
                
                # If auth.log didn't work, try /var/log/secure (RHEL/CentOS)
                if not auth_log_content:
                    try:
                        secure_out = run_and_normalize("tail -n 200 /var/log/secure 2>/dev/null")
                        if secure_out.get("stdout", "").strip():
                            auth_log_content = secure_out.get("stdout", "")[:20000]
                            auth_log_source = "file"
                    except Exception:
                        pass
                
                data["logs"]["auth_log"] = {
                    "content": auth_log_content,
                    "type": "current",
                    "collection_mode": "current",
                    "source": auth_log_source,
                    "line_count": len(auth_log_content.splitlines()) if auth_log_content else 0
                }
                
                # Collect kernel logs - try sources individually
                kernel_content = ""
                kernel_source = "unknown"
                try:
                    # Try dmesg first
                    try:
                        dmesg_out = run_and_normalize("dmesg -T 2>/dev/null | tail -n 200")
                        if dmesg_out.get("stdout", "").strip():
                            kernel_content = dmesg_out.get("stdout", "")[:20000]
                            kernel_source = "dmesg"
                    except Exception:
                        pass
                    
                    # If dmesg didn't work, try /var/log/kern.log
                    if not kernel_content:
                        try:
                            kernlog_out = run_and_normalize("tail -n 200 /var/log/kern.log 2>/dev/null")
                            if kernlog_out.get("stdout", "").strip():
                                kernel_content = kernlog_out.get("stdout", "")[:20000]
                                kernel_source = "file"
                        except Exception:
                            pass
                    
                    # If file-based didn't work, try journalctl -k
                    if not kernel_content:
                        try:
                            journalctl_k_out = run_and_normalize("journalctl -k -n 200 --no-pager 2>/dev/null")
                            if journalctl_k_out.get("stdout", "").strip():
                                kernel_content = journalctl_k_out.get("stdout", "")[:20000]
                                kernel_source = "journalctl"
                        except Exception:
                            pass
                    
                    data["logs"]["kernel"] = {
                        "content": kernel_content,
                        "type": "current",
                        "collection_mode": "current",
                        "source": kernel_source,
                        "line_count": len(kernel_content.splitlines()) if kernel_content else 0
                    }
                except Exception as e:
                    logger.debug("Failed to collect kernel logs: %s", e, exc_info=True)
                    data["logs"]["kernel"] = {
                        "content": "",
                        "type": "current",
                        "collection_mode": "current",
                        "source": "unknown",
                        "line_count": 0
                }
        except Exception as e:
            logger.warning("Failed to collect OS logs: %s", e)
            data["logs"]["syslog"] = {"content": "", "type": "current", "collection_mode": "current", "source": "unknown", "line_count": 0}
            data["logs"]["auth_log"] = {"content": "", "type": "current", "collection_mode": "current", "source": "unknown", "line_count": 0}
            data["logs"]["kernel"] = {"content": "", "type": "current", "collection_mode": "current", "source": "unknown", "line_count": 0}
            data["errors"].append({
                "type": "collection_error",
                "component": "os",
                "stage": "log_collection",
                "message": f"Failed to collect OS logs: {e}",
                "detail": str(e),
                "traceback": traceback.format_exc()
            })

        return data

    # ----------------------------------------------------------------------
    def collect(self):
        """
        Required interface: run OS collection on *all hosts*.
        """
        self.start_collection()

        result = {
            "type": "host",
            "discovered": [],
            "instances": [],
            "findings": {},  # Legacy field for backward compatibility
            "collection_mode": "historical" if self._is_historical_collection() else "current"
        }
        
        # Add time window metadata if historical collection
        if self._is_historical_collection():
            since, until = self._get_time_window()
            result["time_window"] = {
                "since": since.isoformat(),
                "until": until.isoformat(),
                "issue_time": self.issue_time.isoformat() if self.issue_time else None
            }

        # Import here to avoid circular dependency
        from connectors.host_connectors.local_host_connector import LocalHostConnector

        for host_info in self.hosts:
            name = host_info.get("name") or host_info.get("address")

            try:
                # Choose connector: tests/sanity often use LocalHostConnector.
                connector = LocalHostConnector(host_info=host_info)

                # mark host discovered
                result["discovered"].append(name)

                # collect
                instance_data = self.collect_for_host(host_info, connector)
                
                # Add collection_mode and time_window to instance
                instance_data["collection_mode"] = result["collection_mode"]
                if self._is_historical_collection():
                    since, until = self._get_time_window()
                    instance_data["time_window"] = {
                        "since": since.isoformat(),
                        "until": until.isoformat(),
                        "issue_time": self.issue_time.isoformat() if self.issue_time else None
                    }

                # Add to instances array
                result["instances"].append(instance_data)
                
                # Also add to findings for backward compatibility (flattened structure)
                # Merge metrics and logs into top-level for legacy consumers
                legacy_findings = {
                    **instance_data.get("metrics", {}),
                    **instance_data.get("logs", {})
                }
                # Preserve name, errors, commands, collection_mode, time_window
                for key in ["name", "errors", "commands", "collection_mode", "time_window"]:
                    if key in instance_data:
                        legacy_findings[key] = instance_data[key]
                result["findings"][name] = legacy_findings

            except Exception as e:
                result["discovered"].append(name)
                error_entry = {
                    "type": "collection_error",
                    "component": "os",
                    "stage": "host_collection",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }
                result["instances"].append({
                    "name": name,
                    "metrics": {},
                    "logs": {},
                    "commands": [],
                    "errors": [error_entry]
                })
                result["findings"][name] = {
                    "errors": [error_entry]
                }

        self.end_collection()
        return result