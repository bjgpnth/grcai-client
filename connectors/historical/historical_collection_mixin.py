"""
Historical log collection mixin.

Provides reusable historical log collection capabilities that adapters can inherit.
This makes it easy to add historical collection to new services.
"""

import os
from datetime import datetime
from typing import List, Optional, Callable, Tuple
import logging

from connectors.historical.timestamp_parsers import TimestampParserRegistry
from connectors.historical.log_time_filter import (
    filter_logs_by_time_window,
    discover_log_files,
    read_log_file_lines,
    might_contain_time_window
)

logger = logging.getLogger("grcai.historical")


class HistoricalLogCollectionMixin:
    """
    Mixin that provides historical log collection capabilities.
    
    Adapters can inherit from this mixin to automatically get
    historical log collection methods. This makes it easy to
    add historical collection to new services.
    
    Usage:
        class NginxAdapter(BaseConnector, HistoricalLogCollectionMixin):
            def collect_for_host(self, ...):
                if self._is_historical_collection():
                    since, until = self._get_time_window()
                    logs = self._collect_historical_logs(
                        host_connector=host_connector,
                        container_id=container_id,
                        log_paths=["/var/log/nginx/access.log"],
                        parser_name="nginx_access",
                        since=since,
                        until=until
                    )
    """
    
    def _collect_historical_logs(
        self,
        host_connector,
        container_id: Optional[str],
        log_paths: List[str],
        parser_name: str,
        since: datetime,
        until: datetime,
        max_lines: int = 10000,
        chunk_size_mb: int = 50,
        parser_context: Optional[dict] = None
    ) -> str:
        """
        Generic historical log collection that works for any service.
        
        Strategy:
        1. Discover log files (active + rotated) that might contain time window
        2. Read each log file (handling compressed files)
        3. Parse timestamps using registered parser
        4. Filter lines within time window
        5. Combine and return filtered logs
        
        Args:
            host_connector: Host connector instance (LocalHostConnector, SSHHostConnector, etc.)
            container_id: Docker container ID (or None for host-based logs)
            log_paths: List of log file paths to check
            parser_name: Name of registered timestamp parser (e.g., "nginx_access", "tomcat")
            since: Start of time window
            until: End of time window
            max_lines: Maximum lines to return (safety limit)
            chunk_size_mb: Maximum file size to read in MB (for performance)
            parser_context: Optional dict with parser context (e.g., {"year": 2025} for syslog parser)
            
        Returns:
            Filtered log lines as string (newline-separated)
        """
        # Get registered parser
        base_parser = TimestampParserRegistry.get(parser_name)
        if not base_parser:
            logger.warning(f"Parser '{parser_name}' not found, falling back to current logs")
            return self._collect_fallback_logs(host_connector, container_id, log_paths)
        
        # Create parser wrapper if context is provided
        parser_context = parser_context or {}
        if parser_context:
            # Create a wrapper function that passes context to the parser
            import inspect
            sig = inspect.signature(base_parser)
            has_year_param = "year" in sig.parameters
            
            if has_year_param and "year" in parser_context:
                # Parser accepts year parameter - create wrapper that passes it
                year_value = parser_context["year"]
                def parser_with_context(line: str):
                    return base_parser(line, year=year_value)
                parser = parser_with_context
            else:
                # Parser doesn't accept year or no year in context - use as-is
                parser = base_parser
        else:
            parser = base_parser
        
        filtered_lines = []
        
        # Process each log path
        for base_path in log_paths:
            try:
                # Discover log files (active + rotated)
                discovered_files = self._discover_log_files(
                    host_connector,
                    container_id,
                    base_path,
                    since,
                    until
                )
                
                if not discovered_files:
                    logger.debug(f"No log files found for {base_path} within time window")
                    continue
                
                # Process each discovered log file
                for log_path in discovered_files:
                    try:
                        # Read log file
                        lines = self._read_log_file(
                            host_connector,
                            container_id,
                            log_path,
                            max_size_mb=chunk_size_mb
                        )
                        
                        if not lines:
                            continue
                        
                        # Filter by time window
                        filtered = filter_logs_by_time_window(
                            lines,
                            since,
                            until,
                            parser,
                            max_lines=max_lines
                        )
                        
                        filtered_lines.extend(filtered)
                        
                        # Safety limit
                        if len(filtered_lines) >= max_lines:
                            filtered_lines = filtered_lines[:max_lines]
                            logger.info(f"Reached max_lines limit ({max_lines}) for historical collection")
                            break
                            
                    except Exception as e:
                        logger.warning(f"Failed to process log file {log_path}: {e}")
                        continue
                
                # Stop if we've reached the limit
                if len(filtered_lines) >= max_lines:
                    break
                    
            except Exception as e:
                logger.warning(f"Failed to process log path {base_path}: {e}")
                continue
        
        return "\n".join(filtered_lines)
    
    def _discover_log_files(
        self,
        host_connector,
        container_id: Optional[str],
        base_path: str,
        since: datetime,
        until: datetime
    ) -> List[str]:
        """
        Discover log files (active + rotated) that might contain time window.
        
        Uses host_connector to check file existence and modification times.
        
        Args:
            host_connector: Host connector instance
            container_id: Docker container ID (or None)
            base_path: Base log file path
            since: Start of time window
            until: End of time window
            
        Returns:
            List of log file paths that might contain the time window
        """
        candidates = []
        
        # Check base log file
        if self._log_exists(host_connector, container_id, base_path):
            if self._might_contain_window(host_connector, container_id, base_path, since, until):
                candidates.append(base_path)
        
        # Check rotated logs (implementation depends on whether we can list files)
        # For now, try common patterns
        rotated_patterns = self._get_rotation_patterns(base_path)
        
        for pattern in rotated_patterns:
            if self._log_exists(host_connector, container_id, pattern):
                if self._might_contain_window(host_connector, container_id, pattern, since, until):
                    candidates.append(pattern)
        
        return sorted(candidates, reverse=True)  # Newest first
    
    def _get_rotation_patterns(self, base_path: str) -> List[str]:
        """Get common rotation patterns for a log path."""
        patterns = []
        base_dir = os.path.dirname(base_path)
        base_name = os.path.basename(base_path)
        base_name_no_ext = os.path.splitext(base_name)[0]
        base_ext = os.path.splitext(base_name)[1] or ""
        
        # Common patterns
        for i in range(1, 10):  # .log.1 through .log.9
            patterns.append(os.path.join(base_dir, f"{base_name_no_ext}{base_ext}.{i}"))
        
        # Compressed
        patterns.append(f"{base_path}.gz")
        for i in range(1, 10):
            patterns.append(os.path.join(base_dir, f"{base_name_no_ext}{base_ext}.{i}.gz"))
        
        return patterns
    
    def _log_exists(self, host_connector, container_id: Optional[str], path: str) -> bool:
        """Check if log file exists."""
        # Try to read file - if successful, exists
        try:
            result = self._read_log_file(host_connector, container_id, path, max_lines=1)
            return result is not None and len(result) >= 0
        except Exception:
            return False
    
    def _might_contain_window(
        self,
        host_connector,
        container_id: Optional[str],
        path: str,
        since: datetime,
        until: datetime
    ) -> bool:
        """
        Check if log file might contain time window (heuristic based on file mod time).
        
        For now, returns True if file exists (conservative approach).
        In future, could check file modification time if available.
        """
        return self._log_exists(host_connector, container_id, path)
    
    def _read_log_file(
        self,
        host_connector,
        container_id: Optional[str],
        path: str,
        max_lines: Optional[int] = None,
        max_size_mb: Optional[int] = None
    ) -> List[str]:
        """
        Read log file using host_connector.
        
        Handles both container and host-based files.
        Supports compressed files (.gz).
        """
        # Determine reading method based on container_id and host_connector capabilities
        if container_id and hasattr(host_connector, 'read_file_in_container'):
            # Read from container
            result = host_connector.read_file_in_container(container_id, path)
            if isinstance(result, dict):
                content = result.get('stdout', '') or result.get('output', '')
            else:
                content = str(result or '')
        elif hasattr(host_connector, 'read_file'):
            # Read from host
            result = host_connector.read_file(path)
            if isinstance(result, dict):
                content = result.get('stdout', '') or result.get('output', '')
            else:
                content = str(result or '')
        elif hasattr(host_connector, 'exec_cmd'):
            # Fallback: use exec to read file
            cmd = f"cat {path}" if not path.endswith('.gz') else f"zcat {path}"
            result = host_connector.exec_cmd(cmd)
            if isinstance(result, dict):
                content = result.get('stdout', '') or result.get('output', '')
            else:
                content = str(result or '')
        else:
            raise RuntimeError(f"Cannot read file {path}: no read method available")
        
        if not content:
            return []
        
        # Split into lines
        lines = content.splitlines()
        
        # Apply limits
        if max_lines:
            lines = lines[:max_lines]
        
        return lines
    
    def _collect_fallback_logs(
        self,
        host_connector,
        container_id: Optional[str],
        log_paths: List[str]
    ) -> str:
        """
        Fallback when historical collection fails.
        
        Collects current logs (tail -n 100) as fallback.
        """
        fallback_lines = []
        
        for log_path in log_paths:
            try:
                # Try to tail the log file
                if container_id and hasattr(host_connector, 'exec_in_container'):
                    cmd = f"tail -n 100 {log_path} 2>/dev/null || true"
                    result = host_connector.exec_in_container(container_id, cmd)
                elif hasattr(host_connector, 'exec_cmd'):
                    cmd = f"tail -n 100 {log_path} 2>/dev/null || true"
                    result = host_connector.exec_cmd(cmd)
                else:
                    continue
                
                if isinstance(result, dict):
                    content = result.get('stdout', '') or result.get('output', '')
                else:
                    content = str(result or '')
                
                if content:
                    fallback_lines.extend(content.splitlines())
                    
            except Exception:
                continue
        
        return "\n".join(fallback_lines)
