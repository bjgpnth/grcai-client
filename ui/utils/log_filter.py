# ui/utils/log_filter.py
"""
Log filtering utility for LLM Input Review.

Filters out "normal" log lines that don't add troubleshooting value,
reducing payload size while preserving important error/exception information.
"""

import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import copy


# Patterns for lines to KEEP (never remove)
KEEP_PATTERNS = [
    re.compile(r"ERROR", re.IGNORECASE),
    re.compile(r"FATAL", re.IGNORECASE),
    re.compile(r"SEVERE", re.IGNORECASE),
    re.compile(r"CRITICAL", re.IGNORECASE),
    re.compile(r"Exception", re.IGNORECASE),
    re.compile(r"OutOfMemoryError", re.IGNORECASE),
    re.compile(r"StackOverflowError", re.IGNORECASE),
    re.compile(r'"\s*[45]\d{2}\s+', re.IGNORECASE),  # HTTP 4xx/5xx
    re.compile(r"timeout", re.IGNORECASE),
    re.compile(r"connection.*fail", re.IGNORECASE),
    re.compile(r"oom", re.IGNORECASE),
    re.compile(r"Out of memory", re.IGNORECASE),
    re.compile(r"auth.*fail", re.IGNORECASE),
    re.compile(r"unauthorized", re.IGNORECASE),
    re.compile(r"database.*error", re.IGNORECASE),
    re.compile(r"sql.*error", re.IGNORECASE),
    re.compile(r"connection.*pool.*exhausted", re.IGNORECASE),
    re.compile(r"config.*error", re.IGNORECASE),
]

# Patterns for lines to REMOVE (safe to filter)
REMOVE_PATTERNS = [
    re.compile(r"^.*\bINFO\b.*Request processed successfully", re.IGNORECASE),
    re.compile(r"^.*\bDEBUG\b.*", re.IGNORECASE),
    re.compile(r"^.*\bTRACE\b.*", re.IGNORECASE),
    re.compile(r"^.*heartbeat.*$", re.IGNORECASE),
    re.compile(r"^.*healthcheck.*$", re.IGNORECASE),
    re.compile(r"^.*\bINFO\b.*Cache hit", re.IGNORECASE),
    re.compile(r"^.*\bINFO\b.*Cache miss", re.IGNORECASE),
    re.compile(r"/docker-entrypoint.sh:", re.IGNORECASE),  # Docker init messages
    re.compile(r"^.*GC.*\binfo\b.*$", re.IGNORECASE),  # Normal GC logs
    re.compile(r'"\s*200\s+', re.IGNORECASE),  # HTTP 200 (normal requests)
    re.compile(r'"\s*301\s+', re.IGNORECASE),  # HTTP 301 (redirects)
    re.compile(r'"\s*302\s+', re.IGNORECASE),  # HTTP 302 (redirects)
]


class LogFilter:
    """Filters logs to reduce payload size while preserving important information."""
    
    def __init__(self):
        self.filter_stats = {
            "total_lines": 0,
            "kept_lines": 0,
            "removed_lines": 0,
            "context_lines_added": 0,
        }
    
    def filter_logs(
        self, 
        logs: Dict[str, Any], 
        aggressiveness: str = "medium",
        issue_time: Optional[str] = None,
        context_lines: int = 5
    ) -> Tuple[Dict[str, Any], Dict[str, int]]:
        """
        Filter logs based on rules and aggressiveness level.
        
        Args:
            logs: Dictionary of log data (from evidence structure)
            aggressiveness: "light", "medium", or "aggressive"
            issue_time: ISO timestamp of issue time (for time-based filtering)
            context_lines: Number of context lines to keep around errors
            
        Returns:
            Tuple of (filtered_logs, filter_stats)
        """
        # Reset stats
        self.filter_stats = {
            "total_lines": 0,
            "kept_lines": 0,
            "removed_lines": 0,
            "context_lines_added": 0,
        }
        
        # Deep copy to avoid modifying original
        filtered_logs = copy.deepcopy(logs)
        
        # Process each log entry
        self._filter_logs_recursive(filtered_logs, aggressiveness, issue_time, context_lines)
        
        return filtered_logs, self.filter_stats.copy()
    
    def _filter_logs_recursive(
        self,
        obj: Any,
        aggressiveness: str,
        issue_time: Optional[str],
        context_lines: int
    ):
        """Recursively filter logs in dict/list structures."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                # Check if this looks like a log field
                if "log" in key.lower() and isinstance(value, str):
                    obj[key] = self._filter_log_text(value, aggressiveness, issue_time, context_lines)
                elif isinstance(value, (dict, list)):
                    self._filter_logs_recursive(value, aggressiveness, issue_time, context_lines)
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                if isinstance(item, str):
                    # Check if this list contains log lines
                    if any("\n" in item or len(item) > 100 for item in obj if isinstance(item, str)):
                        obj[idx] = self._filter_log_text(item, aggressiveness, issue_time, context_lines)
                    else:
                        self._filter_logs_recursive(item, aggressiveness, issue_time, context_lines)
                elif isinstance(item, (dict, list)):
                    self._filter_logs_recursive(item, aggressiveness, issue_time, context_lines)
    
    def _filter_log_text(
        self,
        log_text: str,
        aggressiveness: str,
        issue_time: Optional[str],
        context_lines: int
    ) -> str:
        """Filter a single log text string."""
        if not log_text or not isinstance(log_text, str):
            return log_text
        
        lines = log_text.split('\n')
        self.filter_stats["total_lines"] += len(lines)
        
        # Determine which lines to keep
        kept_indices = set()
        error_indices = set()
        
        for idx, line in enumerate(lines):
            # Always keep lines matching KEEP patterns
            if self._matches_any_pattern(line, KEEP_PATTERNS):
                kept_indices.add(idx)
                error_indices.add(idx)
            
            # Remove lines matching REMOVE patterns (depending on aggressiveness)
            elif aggressiveness != "light" and self._matches_any_pattern(line, REMOVE_PATTERNS):
                # Don't add to kept_indices - will be filtered
                pass
            
            # For aggressive filtering, remove more normal INFO lines
            elif aggressiveness == "aggressive":
                if re.search(r"^\s*\d{4}-\d{2}-\d{2}.*\bINFO\b.*$", line, re.IGNORECASE):
                    # Filter normal INFO lines in aggressive mode
                    pass
                else:
                    # Keep other lines in aggressive mode
                    kept_indices.add(idx)
            else:
                # Light/Medium: Keep non-matching lines
                kept_indices.add(idx)
        
        # Add context lines around errors
        if context_lines > 0:
            for error_idx in error_indices:
                # Add lines before error
                for i in range(max(0, error_idx - context_lines), error_idx):
                    if i not in error_indices:  # Don't duplicate error lines
                        kept_indices.add(i)
                
                # Add lines after error
                for i in range(error_idx + 1, min(len(lines), error_idx + context_lines + 1)):
                    if i not in error_indices:  # Don't duplicate error lines
                        kept_indices.add(i)
        
        # Apply time-based filtering if issue_time is provided
        if issue_time and aggressiveness != "light":
            kept_indices = self._apply_time_filter(lines, kept_indices, issue_time)
        
        # Always keep first and last N lines (startup/shutdown)
        startup_lines = 10
        shutdown_lines = 10
        for i in range(min(startup_lines, len(lines))):
            kept_indices.add(i)
        for i in range(max(0, len(lines) - shutdown_lines), len(lines)):
            kept_indices.add(i)
        
        # Build filtered log
        filtered_lines = [lines[i] for i in sorted(kept_indices)]
        self.filter_stats["kept_lines"] += len(filtered_lines)
        self.filter_stats["removed_lines"] += len(lines) - len(filtered_lines)
        
        # For very large logs, apply volume-based filtering
        if len(lines) > 1000 and aggressiveness == "aggressive":
            # Keep only first 100 and last 100 lines, plus all errors
            if len(filtered_lines) > 200:
                first_lines = filtered_lines[:100]
                last_lines = filtered_lines[-100:]
                error_lines = [line for line in filtered_lines[100:-100] if self._matches_any_pattern(line, KEEP_PATTERNS)]
                filtered_lines = first_lines + error_lines + last_lines
                self.filter_stats["removed_lines"] += len(lines) - len(filtered_lines)
        
        return '\n'.join(filtered_lines)
    
    def _matches_any_pattern(self, text: str, patterns: List[re.Pattern]) -> bool:
        """Check if text matches any of the patterns."""
        for pattern in patterns:
            if pattern.search(text):
                return True
        return False
    
    def _apply_time_filter(
        self,
        lines: List[str],
        kept_indices: set,
        issue_time: str
    ) -> set:
        """Apply time-based filtering to keep only logs around issue_time."""
        try:
            # Parse issue_time
            if "T" in issue_time:
                issue_dt = datetime.fromisoformat(issue_time.replace("Z", "+00:00"))
            else:
                issue_dt = datetime.fromisoformat(issue_time)
            
            if issue_dt.tzinfo is None:
                issue_dt = issue_dt.replace(tzinfo=timezone.utc)
            
            # Window: 1 hour before and after issue time
            window_seconds = 3600
            start_time = issue_dt.timestamp() - window_seconds
            end_time = issue_dt.timestamp() + window_seconds
            
            # Filter kept_indices to only include lines within time window
            filtered_indices = set()
            
            for idx in kept_indices:
                line = lines[idx]
                # Try to extract timestamp from log line
                # Common formats: ISO, syslog, nginx, etc.
                line_time = self._extract_timestamp_from_line(line)
                
                if line_time:
                    line_ts = line_time.timestamp()
                    if start_time <= line_ts <= end_time:
                        filtered_indices.add(idx)
                else:
                    # If we can't extract timestamp, keep the line (safer)
                    filtered_indices.add(idx)
            
            return filtered_indices
        
        except Exception:
            # If time filtering fails, return original indices (fail safe)
            return kept_indices
    
    def _extract_timestamp_from_line(self, line: str) -> Optional[datetime]:
        """Extract timestamp from a log line."""
        # Try ISO format: 2024-01-15T10:30:45.123Z
        iso_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)')
        match = iso_pattern.search(line)
        if match:
            try:
                ts_str = match.group(1)
                return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except Exception:
                pass
        
        # Try syslog format: Jan 15 10:30:45
        syslog_pattern = re.compile(r'([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})')
        match = syslog_pattern.search(line)
        if match:
            try:
                ts_str = match.group(1)
                # Add current year (rough approximation)
                current_year = datetime.now().year
                ts_str = f"{current_year} {ts_str}"
                return datetime.strptime(ts_str, "%Y %b %d %H:%M:%S")
            except Exception:
                pass
        
        return None


def filter_logs(
    logs: Dict[str, Any],
    aggressiveness: str = "medium",
    issue_time: Optional[str] = None,
    context_lines: int = 5
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """
    Convenience function to filter logs.
    
    Args:
        logs: Dictionary of log data
        aggressiveness: "light", "medium", or "aggressive"
        issue_time: ISO timestamp of issue time
        context_lines: Number of context lines around errors
        
    Returns:
        Tuple of (filtered_logs, filter_stats)
    """
    filterer = LogFilter()
    return filterer.filter_logs(logs, aggressiveness, issue_time, context_lines)

