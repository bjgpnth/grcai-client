"""
Log time filtering utilities for historical log collection.

Provides core functions for:
- Filtering log lines by time window
- Detecting log rotation patterns
- Handling compressed logs
"""

import os
import glob
import gzip
from datetime import datetime, timedelta
from typing import List, Optional, Callable
from pathlib import Path


def filter_logs_by_time_window(
    log_lines: List[str],
    since: datetime,
    until: datetime,
    parser_func: Callable[[str], Optional[datetime]],
    max_lines: Optional[int] = None
) -> List[str]:
    """
    Filter log lines by time window using a timestamp parser.
    
    Args:
        log_lines: List of log lines to filter
        since: Start of time window (inclusive)
        until: End of time window (inclusive)
        parser_func: Function to parse timestamp from log line
        max_lines: Maximum lines to return (None = no limit)
        
    Returns:
        Filtered list of log lines within time window
    """
    filtered = []
    
    for line in log_lines:
        if not line or not line.strip():
            continue
            
        # Parse timestamp from line
        timestamp = parser_func(line)
        
        if timestamp:
            # Normalize timezone for comparison
            # Make both timestamp and window boundaries timezone-aware for comparison
            from datetime import timezone
            
            # Normalize timestamp to UTC if it has timezone
            if timestamp.tzinfo is not None:
                timestamp = timestamp.astimezone(timezone.utc)
            else:
                # If timestamp is naive, assume UTC
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            
            # Normalize since/until to UTC if they have timezone
            since_normalized = since
            if since.tzinfo is not None:
                since_normalized = since.astimezone(timezone.utc)
            elif since.tzinfo is None:
                since_normalized = since.replace(tzinfo=timezone.utc)
            
            until_normalized = until
            if until.tzinfo is not None:
                until_normalized = until.astimezone(timezone.utc)
            elif until.tzinfo is None:
                until_normalized = until.replace(tzinfo=timezone.utc)
            
            # Check if timestamp is within window
            if since_normalized <= timestamp <= until_normalized:
                filtered.append(line)
                
                # Safety limit
                if max_lines and len(filtered) >= max_lines:
                    break
    
    return filtered


def detect_log_rotation_pattern(base_path: str) -> List[str]:
    """
    Detect rotated log files for a given base log path.
    
    Checks for common rotation patterns:
    - .log.1, .log.2, .log.3 (numbered)
    - .log.gz, .log.1.gz, .log.2.gz (compressed)
    - .log.YYYY-MM-DD (date-based)
    - .log.YYYY-MM-DD.gz (date-based compressed)
    
    Args:
        base_path: Base log file path (e.g., "/var/log/nginx/access.log")
        
    Returns:
        List of rotated log file paths that exist
    """
    candidates = []
    base_path_obj = Path(base_path)
    base_dir = base_path_obj.parent
    base_name = base_path_obj.stem  # filename without extension
    base_ext = base_path_obj.suffix  # .log
    
    # Pattern 1: Numbered rotation (.log.1, .log.2, etc.)
    numbered_pattern = str(base_dir / f"{base_name}{base_ext}.*")
    numbered_files = glob.glob(numbered_pattern)
    for f in numbered_files:
        if f != base_path and os.path.isfile(f):
            # Filter out .gz files (handle separately)
            if not f.endswith('.gz'):
                candidates.append(f)
    
    # Pattern 2: Compressed files (.log.gz, .log.1.gz, etc.)
    gz_pattern = str(base_dir / f"{base_name}{base_ext}*.gz")
    gz_files = glob.glob(gz_pattern)
    for f in gz_files:
        if os.path.isfile(f):
            candidates.append(f)
    
    # Pattern 3: Date-based rotation (.log.YYYY-MM-DD)
    date_pattern = str(base_dir / f"{base_name}{base_ext}.[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*")
    date_files = glob.glob(date_pattern)
    for f in date_files:
        if os.path.isfile(f):
            candidates.append(f)
    
    return sorted(candidates, reverse=True)  # Newest first


def might_contain_time_window(
    log_path: str,
    since: datetime,
    until: datetime,
    tolerance_hours: int = 48
) -> bool:
    """
    Check if a log file might contain logs within the time window.
    
    Uses file modification time as a heuristic. If the file was modified
    within tolerance_hours of the time window, it might contain relevant logs.
    
    Args:
        log_path: Path to log file
        since: Start of time window
        until: End of time window
        tolerance_hours: Hours to expand the check window (default: 48)
        
    Returns:
        True if file might contain logs in time window, False otherwise
    """
    if not os.path.exists(log_path):
        return False
    
    try:
        mod_time = datetime.fromtimestamp(os.path.getmtime(log_path))
        
        # Expand window by tolerance
        expanded_since = since - timedelta(hours=tolerance_hours)
        expanded_until = until + timedelta(hours=tolerance_hours)
        
        # File might be relevant if modified within expanded window
        if expanded_since <= mod_time <= expanded_until:
            return True
        
        # Also check if file is newer than until (might have logs from window)
        if mod_time > until:
            return True
        
        return False
    except (OSError, ValueError):
        return False


def discover_log_files(
    base_path: str,
    since: datetime,
    until: datetime
) -> List[str]:
    """
    Discover all log files (active + rotated) that might contain time window.
    
    Strategy:
    1. Check if base log exists and might contain window
    2. Find rotated logs (numbered, compressed, date-based)
    3. Filter by modification time to only include relevant files
    
    Args:
        base_path: Base log file path
        since: Start of time window
        until: End of time window
        
    Returns:
        List of log file paths that might contain the time window
    """
    candidates = []
    
    # 1. Check active log
    if os.path.exists(base_path):
        if might_contain_time_window(base_path, since, until):
            candidates.append(base_path)
    
    # 2. Check rotated logs
    rotated_files = detect_log_rotation_pattern(base_path)
    for rotated_path in rotated_files:
        if might_contain_time_window(rotated_path, since, until):
            candidates.append(rotated_path)
    
    return sorted(candidates, reverse=True)  # Newest first


def is_compressed_file(file_path: str) -> bool:
    """Check if a file is compressed (.gz)."""
    return file_path.endswith('.gz')


def read_log_file_lines(file_path: str, max_size_mb: Optional[int] = None) -> List[str]:
    """
    Read log file lines, handling both plain text and compressed files.
    
    Args:
        file_path: Path to log file
        max_size_mb: Maximum file size in MB to read (None = no limit)
        
    Returns:
        List of log lines
        
    Raises:
        IOError: If file cannot be read
    """
    # Check file size
    if max_size_mb:
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if file_size_mb > max_size_mb:
            raise IOError(f"File {file_path} is too large ({file_size_mb:.1f}MB > {max_size_mb}MB)")
    
    # Read compressed or plain file
    if is_compressed_file(file_path):
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                return f.readlines()
        except (IOError, gzip.BadGzipFile) as e:
            raise IOError(f"Failed to read compressed file {file_path}: {e}")
    else:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.readlines()
        except IOError as e:
            raise IOError(f"Failed to read file {file_path}: {e}")
