# ui/utils/helpers.py
"""
Helper functions for UI operations.

These are pure functions (or as close as possible) that can be easily unit tested.
"""

import json
from typing import Any, Optional, Dict
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Fallback for Python < 3.9
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None


def safe_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Safely navigate nested dictionary.
    
    Args:
        d: Dictionary to navigate
        *keys: Variable number of keys to traverse
        default: Default value if any key is missing or None
        
    Returns:
        Value at the nested path, or default if not found
        
    Examples:
        >>> safe_get({"a": {"b": 1}}, "a", "b")
        1
        >>> safe_get({"a": {"b": 1}}, "a", "c", default=0)
        0
        >>> safe_get({"a": None}, "a", "b", default="missing")
        'missing'
    """
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def load_evidence_json(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Load evidence JSON file from disk.
    
    This is a pure function that handles file I/O and JSON parsing.
    It does not interact with Streamlit session state.
    
    Args:
        file_path: Path to the evidence JSON file
        
    Returns:
        Parsed JSON as dictionary, or None if file cannot be read/parsed
        
    Raises:
        Does not raise exceptions - returns None on any error
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None
    except Exception:
        return None


def format_datetime_local(utc_datetime: Any, include_timezone: bool = True, user_timezone: Optional[str] = None) -> str:
    """
    Format UTC datetime to local timezone for display.
    
    Converts UTC datetime to user's local timezone (detected from browser or system) 
    and formats it with timezone information for clarity.
    
    Args:
        utc_datetime: UTC datetime (can be datetime object, ISO string, or None)
        include_timezone: Whether to include timezone abbreviation in output
        user_timezone: Browser timezone name (e.g., "Asia/Kolkata"). If None, uses system timezone.
        
    Returns:
        Formatted datetime string in local timezone (e.g., "2026-01-06 14:30:00 IST")
        or "N/A" if datetime is None/invalid
    """
    if utc_datetime is None:
        return "N/A"
    
    try:
        # Parse if string
        if isinstance(utc_datetime, str):
            # Handle ISO format with 'Z' or timezone offset
            if 'Z' in utc_datetime:
                dt = datetime.fromisoformat(utc_datetime.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(utc_datetime)
            
            # Ensure it's timezone-aware (assume UTC if not specified)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        elif isinstance(utc_datetime, datetime):
            dt = utc_datetime
            # Ensure it's timezone-aware (assume UTC if not specified)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            return str(utc_datetime)
        
        # Convert to user's timezone (browser timezone if provided, else system timezone)
        try:
            if user_timezone and ZoneInfo:
                # Use browser-detected timezone
                try:
                    user_tz = ZoneInfo(user_timezone)
                    local_dt = dt.astimezone(user_tz)
                except Exception:
                    # If timezone name is invalid, fall back to system timezone
                    local_dt = dt.astimezone()
            else:
                # Use system timezone (container's timezone, usually UTC in Docker)
                local_dt = dt.astimezone()
        except Exception:
            # Fallback to UTC if timezone conversion fails
            local_dt = dt
        
        # Format with timezone info
        if include_timezone:
            # Get timezone abbreviation or offset
            tz_name = local_dt.strftime("%Z")  # Timezone abbreviation (e.g., IST, PST)
            if not tz_name or tz_name == local_dt.strftime("%z"):  # If no abbreviation, use offset
                offset = local_dt.strftime("%z")
                if offset:
                    # Format offset as +HH:MM
                    offset_formatted = f"{offset[:3]}:{offset[3:]}"
                    tz_name = f"UTC{offset_formatted}"
                else:
                    tz_name = "Local"
            
            return f"{local_dt.strftime('%Y-%m-%d %H:%M:%S')} {tz_name}"
        else:
            return local_dt.strftime("%Y-%m-%d %H:%M:%S")
            
    except Exception:
        # Fallback to string representation if parsing fails
        return str(utc_datetime) if utc_datetime else "N/A"


def format_datetime_short(utc_datetime: Any, user_timezone: Optional[str] = None) -> str:
    """
    Format UTC datetime to local timezone (date only, no time).
    
    Args:
        utc_datetime: UTC datetime (can be datetime object, ISO string, or None)
        user_timezone: Browser timezone name (e.g., "Asia/Kolkata"). If None, uses system timezone.
        
    Returns:
        Formatted date string (e.g., "2026-01-06") or "N/A"
    """
    if utc_datetime is None:
        return "N/A"
    
    try:
        # Parse if string
        if isinstance(utc_datetime, str):
            if 'Z' in utc_datetime:
                dt = datetime.fromisoformat(utc_datetime.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(utc_datetime)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        elif isinstance(utc_datetime, datetime):
            dt = utc_datetime
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            return str(utc_datetime)
        
        # Convert to user's timezone (browser timezone if provided, else system timezone)
        try:
            if user_timezone and ZoneInfo:
                try:
                    user_tz = ZoneInfo(user_timezone)
                    local_dt = dt.astimezone(user_tz)
                except Exception:
                    local_dt = dt.astimezone()
            else:
                local_dt = dt.astimezone()
        except Exception:
            local_dt = dt
        
        return local_dt.strftime("%Y-%m-%d")
    except Exception:
        return str(utc_datetime) if utc_datetime else "N/A"


def format_time_only(utc_datetime: Any, user_timezone: Optional[str] = None) -> str:
    """
    Format UTC datetime to local timezone (time only, no date).
    
    Args:
        utc_datetime: UTC datetime (can be datetime object, ISO string, or None)
        user_timezone: Browser timezone name (e.g., "Asia/Kolkata"). If None, uses system timezone.
        
    Returns:
        Formatted time string (e.g., "14:30:00") or "N/A"
    """
    if utc_datetime is None:
        return "N/A"
    
    try:
        # Parse if string
        if isinstance(utc_datetime, str):
            if 'Z' in utc_datetime:
                dt = datetime.fromisoformat(utc_datetime.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(utc_datetime)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        elif isinstance(utc_datetime, datetime):
            dt = utc_datetime
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            return str(utc_datetime)
        
        # Convert to user's timezone (browser timezone if provided, else system timezone)
        try:
            if user_timezone and ZoneInfo:
                try:
                    user_tz = ZoneInfo(user_timezone)
                    local_dt = dt.astimezone(user_tz)
                except Exception:
                    local_dt = dt.astimezone()
            else:
                local_dt = dt.astimezone()
        except Exception:
            local_dt = dt
        
        return local_dt.strftime("%H:%M:%S")
    except Exception:
        return str(utc_datetime) if utc_datetime else "N/A"

