# ui/components/log_viewer.py
"""
Log viewer and formatting utilities.
"""


def format_log_entry(log_data, log_name):
    """
    Format a log entry for display.
    
    Args:
        log_data: dict or string - log content with metadata
        log_name: str - name of the log (e.g., "syslog", "catalina.out")
        
    Returns:
        dict: {
            "name": str,
            "content": str,
            "type": str,
            "source": str,
            "collection_mode": str,
            "time_window": dict or None,
            "line_count": int
        }
    """
    if isinstance(log_data, str):
        # Legacy format - just string content
        return {
            "name": log_name,
            "content": log_data,
            "type": "current",
            "source": "unknown",
            "collection_mode": "current",
            "time_window": None,
            "line_count": len(log_data.splitlines()) if log_data else 0
        }
    
    if isinstance(log_data, dict):
        # Structured format
        content = log_data.get("content", "")
        # Calculate line_count from content if not provided or is 0
        line_count = log_data.get("line_count")
        if line_count is None or line_count == 0:
            line_count = len(content.splitlines()) if content else 0
        
        return {
            "name": log_name,
            "content": content,
            "type": log_data.get("type", "current"),
            "source": log_data.get("source", "unknown"),
            "collection_mode": log_data.get("collection_mode", "current"),
            "time_window": log_data.get("time_window"),
            "line_count": line_count
        }
    
    return {
        "name": log_name,
        "content": "",
        "type": "current",
        "source": "unknown",
        "collection_mode": "current",
        "time_window": None,
        "line_count": 0
    }

