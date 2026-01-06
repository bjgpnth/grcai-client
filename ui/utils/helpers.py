# ui/utils/helpers.py
"""
Helper functions for UI operations.

These are pure functions (or as close as possible) that can be easily unit tested.
"""

import json
from typing import Any, Optional, Dict


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

