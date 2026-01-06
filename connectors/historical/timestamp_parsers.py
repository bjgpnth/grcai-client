# connectors/historical/timestamp_parsers.py
"""
Timestamp parser registry for historical log collection.

Allows services to register timestamp parsers for their log formats.
Parsers are used to extract timestamps from log lines for time-based filtering.
"""

import re
from datetime import datetime
from typing import Optional, Callable, Dict

logger = None  # Will be initialized when logging is available


class TimestampParserRegistry:
    """
    Registry for timestamp parsers.
    Allows services to register their log timestamp format parsers.
    
    Usage:
        @TimestampParserRegistry.register("nginx_access")
        def parse_nginx_access_timestamp(line: str) -> Optional[datetime]:
            # Parse Nginx access.log format
            ...
    """
    
    _parsers: Dict[str, Callable[[str], Optional[datetime]]] = {}
    
    @classmethod
    def register(cls, name: str):
        """
        Decorator to register a timestamp parser.
        
        Args:
            name: Unique name for the parser (e.g., "nginx_access", "tomcat")
            
        Returns:
            Decorator function
        """
        def decorator(parser_func: Callable[[str], Optional[datetime]]):
            cls._parsers[name] = parser_func
            return parser_func
        return decorator
    
    @classmethod
    def get(cls, name: str) -> Optional[Callable[[str], Optional[datetime]]]:
        """
        Get registered parser by name.
        
        Args:
            name: Parser name
            
        Returns:
            Parser function or None if not found
        """
        return cls._parsers.get(name)
    
    @classmethod
    def list_parsers(cls) -> list:
        """List all registered parser names."""
        return list(cls._parsers.keys())
    
    @classmethod
    def is_registered(cls, name: str) -> bool:
        """Check if a parser is registered."""
        return name in cls._parsers


# ============================================================================
# Built-in Nginx timestamp parsers
# ============================================================================

# @TimestampParserRegistry.register("nginx_access")
# def parse_nginx_access_timestamp(line: str) -> Optional[datetime]:
#     """
#     Parse Nginx access.log timestamp.
    
#     Format: '[12/Nov/2025:06:14:55 +0000]'
#     Example: '192.168.1.1 - - [12/Nov/2025:06:14:55 +0000] "GET / HTTP/1.1" 200 ...'
    
#     Args:
#         line: Log line from Nginx access.log
        
#     Returns:
#         Parsed datetime or None if not found/invalid
#     """
#     if not line:
#         return None
    
#     # Pattern: [DD/Mon/YYYY:HH:MM:SS +/-TZ]
#     pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})\]'
#     match = re.search(pattern, line)
    
#     if not match:
#         return None
    
#     try:
#         ts_str = match.group(1)
#         # Parse format: "12/Nov/2025:06:14:55 +0000"
#         dt = datetime.strptime(ts_str, "%d/%b/%Y:%H:%M:%S %z")
#         return dt
#     except (ValueError, AttributeError) as e:
#         return None
    
@TimestampParserRegistry.register("nginx_access")
def parse_nginx_access_timestamp(line: str) -> Optional[datetime]:
    """
    Parse Nginx access log timestamps with or without timezone.
    Supports both:
      [01/Dec/2025:11:03:06 +0000]
      [01/Dec/2025:11:03:06]
    """
    if not line:
        return None

    # Pattern WITH timezone
    match = re.search(r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}) ([+-]\d{4})\]', line)
    if match:
        ts = f"{match.group(1)} {match.group(2)}"
        try:
            return datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S %z")
        except:
            pass

    # Pattern WITHOUT timezone (fallback)
    match = re.search(r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})\]', line)
    if match:
        ts = match.group(1)
        try:
            # Assume UTC if no timezone
            return datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S").replace(tzinfo=None)
        except:
            return None

    return None


@TimestampParserRegistry.register("nginx_error")
def parse_nginx_error_timestamp(line: str) -> Optional[datetime]:
    """
    Parse Nginx error.log timestamp.
    
    Format: 'YYYY/MM/DD HH:MM:SS'
    Example: '2025/11/12 06:14:55 [error] ...'
    
    Also handles: '[timestamp] message' format
    
    Args:
        line: Log line from Nginx error.log
        
    Returns:
        Parsed datetime or None if not found/invalid
    """
    if not line:
        return None
    
    # Try ISO-like format first: YYYY/MM/DD HH:MM:SS
    pattern1 = r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'
    match = re.search(pattern1, line)
    
    if match:
        try:
            ts_str = match.group(1)
            dt = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S")
            return dt
        except (ValueError, AttributeError):
            pass
    
    # Try bracket format: [YYYY/MM/DD HH:MM:SS]
    pattern2 = r'\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\]'
    match = re.search(pattern2, line)
    
    if match:
        try:
            ts_str = match.group(1)
            dt = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S")
            return dt
        except (ValueError, AttributeError):
            pass
    
    # Try unix timestamp format
    pattern3 = r'(\d{10})'
    match = re.search(pattern3, line[:20])  # Check first 20 chars only
    if match:
        try:
            ts = int(match.group(1))
            if 1000000000 < ts < 2000000000:  # Reasonable range
                return datetime.fromtimestamp(ts)
        except (ValueError, OSError):
            pass
    
    return None


# ============================================================================
# Tomcat timestamp parser
# ============================================================================

@TimestampParserRegistry.register("tomcat")
def parse_tomcat_timestamp(line: str) -> Optional[datetime]:
    """
    Parse Tomcat catalina.out timestamp.
    
    Format: 'DD-MMM-YYYY HH:MM:SS.mmm LEVEL ...'
    Example: '20-Nov-2025 11:44:53.965 INFO [main] org.apache.catalina.startup...'
    
    Also handles Docker log format: '2025-11-20T11:44:53.965506720Z 20-Nov-2025 11:44:53.965 INFO ...'
    
    Args:
        line: Log line from Tomcat catalina.out or Docker logs
        
    Returns:
        Parsed datetime or None if not found/invalid
    """
    if not line:
        return None
    
    # First, try to parse Docker timestamp at the beginning (if present)
    # Format: 2025-11-20T11:44:53.965506720Z
    docker_pattern = r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.?\d*Z?\s+'
    match = re.match(docker_pattern, line)
    if match:
        try:
            ts_str = match.group(1)
            dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
            return dt
        except (ValueError, AttributeError):
            pass
    
    # Pattern for Tomcat native format: DD-MMM-YYYY HH:MM:SS.mmm
    # Examples: "20-Nov-2025 11:44:53.965" or "12-Nov-2025 13:47:49.238"
    pattern = r'(\d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3})'
    match = re.search(pattern, line)
    
    if match:
        try:
            ts_str = match.group(1)
            # Parse format: "20-Nov-2025 11:44:53.965"
            dt = datetime.strptime(ts_str, "%d-%b-%Y %H:%M:%S.%f")
            return dt
        except (ValueError, AttributeError):
            pass
    
    return None


# ============================================================================
# Postgres timestamp parser
# ============================================================================

@TimestampParserRegistry.register("postgres")
def parse_postgres_timestamp(line: str) -> Optional[datetime]:
    """
    Parse PostgreSQL log timestamp.
    
    Format: 'YYYY-MM-DD HH:MM:SS.mmm UTC [PID] LEVEL: message'
    Example: '2025-11-23 11:02:55.123 UTC [12345] LOG:  statement: SELECT * FROM users;'
    
    Also handles Docker log format: '2025-11-23T11:02:55.123456789Z 2025-11-23 11:02:55.123 UTC ...'
    
    Args:
        line: Log line from PostgreSQL log file or Docker logs
        
    Returns:
        Parsed datetime or None if not found/invalid
    """
    if not line:
        return None
    
    # First, try to parse Docker timestamp at the beginning (if present)
    # Format: 2025-11-23T11:02:55.123456789Z
    docker_pattern = r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.?\d*Z?\s+'
    match = re.match(docker_pattern, line)
    if match:
        try:
            ts_str = match.group(1)
            dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
            return dt
        except (ValueError, AttributeError):
            pass
    
    # Pattern for Postgres native format: YYYY-MM-DD HH:MM:SS.mmm UTC
    # Examples: "2025-11-23 11:02:55.123 UTC" or "2025-11-23 11:02:55 UTC"
    pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\s+(?:UTC|GMT|[\+\-]\d{4})'
    match = re.search(pattern, line)
    
    if match:
        try:
            ts_str = match.group(1)
            # Try with microseconds first
            try:
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                # Fall back to seconds only
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            return dt
        except (ValueError, AttributeError):
            pass
    
    # Alternative pattern without timezone (some configs)
    pattern2 = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)'
    match = re.search(pattern2, line[:50])  # Check first 50 chars
    if match:
        try:
            ts_str = match.group(1)
            try:
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            return dt
        except (ValueError, AttributeError):
            pass
    
    return None


# ============================================================================
# Syslog timestamp parser
# ============================================================================

@TimestampParserRegistry.register("syslog")
def parse_syslog_timestamp(line: str, year: Optional[int] = None) -> Optional[datetime]:
    """
    Parse syslog timestamp.
    
    Standard syslog format: 'MMM DD HH:MM:SS hostname program: message'
    Example: 'Nov 23 10:30:45 hostname systemd: Started service'
    
    Also handles ISO format: '2025-11-23T10:30:45Z ...'
    And Debian/Ubuntu syslog format: '2025-11-23 10:30:45 hostname ...'
    
    Args:
        line: Log line from syslog or messages
        year: Optional year to use for standard syslog format (without year).
              If None, uses current year. Should be set to issue_time.year for historical collection.
        
    Returns:
        Parsed datetime or None if not found/invalid
    """
    if not line:
        return None
    
    # First, try ISO format: 2025-11-23T10:30:45Z
    iso_pattern = r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
    match = re.match(iso_pattern, line)
    if match:
        try:
            ts_str = match.group(1)
            dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
            return dt
        except (ValueError, AttributeError):
            pass
    
    # Try Debian/Ubuntu format: 2025-11-23 10:30:45
    debian_pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
    match = re.match(debian_pattern, line)
    if match:
        try:
            ts_str = match.group(1)
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            return dt
        except (ValueError, AttributeError):
            pass
    
    # Standard syslog format: MMM DD HH:MM:SS (without year)
    # Examples: "Nov 23 10:30:45" or "Nov  3 10:30:45" (single digit day)
    syslog_pattern = r'^(\w{3})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})'
    match = re.match(syslog_pattern, line)
    if match:
        try:
            month_str = match.group(1)
            day_str = match.group(2)
            time_str = match.group(3)
            
            # Use provided year, or fall back to current year
            # For historical collection, year should be set to issue_time.year
            # This handles cases like collecting on Jan 1 for a Dec 31 event
            use_year = year if year is not None else datetime.now().year
            
            # Build full datetime string
            full_str = f"{month_str} {day_str:>2} {time_str} {use_year}"
            dt = datetime.strptime(full_str, "%b %d %H:%M:%S %Y")
            return dt
        except (ValueError, AttributeError):
            pass
    
    return None


# ============================================================================
# Auth.log timestamp parser
# ============================================================================

@TimestampParserRegistry.register("auth")
def parse_auth_log_timestamp(line: str, year: Optional[int] = None) -> Optional[datetime]:
    """
    Parse auth.log timestamp.
    
    Format: 'MMM DD HH:MM:SS hostname program: message'
    Example: 'Nov 23 10:30:45 hostname sshd[1234]: Failed password for user'
    
    Same format as syslog but for auth.log.
    
    Args:
        line: Log line from /var/log/auth.log
        year: Optional year to use for standard format (without year).
              If None, uses current year. Should be set to issue_time.year for historical collection.
        
    Returns:
        Parsed datetime or None if not found/invalid
    """
    # Auth.log uses same format as syslog
    return parse_syslog_timestamp(line, year=year)


# ============================================================================
# Journalctl timestamp parser (for structured journal logs)
# ============================================================================

@TimestampParserRegistry.register("journalctl")
def parse_journalctl_timestamp(line: str) -> Optional[datetime]:
    """
    Parse journalctl log timestamp.
    
    Format: 'MMM DD HH:MM:SS hostname program[PID]: message'
    Example: 'Nov 23 10:30:45.123 hostname systemd[1]: Started service'
    
    Note: journalctl has native time filtering via --since/--until,
    so this parser is mainly for file-based journal logs or fallback.
    
    Args:
        line: Log line from journalctl output
        
    Returns:
        Parsed datetime or None if not found/invalid
    """
    # Journalctl output typically uses syslog format
    return parse_syslog_timestamp(line)


# ============================================================================
# Future parsers (will be registered as needed)
# ============================================================================
