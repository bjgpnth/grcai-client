# connectors/base_connector.py

from datetime import datetime, timedelta
from typing import Tuple, Optional


class BaseConnector:
    """
    Base class for all service connectors.
    Handles shared parameters such as:
      - name
      - issue_time
      - component_config (service-level config)
      - env_config (environment-level config: hosts, access, etc.)
    """

    def __init__(self, name, issue_time, component_config=None, env_config=None, time_window_minutes=None):
        self.name = name
        self.issue_time = issue_time

        # Service-level config (from services.<service>)
        self.component_config = component_config or {}

        # NEW — environment-level config (hosts, access, etc.)
        self.env_config = env_config or {}
        
        # Time window for historical collection
        # Default: 30 minutes (can be overridden in component_config)
        self.time_window_minutes = (
            time_window_minutes or 
            self.component_config.get("time_window_minutes", 30)
        )

    # Optional hooks used by SessionOrchestrator
    def start_collection(self):
        pass

    def end_collection(self):
        pass
    
    def _get_time_window(
        self, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None
    ) -> Tuple[datetime, datetime]:
        """
        Calculate time window around issue_time.
        
        Strategy:
        - Single time input: 30 minutes before / 30 minutes after (symmetrical)
        - Time range input (start/end): 30 minutes before start / 10 minutes after end (asymmetrical)
        
        Args:
            start_time: Start of event (defaults to issue_time if None)
            end_time: End of event (None = single time, not None = time range)
            
        Returns:
            Tuple of (since, until) datetimes
        """
        if not start_time:
            start_time = self.issue_time
        
        if not self.issue_time:
            # Fallback if no issue_time set
            start_time = datetime.utcnow()
        
        if not end_time:
            # Single time: symmetrical window (30 min before / 30 min after)
            since = start_time - timedelta(minutes=self.time_window_minutes)
            until = start_time + timedelta(minutes=self.time_window_minutes)
        else:
            # Time range: asymmetrical window (30 min before start / 10 min after end)
            since = start_time - timedelta(minutes=self.time_window_minutes)
            until = end_time + timedelta(minutes=10)  # 10 min buffer after end
        
        return since, until
    
    def _is_historical_collection(self) -> bool:
        """
        Determine if issue_time is in the past (more than 5 minutes ago).
        
        If issue_time is recent (< 5 minutes), collect current state.
        If issue_time is in the past, attempt historical collection.
        
        Returns:
            True if collection should be historical, False for current collection
        """
        if not self.issue_time:
            return False
        
        # Check if issue_time is more than 5 minutes in the past
        threshold = datetime.utcnow() - timedelta(minutes=5)
        
        # Normalize timezone if needed
        if self.issue_time.tzinfo is None:
            # Assume UTC if no timezone
            issue_time_utc = self.issue_time
            threshold_utc = threshold
        else:
            # Both have timezones, convert threshold to same timezone
            threshold_utc = threshold.replace(tzinfo=self.issue_time.tzinfo) if not threshold.tzinfo else threshold
        
        return self.issue_time < threshold_utc