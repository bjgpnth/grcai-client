"""
Multi-Pass RCA Progress Display Component
Shows real-time progress for component analysis and multi-pass reasoning.
"""

import streamlit as st
from typing import Dict, Any, Optional


class MultiPassProgressTracker:
    """Tracks and displays multi-pass RCA progress in Streamlit UI."""
    
    def __init__(self, container=None):
        """
        Initialize progress tracker.
        
        Args:
            container: Streamlit container to render progress in (defaults to main area)
        """
        self.container = container or st
        self.components = {}
        self.current_pass = None
        self.status_container = None
        self.progress_container = None
        
    def callback(self, event_type: str, data: Dict[str, Any]):
        """
        Progress callback function to be passed to LLMReasoner.
        
        Args:
            event_type: Type of progress event (e.g., "pass1_start", "pass1_component_complete")
            data: Event data dictionary
        """
        if event_type == "pass1_start":
            self._handle_pass1_start(data)
        elif event_type == "pass1_component_start":
            self._handle_component_start(data)
        elif event_type == "pass1_component_complete":
            self._handle_component_complete(data)
        elif event_type == "pass1_complete":
            self._handle_pass1_complete(data)
        elif event_type == "pass2_start":
            self._handle_pass2_start(data)
        elif event_type == "pass2_complete":
            self._handle_pass2_complete(data)
        elif event_type == "pass3_start":
            self._handle_pass3_start(data)
        elif event_type == "pass3_complete":
            self._handle_pass3_complete(data)
    
    def _handle_pass1_start(self, data: Dict[str, Any]):
        """Handle Pass 1 start event."""
        self.current_pass = 1
        total = data.get("total_components", 0)
        
        # Use the provided container or create a simple container (avoid nested st.status)
        # Don't create a status widget if we're already inside one
        if self.container and self.container is not st:
            self.progress_container = self.container
        else:
            # Create a simple container instead of status to avoid React conflicts
            self.progress_container = st.container()
        
        # Don't create status_container - we'll just use the progress_container
        self.status_container = None
        
        # Show initial message
        if self.progress_container:
            self.progress_container.write("🔍 **Pass 1: Analyzing Components**")
    
    def _handle_component_start(self, data: Dict[str, Any]):
        """Handle component analysis start."""
        # Simplified: Don't show component start, only show completion
        pass
    
    def _handle_component_complete(self, data: Dict[str, Any]):
        """Handle component analysis completion."""
        component = data.get("component", "unknown")
        success = data.get("success", False)
        
        if self.progress_container:
            # Simplified: Just show completion status
            if success:
                self.progress_container.write(f"✅ {component} - Completed")
            else:
                self.progress_container.write(f"⚠️ {component} - Failed (skipped)")
    
    def _handle_pass1_complete(self, data: Dict[str, Any]):
        """Handle Pass 1 completion."""
        succeeded = data.get("succeeded", 0)
        failed = data.get("failed", 0)
        
        if self.progress_container:
            # Simplified: Just show completion message
            self.progress_container.write(f"✅ Pass 1 Complete: {succeeded} components analyzed, {failed} failed")
    
    def _handle_pass2_start(self, data: Dict[str, Any]):
        """Handle Pass 2 start event."""
        self.current_pass = 2
        
        if self.progress_container:
            self.progress_container.write("🔗 **Pass 2: Cross-Component Correlation**")
    
    def _handle_pass2_complete(self, data: Dict[str, Any]):
        """Handle Pass 2 completion."""
        success = data.get("success", False)
        
        if self.progress_container:
            if success:
                self.progress_container.write("✅ Pass 2 Complete: Relationships identified")
            else:
                self.progress_container.write("⚠️ Pass 2 Failed: Using component summaries only")
    
    def _handle_pass3_start(self, data: Dict[str, Any]):
        """Handle Pass 3 start event."""
        self.current_pass = 3
        
        if self.progress_container:
            self.progress_container.write("📝 **Pass 3: Generating Final Report**")
    
    def _handle_pass3_complete(self, data: Dict[str, Any]):
        """Handle Pass 3 completion."""
        success = data.get("success", False)
        
        if self.progress_container:
            if success:
                self.progress_container.write("✅ Pass 3 Complete: Final report generated")
            else:
                self.progress_container.write("⚠️ Pass 3 Failed: Using cross-component analysis only")


def create_progress_tracker(container=None) -> MultiPassProgressTracker:
    """
    Create a progress tracker instance.
    
    Args:
        container: Streamlit container to render progress in
        
    Returns:
        MultiPassProgressTracker instance with callback function
    """
    return MultiPassProgressTracker(container)

