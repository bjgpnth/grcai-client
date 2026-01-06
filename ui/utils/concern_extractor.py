# ui/utils/concern_extractor.py
"""
Concern extraction engine for Evidence Highlights tab.

Automatically detects issues, errors, and anomalies from evidence data
using pattern matching, threshold checks, and deterministic analysis.
"""

import re
from typing import Dict, List, Any, Optional
from datetime import datetime


# Pre-compiled regex patterns for performance
CONCERN_PATTERNS = {
    "out_of_memory": [
        re.compile(r"java\.lang\.OutOfMemoryError", re.IGNORECASE),
        re.compile(r"GC overhead limit exceeded", re.IGNORECASE),
        re.compile(r"OutOfMemoryError", re.IGNORECASE),
        re.compile(r"Out of memory", re.IGNORECASE),
    ],
    "stack_overflow": [
        re.compile(r"StackOverflowError", re.IGNORECASE),
        re.compile(r"Stack overflow", re.IGNORECASE),
    ],
    "nginx_5xx": [
        re.compile(r'"\s*5\d{2}\s+', re.IGNORECASE),
        re.compile(r'HTTP/1\.\d+"\s*5\d{2}', re.IGNORECASE),
    ],
    "nginx_4xx": [
        re.compile(r'"\s*4\d{2}\s+', re.IGNORECASE),
    ],
    "tomcat_severe": [
        re.compile(r"SEVERE\s+", re.IGNORECASE),
    ],
    "tomcat_warning": [
        re.compile(r"WARNING\s+", re.IGNORECASE),
    ],
    "oom_killer": [
        re.compile(r"Out of memory: Kill process", re.IGNORECASE),
        re.compile(r"oom-killer", re.IGNORECASE),
        re.compile(r"Memory cgroup out of memory", re.IGNORECASE),
        re.compile(r"oom_reaper", re.IGNORECASE),
    ],
    "kernel_error": [
        re.compile(r"kernel:\s*.*error", re.IGNORECASE),
        re.compile(r"kernel:\s*.*fail", re.IGNORECASE),
    ],
    "connection_timeout": [
        re.compile(r"connection.*timeout", re.IGNORECASE),
        re.compile(r"timed out", re.IGNORECASE),
        re.compile(r"time.*out", re.IGNORECASE),
    ],
    "connection_refused": [
        re.compile(r"connection.*refused", re.IGNORECASE),
        re.compile(r"connection.*denied", re.IGNORECASE),
    ],
    "auth_failure": [
        re.compile(r"authentication.*fail", re.IGNORECASE),
        re.compile(r"auth.*fail", re.IGNORECASE),
        re.compile(r"login.*fail", re.IGNORECASE),
        re.compile(r"unauthorized", re.IGNORECASE),
    ],
    "permission_denied": [
        re.compile(r"permission.*denied", re.IGNORECASE),
        re.compile(r"access.*denied", re.IGNORECASE),
    ],
    "database_error": [
        re.compile(r"database.*error", re.IGNORECASE),
        re.compile(r"sql.*error", re.IGNORECASE),
        re.compile(r"connection.*pool.*exhausted", re.IGNORECASE),
        re.compile(r"too many connections", re.IGNORECASE),
    ],
    "port_binding": [
        re.compile(r"address already in use", re.IGNORECASE),
        re.compile(r"port.*already.*use", re.IGNORECASE),
        re.compile(r"bind.*fail", re.IGNORECASE),
    ],
    "disk_full": [
        re.compile(r"no space left", re.IGNORECASE),
        re.compile(r"disk.*full", re.IGNORECASE),
        re.compile(r"device.*full", re.IGNORECASE),
    ],
}

# Thresholds for resource concerns
RESOURCE_THRESHOLDS = {
    "high_cpu": 80.0,
    "high_memory": 85.0,
    "high_disk": 80.0,
}


class ConcernExtractor:
    """Extracts concerns and issues from evidence data."""
    
    def __init__(self):
        self.concerns = []
    
    def extract_concerns(self, evidence_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all concerns from evidence data.
        
        Args:
            evidence_data: Full evidence JSON structure
            
        Returns:
            List of concern dictionaries with severity, category, description, etc.
        """
        self.concerns = []
        
        # Process host-level evidence (components)
        host_data = evidence_data.get("host", {})
        
        for component_name, component_data in host_data.items():
            if not isinstance(component_data, dict):
                continue
            
            # Prioritize findings structure (keys are host names)
            findings = component_data.get("findings", {})
            instances = component_data.get("instances", [])
            
            if findings:
                # Use findings structure - keys are host names
                for host_name, finding_data in findings.items():
                    # For components like nginx/tomcat, instances are nested in findings
                    nested_instances = finding_data.get("instances", [])
                    if nested_instances:
                        # Multiple instances per host
                        for instance in nested_instances:
                            self._extract_instance_concerns(
                                component_name,
                                host_name,  # Use actual host name, not instance name
                                instance
                            )
                    else:
                        # Single instance per host - use finding_data as the instance
                        self._extract_instance_concerns(
                            component_name,
                            host_name,  # Use actual host name
                            finding_data
                        )
            elif instances:
                # Fallback: if no findings structure, try to match instances to hosts
                metadata = evidence_data.get("metadata", {})
                host_collection_status = metadata.get("host_collection_status", {})
                
                for instance in instances:
                    instance_name = instance.get("name", "unknown")
                    # Try to find which host this instance belongs to
                    host_name = "unknown"
                    for h_name, h_status in host_collection_status.items():
                        if component_name in h_status.get("collected", []):
                            host_name = h_name
                            break
                    
                    # If we found a host, use it; otherwise use instance name as fallback
                    if host_name == "unknown":
                        host_name = instance_name
                    
                    self._extract_instance_concerns(
                        component_name,
                        host_name,
                        instance
                    )
        
        # Process container-level evidence
        containers = evidence_data.get("containers", {})
        for container_name, container_data in containers.items():
            if isinstance(container_data, dict):
                self._extract_instance_concerns(
                    "container",
                    container_name,
                    container_data
                )
        
        # Sort concerns by severity (Critical > Warning > Info)
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        self.concerns.sort(key=lambda x: severity_order.get(x.get("severity", "info"), 2))
        
        return self.concerns
    
    def _extract_instance_concerns(
        self, 
        component: str, 
        host: str, 
        instance_data: Dict[str, Any]
    ):
        """Extract concerns from a single instance."""
        
        # Note: We intentionally skip collection errors (errors array) here
        # Collection errors are adapter/gRCAi issues, not infrastructure issues
        # They should be shown in the Errors tab, not in Evidence Highlights
        
        # 1. Check metrics for resource issues
        metrics = instance_data.get("metrics", {})
        if metrics:
            self._check_resource_thresholds(component, host, metrics)
        
        # 3. Check logs for error patterns
        logs = instance_data.get("logs", {})
        if logs:
            self._check_log_patterns(component, host, logs)
        
        # Also check top-level log fields (legacy structure)
        log_keys = [k for k in instance_data.keys() if "log" in k.lower()]
        for log_key in log_keys:
            log_content = instance_data.get(log_key)
            if log_content:
                log_dict = {log_key: log_content}
                self._check_log_patterns(component, host, log_dict)
        
        # 4. Check for missing/incomplete data (but not collection errors)
        # Note: Collection errors are excluded - they're adapter issues, not infrastructure concerns
        self._check_data_completeness(component, host, instance_data)
    
    def _check_resource_thresholds(
        self, 
        component: str, 
        host: str, 
        metrics: Dict[str, Any]
    ):
        """Check metrics against resource thresholds."""
        
        # CPU
        cpu_percent = self._extract_numeric_metric(metrics, ["cpu_percent", "cpu", "cpu_usage"])
        if cpu_percent is not None and cpu_percent > RESOURCE_THRESHOLDS["high_cpu"]:
            self._add_concern(
                severity="warning",
                category="Resource Exhaustion",
                component=component,
                host=host,
                description=f"High CPU usage: {cpu_percent:.1f}%",
                details={"metric": "cpu_percent", "value": cpu_percent, "threshold": RESOURCE_THRESHOLDS["high_cpu"]}
            )
        
        # Memory
        mem_percent = self._extract_numeric_metric(metrics, ["mem_percent", "memory_percent", "memory", "mem_usage"])
        if mem_percent is not None and mem_percent > RESOURCE_THRESHOLDS["high_memory"]:
            severity = "critical" if mem_percent > 95.0 else "warning"
            self._add_concern(
                severity=severity,
                category="Resource Exhaustion",
                component=component,
                host=host,
                description=f"High memory usage: {mem_percent:.1f}%",
                details={"metric": "mem_percent", "value": mem_percent, "threshold": RESOURCE_THRESHOLDS["high_memory"]}
            )
        
        # Check for disk metrics in nested structures
        if isinstance(metrics, dict):
            for key, value in metrics.items():
                if "disk" in key.lower() and isinstance(value, (int, float)):
                    if value > RESOURCE_THRESHOLDS["high_disk"]:
                        self._add_concern(
                            severity="warning",
                            category="Resource Exhaustion",
                            component=component,
                            host=host,
                            description=f"High {key}: {value:.1f}%",
                            details={"metric": key, "value": value, "threshold": RESOURCE_THRESHOLDS["high_disk"]}
                        )
    
    def _extract_numeric_metric(self, metrics: Dict[str, Any], possible_keys: List[str]) -> Optional[float]:
        """Extract a numeric metric value trying multiple possible keys."""
        if not isinstance(metrics, dict):
            return None
        
        for key in possible_keys:
            value = metrics.get(key)
            if value is not None:
                # Handle nested dicts (e.g., {"cpu": {"percent": 85.0}})
                if isinstance(value, dict):
                    for sub_key in ["percent", "usage", "value"]:
                        sub_value = value.get(sub_key)
                        if sub_value is not None:
                            try:
                                return float(sub_value)
                            except (ValueError, TypeError):
                                continue
                else:
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        continue
        
        return None
    
    def _check_log_patterns(
        self, 
        component: str, 
        host: str, 
        logs: Dict[str, Any]
    ):
        """Check log content for error patterns."""
        
        for log_name, log_content in logs.items():
            if not log_content:
                continue
            
            # Convert log_content to string if needed
            if isinstance(log_content, dict):
                # Check if it's a structured log entry
                content = log_content.get("content", log_content.get("output", ""))
            elif isinstance(log_content, list):
                content = "\n".join(str(line) for line in log_content)
            else:
                content = str(log_content)
            
            if not content:
                continue
            
            # Check each pattern category
            for pattern_name, patterns in CONCERN_PATTERNS.items():
                count = 0
                matched_lines = []
                
                for pattern in patterns:
                    matches = pattern.finditer(content)
                    for match in matches:
                        count += 1
                        # Extract line context
                        line_start = content.rfind('\n', 0, match.start()) + 1
                        line_end = content.find('\n', match.end())
                        if line_end == -1:
                            line_end = len(content)
                        matched_line = content[line_start:line_end].strip()
                        if matched_line and matched_line not in matched_lines:
                            matched_lines.append(matched_line[:200])  # Limit length
                
                if count > 0:
                    # Determine severity based on pattern type
                    severity = self._get_severity_for_pattern(pattern_name, count)
                    
                    self._add_concern(
                        severity=severity,
                        category=self._get_category_for_pattern(pattern_name),
                        component=component,
                        host=host,
                        description=f"{pattern_name.replace('_', ' ').title()}: {count} occurrence(s) in {log_name}",
                        details={
                            "pattern": pattern_name,
                            "log": log_name,
                            "count": count,
                            "sample_lines": matched_lines[:3]  # Show first 3 matches
                        }
                    )
    
    def _check_data_completeness(
        self, 
        component: str, 
        host: str, 
        instance_data: Dict[str, Any]
    ):
        """Check for missing or incomplete data.
        
        Note: We do NOT report on collection errors here - those are adapter issues,
        not infrastructure concerns. Collection errors belong in the Errors tab.
        This only checks if data appears to be missing entirely.
        """
        
        concerns = []
        
        # Check for empty metrics (but don't report if it's due to collection errors)
        metrics = instance_data.get("metrics", {})
        if not metrics or (isinstance(metrics, dict) and len(metrics) == 0):
            # Only report if we don't have collection errors (if errors exist, data might be missing due to adapter issues)
            errors = instance_data.get("errors", [])
            if not errors:
                concerns.append("No metrics available")
        
        # Check for empty logs (but don't report if it's due to collection errors)
        logs = instance_data.get("logs", {})
        log_keys = [k for k in instance_data.keys() if "log" in k.lower() and k not in ["errors"]]
        has_logs = (logs and len(logs) > 0) or len(log_keys) > 0
        
        if not has_logs:
            # Only report if we don't have collection errors
            errors = instance_data.get("errors", [])
            if not errors:
                concerns.append("No logs available")
        
        # Note: We intentionally skip checking the errors array here
        # Collection errors should be shown in the Errors tab, not in Evidence Highlights
        
        # Only add concern if we have missing data AND it's not due to collection errors
        if concerns:
            self._add_concern(
                severity="info",  # Changed from "warning" since missing data without errors is less critical
                category="Missing Data",
                component=component,
                host=host,
                description=f"Missing data: {', '.join(concerns[:2])}",
                details={"issues": concerns}
            )
    
    def _get_severity_for_pattern(self, pattern_name: str, count: int) -> str:
        """Determine severity level based on pattern type and count."""
        critical_patterns = [
            "out_of_memory", "stack_overflow", "oom_killer", 
            "kernel_error", "disk_full"
        ]
        
        if pattern_name in critical_patterns:
            return "critical"
        elif count > 10:  # High frequency of errors
            return "warning"
        else:
            return "info"
    
    def _get_category_for_pattern(self, pattern_name: str) -> str:
        """Map pattern name to concern category."""
        category_map = {
            "out_of_memory": "Application Errors",
            "stack_overflow": "Application Errors",
            "nginx_5xx": "HTTP/Web Server Issues",
            "nginx_4xx": "HTTP/Web Server Issues",
            "tomcat_severe": "Application Errors",
            "tomcat_warning": "Application Errors",
            "oom_killer": "OS/Infrastructure Issues",
            "kernel_error": "OS/Infrastructure Issues",
            "connection_timeout": "Network Issues",
            "connection_refused": "Network Issues",
            "auth_failure": "Security/Authorization Issues",
            "permission_denied": "Security/Authorization Issues",
            "database_error": "Database Issues",
            "port_binding": "OS/Infrastructure Issues",
            "disk_full": "Resource Exhaustion",
        }
        return category_map.get(pattern_name, "Other Issues")
    
    def _add_concern(
        self,
        severity: str,
        category: str,
        component: str,
        host: str,
        description: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Add a concern to the list."""
        concern = {
            "severity": severity,
            "category": category,
            "component": component,
            "host": host,
            "description": description,
            "details": details or {},
            "id": f"{component}_{host}_{len(self.concerns)}"  # Simple ID for navigation
        }
        self.concerns.append(concern)


def extract_concerns(evidence_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convenience function to extract concerns from evidence data.
    
    Args:
        evidence_data: Full evidence JSON structure
        
    Returns:
        List of concern dictionaries
    """
    extractor = ConcernExtractor()
    return extractor.extract_concerns(evidence_data)

