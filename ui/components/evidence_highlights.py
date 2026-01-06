# ui/components/evidence_highlights.py
"""
Evidence Highlights component for displaying auto-detected concerns and issues.

Host/Service-focused view showing what's happening in each component.
"""

import streamlit as st
import re
from typing import List, Dict, Any
from ui.utils.concern_extractor import extract_concerns
from ui.components.evidence_tree import build_evidence_tree


def render_evidence_highlights(evidence_data: Dict[str, Any]):
    """
    Render the Evidence Highlights dashboard focused on Hosts/Services.
    
    Args:
        evidence_data: Full evidence JSON structure
    """
    if not evidence_data:
        st.warning("⚠️ No evidence data available.")
        return
    
    # Extract concerns
    with st.spinner("🔍 Analyzing evidence for issues..."):
        concerns = extract_concerns(evidence_data)
    
    # Build host/service structure
    host_service_data = _build_host_service_structure(evidence_data, concerns)
    
    # Render host/service-focused view
    _render_host_service_view(host_service_data, concerns, evidence_data)


def _build_host_service_structure(evidence_data: Dict[str, Any], concerns: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build structure grouped by Component -> Host/Service with their concerns.
    
    Returns:
        {
            "component_name": {
                "host_name": {
                    "concerns": [list of concerns],
                    "severity": "critical" | "warning" | "ok",
                    "critical_count": int,
                    "warning_count": int,
                    "info_count": int
                }
            }
        }
    """
    structure = {}
    
    # Initialize structure from evidence tree
    evidence_tree = build_evidence_tree(evidence_data)
    
    for component_name, component_data in evidence_tree.items():
        structure[component_name] = {}
        
        for host_name in component_data.keys():
            structure[component_name][host_name] = {
                "concerns": [],
                "severity": "ok",
                "critical_count": 0,
                "warning_count": 0,
                "info_count": 0
            }
    
    # Group concerns by component and host
    for concern in concerns:
        component = concern.get("component", "unknown")
        host = concern.get("host", "unknown")
        severity = concern.get("severity", "info")
        
        if component not in structure:
            structure[component] = {}
        if host not in structure[component]:
            structure[component][host] = {
                "concerns": [],
                "severity": "ok",
                "critical_count": 0,
                "warning_count": 0,
                "info_count": 0
            }
        
        structure[component][host]["concerns"].append(concern)
        
        if severity == "critical":
            structure[component][host]["critical_count"] += 1
        elif severity == "warning":
            structure[component][host]["warning_count"] += 1
        else:
            structure[component][host]["info_count"] += 1
        
        # Update overall severity (critical > warning > ok)
        current_severity = structure[component][host]["severity"]
        if severity == "critical" or (severity == "warning" and current_severity != "critical"):
            structure[component][host]["severity"] = severity
    
    return structure


def _render_host_service_view(
    host_service_data: Dict[str, Any],
    all_concerns: List[Dict[str, Any]],
    evidence_data: Dict[str, Any]
):
    """Render the host-first highlights view."""
    
    # Restructure data: Group by host first, then components
    host_component_data = _restructure_by_host(host_service_data)
    
    # Sort hosts by severity (hosts with issues first)
    sorted_hosts = sorted(
        host_component_data.items(),
        key=lambda x: (
            0 if any(c["severity"] in ["critical", "warning"] for c in x[1].values()) else 1,
            -sum(c["critical_count"] + c["warning_count"] for c in x[1].values())
        )
    )
    
    # Render each host
    for host_name, components in sorted_hosts:
        # Determine host overall status
        host_has_issues = any(
            comp_data["severity"] in ["critical", "warning"]
            for comp_data in components.values()
        )
        
        # Host header
        if host_has_issues:
            total_critical = sum(c["critical_count"] for c in components.values())
            total_warning = sum(c["warning_count"] for c in components.values())
            
            issue_summary = []
            if total_critical > 0:
                issue_summary.append(f'{total_critical} critical')
            if total_warning > 0:
                issue_summary.append(f'{total_warning} warning')
            
            st.markdown(
                f'✅ **{host_name}** <span style="color: #f57c00;">({", ".join(issue_summary)})</span>',
                unsafe_allow_html=True
            )
        else:
            st.markdown(f'✅ **{host_name}**', unsafe_allow_html=True)
        
        # Sort components by severity (issues first)
        sorted_components = sorted(
            components.items(),
            key=lambda x: (
                0 if x[1]["severity"] == "critical" else (1 if x[1]["severity"] == "warning" else 2),
                -(x[1]["critical_count"] + x[1]["warning_count"])
            )
        )
        
        # Separate components with issues from those without
        components_with_issues = [
            (name, data) for name, data in sorted_components 
            if data["severity"] in ["critical", "warning"]
        ]
        components_ok = [
            (name, data) for name, data in sorted_components 
            if data["severity"] == "ok"
        ]
        
        # Render components with issues first
        for component_name, component_data in components_with_issues:
            concerns = component_data["concerns"]
            severity = component_data["severity"]
            critical_count = component_data["critical_count"]
            warning_count = component_data["warning_count"]
            
            issue_summary = []
            if critical_count > 0:
                issue_summary.append(f'{critical_count} critical')
            if warning_count > 0:
                issue_summary.append(f'{warning_count} warning')
            
            status_emoji = "🔴" if severity == "critical" else "🟡"
            
            with st.expander(
                f"📦 **{component_name}** {status_emoji} ({', '.join(issue_summary)})",
                expanded=(severity == "critical")  # Auto-expand critical
            ):
                _render_host_concerns(component_name, host_name, concerns, evidence_data)
        
        # Add separator if there are both issues and OK components
        if components_with_issues and components_ok:
            st.markdown("")  # Small spacing
        
        # Render components without issues
        for component_name, component_data in components_ok:
            st.markdown(f"📦 **{component_name}** ✅ - No issues detected", unsafe_allow_html=True)
    
    # Summary at the bottom
    _render_summary(host_service_data, all_concerns, evidence_data)


def _restructure_by_host(host_service_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Restructure data from component->host to host->component.
    
    Input: {
        "nginx": {
            "local-docker-host": {concerns, severity, ...},
            "ubuntu-node": {concerns, severity, ...}
        },
        "os": {
            "ubuntu-node": {concerns, severity, ...}
        }
    }
    
    Output: {
        "local-docker-host": {
            "nginx": {concerns, severity, ...},
            "os": {concerns, severity, ...}
        },
        "ubuntu-node": {
            "nginx": {concerns, severity, ...},
            "os": {concerns, severity, ...}
        }
    }
    """
    host_component_data = {}
    
    for component_name, component_hosts in host_service_data.items():
        for host_name, host_data in component_hosts.items():
            if host_name not in host_component_data:
                host_component_data[host_name] = {}
            
            # Store component data under host
            host_component_data[host_name][component_name] = host_data
    
    return host_component_data


def _render_host_concerns(component: str, host: str, concerns: List[Dict[str, Any]], evidence_data: Dict[str, Any]):
    """Render concerns for a specific host."""
    
    # Separate by severity
    critical_concerns = [c for c in concerns if c.get("severity") == "critical"]
    warning_concerns = [c for c in concerns if c.get("severity") == "warning"]
    info_concerns = [c for c in concerns if c.get("severity") == "info"]
    
    # Show critical concerns first
    if critical_concerns:
        st.markdown('<span style="color: #d32f2f; font-weight: bold;">🔴 CRITICAL ISSUES:</span>', unsafe_allow_html=True)
        for concern in critical_concerns[:5]:  # Limit to 5 most critical
            _render_concern_line(concern, is_critical=True, component=component, host=host, evidence_data=evidence_data)
        if len(critical_concerns) > 5:
            st.caption(f"... and {len(critical_concerns) - 5} more critical issue(s)")
    
    # Show warning concerns
    if warning_concerns:
        if critical_concerns:
            st.markdown("")  # Spacing
        st.markdown('<span style="color: #f57c00; font-weight: bold;">🟡 WARNINGS:</span>', unsafe_allow_html=True)
        for concern in warning_concerns[:5]:  # Limit to 5 warnings
            _render_concern_line(concern, is_critical=False, component=component, host=host, evidence_data=evidence_data)
        if len(warning_concerns) > 5:
            st.caption(f"... and {len(warning_concerns) - 5} more warning(s)")
    
    # Show info concerns (only if no critical/warning)
    if info_concerns and not critical_concerns and not warning_concerns:
        for concern in info_concerns[:3]:  # Limit to 3 info items
            _render_concern_line(concern, is_critical=False, component=component, host=host, evidence_data=evidence_data)
        if len(info_concerns) > 3:
            st.caption(f"... and {len(info_concerns) - 3} more info item(s)")
    
    # Navigation button
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("View Details", key=f"nav_{component}_{host}", use_container_width=True):
            st.session_state["nav_to_evidence"] = {
                "component": component,
                "host": host
            }
            st.session_state["active_main_tab"] = "evidence"
            st.query_params["tab"] = "evidence"
            st.rerun()


def _render_concern_line(
    concern: Dict[str, Any], 
    is_critical: bool = False,
    component: str = "",
    host: str = "",
    evidence_data: Dict[str, Any] = None
):
    """Render a single concern as a line item with relevant log lines."""
    description = concern.get("description", "")
    category = concern.get("category", "Other")
    
    # Use red color for critical/error concerns
    color = "#d32f2f" if is_critical else "#424242"
    
    # Show category and description
    st.markdown(
        f'<span style="color: {color};">• <strong>{category}:</strong> {description}</span>',
        unsafe_allow_html=True
    )
    
    # Extract and show relevant log lines
    # First try to get sample_lines from concern details (already extracted by concern_extractor)
    details = concern.get("details", {})
    sample_lines = details.get("sample_lines", [])
    
    # If no sample_lines, try to extract from evidence data
    if not sample_lines:
        sample_lines = _extract_log_lines_for_concern(concern, component, host, evidence_data)
    
    # Display log lines if available
    if sample_lines:
        st.markdown('<div style="margin-left: 20px; margin-top: 5px; margin-bottom: 10px;">', unsafe_allow_html=True)
        for line in sample_lines[:5]:  # Show up to 5 log lines
            st.code(line, language="text")
        if len(sample_lines) > 5:
            st.caption(f"... and {len(sample_lines) - 5} more log line(s)")
        st.markdown('</div>', unsafe_allow_html=True)


def _extract_log_lines_for_concern(
    concern: Dict[str, Any],
    component: str,
    host: str,
    evidence_data: Dict[str, Any]
) -> List[str]:
    """
    Extract relevant log lines from evidence data based on concern details.
    
    Returns list of log line strings (raw log content).
    """
    if not evidence_data:
        return []
    
    details = concern.get("details", {})
    log_name = details.get("log", "")
    pattern_name = details.get("pattern", "")
    
    # Try to find the log content in evidence structure
    host_data = evidence_data.get("host", {})
    component_data = host_data.get(component, {})
    instances = component_data.get("instances", [])
    
    for instance in instances:
        if instance.get("name") != host:
            continue
        
        # Look for log content - could be in various places
        log_content = None
        
        # Check if log_name matches a key in instance (e.g., "access_log_tail")
        if log_name and log_name in instance:
            log_value = instance[log_name]
            if isinstance(log_value, dict):
                log_content = log_value.get("content", "")
            elif isinstance(log_value, str):
                log_content = log_value
        # Check logs dictionary
        elif "logs" in instance and isinstance(instance["logs"], dict):
            logs = instance["logs"]
            if log_name in logs:
                log_value = logs[log_name]
                if isinstance(log_value, dict):
                    log_content = log_value.get("content", "")
                elif isinstance(log_value, str):
                    log_content = log_value
        
        # Also check for keys containing "log" (case-insensitive)
        if not log_content:
            for key, value in instance.items():
                if "log" in key.lower() and isinstance(value, (str, dict)):
                    if isinstance(value, dict):
                        log_content = value.get("content", "")
                    else:
                        log_content = value
                    if log_content:
                        break
        
        if log_content:
            # Extract lines that match the pattern or are relevant
            lines = log_content.splitlines() if isinstance(log_content, str) else []
            
            # Filter lines based on pattern if available
            if pattern_name and lines:
                # For common patterns, filter lines
                filtered_lines = []
                for line in lines:
                    line_stripped = line.strip()
                    if not line_stripped:
                        continue
                    
                    # Filter based on pattern type
                    if "nginx_4xx" in pattern_name or "4xx" in pattern_name.lower():
                        # Match 4xx status codes in access log format
                        if re.search(r'"\s*4\d{2}\s+', line_stripped):
                            filtered_lines.append(line_stripped)
                    elif "nginx_5xx" in pattern_name or "5xx" in pattern_name.lower():
                        # Match 5xx status codes
                        if re.search(r'"\s*5\d{2}\s+', line_stripped):
                            filtered_lines.append(line_stripped)
                    elif "error" in pattern_name.lower():
                        if "error" in line_stripped.lower():
                            filtered_lines.append(line_stripped)
                    else:
                        # For other patterns, include the line if it matches keywords
                        filtered_lines.append(line_stripped)
                
                return filtered_lines[:5]  # Return up to 5 matching lines
            else:
                # Return recent lines (last 5)
                return [l.strip() for l in lines[-5:] if l.strip()]
    
    return []


def _sort_by_severity(host_service_data: Dict[str, Any]) -> List[str]:
    """Sort components by severity (components with issues first)."""
    
    def get_component_severity(component_name: str) -> tuple:
        """Return (has_critical, has_warning, total_issues) for sorting."""
        hosts = host_service_data[component_name]
        has_critical = any(h["severity"] == "critical" for h in hosts.values())
        has_warning = any(h["severity"] == "warning" for h in hosts.values())
        total_issues = sum(h["critical_count"] + h["warning_count"] for h in hosts.values())
        return (not has_critical, not has_warning, -total_issues)
    
    return sorted(host_service_data.keys(), key=get_component_severity)


def _render_summary(
    host_service_data: Dict[str, Any],
    all_concerns: List[Dict[str, Any]],
    evidence_data: Dict[str, Any]
):
    """Render summary statistics at the bottom."""
    
    st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">📊 Summary</h3>', unsafe_allow_html=True)
    
    # Calculate statistics
    total_hosts = sum(len(hosts) for hosts in host_service_data.values())
    hosts_with_issues = sum(
        1
        for hosts in host_service_data.values()
        for host_data in hosts.values()
        if host_data["severity"] in ["critical", "warning"]
    )
    hosts_critical = sum(
        1
        for hosts in host_service_data.values()
        for host_data in hosts.values()
        if host_data["severity"] == "critical"
    )
    hosts_warning = sum(
        1
        for hosts in host_service_data.values()
        for host_data in hosts.values()
        if host_data["severity"] == "warning"
    )
    
    critical_concerns = [c for c in all_concerns if c.get("severity") == "critical"]
    warning_concerns = [c for c in all_concerns if c.get("severity") == "warning"]
    
    # Get evidence metadata
    metadata = evidence_data.get("metadata", {})
    collected_at = metadata.get("collected_at", "N/A")
    
    # Format collected date nicely
    try:
        from datetime import datetime
        if collected_at != "N/A":
            dt = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
            collected_at_formatted = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            collected_at_formatted = collected_at
    except:
        collected_at_formatted = collected_at
    
    # Create two columns for side-by-side layout
    summary_col, status_col = st.columns(2)
    
    # Summary table (vertical format) - Left column
    with summary_col:
        st.markdown("**Summary:**")
        # Create vertical label-value pairs
        summary_data = [
            ("Total Hosts/Services", total_hosts),
            ("Hosts with Issues", hosts_with_issues),
            ("Critical Issues", len(critical_concerns)),
            ("Warnings", len(warning_concerns)),
            ("Components", len(host_service_data))
        ]
        # Use HTML table for proper formatting
        summary_html = "<table style='border-collapse: collapse; width: 100%;'>"
        for label, value in summary_data:
            summary_html += f"<tr><td style='padding: 4px 8px; text-align: left;'>{label}</td><td style='padding: 4px 8px; text-align: right; font-weight: bold;'>{value}</td></tr>"
        summary_html += "</table>"
        st.markdown(summary_html, unsafe_allow_html=True)
    
    # Status Breakdown - Right column
    with status_col:
        st.markdown("**Status Breakdown:**")
        hosts_ok = total_hosts - hosts_with_issues
        status_data = [
            ("🔴 Critical", hosts_critical),
            ("🟡 Warning", hosts_warning),
            ("✅ OK", hosts_ok),
            ("Collected.", collected_at_formatted)
        ]
        # Use HTML table for proper formatting
        status_html = "<table style='border-collapse: collapse; width: 100%;'>"
        for label, value in status_data:
            status_html += f"<tr><td style='padding: 4px 8px; text-align: left;'>{label}</td><td style='padding: 4px 8px; text-align: right; font-weight: bold;'>{value}</td></tr>"
        status_html += "</table>"
        st.markdown(status_html, unsafe_allow_html=True)
