# ui/components/llm_input_review.py
"""
LLM Input Review component for transparency and security.

Shows exactly what will be sent to LLM, with masking and filtering options.
"""

import streamlit as st
import json
import pandas as pd
import re
import sys
import copy
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from ui.utils.data_masking import mask_evidence, DataMasker
from ui.utils.log_filter import filter_logs
from ui.components.evidence_tree import build_evidence_tree

# Add repo root to path for imports
BASE = Path(__file__).resolve().parents[2]
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

try:
    from llm.token_estimator import estimate_tokens, estimate_prompt_tokens
    from config.config_loader import ConfigLoader
    TOKEN_ESTIMATOR_AVAILABLE = True
except ImportError:
    TOKEN_ESTIMATOR_AVAILABLE = False


def render_llm_input_review(evidence_data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Render LLM Input Review tab and return prepared evidence for LLM.
    
    Args:
        evidence_data: Full evidence JSON structure
        
    Returns:
        Tuple of (prepared_evidence_for_llm, metadata_dict)
        Returns (None, {}) if user hasn't configured yet
    """
    if not evidence_data:
        st.warning("⚠️ No evidence data available.")
        return None, {}
    
    # Initialize session state for review settings
    if "llm_review_mask_enabled" not in st.session_state:
        st.session_state["llm_review_mask_enabled"] = True
    if "llm_review_filter_enabled" not in st.session_state:
        st.session_state["llm_review_filter_enabled"] = True
    if "llm_review_aggressiveness" not in st.session_state:
        st.session_state["llm_review_aggressiveness"] = "medium"
    if "llm_review_selected_components" not in st.session_state:
        # Initialize with all components selected
        st.session_state["llm_review_selected_components"] = _get_all_components(evidence_data)
    
    # Section 1: Security & Masking Status
    st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">🔒 Security & Masking Status</h3>', unsafe_allow_html=True)
    
    mask_enabled = st.session_state["llm_review_mask_enabled"]
    filter_enabled = st.session_state["llm_review_filter_enabled"]
    
    col1, col2 = st.columns(2)
    with col1:
        if mask_enabled:
            st.success("✅ **Masking Enabled** - Sensitive data will be masked")
        else:
            st.error("⚠️ **Raw Data Mode** - Unmasked data will be sent to LLM")
    
    with col2:
        if filter_enabled:
            st.info(f"✅ **Log Filtering Enabled** - Aggressiveness: {st.session_state['llm_review_aggressiveness'].title()}")
        else:
            st.info("ℹ️ **Log Filtering Disabled** - All logs will be included")
    
    if not mask_enabled:
        st.warning("⚠️ **WARNING**: You are about to send raw, unmasked data to the LLM. This may include sensitive information like IPs, tokens, passwords, and other PII.")
    
    # Section 2: Filtering & Masking Controls
    st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">⚙️ Filtering & Masking Controls</h3>', unsafe_allow_html=True)
    
    control_cols = st.columns(2)
    
    with control_cols[0]:
        st.markdown("**Mask sensitive data (IPs, tokens, passwords, emails) before sending to LLM**")
        mask_enabled = st.checkbox(
            "Enable data masking",
            value=st.session_state["llm_review_mask_enabled"],
            key="mask_checkbox",
            help="Mask sensitive data like IPs, tokens, passwords. RECOMMENDED: Keep this enabled."
        )
        st.session_state["llm_review_mask_enabled"] = mask_enabled
        
        if not mask_enabled:
            raw_data_warning = st.checkbox(
                "⚠️ I understand I'm sending raw data",
                value=False,
                key="raw_data_warning_checkbox",
                help="You must confirm to send raw data"
            )
            if not raw_data_warning:
                st.stop()  # Prevent proceeding without confirmation
    
    with control_cols[1]:
        st.markdown("**Filter out normal log lines to reduce payload size and focus on relevant errors**")
        # Create columns for checkbox and radio buttons side by side
        filter_col1, filter_col2 = st.columns([1, 2])
        with filter_col1:
            filter_enabled = st.checkbox(
                "Apply log filtering",
                value=st.session_state["llm_review_filter_enabled"],
                key="filter_checkbox",
                help="Remove normal log lines to reduce payload size"
            )
            st.session_state["llm_review_filter_enabled"] = filter_enabled
        
        with filter_col2:
            if filter_enabled:
                aggressiveness = st.radio(
                    "Filtering aggressiveness",
                    ["light", "medium", "aggressive"],
                    index=["light", "medium", "aggressive"].index(st.session_state["llm_review_aggressiveness"]),
                    key="aggressiveness_radio",
                    help="Light: Remove only obvious noise. Medium: Balanced. Aggressive: Remove most normal logs.",
                    horizontal=True
                )
                st.session_state["llm_review_aggressiveness"] = aggressiveness
    
    # Section 3: Component Selection
    all_components = _get_all_components(evidence_data)
    selected_components = st.session_state["llm_review_selected_components"]
    
    # Header
    st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">📦 Component Selection for Analysis</h3>', unsafe_allow_html=True)
    
    # Component checkboxes in columns
    num_cols = 3
    cols = st.columns(num_cols)
    
    for idx, component_info in enumerate(all_components):
        component_name = component_info["name"]
        component_count = component_info["count"]
        
        with cols[idx % num_cols]:
            is_selected = component_name in selected_components
            if st.checkbox(
                f"{component_name} ({component_count})",
                value=is_selected,
                key=f"component_{component_name}"
            ):
                if component_name not in selected_components:
                    selected_components.append(component_name)
            else:
                if component_name in selected_components:
                    selected_components.remove(component_name)
    
    st.session_state["llm_review_selected_components"] = selected_components
    
    # Select/Deselect all button at bottom right
    all_selected = len(selected_components) == len(all_components)
    button_col1, button_col2 = st.columns([4, 1])
    with button_col1:
        st.write("")  # Empty space
    with button_col2:
        button_text = "Deselect All" if all_selected else "Select All"
        if st.button(button_text, key="toggle_select_all", use_container_width=True):
            if all_selected:
                st.session_state["llm_review_selected_components"] = []
            else:
                # Extract just the component names from all_components list
                st.session_state["llm_review_selected_components"] = [comp["name"] for comp in all_components]
            st.rerun()
    
    if not selected_components:
        st.warning("⚠️ Please select at least one component to include.")
        st.stop()
    
    # Section 4: Prepare evidence based on selections
    prepared_evidence = _prepare_evidence(
        evidence_data,
        selected_components,
        mask_enabled,
        filter_enabled,
        st.session_state["llm_review_aggressiveness"]
    )
    
    # Section 5: Payload Preview
    st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">📄 Payload Preview</h3>', unsafe_allow_html=True)
    
    preview_tabs = st.tabs(["Summary", "Structured View", "Raw JSON", "Raw Logs", "Delta View"])
    
    # Calculate payload statistics
    original_size = len(json.dumps(evidence_data, indent=2))
    prepared_size = len(json.dumps(prepared_evidence, indent=2))
    size_reduction = ((original_size - prepared_size) / original_size * 100) if original_size > 0 else 0
    
    # Estimate token count using accurate token estimator (on prepared evidence)
    model_name = st.session_state.get("llm_model", "gpt-4o-mini")
    estimated_tokens_prepared = 0
    if TOKEN_ESTIMATOR_AVAILABLE:
        try:
            prepared_json = json.dumps(prepared_evidence, indent=2)
            estimated_tokens_prepared = estimate_prompt_tokens(prepared_json, model_name)
        except Exception as e:
            # Fallback to rough estimate
            estimated_tokens_prepared = prepared_size / 4
            st.warning(f"Token estimation failed, using rough estimate: {e}")
    else:
        # Fallback to rough estimate
        estimated_tokens_prepared = prepared_size / 4
    
    # Get budget information and apply evidence reduction
    budget_info = {}
    reduced_evidence = prepared_evidence
    reduced_size = prepared_size
    estimated_tokens = estimated_tokens_prepared
    reduction_metadata = None
    
    if TOKEN_ESTIMATOR_AVAILABLE:
        try:
            config_loader = ConfigLoader()
            budget_config = config_loader.load_reasoning_budget()
            max_tokens = budget_config.get("reasoning_budget", {}).get("max_tokens", 180000)
            model_overrides = budget_config.get("reasoning_budget", {}).get("model_overrides", {})
            if model_name in model_overrides:
                max_tokens = model_overrides[model_name].get("max_tokens", max_tokens)
            
            # Apply evidence reduction to preview what will actually be sent
            try:
                from llm.evidence_reducer import reduce_evidence
                from datetime import datetime
                
                # Extract issue_time from evidence
                issue_time = None
                metadata = prepared_evidence.get("metadata", {})
                issue_time_str = metadata.get("issue_time")
                if issue_time_str:
                    try:
                        issue_time = datetime.fromisoformat(issue_time_str.replace('Z', '+00:00'))
                    except:
                        pass
                
                # Apply reduction (use deep copy to avoid modifying prepared_evidence)
                reduced_evidence = reduce_evidence(copy.deepcopy(prepared_evidence), budget_config, issue_time)
                reduced_json = json.dumps(reduced_evidence, indent=2, sort_keys=True)
                reduced_size = len(reduced_json)
                
                # Recalculate tokens on reduced evidence
                estimated_tokens = estimate_prompt_tokens(reduced_json, model_name)
                
                # Get reduction metadata
                if "_reduction_metadata" in reduced_evidence:
                    reduction_metadata = reduced_evidence["_reduction_metadata"]
                
            except Exception as e:
                # If reduction fails, use prepared evidence
                st.warning(f"Could not preview evidence reduction: {e}")
                reduced_evidence = prepared_evidence
                reduced_size = prepared_size
                estimated_tokens = estimated_tokens_prepared
            
            budget_info = {
                "max_tokens": max_tokens,
                "under_budget": estimated_tokens <= max_tokens,
                "percent_used": (estimated_tokens / max_tokens * 100) if max_tokens > 0 else 0
            }
        except Exception as e:
            st.warning(f"Could not load budget configuration: {e}")
    
    # Tab 1: Summary
    with preview_tabs[0]:
        st.markdown("**Payload Statistics**")
        
        # Create a table for payload statistics
        payload_stats_data = [
            {"Metric": "Original Size", "Value": f"{original_size / 1024:.1f} KB"},
            {"Metric": "Prepared Size", "Value": f"{prepared_size / 1024:.1f} KB ({size_reduction:.1f}% reduction)"},
        ]
        
        # Add reduction preview if available
        if reduced_size != prepared_size:
            reduction_from_prepared = ((prepared_size - reduced_size) / prepared_size * 100) if prepared_size > 0 else 0
            total_reduction = ((original_size - reduced_size) / original_size * 100) if original_size > 0 else 0
            payload_stats_data.append({
                "Metric": "After Reduction",
                "Value": f"{reduced_size / 1024:.1f} KB ({reduction_from_prepared:.1f}% from prepared, {total_reduction:.1f}% total)"
            })
        
        payload_stats_data.extend([
            {"Metric": "Estimated Tokens", "Value": f"{estimated_tokens:.0f}"},
            {"Metric": "Components", "Value": str(len(selected_components))}
        ])
        
        # Add budget information if available
        if budget_info:
            max_tokens = budget_info.get("max_tokens", 0)
            under_budget = budget_info.get("under_budget", True)
            percent_used = budget_info.get("percent_used", 0)
            
            status_icon = "✅" if under_budget else "⚠️"
            status_text = "Under Budget" if under_budget else "Over Budget"
            payload_stats_data.append({
                "Metric": "Budget Status",
                "Value": f"{status_icon} {status_text} ({estimated_tokens:.0f}/{max_tokens:.0f}, {percent_used:.1f}%)"
            })
        
        # Show reduction details if available
        if reduction_metadata:
            reductions_applied = reduction_metadata.get("reductions_applied", [])
            if reductions_applied:
                st.markdown("**Reduction Details**")
                st.info(f"Evidence will be reduced before sending to LLM. {len(reductions_applied)} reduction(s) will be applied.")
                with st.expander("View reduction details"):
                    for i, reduction in enumerate(reductions_applied[:10], 1):
                        st.text(f"{i}. {reduction}")
                    if len(reductions_applied) > 10:
                        st.text(f"... and {len(reductions_applied) - 10} more")
        
        # Convert to DataFrame and display without headers/index
        df_stats = pd.DataFrame(payload_stats_data)
        st.dataframe(
            df_stats,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Metric": st.column_config.TextColumn("", width="medium"),
                "Value": st.column_config.TextColumn("", width="large")
            }
        )
        
        # Show masking stats if available
        if "masking_stats" in st.session_state:
            stats = st.session_state["masking_stats"]
            st.markdown("**Masking Statistics**")
            
            # Create a table for masking statistics
            masking_stats_data = [
                {"Metric": "IPs Masked", "Value": str(stats.get("ips_masked", 0))},
                {"Metric": "Tokens Masked", "Value": str(stats.get("tokens_masked", 0))},
                {"Metric": "Emails Masked", "Value": str(stats.get("emails_masked", 0))},
                {"Metric": "Paths Masked", "Value": str(stats.get("paths_masked", 0))},
                {"Metric": "Other Masked", "Value": str(stats.get("other_masked", 0))}
            ]
            
            # Convert to DataFrame and display without headers/index
            df_masking = pd.DataFrame(masking_stats_data)
            st.dataframe(
                df_masking,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Metric": st.column_config.TextColumn("", width="medium"),
                    "Value": st.column_config.TextColumn("", width="medium")
                }
            )
        
        # Show filtering stats if available
        if "filter_stats" in st.session_state:
            stats = st.session_state["filter_stats"]
            st.markdown("**Filtering Statistics**")
            
            # Calculate removal percentage
            removal_percent = (stats.get("removed_lines", 0) / stats.get("total_lines", 1)) * 100 if stats.get("total_lines", 0) > 0 else 0
            
            # Create a table for filtering statistics
            filtering_stats_data = [
                {"Metric": "Total Lines", "Value": str(stats.get("total_lines", 0))},
                {"Metric": "Kept Lines", "Value": str(stats.get("kept_lines", 0))},
                {"Metric": "Removed Lines", "Value": str(stats.get("removed_lines", 0))},
                {"Metric": "Removal Rate", "Value": f"{removal_percent:.1f}%"}
            ]
            
            # Convert to DataFrame and display without headers/index
            df_filtering = pd.DataFrame(filtering_stats_data)
            st.dataframe(
                df_filtering,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Metric": st.column_config.TextColumn("", width="medium"),
                    "Value": st.column_config.TextColumn("", width="medium")
                }
            )
    
    # Tab 2: Structured View
    with preview_tabs[1]:
        st.markdown("**Evidence Structure**")
        
        # Build tree view
        evidence_tree = build_evidence_tree(prepared_evidence)
        
        for component_name in sorted(evidence_tree.keys()):
            if component_name not in selected_components:
                continue
            
            with st.expander(f"📦 {component_name}"):
                component_data = evidence_tree[component_name]
                for host_name in sorted(component_data.keys()):
                    instance_data = component_data[host_name]
                    
                    # Show instance summary
                    st.markdown(f"**{host_name}**")
                    
                    # Check what data is available
                    has_metrics = bool(instance_data.get("metrics"))
                    has_logs = bool(instance_data.get("logs"))
                    has_errors = bool(instance_data.get("errors"))
                    
                    status_cols = st.columns(3)
                    with status_cols[0]:
                        st.caption(f"Metrics: {'✅' if has_metrics else '❌'}")
                    with status_cols[1]:
                        st.caption(f"Logs: {'✅' if has_logs else '❌'}")
                    with status_cols[2]:
                        st.caption(f"Errors: {'⚠️' if has_errors else '✅'}")
    
    # Tab 3: Raw JSON Preview
    with preview_tabs[2]:
        st.markdown("**Raw JSON Payload** (what will be sent to LLM)")
        
        # Show JSON with syntax highlighting
        json_text = json.dumps(prepared_evidence, indent=2)
        st.code(json_text, language="json")
        
        # Download button
        st.download_button(
            "Download Prepared Payload (JSON)",
            data=json_text,
            file_name="llm_input_payload.json",
            mime="application/json"
        )
    
    # Tab 4: Raw Logs
    with preview_tabs[3]:
        _render_raw_logs_tab(prepared_evidence, selected_components)
    
    # Tab 5: Delta View
    with preview_tabs[4]:
        _render_delta_view(evidence_data, prepared_evidence, mask_enabled, filter_enabled)
    
    # Return prepared evidence for LLM
    metadata = {
        "mask_enabled": mask_enabled,
        "filter_enabled": filter_enabled,
        "aggressiveness": st.session_state["llm_review_aggressiveness"],
        "selected_components": selected_components,
        "original_size": original_size,
        "prepared_size": prepared_size,
        "size_reduction_percent": size_reduction,
    }
    
    return prepared_evidence, metadata


def _prepare_evidence(
    evidence_data: Dict[str, Any],
    selected_components: List[str],
    mask_enabled: bool,
    filter_enabled: bool,
    aggressiveness: str
) -> Dict[str, Any]:
    """Prepare evidence by filtering components, masking, and filtering logs."""
    import copy
    
    # Start with a copy
    prepared = copy.deepcopy(evidence_data)
    
    # Filter components
    host_data = prepared.get("host", {})
    filtered_host_data = {}
    
    for component_name, component_data in host_data.items():
        if component_name in selected_components:
            filtered_host_data[component_name] = component_data
    
    prepared["host"] = filtered_host_data
    
    # Also filter containers if present
    containers = prepared.get("containers", {})
    if containers:
        # Keep all containers for now (could add filtering later)
        pass
    
    # Apply log filtering
    if filter_enabled:
        issue_time = prepared.get("metadata", {}).get("issue_time")
        prepared, filter_stats = filter_logs(prepared, aggressiveness, issue_time)
        st.session_state["filter_stats"] = filter_stats
    
    # Apply masking
    if mask_enabled:
        prepared, masking_stats = mask_evidence(prepared, mask_sensitive=True)
        st.session_state["masking_stats"] = masking_stats
    
    return prepared


def _get_all_components(evidence_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get list of all components with their counts."""
    components = []
    
    host_data = evidence_data.get("host", {})
    for component_name, component_data in host_data.items():
        if isinstance(component_data, dict):
            instances = component_data.get("instances", [])
            components.append({
                "name": component_name,
                "count": len(instances)
            })
    
    return components


def _render_raw_logs_tab(prepared_evidence: Dict[str, Any], selected_components: List[str]):
    """
    Render raw logs from prepared evidence in a readable format.
    
    Extracts and displays all log content from the evidence structure,
    organized by component and host.
    """
    st.markdown("**Raw Logs** (readable log content from evidence)")
    st.caption("This view shows all log content extracted from the evidence, organized by component and host.")
    
    host_data = prepared_evidence.get("host", {})
    
    if not host_data:
        st.info("ℹ️ No host data found in evidence.")
        return
    
    # Track if we found any logs
    found_logs = False
    
    # Iterate through components
    for component_name in sorted(host_data.keys()):
        if component_name not in selected_components:
            continue
        
        component_data = host_data.get(component_name, {})
        instances = component_data.get("instances", [])
        
        # Fall back to findings structure (legacy)
        if not instances:
            findings = component_data.get("findings", {})
            for finding_name, finding_data in findings.items():
                if isinstance(finding_data, dict):
                    instances = finding_data.get("instances", [])
                    if instances:
                        break
        
        if not instances:
            continue
        
        # Use header instead of expander to avoid nested expanders
        st.markdown(f"### 📦 {component_name}")
        
        for instance in instances:
            host_name = instance.get("name", "unknown")
            
            # Collect all logs from this instance
            instance_logs = []
            
            # Check all keys for log content
            for key, value in instance.items():
                # Skip non-log keys
                if key in ["name", "metrics", "errors", "findings"]:
                    continue
                
                log_content = None
                log_type = key
                
                if isinstance(value, dict):
                    # Check if it's a log structure with "content"
                    if "content" in value:
                        log_content = value.get("content", "")
                    elif "lines" in value:
                        log_content = "\n".join(value.get("lines", []))
                    else:
                        # Try to extract string content from dict
                        for v in value.values():
                            if isinstance(v, str) and len(v) > 50:  # Likely log content
                                log_content = v
                                break
                elif isinstance(value, str) and len(value) > 50:
                    log_content = value
            
                if log_content:
                    instance_logs.append({
                        "log_name": log_type,
                        "content": log_content
                    })
            
            # Also check logs dictionary
            logs = instance.get("logs", {})
            if isinstance(logs, dict):
                for log_name, log_value in logs.items():
                    log_content = None
                    if isinstance(log_value, dict):
                        log_content = log_value.get("content", "")
                    elif isinstance(log_value, str):
                        log_content = log_value
                    
                    if log_content:
                        # Check if we already added this log
                        if not any(l["log_name"] == log_name for l in instance_logs):
                            instance_logs.append({
                                "log_name": log_name,
                                "content": log_content
                            })
            
            # Display logs for this host
            if instance_logs:
                found_logs = True
                st.markdown(f"**{host_name}**")
                
                for log_entry in instance_logs:
                    log_name = log_entry["log_name"]
                    log_content = log_entry["content"]
                    
                    # Count lines
                    lines = log_content.splitlines() if log_content else []
                    line_count = len(lines)
                    
                    with st.expander(f"📄 {log_name} ({line_count} line(s))"):
                        if log_content:
                            # Display as code block for readability
                            st.code(log_content, language="text", line_numbers=True)
                            
                            # Show statistics
                            st.caption(f"Total characters: {len(log_content)} | Lines: {line_count}")
                        else:
                            st.info("Empty log content")
                
                st.markdown("---")
    
    if not found_logs:
        st.info("ℹ️ No log content found in the prepared evidence.")


def _render_delta_view(
    original: Dict[str, Any],
    prepared: Dict[str, Any],
    mask_enabled: bool,
    filter_enabled: bool
):
    """Render delta view showing summary of changes applied to evidence."""
    
    if not mask_enabled and not filter_enabled:
        st.info("ℹ️ No changes applied - original evidence will be sent as-is.")
        return
    
    # Show masking summary if enabled
    if mask_enabled and "masking_stats" in st.session_state:
        stats = st.session_state["masking_stats"]
        total_masked = (
            stats.get("ips_masked", 0) +
            stats.get("tokens_masked", 0) +
            stats.get("emails_masked", 0) +
            stats.get("paths_masked", 0) +
            stats.get("other_masked", 0)
        )
        
        if total_masked > 0:
            st.markdown("**🔒 Data Masking Summary**")
            masking_data = []
            if stats.get("ips_masked", 0) > 0:
                masking_data.append({"Type": "IP Addresses", "Count": str(stats.get("ips_masked", 0))})
            if stats.get("tokens_masked", 0) > 0:
                masking_data.append({"Type": "Tokens/Keys", "Count": str(stats.get("tokens_masked", 0))})
            if stats.get("emails_masked", 0) > 0:
                masking_data.append({"Type": "Email Addresses", "Count": str(stats.get("emails_masked", 0))})
            if stats.get("paths_masked", 0) > 0:
                masking_data.append({"Type": "File Paths", "Count": str(stats.get("paths_masked", 0))})
            if stats.get("other_masked", 0) > 0:
                masking_data.append({"Type": "Other Sensitive Data", "Count": str(stats.get("other_masked", 0))})
            
            if masking_data:
                df_masking = pd.DataFrame(masking_data)
                st.dataframe(
                    df_masking,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Type": st.column_config.TextColumn("", width="medium"),
                        "Count": st.column_config.TextColumn("", width="small")
                    }
                )
                st.success(f"✅ **Total items masked: {total_masked}**")
            else:
                st.info("ℹ️ Masking enabled but no sensitive data detected.")
        else:
            st.info("ℹ️ Masking enabled but no sensitive data found to mask.")
    
    # Show filtering summary if enabled
    if filter_enabled and "filter_stats" in st.session_state:
        stats = st.session_state["filter_stats"]
        total_lines = stats.get("total_lines", 0)
        kept_lines = stats.get("kept_lines", 0)
        removed_lines = stats.get("removed_lines", 0)
        
        if total_lines > 0:
            st.markdown("**📊 Log Filtering Summary**")
            removal_percent = (removed_lines / total_lines * 100) if total_lines > 0 else 0
            
            filtering_data = [
                {"Metric": "Total Log Lines", "Count": f"{total_lines:,}"},
                {"Metric": "Lines Kept", "Count": f"{kept_lines:,}"},
                {"Metric": "Lines Removed", "Count": f"{removed_lines:,}"},
                {"Metric": "Removal Rate", "Count": f"{removal_percent:.1f}%"}
            ]
            
            df_filtering = pd.DataFrame(filtering_data)
            st.dataframe(
                df_filtering,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Metric": st.column_config.TextColumn("", width="medium"),
                    "Count": st.column_config.TextColumn("", width="small")
                }
            )
        else:
            st.info("ℹ️ Filtering enabled but no log lines found to filter.")
    
    # Show size comparison
    original_size = len(json.dumps(original, indent=2))
    prepared_size = len(json.dumps(prepared, indent=2))
    size_reduction = ((original_size - prepared_size) / original_size * 100) if original_size > 0 else 0
    
    st.markdown("**📏 Size Comparison**")
    size_data = [
        {"Metric": "Original Size", "Value": f"{original_size / 1024:.1f} KB"},
        {"Metric": "Prepared Size", "Value": f"{prepared_size / 1024:.1f} KB"},
        {"Metric": "Size Reduction", "Value": f"{size_reduction:.1f}%"}
    ]
    df_size = pd.DataFrame(size_data)
    st.dataframe(
        df_size,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Metric": st.column_config.TextColumn("", width="medium"),
            "Value": st.column_config.TextColumn("", width="small")
        }
    )
    
    # Show component comparison
    original_components = set(original.get("host", {}).keys())
    prepared_components = set(prepared.get("host", {}).keys())
    removed_components = original_components - prepared_components
    
    if removed_components:
        st.warning(f"⚠️ **Components excluded:** {', '.join(sorted(removed_components))}")
    else:
        st.success(f"✅ **All {len(prepared_components)} components included**")
    
    # Show actual masking examples side by side
    if mask_enabled:
        st.markdown("**🔍 Masking Examples (Before → After)**")
        examples = _find_masking_examples(original, prepared)
        
        if examples:
            # Limit to first 10 examples to keep it readable
            display_examples = examples[:10]
            for i, example in enumerate(display_examples, 1):
                with st.expander(f"Example {i}: {example['type']}", expanded=(i <= 3)):
                    col_before, col_after = st.columns(2)
                    with col_before:
                        st.markdown("**Original:**")
                        st.code(example['original'], language="text")
                    with col_after:
                        st.markdown("**Masked:**")
                        st.code(example['masked'], language="text")
                    if example.get('context'):
                        st.caption(f"📍 Context: {example['context']}")
            
            if len(examples) > 10:
                st.info(f"ℹ️ Showing 10 of {len(examples)} masking examples. More examples available in the prepared evidence.")
        else:
            st.info("ℹ️ No masking examples found (structure may have changed or no sensitive data detected).")


def _find_masking_examples(original: Dict[str, Any], prepared: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Find actual examples of masked data by comparing original and prepared evidence.
    
    Returns list of dicts with keys: type, original, masked, context
    """
    examples = []
    
    def compare_values(orig_obj: Any, prep_obj: Any, path: str = ""):
        """Recursively compare original and prepared to find masked values."""
        if isinstance(orig_obj, dict) and isinstance(prep_obj, dict):
            for key in orig_obj.keys():
                if key not in prep_obj:
                    continue
                
                orig_val = orig_obj[key]
                prep_val = prep_obj[key]
                current_path = f"{path}.{key}" if path else key
                
                # Skip metadata fields
                if key in ["session_id", "collected_at", "saved_at", "environment"]:
                    continue
                
                if isinstance(orig_val, str) and isinstance(prep_val, str):
                    # Check if value changed and contains masking indicators
                    if orig_val != prep_val:
                        # Check for common masking patterns
                        if "XX.XX" in prep_val or "masked" in prep_val.lower() or "***" in prep_val:
                            # Determine type based on patterns in masked value
                            mask_type = "Unknown"
                            if "XX.XX" in prep_val or (orig_val != prep_val and re.search(r"\d+\.\d+", orig_val)):
                                mask_type = "IP Address"
                            elif "masked_jwt_token" in prep_val or "token" in prep_val.lower() or "***" in prep_val and len(orig_val) > 16:
                                mask_type = "Token/Key"
                            elif "@" in orig_val and "@***" in prep_val:
                                mask_type = "Email"
                            elif "/" in orig_val and "***" in prep_val:
                                mask_type = "File Path"
                            else:
                                mask_type = "Sensitive Data"
                            
                            # Extract surrounding context (limit length)
                            context_parts = current_path.split(".")[-3:]
                            context = " → ".join(context_parts)
                            
                            # Truncate long strings for display (show first 150 chars)
                            orig_display = orig_val[:150] + "..." if len(orig_val) > 150 else orig_val
                            prep_display = prep_val[:150] + "..." if len(prep_val) > 150 else prep_val
                            
                            examples.append({
                                "type": mask_type,
                                "original": orig_display,
                                "masked": prep_display,
                                "context": context
                            })
                elif isinstance(orig_val, (dict, list)) and isinstance(prep_val, (dict, list)):
                    compare_values(orig_val, prep_val, current_path)
        
        elif isinstance(orig_obj, list) and isinstance(prep_obj, list):
            for idx in range(min(len(orig_obj), len(prep_obj))):
                compare_values(orig_obj[idx], prep_obj[idx], f"{path}[{idx}]")
    
    # Start comparison
    compare_values(original, prepared)
    
    return examples

