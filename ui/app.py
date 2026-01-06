# ui/app.py
"""
GRCAI Multi-Host RCA UI - Streamlit Application

6-Tab Interface:
1. Environment - Overview and topology
2. Evidence - Forensic analysis tool with tree navigation
3. Evidence Highlights - Auto-detected issues and concerns dashboard
4. Evidence Review - Transparency and security review before sending to LLM
5. Incident Report - Quick, actionable report for immediate incident response
6. Full RCA - Complete root cause analysis report
"""

import streamlit as st
from datetime import datetime, date, time, timezone
from pathlib import Path
import json
import sys
import os
import yaml
import pandas as pd
import re

# Make GRCAI modules importable
BASE = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE))

from orchestrator.session_orchestrator import SessionOrchestrator
from client.rca_client import get_reasoning_client
from config.config_loader import ConfigLoader
from ui.components.evidence_tree import build_evidence_tree, get_evidence_summary
from ui.components.metrics_viewer import format_metrics_table
from ui.components.log_viewer import format_log_entry
from ui.components.evidence_highlights import render_evidence_highlights
from ui.components.llm_input_review import render_llm_input_review
from ui.components.multipass_progress import create_progress_tracker
from ui.utils.helpers import safe_get, load_evidence_json


# -------------------------------------------------------------
# SAFE ERROR ACCESSOR
# -------------------------------------------------------------
def safe_error_data(error, component=None, host=None):
    """Normalize error into a dict shape to avoid UI crashes."""
    if isinstance(error, dict):
        err = error.copy()
    else:
        # String error - convert to dict
        error_str = str(error) if error else "Unknown error"
        err = {
            "type": "error",
            "stage": "unknown",
            "message": error_str
        }

    # Always include component/host if provided
    if component:
        err["component"] = component
    if host:
        err["host"] = host
    
    # Ensure all required keys exist with defaults
    err.setdefault("type", "error")
    err.setdefault("stage", "unknown")
    err.setdefault("message", "No error message")

    return err

# ----------------------------------------------------------------------
# PAGE CONFIG
# ----------------------------------------------------------------------
st.set_page_config(page_title="GRCAI - RCA Assistant", layout="wide")

# Main title with smaller font (reduced by 60-70% - using h4 instead of h1)
st.markdown('#### gRCA*i* – *guided RCA intelligence*')

# Initialize session state
if "evidence_file" not in st.session_state:
    st.session_state["evidence_file"] = None
if "selected_tree_node" not in st.session_state:
    st.session_state["selected_tree_node"] = None
if "evidence_data" not in st.session_state:
    st.session_state["evidence_data"] = None
if "active_main_tab" not in st.session_state:
    st.session_state["active_main_tab"] = None
if "selected_model" not in st.session_state:
    st.session_state["selected_model"] = "gpt-4o-mini"
if "selected_env" not in st.session_state:
    st.session_state["selected_env"] = None

# Check query parameters for tab persistence
query_params = st.query_params
if "tab" in query_params:
    st.session_state["active_main_tab"] = query_params["tab"]

# ----------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------
def save_feedback_to_json(evidence_file, feedback_data):
    """
    Save feedback data to the evidence JSON file.
    
    Args:
        evidence_file: Path to the evidence JSON file
        feedback_data: Dictionary containing feedback information
    """
    try:
        # Load existing evidence
        with open(evidence_file, 'r', encoding='utf-8') as f:
            evidence = json.load(f)
        
        # Add feedback section
        if "analytics" not in evidence:
            evidence["analytics"] = {}
        
        evidence["analytics"]["incident_report_feedback"] = {
            **feedback_data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Save back to file
        with open(evidence_file, 'w', encoding='utf-8') as f:
            json.dump(evidence, f, indent=2, ensure_ascii=False)
        
        return True
    except Exception as e:
        st.error(f"Failed to save feedback: {e}")
        return False

def get_feedback_from_json(evidence_file):
    """
    Get feedback data from the evidence JSON file.
    
    Args:
        evidence_file: Path to the evidence JSON file
        
    Returns:
        Dictionary containing feedback information or None
    """
    try:
        with open(evidence_file, 'r', encoding='utf-8') as f:
            evidence = json.load(f)
        
        return evidence.get("analytics", {}).get("incident_report_feedback")
    except Exception:
        return None

def get_service_config_status():
    """
    Get service configuration status for display in UI.
    
    Returns:
        Dictionary with mode, api_key_status
    """
    reasoning_mode = os.getenv("GRCAI_REASONING_MODE", "remote").lower()
    api_key = os.getenv("OPENAI_API_KEY")
    
    return {
        "mode": "Local" if reasoning_mode == "local" else "Remote",
        "api_key_configured": bool(api_key)
    }

def _save_ir_to_file(evidence_file, ir_text, feedback_data=None):
    """
    Save Incident Report to a separate _ir.json file.
    
    Args:
        evidence_file: Path to the evidence JSON file
        ir_text: The incident report text
        feedback_data: Optional feedback data to include
    """
    try:
        # Load evidence to get metadata
        with open(evidence_file, 'r', encoding='utf-8') as f:
            evidence = json.load(f)
        
        # Get session ID from evidence metadata
        metadata = evidence.get("metadata", {})
        session_id = metadata.get("session_id", Path(evidence_file).stem)
        
        # Create IR file path: rca_YYYY-MM-DD_HH-MM-SS_ir.json
        evidence_path = Path(evidence_file)
        ir_file = evidence_path.parent / f"{evidence_path.stem}_ir.json"
        
        # Build IR data
        ir_data = {
            "session_id": session_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "report_text": ir_text
        }
        
        # Add feedback if available
        if feedback_data:
            ir_data["feedback"] = {
                "accuracy_rating": feedback_data.get("accuracy_rating"),
                "was_helpful": feedback_data.get("was_helpful"),
                "actionable": feedback_data.get("actionable"),
                "trustworthy": (
                    feedback_data.get("accuracy_rating", 5) >= 3 and
                    feedback_data.get("was_helpful", True) and
                    feedback_data.get("actionable", True)
                )
            }
        
        # Save IR file
        with open(ir_file, 'w', encoding='utf-8') as f:
            json.dump(ir_data, f, indent=2, ensure_ascii=False)
        
        return True
    except Exception as e:
        st.error(f"Failed to save IR report: {e}")
        return False

def _normalize_rca_tasks(rca_tasks):
    """
    Normalize RCA tasks into a consistent shape:
      - Component: component / component(s) affected
      - Task: human-readable action
      - Notes: optional extra context

    This is a FINAL SAFETY NET on top of LLM parsing:
    - If 'component' is missing/empty but 'task' looks like a component name,
      we treat 'task' as component and 'notes' as the actual task.
    """
    if not rca_tasks:
        return []

    common_components = {
        "nginx",
        "postgres",
        "postgresql",
        "tomcat",
        "redis",
        "kafka",
        "os",
        "docker",
        "nodejs",
        "mssql",
        "application",
        "network",
        "disk",
    }

    normalized = []
    for t in rca_tasks:
        if not isinstance(t, dict):
            continue

        team = t.get("team") or t.get("owner") or "OPS"
        priority = t.get("priority", "Medium")
        effort = t.get("effort", "M")

        component = (t.get("component") or "").strip()
        task = (t.get("task") or "").strip()
        notes = (t.get("notes") or "").strip()

        task_lower = task.lower()
        notes_lower = notes.lower()

        # Helper to decide if a string looks like a component name list
        def looks_like_component(value: str) -> bool:
            if not value:
                return False
            parts = [p.strip().lower() for p in value.split(",")]
            return all(p in common_components for p in parts)

        # Case 1: component is empty but task looks like component(s)
        if not component and looks_like_component(task_lower):
            component = task
            # If notes is non-empty and reasonably long, treat it as the human task
            if notes and len(notes) > 10:
                task = notes
                notes = ""

        # Case 2: component is set, task also looks like component(s) and notes is a real sentence
        if component and looks_like_component(task_lower) and notes and len(notes) > 10:
            task = notes
            notes = ""

        normalized.append(
            {
                "component": component,
                "task": task or notes,  # fall back to notes if task is still empty
                "notes": notes if task else "",
                "team": team,
                "priority": priority,
                "effort": effort,
            }
        )

    return normalized


def _save_rca_to_file(evidence_file, rca_text, rca_tasks=None, ir_file_path=None):
    """
    Save RCA Report to a separate _rca.json file.
    
    Args:
        evidence_file: Path to the evidence JSON file
        rca_text: The RCA report text
        rca_tasks: Optional list of tasks generated from RCA
        ir_file_path: Optional path to the IR file this RCA is based on
    """
    try:
        # Load evidence to get metadata
        with open(evidence_file, 'r', encoding='utf-8') as f:
            evidence = json.load(f)
        
        # Get session ID from evidence metadata
        metadata = evidence.get("metadata", {})
        session_id = metadata.get("session_id", Path(evidence_file).stem)
        
        # Get feedback to determine trustworthiness
        feedback = get_feedback_from_json(evidence_file)
        trustworthy = True
        if feedback:
            accuracy = feedback.get("accuracy_rating", 5)
            was_helpful = feedback.get("was_helpful", True)
            actionable = feedback.get("actionable", True)
            trustworthy = accuracy >= 3 and was_helpful and actionable
        
        # Create RCA file path: rca_YYYY-MM-DD_HH-MM-SS_rca.json
        evidence_path = Path(evidence_file)
        rca_file = evidence_path.parent / f"{evidence_path.stem}_rca.json"
        
        # Normalize tasks shape before saving (final safety net)
        normalized_tasks = _normalize_rca_tasks(rca_tasks) if rca_tasks else []

        # Build RCA data
        rca_data = {
            "session_id": session_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "report_text": rca_text,
            "trustworthy": trustworthy
        }
        
        # Add tasks if available
        if normalized_tasks:
            rca_data["tasks"] = normalized_tasks
        
        # Add reference to IR file if available
        if ir_file_path:
            rca_data["based_on_ir"] = Path(ir_file_path).name
        else:
            # Try to find corresponding IR file
            ir_file = evidence_path.parent / f"{evidence_path.stem}_ir.json"
            if ir_file.exists():
                rca_data["based_on_ir"] = ir_file.name
        
        # Save RCA file
        with open(rca_file, 'w', encoding='utf-8') as f:
            json.dump(rca_data, f, indent=2, ensure_ascii=False)
        
        return True
    except Exception as e:
        st.error(f"Failed to save RCA report: {e}")
        return False

def scan_historical_incident_reports(environment):
    """
    Scan for historical incident reports from separate IR files.
    Also checks for legacy inline IRs in evidence JSON for backward compatibility.
    
    Args:
        environment: Environment name (e.g., "qa", "prod")
        
    Returns:
        List of dictionaries with IR metadata: date, observation, evidence_file, ir_text, trustworthy
    """
    historical_irs = []
    sessions_dir = Path("grcai_sessions") / environment
    
    if not sessions_dir.exists():
        return historical_irs
    
    # First, scan all separate IR files (rca_*_ir.json) - new format
    for ir_file in sorted(sessions_dir.glob("rca_*_ir.json"), reverse=True):
        try:
            # Load IR file
            with open(ir_file, 'r', encoding='utf-8') as f:
                ir_data = json.load(f)
            
            ir_text = ir_data.get("report_text")
            if not ir_text:
                continue
            
            # Try to find corresponding evidence file to get metadata
            # IR file: rca_YYYY-MM-DD_HH-MM-SS_ir.json
            # Evidence: rca_YYYY-MM-DD_HH-MM-SS.json
            evidence_file_name = ir_file.stem.replace("_ir", "") + ".json"
            evidence_file = ir_file.parent / evidence_file_name
            
            metadata = {}
            observations = "N/A"
            issue_time = None
            
            if evidence_file.exists():
                try:
                    with open(evidence_file, 'r', encoding='utf-8') as f:
                        evidence = json.load(f)
                    metadata = evidence.get("metadata", {})
                    observations = metadata.get("observations", "N/A")
                    issue_time = metadata.get("issue_time")
                except:
                    pass
            
            # Use IR file metadata if evidence not found
            if not metadata:
                session_id = ir_data.get("session_id", ir_file.stem.replace("_ir", ""))
                generated_at = ir_data.get("generated_at", "")
            else:
                session_id = metadata.get("session_id", ir_file.stem.replace("_ir", ""))
                generated_at = ir_data.get("generated_at", metadata.get("collected_at", ""))
            
            # Extract date from generated_at or filename
            date_str = "Unknown"
            if generated_at:
                try:
                    dt = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
                    date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass
            if date_str == "Unknown":
                # Try to extract from filename
                match = re.search(r'rca_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})', ir_file.name)
                if match:
                    date_str = f"{match.group(1)} {match.group(2).replace('-', ':')}"
            
            # Get trustworthy flag from feedback
            feedback = ir_data.get("feedback", {})
            trustworthy = feedback.get("trustworthy", False)
            
            # Format issue_time for display
            issue_time_str = "N/A"
            if issue_time:
                try:
                    if isinstance(issue_time, str):
                        dt = datetime.fromisoformat(issue_time.replace('Z', '+00:00'))
                        issue_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                    elif isinstance(issue_time, datetime):
                        issue_time_str = issue_time.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    issue_time_str = str(issue_time) if issue_time else "N/A"
            
            historical_irs.append({
                "date": date_str,  # Date when report was generated
                "issue_time": issue_time_str,  # Date and time when incident occurred
                "observation": observations,
                "session_id": session_id,
                "ir_filename": ir_file.name,  # Store IR filename
                "ir_file_stem": ir_file.stem,  # Store IR filename without extension
                "evidence_file": str(evidence_file) if evidence_file.exists() else None,
                "ir_text": ir_text,
                "trustworthy": trustworthy
            })
        except Exception as e:
            # Skip files that can't be read
            continue
    
    # Also scan for legacy inline IRs in evidence JSON files (backward compatibility)
    for evidence_file in sorted(sessions_dir.glob("rca_*.json"), reverse=True):
        # Skip IR files we already processed
        if evidence_file.name.endswith("_ir.json"):
            continue
        
        try:
            with open(evidence_file, 'r', encoding='utf-8') as f:
                evidence = json.load(f)
            
            # Check for inline IR in analytics section
            analytics = evidence.get("analytics", {})
            feedback = analytics.get("incident_report_feedback", {})
            ir_text = feedback.get("incident_report_text")
            
            if not ir_text:
                continue
            
            metadata = evidence.get("metadata", {})
            session_id = metadata.get("session_id", evidence_file.stem)
            observations = metadata.get("observations", "N/A")
            collected_at = metadata.get("collected_at", "")
            issue_time = metadata.get("issue_time")
            
            # Extract date (report generation date)
            date_str = "Unknown"
            if collected_at:
                try:
                    dt = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
                    date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass
            if date_str == "Unknown":
                match = re.search(r'rca_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})', evidence_file.name)
                if match:
                    date_str = f"{match.group(1)} {match.group(2).replace('-', ':')}"
            
            # Format issue_time for display
            issue_time_str = "N/A"
            if issue_time:
                try:
                    if isinstance(issue_time, str):
                        dt = datetime.fromisoformat(issue_time.replace('Z', '+00:00'))
                        issue_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                    elif isinstance(issue_time, datetime):
                        issue_time_str = issue_time.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    issue_time_str = str(issue_time) if issue_time else "N/A"
            
            # Get trustworthy flag
            trustworthy = feedback.get("trustworthy", False)
            
            # For legacy inline IRs, create a synthetic filename
            ir_filename = f"{evidence_file.stem}_ir.json"
            historical_irs.append({
                "date": date_str,  # Date when report was generated
                "issue_time": issue_time_str,  # Date and time when incident occurred
                "observation": observations,
                "session_id": session_id,
                "ir_filename": ir_filename,  # Store IR filename
                "ir_file_stem": evidence_file.stem + "_ir",  # Store IR filename without extension
                "evidence_file": str(evidence_file),
                "ir_text": ir_text,
                "trustworthy": trustworthy
            })
        except Exception:
            continue
    
    return historical_irs

def format_incident_response_report(ir_text):
    """
    Format Incident Response Report with tables converted to HTML for better rendering.
    
    Args:
        ir_text: Raw markdown text from LLM
        
    Returns:
        Formatted HTML string that can be displayed with st.markdown(..., unsafe_allow_html=True)
    """
    if not ir_text:
        return ir_text
    
    # First pass: Replace headers with consistent font size
    formatted_text = re.sub(
        r'^(#+)\s*\d+\.\s*(.+)$',
        r'<h3 style="font-size: 1.2rem; margin-top: 1rem;">\2</h3>',
        ir_text,
        flags=re.MULTILINE
    )
    
    # Also handle section headers without numbers
    formatted_text = re.sub(
        r'^(##)\s+(.+)$',
        r'<h3 style="font-size: 1.2rem; margin-top: 1rem;">\2</h3>',
        formatted_text,
        flags=re.MULTILINE
    )
    
    # Clean up: Split tables that might be on same line (handle LLM output quirks)
    formatted_text = re.sub(r'\|\s*\|', '|\n|', formatted_text)
    
    # Convert markdown tables to HTML tables
    lines = formatted_text.split('\n')
    formatted_lines = []
    i = 0
    
    # Patterns for filtering out meaningless content
    skip_patterns = [
        r'no issues detected',
        r'no operational issues',
        r'all systems.*functioning normally',
        r'no immediate follow-up',
        r'continue to monitor',
    ]
    
    while i < len(lines):
        line = lines[i]
        
        # Check if this looks like a markdown table header (starts with |)
        if line.strip().startswith('|') and line.count('|') >= 3:
            # Try to parse table
            table_lines = [line]
            j = i + 1
            
            # Collect separator line
            separator_found = False
            if j < len(lines) and lines[j].strip().startswith('|'):
                sep_line = lines[j].strip()
                if '---' in sep_line or '|--' in sep_line or re.match(r'^\|[-:\s|]+\|$', sep_line):
                    table_lines.append(lines[j])
                    j += 1
                    separator_found = True
            
            # Collect data rows
            while j < len(lines) and lines[j].strip().startswith('|') and lines[j].count('|') >= 3:
                # Skip separator lines
                if not re.match(r'^\|[-:\s|]+\|$', lines[j].strip()):
                    table_lines.append(lines[j])
                j += 1
            
            # If we have a complete table (at least header and separator), convert it
            if len(table_lines) >= 2 or separator_found:
                try:
                    # Parse header (skip first and last empty cells from split)
                    header_line = table_lines[0]
                    headers = [h.strip() for h in header_line.split('|')[1:-1] if h.strip()]
                    
                    if not headers:
                        # Invalid header, skip
                        formatted_lines.append(line)
                        i += 1
                        continue
                    
                    # Parse data rows (skip separator line)
                    rows = []
                    for row_line in table_lines[1:]:
                        # Skip separator lines
                        if re.match(r'^\|[-:\s|]+\|$', row_line.strip()):
                            continue
                        
                        cells = [c.strip() for c in row_line.split('|')[1:-1]]
                        if len(cells) != len(headers):
                            # Padding or trimming cells to match header count
                            while len(cells) < len(headers):
                                cells.append('')
                            cells = cells[:len(headers)]
                        
                        # Check if this row should be filtered out
                        row_text_lower = ' '.join(cells).lower()
                        should_skip = False
                        
                        # Skip if all cells are empty/N/A/None
                        meaningful_cells = [
                            c for c in cells 
                            if c and c.lower() not in [
                                'n/a', 'none', 'no issues detected', 'no', '', 
                                'no issues', 'not applicable', 'n/a', 'healthy',
                                'no operational issues', 'all systems functioning normally'
                            ]
                        ]
                        
                        # Skip if row matches skip patterns
                        for pattern in skip_patterns:
                            if re.search(pattern, row_text_lower):
                                should_skip = True
                                break
                        
                        # Skip if all values are N/A-like
                        if not meaningful_cells:
                            should_skip = True
                        
                        # Skip if "Component" column contains values like "os", "tomcat", etc. 
                        # but all other columns are N/A (component-level actions with no issues)
                        if headers and len(headers) > 1:
                            first_col = cells[0].lower() if cells else ''
                            other_cols = ' '.join(cells[1:]).lower() if len(cells) > 1 else ''
                            # If first column has component name but rest are N/A/no issues
                            if first_col and ('n/a' in other_cols or 'no issues' in other_cols or len(meaningful_cells) <= 1):
                                should_skip = True
                        
                        if not should_skip and meaningful_cells:
                            rows.append(cells)
                    
                    # Check section context for empty table handling
                    prev_lines_text = '\n'.join(formatted_lines[-5:]).lower()
                    section_context = ''
                    if 'immediate actions' in prev_lines_text:
                        section_context = 'actions'
                    elif 'component-level' in prev_lines_text:
                        section_context = 'component'
                    
                    # If we have rows, create HTML table
                    if rows:
                        df = pd.DataFrame(rows, columns=headers)
                        html_table = df.to_html(index=False, classes="table table-striped", escape=False, table_id=None)
                        formatted_lines.append(html_table)
                    else:
                        # Empty table - skip for actions/component sections, show message for others
                        if section_context in ['actions', 'component']:
                            pass  # Skip empty tables in these sections
                        else:
                            # For other sections (summary, confidence), show a simple message
                            formatted_lines.append('<p style="color: #666; font-style: italic;">No items to display.</p>')
                    
                    i = j
                    continue
                except Exception as e:
                    # If parsing fails, keep original lines
                    pass
        
        # Filter out standalone "No component-level issues detected" messages if we just skipped a component table
        line_lower = line.lower()
        if any(re.search(p, line_lower) for p in skip_patterns):
            # Check if this is a standalone message (not part of a table)
            if not line.strip().startswith('|'):
                # Check next few lines to see if there's a duplicate
                next_lines_text = '\n'.join(lines[i+1:i+3]).lower() if i+1 < len(lines) else ''
                if 'component-level' in next_lines_text or 'no component' in next_lines_text:
                    # Skip this duplicate message
                    i += 1
                    continue
        
        # Regular line - keep as is
        formatted_lines.append(line)
        i += 1
    
    formatted_text = '\n'.join(formatted_lines)
    
    # Clean up: Remove any standalone separator lines that might remain
    formatted_text = re.sub(r'^\|[-:\s|]+\|\s*$', '', formatted_text, flags=re.MULTILINE)
    
    # Remove duplicate "No component-level issues detected" messages
    formatted_text = re.sub(
        r'No component-level issues detected\.?\s*All components are operating normally\.?\s*(?:No component-level issues detected\.?)?',
        'No component-level issues detected. All components are operating normally.',
        formatted_text,
        flags=re.IGNORECASE | re.MULTILINE
    )
    
    # Clean up excessive blank lines
    formatted_text = re.sub(r'\n{3,}', '\n\n', formatted_text)
    
    return formatted_text

def format_rca_report(rca_text):
    """
    Format RCA report markdown with consistent header sizes and convert markdown tables to HTML.
    
    Args:
        rca_text: Raw markdown text from LLM
        
    Returns:
        Tuple of (formatted_text, tables_dict) where tables_dict contains table data for separate rendering
    """
    if not rca_text:
        return rca_text, {}
    
    # First pass: Normalize headers (keep as markdown, don't convert to HTML)
    # Pattern 1: Main section headers like "# 1. EXECUTIVE SUMMARY" -> "### EXECUTIVE SUMMARY"
    formatted_text = re.sub(
        r'^(#+)\s*\d+\.\s*(.+)$',
        r'### \2',
        rca_text,
        flags=re.MULTILINE
    )
    
    # Pattern 2: Component headers like "## Component: Nginx" -> "### Component: Nginx"
    formatted_text = re.sub(
        r'^(##)\s+Component:\s*(.+)$',
        r'### Component: \2',
        formatted_text,
        flags=re.MULTILINE | re.IGNORECASE
    )
    
    # Pattern 3: Other ## headers -> ### (but skip if already Component: header)
    formatted_text = re.sub(
        r'^(##)\s+(?!Component:)(.+)$',
        r'### \2',
        formatted_text,
        flags=re.MULTILINE
    )
    
    # Second pass: Convert markdown tables to HTML tables
    # Find all markdown tables and convert them
    table_pattern = r'(\|[^\n]+\|(?:\n\|[^\n]+\|)+)'
    tables_dict = {}
    table_counter = 0
    
    def replace_table(match):
        nonlocal table_counter
        table_markdown = match.group(0)
        
        # Parse markdown table
        lines = [line.strip() for line in table_markdown.split('\n') if line.strip()]
        if len(lines) < 2:
            return table_markdown  # Not a valid table
        
        # Skip separator line (second line with dashes)
        header_line = lines[0]
        data_lines = lines[2:] if len(lines) > 2 and '---' in lines[1] else lines[1:]
        
        # Parse header
        headers = [cell.strip() for cell in header_line.split('|')[1:-1]]
        
        # Parse data rows
        rows = []
        for line in data_lines:
            cells = [cell.strip() for cell in line.split('|')[1:-1]]
            if len(cells) == len(headers):
                rows.append(cells)
        
        if rows:
            # Create DataFrame
            df = pd.DataFrame(rows, columns=headers)
            # Store table for separate rendering
            table_id = f"table_{table_counter}"
            tables_dict[table_id] = df
            table_counter += 1
            # Replace with placeholder
            return f"<!-- TABLE_PLACEHOLDER:{table_id} -->"
        
        return table_markdown
    
    # Replace all markdown tables
    formatted_text = re.sub(table_pattern, replace_table, formatted_text, flags=re.MULTILINE)
    
    # Third pass: Convert error lists to tables
    error_section_pattern = r'\*\*Errors?\s*/\s*Patterns?\s*Detected:\*\*\s*\n((?:-.*\n?)+)'
    
    def replace_error_list(match):
        nonlocal table_counter
        error_list_text = match.group(1)
        error_items = re.findall(r'^-\s*(.+)$', error_list_text, re.MULTILINE)
        if len(error_items) >= 2:  # Convert if 2+ items
            table_rows = [{"Error/Pattern": item.strip()} for item in error_items]
            df_errors = pd.DataFrame(table_rows)
            table_id = f"table_{table_counter}"
            tables_dict[table_id] = df_errors
            table_counter += 1
            return f"**Errors / Patterns Detected:**\n\n<!-- TABLE_PLACEHOLDER:{table_id} -->\n"
        return match.group(0)
    
    formatted_text = re.sub(error_section_pattern, replace_error_list, formatted_text, flags=re.IGNORECASE | re.MULTILINE)
    
    # Convert timeline data (bullets with timestamps) to tables
    lines = formatted_text.split('\n')
    formatted_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Check for bullet lists with timestamps
        if re.match(r'^-\s*', line):
            bullet_text = line.strip()
            # Look for various timestamp patterns
            timestamp_pattern = r'\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}|\d{2}:\d{2}:\d{2}|\d{1,2}/\d{1,2}/\d{4}[\s-]\d{2}:\d{2}'
            
            if re.search(timestamp_pattern, bullet_text):
                # Check if this is part of a timeline (multiple consecutive timestamp bullets)
                timeline_data = [bullet_text]
                j = i + 1
                while j < len(lines) and j < i + 20:  # Look ahead up to 20 lines
                    next_line = lines[j].strip()
                    if re.match(r'^-\s*', next_line) and re.search(timestamp_pattern, next_line):
                        timeline_data.append(next_line)
                        j += 1
                    elif not next_line or next_line.startswith('<h3') or next_line.startswith('#'):
                        break
                    else:
                        j += 1
                
                if len(timeline_data) >= 2:  # Convert if 2+ timeline entries
                    # Extract timestamps and events
                    table_rows = []
                    for timeline_entry in timeline_data:
                        ts_match = re.search(timestamp_pattern, timeline_entry)
                        if ts_match:
                            timestamp = ts_match.group(0)
                            # Remove the bullet and timestamp to get the event description
                            description = re.sub(r'^-\s*', '', timeline_entry).replace(timestamp, '').strip()
                            table_rows.append({"Time": timestamp, "Event": description})
                    
                    if table_rows:
                        df_timeline = pd.DataFrame(table_rows)
                        table_id = f"table_{table_counter}"
                        tables_dict[table_id] = df_timeline
                        table_counter += 1
                        formatted_lines.append(f"<!-- TABLE_PLACEHOLDER:{table_id} -->")
                        formatted_lines.append("")  # Empty line after table
                        i = j
                        continue
        
        # Regular line - keep as is
        formatted_lines.append(line)
        i += 1
    
    formatted_text = '\n'.join(formatted_lines)
    
    return formatted_text, tables_dict

def load_evidence_file(file_path):
    """Load and cache evidence file."""
    # Load if file path changed OR if data is None (force reload scenario)
    if (st.session_state["evidence_file"] != file_path or 
        st.session_state["evidence_data"] is None):
        # Use pure function for file I/O
        evidence_data = load_evidence_json(file_path)
        
        if evidence_data is None:
            # File not found or invalid JSON
            st.error(f"⚠️ Evidence file not found or invalid: {file_path}")
            st.session_state["evidence_data"] = None
            st.session_state["evidence_file"] = None
            return None
        
        st.session_state["evidence_data"] = evidence_data
        st.session_state["evidence_file"] = file_path
    
    return st.session_state["evidence_data"]


# ----------------------------------------------------------------------
# TABS - 5 Tab Structure with Colors
# ----------------------------------------------------------------------
# Add custom CSS for colored tabs
st.markdown("""
<style>
    /* Style the tabs with different colors */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 4px 4px 0 0;
        padding: 8px 16px;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    /* Environment tab - Blue theme (first tab) */
    .stTabs [data-baseweb="tab-list"] button:first-child {
        background-color: #E3F2FD !important;
        color: #1976D2 !important;
    }
    .stTabs [data-baseweb="tab-list"] button:first-child[aria-selected="true"],
    .stTabs [data-baseweb="tab-list"] button:first-child[aria-selected="true"]:hover {
        background-color: #4A90E2 !important;
        color: white !important;
    }
    /* Evidence tab - Green theme (second tab) */
    .stTabs [data-baseweb="tab-list"] button:nth-child(2) {
        background-color: #E8F5E9 !important;
        color: #388E3C !important;
    }
    .stTabs [data-baseweb="tab-list"] button:nth-child(2)[aria-selected="true"],
    .stTabs [data-baseweb="tab-list"] button:nth-child(2)[aria-selected="true"]:hover {
        background-color: #50C878 !important;
        color: white !important;
    }
    /* Evidence Highlights tab - Orange theme (third tab) */
    .stTabs [data-baseweb="tab-list"] button:nth-child(3) {
        background-color: #FFF3E0 !important;
        color: #E65100 !important;
    }
    .stTabs [data-baseweb="tab-list"] button:nth-child(3)[aria-selected="true"],
    .stTabs [data-baseweb="tab-list"] button:nth-child(3)[aria-selected="true"]:hover {
        background-color: #FF9800 !important;
        color: white !important;
    }
    /* Evidence Review tab - Teal theme (fourth tab) */
    .stTabs [data-baseweb="tab-list"] button:nth-child(4) {
        background-color: #E0F2F1 !important;
        color: #00695C !important;
    }
    .stTabs [data-baseweb="tab-list"] button:nth-child(4)[aria-selected="true"],
    .stTabs [data-baseweb="tab-list"] button:nth-child(4)[aria-selected="true"]:hover {
        background-color: #009688 !important;
        color: white !important;
    }
    /* Incident Report tab - Red/Orange theme (fifth tab) */
    .stTabs [data-baseweb="tab-list"] button:nth-child(5) {
        background-color: #FFEBEE !important;
        color: #C62828 !important;
    }
    .stTabs [data-baseweb="tab-list"] button:nth-child(5)[aria-selected="true"],
    .stTabs [data-baseweb="tab-list"] button:nth-child(5)[aria-selected="true"]:hover {
        background-color: #D32F2F !important;
        color: white !important;
    }
    /* Full RCA tab - Purple theme (sixth tab) */
    .stTabs [data-baseweb="tab-list"] button:nth-child(6) {
        background-color: #F3E5F5 !important;
        color: #7B1FA2 !important;
    }
    .stTabs [data-baseweb="tab-list"] button:nth-child(6)[aria-selected="true"],
    .stTabs [data-baseweb="tab-list"] button:nth-child(6)[aria-selected="true"]:hover {
        background-color: #9C27B0 !important;
        color: white !important;
    }
    /* IR History tab - Blue theme (seventh tab) */
    .stTabs [data-baseweb="tab-list"] button:nth-child(7) {
        background-color: #E3F2FD !important;
        color: #1976D2 !important;
    }
    .stTabs [data-baseweb="tab-list"] button:nth-child(7)[aria-selected="true"],
    .stTabs [data-baseweb="tab-list"] button:nth-child(7)[aria-selected="true"]:hover {
        background-color: #2196F3 !important;
        color: white !important;
    }
    /* Style tables in RCA report */
    .table {
        border-collapse: collapse;
        width: 100%;
        margin: 1rem 0;
        font-size: 0.9rem;
    }
    .table th, .table td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: left;
    }
    .table th {
        background-color: #f2f2f2;
        font-weight: 600;
    }
    .table-striped tr:nth-child(even) {
        background-color: #f9f9f9;
    }
    /* Evidence Highlights styling */
    .host-critical {
        border-left: 4px solid #d32f2f;
        padding-left: 8px;
        margin: 4px 0;
    }
    .host-warning {
        border-left: 4px solid #f57c00;
        padding-left: 8px;
        margin: 4px 0;
    }
</style>
""", unsafe_allow_html=True)

# Add a hidden marker for JavaScript to detect which tab should be active
# This is more reliable than URL params which might not update immediately
should_switch_to_evidence = (
    st.session_state.get("active_main_tab") == "evidence" or 
    query_params.get("tab") == "evidence"
)

if should_switch_to_evidence:
    st.markdown('<div id="streamlit-tab-switch" data-target-tab="evidence" style="display:none;"></div>', unsafe_allow_html=True)
    
    # Clear the session state flag (but keep query param for URL)
    if st.session_state.get("active_main_tab") == "evidence":
        st.session_state["active_main_tab"] = None

# Inject JavaScript to preserve Evidence tab selection after reruns
# This script runs globally, outside of tab context, so it always executes
st.markdown("""
<script>
(function() {
    function switchToEvidenceTab() {
        const tabButtons = document.querySelectorAll('.stTabs [data-baseweb="tab"]');
        if (tabButtons.length >= 3) {
            const evidenceTab = tabButtons[1]; // Second tab (index 1)
            const isSelected = evidenceTab.getAttribute('aria-selected') === 'true';
            
            if (!isSelected) {
                evidenceTab.click();
                return true;
            }
            return true;
        }
        return false;
    }
    
    // Check for hidden marker first (more reliable)
    const marker = document.getElementById('streamlit-tab-switch');
    const shouldSwitch = marker && marker.getAttribute('data-target-tab') === 'evidence';
    
    // Also check URL query param as fallback
    const urlParams = new URLSearchParams(window.location.search);
    const urlShouldSwitch = urlParams.get('tab') === 'evidence';
    
    if (shouldSwitch || urlShouldSwitch) {
        // Wait for DOM and tabs to be ready
        function attemptSwitch() {
            // Use MutationObserver to watch for tabs
            const observer = new MutationObserver(function(mutations, obs) {
                if (switchToEvidenceTab()) {
                    obs.disconnect();
                }
            });
            observer.observe(document.body, { childList: true, subtree: true });
            
            // Try immediately
            switchToEvidenceTab();
            
            // Also try after delays
            setTimeout(function() {
                switchToEvidenceTab();
            }, 100);
            setTimeout(function() {
                switchToEvidenceTab();
                observer.disconnect();
            }, 300);
        }
        
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', attemptSwitch);
        } else {
            attemptSwitch();
        }
    }
})();
</script>
""", unsafe_allow_html=True)

# CRITICAL: Pre-load IR history data BEFORE tabs are created
# This MUST happen before any tab code runs, because st.stop() in other tabs
# will prevent the IR History tab code from executing
# We store the data in session state so it's available even if st.stop() is called
def _preload_ir_history(env):
    """Pre-load IR history data to ensure it's available when tab is clicked."""
    if not env:
        return []
    try:
        return scan_historical_incident_reports(env)
    except Exception:
        return []

# Get environment from session state (with fallback to "qa" for IR history preload)
# Note: Environment selector is now in the Environment tab, so we use session state
# If no environment is selected (empty string), use "qa" as fallback for preloading
_preload_env = st.session_state.get("selected_env") or "qa"
# But if it's explicitly empty string, don't preload
if _preload_env == "":
    _preload_env = None

# Always pre-load IR history on every page load and store in session state
# This ensures it's available even if st.stop() is called in other tabs
# We do this BEFORE tabs are created so the data is ready
# Only preload if we have an environment selected
if _preload_env and ("ir_history_cache" not in st.session_state or st.session_state.get("ir_history_env") != _preload_env):
    try:
        st.session_state["ir_history_cache"] = _preload_ir_history(_preload_env)
        st.session_state["ir_history_env"] = _preload_env
    except Exception:
        # If pre-loading fails, initialize with empty list
        st.session_state["ir_history_cache"] = []
        st.session_state["ir_history_env"] = _preload_env
elif not _preload_env:
    # No environment selected, clear cache
    st.session_state["ir_history_cache"] = []
    st.session_state["ir_history_env"] = None

tab_env, tab_evidence, tab_highlights, tab_llm_review, tab_incident_report, tab_rca, tab_ir_history = st.tabs(["🌐 Environment", "📊 Evidence", "🚨 Evidence Highlights", "🔍 Evidence Review", "🚨 Incident Report", "📄 Full RCA", "📜 IR History"])

# ======================================================================
# TAB 1: ENVIRONMENT
# ======================================================================
with tab_env:
    # Tab description
    st.info("💡 *Overview and topology of your environment. Configure hosts, components, and collection settings before gathering evidence.*")
    
    # Environment selector (moved from sidebar)
    cl = ConfigLoader()
    envs = cl.list_environments()
    envs = [e for e in envs if e not in ["__pycache__", ".DS_Store"]]
    
    if not envs:
        st.error("⚠️ No environments found. Check that 'config' directory exists with environment subdirectories.")
        st.info("💡 Make sure the 'config' directory exists with environment subdirectories (e.g., config/qa/qa.yaml).")
        env = None
        st.session_state["selected_env"] = None
    else:
        # Default to None (empty selection)
        # Add "None" option at the beginning of the list
        envs_with_none = [""] + envs
        
        # Initialize session state if not set (default to empty/None)
        if "env_selectbox" not in st.session_state:
            st.session_state["env_selectbox"] = ""
        if "selected_env" not in st.session_state or st.session_state["selected_env"] not in envs_with_none:
            st.session_state["selected_env"] = ""
        
        # Get current index (0 for empty/None, or index+1 for actual env)
        current_env = st.session_state.get("selected_env", "")
        if current_env in envs:
            current_index = envs.index(current_env) + 1  # +1 because of the empty option at index 0
        else:
            current_index = 0  # Default to empty/None
        
        # Environment selector with label on the left, dropdown on the right
        col1, col2 = st.columns([1, 4])
        with col1:
            st.markdown("**Environment:**")  # Label text
        with col2:
            env_selected = st.selectbox("", envs_with_none, index=current_index, key="env_selectbox", label_visibility="collapsed")
            # Update session state and refresh IR history cache if environment changed
            if st.session_state["selected_env"] != env_selected:
                st.session_state["selected_env"] = env_selected
                # Clear IR history cache to force refresh
                if "ir_history_cache" in st.session_state:
                    del st.session_state["ir_history_cache"]
                if "ir_history_env" in st.session_state:
                    del st.session_state["ir_history_env"]
            else:
                st.session_state["selected_env"] = env_selected
            env = env_selected if env_selected else None
    
    # Check if environment is selected
    # Use conditional rendering instead of st.stop() so other tabs can still execute
    if not env:
        st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">🌐 Environment Overview</h3>', unsafe_allow_html=True)
        st.error("⚠️ No environment selected. Please select an environment above.")
        st.info("💡 Make sure the 'config' directory exists with environment subdirectories (e.g., config/qa/qa.yaml).")
    else:
        # Load environment config
        env_config = None
        try:
            env_config = cl.load_environment(env)
        except Exception as e:
            st.markdown(f'<h3 style="font-size: 1.2rem; margin-top: 1rem;">🌐 Environment Overview : {env}</h3>', unsafe_allow_html=True)
            st.error(f"Failed to load environment '{env}': {e}")
            st.info(f"💡 Check that the file exists: config/{env}/{env}.yaml")
            import traceback
            with st.expander("Error details"):
                st.code(traceback.format_exc())
        
        # Only render the rest if config loaded successfully
        if env_config:
            # A. Environment Header
            st.markdown(f'<h3 style="font-size: 1.2rem; margin-top: 1rem;">🌐 Environment Overview : {env}</h3>', unsafe_allow_html=True)
            env_path = cl.base_dir / env / f"{env}.yaml"
            
            # Build single-line caption with Config, Owner, and Description
            caption_parts = [f"Config: {env_path}"]
            if env_config.get("metadata"):
                meta = env_config["metadata"]
                if meta.get("owner"):
                    caption_parts.append(f"Owner: {meta.get('owner')}")
                if meta.get("description"):
                    caption_parts.append(f"Description: {meta.get('description')}")
            
            st.caption(" | ".join(caption_parts))
            
            # Show raw YAML expander (with option to show original with comments)
            with st.expander("📄 Show raw YAML"):
                # Option to show original file (with comments) or parsed YAML
                show_original = st.checkbox("Show original file with comments", value=False, key="yaml_show_original")
                
                if show_original:
                    # Read and display the original YAML file to preserve comments
                    try:
                        env_path = cl.base_dir / env / f"{env}.yaml"
                        if env_path.exists():
                            raw_yaml_content = env_path.read_text(encoding="utf-8")
                            st.code(raw_yaml_content, language="yaml")
                        else:
                            st.warning(f"Original YAML file not found at: {env_path}")
                            st.code(yaml.safe_dump(env_config, sort_keys=False), language="yaml")
                    except Exception as e:
                        st.warning(f"Failed to read original YAML file: {e}")
                        st.code(yaml.safe_dump(env_config, sort_keys=False), language="yaml")
                else:
                    # Show parsed YAML (no comments, but clean structure)
                    st.code(yaml.safe_dump(env_config, sort_keys=False), language="yaml")
            
            # B. Hosts Overview Table
            st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">📋 Hosts Overview</h3>', unsafe_allow_html=True)
            
            hosts = env_config.get("hosts", [])
            if hosts:
                # Prepare table data
                host_rows = []
                for host in hosts:
                    # Extract service names from host.services
                    services = host.get("services", {})
                    service_names = list(services.keys()) if services else []
                    services_str = ", ".join(sorted(service_names)) if service_names else "—"
                    
                    host_rows.append({
                        "Host": host.get("name", "N/A"),
                        "Type": host.get("type", "N/A"),
                        "Services": services_str,
                        "Tags": ", ".join(host.get("tags", [])) if host.get("tags") else "—",
                        "Address": host.get("address", "N/A"),
                        "Status": "✓"  # Placeholder - would check connectivity in future
                    })
                
                st.table(host_rows)
            else:
                st.info("No hosts configured for this environment.")
            
            # Evidence Collection Section
            st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">🔍 Incident / Issue Details & Evidence Collection</h3>', unsafe_allow_html=True)
            
            # Pre-fill parameters from last collection (for convenience and re-run)
            default_date = date.today()
            default_time = datetime.now().time()
            default_components = ["os"]
            default_observations = ""
            
            if "last_collection_params" in st.session_state:
                params = st.session_state["last_collection_params"]
                if isinstance(params.get("issue_time"), datetime):
                    default_date = params["issue_time"].date()
                    default_time = params["issue_time"].time()
                default_components = params.get("components", default_components)
                default_observations = params.get("observations", default_observations)
            
            # Show info if re-run was triggered
            if "rerun_collection" in st.session_state and st.session_state["rerun_collection"]:
                st.info("🔄 Parameters loaded from last collection. Click 'Collect Evidence' to re-run.")
                # Clear the flag after showing message
                del st.session_state["rerun_collection"]
                # Reset time input to default when rerun is triggered
                if "env_issue_time" in st.session_state:
                    del st.session_state["env_issue_time"]
            
            col1, col2 = st.columns(2)
            with col1:
                date_val = st.date_input("Incident / Issue Date", default_date, key="env_issue_date")
            with col2:
                # Initialize session state for time input only once (unless rerun was triggered)
                # This preserves user's selection across reruns
                if "env_issue_time" not in st.session_state:
                    st.session_state["env_issue_time"] = default_time
                # Use the value from session state (which Streamlit updates automatically when user selects)
                time_val = st.time_input("Incident / Issue Time", value=st.session_state["env_issue_time"], key="env_issue_time")
            
            issue_time = datetime.combine(date_val, time_val)
            
            components = st.multiselect(
                "Which components were affected?",
                ["tomcat", "nginx", "postgres", "os", "redis", "kafka", "mssql", "nodejs", "docker"],
                default=default_components,
                key="env_components"
            )
            
            observations = st.text_area("Your observations", height=100, value=default_observations, key="env_observations")
            
            if st.button("🚀 Collect Evidence", type="primary", key="env_collect_btn"):
                api_key = os.getenv("OPENAI_API_KEY")
                if not api_key:
                    st.error("❌ Missing OPENAI_API_KEY environment variable.")
                elif not components:
                    st.warning("⚠️ Please select at least one component.")
                else:
                    with st.spinner("Collecting evidence from all hosts…"):
                        try:
                            orchestrator = SessionOrchestrator()
                            evidence = orchestrator.run_non_interactive(
                                issue_time=issue_time,
                                components=components,
                                observations=observations,
                                environment=env
                            )
                            
                            evidence_file = evidence["evidence_file"]
                            st.session_state["evidence_file"] = evidence_file
                            st.session_state["evidence_data"] = None  # Force reload
                            
                            # Check for connection failures
                            connection_failures = evidence.get("connection_failures", {})
                            if connection_failures:
                                failed_hosts = list(connection_failures.keys())
                                st.warning(f"⚠️ **Connection Failures:** The following hosts were unreachable and skipped:\n\n" + 
                                          "\n".join([f"  - **{host}**: {error}" for host, error in connection_failures.items()]) +
                                          f"\n\nEvidence was collected from other available hosts.")

                            # Show pre-check results (fast connectivity probe)
                            precheck = evidence.get("precheck", {})
                            if precheck:
                                unreachable = {h: r for h, r in precheck.items() if not r.get("accessible")}
                                if unreachable:
                                    st.warning(
                                        "⚠️ **Pre-check Warning:** Some hosts were unreachable during the fast connectivity check.\n\n"
                                        + "\n".join([f"  - **{h}**: {r.get('error') or 'unreachable'}" for h, r in unreachable.items()])
                                    )
                                else:
                                    st.success("✅ Pre-check passed: all hosts reachable.")
                            
                            # Clear previous RCA report since evidence has changed
                            if "rca_text" in st.session_state:
                                del st.session_state["rca_text"]
                            if "rca_tasks" in st.session_state:
                                del st.session_state["rca_tasks"]
                            
                            # Clear prepared evidence from Evidence Review since it's based on old evidence
                            if "prepared_evidence_for_llm" in st.session_state:
                                del st.session_state["prepared_evidence_for_llm"]
                            if "llm_review_metadata" in st.session_state:
                                del st.session_state["llm_review_metadata"]
                            
                            # Store collection parameters for re-run capability
                            st.session_state["last_collection_params"] = {
                                "issue_time": issue_time,
                                "components": components,
                                "observations": observations,
                                "environment": env
                            }
                            
                            # Clear rerun flag if it was set
                            if "rerun_collection" in st.session_state:
                                del st.session_state["rerun_collection"]
                            
                            if connection_failures:
                                st.info(f"✅ Evidence collected and saved to: `{evidence_file}` (some hosts were unreachable)")
                            else:
                                st.success(f"✅ Evidence collected and saved to: `{evidence_file}`")
                            
                            # Set up tab switching
                            st.session_state["active_main_tab"] = "evidence"
                            st.query_params["tab"] = "evidence"
                            
                            # Inject JavaScript to switch to Evidence tab after a brief delay
                            # This allows the success message to be visible before switching
                            st.markdown("""
                            <script>
                            setTimeout(function() {
                                const tabButtons = document.querySelectorAll('.stTabs [data-baseweb="tab"]');
                                if (tabButtons.length >= 2) {
                                    const evidenceTab = tabButtons[1]; // Second tab (index 1) - Evidence
                                    if (evidenceTab && evidenceTab.getAttribute('aria-selected') !== 'true') {
                                        evidenceTab.click();
                                    }
                                }
                            }, 800);
                            </script>
                            """, unsafe_allow_html=True)
                            
                        except RuntimeError as e:
                            # Handle case where all hosts failed
                            error_msg = str(e)
                            if "All hosts unreachable" in error_msg:
                                st.error(f"❌ **Collection Failed:** {error_msg}")
                                st.info("💡 **Next Steps:**\n"
                                       "  - Verify that the hosts are running and accessible\n"
                                       "  - Check SSH configuration in the environment config\n"
                                       "  - Ensure network connectivity to the hosts")
                            else:
                                st.error(f"❌ Collection failed: {error_msg}")
                        except Exception as e:
                            st.error(f"❌ Collection failed: {e}")
                            st.exception(e)


# ======================================================================
# TAB 2: EVIDENCE (Forensic Analysis Tool)
# ======================================================================
with tab_evidence:
    # Tab description
    st.info("💡 *Evidence browser with tree navigation. Explore collected evidence, logs, metrics, and component details in a structured view.*")
    
    # File selector for existing evidence files
    env_for_files = st.session_state.get("selected_env") or "qa"  # Use current environment or default to qa
    if env_for_files == "":
        env_for_files = "qa"  # If explicitly empty, default to qa for file browsing
    sessions_dir = Path("grcai_sessions") / env_for_files
    
    available_files = []
    if sessions_dir.exists():
        # Find all evidence JSON files (excluding _ir.json and _rca.json)
        available_files = sorted(
            [f for f in sessions_dir.glob("rca_*.json") if not f.name.endswith("_ir.json") and not f.name.endswith("_rca.json")],
            reverse=True  # Most recent first
        )
    
    # Try to get evidence file from session state first (for early title display)
    evidence_file = None
    evidence_data = None
    summary = None
    
    if st.session_state.get("evidence_file"):
        evidence_file = st.session_state["evidence_file"]
        evidence_data = load_evidence_file(evidence_file)
        if evidence_data:
            try:
                summary = get_evidence_summary(evidence_data)
            except:
                pass
    
    # Build and display Evidence Browser title with date and time range (at the top)
    if evidence_data and summary:
        metadata = evidence_data.get("metadata", {})
        time_window = summary.get("time_window")
        
        # Format date from collected_at or session_id
        date_str = "N/A"
        collected_at = metadata.get("collected_at")
        if collected_at:
            try:
                # Parse ISO format datetime and extract date
                dt = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
                date_str = dt.strftime("%Y-%m-%d")
            except:
                # Fallback: try to extract from session_id if available
                session_id = metadata.get("session_id", "")
                if session_id and "_" in session_id:
                    date_str = session_id.split("_")[1] if len(session_id.split("_")) > 1 else "N/A"
        
        # Format time range from time_window
        time_range_str = ""
        if time_window and isinstance(time_window, dict):
            since = time_window.get("since", "")
            until = time_window.get("until", "")
            if since and until:
                # Format times (they might be in ISO format, try to extract just the time part)
                try:
                    # Try to parse and format
                    since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
                    until_dt = datetime.fromisoformat(until.replace('Z', '+00:00'))
                    since_time = since_dt.strftime("%H:%M:%S")
                    until_time = until_dt.strftime("%H:%M:%S")
                    time_range_str = f" | from: {since_time} | to: {until_time}"
                except:
                    # If parsing fails, use raw values
                    time_range_str = f" | from: {since} | to: {until}"
        elif collected_at:
            # Fallback: use collected_at as a single time point
            try:
                dt = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
                time_str = dt.strftime("%H:%M:%S")
                time_range_str = f" | collected: {time_str}"
            except:
                pass
        
        browser_title = f"📊 Evidence Browser - {date_str}{time_range_str}"
        st.markdown(f'<h3 style="font-size: 1.2rem; margin-top: 1rem;">{browser_title}</h3>', unsafe_allow_html=True)
    
    # Show file selector if files exist
    if available_files:
        file_options = [f"📄 {f.name} ({f.stat().st_size / 1024:.1f} KB)" for f in available_files]
        file_paths = [str(f) for f in available_files]
        
        # Add option to use current session state file if it exists
        current_file_index = 0
        if st.session_state.get("evidence_file") and st.session_state["evidence_file"] in file_paths:
            current_file_index = file_paths.index(st.session_state["evidence_file"])
        
        # Place label and selectbox side by side
        col_label, col_select = st.columns([1, 4])
        with col_label:
            st.markdown("**📂 Select Evidence File**")
        with col_select:
            selected_file_display = st.selectbox(
                "",
                file_options,
                index=current_file_index,
                key="evidence_file_selector",
                help="Select an existing evidence file to view, or collect new evidence from the Environment tab.",
                label_visibility="collapsed"
            )
        
        # Extract the actual file path
        selected_index = file_options.index(selected_file_display)
        selected_file_path = file_paths[selected_index]
        
        # Update session state if a different file is selected
        if st.session_state.get("evidence_file") != selected_file_path:
            st.session_state["evidence_file"] = selected_file_path
            st.session_state["evidence_data"] = None  # Force reload
            st.rerun()
        
        evidence_file = st.session_state["evidence_file"]
    elif st.session_state.get("evidence_file"):
        # Use existing session state file
        evidence_file = st.session_state["evidence_file"]
        st.info(f"📄 Using evidence file: `{evidence_file}`")
    else:
        # No files available and no session state
        st.warning("⚠️ No evidence file loaded. Please collect evidence from the **Environment** tab first.")
        st.info("💡 After collecting evidence, it will automatically appear here.")
        st.stop()
    
    # Load evidence data if not already loaded
    if not evidence_data:
        evidence_data = load_evidence_file(evidence_file)
    
    # Validate evidence data was loaded
    if evidence_data is None:
        st.error("⚠️ Failed to load evidence data. Please try collecting evidence again.")
        st.stop()
    
    # Get summary statistics (needed for service listing)
    if not summary:
        try:
            summary = get_evidence_summary(evidence_data)
        except Exception as e:
            st.error(f"⚠️ Failed to generate evidence summary: {e}")
            import traceback
            with st.expander("Error details"):
                st.code(traceback.format_exc())
            st.stop()
    
    # ======================================================================
    # UNIFIED EVIDENCE BROWSER (Option B: Grouped by Host with Inline Expansion)
    # ======================================================================
    
    # Helper function to get status priority for sorting
    def _get_status_priority(status: str) -> int:
        """
        Return priority for sorting statuses.
        Lower number = higher priority (shown first).
        Successful should be last (highest number).
        """
        priority_map = {
            "Host Unreachable": 0,
            "Connection Error": 1,
            "Collection Error": 1,  # Same priority as Connection Error
            "Errors in Logs": 2,
            "Process Not Found": 3,
            "Successful": 4
        }
        return priority_map.get(status, 5)  # Unknown statuses go to the end
    
    # Helper function to get all service instances with their data
    def get_all_service_instances(evidence_data, summary):
        """
        Get all service instances with their full data, grouped by host.
        
        Returns:
            list: List of dicts with keys: host, component, instance_name, instance_data, status, status_icon, status_priority
        """
        service_list = []
        per_host_summary = summary.get("per_host_summary", {})
        host_data = evidence_data.get("host", {})
        
        # Iterate through components and their findings (findings keys are host names)
        for component_name, component_data in host_data.items():
            if not isinstance(component_data, dict):
                continue
            
            findings = component_data.get("findings", {})
            instances = component_data.get("instances", [])
            
            # Use findings structure to get host names (findings keys are host names)
            if findings:
                for host_name, finding_data in findings.items():
                    if not isinstance(finding_data, dict):
                        continue
                    
                    # For components like nginx/tomcat, instances are nested in findings
                    nested_instances = finding_data.get("instances", [])
                    if nested_instances:
                        # Multiple instances per host (e.g., nginx with multiple vhosts)
                        for instance in nested_instances:
                            instance_name = instance.get("name", "default")
                            status, status_icon = _determine_instance_status(
                                instance, host_name, component_name, per_host_summary
                            )
                            service_list.append({
                                "host": host_name,
                                "component": component_name,
                                "instance_name": instance_name,
                                "instance_data": instance,
                                "status": status,
                                "status_icon": status_icon,
                                "_status_priority": _get_status_priority(status)
                            })
                    else:
                        # Single instance per host - use finding_data as the instance
                        status, status_icon = _determine_instance_status(
                            finding_data, host_name, component_name, per_host_summary
                        )
                        service_list.append({
                            "host": host_name,
                            "component": component_name,
                            "instance_name": finding_data.get("name", "default"),
                            "instance_data": finding_data,
                            "status": status,
                            "status_icon": status_icon,
                            "_status_priority": _get_status_priority(status)
                        })
            elif instances:
                # Fallback: if no findings structure, use instances list
                # Try to match instances to hosts using per_host_summary
                for instance in instances:
                    instance_name = instance.get("name", "unknown")
                    # Try to find which host this instance belongs to
                    # This is a best-effort match - may not be perfect
                    host_name = "unknown"
                    for h_name, h_status in per_host_summary.items():
                        if component_name in h_status.get("collected", []):
                            host_name = h_name
                            break
                    
                    status, status_icon = _determine_instance_status(
                        instance, host_name, component_name, per_host_summary
                    )
                    service_list.append({
                        "host": host_name,
                        "component": component_name,
                        "instance_name": instance_name,
                        "instance_data": instance,
                        "status": status,
                        "status_icon": status_icon,
                        "_status_priority": _get_status_priority(status)
                    })
        
        # Sort by status priority (issues first, successful last), then by host, then by component, then by instance
        service_list.sort(key=lambda x: (
            x["_status_priority"],
            x["host"],
            x["component"],
            x.get("instance_name", "")
        ))
        
        return service_list
    
    def _determine_instance_status(instance, host_name, component_name, per_host_summary):
        """Determine collection status for an instance."""
        status = "Successful"
        status_icon = "✅"
        
        # Helper to check if an error is a collection/connection error (should be ❌ not ⚠️)
        def is_collection_error(error):
            """Check if error is a collection/connection failure (critical) vs log error (warning)."""
            if isinstance(error, dict):
                error_type = error.get("type", "").lower()
                error_stage = error.get("stage", "").lower()
                error_msg = error.get("message", "").lower()
                
                # Collection errors are critical
                if error_type == "collection_error":
                    return True
                if error_stage in ["command_execution", "connection", "ssh"]:
                    return True
                # Check for connection failure keywords
                if any(keyword in error_msg for keyword in ["ssh", "connection failed", "timed out", "unreachable", "connection error"]):
                    return True
            elif isinstance(error, str):
                error_lower = error.lower()
                if any(keyword in error_lower for keyword in ["ssh", "connection failed", "timed out", "unreachable", "connection error"]):
                    return True
            return False
        
        # Check if host is unreachable
        host_status = per_host_summary.get(host_name, {})
        if host_status.get("unreachable", False):
            status = "Host Unreachable"
            status_icon = "❌"
        else:
            # Check if component is in unavailable list
            unavailable = host_status.get("unavailable", [])
            if component_name in unavailable:
                status = "Connection Error"
                status_icon = "❌"  # Changed from ⚠️ to ❌ - connection errors are critical
            # Check if component is in missing list
            elif component_name in host_status.get("missing", []):
                status = "Process Not Found"
                status_icon = "ℹ️"
            # Check for errors in instance data
            elif instance.get("errors"):
                errors = instance.get("errors", [])
                if isinstance(errors, list) and len(errors) > 0:
                    # Check if any error is a collection/connection error (critical)
                    has_collection_error = any(is_collection_error(err) for err in errors)
                    if has_collection_error:
                        status = "Collection Error"
                        status_icon = "❌"
                    else:
                        status = "Errors in Logs"
                        status_icon = "⚠️"
            # Check for singular error field
            elif instance.get("error"):
                error = instance.get("error")
                if is_collection_error(error):
                    status = "Collection Error"
                    status_icon = "❌"
                else:
                    status = "Errors in Logs"
                    status_icon = "⚠️"
            # Also check raw_findings for errors (e.g., Docker adapter)
            elif instance.get("raw_findings", {}).get("errors"):
                raw_errors = instance.get("raw_findings", {}).get("errors", [])
                if isinstance(raw_errors, list) and len(raw_errors) > 0:
                    has_collection_error = any(is_collection_error(err) for err in raw_errors)
                    if has_collection_error:
                        status = "Collection Error"
                        status_icon = "❌"
                    else:
                        status = "Errors in Logs"
                        status_icon = "⚠️"
        
        return status, status_icon
    
    # Helper function to render expanded evidence content (Metrics/Logs/Errors/Raw JSON)
    def render_instance_evidence(instance_data, component, host_name, instance_name=None):
        """Render Metrics/Logs/Errors/Raw JSON for an instance."""
        # Use tabs (tabs can be inside expanders, unlike nested expanders)
        viewer_tabs = st.tabs(["📈 Metrics", "📄 Logs", "❌ Errors", "📋 Raw JSON"])
        
        # Tab 1: Metrics
        with viewer_tabs[0]:
            # Get metrics from instance data (try multiple locations)
            metrics = instance_data.get("metrics", {})
        
        # Ensure metrics is a dict, not None
            if not isinstance(metrics, dict):
                metrics = {}
            
            # If no metrics dict or empty dict, try to extract from top-level
            if not metrics:
                metric_keys = ["cpu", "memory", "disk", "kernel", "thread_analysis", "gc_logs", "access_log_analysis"]
                metrics = {k: instance_data[k] for k in metric_keys if k in instance_data and instance_data[k] is not None}
            
            # Also check for access_log_analysis at top level
            if "access_log_analysis" in instance_data and "access_log_analysis" not in metrics:
                if not isinstance(metrics, dict):
                    metrics = {}
                metrics["access_log_analysis"] = instance_data["access_log_analysis"]
            
            # Also check legacy flattened structure in findings
            if not metrics and "findings" in instance_data:
                findings_data = instance_data.get("findings", {})
                if isinstance(findings_data, dict):
                    metric_keys = ["cpu", "memory", "disk", "kernel"]
                    metrics = {k: findings_data[k] for k in metric_keys if k in findings_data and findings_data[k] is not None}
            
            # Filter out empty metrics
            metrics = {k: v for k, v in metrics.items() if v is not None and v != "" and v != {}}
            
            if not metrics:
                st.info("No metrics data available for this instance.")
            else:
                formatted_metrics = format_metrics_table(metrics, component_type=component)
                
                if not formatted_metrics:
                    st.warning("⚠️ Metrics found but couldn't be formatted. Showing raw metrics instead.")
                    for metric_name, metric_value in metrics.items():
                        st.markdown(f"#### {metric_name.upper()}")
                        if isinstance(metric_value, dict):
                            st.json(metric_value)
                        elif isinstance(metric_value, str):
                            st.code(metric_value, language="text")
                        else:
                            st.text(str(metric_value))
                else:
                    # Create a table from formatted metrics
                    table_data = []
                    for metric_name, metric_data in formatted_metrics.items():
                        output = metric_data.get("output", "")
                        command = metric_data.get("command", "")
                        error = metric_data.get("error", "")
                        
                        if error:
                            continue
                        
                        output_preview = str(output)[:100] if output else "No output"
                        if len(str(output)) > 100:
                            output_preview += "..."
                        
                        table_data.append({
                            "Metric": metric_name,
                            "Value": output_preview,
                            "Command": command if command != "N/A" else ""
                        })
                    
                    if table_data:
                        df = pd.DataFrame(table_data)
                        st.dataframe(
                            df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Metric": st.column_config.TextColumn("Metric", width="small"),
                                "Value": st.column_config.TextColumn("Value", width="small"),
                                "Command": st.column_config.TextColumn("Command", width="large")
                            }
                        )
                        
                        # Show detailed view for metrics
                        for metric_name, metric_data in formatted_metrics.items():
                            output = metric_data.get("output", "")
                            error = metric_data.get("error", "")
                            
                            if error:
                                continue
                            
                            if output and len(str(output)) > 100:
                                # Use checkbox to toggle full details (can't use nested expanders)
                                detail_key = f"metric_detail_{component}_{host_name}_{instance_name or 'default'}_{metric_name}"
                                show_detail = st.checkbox(f"Show full details for {metric_name}", key=detail_key, value=False)
                                if show_detail:
                                    st.code(output, language="text")
                                    if metric_data.get("command"):
                                        st.caption(f"Command: `{metric_data['command']}`")
        
        # Tab 2: Logs
        with viewer_tabs[1]:
            logs = instance_data.get("logs", {})
            
            # Handle error_logs as a dictionary of log files
            error_logs = instance_data.get("error_logs", {})
            if error_logs and isinstance(error_logs, dict):
                for log_key, log_value in error_logs.items():
                    if log_key not in logs:
                        logs[f"error_logs.{log_key}"] = log_value
            
            # Check raw_findings for Docker adapter logs
            raw_findings = instance_data.get("raw_findings", {})
            if raw_findings and isinstance(raw_findings, dict):
                daemon = raw_findings.get("daemon", {})
                if daemon and isinstance(daemon, dict):
                    daemon_logs = daemon.get("logs")
                    if daemon_logs is not None and isinstance(daemon_logs, str):
                        if not isinstance(logs, dict):
                            logs = {}
                        logs["daemon.logs"] = {
                            "content": daemon_logs if daemon_logs.strip() else "(Docker daemon logs not available or empty)",
                            "line_count": len(daemon_logs.splitlines()) if daemon_logs.strip() else 0,
                            "source": "docker_daemon",
                            "type": "text",
                            "collection_mode": "current"
                        }
            
            # Also check legacy structure
            if not logs:
                log_keys = [k for k in instance_data.keys() if ("log" in k.lower() or k in ["syslog", "auth_log", "access_log_tail", "error_log_tail", "catalina_out_tail", "postgres_logs"]) and k != "access_log_analysis"]
                if log_keys:
                    logs = {k: instance_data[k] for k in log_keys}
            
            # Also check common log field names
            common_log_fields = ["access_log_tail", "error_log_tail", "catalina_out_tail", "postgres_logs", "syslog", "auth_log", "logs"]
            if not logs:
                logs = {k: instance_data[k] for k in common_log_fields if k in instance_data}
            
            if not logs:
                st.info("No log data available for this instance.")
            else:
                for log_name in sorted(logs.keys()):
                    log_data = logs[log_name]
                    formatted_log = format_log_entry(log_data, log_name)
                    
                    # Use checkbox to toggle log visibility (can't use expanders inside tabs that are in expanders)
                    log_header = f"📄 **{log_name}** ({formatted_log['line_count']} lines, {formatted_log['collection_mode']}, source: {formatted_log['source']})"
                    st.markdown(log_header)
                    
                    log_show_key = f"log_show_{component}_{host_name}_{instance_name or 'default'}_{log_name}"
                    show_log = st.checkbox(f"Show {log_name} content", key=log_show_key, value=False)
                    
                    if show_log:
                        if formatted_log.get("time_window"):
                            tw = formatted_log["time_window"]
                            st.caption(f"Time Window: {tw.get('since')} to {tw.get('until')}")
                        
                        log_content = formatted_log.get("content", "")
                        if log_content:
                            if len(log_content) > 50000:
                                st.code(log_content[:50000] + "\n... (truncated)", language="text")
                                st.caption(f"Log truncated. Total size: {len(log_content)} characters")
                            else:
                                st.code(log_content, language="text")
                            
                            st.download_button(
                                f"Download {log_name}",
                                data=log_content,
                                file_name=f"{host_name}_{log_name}.log",
                                key=f"dl_{component}_{host_name}_{instance_name or 'default'}_{log_name}"
                            )
                        else:
                            st.info("Log is empty")
        
        # Tab 3: Errors
        with viewer_tabs[2]:
            errors = instance_data.get("errors", [])
        
            # Check raw_findings for Docker adapter errors
            raw_findings = instance_data.get("raw_findings", {})
            if raw_findings and isinstance(raw_findings, dict):
                raw_errors = raw_findings.get("errors", [])
                if raw_errors and isinstance(raw_errors, list) and len(raw_errors) > 0:
                    if not isinstance(errors, list):
                        errors = []
                    errors.extend(raw_errors)
            
            # Also check legacy format
            if not errors or (isinstance(errors, list) and len(errors) == 0):
                error_field = instance_data.get("error")
                if error_field:
                    if isinstance(error_field, str):
                        errors = [error_field]
                    elif isinstance(error_field, list):
                        errors = error_field
                    elif isinstance(error_field, dict):
                        errors = [error_field]
            
            # Ensure errors is a list
            if not isinstance(errors, list):
                if errors:
                    errors = [errors]
                else:
                    errors = []
            
            # Filter out empty/None errors
            errors = [e for e in errors if e is not None and e != ""]
            
            if not errors:
                st.success("✅ No errors for this instance")
            else:
                st.caption(f"Found {len(errors)} error(s)")
                
                for idx, raw_error in enumerate(errors):
                    error = safe_error_data(raw_error, component=component, host=host_name)
                    
                    error_msg = error.get("message", "No error message")
                    is_docker_finding = (
                        component == "docker" and 
                        isinstance(error_msg, str) and
                        ("Containers with restarts" in error_msg or 
                         "Containers killed by OOM" in error_msg or
                         "Unhealthy containers" in error_msg)
                    )
                    
                    if is_docker_finding:
                        icon = "⚠️"
                        title = f"{icon} Docker Finding"
                    else:
                        icon = "❌"
                        title = f"{icon} {error['type']} - {error['stage']}"
                    
                    # Use checkbox to toggle error details (can't use expanders inside tabs that are in expanders)
                    st.markdown(f"**{title}**")
                    error_show_key = f"error_show_{component}_{host_name}_{instance_name or 'default'}_{idx}"
                    show_error = st.checkbox(f"Show details", key=error_show_key, value=(idx == 0))
                    
                    if show_error:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.text(f"Component: {error.get('component', 'N/A')}")
                            st.text(f"Stage: {error.get('stage', 'N/A')}")
                            st.text(f"Code: {error.get('code', 'N/A')}")
                        with col2:
                            st.text(f"Type: {error.get('type', 'N/A')}")
                            st.text(f"Message: {error.get('message', 'N/A')}")
                        
                        if error.get("detail"):
                            st.text(f"Detail: {error.get('detail')}")
                        
                        if error.get("traceback"):
                            traceback_key = f"show_traceback_{component}_{host_name}_{instance_name or 'default'}_{idx}"
                            show_traceback = st.checkbox("Show Traceback", key=traceback_key, value=False)
                            if show_traceback:
                                st.code(error.get("traceback"), language="text")
        
        # Tab 4: Raw JSON
        with viewer_tabs[3]:
            st.code(json.dumps(instance_data, indent=2), language="json")
            json_str = json.dumps(instance_data, indent=2)
            st.download_button(
                "Download as JSON",
                data=json_str,
                file_name=f"{host_name}_{component}_{instance_name or 'default'}_evidence.json",
                key=f"dl_json_{component}_{host_name}_{instance_name or 'default'}"
            )
    
    # Get all service instances
    all_services = get_all_service_instances(evidence_data, summary)
    
    # Search and filter controls
    col_search, col_filter_host, col_filter_component = st.columns([2, 1, 1])
    with col_search:
        search_query = st.text_input("🔍 Search", placeholder="Search by host, component, or instance...", key="evidence_search")
    with col_filter_host:
        # Get unique hosts for filter
        unique_hosts = sorted(list(set(s["host"] for s in all_services)))
        filter_host = st.selectbox("Filter by Host", ["All"] + unique_hosts, key="evidence_filter_host")
    with col_filter_component:
        # Get unique components for filter
        unique_components = sorted(list(set(s["component"] for s in all_services)))
        filter_component = st.selectbox("Filter by Component", ["All"] + unique_components, key="evidence_filter_component")
    
    # Filter services based on search and filters
    filtered_services = all_services
    if search_query:
        search_lower = search_query.lower()
        filtered_services = [s for s in filtered_services if 
                           search_lower in s["host"].lower() or 
                           search_lower in s["component"].lower() or 
                           search_lower in s["instance_name"].lower()]
    if filter_host != "All":
        filtered_services = [s for s in filtered_services if s["host"] == filter_host]
    if filter_component != "All":
        filtered_services = [s for s in filtered_services if s["component"] == filter_component]
    
    # Initialize session state for expanded services (use list for Streamlit compatibility)
    if "expanded_services" not in st.session_state:
        st.session_state["expanded_services"] = []
    
    # Group services by host
    services_by_host = {}
    for service in filtered_services:
        host = service["host"]
        if host not in services_by_host:
            services_by_host[host] = []
        services_by_host[host].append(service)
    
    # Display services grouped by host (Option B style)
    if not filtered_services:
        st.info("No services match your search/filter criteria.")
    else:
        # Sort hosts
        for host_name in sorted(services_by_host.keys()):
            host_services = services_by_host[host_name]
            
            # Host header with Expand/Collapse toggle button (right-justified)
            col_host, col_button = st.columns([5, 1])
            with col_host:
                st.markdown(f"### 🖥️ {host_name}")
            with col_button:
                # Get all service keys for this host
                host_service_keys = []
                for service in host_services:
                    component = service["component"]
                    instance_name = service["instance_name"]
                    service_key = f"{host_name}_{component}_{instance_name}"
                    host_service_keys.append(service_key)
                
                # Check if all services for this host are expanded
                current_expanded = st.session_state.get("expanded_services", [])
                all_expanded = all(key in current_expanded for key in host_service_keys)
                
                # Toggle button - shows expand if not all expanded, collapse if all expanded
                button_label = "🔼" if all_expanded else "🔽"
                button_help = "Collapse all services for this host" if all_expanded else "Expand all services for this host"
                
                if st.button(button_label, key=f"toggle_host_{host_name}", help=button_help, use_container_width=True):
                    if all_expanded:
                        # Collapse: Remove all service keys for this host
                        st.session_state["expanded_services"] = [key for key in current_expanded if key not in host_service_keys]
                    else:
                        # Expand: Add all service keys for this host
                        for key in host_service_keys:
                            if key not in current_expanded:
                                current_expanded.append(key)
                        st.session_state["expanded_services"] = current_expanded
                    st.rerun()
            
            # Display each service as an expandable row
            for service in host_services:
                component = service["component"]
                instance_name = service["instance_name"]
                status = service["status"]
                status_icon = service["status_icon"]
                instance_data = service["instance_data"]
                
                # Create unique key for this service
                service_key = f"{host_name}_{component}_{instance_name}"
                is_expanded = service_key in (st.session_state["expanded_services"] or [])
                
                # Build row label
                error_count = 0
                errors = instance_data.get("errors", [])
                if isinstance(errors, list):
                    error_count = len(errors)
                elif errors:
                    error_count = 1
                
                # Count errors from raw_findings too
                raw_findings = instance_data.get("raw_findings", {})
                if raw_findings and isinstance(raw_findings, dict):
                    raw_errors = raw_findings.get("errors", [])
                    if isinstance(raw_errors, list):
                        error_count += len(raw_errors)
                
                row_label = f"{status_icon} 📦 **{component}**"
                if instance_name and instance_name != "default":
                    row_label += f" | **{instance_name}**"
                row_label += f" | {status}"
                if error_count > 0:
                    row_label += f" ({error_count} error{'s' if error_count > 1 else ''})"
                
                # Create expandable for each service
                with st.expander(row_label, expanded=is_expanded):
                    # Update session state when expanded
                    if service_key not in st.session_state["expanded_services"]:
                        st.session_state["expanded_services"].append(service_key)
                    
                    # Render the evidence content (Metrics/Logs/Errors/Raw JSON)
                    render_instance_evidence(instance_data, component, host_name, instance_name)
    
    # Handle navigation from Evidence Highlights tab (if needed)
    nav_target = st.session_state.get("nav_to_evidence")
    if nav_target:
        target_component = nav_target.get("component")
        target_host = nav_target.get("host")
        target_instance = nav_target.get("instance")
        
        # Find and expand the target service
        target_key = f"{target_host}_{target_component}_{target_instance or 'default'}"
        if target_key not in st.session_state["expanded_services"]:
            st.session_state["expanded_services"].append(target_key)
            st.rerun()
        
        # Clear nav target after handling
        del st.session_state["nav_to_evidence"]
    
    # Old tree/viewer code removed - now using unified expandable list above
    # Collection Metadata at the bottom
        # Make Evidence Tree collapsible
        with st.expander("📂 Evidence Tree", expanded=False):
            # Get currently selected node for highlighting
            selected_node = st.session_state.get("selected_tree_node")
            
            # Handle navigation from Evidence Highlights tab
            nav_target = st.session_state.get("nav_to_evidence")
            if nav_target and not selected_node:
                # Find and auto-select the target component/host
                target_component = nav_target.get("component")
                target_host = nav_target.get("host")
                target_instance = nav_target.get("instance")
                
                if target_component in evidence_tree and target_host in evidence_tree[target_component]:
                    instance_data = evidence_tree[target_component][target_host]
                    
                    # If target_instance is specified and instance_data has nested instances, find the specific instance
                    if target_instance:
                        nested_instances = instance_data.get("instances", [])
                        if nested_instances and isinstance(nested_instances, list):
                            for inst in nested_instances:
                                if inst.get("name") == target_instance:
                                    instance_data = inst
                                    break
                    
                    st.session_state["selected_tree_node"] = {
                        "component": target_component,
                        "host": target_host,
                        "instance": target_instance,
                        "data": instance_data.copy()
                    }
                selected_node = st.session_state["selected_tree_node"]
                # Clear nav target after handling
                del st.session_state["nav_to_evidence"]
        
            # Restructure tree: Group by Host first, then Components
            # Build host -> components mapping
            host_components_map = {}
            for component_name, component_data in evidence_tree.items():
                for host_name, instance_data in component_data.items():
                    if host_name not in host_components_map:
                        host_components_map[host_name] = {}
                    host_components_map[host_name][component_name] = instance_data
            
            # Tree navigation: Host -> Components
            # Use session state to track which hosts are expanded (since we can't nest expanders)
            # Use a list instead of set (Streamlit session state works better with lists)
            if "expanded_hosts" not in st.session_state:
                st.session_state["expanded_hosts"] = []
            
            for host_name in sorted(host_components_map.keys()):
                components = host_components_map[host_name]
                component_count = len(components)
                
                # Check if any component on this host is selected
                is_host_selected = (
                selected_node is not None and
                    selected_node.get("host") == host_name
                )
                
                # Auto-expand if host has selected item
                if is_host_selected and host_name not in st.session_state["expanded_hosts"]:
                    st.session_state["expanded_hosts"].append(host_name)
            
                # Check current expanded state
                is_expanded = host_name in st.session_state["expanded_hosts"]
                toggle_key = f"toggle_host_{host_name}"
                
                # Host header with toggle using checkbox (more reliable than button)
                col_toggle, col_label = st.columns([0.1, 0.9])
                with col_toggle:
                    # Use checkbox for toggle - it handles state automatically
                    checkbox_expanded = st.checkbox(
                        "",
                        value=is_expanded,
                        key=toggle_key,
                        label_visibility="collapsed"
                    )
                    # Sync session state with checkbox (checkbox is authoritative)
                    if checkbox_expanded:
                        if host_name not in st.session_state["expanded_hosts"]:
                            st.session_state["expanded_hosts"].append(host_name)
                        # Keep tree expanded when expanding a host
                        st.session_state["evidence_tree_expanded"] = True
                    else:
                        if host_name in st.session_state["expanded_hosts"]:
                            st.session_state["expanded_hosts"].remove(host_name)
                    # Use checkbox value for display (it's the source of truth)
                    is_expanded = checkbox_expanded
                
                with col_label:
                    toggle_display = "▼" if is_expanded else "▶"
                    st.markdown(f"{toggle_display} **🖥️ {host_name}** ({component_count} services)")
                
                # Show components if expanded
                if is_expanded:
                    try:
                        for component_name in sorted(components.keys()):
                            instance_data = components[component_name]
                        
                            # Safety check: ensure instance_data is a dict
                            if not isinstance(instance_data, dict):
                                st.warning(f"⚠️ Invalid data format for {component_name}")
                                continue
                            
                            # CHECK FOR MULTIPLE INSTANCES
                            nested_instances = instance_data.get("instances", [])
                            if nested_instances and isinstance(nested_instances, list) and len(nested_instances) > 1:
                                # Multiple instances - show them as sub-items
                                # Component header (non-clickable, just for grouping)
                                col_indent_header, col_header = st.columns([0.15, 0.85])
                                with col_indent_header:
                                    st.write("")  # Spacer for indentation
                                with col_header:
                                    st.markdown(f"📦 **{component_name}** ({len(nested_instances)} instances)")
                                
                                # Show each instance as a clickable button
                                # Use enumerate to ensure unique keys even if instance names are duplicated
                                for idx, instance in enumerate(nested_instances):
                                    instance_name = instance.get("name", "unknown")
                                    
                                    # Check if this instance is selected
                                    is_selected = (
                                        selected_node and
                                        selected_node.get("component") == component_name and
                                        selected_node.get("host") == host_name and
                                        selected_node.get("instance") == instance_name
                                    )
                                    
                                    # Check for errors in this instance
                                    errors = instance.get("errors", [])
                                    if not isinstance(errors, list):
                                        errors = []
                                    error_count = len(errors)
                                    
                                    # Color-coded button based on error count
                                    if error_count == 0:
                                        status_emoji = "✅"
                                        button_type = "secondary"
                                    elif error_count < 3:
                                        status_emoji = "⚠️"
                                        button_type = "secondary"
                                    else:
                                        status_emoji = "🔴"
                                        button_type = "secondary"
                                    
                                    if is_selected:
                                        button_type = "primary"
                                    
                                    # Ensure instance_name is not empty
                                    display_name = instance_name or "unnamed-instance"
                                    label = f"    {status_emoji} {display_name}"
                                    if error_count > 0:
                                        label += f" ({error_count} errors)"
                                    
                                    # Include index in key to ensure uniqueness even if instance names are duplicated
                                    button_key = f"tree_btn_{host_name}_{component_name}_{instance_name}_{idx}"
                                    col_indent_inst, col_btn_inst = st.columns([0.15, 0.85])
                                    with col_indent_inst:
                                        st.write("")  # Spacer for indentation
                                    with col_btn_inst:
                                        if st.button(label, key=button_key, 
                                                    use_container_width=True, type=button_type):
                                            st.session_state["selected_tree_node"] = {
                                                "component": component_name,
                                                "host": host_name,
                                                "instance": instance_name,
                                                "data": instance.copy()
                                            }
                                            st.session_state["active_main_tab"] = "evidence"
                                            st.query_params["tab"] = "evidence"
                            else:
                                # Single instance or legacy structure - show as before
                                # Use the first instance if nested_instances exists, otherwise use instance_data
                                if nested_instances and isinstance(nested_instances, list) and len(nested_instances) == 1:
                                    actual_instance = nested_instances[0]
                                else:
                                    actual_instance = instance_data
                            
                                # Check if this is the selected node
                                is_selected = (
                                    selected_node and
                                    selected_node.get("component") == component_name and
                                    selected_node.get("host") == host_name and
                                    not selected_node.get("instance")  # No instance name means it's the single instance
                                )
                                
                                # Check for errors (check multiple locations)
                                errors = actual_instance.get("errors", [])
                                raw_findings = actual_instance.get("raw_findings", {})
                                if raw_findings and isinstance(raw_findings, dict):
                                    raw_errors = raw_findings.get("errors", [])
                                    if raw_errors and isinstance(raw_errors, list):
                                        if not errors:
                                            errors = []
                                        errors.extend(raw_errors)
                                if not isinstance(errors, list):
                                    errors = []
                                
                                # Also check for errors in legacy structure
                                if not errors:
                                    error_field = actual_instance.get("error")
                                    if error_field:
                                        errors = [error_field] if isinstance(error_field, str) else []
                                
                                error_count = len(errors) if isinstance(errors, list) else 0
                                
                                # Color-coded button based on error count
                                if error_count == 0:
                                    status_emoji = "✅"
                                    button_type = "secondary"
                                elif error_count < 3:
                                    status_emoji = "⚠️"
                                    button_type = "secondary"
                                else:
                                    status_emoji = "🔴"
                                    button_type = "secondary"
                                
                                # Create clickable button for each component
                                # Ensure component_name is not empty
                                display_component = component_name or "unnamed-component"
                                label = f"{status_emoji} 📦 {display_component}"
                                if error_count > 0:
                                    label += f" ({error_count} errors)"
                                
                                # Highlight selected item
                                if is_selected:
                                    button_type = "primary"
                                
                                # Use a unique key that won't conflict with other buttons
                                button_key = f"tree_btn_{host_name}_{component_name}"
                                # Add indentation for components under hosts
                                col_indent, col_btn = st.columns([0.15, 0.85])
                                with col_indent:
                                    st.write("")  # Spacer for indentation
                                with col_btn:
                                    if st.button(label, key=button_key, 
                                                use_container_width=True, type=button_type):
                                        # Store the full instance data
                                        st.session_state["selected_tree_node"] = {
                                            "component": component_name,
                                            "host": host_name,
                                            "data": actual_instance.copy()
                                        }
                                        # Mark that we want to stay on Evidence tab after rerun
                                        st.session_state["active_main_tab"] = "evidence"
                                        # Set query parameter to preserve tab selection
                                        st.query_params["tab"] = "evidence"
                    except Exception as e:
                        st.error(f"⚠️ Tree rendering failed for {host_name}: {e}")
                        import traceback
                        show_tb_key = f"show_tb_tree_{host_name}"
                        if st.checkbox("Show traceback", key=show_tb_key, value=False):
                            st.code(traceback.format_exc(), language="text")
        
        # Also show container-level evidence
        containers = evidence_data.get("containers", {})
        if containers:
            is_container_selected = (
                selected_node is not None and
                selected_node.get("component") == "container"
            )
            with st.expander(f"📦 Containers ({len(containers)})", expanded=is_container_selected):
                try:
                    for container_name in sorted(containers.keys()):
                        container_data = containers[container_name]
                        
                        # Safety check
                        if not isinstance(container_data, dict):
                            st.warning(f"⚠️ Invalid data format for container {container_name}")
                            continue
                        
                        is_selected = (
                            selected_node and
                            selected_node.get("component") == "container" and
                            selected_node.get("host") == container_name
                        )
                        
                        # Check for errors in container
                        errors = container_data.get("errors", [])
                        error_count = len(errors) if isinstance(errors, list) else 0
                        
                        # Color-coded indicator
                        if error_count == 0:
                            status_emoji = "✅"
                        elif error_count < 3:
                            status_emoji = "⚠️"
                        else:
                            status_emoji = "🔴"
                        
                        label = f"{status_emoji} 📦 {container_name}"
                        if error_count > 0:
                            label += f" ({error_count} errors)"
                        
                        button_type = "primary" if is_selected else "secondary"
                        
                        if st.button(label, 
                                    key=f"tree_container_{container_name}", 
                                    use_container_width=True, type=button_type):
                            st.session_state["selected_tree_node"] = {
                                "component": "container",
                                "host": container_name,
                                "data": container_data
                            }
                            # Mark that we want to stay on Evidence tab after rerun
                            st.session_state["active_main_tab"] = "evidence"
                            # Set query parameter to preserve tab selection
                            st.query_params["tab"] = "evidence"
                except Exception as e:
                    st.error(f"⚠️ Container tree rendering failed: {e}")
                    import traceback
                    # Use checkbox to toggle traceback (can't nest expanders)
                    show_tb_key = f"show_tb_container"
                    if st.checkbox("Show traceback", key=show_tb_key, value=False):
                        st.code(traceback.format_exc(), language="text")
    
    # Old Evidence Viewer code removed - using unified expandable list above
    # (This old code block referenced col_viewer and viewer_tabs which no longer exist)
    
    # OLD CODE REMOVED - Unified list is above
    
    # Collection Metadata at the bottom
    if "metadata" in evidence_data:
        metadata = evidence_data["metadata"]
        with st.expander("📋 Collection Metadata", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.text(f"Session ID: {metadata.get('session_id', 'N/A')}")
                st.text(f"Version: {metadata.get('collector_version', 'N/A')}")
                st.text(f"Build: {metadata.get('collector_build', 'N/A')}")
                if metadata.get("issue_time"):
                    st.text(f"Issue Time: {metadata.get('issue_time')}")
            with col2:
                st.text(f"Environment: {metadata.get('environment', 'N/A')}")
                st.text(f"Collected: {metadata.get('collected_at', 'N/A')}")
                st.text(f"Saved: {metadata.get('saved_at', 'N/A')}")
                if metadata.get("observations"):
                    st.text(f"Observations: {metadata.get('observations')}")


# ======================================================================
# TAB 3: EVIDENCE HIGHLIGHTS (Auto-detected Issues Dashboard)
# ======================================================================
with tab_highlights:
    # Tab description
    st.info("💡 *Auto-detected issues and concerns from evidence collection. This dashboard helps you quickly identify and prioritize critical problems before diving into detailed evidence.*")
    
    # Check if evidence file exists
    if not st.session_state["evidence_file"]:
        st.warning("⚠️ No evidence file loaded. Please collect evidence from the **Environment** tab first.")
        st.info("💡 After collecting evidence, auto-detected issues will appear here.")
        st.stop()
    
    evidence_file = st.session_state["evidence_file"]
    evidence_data = load_evidence_file(evidence_file)
    
    # Validate evidence data was loaded
    if evidence_data is None:
        st.error("⚠️ Failed to load evidence data. Please try collecting evidence again.")
        st.stop()
    
    # Handle navigation from concern items
    nav_target = st.session_state.get("nav_to_evidence")
    if nav_target:
        st.info(f"💡 Navigate to Evidence tab to view details for {nav_target.get('component')} / {nav_target.get('host')}")
        # Clear navigation target after showing message
        del st.session_state["nav_to_evidence"]
    
    # Render the highlights dashboard
    render_evidence_highlights(evidence_data)


# ======================================================================
# TAB 4: LLM INPUT REVIEW (Transparency & Security) 
# ======================================================================
with tab_llm_review:
    # Tab description
    st.info("💡 *Transparency and security review before sending to LLM. Review what data will be sent, check token counts, and verify evidence reduction.*")
    
    # Check if evidence file exists
    if not st.session_state["evidence_file"]:
        st.warning("⚠️ No evidence file loaded. Please collect evidence from the **Environment** tab first.")
        st.info("💡 After collecting evidence, you can review what will be sent to the LLM here.")
        st.stop()
    
    evidence_file = st.session_state["evidence_file"]
    evidence_data = load_evidence_file(evidence_file)
    
    # Validate evidence data was loaded
    if evidence_data is None:
        st.error("⚠️ Failed to load evidence data. Please try collecting evidence again.")
        st.stop()
    
    # Render the Evidence Review component
    prepared_evidence, review_metadata = render_llm_input_review(evidence_data)
    
    # Store prepared evidence in session state for use by RCA generation
    if prepared_evidence:
        st.session_state["prepared_evidence_for_llm"] = prepared_evidence
        st.session_state["llm_review_metadata"] = review_metadata


# ======================================================================
# TAB 5: INCIDENT REPORT (Quick Actionable Report)
# ======================================================================
with tab_incident_report:
    # Tab description
    st.info("💡 *Quick, actionable report for immediate incident response. Generate a focused report with immediate actions and what's broken.*")
    
    if not st.session_state["evidence_file"]:
        st.warning("⚠️ No evidence file available. Please collect evidence from the **Environment** tab first.")
        st.info("💡 To view past Incident Reports, go to the **📜 IR History** tab.")
        st.stop()
    
    evidence_file = st.session_state["evidence_file"]
    evidence_data = load_evidence_file(evidence_file)
    
    # Validate evidence data was loaded
    if evidence_data is None:
        st.error("⚠️ Failed to load evidence data. Please try collecting evidence again.")
        st.stop()
    
    # A. Evidence Selection (Collapsible)
    with st.expander("📄 Evidence Selection", expanded=False):
        if "metadata" in evidence_data:
            metadata = evidence_data["metadata"]
            # Create a table for evidence selection without column headers
            evidence_table_data = []
            evidence_table_data.append({"Field": "Session ID", "Value": metadata.get('session_id', 'N/A')})
            evidence_table_data.append({"Field": "Environment", "Value": metadata.get('environment', 'N/A')})
            if metadata.get("issue_time"):
                evidence_table_data.append({"Field": "Issue Time", "Value": metadata.get('issue_time')})
            evidence_table_data.append({"Field": "File", "Value": Path(evidence_file).name})
            evidence_table_data.append({"Field": "Collected", "Value": metadata.get('collected_at', 'N/A')})
            if metadata.get("observations"):
                evidence_table_data.append({"Field": "Observations", "Value": metadata.get('observations')})
            
            # Display as dataframe with hidden column headers and row index
            df = pd.DataFrame(evidence_table_data)
            st.dataframe(
                df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Field": st.column_config.TextColumn("", width="medium"),
                    "Value": st.column_config.TextColumn("", width="large")
                }
            )
    
    # B. Model Selection
    model = st.selectbox("Model", ["gpt-4o-mini", "gpt-4o", "gpt-4.1"], 
                         index=["gpt-4o-mini", "gpt-4o", "gpt-4.1"].index(st.session_state["selected_model"]) if st.session_state["selected_model"] in ["gpt-4o-mini", "gpt-4o", "gpt-4.1"] else 0,
                         key="model_select_ir")
    st.session_state["selected_model"] = model
    
    # B.1. Service Configuration Status
    config_status = get_service_config_status()
    status_col1, status_col2 = st.columns(2)
    with status_col1:
        mode_icon = "🌐" if config_status["mode"] == "Remote" else "💻"
        st.caption(f"{mode_icon} **Mode:** {config_status['mode']}")
    with status_col2:
        api_icon = "✅" if config_status["api_key_configured"] else "❌"
        api_status = "Configured" if config_status["api_key_configured"] else "Not Configured"
        st.caption(f"{api_icon} **API Key:** {api_status}")
    
    # C. Report Generation
    # Check if prepared evidence is available from Evidence Review
    prepared_evidence = st.session_state.get("prepared_evidence_for_llm")
    if prepared_evidence:
        review_metadata = st.session_state.get("llm_review_metadata", {})
        mask_enabled = review_metadata.get("mask_enabled", False)
        filter_enabled = review_metadata.get("filter_enabled", False)
        
        # Create columns to place button next to info message
        info_col, btn_col = st.columns([3, 1])
        with info_col:
            st.info(f"💡 **Prepared evidence available** from Evidence Review tab. "
                    f"Masking: {'✅ Enabled' if mask_enabled else '❌ Disabled'}, "
                    f"Filtering: {'✅ Enabled' if filter_enabled else '❌ Disabled'}. "
                    f"This will be used for report generation.")
        with btn_col:
            generate_ir_btn = st.button("🚨 Generate Incident Report", type="primary", use_container_width=True,
                                        help="Generate a quick, actionable report for immediate incident response.",
                                        key="btn_generate_ir")
    else:
        # No prepared evidence, button spans full width
        generate_ir_btn = st.button("🚨 Generate Incident Report", type="primary", use_container_width=True,
                                    help="Generate a quick, actionable report for immediate incident response.",
                                    key="btn_generate_ir_no_prep")
    
    if generate_ir_btn:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            st.error("❌ Missing OPENAI_API_KEY environment variable.")
            st.stop()
        
        # Show initial status message
        status_container = st.empty()
        with status_container.container():
            st.info("🔄 Generating Incident Report... This may take a few minutes.")
        
        # Create progress tracker
        progress_tracker = create_progress_tracker()
        
        try:
            # Get environment from session state or evidence
            environment = st.session_state.get("selected_environment")
            if not environment and evidence_file:
                # Try to extract from evidence file
                try:
                    with open(evidence_file, 'r') as f:
                        evidence_data = json.load(f)
                        environment = evidence_data.get("metadata", {}).get("environment") or evidence_data.get("environment")
                except:
                    pass
            
            # Use feature flag to get appropriate client (RCAClient or LLMReasoner)
            reasoner = get_reasoning_client(api_key=api_key, model=model, environment=environment)
            
            if prepared_evidence:
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
                    json.dump(prepared_evidence, tmp_file, indent=2)
                    tmp_evidence_file = tmp_file.name
                
                ir_text = reasoner.generate_incident_response_report(tmp_evidence_file, progress_callback=progress_tracker.callback)
                
                try:
                    os.unlink(tmp_evidence_file)
                except Exception:
                    pass
            else:
                ir_text = reasoner.generate_incident_response_report(evidence_file, progress_callback=progress_tracker.callback)
                
            st.session_state["ir_text"] = ir_text
            
            # Clear status container and show success
            status_container.empty()
            
            # Save IR to file
            if _save_ir_to_file(evidence_file, ir_text):
                st.success("✅ Incident Report generated and saved successfully!")
            else:
                st.success("✅ Incident Report generated successfully!")
                st.warning("⚠️ Report generated but failed to save to file.")
            
        except Exception as e:
            # Clear status container and show error
            status_container.empty()
            # Display user-friendly error (exception message should already be sanitized by client)
            st.error(f"❌ Incident Report generation failed: {str(e)}")
            # Don't show full exception traceback to users - error message is already user-friendly
            st.stop()
    
    # C. Report Display
    has_ir = "ir_text" in st.session_state
    
    if has_ir:
        ir_text = st.session_state["ir_text"]
        
        # Process IR headings to reduce size and spacing (consistent with RCA tab)
        def process_ir_headings(text):
            """Reduce heading sizes by 50% and convert to title case, and reduce spacing."""
            # Split text into sections to avoid processing headings inside markdown tables
            # Markdown tables are identified by lines starting with "|"
            lines = text.split('\n')
            processed_lines = []
            in_table = False
            
            for line in lines:
                # Check if we're entering or leaving a table
                if line.strip().startswith('|') and '---' not in line:
                    in_table = True
                    processed_lines.append(line)
                    continue
                elif in_table and not line.strip().startswith('|'):
                    # Exited table
                    in_table = False
                elif '---' in line and '|' in line:
                    # Table separator line
                    in_table = True
                    processed_lines.append(line)
                    continue
                
                # Only process headings if we're not in a table
                if not in_table:
                    # List of headings to process (in order of specificity - longer first)
                    headings = [
                        "IMMEDIATE ACTIONS (Next 15–30 minutes)",
                        "IMMEDIATE ACTIONS (next 15–30 minutes)",
                        "MISSING DATA / FOLLOW-UP REQUESTS",
                        "COMPONENT-LEVEL ACTIONS",
                        "SUMMARY OF ISSUES IDENTIFIED",
                        "CONFIDENCE LEVEL (0–1.0)",
                        "IMMEDIATE ACTIONS",
                        "CONFIDENCE LEVEL",
                    ]
                    
                    processed_line = line
                    for heading in headings:
                        # Convert to title case
                        title_case = heading.title()
                        
                        # Only match headings at start of line (not inside tables or other content)
                        patterns = [
                            # Match at start of line with optional number prefix (case-insensitive)
                            (rf'^(#{{1,3}})\s*\d+\.\s*{re.escape(heading)}\s*$', r'#### ' + title_case, re.IGNORECASE),
                            # Match at start of line without number (case-insensitive)
                            (rf'^(#{{1,3}})\s*{re.escape(heading)}\s*$', r'#### ' + title_case, re.IGNORECASE),
                        ]
                        
                        for pattern, replacement, flags in patterns:
                            processed_line = re.sub(pattern, replacement, processed_line, flags=flags)
                    
                    processed_lines.append(processed_line)
                else:
                    # Inside table - don't process
                    processed_lines.append(line)
            
            text = '\n'.join(processed_lines)
            
            # Reduce spacing between sections (multiple blank lines to single blank line)
            # Replace 3+ consecutive newlines with 2 newlines (one blank line)
            text = re.sub(r'\n{3,}', '\n\n', text)
            
            return text
        
        # Process the IR text
        processed_ir_text = process_ir_headings(ir_text)
        
        # Render as plain markdown to avoid React errors
        # Skip format_incident_response_report which uses HTML
        st.markdown(processed_ir_text)
        
        
        # D. Feedback Section
        st.markdown('### 📝 Feedback')
        st.caption("Help us improve by providing feedback on the accuracy and usefulness of this report.")
        
        # Load existing feedback if available
        existing_feedback = get_feedback_from_json(evidence_file)
        
        with st.form("incident_report_feedback"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                accuracy_rating = st.slider(
                    "Accuracy Rating",
                    min_value=1,
                    max_value=5,
                    value=existing_feedback.get("accuracy_rating", 3) if existing_feedback else 3,
                    help="Rate how accurate this analysis was (1=Very Inaccurate, 5=Very Accurate)"
                )
            
            with col2:
                was_helpful = st.radio(
                    "Was this helpful?",
                    ["Yes", "No"],
                    index=0 if (existing_feedback and existing_feedback.get("was_helpful", True)) or not existing_feedback else 1,
                    horizontal=True
                )
            
            with col3:
                actionable = st.radio(
                    "Were recommendations actionable?",
                    ["Yes", "No"],
                    index=0 if (existing_feedback and existing_feedback.get("actionable", True)) or not existing_feedback else 1,
                    horizontal=True
                )
            
            comments = st.text_area(
                "Comments / Notes",
                value=existing_feedback.get("comments", "") if existing_feedback else "",
                help="Optional: Provide additional feedback or notes about the report",
                height=100
            )
            
            submit_feedback = st.form_submit_button("💾 Save Feedback", type="primary", use_container_width=True)
            
            if submit_feedback:
                feedback_data = {
                    "accuracy_rating": accuracy_rating,
                    "was_helpful": was_helpful == "Yes",
                    "actionable": actionable == "Yes",
                    "comments": comments.strip() if comments else None
                }
                
                # Save feedback to evidence file
                feedback_saved = save_feedback_to_json(evidence_file, feedback_data)
                
                # Also update IR file with feedback if IR exists
                if "ir_text" in st.session_state and feedback_saved:
                    ir_text = st.session_state["ir_text"]
                    _save_ir_to_file(evidence_file, ir_text, feedback_data)
                
                if feedback_saved:
                    st.success("✅ Feedback saved successfully!")
                    st.rerun()
                else:
                    st.error("❌ Failed to save feedback. Please try again.")
        
        
        # Show feedback summary if available (after form)
        if existing_feedback:
            st.info(f"📊 **Saved Feedback:** Accuracy: {existing_feedback.get('accuracy_rating', 'N/A')}/5 | "
                   f"Helpful: {'✅' if existing_feedback.get('was_helpful') else '❌'} | "
                   f"Actionable: {'✅' if existing_feedback.get('actionable') else '❌'}")
            if existing_feedback.get("comments"):
                st.caption(f"💬 Comments: {existing_feedback.get('comments')}")
        
        
        # E. Downloads
        st.subheader("💾 Downloads")
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "Download IR Report (TXT)",
                data=ir_text,
                file_name=f"ir_report_{Path(evidence_file).stem}.txt",
                mime="text/plain",
                key="dl_ir_report_txt"
            )
        with col2:
            evidence_json = json.dumps(evidence_data, indent=2)
            st.download_button(
                "Download Evidence (JSON)",
                data=evidence_json,
                file_name=f"evidence_{Path(evidence_file).stem}.json",
                mime="application/json",
                key="dl_ir_evidence_json"
            )


# ======================================================================
# TAB 6: FULL RCA (Complete Root Cause Analysis)
# ======================================================================
with tab_rca:
    # Tab description
    st.info("💡 *Complete root cause analysis report. Generate a comprehensive RCA with detailed analysis, component summaries, and long-term preventive actions.*")
    
    st.markdown('### 🤖 Generate RCA Report')
    
    if not st.session_state["evidence_file"]:
        st.warning("⚠️ No evidence file available. Please collect evidence from the **Environment** tab first.")
        st.stop()
    
    evidence_file = st.session_state["evidence_file"]
    evidence_data = load_evidence_file(evidence_file)
    
    # Validate evidence data was loaded
    if evidence_data is None:
        st.error("⚠️ Failed to load evidence data. Please try collecting evidence again.")
        st.stop()
    
    # A. Evidence Selection (Collapsible)
    with st.expander("📄 Evidence Selection", expanded=False):
        if "metadata" in evidence_data:
            metadata = evidence_data["metadata"]
            # Create a table for evidence selection without column headers
            evidence_table_data = []
            evidence_table_data.append({"Field": "Session ID", "Value": metadata.get('session_id', 'N/A')})
            evidence_table_data.append({"Field": "Environment", "Value": metadata.get('environment', 'N/A')})
            if metadata.get("issue_time"):
                evidence_table_data.append({"Field": "Issue Time", "Value": metadata.get('issue_time')})
            evidence_table_data.append({"Field": "File", "Value": Path(evidence_file).name})
            evidence_table_data.append({"Field": "Collected", "Value": metadata.get('collected_at', 'N/A')})
            if metadata.get("observations"):
                evidence_table_data.append({"Field": "Observations", "Value": metadata.get('observations')})
            
            # Display as dataframe with hidden column headers and row index
            df = pd.DataFrame(evidence_table_data)
            st.dataframe(
                df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Field": st.column_config.TextColumn("", width="medium"),
                    "Value": st.column_config.TextColumn("", width="large")
                }
            )
    
    # B. Model Selection
    model = st.selectbox("Model", ["gpt-4o-mini", "gpt-4o", "gpt-4.1"], 
                         index=["gpt-4o-mini", "gpt-4o", "gpt-4.1"].index(st.session_state["selected_model"]) if st.session_state["selected_model"] in ["gpt-4o-mini", "gpt-4o", "gpt-4.1"] else 0,
                         key="model_select_rca")
    st.session_state["selected_model"] = model
    
    # B.1. Service Configuration Status
    config_status = get_service_config_status()
    status_col1, status_col2 = st.columns(2)
    with status_col1:
        mode_icon = "🌐" if config_status["mode"] == "Remote" else "💻"
        st.caption(f"{mode_icon} **Mode:** {config_status['mode']}")
    with status_col2:
        api_icon = "✅" if config_status["api_key_configured"] else "❌"
        api_status = "Configured" if config_status["api_key_configured"] else "Not Configured"
        st.caption(f"{api_icon} **API Key:** {api_status}")
    
    # C. RCA Generation Panel
    # Check for feedback from Incident Report
    feedback = get_feedback_from_json(evidence_file)
    feedback_warning = None
    feedback_info = None
    confirm_generate_key = "confirm_rca_with_negative_feedback"
    confirm_no_feedback_key = "confirm_rca_no_feedback"
    
    if not feedback:
        # No feedback provided - evidence is considered unreliable by default
        feedback_warning = (
            "⚠️ **No Feedback Provided:** Incident Report feedback has not been saved.\n\n"
            "**Evidence Reliability:** ⚠️ Unreliable (no validation)\n\n"
            "**Recommendation:** Please review the Incident Report in the **Incident Report** tab "
            "and provide feedback before generating RCA. This ensures the evidence quality is validated."
        )
        confirm_generate = st.checkbox(
            "I understand the evidence is unvalidated and want to generate RCA anyway",
            key=confirm_no_feedback_key
        )
    elif feedback:
        accuracy = feedback.get("accuracy_rating", 5)
        was_helpful = feedback.get("was_helpful", True)
        actionable = feedback.get("actionable", True)
        
        # Determine if feedback is negative
        is_negative = accuracy < 3 or not was_helpful or not actionable
        
        if is_negative:
            feedback_warning = (
                f"⚠️ **Warning:** Incident Report feedback indicates issues:\n"
                f"- Accuracy Rating: {accuracy}/5\n"
                f"- Helpful: {'✅' if was_helpful else '❌'}\n"
                f"- Actionable: {'✅' if actionable else '❌'}\n\n"
                f"**Evidence Reliability:** ⚠️ Poor\n\n"
                f"**Recommendation:** Review the Incident Report feedback before generating RCA. "
                f"The RCA may be based on inaccurate analysis."
            )
            confirm_generate = st.checkbox(
                "I understand the risks and want to generate RCA anyway",
                key=confirm_generate_key
            )
        else:
            # Positive feedback - show summary
            feedback_info = (
                f"✅ **Feedback Summary:**\n"
                f"- Accuracy Rating: {accuracy}/5\n"
                f"- Helpful: ✅\n"
                f"- Actionable: ✅\n\n"
                f"**Evidence Reliability:** ✅ Validated"
            )
            confirm_generate = True  # No confirmation needed for positive feedback
    
    # Show warning/info before button
    if feedback_warning:
        st.warning(feedback_warning)
    elif feedback_info:
        st.info(feedback_info)
    
    # Check if prepared evidence is available from Evidence Review
    prepared_evidence = st.session_state.get("prepared_evidence_for_llm")
    if prepared_evidence:
        review_metadata = st.session_state.get("llm_review_metadata", {})
        mask_enabled = review_metadata.get("mask_enabled", False)
        filter_enabled = review_metadata.get("filter_enabled", False)
        
        # Create columns to place button next to info message
        info_col, btn_col = st.columns([3, 1])
        with info_col:
            st.info(f"💡 **Prepared evidence available** from Evidence Review tab. "
                    f"Masking: {'✅ Enabled' if mask_enabled else '❌ Disabled'}, "
                    f"Filtering: {'✅ Enabled' if filter_enabled else '❌ Disabled'}. "
                    f"This will be used for RCA generation.")
        with btn_col:
            generate_rca_btn = st.button("🚀 Generate RCA", type="primary", use_container_width=True,
                                         key="btn_generate_rca")
    else:
        # No prepared evidence, button spans full width
        generate_rca_btn = st.button("🚀 Generate RCA", type="primary", use_container_width=True,
                                     key="btn_generate_rca_no_prep")
    
    # Check confirmation if no feedback or negative feedback
    if generate_rca_btn:
        if not feedback and not confirm_generate:
            st.error("❌ Please confirm that you understand the evidence is unvalidated before generating RCA.")
            st.stop()
        elif feedback_warning and not confirm_generate:
            st.error("❌ Please confirm that you understand the risks before generating RCA.")
            st.stop()
    
    if generate_rca_btn:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            st.error("❌ Missing OPENAI_API_KEY environment variable.")
            st.stop()
        
        # Show generating message - use a simple container instead of status to avoid React conflicts
        status_container = st.container()
        with status_container:
            st.info("🤖 Generating RCA... This may take a minute.")
            # Create progress tracker with the container
            progress_tracker = create_progress_tracker(status_container)
            
            try:
                # Get environment from session state or evidence
                environment = st.session_state.get("selected_env")
                if not environment and evidence_file:
                    # Try to extract from evidence file
                    try:
                        with open(evidence_file, 'r') as f:
                            evidence_data = json.load(f)
                            environment = evidence_data.get("metadata", {}).get("environment") or evidence_data.get("environment")
                    except:
                        pass
                
                # Use feature flag to get appropriate client (RCAClient or LLMReasoner)
                reasoner = get_reasoning_client(api_key=api_key, model=model, environment=environment)
                
                # Use prepared evidence if available, otherwise use original file
                if prepared_evidence:
                    # Save prepared evidence to temp file for LLMReasoner
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
                        json.dump(prepared_evidence, tmp_file, indent=2)
                        tmp_evidence_file = tmp_file.name
                    
                    rca_text = reasoner.analyze(tmp_evidence_file, progress_callback=progress_tracker.callback)
                    
                    # Clean up temp file
                    try:
                        os.unlink(tmp_evidence_file)
                    except Exception:
                        pass
                else:
                    # Use original evidence file
                    rca_text = reasoner.analyze(evidence_file, progress_callback=progress_tracker.callback)
                
                tasks = reasoner.generate_tasks_from_rca(rca_text)
                
                # Store in session state
                st.session_state["rca_text"] = rca_text
                st.session_state["rca_tasks"] = tasks
                
                # Save RCA to file
                # Try to find corresponding IR file
                evidence_path = Path(evidence_file)
                ir_file = evidence_path.parent / f"{evidence_path.stem}_ir.json"
                ir_file_path = str(ir_file) if ir_file.exists() else None
                
                if _save_rca_to_file(evidence_file, rca_text, tasks, ir_file_path):
                    # Clear status container and show success
                    status_container.empty()
                    st.success("✅ RCA report generated and saved successfully!")
                else:
                    # Clear status container and show success with warning
                    status_container.empty()
                    st.success("✅ RCA report generated successfully!")
                    st.warning("⚠️ Report generated but failed to save to file.")
            
            except Exception as e:
                # Clear status container and show error
                status_container.empty()
                # Display user-friendly error (exception message should already be sanitized by client)
                st.error(f"❌ RCA generation failed: {str(e)}")
                # Don't show full exception traceback to users - error message is already user-friendly
                st.stop()
    
    # C. Report Display
    has_rca = "rca_text" in st.session_state
    
    if has_rca:
        rca_text = st.session_state["rca_text"]
        
        # Process RCA text to reduce heading sizes and convert to title case
        def process_rca_headings(text):
            """Reduce heading sizes by 50% and convert to title case."""
            # List of headings to process (in order of specificity - longer first)
            headings = [
                "IMMEDIATE ACTIONS (next 15–30 minutes)",
                "LONG-TERM / PREVENTIVE ACTIONS",
                "MISSING DATA / FOLLOW-UP REQUESTS",
                "DETAILED ROOT CAUSE ANALYSIS",
                "COMPONENT-LEVEL SUMMARIES",
                "EXECUTIVE SUMMARY",
                "PRIMARY ROOT CAUSE",
                "EVIDENCE SUMMARY",
                "CONFIDENCE LEVEL (0–1.0)",
                "IMMEDIATE ACTIONS",
                "CONFIDENCE LEVEL",
            ]
            
            # Process each heading
            for heading in headings:
                # Convert to title case
                title_case = heading.title()
                
                # Replace various heading formats with smaller heading (#### = h4, which is 50% of h2)
                # Pattern 1: "# 1. EXECUTIVE SUMMARY" -> "#### Executive Summary"
                # Pattern 2: "## EXECUTIVE SUMMARY" -> "#### Executive Summary"
                # Pattern 3: "# EXECUTIVE SUMMARY" -> "#### Executive Summary"
                # Handle both at start of line and in middle of text
                patterns = [
                    # Match at start of line with optional number prefix
                    (rf'^(#{{1,3}})\s*\d+\.\s*{re.escape(heading)}\s*$', r'#### ' + title_case, re.MULTILINE),
                    # Match at start of line without number
                    (rf'^(#{{1,3}})\s*{re.escape(heading)}\s*$', r'#### ' + title_case, re.MULTILINE),
                    # Match anywhere with optional number prefix
                    (rf'(#{{1,3}})\s*\d+\.\s*{re.escape(heading)}\s*', r'#### ' + title_case, 0),
                    # Match anywhere without number
                    (rf'(#{{1,3}})\s*{re.escape(heading)}\s*', r'#### ' + title_case, 0),
                ]
                
                for pattern, replacement, flags in patterns:
                    text = re.sub(pattern, replacement, text, flags=flags)
            
            return text
        
        # Process the RCA text
        processed_rca_text = process_rca_headings(rca_text)
        
        # Completely bypass format_rca_report and render raw markdown to avoid React errors
        # This is a temporary workaround to identify if format_rca_report is the issue
        try:
            # Just render the processed RCA text as markdown (no HTML processing at all)
            # This will show tables as markdown tables which Streamlit handles natively
            st.markdown(processed_rca_text)
        except Exception as e:
            st.error(f"❌ Error rendering RCA report: {str(e)}")
            # Ultimate fallback: show as plain text
            st.text(rca_text)
        
        # D. Task Matrix
        if "rca_tasks" in st.session_state and st.session_state["rca_tasks"]:
            st.subheader("🧩 Task Ownership Matrix")
            
            # Normalize tasks before display to ensure correct Component/Task mapping
            tasks = _normalize_rca_tasks(st.session_state["rca_tasks"])
            
            if tasks and isinstance(tasks, list) and len(tasks) > 0:
                # Convert to table format
                task_rows = []
                for task in tasks:
                    task_rows.append({
                        "Component": task.get("component", "N/A"),
                        "Task": task.get("task", "N/A"),
                        "Owner": task.get("team", task.get("owner", "Infra")),
                        "Priority": task.get("priority", "Medium"),
                        "Effort": task.get("effort", "M")
                    })
                
                st.table(task_rows)
            else:
                st.info("No tasks generated.")
        
        
        # E. Downloads
        st.subheader("💾 Downloads")
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "Download RCA (TXT)",
                data=rca_text,
                file_name=f"rca_report_{Path(evidence_file).stem}.txt",
                mime="text/plain",
                key="dl_rca_report_txt"
            )
        with col2:
            evidence_json = json.dumps(evidence_data, indent=2)
            st.download_button(
                "Download Evidence (JSON)",
                data=evidence_json,
                file_name=f"evidence_{Path(evidence_file).stem}.json",
                mime="application/json",
                key="dl_rca_evidence_json"
            )

# ======================================================================
# TAB 7: IR HISTORY (View Past Incident Reports)
# ======================================================================
# NOTE: This tab works independently of evidence collection.
# It can be accessed at any time to view historical IRs.
with tab_ir_history:
    # Tab description
    st.info("💡 *View past incident reports. Browse and review historical incident response reports from previous sessions.*")
    
    # Get environment from session state
    selected_env = st.session_state.get("selected_env") or None
    if selected_env == "":
        selected_env = None
    
    # Refresh IR history cache if environment changed
    if selected_env and st.session_state.get("ir_history_env") != selected_env:
        try:
            st.session_state["ir_history_cache"] = _preload_ir_history(selected_env)
            st.session_state["ir_history_env"] = selected_env
        except Exception:
            st.session_state["ir_history_cache"] = []
            st.session_state["ir_history_env"] = selected_env
    elif not selected_env:
        # No environment selected, clear cache
        st.session_state["ir_history_cache"] = []
        st.session_state["ir_history_env"] = None
    
    # Check if data is loaded
    historical_irs = st.session_state.get("ir_history_cache", [])
    cache_env = st.session_state.get("ir_history_env")
    
    # Check if we're viewing a specific IR (either from session state or query param)
    query_params = st.query_params
    if "ir_session" in query_params:
        selected_session_id = query_params["ir_session"]
        matching_ir = next((ir for ir in historical_irs if ir["session_id"] == selected_session_id), None)
        if matching_ir:
            st.session_state["selected_historical_ir"] = matching_ir
    
    # If viewing a specific report, show it and hide the list headers
    if st.session_state.get("selected_historical_ir"):
        selected_ir = st.session_state["selected_historical_ir"]
        
        # Back button at the top
        if st.button("← Back to List", key="back_to_ir_list_history"):
            st.session_state["selected_historical_ir"] = None
            # Clear query params
            if "ir_session" in query_params:
                st.query_params.clear()
            st.rerun()
        
        st.markdown("### 📄 Incident Report")
        st.caption(f"**Date:** {selected_ir['date']} | **Session:** {selected_ir['session_id']}")
        
        formatted_ir = format_incident_response_report(selected_ir["ir_text"])
        st.markdown(formatted_ir, unsafe_allow_html=True)
        
        st.stop()  # Stop here - don't show list when viewing a report
    
    # If no cached data or environment changed, show load button
    if not historical_irs or cache_env != selected_env:
        # Show headers only when viewing the list
        st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">📜 Incident Report History</h3>', unsafe_allow_html=True)
        
        st.info("💡 Click the button below to load historical incident reports.")
        st.caption("ℹ️ **Note:** If you don't see the button below, interact with another tab first (e.g., Environment), then return here.")
        col1, col2 = st.columns([3, 1])
        with col1:
            st.caption(f"Environment: **{selected_env}**")
        with col2:
            if st.button("🔄 Load Historical Reports", type="primary", key="ir_history_load_btn"):
                # Load the data
                try:
                    historical_irs = scan_historical_incident_reports(selected_env)
                    st.session_state["ir_history_cache"] = historical_irs
                    st.session_state["ir_history_env"] = selected_env
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error scanning historical incident reports: {e}")
                    import traceback
                    with st.expander("Error details"):
                        st.code(traceback.format_exc())
    else:
        # Data is loaded, show it
        # Show headers only when viewing the list
        st.markdown('<h3 style="font-size: 1.2rem; margin-top: 1rem;">📜 Incident Report History</h3>', unsafe_allow_html=True)
        st.caption(f"📊 Viewing past incident reports for environment: **{selected_env}**")
        
        # Show message if no IRs found
        if not historical_irs:
            st.info("ℹ️ No historical incident reports found.")
            st.info("💡 Go to the **Environment** tab to collect evidence and start a new RCA.")
        else:
            # Create table with clickable rows - select any row to view the report
            table_data = []
            for idx, ir in enumerate(historical_irs):
                # Use IR filename without .json extension
                ir_filename_display = ir.get("ir_file_stem", ir.get("ir_filename", "N/A")).replace("_ir", "")
                trustworthy_text = "✅ Yes" if ir.get("trustworthy", False) else "❌ No"
                
                table_data.append({
                    "Date of Report": ir.get("date", "N/A"),
                    "Date and Time of Incident": ir.get("issue_time", "N/A"),
                    "Observation": ir.get("observation", "N/A"),
                    "Report": ir_filename_display,
                    "Trustworthy": trustworthy_text
                })
            
            df = pd.DataFrame(table_data)
            
            # Display table with row selection enabled - select any row to view report
            st.caption("💡 Select any row (click the checkbox) to view the incident report.")
            
            selected_rows = st.dataframe(
                df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Date of Report": st.column_config.TextColumn("Date of Report", width="small"),
                    "Date and Time of Incident": st.column_config.TextColumn("Date and Time of Incident", width="small"),
                    "Observation": st.column_config.TextColumn("Observation", width="large"),
                    "Report": st.column_config.TextColumn("Report", width="medium"),
                    "Trustworthy": st.column_config.TextColumn("Trustworthy", width="small")
                },
                on_select="rerun",
                selection_mode="single-row",
                key="ir_history_table"
            )
            
            # Handle row selection - show report when row is selected
            if selected_rows and "selection" in selected_rows and "rows" in selected_rows["selection"]:
                selected_row_indices = selected_rows["selection"]["rows"]
                if selected_row_indices:
                    selected_idx = selected_row_indices[0]
                    if selected_idx < len(historical_irs):
                        selected_ir = historical_irs[selected_idx]
                        st.session_state["selected_historical_ir"] = selected_ir
                        st.rerun()
            
            # Handle query parameter for opening via URL (for new tab support)
            query_params = st.query_params
            if "ir_session" in query_params:
                selected_session_id = query_params["ir_session"]
                # Find the IR with matching session_id
                matching_ir = next((ir for ir in historical_irs if ir["session_id"] == selected_session_id), None)
                if matching_ir:
                    st.session_state["selected_historical_ir"] = matching_ir
                    st.rerun()

