# ui/components/evidence_tree.py
"""
Evidence tree navigation component.

Builds hierarchical tree structure from evidence JSON for navigation.
"""
import copy


def build_evidence_tree(evidence_data):
    """
    Build hierarchical tree structure from evidence data.
    
    Returns:
        dict: {
            "os": {
                "ubuntu-node": {...instance data...},
                "local-docker-host": {...instance data...}
            },
            "tomcat": {
                "local-docker-host": {...instance data...}
            },
            ...
        }
    """
    tree = {}
    
    # Process host-level evidence (components)
    host_data = evidence_data.get("host", {})
    
    for component_name, component_data in host_data.items():
        if not isinstance(component_data, dict):
            continue
            
        # Prioritize instances structure (has proper metrics/logs structure)
        # Fall back to findings structure only if instances are not available
        findings = component_data.get("findings", {})
        instances = component_data.get("instances", [])
        
        if instances:
            # Use instances structure - has proper metrics/logs dictionaries
            if component_name not in tree:
                tree[component_name] = {}
            
            # Try to get host names from metadata
            metadata = evidence_data.get("metadata", {})
            host_collection_status = metadata.get("host_collection_status", {})
            
            # Group instances by host (best effort)
            # First, collect all instances per host to avoid duplicates
            instances_by_host = {}
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
                
                # Group instances by host
                if host_name not in instances_by_host:
                    instances_by_host[host_name] = []
                # Make a copy of the instance to avoid mutating the original
                instance_copy = copy.deepcopy(instance)
                instances_by_host[host_name].append(instance_copy)
            
            # Now set the tree structure with all instances for each host
            for host_name, host_instances in instances_by_host.items():
                if len(host_instances) > 1:
                    # Multiple instances - store as dict with instances list
                    tree[component_name][host_name] = {
                        "instances": host_instances
                    }
                elif len(host_instances) == 1:
                    # Single instance - store directly
                    tree[component_name][host_name] = host_instances[0]
                # else: no instances (shouldn't happen, but skip if it does)
        elif findings:
            # Fallback: Use findings structure - keys are host names
            if component_name not in tree:
                tree[component_name] = {}
            
            for host_name, finding_data in findings.items():
                # For components like nginx/tomcat, instances are nested in findings
                nested_instances = finding_data.get("instances", [])
                if nested_instances:
                    # Multiple instances per host - store all instances under host name
                    tree[component_name][host_name] = finding_data
                else:
                    # Single instance per host - use finding_data directly
                    tree[component_name][host_name] = finding_data
    
    return tree


def get_evidence_summary(evidence_data):
    """
    Get summary statistics from evidence.
    
    Returns:
        dict: {
            "total_components": int,
            "total_hosts": int,
            "total_errors": int,
            "collection_mode": str,
            "time_window": dict or None,
            "per_host_summary": dict,  # NEW: per-host breakdown
            "requested_components": list  # NEW: list of requested components
        }
    """
    from datetime import datetime, timezone
    
    host_data = evidence_data.get("host", {})
    components = list(host_data.keys())
    total_hosts = 0
    total_errors = 0
    collection_mode = None
    time_window = None
    
    # First, try to get collection mode from metadata
    metadata = evidence_data.get("metadata", {})
    
    # Get per-host collection status from metadata
    host_collection_status = metadata.get("host_collection_status", {})
    requested_components = metadata.get("requested_components", [])
    
    # Build per-host summary
    per_host_summary = {}
    for host_name, status in host_collection_status.items():
        requested = status.get("requested", [])
        collected = status.get("collected", [])
        missing = status.get("missing", [])
        unavailable = status.get("unavailable", [])
        unreachable = status.get("unreachable", False)
        
        per_host_summary[host_name] = {
            "requested_count": len(requested),
            "collected_count": len(collected),
            "missing_count": len(missing),
            "unavailable_count": len(unavailable),
            "requested": requested,
            "collected": collected,
            "missing": missing,  # Service not running (not an error)
            "unavailable": unavailable,  # Connection failures
            "unreachable": unreachable
        }
    if metadata and metadata.get("issue_time"):
        # Check if issue_time is in the past (historical collection)
        try:
            issue_time_str = metadata.get("issue_time")
            if issue_time_str:
                # Parse ISO format timestamp
                if "T" in issue_time_str:
                    issue_dt = datetime.fromisoformat(issue_time_str.replace("Z", "+00:00"))
                else:
                    issue_dt = datetime.fromisoformat(issue_time_str)
                
                # Make timezone-aware if needed
                if issue_dt.tzinfo is None:
                    issue_dt = issue_dt.replace(tzinfo=timezone.utc)
                else:
                    issue_dt = issue_dt.astimezone(timezone.utc)
                
                # Check if issue_time is more than 5 minutes in the past
                now = datetime.now(timezone.utc)
                if (now - issue_dt).total_seconds() > 300:  # 5 minutes
                    collection_mode = "historical"
                else:
                    collection_mode = "current"
        except Exception:
            pass  # Fall through to component-level check
    
    for component_name, component_data in host_data.items():
        if not isinstance(component_data, dict):
            continue
        
        # Get collection mode and time window from component level (fallback)
        if collection_mode is None:
            collection_mode = component_data.get("collection_mode", "current")
        
        # Also check instance-level collection_mode
        instances = component_data.get("instances", [])
        if not collection_mode or collection_mode == "current":
            for instance in instances:
                inst_mode = instance.get("collection_mode")
                if inst_mode == "historical":
                    collection_mode = "historical"
                    break
        
        if time_window is None:
            time_window = component_data.get("time_window")
            # Also check instance-level time_window
            if not time_window:
                for instance in instances:
                    inst_tw = instance.get("time_window")
                    if inst_tw:
                        time_window = inst_tw
                        break
        
        # Count hosts: prioritize instances over discovered
        if instances:
            total_hosts += len(instances)
        else:
            # Fall back to discovered for legacy evidence
            discovered = component_data.get("discovered", [])
            total_hosts += len(discovered)
        
        # Count errors from instances
        for instance in instances:
            errors = instance.get("errors", [])
            if isinstance(errors, list):
                total_errors += len(errors)
            # Also check for singular "error" field
            error_field = instance.get("error")
            if error_field and not isinstance(error_field, list):
                total_errors += 1
        
        # Also count from legacy findings
        findings = component_data.get("findings", {})
        for host_name, finding_data in findings.items():
            if not isinstance(finding_data, dict):
                continue
            errors = finding_data.get("errors", [])
            if isinstance(errors, list):
                total_errors += len(errors)
            # Also check for singular "error" field
            error_field = finding_data.get("error")
            if error_field and not isinstance(error_field, list):
                total_errors += 1
    
    return {
        "total_components": len(components),
        "total_hosts": total_hosts,
        "total_errors": total_errors,
        "collection_mode": collection_mode or "current",
        "time_window": time_window,
        "per_host_summary": per_host_summary,
        "requested_components": requested_components
    }

