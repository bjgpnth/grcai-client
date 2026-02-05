# client/constraint_extractor.py
"""
Extract minimal, non-sensitive constraints from environment configuration.

This module extracts only the essential constraint information (enabled flag
and responsibilities) from the full environment configuration, excluding all
sensitive data like passwords, IP addresses, connection details, etc.

The extracted constraints are embedded in evidence and sent to the central
service to provide context without exposing sensitive configuration.
"""

import logging

logger = logging.getLogger(__name__)


def extract_minimal_constraints(env_config: dict) -> dict:
    """
    Extract minimal constraints from environment configuration.
    
    Only extracts:
    - enabled: boolean flag indicating if component is enabled
    - responsibilities: list of component responsibilities
    
    Excludes all sensitive data (passwords, IPs, connection details, etc.)
    
    Handles both config formats:
    1. Top-level services (legacy): services.nginx.config_expectations
    2. Host-nested services (current): hosts[].services.nginx.config_expectations
    
    Args:
        env_config: Full environment configuration dict (from ConfigLoader)
        
    Returns:
        dict: Minimal constraints in format:
            {
                "component_name": {
                    "enabled": bool,
                    "responsibilities": [str, ...]
                },
                ...
            }
    """
    constraints = {}
    
    if not env_config:
        logger.warning("Empty environment config provided to extract_minimal_constraints")
        return constraints
    
    # First, check for top-level services (legacy format)
    services = env_config.get("services", {})
    if isinstance(services, dict) and services:
        logger.debug(f"Found top-level services: {list(services.keys())}")
        for component_name, component_config in services.items():
            if not isinstance(component_config, dict):
                continue
            
            # Extract config_expectations if present
            expectations = component_config.get("config_expectations", {})
            if not isinstance(expectations, dict):
                continue
            
            # Only extract enabled and responsibilities
            enabled = expectations.get("enabled", True)  # Default to True if not specified
            responsibilities = expectations.get("responsibilities", [])
            
            # Only add if we have meaningful data
            if enabled or responsibilities:
                constraints[component_name] = {
                    "enabled": enabled,
                    "responsibilities": responsibilities if isinstance(responsibilities, list) else []
                }
                logger.debug(f"Extracted constraints for {component_name} from top-level services: enabled={enabled}, responsibilities={len(responsibilities)}")
    
    # Then, check for services nested under hosts[] (current format)
    hosts = env_config.get("hosts", [])
    if isinstance(hosts, list) and hosts:
        logger.debug(f"Found {len(hosts)} hosts, checking for nested services")
        for host in hosts:
            if not isinstance(host, dict):
                continue
            
            host_name = host.get("name") or host.get("address", "unknown")
            host_services = host.get("services", {})
            if not isinstance(host_services, dict):
                continue
            
            logger.debug(f"Host '{host_name}' has services: {list(host_services.keys())}")
            for component_name, component_config in host_services.items():
                if not isinstance(component_config, dict):
                    continue
                
                # Extract config_expectations if present
                expectations = component_config.get("config_expectations", {})
                if not isinstance(expectations, dict):
                    continue
                
                # Only extract enabled and responsibilities
                enabled = expectations.get("enabled", True)  # Default to True if not specified
                responsibilities = expectations.get("responsibilities", [])
                
                # Only add if we have meaningful data
                if enabled or responsibilities:
                    # If component already exists, merge responsibilities (avoid duplicates)
                    if component_name in constraints:
                        existing_resp = set(constraints[component_name]["responsibilities"])
                        new_resp = set(responsibilities if isinstance(responsibilities, list) else [])
                        merged_resp = list(existing_resp | new_resp)
                        constraints[component_name]["responsibilities"] = merged_resp
                        logger.debug(f"Merged constraints for {component_name} from host '{host_name}': responsibilities={merged_resp}")
                    else:
                        constraints[component_name] = {
                            "enabled": enabled,
                            "responsibilities": responsibilities if isinstance(responsibilities, list) else []
                        }
                        logger.debug(f"Extracted constraints for {component_name} from host '{host_name}': enabled={enabled}, responsibilities={len(responsibilities)}")
    
    if not constraints:
        logger.warning("No config_expectations found in environment config (checked both top-level services and host-nested services)")
    else:
        logger.info(f"Extracted minimal constraints for {len(constraints)} components: {list(constraints.keys())}")
    
    return constraints
