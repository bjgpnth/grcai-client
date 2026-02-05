# client/evidence_sanitizer.py
"""
Comprehensive evidence sanitization for client-side security hardening.

This module provides two layers of sanitization:
1. Structural removal: Removes sensitive fields from the evidence structure
2. Content masking: Masks sensitive patterns in remaining content

This ensures no sensitive data (passwords, IPs, tokens, etc.) is transmitted
to the untrusted central service.
"""

import logging
import copy
from typing import Any, Dict, List, Tuple

# Import DataMasker from UI utils (reuse existing masking logic)
import sys
from pathlib import Path

# Add repo root to path for imports
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ui.utils.data_masking import DataMasker

logger = logging.getLogger(__name__)

# Fields to remove from evidence structure (recursive)
SENSITIVE_FIELDS_TO_REMOVE = [
    "password",
    "api_key",
    "apikey",
    "key_path",
    "address",  # IP addresses and hostnames
    "user",
    "username",
    "database",
    "connection_string",
    "secret",
    "token",
    "auth",
    "credential",
    "private_key",
    "ssh_key",
    "certificate",
    "cert",
    "tls_key",
    "access_key",
    "secret_key",
    "bearer",
    "authorization",
    "jwt",
    "session_id",  # May contain sensitive session info
    "cookie",
    "x-api-key",
    "x-auth-token",
]

# Top-level keys that must NEVER be removed (even if they match sensitive patterns)
PROTECTED_TOP_LEVEL_KEYS = [
    "host",  # Contains all component data - critical!
    "containers",
    "metadata",
    "_constraints",  # Contains minimal constraints - must be preserved
    "schema_version",
    "context",
    "environment",
    "os_nodes",
]


def sanitize_evidence_structure(evidence: dict) -> dict:
    """
    Remove sensitive fields from evidence structure recursively.
    
    This removes entire fields (keys) that match sensitive patterns, while
    preserving the overall structure and protected top-level keys.
    
    Args:
        evidence: Full evidence dictionary
        
    Returns:
        Sanitized evidence with sensitive fields removed
    """
    if not evidence or not isinstance(evidence, dict):
        return evidence
    
    # Deep copy to avoid modifying original
    sanitized = copy.deepcopy(evidence)
    
    # Recursively remove sensitive fields
    _remove_sensitive_fields_recursive(sanitized, path="", is_top_level=True)
    
    return sanitized


def _remove_sensitive_fields_recursive(obj: Any, path: str = "", is_top_level: bool = False):
    """
    Recursively remove sensitive fields from dict/list structures.
    
    Args:
        obj: Object to sanitize (dict, list, or other)
        path: Current path in the structure (for logging)
        is_top_level: Whether this is the top-level dict
    """
    if isinstance(obj, dict):
        keys_to_remove = []
        
        for key, value in obj.items():
            key_lower = key.lower()
            current_path = f"{path}.{key}" if path else key
            
            # Check if this is a protected top-level key
            if is_top_level and key in PROTECTED_TOP_LEVEL_KEYS:
                # Do not remove protected keys, but recurse into their values
                if isinstance(value, (dict, list)):
                    _remove_sensitive_fields_recursive(value, current_path, is_top_level=False)
                continue
            
            # Check if key matches any sensitive pattern
            if any(sensitive in key_lower for sensitive in SENSITIVE_FIELDS_TO_REMOVE):
                keys_to_remove.append(key)
                logger.debug(f"Marked sensitive field for removal: {current_path}")
            elif isinstance(value, (dict, list)):
                # Recurse into nested structures
                _remove_sensitive_fields_recursive(value, current_path, is_top_level=False)
        
        # Remove marked keys
        for key in keys_to_remove:
            del obj[key]
            logger.debug(f"Removed sensitive field: {path}.{key}" if path else f"Removed sensitive field: {key}")
    
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            if isinstance(item, (dict, list)):
                _remove_sensitive_fields_recursive(item, f"{path}[{idx}]", is_top_level=False)


def sanitize_evidence(evidence: dict, mask_sensitive: bool = True) -> Tuple[dict, dict]:
    """
    Comprehensive evidence sanitization: structure removal + content masking.
    
    This function:
    1. Removes sensitive fields from the structure
    2. Masks sensitive patterns in remaining content
    
    Args:
        evidence: Full evidence dictionary
        mask_sensitive: Whether to apply content masking (default: True)
        
    Returns:
        Tuple of (sanitized_evidence, sanitization_stats)
        sanitization_stats contains:
            - fields_removed: count of fields removed
            - ips_masked: count of IPs masked
            - tokens_masked: count of tokens masked
            - emails_masked: count of emails masked
            - etc.
    """
    # Step 1: Remove sensitive fields structurally
    sanitized = sanitize_evidence_structure(evidence)
    
    # Step 2: Apply content masking to remaining data
    if mask_sensitive:
        masker = DataMasker()
        sanitized = masker.mask_evidence(sanitized, mask_sensitive=True)
        masking_stats = masker.get_masking_stats()
    else:
        masking_stats = {
            "ips_masked": 0,
            "tokens_masked": 0,
            "emails_masked": 0,
            "paths_masked": 0,
            "other_masked": 0,
        }
    
    # Combine stats
    stats = {
        "fields_removed": "N/A",  # Hard to count without tracking
        **masking_stats
    }
    
    logger.info(f"Sanitized evidence: {stats.get('ips_masked', 0)} IPs, {stats.get('tokens_masked', 0)} tokens, {stats.get('emails_masked', 0)} emails masked")
    
    return sanitized, stats


def validate_no_sensitive_data(evidence: dict) -> List[str]:
    """
    Validate that evidence contains no obvious sensitive data.
    
    This is a safety check to detect if sanitization failed.
    
    Args:
        evidence: Evidence dictionary to validate
        
    Returns:
        List of violation messages (empty if no violations found)
    """
    violations = []
    
    def check_recursive(obj: Any, path: str = ""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                key_lower = key.lower()
                current_path = f"{path}.{key}" if path else key
                
                # Check for sensitive field names (should have been removed)
                if any(sensitive in key_lower for sensitive in SENSITIVE_FIELDS_TO_REMOVE):
                    # Skip if it's a protected top-level key
                    if path == "" and key in PROTECTED_TOP_LEVEL_KEYS:
                        continue
                    violations.append(f"Sensitive field name detected: {current_path}")
                
                # Recurse
                if isinstance(value, (dict, list)):
                    check_recursive(value, current_path)
                elif isinstance(value, str):
                    # Check for obvious sensitive patterns in values
                    # Skip validation for log content (logs often contain the word "password" in error messages)
                    if ".logs." in current_path or ".log." in current_path or current_path.endswith(".content"):
                        # This is log content - skip validation (logs may contain "password" in error messages)
                        pass
                    else:
                        # For non-log content, check for password-like patterns
                        value_lower = value.lower()
                        # Only flag if it's a long string that might be an actual password value
                        # (not a log message which would contain spaces/newlines)
                        if "password" in value_lower and len(value) > 20:
                            # If it's a single word or mostly alphanumeric without spaces, might be a password
                            stripped = value.replace(" ", "").replace("\n", "").replace("\t", "")
                            if len(stripped) > 20 and (stripped.isalnum() or len(value.split()) < 3):
                                violations.append(f"Possible password in value: {current_path}")
        
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                if isinstance(item, (dict, list, str)):
                    check_recursive(item, f"{path}[{idx}]")
    
    check_recursive(evidence)
    
    if violations:
        logger.warning(f"Found {len(violations)} potential sensitive data violations")
    else:
        logger.debug("No sensitive data violations detected")
    
    return violations
