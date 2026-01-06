# ui/utils/data_masking.py
"""
Data masking utility for LLM Input Review.

Masks sensitive data (IPs, tokens, passwords, etc.) while preserving
analysis value for RCA purposes.
"""

import re
from typing import Dict, Any, List, Optional, Union
import copy


# Pre-compiled regex patterns for masking
MASKING_PATTERNS = {
    "ip_address": re.compile(r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b"),
    "jwt_token": re.compile(r"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*"),
    "api_key": re.compile(r"(?i)(?:api[_-]?key|apikey|authorization)[\s:=]+([A-Za-z0-9_-]{16,})"),
    "bearer_token": re.compile(r"(?i)bearer\s+([A-Za-z0-9_-]{16,})"),
    "password_in_url": re.compile(r"://[^:]+:([^@]+)@"),  # user:pass@host
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    "private_key_path": re.compile(r"(?:/home/|/root/|~\/)[^\s]+(?:\.pem|\.key|\.rsa|\.id_rsa|id_ed25519|\.ssh/)"),
    "connection_string": re.compile(r"(?i)(?:postgresql|mysql|mongodb)://[^:\s]+:[^@\s]+@"),
    "aws_access_key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "aws_secret_key": re.compile(r"(?i)(?:aws[_-]?secret|secret[_-]?key)[\s:=]+([A-Za-z0-9/+=]{20,})"),
    "ssh_key_fingerprint": re.compile(r"(?i)ssh2:\s*(?:RSA|ED25519|ECDSA|DSA)\s+(?:SHA256|SHA1|MD5):([A-Za-z0-9+/=]{20,})"),
}


class DataMasker:
    """Masks sensitive data in evidence structure."""
    
    def __init__(self):
        self.masking_stats = {
            "ips_masked": 0,
            "tokens_masked": 0,
            "emails_masked": 0,
            "paths_masked": 0,
            "other_masked": 0,
        }
    
    def mask_evidence(
        self, 
        evidence_data: Dict[str, Any], 
        mask_sensitive: bool = True
    ) -> Dict[str, Any]:
        """
        Mask sensitive data in evidence structure recursively.
        
        Args:
            evidence_data: Full evidence JSON structure
            mask_sensitive: Whether to apply masking (default: True)
            
        Returns:
            Masked copy of evidence data
        """
        if not mask_sensitive:
            return copy.deepcopy(evidence_data)
        
        # Reset stats
        self.masking_stats = {
            "ips_masked": 0,
            "tokens_masked": 0,
            "emails_masked": 0,
            "paths_masked": 0,
            "other_masked": 0,
        }
        
        # Deep copy to avoid modifying original
        masked_evidence = copy.deepcopy(evidence_data)
        
        # Recursively mask all string values
        self._mask_dict_recursive(masked_evidence)
        
        return masked_evidence
    
    def _mask_dict_recursive(self, obj: Any):
        """Recursively traverse and mask string values in dict/list structures."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                # Skip masking certain metadata fields
                if key in ["session_id", "collected_at", "saved_at", "environment"]:
                    continue
                
                if isinstance(value, str):
                    obj[key] = self._mask_string(value)
                elif isinstance(value, (dict, list)):
                    self._mask_dict_recursive(value)
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                if isinstance(item, str):
                    obj[idx] = self._mask_string(item)
                elif isinstance(item, (dict, list)):
                    self._mask_dict_recursive(item)
    
    def _mask_string(self, text: str) -> str:
        """Apply all masking rules to a string."""
        if not text or not isinstance(text, str):
            return text
        
        masked_text = text
        
        # Mask IP addresses (partial mask: keep network segment)
        def mask_ip(match):
            ip = match.group(0)
            parts = ip.split('.')
            if len(parts) == 4:
                # Keep first two octets, mask last two: 10.45.12.33 -> 10.45.XX.XX
                masked_ip = f"{parts[0]}.{parts[1]}.XX.XX"
                self.masking_stats["ips_masked"] += 1
                return masked_ip
            return ip
        
        masked_text = MASKING_PATTERNS["ip_address"].sub(mask_ip, masked_text)
        
        # Mask JWT tokens
        def mask_jwt(match):
            self.masking_stats["tokens_masked"] += 1
            return "masked_jwt_token"
        
        masked_text = MASKING_PATTERNS["jwt_token"].sub(mask_jwt, masked_text)
        
        # Mask API keys
        def mask_api_key(match):
            self.masking_stats["tokens_masked"] += 1
            # Show first 4 chars, mask rest
            key = match.group(1) if match.lastindex else match.group(0)
            if len(key) > 4:
                return match.group(0).replace(key, key[:4] + "***")
            return "masked_api_key"
        
        masked_text = MASKING_PATTERNS["api_key"].sub(mask_api_key, masked_text)
        masked_text = MASKING_PATTERNS["bearer_token"].sub(mask_api_key, masked_text)
        
        # Mask passwords in URLs
        def mask_password(match):
            self.masking_stats["other_masked"] += 1
            password = match.group(1)
            return match.group(0).replace(password, "***")
        
        masked_text = MASKING_PATTERNS["password_in_url"].sub(mask_password, masked_text)
        
        # Mask emails (keep username, mask domain)
        def mask_email(match):
            email = match.group(0)
            if '@' in email:
                username, domain = email.split('@', 1)
                masked_email = f"{username}@***.{domain.split('.')[-1] if '.' in domain else 'com'}"
                self.masking_stats["emails_masked"] += 1
                return masked_email
            return email
        
        masked_text = MASKING_PATTERNS["email"].sub(mask_email, masked_text)
        
        # Mask private key paths
        def mask_path(match):
            path = match.group(0)
            # Mask user-specific parts: /home/user/.ssh/id_rsa -> /home/***/***/***
            parts = path.split('/')
            masked_parts = []
            for part in parts:
                if part in ['home', 'root', '~']:
                    masked_parts.append(part)
                elif '.' in part or part in ['.ssh', '.pem']:
                    masked_parts.append('***')
                else:
                    masked_parts.append('***')
            self.masking_stats["paths_masked"] += 1
            return '/'.join(masked_parts)
        
        masked_text = MASKING_PATTERNS["private_key_path"].sub(mask_path, masked_text)
        
        # Mask connection strings
        def mask_conn_string(match):
            self.masking_stats["other_masked"] += 1
            return re.sub(r":([^@]+)@", r":***@", match.group(0))
        
        masked_text = MASKING_PATTERNS["connection_string"].sub(mask_conn_string, masked_text)
        
        # Mask AWS access keys
        def mask_aws_key(match):
            self.masking_stats["tokens_masked"] += 1
            key = match.group(0)
            return key[:4] + "***"  # Keep first 4 chars
        
        masked_text = MASKING_PATTERNS["aws_access_key"].sub(mask_aws_key, masked_text)
        
        # Mask AWS secret keys
        def mask_aws_secret(match):
            self.masking_stats["tokens_masked"] += 1
            if match.lastindex:
                return match.group(0).replace(match.group(1), "***")
            return "masked_aws_secret"
        
        masked_text = MASKING_PATTERNS["aws_secret_key"].sub(mask_aws_secret, masked_text)
        
        # Mask SSH key fingerprints (keep key type and algorithm, mask hash)
        def mask_ssh_fingerprint(match):
            self.masking_stats["other_masked"] += 1
            # Extract the full match to preserve key type and algorithm
            full_match = match.group(0)
            # Replace the hash part (after the colon) with ***REDACTED***
            # Pattern: ssh2: ED25519 SHA256:hash -> ssh2: ED25519 SHA256:***REDACTED***
            if ':' in full_match:
                parts = full_match.rsplit(':', 1)  # Split on last colon
                if len(parts) == 2:
                    return f"{parts[0]}:***REDACTED***"
            return full_match.replace(match.group(1) if match.lastindex else "", "***REDACTED***")
        
        masked_text = MASKING_PATTERNS["ssh_key_fingerprint"].sub(mask_ssh_fingerprint, masked_text)
        
        return masked_text
    
    def get_masking_stats(self) -> Dict[str, int]:
        """Get statistics about what was masked."""
        return self.masking_stats.copy()
    
    def detect_sensitive_patterns(self, text: str) -> List[Dict[str, Any]]:
        """
        Detect sensitive patterns in text without masking.
        Useful for showing warnings before masking.
        
        Returns:
            List of detected patterns with type and position
        """
        if not text or not isinstance(text, str):
            return []
        
        detected = []
        
        # Check each pattern
        for pattern_name, pattern in MASKING_PATTERNS.items():
            matches = pattern.finditer(text)
            for match in matches:
                detected.append({
                    "type": pattern_name,
                    "value": match.group(0)[:50],  # Truncate for display
                    "position": match.start()
                })
        
        return detected


def mask_evidence(evidence_data: Dict[str, Any], mask_sensitive: bool = True) -> tuple[Dict[str, Any], Dict[str, int]]:
    """
    Convenience function to mask evidence data.
    
    Args:
        evidence_data: Full evidence JSON structure
        mask_sensitive: Whether to apply masking (default: True)
        
    Returns:
        Tuple of (masked_evidence, masking_stats)
    """
    masker = DataMasker()
    masked_evidence = masker.mask_evidence(evidence_data, mask_sensitive)
    stats = masker.get_masking_stats()
    return masked_evidence, stats

