"""
Evidence schema definitions for GRCAI.

Defines TypedDict schemas for evidence structure, errors, and metadata.
"""
from typing import TypedDict, List, Optional
from datetime import datetime

# Schema version constant
SCHEMA_VERSION = "1.0"


class ErrorEntry(TypedDict, total=False):
    """
    Structured error entry.
    
    All fields are optional except type, component, stage, and message.
    """
    type: str  # e.g., "collection_error", "parsing_error", "connection_error"
    component: str  # e.g., "postgres", "nginx", "os"
    stage: str  # e.g., "log_collection", "config_parsing", "connection"
    message: str  # Human-readable error message
    code: Optional[str]  # Error code (e.g., "ENOCMD", "ECONNREFUSED")
    detail: Optional[str]  # Additional details
    traceback: Optional[str]  # Full traceback if available


class Metadata(TypedDict):
    """Evidence collection metadata."""
    session_id: str  # Format: rca_2025-11-23T11:33:09Z_550e8400
    collector_version: str  # GRCAI version (e.g., "1.2.3")
    collector_build: str  # Git commit/tag (e.g., "abcdef1234")
    environment: str  # Environment name (e.g., "qa", "prod")
    collected_at: str  # ISO 8601 UTC with microseconds
    saved_at: str  # ISO 8601 UTC with microseconds
    issue_time: Optional[str]  # ISO 8601 UTC (from context)
    observations: Optional[str]  # User observations


class EvidenceDocument(TypedDict, total=False):
    """
    Top-level evidence document structure.
    
    Required: schema_version, metadata, host
    Optional: containers, os_nodes, context (legacy), environment (legacy)
    """
    schema_version: str  # Schema version (e.g., "1.0")
    metadata: Metadata
    host: dict  # Component findings organized by component name
    containers: Optional[dict]  # Container-level evidence
    os_nodes: Optional[dict]  # OS node evidence
    
    # Legacy fields (for backward compatibility)
    context: Optional[dict]  # Legacy context (issue_time, components, observations)
    environment: Optional[str]  # Legacy environment field

