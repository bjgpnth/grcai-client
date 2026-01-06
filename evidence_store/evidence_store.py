# evidence_store/evidence_store.py
import json
import uuid
import sys
import os
from pathlib import Path
from datetime import datetime, timezone

# Ensure repo root is on Python path for utils imports
_REPO_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT_STR = str(_REPO_ROOT.resolve())

# Add repo root to path (always insert at front to ensure priority)
if _REPO_ROOT_STR not in sys.path:
    sys.path.insert(0, _REPO_ROOT_STR)
else:
    # Even if it's in sys.path, move it to the front for priority
    sys.path.remove(_REPO_ROOT_STR)
    sys.path.insert(0, _REPO_ROOT_STR)

# Also ensure current working directory is on path as fallback
_cwd = os.getcwd()
if _cwd not in sys.path:
    sys.path.insert(0, _cwd)

from evidence_store.evidence_schema import SCHEMA_VERSION

# Import utils.version with error handling
try:
    from utils.version import get_version, get_build
except ImportError:
    # If import still fails, try direct import
    try:
        import importlib.util
        utils_version_path = _REPO_ROOT / "utils" / "version.py"
        if utils_version_path.exists():
            spec = importlib.util.spec_from_file_location("utils.version", utils_version_path)
            utils_version = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(utils_version)
            get_version = utils_version.get_version
            get_build = utils_version.get_build
        else:
            raise ImportError(f"utils/version.py not found at {utils_version_path}")
    except Exception:
        # Final fallback: provide stub functions
        def get_version():
            return "0.0.0-dev"
        def get_build():
            return "unknown"


class EvidenceStore:
    def __init__(self, base_dir="grcai_sessions"):
        self.base_dir = Path(base_dir)

    def save_session(
        self,
        *,
        context,
        host,
        containers,
        os_nodes,
        environment,
        collected_at=None,
        session_id=None,
        host_collection_status=None
    ):
        """
        Save evidence under:
            grcai_sessions/<environment>/rca_YYYY-MM-DD_HH-MM-SS.json
        
        Args:
            context: Legacy context dict (issue_time, components, observations)
            host: Host-level evidence (component findings)
            containers: Container-level evidence
            os_nodes: OS node evidence
            environment: Environment name (e.g., "qa", "prod")
            collected_at: ISO 8601 timestamp when collection started (optional)
            session_id: Pre-generated session ID (optional, will generate if not provided)
        """
        env_dir = self.base_dir / environment
        env_dir.mkdir(parents=True, exist_ok=True)

        # Generate timestamps
        saved_at = datetime.now(timezone.utc)
        
        # Generate session ID if not provided
        if not session_id:
            # Format: rca_2025-11-23T11:33:09Z_550e8400
            timestamp_part = (collected_at or saved_at).strftime("%Y-%m-%dT%H:%M:%SZ")
            uuid_part = str(uuid.uuid4())[:8]
            session_id = f"rca_{timestamp_part}_{uuid_part}"
        
        # Format timestamps with microseconds
        collected_at_str = collected_at if isinstance(collected_at, str) else collected_at.isoformat() if collected_at else saved_at.isoformat()
        saved_at_str = saved_at.isoformat()
        
        # Build metadata
        metadata = {
            "session_id": session_id,
            "collector_version": get_version(),
            "collector_build": get_build(),
            "environment": environment,
            "collected_at": collected_at_str,
            "saved_at": saved_at_str,
            "issue_time": context.get("issue_time") if context else None,
            "observations": context.get("observations") if context else None,
            "requested_components": context.get("components", []) if context else [],
            "host_collection_status": host_collection_status or {},
        }
        
        # Build evidence document with new schema
        data = {
            "schema_version": SCHEMA_VERSION,
            "metadata": metadata,
            "host": host,
            "containers": containers or {},
            "os_nodes": os_nodes or {},
            
            # Legacy fields for backward compatibility
            "context": context,
            "environment": environment,
        }

        # Generate filename from session_id (remove UUID suffix for readability)
        # Format: rca_YYYY-MM-DD_HH-MM-SS.json
        file_timestamp = saved_at.strftime("rca_%Y-%m-%d_%H-%M-%S.json")
        path = env_dir / file_timestamp

        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return str(path)
