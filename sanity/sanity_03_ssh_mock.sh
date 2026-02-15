#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/sanity_00_common.sh"

header "SANITY 03 — SSH connector (real) with debug params"

section "Real SSH exec + orchestrator via main.py (with parameter debug)"

tmpfile=$(mktemp)

# Run Python script, capturing both stdout and stderr to temp file
# JSON goes to stderr (hidden from screen), debug/logs go to stdout (visible)
$PY - <<PY > >(tee "$tmpfile") 2>>"$tmpfile"
from unittest.mock import patch
from orchestrator.session_orchestrator import SessionOrchestrator
from datetime import datetime, timezone
from connectors.host_connectors.ssh_host_connector import SSHHostConnector
import sys
import os

# Wrap the real _connect to print parameters, then perform the real connection
_orig_connect = SSHHostConnector._connect

def debug_connect(self):
    print("DEBUG SSH params ->",
          f"address={getattr(self, 'address', None)}",
          f"port={getattr(self, 'port', None)}",
          f"username={getattr(self, 'username', None)}",
          f"key_path={getattr(self, 'key_path', None)}",
          f"auth_method={getattr(self, 'auth_method', None)}",
          f"allow_agent={getattr(self, 'allow_agent', None)}",
          f"use_paramiko={getattr(self, 'use_paramiko', None)}")
    return _orig_connect(self)

with patch("connectors.host_connectors.ssh_host_connector.SSHHostConnector._connect", debug_connect):

    orch = SessionOrchestrator()
    res = orch.run_non_interactive(
        issue_time=datetime.now(timezone.utc),
        components=["os"],
        observations="ssh-sanity",
        environment=os.environ.get("SANITY_ENV", "initial")
    )

    import json
    # Print JSON to stderr so it doesn't appear on screen
    print(json.dumps(res, indent=2), file=sys.stderr)
PY
EVID=$(grep -o 'grcai_sessions[^"]*\.json' "$tmpfile" | head -n 1 || true)
if [[ -n "$EVID" ]]; then
    echo "::EVIDENCE::$EVID"
else
    fail "Mock SSH test produced no evidence file"
fi

pass_msg "Mocked SSH test OK"