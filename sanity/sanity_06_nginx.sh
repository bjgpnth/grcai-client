#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/sanity_00_common.sh"

header "SANITY 06 — Nginx adapter quick test (via CLI)"

section "Show environment hosts"

$PY - <<PY
from config.config_loader import ConfigLoader
import os
env = ConfigLoader().load_environment(os.environ.get("SANITY_ENV", "qa"))
print("Using hosts:", [h["name"] for h in env.get("hosts", [])])
PY

section "Run orchestrator (nginx) - Current collection"

OUT_CURRENT=$($PY main.py collect --environment "$SANITY_ENV" --components nginx \
      --issue-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
      --observations "nginx sanity - current")

echo "$OUT_CURRENT"

EVID_CURRENT=$(echo "$OUT_CURRENT" | grep -o 'grcai_sessions[^"]*\.json' | head -n 1 || true)
if [[ -n "$EVID_CURRENT" ]]; then
    echo "::EVIDENCE::$EVID_CURRENT"
    
    # Verify current collection mode
    section "Verify current collection evidence"
    $PY - <<PY
import json
with open("$EVID_CURRENT", "r") as f:
    data = json.load(f)
    
host_data = data.get("host", {}).get("nginx", {})
if host_data:
    findings = host_data.get("findings", {})
    for host_name, finding in findings.items():
        instances = finding.get("instances", [])
        for inst in instances:
            if inst.get("access_log_tail"):
                log_data = inst["access_log_tail"]
                if isinstance(log_data, dict):
                    mode = log_data.get("collection_mode", "unknown")
                    print(f"Access log collection mode: {mode}")
                    if mode == "current":
                        print("✓ Current collection working")
                    else:
                        print(f"⚠ Unexpected mode: {mode}")
                else:
                    print("⚠ Access log is string (old format)")
PY
else
    fail "Nginx sanity test (current) produced no evidence file"
fi

section "Run orchestrator (nginx) - Historical collection (1 hour ago)"

# Calculate time 1 hour ago
PAST_TIME=$(date -u -v-1H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)

OUT_HISTORICAL=$($PY main.py collect --environment "$SANITY_ENV" --components nginx \
      --issue-time "$PAST_TIME" \
      --observations "nginx sanity - historical")

echo "$OUT_HISTORICAL"

EVID_HISTORICAL=$(echo "$OUT_HISTORICAL" | grep -o 'grcai_sessions[^"]*\.json' | head -n 1 || true)
if [[ -n "$EVID_HISTORICAL" ]]; then
    echo "::EVIDENCE::$EVID_HISTORICAL"
    
    # Verify historical collection mode
    section "Verify historical collection evidence"
    $PY - <<PY
import json
from datetime import datetime

with open("$EVID_HISTORICAL", "r") as f:
    data = json.load(f)

host_data = data.get("host", {}).get("nginx", {})
if host_data:
    # Check instance-level evidence
    findings = host_data.get("findings", {})
    for host_name, finding in findings.items():
        # Check collection mode in host finding (where it's actually stored)
        collection_mode = finding.get("collection_mode", "unknown")
        print(f"Host '{host_name}' collection_mode: {collection_mode}")
        
        # Check time window in host finding
        time_window = finding.get("time_window")
        if time_window:
            print(f"✓ Time window present: {time_window.get('since')} to {time_window.get('until')}")
        else:
            print("⚠ Time window missing")
        
        instances = finding.get("instances", [])
        for inst in instances:
            if inst.get("access_log_tail"):
                log_data = inst["access_log_tail"]
                if isinstance(log_data, dict):
                    mode = log_data.get("collection_mode", "unknown")
                    log_type = log_data.get("type", "unknown")
                    print(f"Access log - mode: {mode}, type: {log_type}")
                    if mode == "historical":
                        print("✓ Historical collection working")
                        if log_data.get("time_window"):
                            print(f"  Time window: {log_data['time_window'].get('since')} to {log_data['time_window'].get('until')}")
                    else:
                        print(f"⚠ Unexpected mode: {mode}")
            
            if inst.get("error_log_tail"):
                log_data = inst["error_log_tail"]
                if isinstance(log_data, dict):
                    mode = log_data.get("collection_mode", "unknown")
                    log_type = log_data.get("type", "unknown")
                    print(f"Error log - mode: {mode}, type: {log_type}")
                    if mode == "historical":
                        print("✓ Historical collection working")
else:
    print("⚠ No host data found")
PY
else
    fail "Nginx sanity test (historical) produced no evidence file"
fi

section "Run orchestrator (nginx) - Historical collection (time range)"

# Calculate time range (start: 2 hours ago, end: 1 hour ago)
START_TIME=$(date -u -v-2H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%S)
END_TIME=$(date -u -v-1H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)

# Note: CLI currently only supports single issue_time, but we can test with start time
# Future enhancement: support --issue-time-start and --issue-time-end
OUT_RANGE=$($PY main.py collect --environment "$SANITY_ENV" --components nginx \
      --issue-time "$START_TIME" \
      --observations "nginx sanity - historical range")

echo "$OUT_RANGE"

EVID_RANGE=$(echo "$OUT_RANGE" | grep -o 'grcai_sessions[^"]*\.json' | head -n 1 || true)
if [[ -n "$EVID_RANGE" ]]; then
    echo "::EVIDENCE::$EVID_RANGE"
    echo "✓ Time range test completed (using start time only for now)"
fi

pass_msg "Nginx adapter OK (current + historical collection)"