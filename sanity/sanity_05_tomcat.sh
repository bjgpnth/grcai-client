#!/usr/bin/env bash
# sanity/sanity_05_tomcat.sh
set -euo pipefail
source "$(dirname "$0")/sanity_00_common.sh"

header "SANITY 05 — Tomcat adapter quick-check"

section "Show environment hosts"

$PY - <<PY
from config.config_loader import ConfigLoader
import os
env = ConfigLoader().load_environment(os.environ.get("SANITY_ENV", "initial"))
print("Using hosts:", [h["name"] for h in env.get("hosts", [])])
PY

section "Run orchestrator (tomcat) - Current collection"

OUT_CURRENT=$($PY main.py collect --environment "$SANITY_ENV" --components tomcat \
      --issue-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
      --observations "tomcat sanity - current")

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
    
host_data = data.get("host", {}).get("tomcat", {})
if host_data:
    findings = host_data.get("findings", {})
    for host_name, finding in findings.items():
        instances = finding.get("instances", [])
        for inst in instances:
            if inst.get("catalina_out_tail"):
                log_data = inst["catalina_out_tail"]
                if isinstance(log_data, dict):
                    mode = log_data.get("collection_mode", "unknown")
                    print(f"Catalina.out collection mode: {mode}")
                    if mode == "current":
                        print("✓ Current collection working")
                    else:
                        print(f"⚠ Unexpected mode: {mode}")
                else:
                    print("⚠ Catalina.out is string (old format)")
PY
else
    fail "Tomcat sanity test (current) produced no evidence file"
fi

section "Run orchestrator (tomcat) - Historical collection (1 hour ago)"

# Calculate time 1 hour ago
PAST_TIME=$(date -u -v-1H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)

OUT_HISTORICAL=$($PY main.py collect --environment "$SANITY_ENV" --components tomcat \
      --issue-time "$PAST_TIME" \
      --observations "tomcat sanity - historical")

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

host_data = data.get("host", {}).get("tomcat", {})
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
            if inst.get("catalina_out_tail"):
                log_data = inst["catalina_out_tail"]
                if isinstance(log_data, dict):
                    mode = log_data.get("collection_mode", "unknown")
                    log_type = log_data.get("type", "unknown")
                    source = log_data.get("source", "unknown")
                    print(f"Catalina.out - mode: {mode}, type: {log_type}, source: {source}")
                    if mode == "historical":
                        print("✓ Historical collection working")
                        if log_data.get("time_window"):
                            print(f"  Time window: {log_data['time_window'].get('since')} to {log_data['time_window'].get('until')}")
                    else:
                        print(f"⚠ Unexpected mode: {mode}")
else:
    print("⚠ No host data found")
PY
else
    fail "Tomcat sanity test (historical) produced no evidence file"
fi

section "Run orchestrator (tomcat) - Historical collection (time range)"

# Calculate time range (start: 2 hours ago, end: 1 hour ago)
START_TIME=$(date -u -v-2H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%S)
END_TIME=$(date -u -v-1H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)

# Note: CLI currently only supports single issue_time, but we can test with start time
# Future enhancement: support --issue-time-start and --issue-time-end
OUT_RANGE=$($PY main.py collect --environment "$SANITY_ENV" --components tomcat \
      --issue-time "$START_TIME" \
      --observations "tomcat sanity - historical range")

echo "$OUT_RANGE"

EVID_RANGE=$(echo "$OUT_RANGE" | grep -o 'grcai_sessions[^"]*\.json' | head -n 1 || true)
if [[ -n "$EVID_RANGE" ]]; then
    echo "::EVIDENCE::$EVID_RANGE"
    echo "✓ Time range test completed (using start time only for now)"
fi

pass_msg "Tomcat adapter OK (current + historical collection)"