#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/sanity_00_common.sh"

header "SANITY 04 — Docker host + Docker-backed OS metrics"

section "DockerHostConnector listing + basic exec/read checks"

$PY - <<PY
from connectors.host_connectors.docker_host_connector import DockerHostConnector
from config.config_loader import ConfigLoader
from datetime import datetime, timezone
import os

env = ConfigLoader().load_environment(os.environ.get("SANITY_ENV", "qa"))
hosts = env.get("hosts", [])

print("Testing docker hosts:")
for h in hosts:
    if not h.get("docker"):
        continue
    print(f"  Docker host: {h['name']} @ {h['address']}")
    conn = DockerHostConnector(host_info=h, global_access=env.get("access", {}))
    containers = conn.list_containers() or []
    print("  Containers:", [c.name for c in containers])
    if containers:
        cid = containers[0].id
        print("  uname -a:", conn.exec_in_container(cid, "uname -a").strip())
    conn.close()
PY

section "Run orchestrator (os) - Current collection"

OUT_CURRENT=$($PY main.py collect --environment "$SANITY_ENV" --components os \
      --issue-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
      --observations "os sanity - current")

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
    
host_data = data.get("host", {}).get("os", {})
if host_data:
    findings = host_data.get("findings", {})
    for host_name, finding in findings.items():
        # Check collection mode at top level
        collection_mode = finding.get("collection_mode", "unknown")
        print(f"Host '{host_name}' collection_mode: {collection_mode}")
        
        if "syslog" in finding:
            syslog_data = finding["syslog"]
            if isinstance(syslog_data, dict):
                mode = syslog_data.get("collection_mode", "unknown")
                print(f"  syslog collection_mode: {mode}")
                if mode == "current":
                    print("  ✓ Current collection working")
            else:
                print("  ⚠ syslog is not structured (old format)")
        
        if "auth_log" in finding:
            auth_log_data = finding["auth_log"]
            if isinstance(auth_log_data, dict):
                mode = auth_log_data.get("collection_mode", "unknown")
                print(f"  auth_log collection_mode: {mode}")
                if mode == "current":
                    print("  ✓ Current auth.log collection working")
else:
    print("⚠ No OS host data found")
PY
else
    fail "OS sanity test (current) produced no evidence file"
fi

section "Run orchestrator (os) - Historical collection (1 hour ago)"

# Calculate time 1 hour ago
PAST_TIME=$(date -u -v-1H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)

OUT_HISTORICAL=$($PY main.py collect --environment "$SANITY_ENV" --components os \
      --issue-time "$PAST_TIME" \
      --observations "os sanity - historical")

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

host_data = data.get("host", {}).get("os", {})
if host_data:
    # Check top-level collection mode
    collection_mode = host_data.get("collection_mode", "unknown")
    print(f"Top-level collection_mode: {collection_mode}")
    
    # Check time window at top level
    time_window = host_data.get("time_window")
    if time_window:
        print(f"✓ Time window present: {time_window.get('since')} to {time_window.get('until')}")
    else:
        print("⚠ Time window missing")
    
    findings = host_data.get("findings", {})
    for host_name, finding in findings.items():
        print(f"\nHost '{host_name}':")
        
        # Check syslog
        if finding.get("syslog"):
            syslog_data = finding["syslog"]
            if isinstance(syslog_data, dict):
                mode = syslog_data.get("collection_mode", "unknown")
                log_type = syslog_data.get("type", "unknown")
                source = syslog_data.get("source", "unknown")
                print(f"  syslog - mode: {mode}, type: {log_type}, source: {source}")
                if mode == "historical":
                    print("  ✓ Historical syslog collection working")
                    if syslog_data.get("time_window"):
                        print(f"    Time window: {syslog_data['time_window'].get('since')} to {syslog_data['time_window'].get('until')}")
            else:
                print("  ⚠ syslog is not structured (old format)")
        
        # Check auth_log
        if finding.get("auth_log"):
            auth_log_data = finding["auth_log"]
            if isinstance(auth_log_data, dict):
                mode = auth_log_data.get("collection_mode", "unknown")
                log_type = auth_log_data.get("type", "unknown")
                source = auth_log_data.get("source", "unknown")
                print(f"  auth_log - mode: {mode}, type: {log_type}, source: {source}")
                if mode == "historical":
                    print("  ✓ Historical auth.log collection working")
                    if auth_log_data.get("time_window"):
                        print(f"    Time window: {auth_log_data['time_window'].get('since')} to {auth_log_data['time_window'].get('until')}")
            else:
                print("  ⚠ auth_log is not structured (old format)")
else:
    print("⚠ No OS host data found")
PY
else
    fail "OS sanity test (historical) produced no evidence file"
fi

section "Run orchestrator (os) - Historical collection (time range)"

# Calculate time range (start: 2 hours ago, end: 1 hour ago)
START_TIME=$(date -u -v-2H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%S)
END_TIME=$(date -u -v-1H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)

# Note: CLI currently only supports single issue_time, but we can test with start time
OUT_RANGE=$($PY main.py collect --environment "$SANITY_ENV" --components os \
      --issue-time "$START_TIME" \
      --observations "os sanity - historical range")

echo "$OUT_RANGE"

EVID_RANGE=$(echo "$OUT_RANGE" | grep -o 'grcai_sessions[^"]*\.json' | head -n 1 || true)
if [[ -n "$EVID_RANGE" ]]; then
    echo "::EVIDENCE::$EVID_RANGE"
    echo "✓ Time range test completed (using start time only for now)"
fi

# Basic validations
if echo "$OUT_HISTORICAL" | grep -qi "traceback"; then
    fail "Traceback detected in historical collection output"
fi

pass_msg "DockerHostConnector + OSAdapter OK (current + historical collection)"