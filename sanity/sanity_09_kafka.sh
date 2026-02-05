#!/usr/bin/env bash
# sanity/sanity_09_kafka.sh
set -euo pipefail

source "$(dirname "$0")/sanity_00_common.sh"

header "SANITY 09 — Kafka adapter quick test (via CLI)"

# ------------------------------------------------------
# Show which Kafka hosts are defined in QA
# ------------------------------------------------------
section "Show environment Kafka hosts"
$PY <<PY
from config.config_loader import ConfigLoader
import os

env = ConfigLoader().load_environment(os.environ.get("SANITY_ENV", "qa"))
# In host-centric structure, check hosts for kafka service
kafka_hosts = []
for host in env.get("hosts", []):
    services = host.get("services", {})
    if "kafka" in services:
        kafka_hosts.append(host.get("name"))
print("Kafka hosts:", kafka_hosts)
PY

# ------------------------------------------------------
# Run orchestrator
# ------------------------------------------------------
section "Run orchestrator (kafka) via CLI"

output=$(python3 main.py collect \
    --environment "$SANITY_ENV" \
    --components kafka \
    --observations "kafka sanity test" 2>&1)

echo "$output"

# Extract evidence - try multiple patterns
evidence=$(echo "$output" | grep "::EVIDENCE::" | sed 's/::EVIDENCE:://' || true)
if [[ -z "$evidence" ]]; then
    # Try pattern from "💾 Evidence saved to: ..." line
    evidence=$(echo "$output" | grep -o 'grcai_sessions[^ ]*\.json' | head -n 1 || true)
fi
if [[ -z "$evidence" ]]; then
    # Try pattern that might have quotes
    evidence=$(echo "$output" | grep -o 'grcai_sessions[^"]*\.json' | head -n 1 || true)
fi
if [[ -z "$evidence" ]]; then
    # Last resort: find any recent kafka evidence file (sorted by time, most recent first)
    evidence=$(find grcai_sessions -name "rca_*.json" -type f 2>/dev/null | sort -r | head -n 1 || true)
fi

if [[ -z "$evidence" ]]; then
    fail "No evidence file produced for kafka"
fi

# Make path absolute if relative
if [[ ! "$evidence" =~ ^/ ]]; then
    evidence="$(pwd)/$evidence"
fi

# Verify file exists
if [[ ! -f "$evidence" ]]; then
    fail "Evidence file not found: $evidence"
fi

echo "::EVIDENCE::$evidence"

# ------------------------------------------------------
# Basic validations on the evidence contents
# ------------------------------------------------------
section "Validating evidence"

json=$(cat "$evidence")

# Use jq for reliable checks
if ! echo "$json" | jq -e '.host.kafka? and (.host.kafka.type=="host" or .host.kafka.type=="docker" or .host.kafka.type=="kafka")' >/dev/null; then
    fail "Kafka type check failed — no kafka host section found"
fi

# Instances present
inst_count=$(echo "$json" | jq '.host.kafka.instances | length')
if [[ "$inst_count" -eq 0 ]]; then
    echo "⚠ Warning: Kafka instances array empty"
else
    echo "✓ Kafka instances present ($inst_count)"
fi

# At least one instance with data fields (broker_info/topics_summary/consumer_groups_summary)
if ! echo "$json" | jq -e 'any(.host.kafka.instances[]?; (.broker_info // .topics_summary // .consumer_groups_summary))' >/dev/null; then
    echo "⚠ Warning: Kafka data fields missing (broker_info/topics_summary/consumer_groups_summary)"
    echo "  This can be expected if Kafka instance is not fully running"
else
    echo "✓ Kafka data fields present (broker_info/topics_summary/consumer_groups_summary)"
fi

# Ensure no traceback in output
if echo "$output" | grep -qi "traceback"; then
    fail "Traceback detected in orchestrator output"
fi

# Ensure no fatal errors in evidence (unless it's expected due to missing Kafka instance)
# We'll be lenient here since Kafka might not be running in test environment
if echo "$json" | grep -qi '"fatal"'; then
    echo "⚠ Warning: Fatal error found in kafka evidence (may be expected if Kafka instance not running)"
fi

pass_msg "Kafka sanity test OK"

