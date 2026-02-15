#!/usr/bin/env bash
# sanity/sanity_08_redis.sh
set -euo pipefail

source "$(dirname "$0")/sanity_00_common.sh"

export GRCAI_LOG_LEVEL=DEBUG

header "SANITY 08 — Redis adapter quick test (via CLI)"

# ------------------------------------------------------
# Show which Redis hosts are defined in QA
# ------------------------------------------------------
section "Show environment Redis hosts"
$PY <<PY
from config.config_loader import ConfigLoader
import os

env = ConfigLoader().load_environment(os.environ.get("SANITY_ENV", "initial"))
# In host-centric structure, check hosts for redis service
redis_hosts = []
for host in env.get("hosts", []):
    services = host.get("services", {})
    if "redis" in services:
        redis_hosts.append(host.get("name"))
print("Redis hosts:", redis_hosts)
PY

# ------------------------------------------------------
# Run orchestrator
# ------------------------------------------------------
section "Run orchestrator (redis) via CLI"

output=$(python3 main.py collect \
    --environment "$SANITY_ENV" \
    --components redis \
    --observations "redis sanity test" 2>&1)

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
    # Last resort: find any recent redis evidence file (sorted by time, most recent first)
    evidence=$(find grcai_sessions -name "rca_*.json" -type f 2>/dev/null | sort -r | head -n 1 || true)
fi

if [[ -z "$evidence" ]]; then
    fail "No evidence file produced for redis"
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

# Ping should be PONG
if ! echo "$json" | grep -q '"ping": "PONG"'; then
    fail "Redis ping check failed — no PONG found"
fi

# Should contain some server metrics
if ! echo "$json" | grep -q '"server"'; then
    fail "Redis server metrics missing"
fi

# Ensure no traceback in output
if echo "$output" | grep -qi "traceback"; then
    fail "Traceback detected in orchestrator output"
fi

# Ensure no fatal errors in evidence
if echo "$json" | grep -qi "fatal"; then
    fail "Fatal error found in redis evidence"
fi

pass_msg "Redis sanity test OK"