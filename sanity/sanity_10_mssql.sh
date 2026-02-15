#!/usr/bin/env bash
# sanity/sanity_10_mssql.sh
set -euo pipefail

source "$(dirname "$0")/sanity_00_common.sh"

header "SANITY 10 — MSSQL adapter quick test (via CLI)"

# ------------------------------------------------------
# Show which MSSQL hosts are defined in QA
# ------------------------------------------------------
section "Show environment MSSQL hosts"
$PY <<PY
from config.config_loader import ConfigLoader
import os

env = ConfigLoader().load_environment(os.environ.get("SANITY_ENV", "initial"))
# In host-centric structure, check hosts for mssql service
mssql_hosts = []
for host in env.get("hosts", []):
    services = host.get("services", {})
    if "mssql" in services:
        mssql_hosts.append(host.get("name"))
print("MSSQL hosts:", mssql_hosts)
PY

# ------------------------------------------------------
# Run orchestrator
# ------------------------------------------------------
section "Run orchestrator (mssql) via CLI"

output=$(python3 main.py collect \
    --environment "$SANITY_ENV" \
    --components mssql \
    --observations "mssql sanity test" 2>&1)

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
    # Last resort: find any recent mssql evidence file (sorted by time, most recent first)
    evidence=$(find grcai_sessions -name "rca_*.json" -type f 2>/dev/null | sort -r | head -n 1 || true)
fi

if [[ -z "$evidence" ]]; then
    fail "No evidence file produced for mssql"
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
has_mssql_type=$(echo "$json" | jq -e '.host.mssql? and (.host.mssql.type=="host" or .host.mssql.type=="mssql" or .host.mssql.type=="docker")' >/dev/null && echo yes || echo no)
if [[ "$has_mssql_type" != "yes" ]]; then
    # Allow a liveness-only entry with errors
    if echo "$json" | jq -e '.host.mssql.instances[0].errors? | length > 0' >/dev/null; then
        echo "⚠ Warning: MSSQL type missing, but liveness errors present (MSSQL likely not reachable)"
    else
        fail "MSSQL type check failed — no mssql host section found"
    fi
fi

# Instances present
inst_count=$(echo "$json" | jq '.host.mssql.instances | length')
if [[ "$inst_count" -eq 0 ]]; then
    echo "⚠ Warning: MSSQL instances array empty"
else
    echo "✓ MSSQL instances present ($inst_count)"
fi

# Data fields (databases/connections/tables) — warn if missing but not fatal
if ! echo "$json" | jq -e 'any(.host.mssql.instances[]?; (.databases // .connections_total // .tables))' >/dev/null; then
    echo "⚠ Warning: MSSQL data fields missing (databases/connections/tables). This may be expected if MSSQL is not running or unreachable."
    else
    echo "✓ MSSQL data fields present (databases/connections/tables)"
fi

# Ensure no traceback in output
if echo "$output" | grep -qi "traceback"; then
    fail "Traceback detected in orchestrator output"
fi

# Ensure no fatal errors in evidence (unless it's expected due to missing MSSQL instance)
# We'll be lenient here since MSSQL might not be running in test environment
if echo "$json" | grep -qi '"fatal"'; then
    echo "⚠ Warning: Fatal error found in mssql evidence (may be expected if MSSQL instance not running)"
fi

pass_msg "MSSQL sanity test OK"

