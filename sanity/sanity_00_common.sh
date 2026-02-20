#!/usr/bin/env bash
set -euo pipefail

# Common header + helper functions for sanity tests.
# All scripts should source this file.

# Environment configuration - defaults to 'initial' (sample env in repo) if not set
export SANITY_ENV="${SANITY_ENV:-initial}"

# Command to run Python in your environment (adjust if needed)
PY="python3"

header() {
  echo ""
  echo "======================================================"
  echo " $1"
  echo "======================================================"
}

section() {
  echo ""
  echo "------------------------------------------------------"
  echo " $1"
  echo "------------------------------------------------------"
}

fail() {
  echo "❌ FAILED: $1"
  exit 1
}

pass_msg() {
  echo "✅ $1"
}

# Run small python snippet safely; prints to stdout on success
run_py() {
  local code="$1"
  $PY - <<'PYCODE' || return 1
${code}
PYCODE
}

# Utility: safe print JSON with jq if available
json_pretty() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  else
    python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))"
  fi
}