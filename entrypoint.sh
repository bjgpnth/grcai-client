#!/bin/bash
set -e

# Config home in container is /config (GRCAI_CONFIG_HOME). If empty, seed from repo sample in /grcai/config.
if [ -z "$(ls -A /config 2>/dev/null)" ]; then
    echo "📋 /config is empty, copying sample config..."
    cp -r /grcai/config/initial /grcai/config/template /grcai/config/defaults /config/ 2>/dev/null || true
    cp /grcai/config/reasoning_budget.yaml /grcai/config/env_schema.json /config/ 2>/dev/null || true
    echo "✅ Sample config copied to /config"
fi

# Ensure sessions directory exists (client-specific, sensitive; mount from host or create)
mkdir -p "${GRCAI_SESSIONS_HOME:-/grcai/grcai_sessions}"

# Execute the original command
exec "$@"


