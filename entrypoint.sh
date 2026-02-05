#!/bin/bash
set -e

# Check if mounted config directory is empty
if [ -z "$(ls -A /grcai/config 2>/dev/null)" ]; then
    echo "📋 Config directory is empty, copying templates..."
    cp -r /grcai/config/template/* /grcai/config/ 2>/dev/null || true
    echo "✅ Templates copied to /grcai/config"
else
    echo "✅ Config directory already has files, skipping template copy"
fi

# Ensure sessions directory exists
mkdir -p /grcai/grcai_sessions

# Execute the original command
exec "$@"


