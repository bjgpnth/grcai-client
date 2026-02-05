#!/usr/bin/env bash
# sanity/sanity_01_registry.sh
set -euo pipefail
source "$(dirname "$0")/sanity_00_common.sh"

header "SANITY 01 — Registry, Instantiation, Collect()"

#
# 1. Registry loads
#
section "Registry Loads"
if ! $PY - <<'PY'
from connectors.registry import CONNECTOR_REGISTRY
print("Registry keys:", sorted(CONNECTOR_REGISTRY.keys()))
PY
then
    fail "Registry load failed"
fi
pass_msg "Registry loaded"

#
# 2. Connector instantiation
#
section "Connector instantiation"
if ! $PY <<'PY'
from datetime import datetime, timezone
from connectors.registry import CONNECTOR_REGISTRY

now = datetime.now(timezone.utc)

for key, cls in sorted(CONNECTOR_REGISTRY.items()):
    print("Instantiating:", key)
    c = cls(issue_time=now, component_config={})

print("Done.")
PY
then
    fail "Connector instantiation failed"
fi
pass_msg "Instantiation OK"

#
# 3. collect()/collect_for_host() smoke test
#
section "collect() / collect_for_host() smoke test"

if ! $PY <<'PY'
from datetime import datetime, timezone
from connectors.registry import CONNECTOR_REGISTRY
from connectors.host_connectors.local_host_connector import LocalHostConnector

now = datetime.now(timezone.utc)
fake_host = {"name": "local-fallback", "address": "localhost"}

for key, cls in sorted(CONNECTOR_REGISTRY.items()):
    print("Testing connector:", key)
    adapter = cls(issue_time=now, component_config={})

    # Prefer collect() (legacy compatible)
    if hasattr(adapter, "collect"):
        try:
            out = adapter.collect()
        except Exception as e:
            raise RuntimeError(f"{key}.collect() failed: {e}")
    else:
        # Fallback to collect_for_host
        try:
            host_conn = LocalHostConnector(host_info=fake_host)
        except Exception as e:
            raise RuntimeError(f"LocalHostConnector creation failed for {key}: {e}")

        if not hasattr(adapter, "collect_for_host"):
            raise RuntimeError(f"{key} has neither collect() nor collect_for_host()")

        try:
            out = adapter.collect_for_host(fake_host, host_conn)
        except Exception as e:
            raise RuntimeError(f"{key}.collect_for_host() failed: {e}")

    if not isinstance(out, dict):
        raise ValueError(f"{key} returned non-dict result")

    print(f"{key}: OK")

print("Done.")
PY
then
    fail "collect() / collect_for_host() smoke test failed"
fi

pass_msg "collect() / collect_for_host() OK on all connectors"