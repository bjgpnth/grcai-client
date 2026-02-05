#!/usr/bin/env python3
import json
import sys
from pathlib import Path

def err(msg):
    print(f"  ❌ ERROR: {msg}")

def warn(msg):
    print(f"  ⚠️  WARNING: {msg}")

def validate_file(path: Path):
    errors = 0
    warnings = 0

    try:
        data = json.loads(path.read_text())
    except Exception as e:
        err(f"{path}: invalid JSON ({e})")
        return 1, 0

    print(f"\n▶ FILE: {path}")

    #
    # 1. Top-level keys
    #
    required_top = ["context", "environment", "host", "containers", "os_nodes"]
    for key in required_top:
        if key not in data:
            errors += 1
            err(f"Missing top-level key: {key}")

    # context structure
    ctx = data.get("context", {})
    if not isinstance(ctx, dict):
        errors += 1
        err("context must be an object")

    #
    # 2. host → per component
    #
    host = data.get("host", {})
    if not isinstance(host, dict):
        errors += 1
        err("'host' must be an object mapping component → details")
        return errors, warnings

    # each component: nginx, os, tomcat ...
    for comp_name, comp in host.items():
        if not isinstance(comp, dict):
            errors += 1
            err(f"component '{comp_name}' is not an object")
            continue

        # Required fields
        for field in ["type", "discovered", "findings"]:
            if field not in comp:
                errors += 1
                err(f"{path}: component '{comp_name}' missing '{field}'")
        
        # type must be "host"
        if comp.get("type") != "host":
            errors += 1
            err(f"{path}: component '{comp_name}' type should be 'host'")

        # discovered must be list
        disc = comp.get("discovered")
        if not isinstance(disc, list):
            errors += 1
            err(f"{path}: component '{comp_name}' discovered must be a list")

        # findings must be dict
        findings = comp.get("findings")
        if not isinstance(findings, dict):
            errors += 1
            err(f"{path}: component '{comp_name}' findings must be an object")
            continue

        #
        # Validate each host inside the component
        #
        for host_name, host_data in findings.items():
            if not isinstance(host_data, dict):
                errors += 1
                err(f"{path}: host '{host_name}' in '{comp_name}' must be an object")
                continue

            # For OS adapter we expect cpu/memory/disk/kernel
            if comp_name == "os":
                for f in ["cpu", "memory", "disk", "kernel"]:
                    if f not in host_data:
                        warnings += 1
                        warn(f"{path}: os host '{host_name}' missing '{f}'")

            # For nginx/tomcat we expect instances list
            if comp_name in ("nginx", "tomcat"):
                inst = host_data.get("instances")
                if not isinstance(inst, list):
                    errors += 1
                    err(f"{path}: '{comp_name}' host '{host_name}' missing instances[]")
                else:
                    for i, obj in enumerate(inst):
                        if "name" not in obj:
                            warnings += 1
                            warn(f"{path}: '{comp_name}' instance {i} missing name")
                        if "errors" not in obj:
                            warnings += 1
                            warn(f"{path}: '{comp_name}' instance {i} missing errors list")

    return errors, warnings


def main():
    if len(sys.argv) < 2:
        print("Usage: sanity_json_validator.py <file1.json> <file2.json> ...")
        return 1

    total_errors = 0
    total_warnings = 0

    for file in sys.argv[1:]:
        e, w = validate_file(Path(file))
        total_errors += e
        total_warnings += w

    print("\n======================================================")
    print(f" VALIDATION RESULT — {total_errors} errors, {total_warnings} warnings")
    print("======================================================")

    return 1 if total_errors > 0 else 0


if __name__ == "__main__":
    sys.exit(main())