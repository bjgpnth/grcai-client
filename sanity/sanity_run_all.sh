#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/sanity_00_common.sh"

echo "======================================================"
echo " GRCAI — FULL SANITY TEST SUITE"
echo "======================================================"

TEST_NAMES=()
TEST_STATUS=()
TEST_DURATION_MS=()
EVIDENCE_FILES=()

run_test() {
    local f="$1"
    local name
    name="$(basename "$f")"
    TEST_NAMES+=("$name")

    echo ""
    echo ">>> Running: $name"

    # High-res timer
    start_ms=$(python3 - <<'PY'
import time; print(int(time.time()*1000))
PY
)

    # Capture output (used to extract evidence filenames). Allow failures to continue.
    set +e
    output=$(bash "$f" 2>&1)
    rc=$?
    set -e

    echo "$output"

    end_ms=$(python3 - <<'PY'
import time; print(int(time.time()*1000))
PY
)

    duration_ms=$((end_ms - start_ms))
    TEST_DURATION_MS+=("$duration_ms")

    # Extract lines printed inside tests
    # e.g.    ::EVIDENCE::grcai_sessions/qa/rca_...
    while IFS= read -r line; do
        if [[ "$line" == ::EVIDENCE::* ]]; then
            file="${line#::EVIDENCE::}"
            EVIDENCE_FILES+=("$file")
        fi
    done <<< "$output"

    if [[ $rc -eq 0 ]]; then
        TEST_STATUS+=("PASS")
    else
        TEST_STATUS+=("FAIL")
    fi
}

# Run tests in order
for f in "$DIR"/sanity_[0-9][0-9]*.sh; do
    [[ -e "$f" ]] || continue
    case "$(basename "$f")" in
        sanity_00_common.sh|sanity_run_all.sh) continue;;
    esac
    run_test "$f"
done

# SUMMARY
echo ""
echo "======================================================"
echo " TEST SUITE SUMMARY"
echo "======================================================"
printf "%-30s | %-6s | %-10s\n" "TEST" "STATUS" "TIME"
printf "%-30s-+-%-6s-+-%-10s\n" "------------------------------" "------" "----------"

for i in "${!TEST_NAMES[@]}"; do
    printf "%-30s | %-6s | %6s ms\n" \
        "${TEST_NAMES[$i]}" \
        "${TEST_STATUS[$i]}" \
        "${TEST_DURATION_MS[$i]}"
done

# VALIDATION
echo ""
echo "======================================================"
echo " VALIDATING GENERATED EVIDENCE FILES"
echo "======================================================"

if [ ${#EVIDENCE_FILES[@]} -eq 0 ]; then
    echo "No evidence files to validate."
else
    echo "Validating ${#EVIDENCE_FILES[@]} evidence file(s)..."
    python3 "$(dirname "$0")/sanity_validate_reports.py" "${EVIDENCE_FILES[@]}"
    VALIDATION_STATUS=$?
    if [ $VALIDATION_STATUS -ne 0 ]; then
        echo "❌ VALIDATION FAILED"
        exit 1
    fi
fi

echo ""
echo "======================================================"
echo " 🎉 SANITY TESTS COMPLETED"
echo "======================================================"

# Exit non-zero if any test failed
if printf '%s\n' "${TEST_STATUS[@]}" | grep -q "FAIL"; then
    exit 1
else
    exit 0
fi