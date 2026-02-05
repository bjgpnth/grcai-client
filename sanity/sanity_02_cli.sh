#!/usr/bin/env bash
# sanity/sanity_02_cli.sh
set -euo pipefail
source "$(dirname "$0")/sanity_00_common.sh"

header "SANITY 02 — CLI evidence collection (via main.py)"

extract_evidence() {
    grep -o 'grcai_sessions[^"]*\.json' | head -n 1
}

#
# OS
#
section "OS only"
echo "Running: main.py collect --environment $SANITY_ENV --components os"
OUT=$( $PY main.py collect --environment "$SANITY_ENV" --components os \
       --issue-time "2025-11-16T12:45:00" \
       --observations "os test" )
echo "$OUT"

EVID_OS=$(echo "$OUT" | extract_evidence || true)
[[ -n "$EVID_OS" ]] || fail "OS-only test did not produce a JSON evidence file"
echo "::EVIDENCE::$EVID_OS"

#
# Tomcat
#
section "Tomcat only"
echo "Running: main.py collect --environment $SANITY_ENV --components tomcat"
OUT=$( $PY main.py collect --environment "$SANITY_ENV" --components tomcat \
       --issue-time "2025-11-16T12:45:00" \
       --observations "tomcat test" )
echo "$OUT"

EVID_TOM=$(echo "$OUT" | extract_evidence || true)
[[ -n "$EVID_TOM" ]] || fail "Tomcat-only test did not produce a JSON evidence file"
echo "::EVIDENCE::$EVID_TOM"

#
# Nginx
#
section "Nginx only"
echo "Running: main.py collect --environment $SANITY_ENV --components nginx"
OUT=$( $PY main.py collect --environment "$SANITY_ENV" --components nginx \
       --issue-time "2025-11-16T12:45:00" \
       --observations "nginx test" )
echo "$OUT"

EVID_NGX=$(echo "$OUT" | extract_evidence || true)
[[ -n "$EVID_NGX" ]] || fail "Nginx-only test did not produce a JSON evidence file"
echo "::EVIDENCE::$EVID_NGX"

#
# Postgres
#
section "Postgres only"
echo "Running: main.py collect --environment $SANITY_ENV --components postgres"
OUT=$( $PY main.py collect --environment "$SANITY_ENV" --components postgres \
       --issue-time "2025-11-16T12:45:00" \
       --observations "postgres test" )
echo "$OUT"

EVID_DB=$(echo "$OUT" | extract_evidence || true)
[[ -n "$EVID_DB" ]] || fail "Postgres-only test did not produce a JSON evidence file"
echo "::EVIDENCE::$EVID_DB"

#
# Combined run
#
section "Tomcat + OS + Nginx + Postgres"
echo "Running: main.py collect --environment $SANITY_ENV --components tomcat,os,nginx,postgres"
OUT=$( $PY main.py collect --environment "$SANITY_ENV" --components tomcat,os,nginx,postgres \
       --issue-time "2025-11-16T12:45:00" \
       --observations "combined test" )
echo "$OUT"

EVID_COMBO=$(echo "$OUT" | extract_evidence || true)
[[ -n "$EVID_COMBO" ]] || fail "Combined test did not produce a JSON evidence file"
echo "::EVIDENCE::$EVID_COMBO"

pass_msg "CLI collection tests OK"