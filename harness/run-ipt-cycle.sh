#!/usr/bin/env bash
# Full local IPT cycle: execute (with OGC_JOB_ID + Redis progress) → describeProcessing → STAC.
#
#   docker compose -f compose.ipt.yaml up -d redis
#   ./harness/run-ipt-cycle.sh
#   ./harness/run-ipt-cycle.sh harness/input-fixture.example.json
#
# Artifacts: harness/work/{out.gpkg,execute.stderr,describe-context.json,stac-item.json}

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="${ROOT}/compose.ipt.yaml"

dcompose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

INPUT="${1:-${ROOT}/harness/input-smoke.json}"
WORK="${ROOT}/harness/work"
EXEC_STDERR="${WORK}/execute.stderr"
JOB_ID="${JOB_ID:-ipt-local-$(date +%s)}"
PROCESS_ID="${PROCESS_ID:-sta_to_gpkg}"

mkdir -p "$WORK"
cp "$INPUT" "$WORK/input.json"

echo "==> IPT harness job ${JOB_ID}"
echo "    input: ${INPUT}"
echo "    work:  ${WORK}"

echo "==> Starting Redis (port 6380 on host)…"
dcompose up -d redis
dcompose exec -T redis redis-cli ping >/dev/null

echo "==> Building sta-to-gpkg image (picks up sta_to_gpkg.py changes)…"
dcompose build sta-to-gpkg

now_iso() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

seed_job() {
  local pct="${1:-0}"
  local status="${2:-running}"
  local payload
  payload="$(jq -nc \
    --arg jobID "$JOB_ID" \
    --arg processID "$PROCESS_ID" \
    --arg updated "$(now_iso)" \
    --arg status "$status" \
    --argjson processProgress "$pct" \
    '{jobID:$jobID,processID:$processID,status:$status,processProgress:$processProgress,updated:$updated}')"
  dcompose exec -T redis redis-cli SET "ogc:job:${JOB_ID}" "$payload" EX 3600 >/dev/null
}

apply_progress_line() {
  local line="$1"
  if [[ "$line" =~ ^OGC_PROCESSING_PROGRESS:([0-9]+)$ ]]; then
    seed_job "${BASH_REMATCH[1]}"
  elif [[ "$line" =~ ^OGC_PROCESSING_PROGRESS=([0-9]+)$ ]]; then
    seed_job "${BASH_REMATCH[1]}"
  fi
}

sync_redis_from_stderr_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    apply_progress_line "$line"
  done <"$f"
}

is_geopackage_file() {
  local f="$1"
  [[ -f "$f" ]] || return 1
  [[ "$(wc -c <"$f" | tr -d ' ')" -ge 100 ]] || return 1
  [[ "$(head -c 15 "$f")" == "SQLite format 3" ]]
}

report_execute_failure() {
  local exit_code="$1"
  echo "Execute failed (container exit ${exit_code})." >&2
  if [[ -f "$EXEC_STDERR" ]] && [[ -s "$EXEC_STDERR" ]]; then
    echo "--- execute.stderr (last 40 lines) ---" >&2
    tail -40 "$EXEC_STDERR" >&2
  else
    echo "(execute.stderr is empty — check docker network / STA URL in input.json)" >&2
  fi
  if [[ -f "${WORK}/out.gpkg" ]]; then
    local n
    n="$(wc -c <"${WORK}/out.gpkg" | tr -d ' ')"
    echo "stdout size: ${n} bytes (GeoPackage should start with GP)" >&2
    if ! is_geopackage_file "${WORK}/out.gpkg"; then
      echo "stdout looks like an error message, not a GeoPackage (expected SQLite header):" >&2
      head -c 800 "${WORK}/out.gpkg" >&2
      echo "" >&2
    fi
  fi
}

echo "==> [1/3] Execute (OGC_JOB_ID → stderr → Redis ogc:job:…)…"
echo "    Under OGC_JOB_ID there are no human [INFO] logs — only OGC_PROCESSING_PROGRESS / META lines."
echo "    Watch progress: tail -f \"${EXEC_STDERR}\""
seed_job 0
: >"$EXEC_STDERR"

START_TS=$(date +%s)

set +e
dcompose run --rm --no-deps \
  -e "OGC_JOB_ID=${JOB_ID}" \
  sta-to-gpkg <"$WORK/input.json" >"${WORK}/out.gpkg" 2>>"$EXEC_STDERR" &
DCOMP_PID=$!

# Heartbeat while execute runs (avoids background tail -f + wait hangs on macOS).
(
  while kill -0 "$DCOMP_PID" 2>/dev/null; do
    sleep 20
    elapsed=$(( $(date +%s) - START_TS ))
    last="$(grep -E '^OGC_PROCESSING_PROGRESS' "$EXEC_STDERR" 2>/dev/null | tail -1 || true)"
    if [[ -n "$last" ]]; then
      echo "    … ${elapsed}s — ${last}"
    else
      echo "    … ${elapsed}s — still running (STA fetch / export; no progress line yet)"
    fi
  done
) &
HEARTBEAT_PID=$!

wait "$DCOMP_PID"
EXEC_EXIT=$?
set -e

kill "$HEARTBEAT_PID" 2>/dev/null || true
wait "$HEARTBEAT_PID" 2>/dev/null || true

ELAPSED=$(( $(date +%s) - START_TS ))
echo "    Execute container finished in ${ELAPSED}s (exit ${EXEC_EXIT})"

sync_redis_from_stderr_file "$EXEC_STDERR"

if [[ "$EXEC_EXIT" -ne 0 ]]; then
  report_execute_failure "$EXEC_EXIT"
  exit 1
fi

if ! grep -q '^OGC_PROCESSING_META:' "$EXEC_STDERR" 2>/dev/null; then
  echo "Missing OGC_PROCESSING_META in execute.stderr" >&2
  report_execute_failure 0
  exit 1
fi

if ! is_geopackage_file "${WORK}/out.gpkg"; then
  echo "out.gpkg is not a valid GeoPackage (execute may have written an error to stdout)." >&2
  report_execute_failure 0
  exit 1
fi

GPKG_BYTES="$(wc -c <"${WORK}/out.gpkg" | tr -d ' ')"
echo "    GeoPackage: ${GPKG_BYTES} bytes → ${WORK}/out.gpkg"

echo "==> [2/3] Build describeProcessing context (framework stdin)…"
python3 "${ROOT}/harness/build-describe-context.py" \
  --work "$WORK" \
  --job-id "$JOB_ID" \
  --process-id "$PROCESS_ID" \
  >"$WORK/describe-context.json"

echo "==> [3/3] describeProcessing (OGC_ACTION, network none, like production)…"
dcompose run --rm --no-deps \
  -e OGC_ACTION=describeProcessing \
  -e "OGC_JOB_ID=${JOB_ID}" \
  sta-to-gpkg-describe <"$WORK/describe-context.json" >"$WORK/stac-item.json"

seed_job 100 successful

echo ""
echo "==> Done"
echo "    STAC Item:  ${WORK}/stac-item.json"
echo "    Redis key:  ogc:job:${JOB_ID}  (redis-cli -p 6380 GET ogc:job:${JOB_ID})"
echo ""
jq '{type,stac_version,id,geometry,bbox,properties:(.properties|{title,collection,chaincode,start_datetime,end_datetime,"processing:datetime","gpkg:metadata"}),links:(.links|map({rel,href})),assets:{PRODUCT:(.assets.PRODUCT|{title,type,href,"file:checksum","file:size"})}}' \
  "$WORK/stac-item.json"
