#!/usr/bin/env bash
# Re-run describeProcessing from existing harness/work artifacts (skip STA fetch).
#
#   ./harness/describe-only.sh [job-id]

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="${ROOT}/compose.ipt.yaml"

dcompose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

WORK="${ROOT}/harness/work"
JOB_ID="${1:-ipt-local-redescribe}"
PROCESS_ID="${PROCESS_ID:-sta_to_gpkg}"

if [[ ! -f "$WORK/out.gpkg" ]] || [[ ! -f "$WORK/execute.stderr" ]]; then
  echo "Missing ${WORK}/out.gpkg or execute.stderr — run ./harness/run-ipt-cycle.sh first." >&2
  exit 1
fi

python3 "${ROOT}/harness/build-describe-context.py" \
  --work "$WORK" \
  --job-id "$JOB_ID" \
  --process-id "$PROCESS_ID" \
  >"$WORK/describe-context.json"

dcompose run --rm --no-deps \
  -e OGC_ACTION=describeProcessing \
  sta-to-gpkg-describe <"$WORK/describe-context.json" | tee "$WORK/stac-item.json" | jq .
