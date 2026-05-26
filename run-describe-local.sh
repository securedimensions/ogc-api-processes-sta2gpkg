#!/usr/bin/env bash
# Simulate ogc-api-processes describeProcessing after a local execute.
#
# Usage:
#   ./run.sh                          # or: … | python3 sta_to_gpkg.py > out.gpkg 2> export.log
#   ./run-describe-local.sh export.log
#
# Requires: jq

set -euo pipefail

LOG_FILE="${1:-export.log}"
META_LINE="$(grep '^OGC_PROCESSING_META:' "$LOG_FILE" | head -1 || true)"

if [[ -z "$META_LINE" ]]; then
  echo "No OGC_PROCESSING_META line in $LOG_FILE — run execute first." >&2
  exit 1
fi

PROCESSING_JSON="${META_LINE#OGC_PROCESSING_META:}"

GPKG_BYTES="$(wc -c < out.gpkg 2>/dev/null | tr -d ' ' || echo 0)"

jq -n \
  --arg jobId "local-test" \
  --arg processId "sta_to_gpkg" \
  --arg stderr "$META_LINE" \
  --argjson processing "$PROCESSING_JSON" \
  --argjson outputBytes "${GPKG_BYTES:-0}" \
  '{
    jobId: $jobId,
    processId: $processId,
    inputs: {},
    execution: {
      outputSha256: "sha256:00",
      outputBytes: $outputBytes,
      mediaType: "application/geopackage+sqlite3",
      ipfs: {
        cid: "bafylocal",
        ipfsUri: "ipfs://bafylocal",
        gatewayUrl: "https://gateway.example/ipfs/bafylocal"
      }
    },
    iptLabels: { domain: "sensorthings", output: "geopackage" },
    processing: $processing,
    stderr: $stderr
  }' | OGC_ACTION=describeProcessing python3 sta_to_gpkg.py
