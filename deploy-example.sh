#!/usr/bin/env bash
# Deploy and execute sta-to-gpkg on ogc-api-processes.
#
# Prerequisites:
#   - Image built and pushed; labels ogcapi.describeProcessing=true (see Dockerfile)
#   - ALLOWED_REGISTRIES on the server includes your image registry
#   - Bearer token with ogcapi:deploy (deploy) and ogcapi:execute (run)
#   - For network=bridge: admin bridge grant for this image+digest (unless you are admin)
#
# Usage:
#   export OGC_API_URL="https://your-server"
#   export OGC_TOKEN="…"
#   export STA_IMAGE="docker.io/yourorg/sta-to-gpkg:1.0"
#   ./deploy-example.sh deploy
#   ./deploy-example.sh execute

set -euo pipefail

OGC_API_URL="${OGC_API_URL:-http://localhost:3000}"
OGC_TOKEN="${OGC_TOKEN:?Set OGC_TOKEN to a Bearer access token}"
STA_IMAGE="${STA_IMAGE:?Set STA_IMAGE e.g. docker.io/yourorg/sta-to-gpkg:1.0}"

IMAGE_HASH="${IMAGE_HASH:-}"
if [[ -z "$IMAGE_HASH" ]]; then
  REPO_DIGEST="$(docker inspect --format='{{index .RepoDigests 0}}' "$STA_IMAGE" 2>/dev/null || true)"
  if [[ -n "$REPO_DIGEST" && "$REPO_DIGEST" == *@sha256:* ]]; then
    IMAGE_HASH="${REPO_DIGEST#*@}"
  else
    IMAGE_ID="$(docker inspect --format='{{.Id}}' "$STA_IMAGE")"
    IMAGE_HASH="sha256:${IMAGE_ID#sha256:}"
  fi
fi

DEPLOY_BODY="$(jq \
  --arg image "$STA_IMAGE" \
  --arg imageHash "$IMAGE_HASH" \
  '.executionUnit.image = $image | .executionUnit.imageHash = $imageHash' \
  deploy-process.json)"

cmd="${1:-deploy}"

case "$cmd" in
  deploy)
    curl -sS -X POST "${OGC_API_URL}/api/1.0/processes" \
      -H "Authorization: Bearer ${OGC_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "$DEPLOY_BODY" | jq .
    ;;
  execute|execute-async)
    STA_URL="${STA_URL:-https://citiobs.demo.secure-dimensions.de/staplustest/v1.1/Observations}"
    EXEC_BODY="$(jq -n \
      --arg url "$STA_URL" \
      '{
        inputs: {
          url: $url,
          top: 500,
          timeout: 30,
          max_observations: 200,
          verbose: false
        }
      }')"
    EXTRA=()
    [[ "$cmd" == "execute-async" ]] && EXTRA=(-H "Prefer: respond-async")
    curl -sS -X POST "${OGC_API_URL}/api/1.0/processes/sta-to-gpkg/execution" \
      -H "Authorization: Bearer ${OGC_TOKEN}" \
      -H "Content-Type: application/json" \
      "${EXTRA[@]}" \
      -d "$EXEC_BODY" | jq .
    ;;
  *)
    echo "Usage: $0 deploy|execute|execute-async" >&2
    exit 1
    ;;
esac
