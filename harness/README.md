# IPT local test harness

Simulates the **OGC API Processes two-container IPT flow** on your machine:

1. **Execute** — `sta_to_gpkg` with `OGC_JOB_ID` (progress on stderr → dummy **Redis** `ogc:job:{id}`)
2. **describeProcessing** — second container run with `OGC_ACTION=describeProcessing` and framework context on stdin → **STAC Item** on stdout

Uses [compose.ipt.yaml](../compose.ipt.yaml) (Redis + built `sta_to_gpkg` image). No full ogc-api-processes stack required.

## Prerequisites

- Docker + Docker Compose v2
- `jq` (progress → Redis + pretty-print STAC)
- Python 3 (harness scripts; same as `sta_to_gpkg.py`)
- Network access from the container to your STA URL (execute only; describe uses the `sta-to-gpkg-describe` service with `network_mode: none`)

## Quick start

```bash
cd sta_to_gpkg   # quote paths if you invoke docker compose manually (spaces in dirname)

# Start dummy Redis (host port 6380 → container 6379)
docker compose -f compose.ipt.yaml up -d redis
# or from anywhere: docker compose -f "/path/to/sta_to_gpkg/compose.ipt.yaml" up -d redis

# Full cycle: execute → build context → describeProcessing
chmod +x harness/run-ipt-cycle.sh harness/describe-only.sh
./harness/run-ipt-cycle.sh

# Or with your own inputs (copy harness/input-fixture.example.json first)
./harness/run-ipt-cycle.sh harness/input-fixture.example.json
```

## Artifacts (`harness/work/`)

| File | Description |
|------|-------------|
| `input.json` | Copy of the request used for execute |
| `out.gpkg` | GeoPackage from execute (stdout) |
| `execute.stderr` | Progress lines + `OGC_PROCESSING_META` |
| `describe-context.json` | Framework stdin for describeProcessing |
| `stac-item.json` | STAC Feature result |

`harness/work/` is gitignored.

## Redis (dummy job store)

Matches ogc-api-processes job keys used for dashboard polling:

- Key: `ogc:job:{JOB_ID}`
- Field updated during execute: `processProgress` (0–100) from `OGC_PROCESSING_PROGRESS:*` stderr lines

```bash
# Inspect job record (default JOB_ID from last run — check script output)
redis-cli -p 6380 GET 'ogc:job:ipt-local-…'

# Or via compose
docker compose -f compose.ipt.yaml exec redis redis-cli GET 'ogc:job:YOUR_JOB_ID'
```

## Commands

| Script | Purpose |
|--------|---------|
| `./harness/run-ipt-cycle.sh [input.json]` | Execute + describe + seed Redis |
| `./harness/describe-only.sh [job-id]` | Re-run describe from existing `work/` |
| `python3 harness/build-describe-context.py --work harness/work --job-id …` | Build context JSON only |

Environment overrides:

- `JOB_ID` — fixed job id (default: `ipt-local-<timestamp>`)
- `PROCESS_ID` — STAC id prefix (default: `sta_to_gpkg`)
- `IPT_HARNESS_IMAGE` — image inspected for IPT labels (default: `sta_to_gpkg:ipt-harness`)
- `IPT_LABELS_FILE` — optional JSON overrides (default: `harness/ipt-labels.json` if that file exists)

## IPT labels (`iptLabels` on describe stdin)

In production, **ogc-api-processes** reads `LABEL ogcapi.processes.ipt.*` from the deployed image (`docker inspect`) and passes them on describe stdin as `iptLabels` (prefix stripped). The harness mirrors that in step **[2/3]** via `build-describe-context.py`.

| Where to set labels | Use for |
|---------------------|---------|
| **[Dockerfile](../Dockerfile)** `LABEL ogcapi.processes.ipt.<name>="…"` | Defaults baked into the image (rebuild with `dcompose build`) |
| **`harness/ipt-labels.json`** | Local overrides without editing the Dockerfile — copy from [ipt-labels.example.json](ipt-labels.example.json) |
| **CLI** | `python3 harness/build-describe-context.py --ipt-labels-file my-labels.json --job-id …` |
| **`--skip-image-labels`** | Only use the JSON file (ignore image labels) |

Example `harness/ipt-labels.json` (keys **without** the `ogcapi.processes.ipt.` prefix — same as production):

```json
{
  "domain": "sensorthings",
  "output": "geopackage",
  "software-provider": "Secure Dimensions GmbH",
  "processing-level": "L2",
  "processing-facility": "SECD"
}
```

| iptLabel key | Used for |
|--------------|----------|
| `software-provider` | `properties.providers` |
| `processing-level` | `processing:level` |
| `processing-facility` | `processing:facility` |
| `processing-version` | `processing:version` + software map |
| **`stac-catalog-root`** | **Required.** STAC API root URL (no trailing slash), e.g. `https://stac.example` |
| **`stac-collection-id`** | **Required.** Target collection id; `properties.collection` + `collection` link |
| **`chaincode`** | **Required.** Catalog chaincode; `properties.chaincode` (worker PUT path segment) |
| `stac-api-prefix` | Optional path before `/collections`, e.g. `stac` → `{root}/stac/collections/…` |
| `stac-license` | Optional `properties.license` |
| (other keys) | copied as-is (e.g. `domain`, `output`) |

`processing:software` is a map (`processId` → version, image name → digest) from `context.docker` (deploy record / worker).

Each Item gets a new **`id`** (UUIDv4). Catalog **links** are built from the labels above:

| `rel` | `href` pattern |
|-------|----------------|
| `root` | `{catalog-root}/[{prefix}/]` |
| `collection` | `…/collections/{stac-collection-id}` |
| `self` | `…/collections/{id}/items/{uuid}` |
| `derived_from` | STA Observations URL (input data) |
| `via` | STA service root when derivable |
| `processing-execution` | OGC API Processes job (`processesApiUrl` + `jobId`) |

Inspect what the harness will send:

```bash
docker inspect sta_to_gpkg:ipt-harness --format '{{json .Config.Labels}}' | jq 'with_entries(select(.key|startswith("ogcapi.processes.ipt.")))'
jq .iptLabels harness/work/describe-context.json
```

## Build / refresh image

```bash
./harness/run-ipt-cycle.sh   # uses quoted -f internally

docker compose -f compose.ipt.yaml build sta-to-gpkg
```

## Compare with bare-metal

Same flow without Compose:

```bash
export OGC_JOB_ID=local-test
python3 sta_to_gpkg.py < harness/input-smoke.json > harness/work/out.gpkg 2> harness/work/execute.stderr
./run-describe-local.sh harness/work/execute.stderr
```

The harness adds **Redis progress mirroring** and **describe via Docker** (`network none`) like production.

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| `Missing OGC_PROCESSING_META` with empty `execute.stderr` | STA unreachable from the container (network/DNS). Test: `dcompose run --rm sta-to-gpkg` with a tiny input. |
| `Missing OGC_PROCESSING_META` but stderr has progress lines | Fixed in `run-ipt-cycle.sh` (older versions used `2> >(…)` and could drop the last stderr line). Pull latest script and re-run. |
| `stdout looks like an error message` | Execute failed; under `OGC_JOB_ID` errors go to **stdout** (not stderr). Read `out.gpkg` as text or see stderr tail in the script output. |
| Redis not updating live | Progress is replayed from `execute.stderr` after execute; check `redis-cli -p 6380 GET ogc:job:…` at the end. |
| Harness stops after `[1/3] Execute` with no output | Execute is running but silent under `OGC_JOB_ID` (no `[INFO]` logs). You should see a heartbeat every 20s; use `tail -f harness/work/execute.stderr`. |
| `unknown flag: --network` on describe | Use `sta-to-gpkg-describe` from `compose.ipt.yaml` (not `docker compose run --network`). |
| STAC `geometry` is a **Point** (bbox center) | Stale Docker image — `run-ipt-cycle.sh` rebuilds the image each run; or `docker compose -f compose.ipt.yaml build sta-to-gpkg`. |

## Link to ogc-api-processes

When the full API is running, deploy with [deploy-process.json](../deploy-process.json) (`iptCompliant: true`, label `ogcapi.describeProcessing=true`). This harness validates the **container contract** in isolation.
