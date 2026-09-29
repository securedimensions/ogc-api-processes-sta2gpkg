# sta_to_gpkg

Export OGC SensorThings API v1.1 Observations to GeoPackage. Designed for [OGC API — Processes](https://github.com/securedimensions/OGC-API-Processes) with **IPT** support (`iptCompliant: true`).

## OData `$metadata` (required)

Before export, the process derives the STA service root from the Observations URL and **requires** OData CSDL JSON at:

```text
{parent-of-STA-root}/ODATA_4.01/$metadata?$format=json
```

Example: `…/staplustest/v1.1/Observations` → `…/staplustest/ODATA_4.01/$metadata?$format=json`.

If that path is missing or unreadable, the process prints an error and exits. Property types from `$metadata` drive GeoPackage column affinities (e.g. `Edm.Int64` → `INTEGER`, geometries → `BLOB`).

## I/O contract

| Stream | Content |
|--------|---------|
| **stdin** | JSON request (`url`, optional `filter`, `max_observations`, …) |
| **stdout** | GeoPackage bytes on success; UTF-8 error text on failure (under worker) |
| **stderr** | Machine-readable lines only under `OGC_JOB_ID` (see below) |

## stderr under the worker (`OGC_JOB_ID`)

Human progress logs are **disabled** in production runs. stderr carries structured lines the server parses:

| Line | Example | Purpose |
|------|---------|---------|
| Progress | `OGC_PROCESSING_PROGRESS:42` | Async job UI (0–100), stored on Redis `processProgress` |
| Metadata | `OGC_PROCESSING_META:{...}` | Execute-phase facts for `describeProcessing` (bbox, table counts, …) |

Use `PYTHONUNBUFFERED=1` (set in the Dockerfile) so progress lines reach the worker while the job runs.

## IPT flow (two containers)

1. **Execute** — GeoPackage on stdout; progress + metadata lines on stderr.
2. **describeProcessing** — Server passes stdin context (`processing`, `iptLabels`, `execution`/IPFS) → STAC Item on stdout.

Deploy with `ogcapi.describeProcessing=true` and `iptCompliant: true`. See [deploy-process.json](deploy-process.json) and [deploy-example.sh](deploy-example.sh).

## Local testing

### 1. Install dependencies

```bash
cd sta_to_gpkg
python3 -m venv .venv && source .venv/bin/activate   # optional
pip install requests shapely
```

### 2. Run an export

Use a real STA **Observations** URL and optional OData `$filter` (same fields as [deploy-process.json](deploy-process.json)):

```bash
cat > input.json <<'EOF'
{
  "url": "https://YOUR-SERVER/v1.1/Observations",
  "filter": "phenomenonTime ge 2024-01-01T00:00:00Z",
  "top": 1000,
  "timeout": 120,
  "verbose": true,
  "max_observations": 10000
}
EOF

python3 sta_to_gpkg.py < input.json > out.gpkg 2> export.log
```

Or use [run.sh](run.sh) (edit the JSON inside the script first).

### 3. Check the result

```bash
# Row count in the GeoPackage (should match observations table)
sqlite3 out.gpkg "SELECT COUNT(*) FROM observations;"

# Metadata written by the exporter
sqlite3 out.gpkg "SELECT key, value FROM export_metadata WHERE key LIKE '%count%';"

# What the process logged
grep -E 'Server reports|Fetch complete|skipped|max_observations|OGC_PROCESSING_META' export.log
```

### 4. Simulate the worker (optional)

Under `OGC_JOB_ID`, human logs are suppressed; only progress/meta lines go to stderr:

```bash
export OGC_JOB_ID=local-test
python3 sta_to_gpkg.py < input.json > out.gpkg 2> export.log
unset OGC_JOB_ID

./run-describe-local.sh export.log   # needs jq; prints STAC JSON to stdout
```

### `max_observations` vs rows in the GeoPackage

| Number | Meaning |
|--------|---------|
| **`max_observations`** | When &gt; 0, fetch **exactly** this many Observation entities from STA (if the service has enough rows). |
| **`Server reports N total`** (`@iot.count`) | How many observations match your URL + `$filter` on the server. |
| **`observations_fetched` / table rows** | How many were ingested; may be lower if some rows lack `Datastream`/`MultiDatastream`. |

**Paging:** Each HTTP request uses `$top = min(top, remaining)`. When `max_observations` is large and equals `top`, the exporter automatically pages with **`$top=1000`** per request so STA servers do not truncate a single huge page and stop early (a common cause of “7370 instead of 10000”).

Recommended for testing against millions of rows:

```json
{
  "url": "https://YOUR-SERVER/v1.1/Observations",
  "top": 1000,
  "max_observations": 10000
}
```

You get **fewer than `max_observations`** only when:

1. The server has fewer matching rows than the cap (check `Server reports N total` in the log).
2. Pagination stops early (warning in log — lower `top` or check STA `nextLink`).
3. **Ingest skips** — see `skipped_no_stream` in `export.log` / `OGC_PROCESSING_META`.

To verify on your service:

```bash
# Unfiltered cap smoke test (small)
echo '{"url":"https://YOUR-SERVER/v1.1/Observations","max_observations":50,"top":50}' \
  | python3 sta_to_gpkg.py > smoke.gpkg 2> smoke.log
sqlite3 smoke.gpkg "SELECT COUNT(*) FROM observations;"
```

Compare `sqlite3` count with `table_counts.observations` inside the `OGC_PROCESSING_META` JSON line in `export.log`.

Without `OGC_JOB_ID`, normal INFO/WARNING logging goes to stderr for debugging.

## IPT docker-compose harness (execute + describeProcessing + Redis)

For the full two-container flow with a **dummy Redis** job store (mirrors ogc-api-processes `ogc:job:{id}` progress keys):

```bash
# Use the harness scripts (they quote -f for paths with spaces in the dirname)
chmod +x harness/run-ipt-cycle.sh harness/describe-only.sh
./harness/run-ipt-cycle.sh
```

See **[harness/README.md](harness/README.md)** for artifacts, `describe-only`, and Redis inspection on port **6380**.
