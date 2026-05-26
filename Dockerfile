# ── Stage 1: build deps ───────────────────────────────────────────────────
# Install Python packages in an isolated layer so the final image stays lean.
FROM python:3.12-slim AS builder

# libgeos-dev is required to compile the shapely C extension
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        libgeos-dev \
        gcc \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime image ────────────────────────────────────────────────
FROM python:3.12-slim

LABEL org.opencontainers.image.title="sta_to_gpkg" \
      org.opencontainers.image.description="SensorThings API v1.1 → GeoPackage exporter (stdin→stdout)" \
      org.opencontainers.image.licenses="MIT" \
      ogcapi.describeProcessing="true" \
      ogcapi.processes.ipt.domain="sensorthings" \
      ogcapi.processes.ipt.output="geopackage" \
      ogcapi.processes.ipt.software-provider="Secure Dimensions GmbH" \
      ogcapi.processes.ipt.processing-level="AX" \
      ogcapi.processes.ipt.processing-facility="SECD" \
      ogcapi.processes.ipt.processing-version="1.0" \
      ogcapi.processes.ipt.stac-catalog-root="https://ic.ogc.secd.eu/stac" \
      ogcapi.processes.ipt.stac-collection-id="test_dss" \
      ogcapi.processes.ipt.chaincode="dss" \
      ogcapi.processes.ipt.stac-license="proprietary"

# Only the shared library is needed at runtime (not the dev headers)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        libgeos-c1v5 \
 && rm -rf /var/lib/apt/lists/*

# Copy installed packages from the build stage
COPY --from=builder /install /usr/local

WORKDIR /app
COPY sta_to_gpkg.py .

# Line-buffered stderr so OGC_PROCESSING_PROGRESS reaches the worker while the job runs
ENV PYTHONUNBUFFERED=1

# Run as a non-root user
RUN useradd --no-create-home --shell /bin/false appuser
USER appuser

# Execute: JSON on stdin → GeoPackage binary on stdout (logs + OGC_PROCESSING_META on stderr).
# IPT describe: OGC_ACTION=describeProcessing, framework context JSON on stdin → STAC Item on stdout.
#
# Usage:
#   echo '{"url":"https://example.org/v1.1/Observations"}' \
#     | docker run --rm -i sta_to_gpkg > export.gpkg
#
ENTRYPOINT ["python", "sta_to_gpkg.py"]
