"""
SensorThings API v1.1 → GeoPackage Exporter
============================================
Reads a JSON request document from STDIN, writes a GeoPackage to STDOUT.
All log messages go to STDERR so they never corrupt the binary stream.

On startup the script fetches the service landing page to detect:
  • Whether the MultiDatastream extension is active
    (checked via conformance URIs AND the entity list)
  • The base service URL (derived automatically from the request URL)

The $expand is then built dynamically:
  Core:
    Datastream($expand=ObservedProperty,Sensor,Thing($expand=Locations))
    &$expand=FeatureOfInterest
  +MultiDatastream (if detected):
    MultiDatastream($expand=ObservedProperties,Sensor,Thing($expand=Locations))

STDIN JSON schema
-----------------
  {
    "url":     "https://example.org/v1.1/Observations",  -- required
    "filter":  "phenomenonTime ge 2024-01-01T00:00:00Z", -- optional OData $filter
    "top":     1000,                                      -- optional page size (default 1000)
    "max_observations": 5000,                             -- fetch exactly this many when server has enough (0 = no limit)
    "timeout": 30,                                        -- optional HTTP timeout seconds (default 30)
    "verbose": false                                      -- optional debug logging (default false)
  }

Usage
-----
  echo '{"url": "https://example.org/v1.1/Observations"}' | python sta_to_gpkg.py > out.gpkg
  cat request.json | python sta_to_gpkg.py > out.gpkg 2>export.log

IPT (OGC API Processes)
-----------------------
  Deploy with iptCompliant=true and image label ogcapi.describeProcessing=true.
  Two containers:
    1) main() — GeoPackage on stdout; stderr machine lines only under OGC_JOB_ID:
       OGC_PROCESSING_PROGRESS:0..100 and OGC_PROCESSING_META:{...json...}
       On failure: UTF-8 error text on stdout → worker stores in Redis (not IPFS)
    2) describeProcessing — stdin context (processing + iptLabels + execution) → STAC stdout
       Item id: UUIDv4. Catalog links from IPT labels. ``datetime`` is set by the
       framework at catalog upload; ``start_datetime`` / ``end_datetime`` come from
       observation phenomenon times; ``processing:datetime`` is export time.

  Local testing: run without OGC_JOB_ID — logs go to stderr as usual.

GeoPackage tables produced
--------------------------
  locations             — geometry layer (shown by default in QGIS/ArcGIS)
  features_of_interest  — geometry layer (FoI locations)
  things                — IoT Things
  datastreams           — Datastream metadata
  multi_datastreams     — MultiDatastream metadata (if extension active)
  observed_properties   — What is being measured
  sensors               — Sensor hardware metadata
  observations          — Observation results + timestamps
  _export_metadata      — Provenance (source URL, time, request count, …)
"""

import hashlib
import io
import json
import logging
import math
import os
import warnings
import shutil
import sqlite3
import struct
import sys
import tempfile
import threading
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Iterator
from urllib.parse import urlparse

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _running_under_ogc_worker() -> bool:
    return bool(os.environ.get("OGC_JOB_ID"))


def _configure_logging() -> None:
    """
    Local CLI: human-readable logs on stderr.
    OGC worker (OGC_JOB_ID set): silence all loggers — stdout is binary GPKG;
    only emit_processing_meta() may write one line to stderr.
    """
    if _running_under_ogc_worker():
        logging.basicConfig(
            handlers=[logging.NullHandler()],
            level=logging.CRITICAL + 1,
            force=True,
        )
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(logging.NullHandler())
        root.setLevel(logging.CRITICAL + 1)
        for name in (
            "sta2gpkg",
            "urllib3",
            "urllib3.connectionpool",
            "requests",
            "charset_normalizer",
        ):
            lg = logging.getLogger(name)
            lg.handlers.clear()
            lg.addHandler(logging.NullHandler())
            lg.propagate = False
            lg.setLevel(logging.CRITICAL + 1)
        warnings.filterwarnings("ignore")
        if hasattr(sys.stderr, "reconfigure"):
            try:
                sys.stderr.reconfigure(line_buffering=True)
            except Exception:
                pass
        return

    logging.basicConfig(
        stream=sys.stderr,
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.INFO,
        datefmt="%H:%M:%S",
    )


_configure_logging()
log = logging.getLogger("sta2gpkg")
if _running_under_ogc_worker():
    log.propagate = False

GPKG_MEDIA_TYPE = "application/geopackage+sqlite3"
OGC_PROCESSING_META_PREFIX = "OGC_PROCESSING_META:"
OGC_PROCESSING_PROGRESS_PREFIX = "OGC_PROCESSING_PROGRESS:"
IPT_BBOX_REQUIRED_MESSAGE = "BBOX null"


def execution_fail(message: str, code: int = 1) -> None:
    """Report failure: under OGC worker write message to stdout (stored on Redis job record)."""
    text = message if message.endswith("\n") else f"{message}\n"
    if os.environ.get("OGC_JOB_ID"):
        sys.stdout.buffer.write(text.encode("utf-8"))
        sys.stdout.buffer.flush()
    raise SystemExit(code)


STAC_PROCESSING_EXTENSION = "https://stac-extensions.github.io/processing/v1.1.0/schema.json"
STAC_FILE_EXTENSION = "https://stac-extensions.github.io/file/v2.1.0/schema.json"

DEFAULT_PROCESS_VERSION = "2.0.0"

# When max_observations is set, avoid one HTTP call with $top equal to the full cap — many STA
# services return a truncated page and omit @iot.nextLink. Page in chunks of at most this size.
MAX_HTTP_TOP_WHEN_CAPPED = 1000

# User data tables (exclude gpkg_* / sqlite_* / rtree_*).
GPKG_USER_TABLES = (
    "locations",
    "features_of_interest",
    "things",
    "datastreams",
    "multi_datastreams",
    "multi_datastream_observed_properties",
    "observed_properties",
    "sensors",
    "observations",
    "_export_metadata",
)


# ---------------------------------------------------------------------------
# Capability detection via landing page
# ---------------------------------------------------------------------------

MDS_CONFORMANCE_URI = "http://www.opengis.net/spec/iot_sensing/1.1/req/multi-datastream"


class ServiceCapabilities:
    """Fetches and parses the STA landing page to detect active extensions."""

    def __init__(self, base_url: str, session: requests.Session, timeout: int):
        self.base_url = base_url
        self.has_multi_datastream = False
        self.entity_names: set[str] = set()
        self._fetch(session, timeout)

    def _fetch(self, session: requests.Session, timeout: int):
        try:
            r = session.get(self.base_url, timeout=timeout)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            log.warning("Could not fetch landing page (%s) — assuming core-only", exc)
            return

        # Collect entity names from the value array
        for entry in data.get("value", []):
            name = entry.get("name", "")
            if name:
                self.entity_names.add(name)

        # Check conformance URIs (serverSettings.conformance)
        conformance = (
            data.get("serverSettings", {}).get("conformance", [])
            or data.get("conformance", [])
        )
        has_mds_conf = MDS_CONFORMANCE_URI in conformance
        has_mds_entity = "MultiDatastreams" in self.entity_names

        self.has_multi_datastream = has_mds_conf or has_mds_entity

        log.info(
            "Landing page: entities=%s, MultiDatastream=%s "
            "(conf=%s, entity-list=%s)",
            sorted(self.entity_names),
            self.has_multi_datastream,
            has_mds_conf,
            has_mds_entity,
        )

    # ------------------------------------------------------------------
    @staticmethod
    def base_url_from_obs_url(obs_url: str) -> str:
        """
        Derive the service root by finding the first STA collection name in
        the path and stripping it (plus everything after it).  This preserves
        any path prefix that precedes the collection, such as /sta/, /staplus/,
        or /iot/sensing/v1.1/, which the old version-segment regex dropped.

        Examples
          …/v1.1/Observations                      → …/v1.1
          …/staplus/v1.1/Observations              → …/staplus/v1.1
          …/sta/v1.1/Things(5)/Datastreams(1)/Observations → …/sta/v1.1
          …/api/Observations                       → …/api
        """
        STA_COLLECTIONS = (
            "/Observations",
            "/Datastreams",
            "/MultiDatastreams",
            "/Things",
            "/Locations",
            "/HistoricalLocations",
            "/Sensors",
            "/ObservedProperties",
            "/FeaturesOfInterest",
        )
        parsed = urlparse(obs_url)
        path = parsed.path

        # Find the earliest STA collection segment and cut there
        cut = len(path)
        for col in STA_COLLECTIONS:
            idx = path.find(col)
            if idx != -1 and idx < cut:
                cut = idx

        root_path = path[:cut] if cut > 0 else "/"
        return f"{parsed.scheme}://{parsed.netloc}{root_path}"


# ---------------------------------------------------------------------------
# Dynamic $expand builder
# ---------------------------------------------------------------------------

def build_expand(has_mds: bool) -> str:
    """
    Build the $expand string based on detected capabilities.

    Always included
    ---------------
    • Datastream → ObservedProperty, Sensor, Thing → Locations
    • FeatureOfInterest   (geometry of the observation itself)

    When MultiDatastream is active
    ------------------------------
    • MultiDatastream → ObservedProperties (list), Sensor, Thing → Locations
    """
    parts = [
        # Core Datastream branch
        "Datastream("
        "$expand="
        "ObservedProperty,"
        "Sensor,"
        "Thing($expand=Locations)"
        ")",
        # FeatureOfInterest is always present in STA core
        "FeatureOfInterest",
    ]

    if has_mds:
        parts.append(
            # MultiDatastream uses ObservedProperties (plural) per STA spec
            "MultiDatastream("
            "$expand="
            "ObservedProperties,"
            "Sensor,"
            "Thing($expand=Locations)"
            ")"
        )

    return ",".join(parts)


# ---------------------------------------------------------------------------
# SensorThings client
# ---------------------------------------------------------------------------

class STAClient:
    """Paginates through an Observations collection with a dynamic $expand."""

    def __init__(
        self,
        obs_url: str,
        capabilities: ServiceCapabilities,
        page_size: int = 1000,
        timeout: int = 30,
    ):
        self.obs_url = obs_url
        self.caps = capabilities
        self.page_size = page_size
        self.timeout = timeout
        self.session = requests.Session()
        self.obs_total: int | None = None
        self.session.headers["Accept"] = "application/json"
        self._request_count = 0
        self._expand = build_expand(capabilities.has_multi_datastream)
        log.info("$expand = %s", self._expand)

    # ------------------------------------------------------------------
    @staticmethod
    def _replace_odata_top(url: str, top: int) -> str:
        """Set $top on a collection URL or @iot.nextLink for the next page request."""
        top = max(1, int(top))
        if re.search(r"(?:\$|%24)top=\d+", url, re.IGNORECASE):
            return re.sub(
                r"((?:\$|%24)top=)\d+",
                rf"\g<1>{top}",
                url,
                count=1,
                flags=re.IGNORECASE,
            )
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}$top={top}"

    # ------------------------------------------------------------------
    @staticmethod
    def _build_url(url: str, params: dict) -> str:
        """
        Append OData query parameters to a URL without percent-encoding them.

        requests' params= kwarg encodes every character including $ ( ) , which
        breaks OData operators like $expand, $filter, and $top on most backends.
        Instead we join the key=value pairs with & and append them raw.
        Values are cast to str so callers can pass ints (e.g. $top=1000).
        """
        if not params:
            return url
        sep = "&" if "?" in url else "?"
        qs = "&".join(f"{k}={str(v)}" for k, v in params.items())
        return f"{url}{sep}{qs}"

    # ------------------------------------------------------------------
    def _get(
        self,
        url: str,
        params: dict | None = None,
        on_waiting: Callable[[], None] | None = None,
    ) -> dict:
        self._request_count += 1
        final_url = self._build_url(url, params) if params else url
        log.debug("GET %s  (#%d)", final_url, self._request_count)

        stop = threading.Event()

        def _pulse_while_waiting() -> None:
            while not stop.wait(2.0):
                if on_waiting:
                    try:
                        on_waiting()
                    except Exception:
                        pass

        pulse_thread = None
        if on_waiting:
            pulse_thread = threading.Thread(target=_pulse_while_waiting, daemon=True)
            pulse_thread.start()
        try:
            r = self.session.get(final_url, timeout=self.timeout)
            r.raise_for_status()
            return r.json()
        finally:
            stop.set()
            if pulse_thread is not None:
                pulse_thread.join(timeout=0.2)

    # ------------------------------------------------------------------
    def paginate(
        self,
        extra_params: dict | None = None,
        max_observations: int = 0,
        on_progress: Callable[[int, int | None, int, int, bool], None] | None = None,
    ) -> Iterator[dict]:
        """
        Yield observations from the collection, page by page.

        Parameters
        ----------
        extra_params     : additional OData query params (e.g. $filter)
        max_observations : when > 0, yield exactly this many items if the server has
                           enough rows (paginated $top on every request, including
                           @iot.nextLink). 0 = no limit.
        """
        target = max_observations if max_observations > 0 else None

        base_params: dict[str, Any] = {
            "$expand": self._expand,
            "$count": "true",
        }
        if extra_params:
            base_params.update(extra_params)

        next_url: str | None = self.obs_url
        first = True
        total: int | None = None
        fetched = 0

        while next_url:
            need = (target - fetched) if target is not None else None
            page_top = min(self.page_size, need) if need is not None else self.page_size
            page_top = max(1, page_top)

            def _waiting_pulse() -> None:
                if on_progress:
                    on_progress(fetched, total, max_observations, page_top, True)

            if on_progress:
                on_progress(fetched, total, max_observations, page_top, False)

            if first:
                params = {**base_params, "$top": page_top}
                data = self._get(next_url, params, on_waiting=_waiting_pulse)
                first = False
            else:
                data = self._get(
                    self._replace_odata_top(next_url, page_top),
                    on_waiting=_waiting_pulse,
                )

            if total is None and "@iot.count" in data:
                total = data["@iot.count"]
                self.obs_total = int(total) if total is not None else None
                log.info("Server reports %d total observations", total)

            batch = list(data.get("value") or [])
            if need is not None:
                batch = batch[:need]

            if not batch and not data.get("@iot.nextLink"):
                break

            fetched += len(batch)
            cap_info = f", target={target}" if target is not None else ""
            pct = f" ({fetched/total*100:.0f}%)" if total else ""
            log.info("  Fetched %d%s%s observations ($top=%d)", fetched, pct, cap_info, page_top)

            if on_progress:
                on_progress(fetched, total, max_observations, 0, False)

            yield from batch

            if target is not None and fetched >= target:
                log.info("  Reached max_observations (%d)", target)
                break

            next_url = data.get("@iot.nextLink")
            if target is not None and fetched < target and not next_url:
                log.warning(
                    "  Pagination ended at %d observation(s); max_observations was %d "
                    "(server returned no @iot.nextLink — try a smaller 'top' page size)",
                    fetched,
                    target,
                )
                break
            if next_url:
                time.sleep(0.05)

    # ------------------------------------------------------------------
    @property
    def request_count(self) -> int:
        return self._request_count


# ---------------------------------------------------------------------------
# Entity graph collector
# ---------------------------------------------------------------------------

class EntityGraph:
    """
    Accumulates de-duplicated STA entities from expanded Observation payloads.
    Handles both Datastream and MultiDatastream observations.
    """

    def __init__(self):
        self.observations: list[dict] = []
        self.skipped_no_stream: int = 0
        self.datastreams: dict[str, dict] = {}
        self.multi_datastreams: dict[str, dict] = {}
        self.things: dict[str, dict] = {}
        self.locations: dict[str, dict] = {}
        self.features_of_interest: dict[str, dict] = {}
        self.observed_properties: dict[str, dict] = {}
        self.sensors: dict[str, dict] = {}

    # ------------------------------------------------------------------
    def _ingest_thing(self, thing: dict):
        tid = str(thing["@iot.id"])
        if tid not in self.things:
            self.things[tid] = thing
        for loc in thing.get("Locations", []):
            lid = str(loc["@iot.id"])
            if lid not in self.locations:
                loc["_thing_id"] = tid
                self.locations[lid] = loc

    # ------------------------------------------------------------------
    def _ingest_stream(self, stream: dict, store: dict):
        """Shared logic for Datastream and MultiDatastream."""
        sid = str(stream["@iot.id"])
        if sid not in store:
            store[sid] = stream

        # ObservedProperty (singular on DS, potentially list on MDS)
        op = stream.get("ObservedProperty")
        ops = stream.get("ObservedProperties") or ([] if op is None else [op])
        for o in ops:
            oid = str(o["@iot.id"])
            if oid not in self.observed_properties:
                self.observed_properties[oid] = o

        sensor = stream.get("Sensor") or {}
        if sensor.get("@iot.id") is not None:
            snid = str(sensor["@iot.id"])
            if snid not in self.sensors:
                self.sensors[snid] = sensor

        thing = stream.get("Thing") or {}
        if thing.get("@iot.id") is not None:
            self._ingest_thing(thing)

    # ------------------------------------------------------------------
    def ingest(self, obs: dict) -> None:
        oid = obs.get("@iot.id")

        ds  = obs.get("Datastream")
        mds = obs.get("MultiDatastream")

        if not ds and not mds:
            self.skipped_no_stream += 1
            log.warning("Observation %s has neither Datastream nor MultiDatastream — skipped", oid)
            return

        ds_id  = str(ds["@iot.id"])  if ds  else None
        mds_id = str(mds["@iot.id"]) if mds else None

        if ds:
            self._ingest_stream(ds, self.datastreams)
        if mds:
            self._ingest_stream(mds, self.multi_datastreams)

        # FeatureOfInterest
        foi = obs.get("FeatureOfInterest") or {}
        foi_id = foi.get("@iot.id")
        if foi_id is not None:
            fid = str(foi_id)
            if fid not in self.features_of_interest:
                self.features_of_interest[fid] = foi

        self.observations.append({
            "id": str(oid),
            "phenomenonTime": obs.get("phenomenonTime"),
            "resultTime": obs.get("resultTime"),
            "result": obs.get("result"),
            "resultQuality": json.dumps(obs["resultQuality"]) if "resultQuality" in obs else None,
            "parameters": json.dumps(obs["parameters"]) if "parameters" in obs else None,
            "datastream_id": ds_id,
            "multi_datastream_id": mds_id,
            "feature_of_interest_id": str(foi_id) if foi_id is not None else None,
        })

    # ------------------------------------------------------------------
    def summary(self) -> str:
        return (
            f"observations={len(self.observations)}, "
            f"skipped_no_stream={self.skipped_no_stream}, "
            f"datastreams={len(self.datastreams)}, "
            f"multi_datastreams={len(self.multi_datastreams)}, "
            f"things={len(self.things)}, "
            f"locations={len(self.locations)}, "
            f"features_of_interest={len(self.features_of_interest)}, "
            f"observed_properties={len(self.observed_properties)}, "
            f"sensors={len(self.sensors)}"
        )


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _gpkg_header(srs_id: int = 4326) -> bytes:
    """8-byte GeoPackage binary header (no envelope)."""
    return b"GP\x00\x01" + struct.pack("<i", srs_id)


def _encode_geom(geojson: dict | None) -> bytes | None:
    if not geojson:
        return None
    gtype = geojson.get("type", "")
    coords = geojson.get("coordinates")

    if not coords and gtype != "GeometryCollection":
        return None

    if gtype == "Point":
        lon, lat = coords[0], coords[1]
        wkb = b"\x01" + struct.pack("<I", 1) + struct.pack("<dd", lon, lat)
        return _gpkg_header() + wkb

    # Non-point: use shapely for proper WKB
    try:
        from shapely.geometry import shape
        from shapely.wkb import dumps as wkb_dumps
        geom = shape(geojson)
        return _gpkg_header() + wkb_dumps(geom, little_endian=True)
    except Exception as exc:
        log.warning("Could not encode geometry %s: %s", gtype, exc)
        return None


def _collect_positions(coords, lons: list, lats: list) -> None:
    """Walk nested GeoJSON coordinate arrays and collect lon/lat pairs."""
    if coords is None:
        return
    if isinstance(coords, (int, float)):
        return
    if (
        isinstance(coords, (list, tuple))
        and len(coords) >= 2
        and isinstance(coords[0], (int, float))
        and isinstance(coords[1], (int, float))
    ):
        lons.append(float(coords[0]))
        lats.append(float(coords[1]))
        return
    for item in coords:
        _collect_positions(item, lons, lats)


def _collect_lon_lat_from_geojson(g: dict, lons: list, lats: list) -> None:
    if not g:
        return
    gtype = g.get("type", "")
    if gtype == "GeometryCollection":
        for sub in g.get("geometries") or []:
            _collect_lon_lat_from_geojson(sub, lons, lats)
        return
    _collect_positions(g.get("coordinates"), lons, lats)


def _bounds_from_gpkg_blob(blob: bytes | None) -> tuple[float, float, float, float] | None:
    """Return (min_x, min_y, max_x, max_y) from a GeoPackage geometry BLOB."""
    if not blob or len(blob) < 9:
        return None
    try:
        from shapely.wkb import loads
        geom = loads(blob[8:])  # skip 8-byte GPKG header
        if geom.is_empty:
            return None
        b = geom.bounds
        return (float(b[0]), float(b[1]), float(b[2]), float(b[3]))
    except ImportError:
        pass
    except Exception as exc:
        log.debug("Could not read bounds from GPKG blob via shapely: %s", exc)

    # Point WKB fallback (matches _encode_geom Point path)
    wkb = blob[8:]
    if len(wkb) >= 21 and wkb[0] == 0x01:
        gtype = struct.unpack("<I", wkb[1:5])[0]
        if gtype == 1:
            x, y = struct.unpack("<dd", wkb[5:21])
            return (x, y, x, y)
    return None


def _bbox_from_geom_table(con: sqlite3.Connection, table: str) -> tuple | None:
    """Aggregate extent from encoded geom column (authoritative after write)."""
    min_x = min_y = float("inf")
    max_x = max_y = float("-inf")
    for (blob,) in con.execute(f"SELECT geom FROM [{table}] WHERE geom IS NOT NULL"):
        b = _bounds_from_gpkg_blob(blob)
        if not b:
            continue
        min_x, min_y = min(min_x, b[0]), min(min_y, b[1])
        max_x, max_y = max(max_x, b[2]), max(max_y, b[3])
    if min_x == float("inf"):
        return None
    return (min_x, min_y, max_x, max_y)


def _bbox_from_geoms(geojsons: list[dict | None]) -> tuple | None:
    """Extent from GeoJSON dicts (any geometry type; fallback when table scan is empty)."""
    lons: list[float] = []
    lats: list[float] = []
    for g in geojsons:
        _collect_lon_lat_from_geojson(g, lons, lats)
    if not lons:
        return None
    return (min(lons), min(lats), max(lons), max(lats))


def _load_shapely_from_gpkg_blob(blob: bytes | None):
    if not blob or len(blob) < 9:
        return None
    try:
        from shapely.wkb import loads
        geom = loads(blob[8:])
        return None if geom.is_empty else geom
    except Exception:
        return None


def _iter_shapely_xy(geom) -> list[tuple[float, float]]:
    if geom is None:
        return []
    if geom.geom_type == "Point":
        return [(geom.x, geom.y)]
    if hasattr(geom, "geoms"):
        out: list[tuple[float, float]] = []
        for g in geom.geoms:
            out.extend(_iter_shapely_xy(g))
        return out
    if hasattr(geom, "exterior"):
        return list(geom.exterior.coords)
    if hasattr(geom, "coords"):
        return list(geom.coords)
    return []


def _collect_lon_lat_from_gpkg_blob(blob: bytes | None, lons: list, lats: list) -> None:
    geom = _load_shapely_from_gpkg_blob(blob)
    if geom is not None:
        for x, y in _iter_shapely_xy(geom):
            lons.append(float(x))
            lats.append(float(y))
        return
    bounds = _bounds_from_gpkg_blob(blob)
    if bounds:
        min_x, min_y, max_x, max_y = bounds
        for x, y in ((min_x, min_y), (max_x, min_y), (max_x, max_y), (min_x, max_y)):
            lons.append(x)
            lats.append(y)


GPKG_FEATURE_TABLES = ("locations", "features_of_interest")


def _collect_lon_lat_from_gpkg(con: sqlite3.Connection, lons: list, lats: list) -> None:
    for table in GPKG_FEATURE_TABLES:
        try:
            for (blob,) in con.execute(f"SELECT geom FROM [{table}] WHERE geom IS NOT NULL"):
                _collect_lon_lat_from_gpkg_blob(blob, lons, lats)
        except sqlite3.Error:
            continue


def _cross(o, a, b) -> float:
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


def _monotone_chain_hull(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    pts = sorted(set(points))
    if len(pts) <= 1:
        return pts
    lower: list[tuple[float, float]] = []
    for p in pts:
        while len(lower) >= 2 and _cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    upper: list[tuple[float, float]] = []
    for p in reversed(pts):
        while len(upper) >= 2 and _cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    return lower[:-1] + upper[:-1]


def _bbox_as_polygon_geojson(bbox: list[float] | tuple | None) -> dict | None:
    if not bbox or len(bbox) != 4:
        return None
    west, south, east, north = bbox
    ring = [
        [west, south],
        [east, south],
        [east, north],
        [west, north],
        [west, south],
    ]
    return {"type": "Polygon", "coordinates": [ring]}


def _bbox_from_geometry(geometry: dict | None) -> list[float] | None:
    """Derive STAC bbox [minx, miny, maxx, maxy] from GeoJSON-like geometry."""
    if not isinstance(geometry, dict):
        return None
    lons: list[float] = []
    lats: list[float] = []
    _collect_lon_lat_from_geojson(geometry, lons, lats)
    if not lons:
        return None
    return [min(lons), min(lats), max(lons), max(lats)]


def _aggregate_gpkg_bbox(con: sqlite3.Connection) -> list[float] | None:
    """Extent from feature-table geom columns (authoritative when gpkg_contents is empty)."""
    min_x = min_y = float("inf")
    max_x = max_y = float("-inf")
    for table in GPKG_FEATURE_TABLES:
        bb = _bbox_from_geom_table(con, table)
        if not bb:
            continue
        min_x, min_y = min(min_x, bb[0]), min(min_y, bb[1])
        max_x, max_y = max(max_x, bb[2]), max(max_y, bb[3])
    if min_x == float("inf"):
        return None
    return [min_x, min_y, max_x, max_y]


def _normalize_footprint_polygon(footprint: dict | None) -> dict | None:
    if not isinstance(footprint, dict):
        return None
    if footprint.get("type") == "Polygon":
        return footprint
    if footprint.get("type") == "MultiPolygon":
        coords = footprint.get("coordinates") or []
        if len(coords) == 1:
            return {"type": "Polygon", "coordinates": coords[0]}
        return footprint
    return None


def _is_valid_stac_bbox(bbox: object) -> bool:
    """True when bbox is a finite STAC extent [minx, miny, maxx, maxy]."""
    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        return False
    try:
        vals = [float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])]
    except (TypeError, ValueError):
        return False
    return all(math.isfinite(v) for v in vals)


def _require_ipt_stac_bbox(bbox: object) -> None:
    """IPT describeProcessing: STAC Item must have a spatial bbox."""
    if not _is_valid_stac_bbox(bbox):
        execution_fail(IPT_BBOX_REQUIRED_MESSAGE)


def _resolve_stac_spatial(processing: dict) -> tuple[list[float] | None, dict | None]:
    """
    Resolve STAC Item bbox + geometry from execute metadata.

    Order: explicit bbox → footprint polygon → bbox as rectangular polygon.
    """
    raw_bbox = processing.get("bbox")
    bbox: list[float] | None = None
    if isinstance(raw_bbox, (list, tuple)) and len(raw_bbox) == 4:
        try:
            bbox = [float(raw_bbox[0]), float(raw_bbox[1]),
                    float(raw_bbox[2]), float(raw_bbox[3])]
        except (TypeError, ValueError):
            bbox = None

    footprint = _normalize_footprint_polygon(processing.get("footprint"))
    geometry = footprint or _bbox_as_polygon_geojson(bbox)

    if bbox is None and geometry is not None:
        bbox = _bbox_from_geometry(geometry)
    if bbox is None and footprint is not None:
        bbox = _bbox_from_geometry(footprint)

    return bbox, geometry


def _convex_hull_polygon_from_lon_lats(lons: list[float], lats: list[float]) -> dict | None:
    if not lons:
        return None
    try:
        from shapely.geometry import MultiPoint, mapping
        mp = MultiPoint(list(zip(lons, lats)))
        hull = mp.convex_hull
        if hull.is_empty:
            return None
        if hull.geom_type not in ("Polygon", "MultiPolygon"):
            hull = mp.envelope
        geo = mapping(hull)
        if geo.get("type") == "MultiPolygon" and len(geo.get("coordinates", [])) == 1:
            return {"type": "Polygon", "coordinates": geo["coordinates"][0]}
        if geo.get("type") == "Polygon":
            return geo
        return _bbox_as_polygon_geojson((min(lons), min(lats), max(lons), max(lats)))
    except ImportError:
        hull_pts = _monotone_chain_hull(list(zip(lons, lats)))
        if len(hull_pts) < 3:
            return _bbox_as_polygon_geojson((min(lons), min(lats), max(lons), max(lats)))
        ring = [[x, y] for x, y in hull_pts]
        if ring[0] != ring[-1]:
            ring.append(ring[0])
        return {"type": "Polygon", "coordinates": [ring]}


def _convex_hull_footprint_from_gpkg(con: sqlite3.Connection) -> dict | None:
    """STAC footprint: convex hull over all feature geometries in the GeoPackage."""
    try:
        from shapely.geometry import mapping
        from shapely.ops import unary_union
    except ImportError:
        lons: list[float] = []
        lats: list[float] = []
        _collect_lon_lat_from_gpkg(con, lons, lats)
        return _convex_hull_polygon_from_lon_lats(lons, lats)

    geoms = []
    for table in GPKG_FEATURE_TABLES:
        try:
            for (blob,) in con.execute(f"SELECT geom FROM [{table}] WHERE geom IS NOT NULL"):
                geom = _load_shapely_from_gpkg_blob(blob)
                if geom is not None:
                    geoms.append(geom)
        except sqlite3.Error:
            continue
    if not geoms:
        return None

    union = unary_union(geoms)
    hull = union.convex_hull
    if hull.is_empty:
        return None
    if hull.geom_type not in ("Polygon", "MultiPolygon"):
        hull = union.envelope
    geo = mapping(hull)
    if geo.get("type") == "MultiPolygon" and len(geo.get("coordinates", [])) == 1:
        return {"type": "Polygon", "coordinates": geo["coordinates"][0]}
    if geo.get("type") == "Polygon":
        return geo
    bounds = union.bounds
    return _bbox_as_polygon_geojson(bounds)


# ---------------------------------------------------------------------------
# GeoPackage writer
# ---------------------------------------------------------------------------

class GeoPackageWriter:

    def __init__(self, path: str):
        self.path = path
        self.con = sqlite3.connect(path)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA foreign_keys=ON")
        self._init_core()

    # ------------------------------------------------------------------
    def _init_core(self):
        # application_id / user_version are written via PRAGMA but Python's sqlite3
        # module defers the page-1 flush; we patch the raw bytes in close() instead.
        self.con.execute("PRAGMA user_version = 10300")
        self.con.executescript("""
            CREATE TABLE IF NOT EXISTS gpkg_spatial_ref_sys (
                srs_name                 TEXT    NOT NULL,
                srs_id                   INTEGER NOT NULL PRIMARY KEY,
                organization             TEXT    NOT NULL,
                organization_coordsys_id INTEGER NOT NULL,
                definition               TEXT    NOT NULL,
                description              TEXT
            );
            INSERT OR IGNORE INTO gpkg_spatial_ref_sys VALUES
                ('Undefined cartesian',  -1, 'NONE', -1, 'undefined', NULL),
                ('Undefined geographic',  0, 'NONE',  0, 'undefined', NULL),
                ('WGS 84 geodetic', 4326, 'EPSG', 4326,
                 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]',
                 'WGS 84 geographic 2D');

            CREATE TABLE IF NOT EXISTS gpkg_contents (
                table_name  TEXT     NOT NULL PRIMARY KEY,
                data_type   TEXT     NOT NULL,
                identifier  TEXT,
                description TEXT     DEFAULT '',
                last_change DATETIME NOT NULL
                    DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                min_x REAL, min_y REAL, max_x REAL, max_y REAL,
                srs_id INTEGER REFERENCES gpkg_spatial_ref_sys(srs_id)
            );

            CREATE TABLE IF NOT EXISTS gpkg_geometry_columns (
                table_name         TEXT NOT NULL,
                column_name        TEXT NOT NULL,
                geometry_type_name TEXT NOT NULL,
                srs_id             INTEGER NOT NULL
                    REFERENCES gpkg_spatial_ref_sys(srs_id),
                z INTEGER NOT NULL,
                m INTEGER NOT NULL,
                CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
                CONSTRAINT fk_gc_tn FOREIGN KEY (table_name)
                    REFERENCES gpkg_contents(table_name)
            );
        """)
        self.con.commit()

    # ------------------------------------------------------------------
    def _reg_feature(self, table: str, ident: str, desc: str,
                     geom_type: str = "GEOMETRY", bbox=None):
        bb = bbox or (None, None, None, None)
        self.con.execute(
            "INSERT OR REPLACE INTO gpkg_contents "
            "(table_name,data_type,identifier,description,srs_id,min_x,min_y,max_x,max_y) "
            "VALUES (?,?,?,?,4326,?,?,?,?)",
            (table, "features", ident, desc, *bb),
        )
        self.con.execute(
            "INSERT OR REPLACE INTO gpkg_geometry_columns "
            "(table_name,column_name,geometry_type_name,srs_id,z,m) VALUES (?,?,?,4326,0,0)",
            (table, "geom", geom_type),
        )

    def _reg_attrs(self, table: str, ident: str, desc: str):
        self.con.execute(
            "INSERT OR REPLACE INTO gpkg_contents "
            "(table_name,data_type,identifier,description) VALUES (?,?,?,?)",
            (table, "attributes", ident, desc),
        )

    def _update_bbox(self, table: str, bbox: tuple):
        self.con.execute(
            "UPDATE gpkg_contents SET min_x=?,min_y=?,max_x=?,max_y=? "
            "WHERE table_name=?",
            (*bbox, table),
        )

    def _sync_feature_bbox(self, table: str, geojsons: list | None = None):
        """Set gpkg_contents extent from stored geometries (required for collect_gpkg_stats)."""
        bb = _bbox_from_geom_table(self.con, table)
        if bb is None and geojsons:
            bb = _bbox_from_geoms(geojsons)
        if bb:
            self._update_bbox(table, bb)

    # ------------------------------------------------------------------
    def write(self, graph: EntityGraph, source_url: str,
              request_count: int, has_mds: bool):
        con = self.con

        # ── locations ─────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                id TEXT PRIMARY KEY, name TEXT, description TEXT,
                encoding_type TEXT, geom BLOB, properties TEXT, thing_id TEXT
            )
        """)
        self._reg_feature("locations", "Locations",
                          "SensorThings Thing Locations")
        geojsons = []
        for loc in graph.locations.values():
            gj = loc.get("location") or loc.get("feature")
            geojsons.append(gj)
            con.execute(
                "INSERT OR REPLACE INTO locations VALUES (?,?,?,?,?,?,?)",
                (str(loc["@iot.id"]), loc.get("name"), loc.get("description"),
                 loc.get("encodingType"), _encode_geom(gj),
                 json.dumps(loc.get("properties") or {}),
                 str(loc.get("_thing_id", ""))),
            )
        self._sync_feature_bbox("locations", geojsons)

        # ── features_of_interest ──────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS features_of_interest (
                id TEXT PRIMARY KEY, name TEXT, description TEXT,
                encoding_type TEXT, geom BLOB, properties TEXT
            )
        """)
        self._reg_feature("features_of_interest", "FeaturesOfInterest",
                          "SensorThings FeaturesOfInterest")
        foi_geojsons = []
        for foi in graph.features_of_interest.values():
            gj = foi.get("feature") or foi.get("location")
            foi_geojsons.append(gj)
            con.execute(
                "INSERT OR REPLACE INTO features_of_interest VALUES (?,?,?,?,?,?)",
                (str(foi["@iot.id"]), foi.get("name"), foi.get("description"),
                 foi.get("encodingType"), _encode_geom(gj),
                 json.dumps(foi.get("properties") or {})),
            )
        self._sync_feature_bbox("features_of_interest", foi_geojsons)

        # ── things ────────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS things (
                id TEXT PRIMARY KEY, name TEXT, description TEXT, properties TEXT
            )
        """)
        self._reg_attrs("things", "Things", "SensorThings Things")
        for t in graph.things.values():
            con.execute("INSERT OR REPLACE INTO things VALUES (?,?,?,?)",
                        (str(t["@iot.id"]), t.get("name"), t.get("description"),
                         json.dumps(t.get("properties") or {})))

        # ── observed_properties ───────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS observed_properties (
                id TEXT PRIMARY KEY, name TEXT, description TEXT,
                definition TEXT, properties TEXT
            )
        """)
        self._reg_attrs("observed_properties", "ObservedProperties",
                        "SensorThings ObservedProperties")
        for op in graph.observed_properties.values():
            con.execute("INSERT OR REPLACE INTO observed_properties VALUES (?,?,?,?,?)",
                        (str(op["@iot.id"]), op.get("name"), op.get("description"),
                         op.get("definition"), json.dumps(op.get("properties") or {})))

        # ── sensors ───────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS sensors (
                id TEXT PRIMARY KEY, name TEXT, description TEXT,
                encoding_type TEXT, metadata TEXT, properties TEXT
            )
        """)
        self._reg_attrs("sensors", "Sensors", "SensorThings Sensors")
        for s in graph.sensors.values():
            meta = s.get("metadata")
            con.execute("INSERT OR REPLACE INTO sensors VALUES (?,?,?,?,?,?)",
                        (str(s["@iot.id"]), s.get("name"), s.get("description"),
                         s.get("encodingType"),
                         json.dumps(meta) if isinstance(meta, dict) else meta,
                         json.dumps(s.get("properties") or {})))

        # ── datastreams ───────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS datastreams (
                id TEXT PRIMARY KEY, name TEXT, description TEXT,
                unit_name TEXT, unit_symbol TEXT, unit_definition TEXT,
                observation_type TEXT, phenomenon_time TEXT, result_time TEXT,
                properties TEXT,
                thing_id TEXT REFERENCES things(id),
                observed_property_id TEXT REFERENCES observed_properties(id),
                sensor_id TEXT REFERENCES sensors(id)
            )
        """)
        self._reg_attrs("datastreams", "Datastreams", "SensorThings Datastreams")
        for ds in graph.datastreams.values():
            uom = ds.get("unitOfMeasurement") or {}
            con.execute("INSERT OR REPLACE INTO datastreams VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (str(ds["@iot.id"]), ds.get("name"), ds.get("description"),
                         uom.get("name"), uom.get("symbol"), uom.get("definition"),
                         ds.get("observationType"), ds.get("phenomenonTime"), ds.get("resultTime"),
                         json.dumps(ds.get("properties") or {}),
                         str((ds.get("Thing") or {}).get("@iot.id", "")),
                         str((ds.get("ObservedProperty") or {}).get("@iot.id", "")),
                         str((ds.get("Sensor") or {}).get("@iot.id", ""))))

        # ── multi_datastreams (conditional) ───────────────────────────
        if has_mds:
            con.execute("""
                CREATE TABLE IF NOT EXISTS multi_datastreams (
                    id TEXT PRIMARY KEY, name TEXT, description TEXT,
                    unit_names TEXT, unit_symbols TEXT, unit_definitions TEXT,
                    observation_type TEXT,
                    multi_observation_data_types TEXT,
                    phenomenon_time TEXT, result_time TEXT,
                    properties TEXT,
                    thing_id TEXT REFERENCES things(id),
                    sensor_id TEXT REFERENCES sensors(id)
                )
            """)
            self._reg_attrs("multi_datastreams", "MultiDatastreams",
                            "SensorThings MultiDatastreams")
            # Junction table: MultiDatastream ↔ ObservedProperty (ordered)
            con.execute("""
                CREATE TABLE IF NOT EXISTS multi_datastream_observed_properties (
                    multi_datastream_id TEXT REFERENCES multi_datastreams(id),
                    observed_property_id TEXT REFERENCES observed_properties(id),
                    rank INTEGER,
                    PRIMARY KEY (multi_datastream_id, observed_property_id)
                )
            """)
            self._reg_attrs("multi_datastream_observed_properties",
                            "MDS-OP Links",
                            "MultiDatastream ↔ ObservedProperty junction")

            for mds in graph.multi_datastreams.values():
                uoms = mds.get("unitOfMeasurements") or []
                names  = json.dumps([u.get("name")       for u in uoms])
                syms   = json.dumps([u.get("symbol")     for u in uoms])
                defs   = json.dumps([u.get("definition") for u in uoms])
                con.execute(
                    "INSERT OR REPLACE INTO multi_datastreams VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (str(mds["@iot.id"]), mds.get("name"), mds.get("description"),
                     names, syms, defs,
                     mds.get("observationType"),
                     json.dumps(mds.get("multiObservationDataTypes") or []),
                     mds.get("phenomenonTime"), mds.get("resultTime"),
                     json.dumps(mds.get("properties") or {}),
                     str((mds.get("Thing") or {}).get("@iot.id", "")),
                     str((mds.get("Sensor") or {}).get("@iot.id", ""))))

                ops = mds.get("ObservedProperties") or []
                for rank, op in enumerate(ops):
                    con.execute(
                        "INSERT OR REPLACE INTO multi_datastream_observed_properties "
                        "VALUES (?,?,?)",
                        (str(mds["@iot.id"]), str(op["@iot.id"]), rank))

        # ── observations ──────────────────────────────────────────────
        # The multi_datastream_id FK is only added when the MDS table exists.
        mds_fk = "REFERENCES multi_datastreams(id)" if has_mds else ""
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS observations (
                id TEXT PRIMARY KEY,
                phenomenon_time TEXT,
                result_time TEXT,
                result TEXT,
                result_quality TEXT,
                parameters TEXT,
                datastream_id TEXT REFERENCES datastreams(id),
                multi_datastream_id TEXT {mds_fk},
                feature_of_interest_id TEXT REFERENCES features_of_interest(id)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_obs_ds  ON observations(datastream_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_obs_mds ON observations(multi_datastream_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_obs_pt  ON observations(phenomenon_time)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_obs_foi ON observations(feature_of_interest_id)")
        self._reg_attrs("observations", "Observations", "SensorThings Observations")

        rows = [
            (o["id"], _sta_time_to_storage(o.get("phenomenonTime")),
             _sta_time_to_storage(o.get("resultTime")),
             json.dumps(o["result"]) if not isinstance(o["result"], str) else o["result"],
             o.get("resultQuality"), o.get("parameters"),
             o.get("datastream_id"), o.get("multi_datastream_id"),
             o.get("feature_of_interest_id"))
            for o in graph.observations
        ]
        con.executemany(
            "INSERT OR REPLACE INTO observations VALUES (?,?,?,?,?,?,?,?,?)", rows
        )

        # ── export metadata ───────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS _export_metadata (
                key TEXT PRIMARY KEY, value TEXT
            )
        """)
        self._reg_attrs("_export_metadata", "Export Metadata", "Export provenance")
        con.executemany("INSERT OR REPLACE INTO _export_metadata VALUES (?,?)", [
            ("source_url",                  source_url),
            ("export_time",                 datetime.now(timezone.utc).isoformat()),
            ("sta_version",                 "1.1"),
            ("multi_datastream_extension",  str(has_mds)),
            ("http_requests_made",          str(request_count)),
            ("observations_count",          str(len(graph.observations))),
            ("datastreams_count",           str(len(graph.datastreams))),
            ("multi_datastreams_count",     str(len(graph.multi_datastreams))),
            ("things_count",                str(len(graph.things))),
            ("locations_count",             str(len(graph.locations))),
            ("features_of_interest_count",  str(len(graph.features_of_interest))),
            ("observed_properties_count",   str(len(graph.observed_properties))),
            ("sensors_count",               str(len(graph.sensors))),
        ])

        con.commit()
        log.info("GeoPackage committed to %s", self.path)

    def close(self):
        self.con.close()
        # Python's sqlite3 module defers page-1 writes, so PRAGMA application_id
        # never reaches the file bytes.  Patch offset 60 directly after closing.
        with open(self.path, "r+b") as f:
            f.seek(60)
            f.write(struct.pack(">I", int.from_bytes(b"GPKG", "big")))


# ---------------------------------------------------------------------------
# STA / STAC temporal helpers
# ---------------------------------------------------------------------------

def _sta_time_to_storage(value: Any) -> str | None:
    """Store STA phenomenonTime/resultTime in SQLite (JSON for intervals)."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return str(value).strip() or None


def _normalize_iso_datetime(value: str | None) -> str | None:
    """Normalize STA/OData timestamps to ISO 8601 UTC for STAC properties."""
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        return s
    if re.match(r"^\d{4}-\d{2}-\d{2}T", s) and "+" not in s and not s.endswith("Z"):
        return s + "Z"
    return s


def _parse_sta_time(value: Any) -> tuple[str | None, str | None]:
    """
    Parse SensorThings phenomenonTime / resultTime to (start, end) instants.

    Handles ISO strings, interval objects, and JSON interval strings in SQLite.
    """
    if value is None or value == "":
        return None, None
    if isinstance(value, dict):
        start = value.get("start") or value.get("time")
        end = value.get("end") or start
        return _normalize_iso_datetime(start), _normalize_iso_datetime(end)
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("{"):
            try:
                return _parse_sta_time(json.loads(s))
            except json.JSONDecodeError:
                pass
        inst = _normalize_iso_datetime(s)
        return inst, inst
    return None, None


def _collect_phenomenon_time_range(con: sqlite3.Connection) -> dict[str, str] | None:
    """Min/max phenomenon time over all observations (parsed, not raw TEXT MIN/MAX)."""
    starts: list[str] = []
    ends: list[str] = []
    try:
        rows = con.execute(
            "SELECT phenomenon_time FROM observations WHERE phenomenon_time IS NOT NULL",
        ).fetchall()
    except sqlite3.Error:
        return None
    for (raw,) in rows:
        start, end = _parse_sta_time(raw)
        if start:
            starts.append(start)
        if end:
            ends.append(end)
        elif start:
            ends.append(start)
    if not starts:
        return None
    return {"start": min(starts), "end": max(ends)}


def _apply_data_temporal_extent(
    properties: dict[str, Any],
    phenomenon: dict[str, Any],
) -> None:
    """STAC data extent: start_datetime / end_datetime from export phenomenon times."""
    start = _normalize_iso_datetime(phenomenon.get("start")) if phenomenon.get("start") else None
    end = _normalize_iso_datetime(phenomenon.get("end")) if phenomenon.get("end") else None
    if start:
        properties["start_datetime"] = start
    if end:
        properties["end_datetime"] = end
    elif start:
        properties["end_datetime"] = start


# ---------------------------------------------------------------------------
# GeoPackage statistics & IPT describeProcessing
# ---------------------------------------------------------------------------

def collect_gpkg_stats(path: str) -> dict:
    """Row counts, export metadata, spatial extent, and phenomenon time range."""
    con = sqlite3.connect(path)
    try:
        table_counts: dict[str, int] = {}
        for table in GPKG_USER_TABLES:
            try:
                row = con.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()
                table_counts[table] = int(row[0]) if row else 0
            except sqlite3.Error:
                table_counts[table] = 0

        export_meta: dict[str, str] = {}
        try:
            for key, value in con.execute("SELECT key, value FROM _export_metadata"):
                export_meta[str(key)] = value
        except sqlite3.Error:
            pass

        bbox = None
        try:
            row = con.execute(
                "SELECT MIN(min_x), MIN(min_y), MAX(max_x), MAX(max_y) "
                "FROM gpkg_contents WHERE min_x IS NOT NULL"
            ).fetchone()
            if row and row[0] is not None:
                bbox = [float(row[0]), float(row[1]), float(row[2]), float(row[3])]
        except sqlite3.Error:
            pass

        if bbox is None:
            bbox = _aggregate_gpkg_bbox(con)

        phenomenon_range = _collect_phenomenon_time_range(con)

        footprint = _convex_hull_footprint_from_gpkg(con)

        if bbox is None and footprint is not None:
            bbox = _bbox_from_geometry(footprint)

        return {
            "table_counts":   table_counts,
            "export_metadata": export_meta,
            "bbox":           bbox,
            "footprint":      footprint,
            "phenomenon_time_range": phenomenon_range,
        }
    finally:
        con.close()


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


# Multihash prefix for SHA2-256 per STAC File extension / multiformats multihash.
_MULTIHASH_SHA2_256_PREFIX = "1220"


def sha256_digest_to_file_checksum(digest: str | None) -> str | None:
    """
    Convert sha256:… or raw hex to STAC file:checksum (multihash, lowercase hex).

    See https://github.com/stac-extensions/file — e.g. sha2-256 of ``test`` →
    ``12209f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08``.
    """
    if not digest or not isinstance(digest, str):
        return None
    value = digest.strip().lower()
    if value.startswith("sha256:"):
        value = value[7:]
    elif value.startswith(_MULTIHASH_SHA2_256_PREFIX) and len(value) == 68:
        return value
    if len(value) != 64 or not all(c in "0123456789abcdef" for c in value):
        return None
    return _MULTIHASH_SHA2_256_PREFIX + value


# iptLabels keys consumed when building STAC (not copied verbatim into properties).
_IPT_LABELS_STRUCTURED = frozenset({
    "software-provider",
    "processing-level",
    "processing-facility",
    "processing-version",
    "stac-catalog-root",
    "stac-collection-id",
    "chaincode",
    "stac-api-prefix",
    "stac-license",
})

STAC_GEOJSON_TYPE = "application/geo+json"
STAC_JSON_TYPE = "application/json"


def _effective_ipt_labels(context: dict) -> dict[str, str]:
    """
    IPT labels from describe stdin (image labels + optional deploy ``stac`` block).

    Framework may pass ``context.stac`` when catalog URLs are configured at deploy time
    instead of only in the Docker image.
    """
    labels = dict(context.get("iptLabels") or {})
    stac = context.get("stac") or {}
    mapping = {
        "catalogRoot":    "stac-catalog-root",
        "collectionId":   "stac-collection-id",
        "chaincode":      "chaincode",
        "apiPrefix":      "stac-api-prefix",
        "license":        "stac-license",
    }
    for src, dst in mapping.items():
        if stac.get(src) and not labels.get(dst):
            labels[dst] = str(stac[src])
    return labels


def _ipt_label(ipt_labels: dict, key: str) -> str | None:
    value = ipt_labels.get(key)
    if value is None or value == "":
        return None
    return str(value)


def _passthrough_ipt_labels(ipt_labels: dict) -> dict[str, Any]:
    """Extra iptLabels merged into properties (excluding structured keys)."""
    return {
        k: v for k, v in ipt_labels.items()
        if k not in _IPT_LABELS_STRUCTURED
    }


def _emit_stderr_line(line: str) -> None:
    sys.stderr.write(line + "\n")
    sys.stderr.flush()


_last_ogc_progress = -1


def emit_progress(percent: int) -> None:
    """Report execute-phase progress 0–100 on stderr for async job UI (OGC worker only)."""
    global _last_ogc_progress
    if not _running_under_ogc_worker():
        return
    pct = max(0, min(100, int(percent)))
    if pct <= _last_ogc_progress:
        return
    _last_ogc_progress = pct
    _emit_stderr_line(f"{OGC_PROCESSING_PROGRESS_PREFIX}{pct}")


def emit_processing_meta(meta: dict) -> None:
    """One machine-readable metadata line on stderr (IPT handoff; not progress logging)."""
    _emit_stderr_line(OGC_PROCESSING_META_PREFIX + json.dumps(meta, separators=(",", ":")))


def parse_processing_meta(stderr: str | None) -> dict:
    if not stderr:
        return {}
    for line in stderr.splitlines():
        if line.startswith(OGC_PROCESSING_META_PREFIX):
            try:
                return json.loads(line[len(OGC_PROCESSING_META_PREFIX):])
            except json.JSONDecodeError as exc:
                log.warning("Invalid OGC_PROCESSING_META on stderr: %s", exc)
                return {}
    return {}


def read_json_stdin() -> dict:
    try:
        raw = sys.stdin.buffer.read()
    except Exception as exc:
        execution_fail(f"Failed to read STDIN: {exc}")
    if not raw.strip():
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        execution_fail(f"STDIN is not valid JSON: {exc}")
    if not isinstance(data, dict):
        execution_fail("STDIN JSON must be an object / dict")
    return data


def _build_gpkg_metadata(processing: dict) -> dict[str, Any]:
    """GeoPackage-specific metadata (table counts + export run facts)."""
    meta: dict[str, Any] = {
        "table_counts": processing.get("table_counts") or {},
        "export_metadata": processing.get("export_metadata") or {},
    }
    for key in (
        "observations_fetched",
        "skipped_no_stream",
        "max_observations",
        "fetch_seconds",
        "output_bytes",
    ):
        if processing.get(key) is not None:
            meta[key] = processing[key]
    return meta


def _build_processing_lineage(
    *,
    source_url: str | None,
    export_meta: dict,
    processing: dict,
    table_counts: dict,
) -> str:
    lines = [
        "SensorThings API observations exported to GeoPackage (sta_to_gpkg).",
    ]
    if source_url:
        lines.append(f"- **Source:** {source_url}")
    sta_ver = export_meta.get("sta_version")
    if sta_ver:
        lines.append(f"- **STA version:** {sta_ver}")
    mds = export_meta.get("multi_datastream_extension")
    if mds is not None:
        lines.append(f"- **MultiDatastream extension:** {mds}")
    http_req = export_meta.get("http_requests_made")
    if http_req is not None:
        lines.append(f"- **HTTP requests:** {http_req}")
    fetch_s = processing.get("fetch_seconds")
    if fetch_s is not None:
        lines.append(f"- **Fetch duration (s):** {fetch_s}")
    obs_n = table_counts.get("observations")
    if obs_n is not None:
        lines.append(f"- **Observations in GeoPackage:** {obs_n}")
    skipped = processing.get("skipped_no_stream")
    if skipped:
        lines.append(f"- **Skipped (no datastream):** {skipped}")
    return "\n".join(lines)


def _build_processing_expression(process_id: str, inputs: dict) -> dict[str, Any]:
    return {
        "format": "ogc-api-processes",
        "expression": {
            "processId": process_id,
            "inputs": inputs,
        },
    }


def _build_processing_software_map(
    process_id: str,
    process_version: str,
    docker: dict,
) -> dict[str, str]:
    software: dict[str, str] = {process_id: process_version}
    image_name = docker.get("imageName") or docker.get("image")
    image_hash = docker.get("imageHash") or docker.get("image_hash")
    if image_name:
        key = str(image_name)
        software[key] = str(image_hash) if image_hash else process_version
    return software


def _stac_api_base(catalog_root: str, api_prefix: str | None) -> str:
    """STAC API base URL: catalog root plus optional path segment (e.g. ``stac``)."""
    base = catalog_root.rstrip("/")
    if not api_prefix or api_prefix.strip() in ("", "/"):
        return base
    return f"{base}/{api_prefix.strip().strip('/')}"


def _stac_catalog_hrefs(
    catalog_root: str,
    collection_id: str,
    item_id: str,
    *,
    api_prefix: str | None = None,
) -> dict[str, str]:
    """OGC STAC API layout: {base}/collections/{id}/items/{itemId}."""
    api_base = _stac_api_base(catalog_root, api_prefix)
    collection_href = f"{api_base}/collections/{collection_id}"
    return {
        "root":       f"{api_base}/",
        "collection": collection_href,
        "self":       f"{collection_href}/items/{item_id}",
    }


def _require_ipt_label(ipt_labels: dict, key: str) -> str:
    value = _ipt_label(ipt_labels, key)
    if not value:
        execution_fail(
            f"describeProcessing requires iptLabels[{key!r}] "
            f"(Docker LABEL ogcapi.processes.ipt.{key}=…)",
        )
    return value


def _new_stac_item_id() -> str:
    return str(uuid.uuid4())


def _build_stac_catalog_links(ipt_labels: dict, item_id: str) -> list[dict[str, Any]]:
    """
    Catalog navigation links from IPT image labels (required for catalog upload).

    Labels (prefix ogcapi.processes.ipt. on the image):
      stac-catalog-root   — STAC API root, e.g. https://stac.example (no trailing slash)
      stac-collection-id  — collection id this process publishes into
      chaincode           — catalog chaincode for asset PUT (properties.chaincode)
      stac-api-prefix     — optional extra segment before /collections, e.g. ``stac``
      stac-license        — optional; copied to Item properties.license
    """
    catalog_root = _require_ipt_label(ipt_labels, "stac-catalog-root")
    collection_id = _require_ipt_label(ipt_labels, "stac-collection-id")
    api_prefix = _ipt_label(ipt_labels, "stac-api-prefix")
    hrefs = _stac_catalog_hrefs(
        catalog_root, collection_id, item_id, api_prefix=api_prefix,
    )
    return [
        {
            "rel":   "root",
            "href":  hrefs["root"],
            "type":  STAC_JSON_TYPE,
            "title": "STAC catalog root",
        },
        {
            "rel":        "collection",
            "href":       hrefs["collection"],
            "type":       STAC_JSON_TYPE,
            "title":      f"STAC collection {collection_id}",
            "collection": collection_id,
        },
        {
            "rel":   "self",
            "href":  hrefs["self"],
            "type":  STAC_GEOJSON_TYPE,
            "title": "This STAC Item",
        },
    ]


def _resolve_output_bytes(execution: dict, processing: dict) -> int | None:
    """Output byte count from framework execution block or execute-phase metadata."""
    for source in (
        execution.get("outputBytes"),
        execution.get("output_bytes"),
        (execution.get("ipfs") or {}).get("size"),
        processing.get("output_bytes"),
        processing.get("outputBytes"),
    ):
        if source is None or source == "":
            continue
        try:
            return int(source)
        except (TypeError, ValueError):
            continue
    return None


def _merge_stac_links(*link_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge link lists; first occurrence wins per ``rel``."""
    by_rel: dict[str, dict[str, Any]] = {}
    for group in link_groups:
        for link in group:
            rel = link.get("rel")
            if rel and rel not in by_rel:
                by_rel[rel] = link
    return list(by_rel.values())


def _sta_service_root_url(observations_url: str) -> str | None:
    """Derive STA service root (…/v1.1/) from an Observations collection URL."""
    path = urlparse(observations_url).path.rstrip("/")
    if path.endswith("/Observations"):
        return observations_url[: observations_url.index(path)] + path[: -len("/Observations")] + "/"
    return None


def _build_stac_lineage_links(
    context: dict,
    source_url: str | None,
    *,
    inputs: dict | None = None,
) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    if source_url:
        derived: dict[str, Any] = {
            "rel":   "derived_from",
            "href":  source_url,
            "type":  "application/json",
            "title": "SensorThings observations (input data)",
        }
        odata_filter = (inputs or {}).get("filter")
        if odata_filter:
            derived["description"] = f"OData $filter: {odata_filter}"
        links.append(derived)

        service_root = _sta_service_root_url(source_url)
        if service_root and service_root != source_url:
            links.append({
                "rel":   "via",
                "href":  service_root,
                "type":  "application/json",
                "title": "SensorThings API service root",
            })
        else:
            links.append({
                "rel":   "via",
                "href":  source_url,
                "type":  "application/json",
                "title": "SensorThings API",
            })
    job_id = context.get("jobId")
    api_base = (
        context.get("processesApiUrl")
        or os.environ.get("OGC_PROCESSES_API_URL")
    )
    if api_base and job_id:
        links.append({
            "rel":   "processing-execution",
            "href":  f"{str(api_base).rstrip('/')}/api/1.0/jobs/{job_id}",
            "title": "OGC API Processes job",
        })
    return links


def build_stac_item(context: dict, processing: dict) -> dict:
    """Build STAC Item JSON from framework context + execute-phase processing meta."""
    job_id     = context.get("jobId") or os.environ.get("OGC_JOB_ID", "unknown")
    process_id = context.get("processId") or "sta_to_gpkg"
    execution  = context.get("execution") or {}
    ipt_labels = _effective_ipt_labels(context)
    inputs     = context.get("inputs") or {}

    export_meta = processing.get("export_metadata") or {}
    table_counts = processing.get("table_counts") or {}
    phenomenon = processing.get("phenomenon_time_range") or {}

    source_url = export_meta.get("source_url") or inputs.get("url")
    export_time = export_meta.get("export_time")

    docker = context.get("docker") or execution.get("docker") or {}
    process_version = (
        _ipt_label(ipt_labels, "processing-version") or DEFAULT_PROCESS_VERSION
    )

    properties: dict[str, Any] = {
        "title":    "SensorThings observations export (GeoPackage)",
        "description": (
            f"GeoPackage export from SensorThings API ({source_url or 'unknown'})"
        ),
        "gpkg:metadata": _build_gpkg_metadata(processing),
        **_passthrough_ipt_labels(ipt_labels),
    }

    if export_time:
        properties["processing:datetime"] = export_time

    _apply_data_temporal_extent(properties, phenomenon)

    properties["processing:version"] = process_version
    properties["processing:lineage"] = _build_processing_lineage(
        source_url=source_url,
        export_meta=export_meta,
        processing=processing,
        table_counts=table_counts,
    )
    properties["processing:expression"] = _build_processing_expression(
        process_id, inputs,
    )
    properties["processing:software"] = _build_processing_software_map(
        process_id, process_version, docker,
    )

    provider_name = _ipt_label(ipt_labels, "software-provider")
    if provider_name:
        properties["providers"] = [{"name": provider_name}]

    proc_level = _ipt_label(ipt_labels, "processing-level")
    if proc_level:
        properties["processing:level"] = proc_level

    proc_facility = _ipt_label(ipt_labels, "processing-facility")
    if proc_facility:
        properties["processing:facility"] = proc_facility

    collection_id = _require_ipt_label(ipt_labels, "stac-collection-id")
    properties["collection"] = collection_id
    properties["chaincode"] = _require_ipt_label(ipt_labels, "chaincode")
    stac_license = _ipt_label(ipt_labels, "stac-license")
    if stac_license:
        properties["license"] = stac_license

    item_id = _new_stac_item_id()
    catalog_links = _build_stac_catalog_links(ipt_labels, item_id)
    lineage_links = _build_stac_lineage_links(context, source_url, inputs=inputs)
    links = _merge_stac_links(catalog_links, lineage_links)

    bbox, geometry = _resolve_stac_spatial(processing)

    exec_hash = execution.get("outputSha256") or execution.get("output_sha256")
    exec_bytes = _resolve_output_bytes(execution, processing)
    ipfs = execution.get("ipfs") or {}

    result_asset: dict[str, Any] = {
        "title": "SensorThings GeoPackage export",
        "roles": ["data"],
        "type":  GPKG_MEDIA_TYPE,
    }
    file_checksum: str | None = sha256_digest_to_file_checksum(exec_hash)
    if file_checksum:
        result_asset["file:checksum"] = file_checksum
    if exec_bytes is not None and exec_bytes >= 0:
        result_asset["file:size"] = exec_bytes
    asset_href = ipfs.get("gatewayUrl") or ipfs.get("ipfsUri")
    if asset_href:
        result_asset["href"] = asset_href

    return {
        "type":            "Feature",
        "stac_version":    "1.0.0",
        "stac_extensions": [STAC_PROCESSING_EXTENSION, STAC_FILE_EXTENSION],
        "id":              item_id,
        "geometry":        geometry,
        "bbox":            bbox,
        "properties":      properties,
        "assets":          {"PRODUCT": result_asset},
        "links":           links,
    }


def describe_processing() -> None:
    """IPT describeProcessing: framework context on stdin → STAC Item on stdout."""
    try:
        context = read_json_stdin()
        processing = context.get("processing")
        if not isinstance(processing, dict) or not processing:
            processing = parse_processing_meta(context.get("stderr"))
        if not processing:
            execution_fail(
                "describeProcessing: missing processing metadata "
                "(no OGC_PROCESSING_META on execute stderr)",
            )
        stac_item = build_stac_item(context, processing)
        _require_ipt_stac_bbox(stac_item.get("bbox"))
        json.dump(stac_item, sys.stdout, ensure_ascii=False)
        sys.stdout.write("\n")
        sys.stdout.flush()
    except SystemExit:
        raise
    except Exception as exc:
        execution_fail(f"describeProcessing failed: {exc}")


# ---------------------------------------------------------------------------
# stdin → stdout entry point
# ---------------------------------------------------------------------------

def read_request() -> dict:
    """
    Read and validate the JSON request document from STDIN.

    Required fields
    ---------------
      url     : str   — any STA /Observations URL

    Optional fields
    ---------------
      filter           : str   — OData $filter expression
      top              : int   — page size per HTTP request (default 1000)
      max_observations : int   — fetch exactly this many when the server has enough
                                 (0 = no limit); paged $top on every request
      timeout          : int   — HTTP timeout in seconds (default 30)
      verbose          : bool  — enable DEBUG logging (default false)
    """
    try:
        raw = sys.stdin.buffer.read()
    except Exception as exc:
        execution_fail(f"Failed to read STDIN: {exc}")

    try:
        req = json.loads(raw)
    except json.JSONDecodeError as exc:
        execution_fail(f"STDIN is not valid JSON: {exc}")

    if not isinstance(req, dict):
        execution_fail("STDIN JSON must be an object / dict")

    if "url" not in req:
        execution_fail('STDIN JSON must contain a "url" field')

    return req


def run_export(req: dict, output_path: str) -> dict:
    """
    Fetch STA observations and write a GeoPackage to output_path.
    Returns processing metadata for describeProcessing / STAC.
    """
    obs_url    = req["url"]
    sta_filter = req.get("filter")
    page_size  = int(req.get("top",              1000))
    max_obs    = int(req.get("max_observations", 0))
    timeout    = int(req.get("timeout",          30))
    verbose    = bool(req.get("verbose",         False))

    if max_obs < 0:
        execution_fail("max_observations must be >= 0")

    if max_obs > 0:
        capped = min(page_size, max_obs)
        if capped >= max_obs and max_obs > MAX_HTTP_TOP_WHEN_CAPPED:
            log.info(
                "max_observations=%d: paging with $top=%d per request (input top=%d) "
                "so the cap is reached via @iot.nextLink",
                max_obs,
                MAX_HTTP_TOP_WHEN_CAPPED,
                page_size,
            )
            page_size = MAX_HTTP_TOP_WHEN_CAPPED
        else:
            page_size = capped

    if verbose and not _running_under_ogc_worker():
        log.setLevel(logging.DEBUG)

    _last_fetch_progress = 10

    def _emit_fetch_progress(pct: int) -> None:
        nonlocal _last_fetch_progress
        bounded = max(10, min(79, int(pct)))
        if bounded > _last_fetch_progress:
            _last_fetch_progress = bounded
            emit_progress(bounded)

    def report_fetch_progress(
        fetched: int,
        total: int | None,
        cap: int,
        page_top_hint: int = 0,
        waiting_pulse: bool = False,
    ) -> None:
        """
        Map fetch state to 10–79% (80+ reserved for write/finalize).

        ``page_top_hint`` > 0: optimistic advance before/during an HTTP page.
        ``waiting_pulse``: +1 every ~2s while a STA request is in flight.
        """
        if waiting_pulse:
            _emit_fetch_progress(min(79, _last_fetch_progress + 1))
            return

        est = fetched
        if page_top_hint > 0:
            inflight = max(1, page_top_hint // 2)
            if total and total > 0:
                est = min(int(total), fetched + inflight)
            elif cap > 0:
                est = min(cap, fetched + inflight)

        if total and total > 0:
            _emit_fetch_progress(10 + int(70 * min(est, total) / total))
        elif cap > 0:
            _emit_fetch_progress(10 + int(70 * min(est, cap) / cap))
        elif page_top_hint > 0 or fetched > 0:
            _emit_fetch_progress(min(79, 12 + fetched // 25))

    emit_progress(0)

    log.info("=== SensorThings → GeoPackage ===")
    log.info("Source           : %s", obs_url)
    log.info("Output path      : %s", output_path)
    log.info("max_observations : %s", max_obs if max_obs > 0 else "unlimited")

    base_url = ServiceCapabilities.base_url_from_obs_url(obs_url)
    log.info("Service root: %s", base_url)

    emit_progress(2)
    probe_session = requests.Session()
    probe_session.headers["Accept"] = "application/json"
    caps = ServiceCapabilities(base_url, probe_session, timeout)
    emit_progress(5)

    client = STAClient(obs_url, caps, page_size=page_size, timeout=timeout)
    graph  = EntityGraph()

    extra: dict[str, str] = {}
    if sta_filter:
        extra["$filter"] = sta_filter

    log.info("Fetching observations…")
    t0 = time.time()
    ingest_progress_stride = 200
    for obs in client.paginate(
        extra, max_observations=max_obs, on_progress=report_fetch_progress,
    ):
        graph.ingest(obs)
        n = len(graph.observations)
        if n > 0 and n % ingest_progress_stride == 0:
            report_fetch_progress(n, client.obs_total, max_obs)
    elapsed = time.time() - t0
    emit_progress(80)

    log.info("Fetch complete in %.1fs — %s", elapsed, graph.summary())
    if graph.skipped_no_stream:
        log.warning(
            "Skipped %d fetched observation(s) without Datastream/MultiDatastream "
            "(not written to observations table)",
            graph.skipped_no_stream,
        )
    log.info("Total HTTP requests: %d", client.request_count)

    emit_progress(85)
    log.info("Writing GeoPackage…")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    gpkg = GeoPackageWriter(output_path)
    gpkg.write(graph, source_url=obs_url,
               request_count=client.request_count,
               has_mds=caps.has_multi_datastream)
    gpkg.close()

    gpkg_bytes = os.path.getsize(output_path)
    gpkg_stats = collect_gpkg_stats(output_path)
    emit_progress(95)
    log.info("GeoPackage written — %d bytes", gpkg_bytes)

    emit_progress(100)

    return {
        "export_metadata":       gpkg_stats.get("export_metadata", {}),
        "table_counts":          gpkg_stats.get("table_counts", {}),
        "bbox":                  gpkg_stats.get("bbox"),
        "footprint":             gpkg_stats.get("footprint"),
        "phenomenon_time_range": gpkg_stats.get("phenomenon_time_range"),
        "fetch_seconds":         round(elapsed, 3),
        "output_bytes":          gpkg_bytes,
        "local_sha256":          file_sha256(output_path),
        "media_type":            GPKG_MEDIA_TYPE,
        "skipped_no_stream":     graph.skipped_no_stream,
        "max_observations":      max_obs if max_obs > 0 else None,
        "observations_fetched":  len(graph.observations),
    }


def main():
    """Execute: GeoPackage on stdout; OGC_PROCESSING_META on stderr; errors on stdout under OGC."""
    try:
        req = read_request()
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            processing = run_export(req, tmp_path)
            emit_processing_meta(processing)
            stdout_bin = sys.stdout.buffer if hasattr(sys.stdout, "buffer") else sys.stdout
            with open(tmp_path, "rb") as f:
                shutil.copyfileobj(f, stdout_bin)
            stdout_bin.flush()
        finally:
            os.unlink(tmp_path)
    except SystemExit:
        raise
    except Exception as exc:
        execution_fail(f"{type(exc).__name__}: {exc}")


if __name__ == "__main__":
    if os.environ.get("OGC_ACTION") == "describeProcessing":
        describe_processing()
    else:
        main()
