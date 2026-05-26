#!/usr/bin/env python3
"""Build describeProcessing stdin JSON from harness execute artifacts."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sta_to_gpkg import collect_gpkg_stats  # noqa: E402

GPKG_MEDIA = "application/geopackage+sqlite3"
META_PREFIX = "OGC_PROCESSING_META:"
IPT_LABEL_PREFIX = "ogcapi.processes.ipt."
DEFAULT_IMAGE = "sta_to_gpkg:ipt-harness"
DEFAULT_LABELS_FILE = os.path.join(os.path.dirname(__file__), "ipt-labels.json")


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def parse_processing_meta(stderr_path: str) -> dict:
    if not os.path.isfile(stderr_path):
        return {}
    with open(stderr_path, encoding="utf-8") as f:
        for line in f:
            if line.startswith(META_PREFIX):
                return json.loads(line[len(META_PREFIX) :])
    return {}


def normalize_ipt_labels(raw: dict) -> dict[str, str]:
    """
    Normalize to describe stdin iptLabels (same as ogc-api-processes filterIptLabels).

    Accepts keys with or without the ogcapi.processes.ipt. prefix.
    """
    out: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        k = key[len(IPT_LABEL_PREFIX):] if key.startswith(IPT_LABEL_PREFIX) else key
        out[k] = str(value)
    return out


def ipt_labels_from_docker_image(image: str) -> dict[str, str]:
    """Read LABEL ogcapi.processes.ipt.* from a built image (production-like)."""
    try:
        proc = subprocess.run(
            ["docker", "inspect", image, "--format", "{{json .Config.Labels}}"],
            capture_output=True,
            text=True,
            check=True,
        )
        all_labels = json.loads(proc.stdout.strip() or "{}")
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"Warning: could not read IPT labels from image {image}: {exc}", file=sys.stderr)
        return {}

    ipt_only = {
        k: v for k, v in all_labels.items()
        if isinstance(k, str) and k.startswith(IPT_LABEL_PREFIX)
    }
    return normalize_ipt_labels(ipt_only)


def load_ipt_labels(
    *,
    image: str,
    labels_file: str | None,
    skip_image: bool,
) -> dict[str, str]:
    merged: dict[str, str] = {}
    if not skip_image:
        merged.update(ipt_labels_from_docker_image(image))
    if labels_file and os.path.isfile(labels_file):
        with open(labels_file, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise SystemExit(f"{labels_file} must be a JSON object")
        merged.update(normalize_ipt_labels(data))
    return merged


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "IPT labels on describe stdin mirror production: ogc-api-processes reads "
            f"Docker LABEL {IPT_LABEL_PREFIX}<name> and passes iptLabels with the prefix stripped."
        ),
    )
    ap.add_argument("--work", default="harness/work", help="Directory with out.gpkg and execute.stderr")
    ap.add_argument("--job-id", required=True)
    ap.add_argument("--process-id", default="sta_to_gpkg")
    ap.add_argument("--cid", default="bafybeiharnesslocalipttest")
    ap.add_argument(
        "--image",
        default=os.environ.get("IPT_HARNESS_IMAGE", DEFAULT_IMAGE),
        help=f"Docker image to inspect for LABEL {IPT_LABEL_PREFIX}* (default: {DEFAULT_IMAGE})",
    )
    ap.add_argument(
        "--ipt-labels-file",
        default=os.environ.get("IPT_LABELS_FILE", DEFAULT_LABELS_FILE),
        help="Optional JSON object of extra/overriding ipt labels (see harness/ipt-labels.example.json)",
    )
    ap.add_argument(
        "--skip-image-labels",
        action="store_true",
        help="Do not read labels from the Docker image (use file/env overrides only)",
    )
    args = ap.parse_args()

    gpkg_path = os.path.join(args.work, "out.gpkg")
    stderr_path = os.path.join(args.work, "execute.stderr")
    input_path = os.path.join(args.work, "input.json")

    if not os.path.isfile(gpkg_path):
        print(f"Missing {gpkg_path} — run execute first.", file=sys.stderr)
        return 1

    processing = parse_processing_meta(stderr_path)
    if not processing:
        print(f"No {META_PREFIX} line in {stderr_path}", file=sys.stderr)
        return 1

    gpkg_stats = collect_gpkg_stats(gpkg_path)
    if gpkg_stats.get("bbox"):
        processing["bbox"] = gpkg_stats["bbox"]
    if gpkg_stats.get("footprint"):
        processing["footprint"] = gpkg_stats["footprint"]
    if gpkg_stats.get("phenomenon_time_range"):
        processing["phenomenon_time_range"] = gpkg_stats["phenomenon_time_range"]

    labels_file = args.ipt_labels_file if args.ipt_labels_file and os.path.isfile(args.ipt_labels_file) else None
    ipt_labels = load_ipt_labels(
        image=args.image,
        labels_file=labels_file,
        skip_image=args.skip_image_labels,
    )
    if not ipt_labels:
        print(
            "Warning: iptLabels is empty — add LABEL ogcapi.processes.ipt.* to the Dockerfile "
            f"or copy harness/ipt-labels.example.json to harness/ipt-labels.json",
            file=sys.stderr,
        )

    size = os.path.getsize(gpkg_path)
    sha = file_sha256(gpkg_path)
    inputs: dict = {}
    if os.path.isfile(input_path):
        with open(input_path, encoding="utf-8") as f:
            inputs = json.load(f)

    stderr_text = ""
    if os.path.isfile(stderr_path):
        stderr_text = open(stderr_path, encoding="utf-8").read()

    docker_meta: dict[str, str] = {}
    deploy_path = os.path.join(ROOT, "deploy-process.json")
    if os.path.isfile(deploy_path):
        with open(deploy_path, encoding="utf-8") as f:
            deploy = json.load(f)
        eu = deploy.get("executionUnit") or {}
        if eu.get("image"):
            docker_meta["imageName"] = eu["image"]
        if eu.get("imageHash"):
            docker_meta["imageHash"] = eu["imageHash"]

    processes_api_url = os.environ.get(
        "OGC_PROCESSES_API_URL", "http://127.0.0.1:8090",
    )

    context = {
        "jobId": args.job_id,
        "processId": args.process_id,
        "inputs": inputs,
        "processesApiUrl": processes_api_url,
        "docker": docker_meta,
        "execution": {
            "outputSha256": sha,
            "outputBytes": size,
            "mediaType": GPKG_MEDIA,
            "ipfs": {
                "cid": args.cid,
                "ipfsUri": f"ipfs://{args.cid}",
                "gatewayUrl": f"http://127.0.0.1:8088/ipfs/{args.cid}",
                "size": size,
            },
        },
        "iptLabels": ipt_labels,
        "processing": processing,
        "stderr": stderr_text,
    }

    json.dump(context, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
