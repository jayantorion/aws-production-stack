"""Bronze (raw) landing utilities — the idempotency core of the pipeline.

Design guarantees (see LINEAGE.md §3):
- Every landing is scoped by a unique ``batch_id`` — re-running the same batch
  overwrites the same S3 prefix (never appends).
- A ``_MANIFEST.json`` (row counts, per-file checksums, watermark used) is
  written with every batch; S3 events on the manifest trigger Lambda validation.
- Watermarks are supplied by the caller (Airflow control table) and only
  advance AFTER the manifest is verified downstream.

S3 layout:
    s3://{raw_bucket}/raw/{source}/{entity}/ingest_date=YYYY-MM-DD/batch_id={id}/part-00000.csv
    s3://{raw_bucket}/raw/{source}/{entity}/ingest_date=YYYY-MM-DD/batch_id={id}/_MANIFEST.json
"""
from __future__ import annotations

import hashlib
import io
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

MANIFEST_NAME = "_MANIFEST.json"
ROWS_PER_FILE = 200_000  # ~ keeps parts well under the 256 MB target


def generate_batch_id() -> str:
    return uuid.uuid4().hex


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_source_csv(entity: str, entity_cfg: Dict[str, Any], base_path: str) -> pd.DataFrame:
    """Read a raw source file as-is (bronze = faithful landing, no typing yet).

    Location resolution: explicit ``source.location`` in entities.yaml, else the
    conventional layout ``<entity>/part-00000`` under the source base path.
    """
    location = entity_cfg.get("source", {}).get("location", f"{entity}/part-00000")
    path = Path(base_path) / location
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    names = [c["name"] for c in entity_cfg["columns"]]
    return pd.read_csv(
        path,
        header=None,
        names=names,
        delimiter=entity_cfg.get("delimiter", ","),
        dtype=str,          # bronze stores raw strings; typing happens at silver
        keep_default_na=False,
        encoding=entity_cfg.get("encoding", "utf-8"),
    )


def apply_incremental_filter(
    df: pd.DataFrame,
    entity_cfg: Dict[str, Any],
    watermark: Optional[str],
    ceiling: Optional[str],
) -> pd.DataFrame:
    """Filter rows to (watermark, ceiling] window. Full snapshots pass through."""
    if entity_cfg.get("load_type") != "incremental":
        return df
    col = entity_cfg["incremental"]["watermark_column"]
    if entity_cfg["incremental"].get("driver_entity"):
        # child entity (e.g. order_items) — filtered by parent key range upstream;
        # caller pre-joins scope. Pass through here.
        return df
    if watermark is not None:
        df = df[df[col] > watermark]
    if ceiling is not None:
        df = df[df[col] <= ceiling]
    return df


def checksum_bytes(payload: bytes) -> str:
    return hashlib.md5(payload).hexdigest()


def manifest_s3_key(source: str, entity: str, ingest_date: str, batch_id: str) -> str:
    return f"raw/{source}/{entity}/ingest_date={ingest_date}/batch_id={batch_id}/{MANIFEST_NAME}"


def land_to_bronze(
    df: pd.DataFrame,
    entity: str,
    entity_cfg: Dict[str, Any],
    batch_id: str,
    raw_bucket: str,
    s3_client: Any,
    watermark: Optional[str] = None,
    ceiling: Optional[str] = None,
) -> Dict[str, Any]:
    """Write dataframe to bronze S3 as delimited parts + manifest. Returns manifest dict."""
    source = entity_cfg.get("source_type", "file")
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base_key = f"raw/{source}/{entity}/ingest_date={ingest_date}/batch_id={batch_id}"

    files: List[Dict[str, Any]] = []
    total_rows = 0
    delimiter = entity_cfg.get("delimiter", ",")

    for part_no, start in enumerate(range(0, max(len(df), 1), ROWS_PER_FILE)):
        chunk = df.iloc[start:start + ROWS_PER_FILE]
        buffer = io.StringIO()
        chunk.to_csv(buffer, header=False, index=False, sep=delimiter, lineterminator="\n")
        payload = buffer.getvalue().encode("utf-8")
        key = f"{base_key}/part-{part_no:05d}.csv"
        s3_client.put_object(Bucket=raw_bucket, Key=key, Body=payload)
        files.append({"key": key, "bytes": len(payload), "checksum": checksum_bytes(payload), "rows": len(chunk)})
        total_rows += len(chunk)

    manifest: Dict[str, Any] = {
        "batch_id": batch_id,
        "entity": entity,
        "source": source,
        "ingest_date": ingest_date,
        "s3_prefix": f"s3://{raw_bucket}/{base_key}/",
        "row_count": total_rows,
        "file_count": len(files),
        "files": files,
        "watermark_used": watermark,
        "ceiling_used": ceiling,
        "load_type": entity_cfg.get("load_type"),
        "arrived_at": utc_now_iso(),
        "schema": [{"name": c["name"], "type": c["type"]} for c in entity_cfg["columns"]],
    }
    s3_client.put_object(
        Bucket=raw_bucket,
        Key=manifest_s3_key(source, entity, ingest_date, batch_id),
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
    )
    return manifest
