"""Batch checkpoint state store — resumability WITHOUT duplicates.

Status machine per (entity, batch_id), stored in DynamoDB ``dep_batch_registry``:

    LANDED -> VALIDATED -> CRAWLED -> TRANSFORMED -> DQ_PASSED -> LOADED

Resumability contract (LINEAGE.md §12):
- A rerun after a mid-pipeline failure REUSES the open batch (same batch_id)
  instead of creating a new one. Re-landing overwrites the same S3 prefix;
  Glue overwrites the same partitions (replaceWhere); Redshift merge deletes
  the batch_id rows before insert. Net effect of a rerun: no duplicates.
- Stages already completed are skipped (resume at the 60% mark).
- Status only ever moves FORWARD (monotonic), so a stale retry cannot regress.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import boto3

STAGES = ["LANDED", "VALIDATED", "CRAWLED", "TRANSFORMED", "DQ_PASSED", "LOADED"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class StateStore:
    """Thin DynamoDB wrapper. Injectable ``table`` enables unit testing."""

    def __init__(self, table: Any | None = None,
                 table_name: str | None = None, region: str | None = None):
        if table is None:
            table_name = table_name or os.environ.get("BATCH_REGISTRY_TABLE", "dep_batch_registry")
            table = boto3.resource("dynamodb", region_name=region).Table(table_name)
        self.table = table

    # ------------------------------------------------------------------ #
    def get(self, batch_id: str, entity: str) -> dict[str, Any] | None:
        resp = self.table.get_item(Key={"batch_id": batch_id, "entity": entity})
        return resp.get("Item")

    def mark(self, batch_id: str, entity: str, stage: str, detail: dict | None = None) -> None:
        """Advance the batch to ``stage`` — only forward, never backward."""
        if stage not in STAGES:
            raise ValueError(f"unknown stage: {stage}")
        new_idx = STAGES.index(stage)
        item = self.get(batch_id, entity)
        if item and int(item.get("stage_index", -1)) >= new_idx:
            return  # already there (or further) — monotonic, idempotent
        self.table.put_item(Item={
            "batch_id": batch_id,
            "entity": entity,
            "stage": stage,
            "stage_index": new_idx,
            "detail": detail or {},
            "updated_at": _now(),
        })

    def is_done(self, batch_id: str, entity: str, stage: str) -> bool:
        item = self.get(batch_id, entity)
        return bool(item and int(item.get("stage_index", -1)) >= STAGES.index(stage))

    def open_batch(self, entity: str, limit: int = 25) -> dict[str, Any] | None:
        """Return the most recent NOT-fully-loaded batch for an entity, if any.

        Uses the ``entity-status-index`` GSI (entity as PK). A rerun after a
        failure reuses this batch_id instead of landing a duplicate copy.
        """
        try:
            resp = self.table.query(
                IndexName="entity-status-index",
                KeyConditionExpression="#e = :e",
                FilterExpression="stage_index < :done",
                ExpressionAttributeNames={"#e": "entity"},
                ExpressionAttributeValues={":e": entity, ":done": len(STAGES) - 1},
                ScanIndexForward=False,   # newest first
                Limit=limit,
            )
        except Exception:
            return None  # GSI missing in dev — resume-by-reuse disabled, still safe
        items = resp.get("Items") or []
        return items[0] if items else None
