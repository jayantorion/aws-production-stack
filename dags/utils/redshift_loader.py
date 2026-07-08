"""Redshift loader — COPY silver -> staging -> transactional merge.

Uses the Redshift Data API (boto3, no driver dependency). Loads are
batch_id-scoped: re-running a batch is a net no-op (LINEAGE.md §3/§6).
IAM_ROLE / network access must be configured per environment.
"""
from __future__ import annotations

from typing import Any

import boto3

from utils.config_loader import load_entities

ENTITIES = load_entities()

COLUMNS = {
    e: ", ".join(c["name"] for c in cfg["columns"])
    for e, cfg in ENTITIES.items()
}
PKS = {
    e: next(c["name"] for c in cfg["columns"] if c.get("primary_key"))
    for e, cfg in ENTITIES.items()
}

LOAD_SQL = """
BEGIN;
COPY staging.{entity} ({cols})
FROM '{silver_path}'
IAM_ROLE '{iam_role}'
FORMAT AS PARQUET SERIALIZETOJSON;
DELETE FROM analytics.{entity} WHERE batch_id = '{batch_id}';
DELETE FROM analytics.{entity} USING staging.{entity} s
  WHERE analytics.{entity}.{pk} = s.{pk};
INSERT INTO analytics.{entity} ({cols}, batch_id)
SELECT {cols}, '{batch_id}' FROM staging.{entity};
TRUNCATE staging.{entity};
ANALYZE analytics.{entity};
COMMIT;
"""


def _exec(rs: Any, settings: dict, sql: str) -> None:
    resp = rs.execute_statement(
        WorkgroupName=settings["redshift"]["workgroup"],
        Database=settings["redshift"]["database"],
        Sql=sql,
        WaitInSeconds=0,
    )
    qid = resp["Id"]
    while True:
        status = rs.describe_statement(Id=qid)
        if status["Status"] == "FINISHED":
            return
        if status["Status"] == "FAILED":
            raise RuntimeError(f"Redshift statement failed: {status.get('Error')}")


def load_entity(settings: dict, entity: str, batch_id: str) -> str:
    rs = boto3.client("redshift-data", region_name=settings["region"])
    import os
    sql = LOAD_SQL.format(
        entity=entity, cols=COLUMNS[entity], pk=PKS[entity], batch_id=batch_id,
        silver_path=f"s3://{settings['s3']['silver_bucket']}/silver/{entity}/",
        iam_role=os.environ.get("DEP_REDSHIFT_IAM_ROLE", ""),
    )
    _exec(rs, settings, sql)
    return f"{entity}:{batch_id}:loaded"


def load_all_entities(settings: dict, batch_ids: list[str]) -> list[str]:
    results = []
    for entity, batch_id in zip(ENTITIES.keys(), batch_ids):
        results.append(load_entity(settings, entity, batch_id))
    return results
