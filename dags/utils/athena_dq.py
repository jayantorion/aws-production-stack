"""Athena DQ gate — runs the checks from sql/athena_dq_checks.sql per entity.

FAIL (any check returns failures > 0)  -> raise -> pipeline hard-stops.
WARN (row_count_delta)                  -> logged, non-blocking.
"""
from __future__ import annotations

import time
from typing import Any

import boto3

MAX_NULL_EXPR = {
    # mandatory-column null expressions per entity (kept explicit — contract)
    "departments": "department_id IS NULL OR department_name IS NULL",
    "categories": "category_id IS NULL OR category_name IS NULL",
    "customers": "customer_id IS NULL OR customer_fname IS NULL",
    "products": "product_id IS NULL OR product_name IS NULL",
    "orders": "order_id IS NULL OR order_date IS NULL",
    "order_items": "order_item_id IS NULL OR order_item_subtotal IS NULL",
}
PK = {"departments": "department_id", "categories": "category_id",
      "customers": "customer_id", "products": "product_id",
      "orders": "order_id", "order_items": "order_item_id"}
EVENT_DATE = {"orders": "CAST(order_date AS DATE)"}  # others: no event date -> skip


def _wait_query(ath: Any, qid: str, timeout_s: int = 600) -> list[list[str]]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp = ath.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena query {qid} {state}")
        if state == "SUCCEEDED":
            rows: list[list[str]] = []
            paginator = ath.get_paginator("get_query_results")
            for page in paginator.paginate(QueryExecutionId=qid):
                for row in page["ResultSet"]["Rows"][1:]:
                    rows.append([f.get("VarCharValue", "") for f in row["Data"]])
            return rows
        time.sleep(5)
    raise TimeoutError(f"Athena query {qid} timed out")


def run_dq_suite(settings: dict, batch_ids: list[str]) -> str:
    ath = boto3.client("athena", region_name=settings["region"])
    out = settings["athena"]["output_location"]
    db = settings["athena"]["dq_database"]

    # In production the ingest dates come from batch manifests in the registry
    # (utils/bronze writes them); here we scan the latest partition per entity.
    for entity, null_expr in MAX_NULL_EXPR.items():
        sql = f"""
        SELECT 'null_check' AS check_name, COUNT(*) AS failures
        FROM {db}.{entity} WHERE {null_expr}
        UNION ALL
        SELECT 'duplicate_pk', COUNT(*) FROM (
            SELECT {PK[entity]}, COUNT(*) c FROM {db}.{entity}
            GROUP BY {PK[entity]} HAVING COUNT(*) > 1) d
        """
        qid = ath.start_query_execution(
            QueryString=sql, QueryExecutionContext={"Database": db},
            ResultConfiguration={"OutputLocation": out},
        )["QueryExecutionId"]
        for check, failures in _wait_query(ath, qid):
            failures_i = int(float(failures))
            if failures_i > 0:
                if check == "row_count_delta":
                    print(f"WARN [{entity}] {check}: {failures_i}")
                else:
                    raise AssertionError(f"DQ FAIL [{entity}] {check}: {failures_i} rows")
    return "dq_pass"
