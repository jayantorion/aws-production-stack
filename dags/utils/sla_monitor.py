"""SLA enforcement & monitoring (docs/SLA.md, config/sla.yaml).

Enforcement points:
  - Per-task Airflow `sla` budgets  -> `sla_miss_alert` callback -> SNS (P1/P2)
  - `gold_freshness_sla_gate` task  -> SLA-F1: gold ready by 04:00 UTC,
    CloudWatch metric `GoldFreshnessLagMinutes` under namespace `DEP/SLA`
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any

import boto3

from utils.config_loader import load_yaml_file

SLA: dict[str, Any] = load_yaml_file("sla.yaml")
DEFAULT_STAGE_SLA_MIN = 30


def stage_sla_minutes(task_id: str) -> int:
    """Airflow `sla` budget (minutes from run start) for a task id.
    Matches by substring, e.g. land_bronze_customers -> land_bronze: 30."""
    budgets = SLA["pipeline"]["stage_sla_minutes"]
    for stage, minutes in budgets.items():
        if stage in task_id:
            return int(minutes)
    return DEFAULT_STAGE_SLA_MIN


def deadline_for_data_date(data_date: date,
                           gold_ready_by_utc: str | None = None) -> datetime:
    """SLA-F1: data of `data_date` must be in gold by 04:00 UTC the NEXT day."""
    hh, mm = (gold_ready_by_utc or SLA["freshness"]["gold_ready_by_utc"]).split(":")
    return datetime.combine(data_date + timedelta(days=1),
                            time(int(hh), int(mm)), tzinfo=timezone.utc)


def evaluate_gold_freshness(now_utc: datetime, data_date: date,
                            gold_ready_by_utc: str | None = None) -> dict[str, Any]:
    """Pure SLA-F1 evaluation (unit-testable): OK if now <= deadline, else breach
    with lag in minutes."""
    deadline = deadline_for_data_date(data_date, gold_ready_by_utc)
    lag_minutes = round((now_utc - deadline).total_seconds() / 60, 1)
    return {"ok": now_utc <= deadline, "deadline_utc": deadline.isoformat(),
            "lag_minutes": max(lag_minutes, 0.0)}


def publish_metric(metric_name: str, value: float,
                   region: str | None = None) -> None:
    """Emit a CloudWatch metric under the DEP/SLA namespace (NFR-8/SLA reporting)."""
    cw = boto3.client("cloudwatch", region_name=region)
    cw.put_metric_data(
        Namespace=SLA["alerts"]["cloudwatch_namespace"],
        MetricData=[{"MetricName": metric_name,
                     "Value": float(value), "Unit": "None"}],
    )


def alert_sla_miss(subject: str, message: str, region: str | None = None,
                   settings: dict[str, Any] | None = None) -> None:
    """Publish a P1/P2 SLA-miss alert to the pipeline's SNS alerts topic."""
    topic_arn = (settings or {}).get("sns", {}).get("alerts_topic_arn", "")
    if not topic_arn:
        return
    sns = boto3.client("sns", region_name=region)
    sns.publish(TopicArn=topic_arn, Subject=subject[:99], Message=message)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis, *args, **kwargs):
    """Airflow DAG-level `sla_miss_callback` — turns a missed task SLA into a P2 alert."""
    alert_sla_miss(
        subject="[DEP][SLA-MISS] P2: task budget exceeded",
        message=f"dag={getattr(dag, 'dag_id', '?')} tasks={task_list} "
                f"blocking={blocking_task_list}",
    )
    publish_metric("SlasMissed", 1)


def check_gold_freshness(settings: dict[str, Any], now_utc: datetime,
                         data_date: date, region: str | None = None) -> dict[str, Any]:
    """Task-side wrapper: evaluate SLA-F1, emit metric, alert+raise on breach (P1)."""
    result = evaluate_gold_freshness(now_utc, data_date)
    publish_metric("GoldFreshnessLagMinutes", result["lag_minutes"], region=region)
    if not result["ok"]:
        alert_sla_miss(
            subject="[DEP][SLA-MISS] P1: gold marts missed 04:00 UTC freshness SLA",
            message=f"data_date={data_date} lag_minutes={result['lag_minutes']} "
                    f"deadline={result['deadline_utc']}",
            region=region, settings=settings,
        )
        raise RuntimeError(
            f"SLA-F1 BREACH: gold not ready by {result['deadline_utc']} "
            f"(lag {result['lag_minutes']} min) — P1 incident opened")
    return result
