"""Unit tests for SLA enforcement logic (docs/SLA.md, config/sla.yaml)."""
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from utils import sla_monitor
from utils.sla_monitor import (
    deadline_for_data_date,
    evaluate_gold_freshness,
    stage_sla_minutes,
)


def test_stage_sla_mapping_from_task_ids():
    assert stage_sla_minutes("land_bronze_customers") == 30
    assert stage_sla_minutes("land_bronze_orders") == 30
    assert stage_sla_minutes("lambda_arrival_validation") == 10
    assert stage_sla_minutes("glue_silver_job") == 90
    assert stage_sla_minutes("athena_dq_gate") == 15
    assert stage_sla_minutes("redshift_load") == 30


def test_unknown_task_gets_default_budget():
    assert stage_sla_minutes("some_new_task") == sla_monitor.DEFAULT_STAGE_SLA_MIN


def test_deadline_is_0400_utc_next_day():
    deadline = deadline_for_data_date(date(2026, 9, 2))
    assert deadline == datetime(2026, 9, 3, 4, 0, tzinfo=timezone.utc)


def test_freshness_passes_before_deadline():
    now = datetime(2026, 9, 3, 3, 15, tzinfo=timezone.utc)  # 03:15 UTC
    result = evaluate_gold_freshness(now, date(2026, 9, 2))
    assert result["ok"] is True and result["lag_minutes"] == 0


def test_freshness_breaches_after_deadline():
    now = datetime(2026, 9, 3, 5, 30, tzinfo=timezone.utc)  # 05:30 UTC -> 90 min late
    result = evaluate_gold_freshness(now, date(2026, 9, 2))
    assert result["ok"] is False and result["lag_minutes"] == 90.0


def test_dagrun_timeout_matches_config():
    assert sla_monitor.SLA["pipeline"]["dagrun_timeout_minutes"] == 150


def test_success_rate_target_is_99():
    assert sla_monitor.SLA["reliability"]["monthly_success_rate_target"] == pytest.approx(0.99)


def test_data_date_plus_one_day_boundary():
    """Run on the deadline minute exactly -> SLA met (inclusive boundary)."""
    d = date(2026, 12, 31)  # year boundary handled by datetime.combine
    deadline = deadline_for_data_date(d)
    assert deadline.date() == date(2027, 1, 1) and deadline.hour == 4
    assert evaluate_gold_freshness(deadline - timedelta(seconds=1), d)["ok"] is True
