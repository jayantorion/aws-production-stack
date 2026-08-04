"""Unit tests for dynamic capacity planning (data growth 40 GB -> 80 GB)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from utils.capacity import plan_from_manifests, plan_glue_capacity

GB = 1024 ** 3


def test_small_batch_gets_small_fleet():
    plan = plan_glue_capacity(500 * 1024 ** 2)   # 0.5 GB (like retail_db dev data)
    assert plan["worker_type"] == "G.1X" and plan["number_of_workers"] == 4


def test_40gb_day():
    plan = plan_glue_capacity(40 * GB)
    assert plan["number_of_workers"] == 40


def test_80gb_day_scales_up_within_sla():
    small = plan_glue_capacity(40 * GB)
    big = plan_glue_capacity(80 * GB)
    assert big["number_of_workers"] > small["number_of_workers"]
    assert big["worker_type"] == "G.2X"
    # runtime stays bounded even when volume doubles
    assert big["estimated_minutes"] <= small["estimated_minutes"] + 30


def test_monotonic_scaling():
    workers = [plan_glue_capacity(g * GB)["number_of_workers"] for g in (1, 5, 20, 50, 100)]
    assert workers == sorted(workers)


def test_plan_from_manifests_sums_real_bytes():
    manifests = [
        {"files": [{"bytes": 20 * GB}, {"bytes": 5 * GB}]},
        {"files": [{"bytes": 15 * GB}]},
    ]
    plan = plan_from_manifests(manifests)
    assert plan["input_gb"] == 40.0 and plan["number_of_workers"] == 40
