"""Unit tests for the bronze landing utilities (idempotency core)."""
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from utils import bronze


class FakeS3:
    """Minimal in-memory S3 stand-in capturing put_object calls."""

    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[f"s3://{Bucket}/{Key}"] = Body


@pytest.fixture
def entity_cfg():
    return {
        "source_type": "file",
        "delimiter": ",",
        "load_type": "full_snapshot",
        "columns": [
            {"name": "id", "type": "int", "primary_key": True, "nullable": False},
            {"name": "name", "type": "string", "nullable": False},
        ],
    }


@pytest.fixture
def df():
    return pd.DataFrame({"id": ["1", "2", "3"], "name": ["a", "b", "c"]})


def test_generate_batch_id_unique():
    ids = {bronze.generate_batch_id() for _ in range(1000)}
    assert len(ids) == 1000
    assert all(len(i) == 32 for i in ids)


def test_read_source_csv(tmp_path, entity_cfg):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "part-00000").write_text("1,alpha\n2,beta\n")
    entity_cfg["source"] = {"location": "src/part-00000"}
    out = bronze.read_source_csv("customers", entity_cfg, str(tmp_path))
    assert list(out.columns) == ["id", "name"]
    assert len(out) == 2


def test_incremental_filter_respects_window(entity_cfg):
    entity_cfg["load_type"] = "incremental"
    entity_cfg["incremental"] = {"watermark_column": "name"}
    df = pd.DataFrame({"id": ["1", "2", "3"], "name": ["a", "b", "c"]})
    out = bronze.apply_incremental_filter(df, entity_cfg, watermark="a", ceiling="c")
    assert list(out["name"]) == ["b", "c"]
    assert bronze.apply_incremental_filter(df, entity_cfg, None, None).equals(df)


def test_land_to_bronze_manifest_and_idempotent_overwrite(tmp_path, df, entity_cfg):
    s3 = FakeS3()
    m1 = bronze.land_to_bronze(df, "orders_test", entity_cfg, "batch123",
                               "test-raw", s3, watermark=None, ceiling=None)
    assert m1["row_count"] == 3 and m1["batch_id"] == "batch123"
    manifest_key = bronze.manifest_s3_key("file", "orders_test", m1["ingest_date"], "batch123")
    manifest = json.loads(s3.objects[f"s3://test-raw/{manifest_key}"])
    assert manifest["file_count"] == 1
    assert manifest["files"][0]["checksum"] == bronze.checksum_bytes(
        s3.objects[f"s3://test-raw/{manifest['files'][0]['key']}"])

    # Re-landing the SAME batch overwrites the same prefix (idempotent, no dupes)
    n_before = len(s3.objects)
    bronze.land_to_bronze(df, "orders_test", entity_cfg, "batch123",
                          "test-raw", s3, watermark=None, ceiling=None)
    assert len(s3.objects) == n_before
