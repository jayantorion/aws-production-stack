"""Unit tests for the DynamoDB batch checkpoint store (resumability)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from utils.state_store import STAGES, StateStore


class FakeTable:
    """Mimics the DynamoDB Table API surface used by StateStore."""

    def __init__(self):
        self.items = {}

    def get_item(self, Key):
        key = (Key["batch_id"], Key["entity"])
        if key in self.items:
            return {"Item": dict(self.items[key])}
        return {}

    def put_item(self, Item):
        self.items[(Item["batch_id"], Item["entity"])] = dict(Item)

    def query(self, **kwargs):
        values = kwargs["ExpressionAttributeValues"]
        entity = values[":e"]
        done = values[":done"]
        matches = [dict(v) for (b, e), v in self.items.items()
                   if e == entity and v["stage_index"] < done]
        matches.sort(key=lambda x: x["updated_at"], reverse=True)
        return {"Items": matches}


@pytest.fixture
def store():
    return StateStore(table=FakeTable())


def test_stage_machine_forward_only(store):
    store.mark("b1", "orders", "LANDED")
    store.mark("b1", "orders", "VALIDATED")
    assert store.is_done("b1", "orders", "LANDED")
    assert store.is_done("b1", "orders", "VALIDATED")
    assert not store.is_done("b1", "orders", "TRANSFORMED")
    # a stale retry must NOT regress the checkpoint
    store.mark("b1", "orders", "LANDED")
    assert store.is_done("b1", "orders", "VALIDATED")


def test_invalid_stage_rejected(store):
    with pytest.raises(ValueError):
        store.mark("b1", "orders", "TELEPORTED")


def test_resume_reuses_open_batch_after_mid_failure(store):
    """60% processed failure: batch open (not LOADED) -> rerun reuses batch_id."""
    store.mark("b1", "orders", "LANDED")
    store.mark("b1", "orders", "TRANSFORMED")     # pipeline died here (60%)
    opened = store.open_batch("orders")
    assert opened is not None and opened["batch_id"] == "b1"

    store.mark("b2", "orders", "LOADED")          # a completed batch is NOT open
    opened = store.open_batch("orders")
    assert opened["batch_id"] == "b1"

    store.mark("b1", "orders", "LOADED")          # close it -> nothing open
    assert store.open_batch("orders") is None


def test_all_stages_sequential(store):
    batch, entity = "b9", "customers"
    for stage in STAGES:
        store.mark(batch, entity, stage)
    assert store.is_done(batch, entity, "LOADED")
