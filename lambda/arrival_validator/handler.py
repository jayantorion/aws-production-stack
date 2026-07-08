"""Lambda: dep-arrival-validator — event-driven basic validation gate.

Triggered by:
  1. S3 Event Notification on raw/*/_MANIFEST.json (primary), or
  2. Direct Airflow invoke with {"batches": [manifest, ...]} (belt-and-braces).

Responsibilities (LINEAGE.md Stage 2):
  - verify each manifest file exists, size+checksum match, row_count > 0
  - PASS  -> record batch in DynamoDB batch_registry, start Glue Crawler
  - FAIL  -> move batch to quarantine bucket, registry=QUARANTINED, SNS alert

Idempotency: registry lookup on batch_id short-circuits duplicate invocations.
"""
from __future__ import annotations

import hashlib
import json
import os

import boto3

S3 = boto3.client("s3")
GLUE = boto3.client("glue")
DDB = boto3.resource("dynamodb")
SNS = boto3.client("sns")

REGISTRY = DDB.Table(os.environ.get("BATCH_REGISTRY_TABLE", "dep_batch_registry"))
CRAWLER = os.environ.get("CRAWLER_NAME", "dep-raw-crawler")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC_ARN", "")
QUARANTINE_BUCKET = os.environ.get("QUARANTINE_BUCKET", "")


def _already_processed(batch_id: str) -> bool:
    resp = REGISTRY.get_item(Key={"batch_id": batch_id})
    item = resp.get("Item")
    return bool(item and item.get("status") in ("VALIDATED", "QUARANTINED", "COMPLETED"))


def _register(batch_id: str, entity: str, status: str, detail: dict | None = None) -> None:
    REGISTRY.put_item(Item={
        "batch_id": batch_id,
        "entity": entity,
        "status": status,
        "detail": json.dumps(detail or {}),
        "updated_at": __import__("datetime").datetime.utcnow().isoformat(),
    })


def _validate_manifest(manifest: dict) -> tuple[bool, str]:
    bucket = manifest["s3_prefix"].replace("s3://", "").split("/")[0]
    if manifest.get("row_count", 0) <= 0:
        return False, "row_count is zero"
    for f in manifest.get("files", []):
        try:
            obj = S3.get_object(Bucket=bucket, Key=f["key"])
        except S3.exceptions.NoSuchKey:
            return False, f"missing file {f['key']}"
        body = obj["Body"].read()
        if len(body) != f.get("bytes"):
            return False, f"size mismatch on {f['key']}"
        if hashlib.md5(body).hexdigest() != f.get("checksum"):
            return False, f"checksum mismatch on {f['key']}"
    return True, "ok"


def _quarantine(manifest: dict, reason: str) -> None:
    src_bucket = manifest["s3_prefix"].replace("s3://", "").split("/")[0]
    prefix = manifest["s3_prefix"].replace(f"s3://{src_bucket}/", "")
    resp = S3.list_objects_v2(Bucket=src_bucket, Prefix=prefix)
    for obj in resp.get("Contents", []):
        S3.copy_object(
            Bucket=QUARANTINE_BUCKET,
            Key=f"quarantine/{manifest['entity']}/{manifest['batch_id']}/{obj['Key'].split('/')[-1]}",
            CopySource={"Bucket": src_bucket, "Key": obj["Key"]},
        )
        S3.delete_object(Bucket=src_bucket, Key=obj["Key"])
    if ALERTS_TOPIC:
        SNS.publish(TopicArn=ALERTS_TOPIC, Subject=f"[DEP] batch quarantined: {manifest['entity']}",
                    Message=json.dumps({"batch_id": manifest["batch_id"], "reason": reason}))


def handler(event: dict, context) -> dict:
    batches = event.get("batches")
    if batches is None:  # S3 event path: fetch the manifest object
        batches = []
        for rec in event.get("Records", []):
            key = rec["s3"]["object"]["key"]
            if key.endswith("_MANIFEST.json"):
                bucket = rec["s3"]["bucket"]["name"]
                batches.append(json.loads(S3.get_object(Bucket=bucket, Key=key)["Body"].read()))

    results = []
    for manifest in batches:
        batch_id, entity = manifest["batch_id"], manifest["entity"]
        if _already_processed(batch_id):
            results.append({"batch_id": batch_id, "skipped": True})
            continue
        ok, reason = _validate_manifest(manifest)
        if ok:
            _register(batch_id, entity, "VALIDATED", {"row_count": manifest["row_count"]})
            GLUE.start_crawler(Name=CRAWLER)
        else:
            _register(batch_id, entity, "QUARANTINED", {"reason": reason})
            if QUARANTINE_BUCKET:
                _quarantine(manifest, reason)
        results.append({"batch_id": batch_id, "entity": entity, "ok": ok, "reason": reason})
    return {"results": results}
