"""retail_medallion_pipeline — central orchestration DAG.

Task chain (LINEAGE.md §5):
  land_bronze_<entity>  ->  lambda_arrival_validation  ->  glue_crawler_ready
  ->  glue_silver_job  ->  glue_silver_done  ->  athena_dq_gate  ->  redshift_load

Idempotency: every run lands entity batches under fresh batch_ids; the Lambda
validator, Glue job (replaceWhere) and Redshift merge are all batch_id-scoped,
so retries/re-runs never create duplicates (LINEAGE.md §3).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonSensor

from utils.bronze import (
    apply_incremental_filter,
    generate_batch_id,
    land_to_bronze,
    read_source_csv,
)
from utils.config_loader import load_entities, load_settings

SETTINGS = load_settings()
ENTITIES = load_entities()
ENTITY_NAMES = list(ENTITIES.keys())

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,          # 5 -> 15 -> 45 min
    "max_retry_delay": timedelta(minutes=45),
}


# --------------------------------------------------------------------------- #
# Task callables
# --------------------------------------------------------------------------- #
def land_entity_task(entity: str, watermark: str | None, ceiling: str | None, **context) -> dict:
    """Extract (or simulate extract) -> land to bronze -> return manifest."""
    cfg = ENTITIES[entity]
    batch_id = generate_batch_id()
    df = read_source_csv(entity, cfg, SETTINGS["sources"]["local_base_path"])
    df = apply_incremental_filter(df, cfg, watermark, ceiling)
    s3 = boto3.client("s3", region_name=SETTINGS["region"])
    return land_to_bronze(
        df, entity, cfg, batch_id, SETTINGS["s3"]["raw_bucket"], s3,
        watermark=watermark, ceiling=ceiling,
    )


def trigger_arrival_validation(**context) -> list[str]:
    """Invoke the Lambda validator for each landed batch (belt-and-braces; the
    S3 event notification is the primary trigger — Lambda short-circuits on
    already-processed batch_ids, so double invocation is safe)."""
    ti = context["ti"]
    manifests = [ti.xcom_pull(task_ids=f"land_bronze_{e}") for e in ENTITY_NAMES]
    batch_ids = [m["batch_id"] for m in manifests]
    lam = boto3.client("lambda", region_name=SETTINGS["region"])
    lam.invoke(
        FunctionName=SETTINGS["lambda"]["arrival_validator"],
        InvocationType="Event",
        Payload=json.dumps({"batches": manifests}),
    )
    return batch_ids



def _crawler_ready() -> bool:
    glue = boto3.client("glue", region_name=SETTINGS["region"])
    resp = glue.get_crawler(Name=SETTINGS["glue"]["crawler_name"])
    return resp["Crawler"]["State"] == "READY" and (
        resp["Crawler"].get("LastCrawl", {}).get("Status") in (None, "SUCCEEDED")
    )


def start_glue_silver_job(**context) -> str:
    """Submit the Glue silver ETL job run for all landed batches."""
    ti = context["ti"]
    batch_ids = ti.xcom_pull(task_ids="lambda_arrival_validation")
    glue = boto3.client("glue", region_name=SETTINGS["region"])
    resp = glue.start_job_run(
        JobName=SETTINGS["glue"]["job_name"],
        Arguments={
            "--ENTITIES": json.dumps(ENTITY_NAMES),
            "--BATCH_IDS": json.dumps(batch_ids),
            "--RAW_DB": SETTINGS["glue"]["database_raw"],
            "--SILVER_BUCKET": SETTINGS["s3"]["silver_bucket"],
            "--CONFIG_S3": f"s3://{SETTINGS['s3']['raw_bucket']}/{SETTINGS['s3']['config_prefix']}/entities.yaml",
        },
    )
    return resp["JobRunId"]


def _glue_job_done(**context) -> bool:
    ti = context["ti"]
    job_run_id = ti.xcom_pull(task_ids="glue_silver_job")
    glue = boto3.client("glue", region_name=SETTINGS["region"])
    resp = glue.get_job_run(JobName=SETTINGS["glue"]["job_name"], RunId=job_run_id)
    state = resp["JobRun"]["JobRunState"]
    if state in ("FAILED", "ERROR", "STOPPED", "TIMEOUT"):
        raise RuntimeError(f"Glue job {job_run_id} ended in {state}")
    return state == "SUCCEEDED"


def athena_dq_gate(**context) -> str:
    """Run Athena DQ checks against silver; hard-stop the pipeline on FAIL."""
    from utils.athena_dq import run_dq_suite  # uses sql/athena_dq_checks.sql logic

    return run_dq_suite(SETTINGS, batch_ids=context["ti"].xcom_pull(
        task_ids="lambda_arrival_validation"))


def redshift_load(**context) -> list[str]:
    """COPY silver -> staging -> transactional merge for each entity batch."""
    from utils.redshift_loader import load_all_entities

    return load_all_entities(
        SETTINGS,
        batch_ids=context["ti"].xcom_pull(task_ids="lambda_arrival_validation"),
    )


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #
with DAG(
    dag_id="retail_medallion_pipeline",
    description="Retail DB: bronze landing -> validation -> silver -> DQ -> redshift",
    schedule=SETTINGS["pipeline"]["schedule_cron"],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                # prevents out-of-order batch overwrites
    default_args=DEFAULT_ARGS,
    tags=["retail", "medallion", "production"],
) as dag:
    land_tasks = [
        PythonOperator(
            task_id=f"land_bronze_{entity}",
            python_callable=land_entity_task,
            op_kwargs={"entity": entity,
                       "watermark": None,   # pulled from control table in prod (LINEAGE §4)
                       "ceiling": None},
        )
        for entity in ENTITY_NAMES
    ]

    validate = PythonOperator(task_id="lambda_arrival_validation",
                              python_callable=trigger_arrival_validation)

    crawl_ready = PythonSensor(task_id="glue_crawler_ready", poke_callable=_crawler_ready,
                               mode="reschedule", timeout=60 * 60, poke_interval=60)

    glue_start = PythonOperator(task_id="glue_silver_job", python_callable=start_glue_silver_job)
    glue_done = PythonSensor(task_id="glue_silver_done", poke_callable=_glue_job_done,
                             mode="reschedule", timeout=60 * 60 * 3, poke_interval=120)

    dq_gate = PythonOperator(task_id="athena_dq_gate", python_callable=athena_dq_gate)
    rs_load = PythonOperator(task_id="redshift_load", python_callable=redshift_load)

    land_tasks >> validate >> crawl_ready >> glue_start >> glue_done >> dq_gate >> rs_load

