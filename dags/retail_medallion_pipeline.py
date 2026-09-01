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
from utils.capacity import plan_from_manifests
from utils.config_loader import load_entities, load_settings
from utils.sla_monitor import (
    SLA,
    check_gold_freshness,
    sla_miss_callback,
    stage_sla_minutes,
)
from utils.state_store import StateStore

SETTINGS = load_settings()
ENTITIES = load_entities()
ENTITY_NAMES = list(ENTITIES.keys())
STATE = StateStore(region=SETTINGS["region"])

# SLA enforcement (docs/SLA.md, config/sla.yaml): per-task budgets + total budget
DAGRUN_TIMEOUT = timedelta(minutes=SLA["pipeline"]["dagrun_timeout_minutes"])

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
    """Extract -> land to bronze -> checkpoint LANDED.

    Resume-safe: if a previous run of this entity failed mid-pipeline, its open
    batch is REUSED (same batch_id -> same S3 prefix overwritten, never
    appended), so a rerun after a 60%-processed failure creates no duplicates.
    """
    cfg = ENTITIES[entity]
    open_batch = STATE.open_batch(entity)
    batch_id = open_batch["batch_id"] if open_batch else generate_batch_id()
    df = read_source_csv(entity, cfg, SETTINGS["sources"]["local_base_path"])
    df = apply_incremental_filter(df, cfg, watermark, ceiling)
    s3 = boto3.client("s3", region_name=SETTINGS["region"])
    manifest = land_to_bronze(
        df, entity, cfg, batch_id, SETTINGS["s3"]["raw_bucket"], s3,
        watermark=watermark, ceiling=ceiling,
    )
    STATE.mark(batch_id, entity, "LANDED", {"row_count": manifest["row_count"]})
    return manifest


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
    for m in manifests:
        STATE.mark(m["batch_id"], m["entity"], "VALIDATED",
                   {"row_count": m["row_count"]})
    return batch_ids



def _crawler_ready() -> bool:
    glue = boto3.client("glue", region_name=SETTINGS["region"])
    resp = glue.get_crawler(Name=SETTINGS["glue"]["crawler_name"])
    return resp["Crawler"]["State"] == "READY" and (
        resp["Crawler"].get("LastCrawl", {}).get("Status") in (None, "SUCCEEDED")
    )


def start_glue_silver_job(**context) -> str:
    """Submit the Glue silver ETL run, RIGHT-SIZED to the actual batch volume.

    Capacity is planned from the real landed bytes (manifests) — a day where
    the source doubles from 40 GB to 80 GB automatically gets double the
    workers, keeping runtime inside the same SLA (utils/capacity.py tiers).
    """
    ti = context["ti"]
    manifests = [ti.xcom_pull(task_ids=f"land_bronze_{e}") for e in ENTITY_NAMES]
    batch_ids = [m["batch_id"] for m in manifests]
    plan = plan_from_manifests(manifests)
    glue = boto3.client("glue", region_name=SETTINGS["region"])
    resp = glue.start_job_run(
        JobName=SETTINGS["glue"]["job_name"],
        WorkerType=plan["worker_type"],                 # dynamic right-sizing
        NumberOfWorkers=plan["number_of_workers"],
        Arguments={
            "--ENTITIES": json.dumps(ENTITY_NAMES),
            "--BATCH_IDS": json.dumps(batch_ids),
            "--RAW_DB": SETTINGS["glue"]["database_raw"],
            "--SILVER_BUCKET": SETTINGS["s3"]["silver_bucket"],
            "--CONFIG_S3": f"s3://{SETTINGS['s3']['raw_bucket']}/{SETTINGS['s3']['config_prefix']}/entities.yaml",
            "--INPUT_GB": str(plan["input_gb"]),
        },
    )
    print(f"Glue capacity plan: {plan}")
    return resp["JobRunId"]


def _glue_job_done(**context) -> bool:
    ti = context["ti"]
    job_run_id = ti.xcom_pull(task_ids="glue_silver_job")
    glue = boto3.client("glue", region_name=SETTINGS["region"])
    resp = glue.get_job_run(JobName=SETTINGS["glue"]["job_name"], RunId=job_run_id)
    state = resp["JobRun"]["JobRunState"]
    if state in ("FAILED", "ERROR", "STOPPED", "TIMEOUT"):
        raise RuntimeError(f"Glue job {job_run_id} ended in {state}")
    if state == "SUCCEEDED":
        manifests = [ti.xcom_pull(task_ids=f"land_bronze_{e}") for e in ENTITY_NAMES]
        for m in manifests:                      # checkpoint: TRANSFORMED
            STATE.mark(m["batch_id"], m["entity"], "TRANSFORMED")
        return True
    return False


def athena_dq_gate(**context) -> str:
    """Run Athena DQ checks against silver; hard-stop the pipeline on FAIL."""
    from utils.athena_dq import run_dq_suite  # uses sql/athena_dq_checks.sql logic

    result = run_dq_suite(SETTINGS, batch_ids=context["ti"].xcom_pull(
        task_ids="lambda_arrival_validation"))
    manifests = [context["ti"].xcom_pull(task_ids=f"land_bronze_{e}") for e in ENTITY_NAMES]
    for m in manifests:                              # checkpoint: DQ_PASSED
        STATE.mark(m["batch_id"], m["entity"], "DQ_PASSED")
    return result


def redshift_load(**context) -> list[str]:
    """COPY silver -> staging -> transactional merge for each entity batch."""
    from utils.redshift_loader import load_all_entities

    results = load_all_entities(
        SETTINGS,
        batch_ids=context["ti"].xcom_pull(task_ids="lambda_arrival_validation"),
    )
    manifests = [context["ti"].xcom_pull(task_ids=f"land_bronze_{e}") for e in ENTITY_NAMES]
    for m in manifests:                              # checkpoint: LOADED (batch closed)
        STATE.mark(m["batch_id"], m["entity"], "LOADED")
    return results


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #
def gold_freshness_gate(**context) -> dict:
    """SLA-F1 (docs/SLA.md): gold must be ready by 04:00 UTC the day after the
    data date. Emits CloudWatch `GoldFreshnessLagMinutes`; breach = P1 incident."""
    from datetime import datetime, timezone

    now_utc = datetime.now(timezone.utc)
    data_date = context["logical_date"].date()
    return check_gold_freshness(
        SETTINGS, now_utc, data_date, region=SETTINGS["region"])


with DAG(
    dag_id="retail_medallion_pipeline",
    description="Retail DB: bronze landing -> validation -> silver -> DQ -> redshift",
    schedule=SETTINGS["pipeline"]["schedule_cron"],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                # prevents out-of-order batch overwrites
    dagrun_timeout=DAGRUN_TIMEOUT,    # SLA: total run budget (150 min)
    sla_miss_callback=sla_miss_callback,   # SLA-miss -> P2 SNS alert + CW metric
    default_args=DEFAULT_ARGS,
    tags=["retail", "medallion", "production", "sla"],
) as dag:
    land_tasks = [
        PythonOperator(
            task_id=f"land_bronze_{entity}",
            python_callable=land_entity_task,
            sla=timedelta(minutes=stage_sla_minutes("land_bronze")),
            op_kwargs={"entity": entity,
                       "watermark": None,   # pulled from control table in prod (LINEAGE §4)
                       "ceiling": None},
        )
        for entity in ENTITY_NAMES
    ]

    validate = PythonOperator(task_id="lambda_arrival_validation",
                              sla=timedelta(minutes=stage_sla_minutes("lambda_arrival_validation")),
                              python_callable=trigger_arrival_validation)

    crawl_ready = PythonSensor(task_id="glue_crawler_ready", poke_callable=_crawler_ready,
                               mode="reschedule",
                               timeout=stage_sla_minutes("glue_crawler_ready") * 60,
                               poke_interval=60)

    glue_start = PythonOperator(task_id="glue_silver_job",
                                sla=timedelta(minutes=stage_sla_minutes("glue_silver_job")),
                                python_callable=start_glue_silver_job)
    glue_done = PythonSensor(task_id="glue_silver_done", poke_callable=_glue_job_done,
                             mode="reschedule", timeout=60 * 60 * 3, poke_interval=120)

    dq_gate = PythonOperator(task_id="athena_dq_gate",
                             sla=timedelta(minutes=stage_sla_minutes("athena_dq_gate")),
                             python_callable=athena_dq_gate)
    rs_load = PythonOperator(task_id="redshift_load",
                             sla=timedelta(minutes=stage_sla_minutes("redshift_load")),
                             python_callable=redshift_load)
    fresh_gate = PythonOperator(task_id="gold_freshness_sla_gate",   # SLA-F1 gate
                                python_callable=gold_freshness_gate)

    land_tasks >> validate >> crawl_ready >> glue_start >> glue_done >> dq_gate >> rs_load >> fresh_gate

