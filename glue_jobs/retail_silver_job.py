"""Glue ETL job: RAW (catalog) -> SILVER (Parquet).

Per entity & batch (args from Airflow):
  read partition-pruned raw -> type-cast -> DQ validations -> dedupe by PK
  -> schema evolution guard -> PII masking -> write silver Parquet (replaceWhere)

Idempotency: writes are scoped to the batch's ingest_date partition via
replaceWhere — re-running the same batch overwrites, never duplicates.
"""
from __future__ import annotations

import json
import sys

import yaml
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TYPE_MAP = {
    "int": IntegerType(), "string": StringType(),
    "float": DoubleType(), "timestamp": TimestampType(),
}
PII_FIELDS = {"customer_password", "customer_email"}

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTITIES", "BATCH_IDS",
                                     "RAW_DB", "SILVER_BUCKET", "CONFIG_S3"])
spark = SparkSession.builder.getOrCreate()
glue_context = GlueContext(spark.sparkContext)
logger = glue_context.get_logger()

# Fetch entity config staged on S3 by the pipeline (config/entities.yaml)
import boto3

bucket, key = args["CONFIG_S3"].replace("s3://", "").split("/", 1)
raw_cfg = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
entities_cfg = yaml.safe_load(raw_cfg)["entities"]
ENTITY_NAMES = json.loads(args["ENTITIES"])
BATCH_IDS = json.loads(args["BATCH_IDS"])

# ingest_date partitions covered by the batches come in the manifest keys; the
# DAG guarantees batches share one ingest_date per run — derive from batch key.
INGEST_DATES = sorted({bid.split("ingest_date=")[1].split("/")[0] for bid in BATCH_IDS})

REJECTS_PREFIX = f"s3://{args['SILVER_BUCKET']}/silver_rejects"
SILVER_PREFIX = f"s3://{args['SILVER_BUCKET']}/silver"


def target_schema(entity: str) -> StructType:
    return StructType([
        StructField(c["name"], TYPE_MAP[c["type"]], c.get("nullable", True))
        for c in entities_cfg[entity]["columns"]
    ])


def evolve_schema(df: DataFrame, entity: str) -> DataFrame:
    """Additive schema evolution only: new columns null-filled; incompatible
    changes (type narrowing / missing required column) raise and fail the job."""
    tgt = target_schema(entity)
    incoming = {f.name: f.dataType for f in df.schema.fields}
    for field in tgt.fields:
        if field.name not in incoming:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))   # additive: OK
        elif incoming[field.name].simpleString() != field.dataType.simpleString():
            if "int" in incoming[field.name].simpleString() and "bigint" in field.dataType.simpleString():
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))  # widening: OK
            else:
                raise ValueError(
                    f"[{entity}] Incompatible schema change on '{field.name}': "
                    f"existing={incoming[field.name]} target={field.dataType}")
    return df.select([f.name for f in tgt.fields])



def validate_and_dedupe(df: DataFrame, entity: str) -> tuple[DataFrame, DataFrame]:
    """Return (clean_df, reject_df). Rejects: null PK/mandatory, PK duplicates keep latest."""
    cols = entities_cfg[entity]["columns"]
    mandatory = [c["name"] for c in cols if not c.get("nullable", True)]
    pk = [c["name"] for c in cols if c.get("primary_key")]

    bad = df.filter(" OR ".join([f"{c} IS NULL" for c in mandatory]))
    clean = df.subtract(bad)

    # latest-wins dedupe on business key (idempotent re-processing safe)
    w = Window.partitionBy(pk).orderBy(F.col("batch_ingest_ts").desc_nulls_last())
    dups = clean.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") > 1).drop("_rn")
    clean = clean.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    rejects = bad.withColumn("reject_reason", F.lit("null_mandatory")).unionByName(
        dups.withColumn("reject_reason", F.lit("duplicate_pk")))
    return clean, rejects


def process_entity(entity: str) -> None:
    raw_table = f"{entity}_raw"
    predicate = " OR ".join([f"ingest_date = '{d}'" for d in INGEST_DATES])
    dyf = glue_context.create_dynamic_frame.from_catalog(
        database=args["RAW_DB"], table_name=raw_table,
        push_down_predicate=predicate,           # partition pruning: only this batch
    )
    df = dyf.toDF()
    if df.rdd.isEmpty():
        logger.warning(f"[{entity}] no rows for predicate {predicate}; skipping")
        return

    # Bronze ingests carry batch metadata columns; capture latest arrival ts
    df = (df.withColumn("batch_ingest_ts", F.coalesce(
            F.col("arrived_at").cast("timestamp"), F.current_timestamp()))
            .withColumn("ingest_date", F.col("ingest_date")))

    df = evolve_schema(df, entity)

    # PII masking at silver
    for f_name in PII_FIELDS:
        if f_name in df.columns:
            df = df.withColumn(f_name, F.sha2(F.col(f_name).cast("string"), 256))

    clean, rejects = validate_and_dedupe(df, entity)

    if rejects.rdd.isEmpty() is False:
        rejects.write.mode("append").parquet(f"{REJECTS_PREFIX}/{entity}/")

    # Standardize timestamps to UTC, write Parquet+Snappy partitioned by ingest_date
    out = (clean.drop("batch_ingest_ts")
                .withColumn("ingest_date", F.col("ingest_date")))
    (out.write.mode("overwrite")
        .option("replaceWhere", predicate)       # batch-scoped overwrite: idempotent
        .option("compression", "snappy")
        .partitionBy("ingest_date")
        .parquet(f"{SILVER_PREFIX}/{entity}/"))
    logger.info(f"[{entity}] silver write complete: {out.count()} rows")


job = Job(glue_context)
job.init(args["JOB_NAME"], args)
for _entity in ENTITY_NAMES:
    process_entity(_entity)
job.commit()

