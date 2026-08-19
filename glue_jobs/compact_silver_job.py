"""Glue ETL job: compact small silver files into ~128 MB files.

Run for historical partitions (or after schema/ops changes) to undo small-file
damage: re-reads silver, coalesces to 128 MB targets, rewrites in place.

Args: --SILVER_BUCKET, --ENTITIES, --INGEST_DATES (JSON list, empty = all)
Idempotent: rewrites the same partitions via replaceWhere.
"""
from __future__ import annotations

import json
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from spark_utils import TARGET_FILE_MB, configure_aqe, write_sized

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SILVER_BUCKET", "ENTITIES", "INGEST_DATES"])
spark = SparkSession.builder.getOrCreate()
glue_context = GlueContext(spark.sparkContext)
logger = glue_context.get_logger()
configure_aqe(spark, TARGET_FILE_MB)

ENTITIES = json.loads(args["ENTITIES"])
DATES = json.loads(args["INGEST_DATES"]) if args["INGEST_DATES"] else None


def compact_entity(entity: str) -> None:
    base = f"s3://{args['SILVER_BUCKET']}/silver/{entity}/"
    df = spark.read.parquet(base)
    if DATES:
        predicate = " OR ".join([f"ingest_date = '{d}'" for d in DATES])
        df = df.filter(predicate)
        replace_where = predicate
    else:
        replace_where = None  # full-table rewrite

    before = df.rdd.getNumPartitions()
    files = write_sized(df, base, partition_cols=["ingest_date"],
                        target_mb=TARGET_FILE_MB, replace_where=replace_where)
    logger.info(f"[{entity}] compacted: input_partitions={before} "
                f"target_files={files} at ~{TARGET_FILE_MB} MB each")


job = Job(glue_context)
job.init(args["JOB_NAME"], args)
for _entity in ENTITIES:
    compact_entity(_entity)
job.commit()
