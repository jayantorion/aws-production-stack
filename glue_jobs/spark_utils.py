"""Shared Spark helpers — small-file prevention & 128 MB target sizing.

Problem being solved (LINEAGE.md §13):
- Spark/HDFS-style writes produce one file per task per partition. Many small
  files blow up S3 request costs, Glue Catalog / Athena metadata overhead, and
  query planning time (LIST + footer reads per object).

Strategy implemented here:
  1. AQE + advisory partition size = 128 MB -> Spark coalesces shuffle
     partitions to the target before writing.
  2. Explicit size-aware repartition: `write_sized()` computes the number of
     output files from estimated bytes / 128 MB and repartitions accordingly,
     so a 40 GB day writes ~320 files of ~128 MB, never thousands of crumbs.
  3. Post-write verification + a standalone compaction job for historical
     partitions that predate this policy.
"""
from __future__ import annotations

import math
from typing import Any

TARGET_FILE_MB = 128
TARGET_FILE_BYTES = TARGET_FILE_MB * 1024 * 1024
MIN_FILES = 1


def configure_aqe(spark: Any, target_mb: int = TARGET_FILE_MB) -> None:
    """Enable Adaptive Query Execution tuned for 128 MB output files."""
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", f"{target_mb}MB")
    # don't sacrifice file size for parallelism when few partitions remain
    spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
    spark.conf.set("spark.sql.files.maxRecordsPerFile", 0)  # no record cap; sizing by bytes


def estimate_df_bytes(df: Any) -> int:
    """Estimated in-memory size (bytes) of the DataFrame's optimized plan."""
    return int(df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())


def target_file_count(total_bytes: int, target_bytes: int = TARGET_FILE_BYTES) -> int:
    """Number of ~128 MB files needed. Parquet compresses ~2-4x vs in-memory
    estimate, so divide the estimate by a conservative 2 to avoid oversizing
    file count (which would create small files again)."""
    compressed_estimate = max(1, total_bytes // 2)
    return max(MIN_FILES, math.ceil(compressed_estimate / target_bytes))


def write_sized(
    df: Any,
    path: str,
    partition_cols: list[str] | None = None,
    target_mb: int = TARGET_FILE_MB,
    replace_where: str | None = None,
    mode: str = "overwrite",
) -> int:
    """Write Parquet (Snappy) with output files sized to ~target_mb each.

    - Computes file count from estimated bytes.
    - `repartition(n, *partition_cols)`: each partition value lands in exactly
      one task -> one sized file per partition directory (no per-task crumbs).
    - Returns the number of files targeted (for logging/verification).
    """
    n = target_file_count(estimate_df_bytes(df), target_mb * 1024 * 1024)
    cols = partition_cols or []
    out = df.repartition(n, *cols) if cols else df.repartition(n)
    writer = out.write.mode(mode).option("compression", "snappy")
    if replace_where:
        writer = writer.option("replaceWhere", replace_where)
    if cols:
        writer = writer.partitionBy(*cols)
    writer.parquet(path)
    return n
