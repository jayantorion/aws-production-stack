# 📊 Data Engineering Platform

Production-grade, cloud-native data engineering platform on **AWS** with **Apache Airflow** as the central orchestrator. Built to process **30–50 GB/day** with incremental loading, idempotent (retry-safe) processing, and full data lineage.

## 🏗️ Architecture (Medallion on AWS)

```
MySQL / SFTP / REST API
        │  (Airflow: chunked, watermarked extraction)
        ▼
S3 BRONZE (raw, immutable, batch_id partitioned)
        │  S3 Event Notification
        ▼
Lambda (arrival validation) → Glue Crawler → Glue Data Catalog
        │  (crawler success flag)
        ▼
Glue ETL (PySpark): DQ validation • dedupe • schema evolution
        ▼
S3 SILVER (Parquet, Snappy, partitioned) → Athena DQ gate
        │  (DQ pass)
        ▼
Redshift: COPY → staging → transactional merge → optimized marts
        ▼
Power BI (gold consumption layer)
```

## 📖 Documentation

- **[LINEAGE.md](./LINEAGE.md)** — the single source of truth: full flowchart, stage-by-stage data flow, idempotency design, incremental loading strategy, failure recovery, capacity plan, security, and the dataset lineage tracker. **Keep it updated.**

## 🚦 Key Guarantees

| Concern | How it's solved |
|---|---|
| Duplicate data on failure/retry | `batch_id`-scoped writes, `replaceWhere` partition overwrites, transactional `DELETE+INSERT` in Redshift |
| Incremental loading | Watermark control table per source (`updated_at` / file ledger / API cursor) |
| Failure recovery | Airflow retries with exponential backoff, quarantine bucket, control-table audit, replay-safe backfills |
| Schema drift | Additive evolution allowed; breaking changes blocked + alerted |
| Scale (30–50 GB/day) | Partition pruning, 128–512 MB files, Glue auto-scaling, Redshift dist/sort keys + WLM |

## 📁 Repository Structure

```
data-engineering-platform/
├── LINEAGE.md        # architecture + lineage tracker (always current)
├── dags/             # Airflow DAGs
├── glue_jobs/        # PySpark ETL scripts
├── lambda/           # arrival-validation Lambda
├── sql/              # DQ checks, Redshift DDL, COPY/merge scripts
├── config/           # environment configs, table definitions
├── tests/            # unit + data quality tests
└── .github/workflows # CI (lint, DAG validation)
```
