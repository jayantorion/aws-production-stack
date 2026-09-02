# Service Level Agreement (SLA) — Retail Analytics Data Platform

> Version 1.0 · Effective: 2026-08-31 · Provider: Data Platform Team · Consumers: BI Team / Power BI developers / business stakeholders
> Enforcement: `config/sla.yaml` → Airflow task SLAs, SLA-miss alerts, freshness gate (`dags/utils/sla_monitor.py`)

## 1. Scope

Covers the daily batch pipeline `retail_medallion_pipeline` (ingestion → bronze →
validation → silver → DQ → gold) and availability of the `analytics` schema in
Redshift for Power BI.

## 2. Data Freshness SLA (the headline commitment)

| SLA ID | Commitment | Target | Measurement |
|--------|-----------|--------|-------------|
| SLA-F1 | Gold marts (`analytics.*`) refreshed and ready for Power BI | **by 04:00 UTC every day** | `gold_freshness_sla_gate` task + CloudWatch metric `GoldFreshnessLagMinutes` |
| SLA-F2 | Event-to-gold data lag (order placed → visible in BI) | ≤ 26 hours (daily batch) | manifest arrival ts vs. gold load ts |
| SLA-F3 | Silver datasets published | by 03:15 UTC | Glue job completion timestamp |
| SLA-F4 | Bronze landing complete for all 6 entities | by 02:40 UTC | `land_bronze_*` task finish |

## 3. Pipeline Runtime Budgets (per stage, measured per DAG run)

| Stage | Budget (from run start) | Enforced via |
|-------|------------------------|--------------|
| `land_bronze_<entity>` (all 6) | 30 min | Airflow task `sla` |
| `lambda_arrival_validation` | 10 min | Airflow task `sla` |
| `glue_crawler_ready` (sensor) | 45 min | sensor `timeout` |
| `glue_silver_job` + completion | 90 min | Airflow task `sla` |
| `athena_dq_gate` | 15 min | Airflow task `sla` |
| `redshift_load` | 30 min | Airflow task `sla` |
| **Total DAG run** (`dagrun_timeout`) | **150 min** | Airflow `dagrun_timeout` |

## 4. Reliability & Quality SLAs

| SLA ID | Commitment | Target |
|--------|-----------|--------|
| SLA-R1 | Monthly successful DAG-run rate | ≥ 99% |
| SLA-R2 | Batch-level data completeness (manifest rows = gold rows − rejected) | 100% reconciled |
| SLA-R3 | Duplicate business keys in gold | 0 (hard guarantee) |
| SLA-R4 | DQ-gate false-pass rate | 0 (every check evaluated per run) |
| SLA-R5 | Quarantined batches triaged | within 1 business day |

## 5. Recovery Objectives

| Objective | Target | Mechanism |
|-----------|--------|-----------|
| RPO — max data loss on failure | 24 h (the current day's window) | reprocess from source or immutable bronze |
| RTO — pipeline back in service after P1 outage | 4 h | idempotent rerun from failed task (no manual cleanup) |
| Backfill turnaround (any single day) | 2 h on demand | parameterized DAG replay |

## 6. Support Model & Severity Matrix

| Severity | Definition | Response | Resolution target | Example |
|----------|-----------|----------|-------------------|---------|
| **P1** | Gold marts not refreshed by 04:00 UTC / pipeline down | 15 min (24×7) | 4 h | Glue job fails, no silver by 03:15 |
| **P2** | SLA at risk — stage running past budget, DQ warn-threshold breached | 1 h (business hours) | same day | crawler slow, row-count delta > 20% |
| **P3** | Non-blocking defect — quarantine events, minor data issue | 4 business h | 2 business days | single partner file checksum mismatch |
| **P4** | Cosmetic / question | 1 business day | best effort | documentation query |

Alert routing: P1/P2 → SNS `dep-alerts` (email + Slack webhook) 24×7 on-call;
P3/P4 → Jira queue. All SLA misses also emit CloudWatch metric `DEP/SLA SlasMissed`.

## 7. Measurement, Reporting & Exclusions

- **Where measured:** Airflow task durations & SLA misses (authoritative), CloudWatch
  custom metrics (`DEP/SLA` namespace: `GoldFreshnessLagMinutes`, `SlasMissed`,
  `StageOverrunMinutes`), DynamoDB `batch_registry` (batch-level audit).
- **Reporting:** monthly SLA report to stakeholders — freshness attainment, success
  rate, incidents by severity, trend.
- **Exclusions (not counted as SLA breach):**
  1. Source systems publish late (documented by source-system team) — sensors wait, clock shifts with alert, not a breach if downstream stages meet budgets.
  2. AWS region-level outages (vendor incident).
  3. Force majeure; changes frozen during declared change-freeze windows.
  4. Runs explicitly triggered as backfills/manual (still reported, not SLA-counted).
- **Review:** SLA reviewed quarterly with stakeholders; targets may tighten as the
  platform matures.
