# Business Requirements — Retail Analytics Data Platform

> Document owner: Data Platform team · Version 1.0 · Status: Approved
> Related: [SLA](./SLA.md) · Architecture details: `LINEAGE.md` (local)

## 1. Background

**Acme Retail Group** operates 500+ physical stores, an e-commerce platform, and a
wholesale channel. Business decisions (pricing, inventory, marketing spend) are made
daily, but reporting today relies on fragmented spreadsheets refreshed manually by
analysts — slow, error-prone, and not scalable.

The Data Platform team will deliver a single, trusted, automated analytics platform
that lands every source system's data daily and serves consistent dashboards.

## 2. Stakeholders & Consumers

| Stakeholder | Role in this project | What they consume |
|---|---|---|
| VP Sales | Business sponsor | Daily sales performance dashboards |
| Finance Controller | Data consumer, sign-off on numbers | Revenue/reconciliation reports |
| Marketing | Campaign targeting | Customer analytics segments |
| Supply Chain | Inventory planning | Product/category movement |
| BI Team | Power BI report builders | Redshift `analytics` schema |
| Data Platform Team | Build & operate the pipeline | Airflow, CloudWatch, registries |
| Power BI Developers | Dashboard delivery | Gold marts only (never bronze/silver) |

## 3. Business Objectives

1. **BO-1** — One trusted daily view of sales across all channels by 04:00 UTC.
2. **BO-2** — Reduce manual reporting effort by 90% (eliminate spreadsheet refresh).
3. **BO-3** — Enable customer-level analytics (acquisition, repeat purchase) with PII safety.
4. **BO-4** — Support 2× data growth over 24 months without re-architecture.
5. **BO-5** — Full auditability: every published number traceable to a source batch.

## 4. Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1 | Ingest daily data from MySQL (OLTP: orders, customers, products, categories, departments) | Must |
| FR-2 | Ingest daily flat files via SFTP (partner product feeds) | Must |
| FR-3 | Ingest customer events via REST API (paginated) | Must |
| FR-4 | Land all sources immutably in S3 Bronze with batch audit records (manifests) | Must |
| FR-5 | Validate every landed batch before processing (checksum, row count, schema sniff) | Must |
| FR-6 | Publish cleaned, deduplicated, typed Parquet datasets (Silver) with business-date partitions | Must |
| FR-7 | Maintain a Glue Data Catalog of all raw and refined datasets | Must |
| FR-8 | Load Redshift gold marts: `analytics.orders`, `order_items`, `customers`, `products`, `categories`, `departments` — incremental, no duplicates | Must |
| FR-9 | Enforce data-quality gates; bad data must never reach Power BI | Must |
| FR-10 | Mask PII (customer email/password) before any consumer-facing layer | Must |
| FR-11 | Support backfill of any historical date range on demand | Should |
| FR-12 | Auto-handle schema additions from sources without pipeline downtime | Should |
| FR-13 | Retention: raw 180 days (Glacier after 30), silver 13 months | Should |

## 5. Non-Functional Requirements

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-1 | Data freshness — gold marts ready for BI | By **04:00 UTC** daily (see SLA) |
| NFR-2 | Daily volume capacity | 30–50 GB/day now; 100 GB/day headroom; 2× growth/24 months |
| NFR-3 | Pipeline reliability | ≥ 99% monthly run success rate |
| NFR-4 | Duplicate protection | Zero duplicate business keys in gold, regardless of retries/failures |
| NFR-5 | Recovery | RPO 24 h (reprocessable from source/bronze), RTO 4 h for pipeline outage |
| NFR-6 | Security | Encryption at rest/transit; least-privilege IAM; secrets in Secrets Manager |
| NFR-7 | Auditability | Batch-level lineage in DynamoDB `batch_registry`; row in/out per stage |
| NFR-8 | Observability | Alerts (SNS) on failure/SLA-miss/quarantine; CloudWatch SLA metrics |
| NFR-9 | Cost efficiency | Sized compute (no idle clusters), lifecycle policies, partition-pruned scans |

## 6. Data Volumes (current baseline)

| Entity | Rows (baseline) | Daily growth | Source |
|---|---|---|---|
| orders | 68,883 | ~+500/day | MySQL |
| order_items | 172,198 | ~+1,200/day | MySQL |
| customers | 12,435 | ~+30/day | MySQL |
| products | 1,345 | ~+10/day | MySQL |
| categories / departments | 58 / 6 | rare | MySQL |
| Partner feeds (SFTP) / events (REST) | scaled at 40–50 GB/day aggregate | growing | SFTP / REST |

## 7. Acceptance Criteria (per release)

1. A full DAG run completes and gold freshness gate passes before 04:00 UTC.
2. Row reconciliation: gold row counts match manifest counts (in = out, minus rejected).
3. Injected failure mid-run → rerun → zero duplicate business keys (verified by DQ).
4. Schema test: new column added at source → pipeline succeeds, column visible in silver.
5. PII scan: no raw email/password values in silver/gold (hash-only).
6. All CI checks (ruff + pytest) green.

## 8. Out of Scope (v1)

- Real-time (streaming) analytics — v2 via Kinesis
- Predictive ML models
- Self-service data prep for business users
