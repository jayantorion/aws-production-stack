# 📊 Data Engineering Platform (`aws-production-stack`)

Production-grade, cloud-native data engineering platform on **AWS** with **Apache Airflow** as the central orchestrator. Processes **30–50 GB/day** (gracefully scales to 80 GB+ as source volume grows), loads **incrementally**, is **idempotent** (re-runs never duplicate data — even after a mid-pipeline failure at 60%), and tracks **full data lineage**.

**Key capabilities**
- 🥉🥈🥇 Medallion architecture: S3 Bronze → S3 Silver (Parquet) → Redshift Gold
- ⚡ Event-driven validation (S3 → Lambda → Glue Crawler → Data Catalog)
- 📈 Auto-scaling compute: Glue workers are right-sized from actual batch volume every run
- ♻️ Checkpointed resumability: failed runs resume from the last completed stage with zero duplicates
- 🔍 Data quality gates: arrival validation (Lambda) + Athena SQL checks before warehouse load
- 🔐 PII masking, Secrets Manager, least-privilege IAM, SSE encryption

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

- **`LINEAGE.md`** — internal project source of truth (full flowchart, stage-by-stage data flow, idempotency design, incremental loading strategy, failure recovery, capacity plan, security, dataset lineage tracker). **Local-only by design — kept out of the repo via `.gitignore`; maintain it in your working copy.**

## 🚦 Key Guarantees

| Concern | How it's solved |
|---|---|
| Duplicate data on failure/retry | `batch_id`-scoped writes, `replaceWhere` partition overwrites, transactional `DELETE+INSERT` in Redshift |
| Mid-pipeline failure (e.g. 60% done) | DynamoDB checkpoint state machine (`dags/utils/state_store.py`): rerun **reuses the open batch** (same batch_id → overwrite, never append) and skips completed stages |
| Data growth (40 GB → 80 GB → daily growth) | `dags/utils/capacity.py` sizes Glue workers from real manifest bytes each run; partition-pruned reads keep cost O(new data); Redshift incremental COPY + RA3 resize |
| Incremental loading | Watermark control per source (`updated_at` / file ledger / API cursor) + 1-day lookback for late arrivals |
| Schema drift | Additive evolution allowed; breaking changes blocked + alerted (`glue_jobs/retail_silver_job.py`) |
| Bad data | Quarantine bucket + rejects path + Athena DQ hard-stop before Redshift |
| Scale (30–50 GB/day) | Partition pruning, 128–512 MB files, Glue auto-scaling, Redshift dist/sort keys + WLM |

## 🗃️ Sample Data (in-repo)

Development source-of-record: the classic **retail_db** dataset, committed under
**`sample_data/retail_db/`** — 6 entities: `departments` (6), `categories` (58),
`customers` (12,435), `products` (1,345), `orders` (68,883, incremental on `order_date`),
`order_items` (172,198, incremental via orders window). Format: headerless delimited text;
column order defined per entity in `config/entities.yaml`. See
[`sample_data/retail_db/README.md`](./sample_data/retail_db/README.md).
The default source path in `config/settings.yaml` points at `sample_data/retail_db`
(relative to repo root); in production switch the same entity configs to MySQL/SFTP/REST
— no code change required. To simulate a 40–80 GB day, multiply the sample files — the
capacity planner scales Glue workers automatically.

## 📁 Repository Structure — file-by-file reference

```
data-engineering-platform/
├── LINEAGE.md                        # ⭐ project's internal source of truth (architecture, data flow,
│                                     #   idempotency, capacity, lineage table, change log) — LOCAL ONLY,
│                                     #   kept out of GitHub via .gitignore
├── README.md                         # this guide
│
├── config/
│   ├── entities.yaml                 # per-entity contract: columns, types, PK, nullable,
│   │                                 #   load_type (full_snapshot|incremental), watermark column,
│   │                                 #   source type/location. Consumed by DAG, Glue, DQ, Redshift.
│   └── settings.yaml                 # environment config: buckets, region, Glue/Redshift/Lambda
│                                     #   names, schedule cron, source paths. Env vars override
│                                     #   (SETTINGS_S3__RAW_BUCKET=... pattern).
│
├── dags/
│   ├── retail_medallion_pipeline.py  # ⭐ central Airflow DAG: land → validate → crawl →
│   │                                 #   glue ETL → athena DQ → redshift load, with checkpointing
│   │                                 #   and dynamic capacity planning at every stage
│   └── utils/
│       ├── config_loader.py          # YAML loader + SETTINGS_* env-var overrides
│       ├── bronze.py                 # ⭐ idempotency core: batch_id, S3 raw landing layout,
│       │                             #   per-file checksums, _MANIFEST.json, incremental filter
│       ├── state_store.py            # ⭐ DynamoDB checkpoint state machine (LANDED→…→LOADED);
│       │                             #   open-batch reuse makes reruns duplicate-free
│       ├── capacity.py               # ⭐ dynamic Glue worker planning from real batch bytes
│       ├── athena_dq.py              # Athena DQ suite runner (hard-stop on FAIL)
│       └── redshift_loader.py        # COPY → staging → transactional merge via Redshift Data API
│
├── glue_jobs/
│   └── retail_silver_job.py          # PySpark ETL: partition-pruned read, schema-evolution guard,
│                                     #   null/dup validation + rejects, PII sha2 masking,
│                                     #   Parquet+Snappy write with replaceWhere (idempotent)
│
├── lambda/arrival_validator/
│   ├── handler.py                    # event gate: checksum/rowcount verify → registry → crawler
│   │                                 #   start, or quarantine + SNS alert
│   └── requirements.txt              # boto3
│
├── sql/
│   ├── redshift_ddl.sql              # staging + analytics DDL (DISTKEY/SORTKEY optimization)
│   ├── redshift_load_template.sql    # idempotent batch load template (COPY/DELETE/INSERT/ANALYZE)
│   └── athena_dq_checks.sql          # DQ check definitions (nulls, dups, orphans, freshness, delta)
│
├── infra/
│   └── cloudformation.yaml           # IaC: buckets, DynamoDB registry, SNS, IAM roles,
│                                     #   Glue DB/crawler/job, Lambda
├── scripts/
│   ├── deploy.sh                     # end-to-end AWS deployment (package → deploy → wire events)
│   └── smoke_bronze.py               # local end-to-end smoke test of bronze landing
│
├── sample_data/retail_db/            # ⭐ sample dataset (6 entities, in-repo)
├── tests/                            # pytest: bronze idempotency, state store resume, capacity tiers
├── requirements.txt                  # runtime deps (pandas, pyyaml, boto3, apache-airflow)
├── requirements-dev.txt              # pytest, ruff
└── .github/workflows/ci.yml          # CI: ruff lint + pytest on every push/PR
```

## 📦 Where every file lives (local → AWS mapping)

| Local file/folder | AWS service it becomes | Exact location / how it gets there |
|---|---|---|
| `dags/retail_medallion_pipeline.py` + `dags/utils/*` | **Airflow / MWAA** | `$AIRFLOW_HOME/dags/` (local) or MWAA DAG S3 folder (synchronized automatically) |
| `config/entities.yaml` | **S3** (raw bucket) + read by Glue & Lambda | `s3://<raw-bucket>/config/entities.yaml` — uploaded by `scripts/deploy.sh` |
| `config/settings.yaml` | Environment config for Airflow tasks | Stays with the DAG; per-env overrides via `SETTINGS_*` env vars in MWAA/Airflow |
| `glue_jobs/retail_silver_job.py` + `glue_jobs/spark_utils.py` | **Glue ETL job** (Spark) | `s3://<deploy-bucket>/glue/` — set as the job's ScriptLocation (`scripts/deploy.sh`) |
| `glue_jobs/compact_silver_job.py` | **Glue ETL job** (Spark) | `s3://<deploy-bucket>/glue/` — registered as `dep-silver-compact-job` |
| `lambda/arrival_validator/handler.py` | **Lambda** | zipped (`scripts/deploy.sh`) → function `dep-arrival-validator`; S3 event on `*_MANIFEST.json` triggers it |
| `sql/redshift_ddl.sql` | **Redshift** | Run once via Redshift Query Editor / `psql -f` |
| `sql/redshift_load_template.sql` | Logic embedded in `dags/utils/redshift_loader.py` | Executed per batch via the **Redshift Data API** at runtime |
| `sql/athena_dq_checks.sql` | Logic embedded in `dags/utils/athena_dq.py` | Executed per run via the **Athena** engine at runtime |
| `infra/cloudformation.yaml` | **CloudFormation** stack `dep-core` | `aws cloudformation deploy` (Step 3 of the guide) |
| `sample_data/retail_db/` | Dev source-of-record | Local files read by the DAG; replaced by MySQL/SFTP/REST in prod |
| `scripts/deploy.sh` | Runs from laptop/CI | Orchestrates packaging + CFN + config upload + S3 event wiring |

**Spark data flow reminder (inside Glue):** raw S3 → `create_dynamic_frame.from_catalog` (partition-pruned) → `toDF()` → PySpark transforms (cast, DQ, dedupe, evolution) → `write_sized()` → silver S3 (Parquet, ~128 MB files) → COPYed to Redshift.

## 🐞 Debugging guide — error → where to look → fix

| Symptom | Where to look | Likely cause → Fix |
|---|---|---|
| DAG fails to parse / import error | Airflow UI → DAGs → red import error; scheduler logs | Syntax/bad import → fix locally (`ruff check`, `pytest`); verify `dags/utils` packaged with DAG |
| `land_bronze_*` fails: `FileNotFoundError` | Airflow task log | `sources.local_base_path` wrong → fix `settings.yaml` or set `SETTINGS_SOURCES__LOCAL_BASE_PATH` |
| Lambda never fires | CloudWatch → log group `/aws/lambda/dep-arrival-validator`; S3 → Properties → Event notifications | Notification not wired → rerun Step 4; suffix filter missing `_MANIFEST.json` |
| Batches quarantined | `s3://<quarantine-bucket>/`, SNS email, DynamoDB `batch_registry` → `detail.reason` | checksum/rowcount mismatch → verify source file integrity; re-run after fixing upstream |
| Crawler `FAILED` | Glue console → Crawler → Runs; IAM role `dep-crawler-*` | Missing `AWSGlueServiceRole` or S3 read → fix role policy |
| Glue job `FAILED` / OOM | CloudWatch → `/aws-glue/jobs/output` & `/aws-glue/jobs/error` logs | OOM on dedupe/join → raise workers (capacity tiers) or bump DPU; bad data → check rejects path |
| "Incompatible schema change" raised | Glue job logs | Backward-incompatible change → align `entities.yaml` with source; additive columns auto-handled |
| Athena DQ check fails (pipeline hard-stops) | Athena console → Query history; `s3://…/silver_rejects/` | Bad data in silver → inspect rejects, fix upstream, re-run; check definition if threshold too strict |
| Redshift COPY error / access denied | `STL_LOAD_ERRORS`, `SVL_STATEMENTSUMMARY`; cluster IAM role | Role missing silver-bucket read → attach policy; wrong workgroup → check `settings.yaml` |
| Duplicate rows in analytics | `batch_registry` stage history; compare `batch_id` counts | Should be impossible by design → check a stage bypassed the merge template; rerun the load (self-healing) |
| Small files creeping in | `aws s3 ls` on silver prefix; avg object size | Backfills/high-frequency runs → run `compact_silver_job` for affected `ingest_date`s |
| Athena queries slow/expensive | Athena → query scan bytes | Missing partition filter → ensure `ingest_date` predicate; columnar Parquet + 128 MB files already mitigate |
| Redshift BI queries slow | `EXPLAIN`, `SVL_QUERY_SUMMARY` (skew), WLM queue logs | Skewed distkey → check `DISTSTYLE`; stale stats → `ANALYZE`; move heavy aggregates to materialized views |
| DAG runs late / SLA miss | Airflow → Gantt/SLA views; sensor timeouts | Upstream late arrival → sensors `reschedule` (no worker held); raise `timeout`, check Glue queue depth |


# 🛠️ STEP-BY-STEP IMPLEMENTATION GUIDE

## Step 0 — Prerequisites

| Tool | Version | Used for |
|------|---------|----------|
| Python | 3.11+ | pipeline code, tests, Glue/PySpark logic |
| AWS CLI v2 | latest | deployment (`aws cloudformation`, `aws s3`, ...) |
| Apache Airflow | 2.9+ | orchestration (local standalone for dev, **MWAA** for prod) |
| AWS Glue | 4.0 (Spark 3.3) | distributed transformation engine |
| AWS Lambda | python3.12 | event-driven arrival validation |
| DynamoDB | on-demand | batch checkpoint registry |
| Amazon Redshift (RA3) | latest | gold warehouse for Power BI |
| Amazon Athena | — | zero-copy SQL data-quality gate |
| Power BI Desktop/Service | latest | BI consumption (DirectQuery/Import on Redshift) |
| Git + GitHub Actions | — | version control + CI |

AWS services used: S3, Glue (Catalog + Crawler + ETL), Lambda, DynamoDB, Athena,
Redshift, SNS, Secrets Manager, IAM.

## Step 1 — Clone & run locally (no AWS account needed)

```bash
git clone https://github.com/jayantorion/aws-production-stack.git
cd aws-production-stack
python -m venv .venv && .venv\Scripts\activate     # Windows (or: source .venv/bin/activate)
pip install -r requirements.txt -r requirements-dev.txt

python -m pytest tests/ -v          # 13 unit tests (idempotency, resume, capacity)
python scripts/smoke_bronze.py      # lands all 6 sample entities through the bronze logic
```

The smoke test proves the bronze core works end-to-end against `sample_data/retail_db`
using an in-memory S3 fake — 12 objects (6 data parts + 6 manifests) with correct row counts.

## Step 2 — Understand the configuration (all knobs live here)

1. **`config/entities.yaml`** — one block per entity: columns/types/PK/nullability,
   `load_type` (`full_snapshot` for dims, `incremental` + `watermark_column` for facts).
   Add a new source table here first; every stage picks it up automatically.
2. **`config/settings.yaml`** — buckets, Glue/Redshift/Lambda names, cron, source paths.
   Override per environment without editing: `SETTINGS_S3__RAW_BUCKET=prod-dep-raw`.
3. Deploy-time, `entities.yaml` is uploaded to S3 so Glue/Lambda read the same contract.

## Step 3 — Deploy AWS infrastructure

```bash
# 3a. create a deployment bucket for artifacts
aws s3 mb s3://my-deploy-bucket

# 3b. package lambda + upload glue script
cd lambda/arrival_validator && zip -r ../../arrival_validator.zip . && cd ../..
aws s3 cp arrival_validator.zip s3://my-deploy-bucket/lambda/arrival_validator.zip
aws s3 cp glue_jobs/retail_silver_job.py s3://my-deploy-bucket/glue/retail_silver_job.py

# 3c. create a Glue ETL role (or reuse existing), then deploy the stack
aws cloudformation deploy \
  --template-file infra/cloudformation.yaml \
  --stack-name dep-core --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides Environment=dev DeployBucket=my-deploy-bucket GlueJobRoleArn=<role-arn>

# 3d. upload pipeline config consumed by Glue/Lambda
aws s3 cp config/entities.yaml s3://<raw-bucket>/config/entities.yaml
```
(Or run everything at once: `bash scripts/deploy.sh dev`.)
This creates: 4 S3 buckets (raw w/ lifecycle→Glacier, silver, quarantine, athena-results),
DynamoDB `dep_batch_registry` (+ GSI for resume), SNS alerts topic, IAM least-privilege
roles, Glue database/crawler/silver-job, and the Lambda validator.

## Step 4 — Wire the S3 event trigger

```bash
FUNCTION_ARN=$(aws cloudformation describe-stacks --stack-name dep-core \
  --query "Stacks[0].Outputs[?OutputKey=='ValidatorArn'].OutputValue" --output text)
RAW_BUCKET=$(aws cloudformation describe-stacks --stack-name dep-core \
  --query "Stacks[0].Outputs[?OutputKey=='RawBucket'].OutputValue" --output text)
aws s3api put-bucket-notification-configuration --bucket "$RAW_BUCKET" \
  --notification-configuration '{"LambdaFunctionConfigurations":[{"LambdaFunctionArn":"'$FUNCTION_ARN'","Events":["s3:ObjectCreated:*"],"Filter":{"Key":{"FilterRules":[{"Name":"suffix","Value":"_MANIFEST.json"}]}}}]}'
```
Now every landed batch auto-triggers validation → crawler. (The DAG also invokes the
Lambda directly — safe, because the registry short-circuits duplicate batch_ids.)

## Step 5 — Set up Airflow

**Local (dev):** `pip install "apache-airflow==2.9.*" && airflow standalone`, then copy
`dags/` into `$AIRFLOW_HOME/dags/`. **Production:** MWAA — upload `dags/` to the MWAA DAG S3 folder.

Set up:
- `SETTINGS_*` env overrides for the target environment (or env-specific `settings.yaml`)
- `BATCH_REGISTRY_TABLE` + `DEP_REDSHIFT_IAM_ROLE` (used by the COPY command)
- AWS credentials via Airflow connection `aws_default` (never in code)
- Source credentials (MySQL/SFTP/API keys) in **AWS Secrets Manager**

Unpause `retail_medallion_pipeline` — it runs daily at 02:00 UTC
(`config/settings.yaml → pipeline.schedule_cron`).

## Step 6 — Create the Redshift gold layer

```bash
psql -h <redshift-endpoint> -U admin -d devdw -f sql/redshift_ddl.sql   # run once
```
- Create an `analytics` read-only group for BI users.
- Attach a cluster IAM role that can read the silver bucket (`IAM_ROLE` in `sql/redshift_load_template.sql`).

## Step 7 — Run the pipeline & monitor

1. Trigger manually first: Airflow UI → `retail_medallion_pipeline` → **Trigger DAG**.
2. Watch the chain: `land_bronze_*` → `lambda_arrival_validation` → `glue_crawler_ready`
   → `glue_silver_job` → `glue_silver_done` → `athena_dq_gate` → `redshift_load`.
3. Audit trail: query DynamoDB `dep_batch_registry` — every batch's current stage + status.
4. Alerts: subscribe your email to the SNS `dep-alerts` topic (quarantine + DQ failures).

## Step 8 — Connect Power BI (consumption)

1. Power BI Desktop → Get Data → **Amazon Redshift** → server + database.
2. Use **DirectQuery** for live gold marts, **Import** for small dims/aggregates.
3. Build the semantic layer on `analytics.*` tables only — never bronze/silver.

## Step 9 — CI

Every push/PR runs `.github/workflows/ci.yml`: **ruff** lint over `dags/ glue_jobs/ lambda/ tests/`
+ **pytest**. Keep it green before merging.

---

## 📈 How data growth is handled (40 GB → 80 GB → growing every day)

1. **Glue right-sizing:** `dags/utils/capacity.py` reads the actual landed bytes from the
   batch manifests and picks a worker tier (4 G.1X → 120 G.2X). An 80 GB day automatically
   gets ~2× the workers of a 40 GB day — same runtime SLA, zero code change.
2. **Partition pruning everywhere:** every read touches only the batch's `ingest_date`
   partitions, so cost/runtime grow with *new* data, not total data.
3. **Redshift:** daily incremental COPY of only new data; RA3 nodes resize online as the
   warehouse grows; WLM queues isolate BI from ETL workloads.
4. **S3 lifecycle:** raw → Glacier at 30 days, expire at 180 days; silver is compressed
   Parquet (~0.2× raw size).
5. **Concurrency guard:** `max_active_runs=1` prevents overlapping runs on a big day.

## ♻️ How mid-pipeline failure recovery works (60% processed, 40% left)

| Failure point | What a rerun does | Why no duplicates |
|---|---|---|
| during `land_bronze_*` | reuses the open batch_id (state store), re-lands **over** the same S3 prefix | overwrite, never append |
| during Lambda / crawler | Lambda registry short-circuits; crawler re-crawl is a metadata no-op | DynamoDB `batch_registry` |
| during Glue ETL | re-reads the same batch, rewrites the same partitions via `replaceWhere` | partition-scoped overwrite + PK latest-wins dedupe |
| during Athena DQ | pure SQL re-run | stateless |
| during Redshift load | `DELETE WHERE batch_id` + natural-key merge inside one transaction | all-or-nothing; net no-op |

The checkpoint state machine (`LANDED → VALIDATED → CRAWLED → TRANSFORMED → DQ_PASSED → LOADED`)
moves **forward only** (`dags/utils/state_store.py`). Airflow retries failed tasks with
exponential backoff (5→15→45 min); the pipeline resumes exactly where it stopped.

## 🧹 Small-file policy — 128 MB everywhere (added 2026-09-02)

Small files are the #1 silent killer of S3-based lakehouses: thousands of tiny
objects → S3 request costs, Glue/Athena metadata (LIST + Parquet footer) overhead,
slow query planning. This project prevents them at **every write path**:

| Layer | Mechanism |
|---|---|
| Bronze landing | `rows_per_file_for()` samples real row widths and splits parts to ~**128 MB** each (no fixed row counts) |
| Silver ETL | `write_sized()` computes file count from estimated bytes → `repartition(n, partition_cols)` → ~128 MB files per partition dir |
| Spark config | AQE enabled with `advisoryPartitionSizeInBytes=128MB`, `parallelismFirst=false` |
| Historical repair | `glue_jobs/compact_silver_job.py` re-reads any date range and rewrites it compacted (replaceWhere, idempotent) |
| Prevention guardrail | One batch per entity per day → 1–2 files per partition by construction |

> 📖 Deep design details live in `LINEAGE.md` (local-only, not in this repo).




