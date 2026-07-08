-- =============================================================================
-- Redshift load template — idempotent batch load (per entity, per batch_id)
-- Placeholders filled by utils/redshift_loader.py: {entity}, {batch_id},
-- {silver_path}, {iam_role}, {columns}, {pk}
-- Transaction wraps DELETE+INSERT: all-or-nothing; re-running the same batch_id
-- is a net no-op => NO DUPLICATES on retry (LINEAGE.md §3/§6).
-- =============================================================================
BEGIN;

-- 1) COPY silver S3 -> staging (parallel, columnar; manifest-scoped)
COPY staging.{entity} ({columns})
FROM '{silver_path}'
IAM_ROLE '{iam_role}'
FORMAT AS PARQUET
SERIALIZETOJSON;

-- 2) Remove any prior copy of THIS batch (retry safety)
DELETE FROM analytics.{entity} WHERE batch_id = '{batch_id}';

-- 3) Merge semantics: delete stale natural keys, insert fresh rows
DELETE FROM analytics.{entity}
USING staging.{entity} s
WHERE analytics.{entity}.{pk} = s.{pk};

INSERT INTO analytics.{entity} ({columns}, batch_id)
SELECT {columns}, '{batch_id}' FROM staging.{entity};

-- 4) Clear staging + refresh planner stats
TRUNCATE staging.{entity};
ANALYZE analytics.{entity};

COMMIT;
