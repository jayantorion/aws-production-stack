-- =============================================================================
-- Athena DQ gate — zero-copy validation of SILVER before Redshift load
-- Executed per entity by utils/athena_dq.py against silver_db tables.
-- PASS required (0 failing rows) before any warehouse load. FAIL = hard stop.
-- =============================================================================

-- 1) NULL CHECK — mandatory columns must be populated
SELECT 'null_check' AS check_name, COUNT(*) AS failures
FROM silver_db.{entity}
WHERE ingest_date IN ({ingest_dates})
  AND ({mandatory_null_expr});

-- 2) DUPLICATE CHECK — business key must be unique
SELECT 'duplicate_pk' AS check_name, COUNT(*) AS failures
FROM (
    SELECT {pk}, COUNT(*) AS c
    FROM silver_db.{entity}
    WHERE ingest_date IN ({ingest_dates})
    GROUP BY {pk}
    HAVING COUNT(*) > 1
) d;

-- 3) ROW-COUNT DELTA — today vs previous ingest_date (alert if collapse)
SELECT 'row_count_delta' AS check_name,
       ABS(1.0 * t.today - GREATEST(y.yesterday, 1)) / GREATEST(t.today, 1) AS failures
FROM (SELECT COUNT(*) AS today FROM silver_db.{entity}
      WHERE ingest_date = '{latest_date}') t
CROSS JOIN (SELECT COUNT(*) AS yesterday FROM silver_db.{entity}
      WHERE ingest_date = DATE_ADD('day', -1, DATE '{latest_date}')) y
WHERE y.yesterday > 0 AND ABS(1.0 * t.today - y.yesterday) / y.yesterday > 0.20;

-- 4) FRESHNESS — max event date must be within expected window
SELECT 'freshness' AS check_name, COUNT(*) AS failures
FROM silver_db.{entity}
WHERE ingest_date = '{latest_date}'
  AND ({event_date_expr} < DATE_ADD('day', -7, DATE '{latest_date}'));

-- 5) REFERENTIAL — orders must reference known customers
SELECT 'orphan_fk_orders' AS check_name, COUNT(*) AS failures
FROM silver_db.orders o
LEFT JOIN silver_db.customers c ON o.order_customer_id = c.customer_id
WHERE o.ingest_date IN ({ingest_dates}) AND c.customer_id IS NULL;
