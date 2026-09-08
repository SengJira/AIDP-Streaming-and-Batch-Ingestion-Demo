-- 07 - Batch ingestion validation
--
-- Run these from Starburst after jobs/batch_ingestion_job.py (Spark on AIDP).
-- Every query answers one question a demo audience actually asks, and each is
-- a plain read-only SELECT, so they are safe to run at any time.

-- 1. What did the last run do?
--    One row per source per batch. rows_inserted / rows_updated come from the
--    Iceberg snapshot the MERGE produced, so they describe what was committed,
--    not what the job intended.
SELECT
    batch_id,
    source,
    target_table,
    mode,
    watermark,
    rows_read,
    rows_inserted,
    rows_updated,
    ROUND(duration_sec, 1) AS duration_sec,
    status,
    message
FROM js_financial_ice.ingestion.ingestion_audit
WHERE batch_id = (
    SELECT MAX(batch_id) FROM js_financial_ice.ingestion.ingestion_audit
)
ORDER BY started_at;

-- 2. Is the ingestion idempotent?
--    Re-running the same batch must upsert, never duplicate. Both counts must
--    be equal to the row count, and duplicate_keys must be 0.
SELECT
    'dim_customer'                                  AS table_name,
    COUNT(*)                                        AS rows_total,
    COUNT(DISTINCT customer_id)                     AS distinct_keys,
    COUNT(*) - COUNT(DISTINCT customer_id)          AS duplicate_keys
FROM js_financial_ice.ingestion.dim_customer
UNION ALL
SELECT
    'dim_account',
    COUNT(*),
    COUNT(DISTINCT account_id),
    COUNT(*) - COUNT(DISTINCT account_id)
FROM js_financial_ice.ingestion.dim_account
UNION ALL
SELECT
    'fact_payment_daily',
    COUNT(*),
    COUNT(DISTINCT (event_date, account_id)),
    COUNT(*) - COUNT(DISTINCT (event_date, account_id))
FROM js_financial_ice.ingestion.fact_payment_daily;

-- 3. Did the batch load match the source of record?
--    dim_customer is a copy of the MySQL table taken through the data catalog,
--    so the counts must agree. A shortfall means an incremental run has not
--    caught up (check the watermark in query 1).
SELECT
    (SELECT COUNT(*) FROM js_mysql_customer360.customer360.customers) AS mysql_rows,
    (SELECT COUNT(*) FROM js_financial_ice.ingestion.dim_customer)      AS iceberg_rows,
    (SELECT COUNT(*) FROM js_mysql_customer360.customer360.customers)
      - (SELECT COUNT(*) FROM js_financial_ice.ingestion.dim_customer)  AS not_yet_ingested;

-- 4. Does the rollup agree with the events it was built from?
--    fact_payment_daily is a derived table; if it disagrees with a direct
--    aggregate over payment_transactions, the rollup is stale for that day.
--    diff_txn_count must be 0 for every settled day.
WITH ground_truth AS (
    SELECT
        event_date,
        COUNT(*)      AS txn_count,
        SUM(amount)   AS total_amount
    FROM js_financial_ice.banking.payment_transactions
    WHERE event_date >= current_date - INTERVAL '3' DAY
    GROUP BY event_date
),
rollup AS (
    SELECT
        event_date,
        SUM(txn_count)    AS txn_count,
        SUM(total_amount) AS total_amount
    FROM js_financial_ice.ingestion.fact_payment_daily
    WHERE event_date >= current_date - INTERVAL '3' DAY
    GROUP BY event_date
)
SELECT
    g.event_date,
    g.txn_count                                     AS events_txn_count,
    r.txn_count                                     AS rollup_txn_count,
    g.txn_count - COALESCE(r.txn_count, 0)          AS diff_txn_count,
    ROUND(g.total_amount, 2)                        AS events_total,
    ROUND(COALESCE(r.total_amount, 0), 2)           AS rollup_total,
    ROUND(g.total_amount - COALESCE(r.total_amount, 0), 2) AS diff_total
FROM ground_truth g
LEFT JOIN rollup r ON r.event_date = g.event_date
ORDER BY g.event_date DESC;

-- 5. Batch history: how has each source behaved over the last runs?
--    Rising rows_updated with flat rows_inserted is the healthy steady state
--    for a dimension; a FAILED row here is the first place to look when the
--    federated queries go stale.
SELECT
    source,
    COUNT(*)                                        AS runs,
    SUM(rows_read)                                  AS rows_read_total,
    SUM(rows_inserted)                              AS rows_inserted_total,
    SUM(rows_updated)                               AS rows_updated_total,
    COUNT_IF(status = 'FAILED')                     AS failures,
    MAX(finished_at)                                AS last_run,
    ROUND(AVG(duration_sec), 1)                     AS avg_duration_sec
FROM js_financial_ice.ingestion.ingestion_audit
GROUP BY source
ORDER BY source;

-- 6. Lineage: which landing file and which batch does a row come from?
--    Every ingested row carries source_system, batch_id and ingested_at, so
--    "where did this number come from" is answerable without reading the job.
SELECT
    source_system,
    batch_id,
    source_file,
    COUNT(*)            AS rows_from_file,
    MAX(ingested_at)    AS ingested_at
FROM js_financial_ice.ingestion.dim_account
GROUP BY source_system, batch_id, source_file
ORDER BY ingested_at DESC
LIMIT 20;

-- 7. The point of the whole exercise: the batch-loaded dimension joins the
--    streaming fact table inside one Iceberg catalog, so this needs no
--    federation at query time — Starburst reads a single catalog.
--    Compare with sql/02_customer_payment_360.sql, which joins MySQL live.
SELECT
    c.customer_tier,
    c.risk_rating,
    COUNT(DISTINCT f.account_id)        AS accounts_with_payments,
    SUM(f.txn_count)                    AS txn_count,
    ROUND(SUM(f.total_amount), 2)       AS total_amount,
    ROUND(AVG(f.avg_risk_score), 3)     AS avg_risk_score
FROM js_financial_ice.ingestion.fact_payment_daily f
JOIN js_financial_ice.ingestion.dim_customer c
  ON c.account_id = f.account_id
WHERE f.event_date >= current_date - INTERVAL '7' DAY
GROUP BY c.customer_tier, c.risk_rating
ORDER BY total_amount DESC;
