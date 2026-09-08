-- Iceberg tables maintained by jobs/batch_ingestion_job.py (Spark on AIDP).
--
-- The job creates them itself with CREATE TABLE IF NOT EXISTS, so this file is
-- not a prerequisite. It exists for two reasons: to review the target model
-- before the first run, and to create the tables from Starburst when the Spark
-- job is not the first thing to touch the schema. Keep the two in sync — the
-- column names and order are the MERGE contract.
--
-- Companion to Iceberg_Table.sql, which holds the streaming event tables in
-- js_financial_ice.banking. The batch targets live in their own schema,
-- js_financial_ice.ingestion, so the two never mix; the only cross-schema
-- reference is the rollup reading banking.payment_transactions.

-- The schema is expected to exist already, created with an explicit location
-- (a Glue database's location wins over the job's --warehouse for every table
-- in it):
--
--   CREATE SCHEMA IF NOT EXISTS js_financial_ice.ingestion
--   WITH (location = 's3://js-demo/warehouse/ingestion');

-- 1. Customer dimension — source: js_mysql_customer360.customer360.customers
--    read through the AIDP data catalog (Trino JDBC) by the Spark job.
--    Deliberately unpartitioned: 10k rows, and every MERGE rewrites whole data
--    files, so partitioning this dimension only creates small files.
CREATE TABLE IF NOT EXISTS js_financial_ice.ingestion.dim_customer (
    customer_id                 VARCHAR,
    account_id                  VARCHAR,
    full_name                   VARCHAR,
    national_id_masked          VARCHAR,
    date_of_birth               DATE,
    age_group                   VARCHAR,
    gender                      VARCHAR,
    email                       VARCHAR,
    mobile_number_masked        VARCHAR,
    city                        VARCHAR,
    province                    VARCHAR,
    country                     VARCHAR,
    postal_code                 VARCHAR,
    customer_tier               VARCHAR,
    occupation                  VARCHAR,
    annual_income               DECIMAL(14, 2),
    risk_rating                 VARCHAR,
    kyc_status                  VARCHAR,
    pep_flag                    BOOLEAN,
    sanctions_screening_status  VARCHAR,
    account_open_date           DATE,
    preferred_channel           VARCHAR,
    marketing_consent           BOOLEAN,
    -- src_* are the source system's own audit columns. src_updated_at is the
    -- incremental high-water mark: the job reads MAX(src_updated_at) here and
    -- asks the source for rows changed after it.
    src_created_at              TIMESTAMP(6) WITH TIME ZONE,
    src_updated_at              TIMESTAMP(6) WITH TIME ZONE,
    source_system               VARCHAR,
    batch_id                    VARCHAR,
    ingested_at                 TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    location = 's3://js-demo/warehouse/ingestion/dim_customer'
);

-- 2. Account dimension — source: CSV files in the object-storage landing zone
--    (s3a://js-demo/landing/accounts/*.csv). Full snapshot every run: the file
--    carries no change column, so the MERGE is what makes a re-drop idempotent.
CREATE TABLE IF NOT EXISTS js_financial_ice.ingestion.dim_account (
    account_id      VARCHAR,
    customer_tier   VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    -- Which landing file the row came from, for lineage questions.
    source_file     VARCHAR,
    src_updated_at  TIMESTAMP(6) WITH TIME ZONE,
    source_system   VARCHAR,
    batch_id        VARCHAR,
    ingested_at     TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    location = 's3://js-demo/warehouse/ingestion/dim_account'
);

-- 3. Daily payment rollup — source: js_financial_ice.banking.payment_transactions
--    (the Iceberg table the streaming job writes). Merge key is
--    (event_date, account_id), so recomputing a day overwrites its row instead
--    of double counting. Partitioned to match the source's event_date.
CREATE TABLE IF NOT EXISTS js_financial_ice.ingestion.fact_payment_daily (
    event_date      DATE,
    account_id      VARCHAR,
    txn_count       BIGINT,
    total_amount    DOUBLE,
    avg_amount      DOUBLE,
    max_amount      DOUBLE,
    approved_count  BIGINT,
    declined_count  BIGINT,
    intl_count      BIGINT,
    avg_risk_score  DOUBLE,
    first_txn_at    TIMESTAMP(6) WITH TIME ZONE,
    last_txn_at     TIMESTAMP(6) WITH TIME ZONE,
    source_system   VARCHAR,
    batch_id        VARCHAR,
    ingested_at     TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['event_date'],
    location = 's3://js-demo/warehouse/ingestion/fact_payment_daily'
);

-- 4. Run log — one row per source per batch, appended after every run.
--    rows_inserted / rows_updated come from the Iceberg snapshot the MERGE
--    produced, not from a counter in the job, so they cannot drift from what
--    was actually committed.
CREATE TABLE IF NOT EXISTS js_financial_ice.ingestion.ingestion_audit (
    batch_id        VARCHAR,
    source          VARCHAR,
    target_table    VARCHAR,
    mode            VARCHAR,
    watermark       TIMESTAMP(6) WITH TIME ZONE,
    rows_read       BIGINT,
    rows_inserted   BIGINT,
    rows_updated    BIGINT,
    snapshot_id     BIGINT,
    started_at      TIMESTAMP(6) WITH TIME ZONE,
    finished_at     TIMESTAMP(6) WITH TIME ZONE,
    duration_sec    DOUBLE,
    status          VARCHAR,
    message         VARCHAR
)
WITH (
    format = 'PARQUET',
    location = 's3://js-demo/warehouse/ingestion/ingestion_audit'
);
