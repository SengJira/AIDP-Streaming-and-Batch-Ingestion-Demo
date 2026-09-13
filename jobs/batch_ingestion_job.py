#!/usr/bin/env python3
"""Batch ingestion pipeline — multiple sources -> Iceberg, idempotent upserts.

The batch counterpart of jobs/banking_streaming_job.py. Where that job keeps the
event tables live from Kafka, this one loads the *reference and rollup* tables
that the federated Customer 360 demo joins against, from three different kinds
of source in one Spark application:

    customers       Starburst / AIDP data catalog over JDBC
                    (js_mysql_customer360.customer360.customers -> MySQL).
                    The catalog is the source of record, so Spark reads through
                    it instead of opening a second connection to MySQL; set
                    --customers-reader mysql to bypass it (see below).
    accounts        Delimited files in the object-storage landing zone
                    (s3a://js-demo/landing/accounts/*.csv).
    payments_daily  An existing Iceberg table in the same catalog
                    (banking.payment_transactions, written by the streaming
                    job), rolled up per account/day.

Targets are written to a schema of their own — js_financial_ice.ingestion — so
the batch tables never mix with the streaming event tables in banking. The
rollup source keeps its own --payments-db for that reason.

Every source lands in Iceberg through the same three steps, so adding a fourth
means writing one reader function and one Target:

    1. read      -> a DataFrame with exactly the target's columns
    2. dedupe    -> one row per merge key, newest wins
    3. MERGE INTO the target on that key

That makes each run idempotent: re-running a batch updates rows in place rather
than appending duplicates, which is what lets the job be retried safely after a
failure. Row counts are taken from the Iceberg snapshot the MERGE produced and
recorded in banking.ingestion_audit.

Incremental loads use --mode incremental (the default), which reads the target's
own high-water mark instead of an external watermark store — no state to lose:

    customers       max(src_updated_at)  -> WHERE updated_at > <watermark>
    payments_daily  max(last_txn_at)     -> WHERE event_date >= <date> - lookback
    accounts        full snapshot every run (the CSV carries no change column)

Usage
-----
    # on AIDP (see scripts/submit_batch_aidp.sh)
    dell-data-processing-engine submit ... batch_ingestion_job.py --source all

    # locally against a throwaway warehouse, no Glue and no S3
    python jobs/batch_ingestion_job.py --source accounts \
        --catalog-type hadoop --warehouse /tmp/wh --accounts-path data

Configuration
-------------
Endpoints are command-line arguments (each falls back to an environment
variable), matching the streaming job: the AIDP CLI rejects
`spark.kubernetes.driverEnv.*` as reserved configuration.

Credentials are read from the environment only, so they can be injected with
`uploads create-secret` + `--uploaded-secrets` rather than appearing in the
submit command, which the CLI persists by default:

    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY   Glue metastore credentials.
    S3_ACCESS_KEY / S3_SECRET_KEY               Object storage credentials.
    AIDP_USERNAME / AIDP_PASSWORD               Starburst JDBC (customers).
    MYSQL_USER / MYSQL_PASSWORD                 Only for --customers-reader mysql.
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    avg, col, count, lit, max as smax, min as smin, row_number, sum as ssum, when,
)
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

TRINO_DRIVER = "io.trino.jdbc.TrinoDriver"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


# ── Targets ─────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Target:
    """An Iceberg table this job maintains.

    ``columns`` is the contract between reader and writer: MERGE's ``UPDATE SET
    *`` / ``INSERT *`` require the staged DataFrame to expose exactly these
    columns, in this order, so every reader ends with a select on it.
    """

    table: str
    keys: tuple[str, ...]
    ddl: str
    partition_by: tuple[str, ...] = ()
    watermark_column: str | None = None
    # Guards against re-applying an older version of a row when a batch is
    # replayed; skipped for targets whose reader recomputes the value anyway.
    ordering_column: str | None = None
    columns: tuple[str, ...] = field(init=False)

    def __post_init__(self):
        columns = tuple(
            line.split()[0]
            for line in (raw.strip() for raw in self.ddl.strip().splitlines())
            if line and not line.startswith("--")
        )
        object.__setattr__(self, "columns", columns)
        missing = [key for key in self.keys if key not in columns]
        if missing:
            raise ValueError(f"{self.table}: merge key(s) not in DDL: {missing}")


AUDIT_COLUMNS = """
    source_system   string,
    batch_id        string,
    ingested_at     timestamp
"""

DIM_CUSTOMER = Target(
    table="dim_customer",
    keys=("customer_id",),
    # Unpartitioned on purpose: 10k rows, and every partition file a MERGE
    # rewrites is a full data file. Partitioning a dimension this small only
    # adds small-file pressure.
    watermark_column="src_updated_at",
    ordering_column="src_updated_at",
    ddl="""
    customer_id                 string,
    account_id                  string,
    full_name                   string,
    national_id_masked          string,
    date_of_birth               date,
    age_group                   string,
    gender                      string,
    email                       string,
    mobile_number_masked        string,
    city                        string,
    province                    string,
    country                     string,
    postal_code                 string,
    customer_tier               string,
    occupation                  string,
    annual_income               decimal(14,2),
    risk_rating                 string,
    kyc_status                  string,
    pep_flag                    boolean,
    sanctions_screening_status  string,
    account_open_date           date,
    preferred_channel           string,
    marketing_consent           boolean,
    src_created_at              timestamp,
    src_updated_at              timestamp,
""" + AUDIT_COLUMNS,
)

DIM_ACCOUNT = Target(
    table="dim_account",
    keys=("account_id",),
    ordering_column="src_updated_at",
    ddl="""
    account_id      string,
    customer_tier   string,
    city            string,
    country         string,
    source_file     string,
    src_updated_at  timestamp,
""" + AUDIT_COLUMNS,
)

FACT_PAYMENT_DAILY = Target(
    table="fact_payment_daily",
    keys=("event_date", "account_id"),
    partition_by=("event_date",),
    watermark_column="last_txn_at",
    ddl="""
    event_date      date,
    account_id      string,
    txn_count       bigint,
    total_amount    double,
    avg_amount      double,
    max_amount      double,
    approved_count  bigint,
    declined_count  bigint,
    intl_count      bigint,
    avg_risk_score  double,
    first_txn_at    timestamp,
    last_txn_at     timestamp,
""" + AUDIT_COLUMNS,
)

AUDIT = Target(
    table="ingestion_audit",
    keys=("batch_id", "source"),
    ddl="""
    batch_id        string,
    source          string,
    target_table    string,
    mode            string,
    watermark       timestamp,
    rows_read       bigint,
    rows_inserted   bigint,
    rows_updated    bigint,
    snapshot_id     bigint,
    started_at      timestamp,
    finished_at     timestamp,
    duration_sec    double,
    status          string,
    message         string
""",
)

AUDIT_SCHEMA = StructType([
    StructField("batch_id",      StringType(),    True),
    StructField("source",        StringType(),    True),
    StructField("target_table",  StringType(),    True),
    StructField("mode",          StringType(),    True),
    StructField("watermark",     TimestampType(), True),
    StructField("rows_read",     LongType(),      True),
    StructField("rows_inserted", LongType(),      True),
    StructField("rows_updated",  LongType(),      True),
    StructField("snapshot_id",   LongType(),      True),
    StructField("started_at",    TimestampType(), True),
    StructField("finished_at",   TimestampType(), True),
    StructField("duration_sec",  DoubleType(),    True),
    StructField("status",        StringType(),    True),
    StructField("message",       StringType(),    True),
])

# The landing files are read with an explicit schema: inferSchema costs an extra
# pass and, worse, lets a source-side change alter the target silently.
ACCOUNTS_CSV_SCHEMA = StructType([
    StructField("account_id",    StringType(), True),
    StructField("customer_tier", StringType(), True),
    StructField("city",          StringType(), True),
    StructField("country",       StringType(), True),
])


# ── Session ─────────────────────────────────────────────────────────────────
def build_session(cfg) -> SparkSession:
    """Session with the Iceberg catalog wiring for the selected catalog type.

    Master, driver memory and executor resources are deliberately NOT set here:
    on AIDP they come from the submit flags and the resource pool.
    """
    catalog = cfg.catalog
    builder = (
        SparkSession.builder
        .appName(f"BatchIngestionJob-{cfg.source}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", cfg.warehouse)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        # UTC everywhere. The watermark is read back out of Iceberg as a naive
        # datetime and sent to the source as a SQL literal, so a session in
        # local time would silently shift the incremental window.
        .config("spark.sql.session.timeZone", "UTC")
        # MERGE rewrites whole data files; without this a 10k-row dimension
        # ends up as one file per shuffle partition.
        .config("spark.sql.adaptive.enabled", "true")
    )

    if cfg.catalog_type == "hadoop":
        # Local/dev: filesystem-backed catalog, no Glue and no credentials.
        return builder.config(f"spark.sql.catalog.{catalog}.type", "hadoop").getOrCreate()

    s3_key = os.environ.get("S3_ACCESS_KEY")
    s3_secret = os.environ.get("S3_SECRET_KEY")
    missing = [name for name, value in (
        ("S3_ACCESS_KEY", s3_key),
        ("S3_SECRET_KEY", s3_secret),
        ("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID")),
        ("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY")),
    ) if not value]
    if missing:
        sys.exit(
            f"Missing required credential env vars: {', '.join(missing)}.\n"
            "Attach them with: --uploaded-secrets <upload_id>"
        )

    return (
        builder
        # Glue-compatible managed metastore. AWS_* is read by the SDK
        # credential chain, not set here: Iceberg's GlueCatalog ignores
        # catalog-scoped client keys (apache/iceberg#10614).
        .config(f"spark.sql.catalog.{catalog}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.glue.id", cfg.glue_catalog_id)
        .config(f"spark.sql.catalog.{catalog}.glue.endpoint", cfg.glue_endpoint)
        .config(f"spark.sql.catalog.{catalog}.client.region", cfg.glue_region)

        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", cfg.s3_endpoint)
        .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", s3_key)
        .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", s3_secret)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.http-client.type", "apache")
        .config(f"spark.sql.catalog.{catalog}.http-client.apache.connection-timeout-ms", "5000")
        .config(f"spark.sql.catalog.{catalog}.http-client.apache.socket-timeout-ms", "30000")

        # The landing-zone files are read through Hadoop's FileSystem layer,
        # not S3FileIO, so S3A needs its own configuration.
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .getOrCreate()
    )


# ── Source 1: the AIDP data catalog (Starburst JDBC) ────────────────────────
def _timestamp_literal(moment: datetime) -> str:
    """Render a watermark for a SQL predicate, always in UTC.

    Timestamps read back from Iceberg arrive naive; they are UTC because the
    session timezone is pinned to UTC, so tagging them is safe. Anything
    tz-aware (``--since``) is converted rather than assumed.
    """
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _jdbc_read(spark: SparkSession, options: dict, sql: str, cfg) -> DataFrame:
    """Run ``sql`` over JDBC, optionally split into --num-partitions readers.

    Spark's `query` option cannot be combined with `partitionColumn`, so a
    parallel read has to wrap the statement as a subquery exposing a numeric
    shard column. One partition (the default) skips that and reads serially,
    which is the right choice for a 10k-row dimension.
    """
    shard = options.pop("__shard_expr__")
    reader = spark.read.format("jdbc").options(**options)
    if cfg.num_partitions <= 1:
        return reader.option("query", sql).load()

    return (
        reader
        .option("dbtable", f"(SELECT t.*, {shard} AS _shard FROM ({sql}) t) shards")
        .option("partitionColumn", "_shard")
        .option("lowerBound", "0")
        .option("upperBound", str(cfg.num_partitions))
        .option("numPartitions", str(cfg.num_partitions))
        .load()
        .drop("_shard")
    )


# Columns shared by both customer readers. The flag columns are TINYINT(1) in
# MySQL, which the Trino connector surfaces as tinyint and Connector/J as bit,
# so both are cast to boolean on the Spark side rather than in SQL.
CUSTOMER_COLUMNS = """
    customer_id, account_id, full_name, national_id_masked, date_of_birth,
    age_group, gender, email, mobile_number_masked, city, province, country,
    postal_code, customer_tier, occupation, annual_income, risk_rating,
    kyc_status, pep_flag, sanctions_screening_status, account_open_date,
    preferred_channel, marketing_consent,
"""

# MySQL TIMESTAMP reaches Trino as `timestamp(0) with time zone`, and Spark's
# JDBC dialect has no mapping for TIMESTAMP_WITH_TIMEZONE — the read fails with
# UNRECOGNIZED_SQL_TYPE. Converting to UTC and casting to a plain timestamp in
# the query fixes it at the source and keeps the column instant-correct
# regardless of the coordinator's session timezone.
TRINO_UTC_TIMESTAMP = "CAST({column} AT TIME ZONE 'UTC' AS timestamp(6))"


def customer_projection(created: str, updated: str) -> str:
    """SELECT list, with each reader's own expression for the audit timestamps."""
    return (f"{CUSTOMER_COLUMNS} {created} AS src_created_at, "
            f"{updated} AS src_updated_at")


def customers_request(cfg, watermark: datetime | None) -> tuple[dict, str, str]:
    """JDBC options, SQL and source label for the customer read.

    Kept free of Spark so the connection details — TLS mode, the incremental
    predicate, and the fact that credentials go in JDBC properties and never
    into the statement — can be asserted without a cluster.

    Missing credentials raise rather than exit: a source the secret set cannot
    reach must fail on its own and leave the rest of the batch to run.
    """
    if cfg.customers_reader == "mysql":
        user, password = os.environ.get("MYSQL_USER"), os.environ.get("MYSQL_PASSWORD")
        if not (user and password):
            raise RuntimeError(
                "--customers-reader mysql needs MYSQL_USER and MYSQL_PASSWORD in the "
                "environment; attach them with --uploaded-secrets <upload_id>")
        where = (f"WHERE updated_at > '{_timestamp_literal(watermark)}'" if watermark else "")
        options = {
            # connectionTimeZone=UTC matches the container's --default-time-zone
            # and the UTC Spark session, so TIMESTAMP columns are not shifted.
            "url": (f"jdbc:mysql://{cfg.mysql_host}:{cfg.mysql_port}/{cfg.mysql_db}"
                    "?connectionTimeZone=UTC"),
            "driver": MYSQL_DRIVER,
            "user": user,
            "password": password,
            "fetchsize": str(cfg.fetch_size),
            "__shard_expr__": "crc32(t.customer_id) % " + str(cfg.num_partitions),
        }
        projection = customer_projection("created_at", "updated_at")
        sql = f"SELECT {projection} FROM {cfg.mysql_table} {where}"
        source_system = f"mysql://{cfg.mysql_host}/{cfg.mysql_db}.{cfg.mysql_table}"
    else:
        user, password = os.environ.get("AIDP_USERNAME"), os.environ.get("AIDP_PASSWORD")
        if not (user and password):
            raise RuntimeError(
                "--customers-reader starburst needs AIDP_USERNAME and AIDP_PASSWORD in "
                "the environment; attach them with --uploaded-secrets <upload_id>")
        # SSLVerification=NONE is the JDBC equivalent of AIDP_VERIFY_TLS=false:
        # lab-only, because the coordinator's certificate is issued by a private
        # CA (CN=TitanCA). Prefer a trust store holding that CA.
        url = f"jdbc:trino://{cfg.aidp_host}:{cfg.aidp_port}?SSL=true"
        if cfg.aidp_truststore:
            url += (f"&SSLTrustStorePath={cfg.aidp_truststore}"
                    f"&SSLTrustStorePassword={os.environ.get('AIDP_TRUSTSTORE_PASSWORD', '')}")
        elif not cfg.aidp_verify_tls:
            print("WARNING: TLS verification disabled for the Starburst JDBC "
                  "connection — lab testing only", flush=True)
            url += "&SSLVerification=NONE"
        updated_utc = TRINO_UTC_TIMESTAMP.format(column="updated_at")
        # Compared in UTC too, so the incremental window does not depend on the
        # coordinator's session timezone.
        where = (f"WHERE {updated_utc} > TIMESTAMP '{_timestamp_literal(watermark)}'"
                 if watermark else "")
        options = {
            "url": url,
            "driver": TRINO_DRIVER,
            "user": user,
            "password": password,
            "fetchsize": str(cfg.fetch_size),
            "__shard_expr__": "abs(from_big_endian_64(xxhash64(to_utf8(t.customer_id)))) % "
                              + str(cfg.num_partitions),
        }
        projection = customer_projection(
            TRINO_UTC_TIMESTAMP.format(column="created_at"), updated_utc)
        sql = (f"SELECT {projection} "
               f"FROM {cfg.source_catalog}.{cfg.source_schema}.{cfg.source_table} {where}")
        source_system = f"{cfg.source_catalog}.{cfg.source_schema}.{cfg.source_table}"

    return options, " ".join(sql.split()), source_system


def read_customers(spark: SparkSession, cfg, watermark: datetime | None) -> DataFrame:
    """Customers from the Starburst data catalog (or MySQL directly).

    Reading through the catalog keeps one governed access path for the whole
    demo: the same `js_mysql_customer360` catalog the Streamlit app queries,
    with the same read-only credentials and audit trail. `--customers-reader
    mysql` exists for the case where the coordinator is unavailable, or to
    compare a direct connector read against the federated one.
    """
    options, sql, source_system = customers_request(cfg, watermark)
    if watermark:
        print(f"  incremental: rows changed after {watermark.isoformat()}", flush=True)

    raw = _jdbc_read(spark, options, sql, cfg)
    return (
        raw
        .withColumn("pep_flag", col("pep_flag").cast("boolean"))
        .withColumn("marketing_consent", col("marketing_consent").cast("boolean"))
        .withColumn("annual_income", col("annual_income").cast("decimal(14,2)"))
        .withColumn("source_system", lit(source_system))
    )


# ── Source 2: files in the landing zone ─────────────────────────────────────
def read_accounts(spark: SparkSession, cfg, watermark: datetime | None) -> DataFrame:
    """Account reference data from delimited files.

    A full snapshot every run: the CSV carries no change column, so there is
    nothing to read incrementally. The MERGE is what keeps that cheap and
    idempotent — unchanged rows are rewritten, never duplicated.

    ``_metadata.file_path`` rather than ``input_file_name()``: the latter is a
    non-deterministic expression, and Iceberg refuses a MERGE whose source plan
    contains one (it may scan the source twice).
    """
    path = f"{cfg.accounts_path.rstrip('/')}/{cfg.accounts_pattern}"
    print(f"  reading {path} (full snapshot)", flush=True)
    return (
        spark.read
        .schema(ACCOUNTS_CSV_SCHEMA)
        .option("header", "true")
        # Corrupt lines are dropped rather than nulled through, so a malformed
        # landing file cannot quietly blank out good rows in the dimension.
        .option("mode", "DROPMALFORMED")
        .csv(path)
        .filter(col("account_id").isNotNull())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("src_updated_at", lit(cfg.batch_ts).cast("timestamp"))
        .withColumn("source_system", lit(f"file://{cfg.accounts_path}"))
    )


# ── Source 3: an existing Iceberg table (rollup) ────────────────────────────
def read_payments_daily(spark: SparkSession, cfg, watermark: datetime | None) -> DataFrame:
    """Per-account daily rollup of banking.payment_transactions.

    The lookback matters: the streaming job writes late-arriving events into
    days that were already rolled up, so an incremental run has to recompute a
    trailing window instead of only days after the watermark. Recomputation is
    safe because the MERGE key is (event_date, account_id) — a replay overwrites
    the day's row with the newer total rather than adding to it.
    """
    # Read from --payments-db, not --db: the events are written by the streaming
    # job into its own schema, while this job's targets live in the ingestion
    # schema. They are deliberately not the same place.
    source = f"{cfg.catalog}.{cfg.payments_db}.{cfg.payments_table}"
    events = spark.table(source)
    if watermark:
        floor_date = (watermark - timedelta(days=cfg.lookback_days)).date()
        print(f"  incremental: event_date >= {floor_date} "
              f"(watermark {watermark.isoformat()} - {cfg.lookback_days}d lookback)",
              flush=True)
        events = events.filter(col("event_date") >= lit(floor_date.isoformat()).cast("date"))

    return (
        events
        .groupBy(col("event_date"), col("account_id"))
        .agg(
            count("*").alias("txn_count"),
            ssum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            smax("amount").alias("max_amount"),
            ssum(when(col("status") == "APPROVED", 1).otherwise(0)).alias("approved_count"),
            ssum(when(col("status") == "DECLINED", 1).otherwise(0)).alias("declined_count"),
            ssum(when(col("is_international"), 1).otherwise(0)).alias("intl_count"),
            avg("risk_score").alias("avg_risk_score"),
            smin("timestamp").alias("first_txn_at"),
            smax("timestamp").alias("last_txn_at"),
        )
        .withColumn("source_system", lit(source))
    )


@dataclass(frozen=True)
class Source:
    name: str
    target: Target
    reader: Callable[[SparkSession, object, datetime | None], DataFrame]


SOURCES = {
    source.name: source for source in (
        Source("customers", DIM_CUSTOMER, read_customers),
        Source("accounts", DIM_ACCOUNT, read_accounts),
        Source("payments_daily", FACT_PAYMENT_DAILY, read_payments_daily),
    )
}


# ── Iceberg write path ──────────────────────────────────────────────────────
def ensure_table(spark: SparkSession, cfg, target: Target) -> str:
    """Create the target if absent. Safe to call on every run."""
    name = f"{cfg.catalog}.{cfg.db}.{target.table}"
    partitioning = (f"PARTITIONED BY ({', '.join(target.partition_by)})"
                    if target.partition_by else "")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.db}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {name} (
        {target.ddl.strip().rstrip(',')}
        ) USING iceberg {partitioning}
        TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')
    """)
    return name


def read_watermark(spark: SparkSession, table: str, target: Target) -> datetime | None:
    """High-water mark from the target itself, so there is no state to lose."""
    if not target.watermark_column:
        return None
    row = spark.table(table).agg(smax(target.watermark_column).alias("hwm")).first()
    return row["hwm"] if row else None


def stage(df: DataFrame, target: Target) -> DataFrame:
    """Project onto the target's columns and reduce to one row per merge key.

    Without this a source that legitimately contains two versions of the same
    key in one batch makes the MERGE fail with "a single row from the target
    matched multiple rows from the source" — Iceberg refuses ambiguous updates.
    """
    order = (col(target.ordering_column).desc_nulls_last()
             if target.ordering_column else lit(1))
    ranked = df.withColumn(
        "_rn",
        row_number().over(Window.partitionBy(*target.keys).orderBy(order)),
    )
    return ranked.filter(col("_rn") == 1).select(*target.columns)


def merge(spark: SparkSession, table: str, staged: DataFrame, target: Target) -> None:
    """Upsert ``staged`` into ``table`` on the target's merge key."""
    view = f"stg_{target.table}"
    staged.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{key} = s.{key}" for key in target.keys)
    # UPDATE SET * / INSERT * are why stage() pins the column list: they match
    # by name, and a missing column is an analysis error, not a silent null.
    guard = (f"AND s.{target.ordering_column} >= t.{target.ordering_column} "
             if target.ordering_column else "")
    spark.sql(f"""
        MERGE INTO {table} t
        USING {view} s
        ON {on}
        WHEN MATCHED {guard}THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def snapshot_counts(spark: SparkSession, table: str, previous: int | None) -> dict:
    """Rows added/removed by the newest snapshot, from Iceberg metadata.

    Copy-on-write MERGE reports an update as one added and one deleted record,
    so `deleted-records` is the update count and the difference is the number of
    genuinely new rows. Returns zeros when the MERGE committed nothing.
    """
    row = (
        spark.table(f"{table}.snapshots")
        # Ordered by column name rather than col("...").desc(): col() needs an
        # active JVM, which keeps this function unit-testable.
        .orderBy("committed_at", ascending=False)
        .select("snapshot_id", "summary")
        .first()
    )
    if row is None or row["snapshot_id"] == previous:
        return {"snapshot_id": previous, "inserted": 0, "updated": 0}
    summary = row["summary"] or {}
    added = int(summary.get("added-records", 0))
    deleted = int(summary.get("deleted-records", 0))
    return {
        "snapshot_id": row["snapshot_id"],
        "inserted": max(added - deleted, 0),
        "updated": min(added, deleted),
    }


def current_snapshot(spark: SparkSession, table: str) -> int | None:
    row = (spark.table(f"{table}.snapshots")
           .orderBy("committed_at", ascending=False).select("snapshot_id").first())
    return row["snapshot_id"] if row else None


def write_audit(spark: SparkSession, cfg, rows: list[tuple]) -> None:
    if cfg.dry_run or not rows:
        return
    table = ensure_table(spark, cfg, AUDIT)
    # coalesce(1): a handful of rows, and the default parallelism would leave
    # one (mostly empty) data file per partition behind on every run.
    spark.createDataFrame(rows, schema=AUDIT_SCHEMA).coalesce(1).writeTo(table).append()


# ── Orchestration ───────────────────────────────────────────────────────────
def ingest(spark: SparkSession, cfg, source: Source) -> tuple:
    """Run one source end to end. Returns its audit row."""
    started = datetime.now(timezone.utc)
    batch_id = cfg.batch_id
    target = source.target
    table = ensure_table(spark, cfg, target)
    print(f"\n[{source.name}] -> {table}", flush=True)

    watermark = None
    if cfg.mode == "incremental":
        watermark = (cfg.since if cfg.since is not None
                     else read_watermark(spark, table, target))
        if target.watermark_column and watermark is None:
            print("  no watermark in target — first load reads everything", flush=True)

    # One ingested_at for the whole batch, as a literal: it makes every row of
    # a run comparable, and MERGE rejects a non-deterministic source plan.
    staged = stage(
        source.reader(spark, cfg, watermark)
        .withColumn("batch_id", lit(batch_id))
        .withColumn("ingested_at", lit(cfg.batch_ts).cast("timestamp")),
        target,
    )
    # Counted (and cached) before the MERGE: the staged plan re-reads the source
    # otherwise, and a JDBC source would be scanned twice.
    staged.persist()
    try:
        rows_read = staged.count()
        print(f"  staged {rows_read:,} row(s) after dedupe on {'+'.join(target.keys)}",
              flush=True)

        if cfg.dry_run:
            staged.show(5, truncate=40)
            counts = {"snapshot_id": None, "inserted": 0, "updated": 0}
            status, message = "DRY_RUN", "no write performed"
        elif rows_read == 0:
            counts = {"snapshot_id": current_snapshot(spark, table),
                      "inserted": 0, "updated": 0}
            status, message = "OK", "empty batch — nothing to merge"
            print("  empty batch — nothing to merge", flush=True)
        else:
            before = current_snapshot(spark, table)
            merge(spark, table, staged, target)
            counts = snapshot_counts(spark, table, before)
            status, message = "OK", None
            print(f"  merged: {counts['inserted']:,} inserted, "
                  f"{counts['updated']:,} updated (snapshot {counts['snapshot_id']})",
                  flush=True)
    finally:
        staged.unpersist()

    finished = datetime.now(timezone.utc)
    return (
        batch_id, source.name, target.table, "DRY_RUN" if cfg.dry_run else cfg.mode,
        watermark, rows_read, counts["inserted"], counts["updated"],
        counts["snapshot_id"], started, finished,
        (finished - started).total_seconds(), status, message,
    )


def parse_args(argv=None):
    def env(name, default):
        return os.environ.get(name, default)

    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    p.add_argument("--source", default="all", choices=["all", *SOURCES],
                   help="which source(s) to ingest (default: all)")
    p.add_argument("--mode", default="incremental", choices=["incremental", "full"],
                   help="incremental reads the target's high-water mark; full rereads "
                        "everything and upserts it (default: incremental)")
    p.add_argument("--since", type=_parse_since, default=None,
                   help="override the watermark, e.g. 2026-09-01 or "
                        "2026-09-01T12:00:00 (UTC)")
    p.add_argument("--lookback-days", type=int, default=1,
                   help="days before the watermark to recompute for rollups, to pick "
                        "up late-arriving events (default: 1)")
    p.add_argument("--dry-run", action="store_true",
                   help="read, dedupe and report row counts without writing")

    # Iceberg target
    p.add_argument("--catalog", default=env("CATALOG", "js_financial_ice"))
    p.add_argument("--db", default=env("DB", "ingestion"),
                   help="schema the batch targets are written to; separate from the "
                        "streaming job's schema (default: ingestion)")
    p.add_argument("--catalog-type", default=env("CATALOG_TYPE", "glue"),
                   choices=["glue", "hadoop"],
                   help="glue for AIDP; hadoop for a local warehouse (default: glue)")
    # Only a fallback: a Glue database created with an explicit location (as
    # both banking and ingestion are) overrides it for tables in that schema.
    p.add_argument("--warehouse", default=env("WAREHOUSE", "s3://js-demo/warehouse/ingestion"))
    p.add_argument("--glue-endpoint", default=env(
        "GLUE_ENDPOINT",
        "http://managed-metastore.ddae.svc.cluster.local:8080/api/v1/glue"))
    p.add_argument("--glue-region", default=env("GLUE_REGION", "us-east-1"))
    p.add_argument("--glue-catalog-id", default=env("GLUE_CATALOG_ID", "js_banking_ice"))
    p.add_argument("--s3-endpoint", default=env("S3_ENDPOINT", "http://172.18.11.31:9020"))

    # Source 1 — AIDP data catalog over JDBC
    p.add_argument("--customers-reader", default=env("CUSTOMERS_READER", "starburst"),
                   choices=["starburst", "mysql"],
                   help="read customers through the AIDP data catalog (default) or "
                        "straight from MySQL")
    p.add_argument("--aidp-host", default=env("AIDP_HOST", "ddae.lab9bgp.com"))
    p.add_argument("--aidp-port", type=int, default=int(env("AIDP_PORT", "443")))
    p.add_argument("--aidp-verify-tls", dest="aidp_verify_tls",
                   action=argparse.BooleanOptionalAction,
                   default=env("AIDP_VERIFY_TLS", "true").lower() != "false",
                   help="verify the coordinator's certificate (default: true). "
                        "--no-aidp-verify-tls is for lab testing only")
    p.add_argument("--aidp-truststore", default=env("AIDP_TRUSTSTORE", ""),
                   help="JKS/PKCS12 trust store holding the private CA; password from "
                        "AIDP_TRUSTSTORE_PASSWORD")
    p.add_argument("--source-catalog", default=env("SOURCE_CATALOG", "js_mysql_customer360"))
    p.add_argument("--source-schema", default=env("SOURCE_SCHEMA", "customer360"))
    p.add_argument("--source-table", default=env("SOURCE_TABLE", "customers"))
    p.add_argument("--mysql-host", default=env("MYSQL_HOST", "172.18.1.177"))
    p.add_argument("--mysql-port", type=int, default=int(env("MYSQL_PORT", "3306")))
    p.add_argument("--mysql-db", default=env("MYSQL_DATABASE", "customer360"))
    p.add_argument("--mysql-table", default=env("MYSQL_TABLE", "customers"))

    # Source 2 — landing zone
    p.add_argument("--accounts-path", default=env(
        "ACCOUNTS_PATH", "s3a://js-demo/landing/accounts"))
    p.add_argument("--accounts-pattern", default=env("ACCOUNTS_PATTERN", "*.csv"))

    # Source 3 — Iceberg rollup. Lives in the streaming job's schema, which is
    # why it has its own --payments-db rather than reusing --db.
    p.add_argument("--payments-db", default=env("PAYMENTS_DB", "banking"))
    p.add_argument("--payments-table", default=env("PAYMENTS_TABLE", "payment_transactions"))

    # Tuning
    p.add_argument("--num-partitions", type=int, default=1,
                   help="parallel JDBC readers; >1 wraps the query in a shard subquery")
    p.add_argument("--fetch-size", type=int, default=5000, help="JDBC rows per round trip")
    p.add_argument("--shuffle-partitions", type=int, default=8)

    cfg = p.parse_args(argv)
    # Batch identity is stamped on every ingested row and every audit entry, so
    # it is fixed once here instead of being recomputed per source.
    cfg.batch_ts = datetime.now(timezone.utc)
    cfg.batch_id = f"{cfg.batch_ts:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    return cfg


def _parse_since(value: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"invalid timestamp {value!r}; use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")


def main(argv=None) -> int:
    cfg = parse_args(argv)
    if cfg.since and cfg.mode == "full":
        sys.exit("--since applies to --mode incremental only")

    batch_id = cfg.batch_id
    spark = build_session(cfg)
    spark.sparkContext.setLogLevel("WARN")

    selected = [SOURCES[name] for name in (SOURCES if cfg.source == "all" else [cfg.source])]
    print(f"Spark {spark.version} | batch {batch_id} | {cfg.catalog}.{cfg.db} | "
          f"mode={cfg.mode}{' | DRY RUN' if cfg.dry_run else ''} | "
          f"sources={', '.join(s.name for s in selected)}", flush=True)

    audit_rows, failed = [], []
    for source in selected:
        try:
            audit_rows.append(ingest(spark, cfg, source))
        except Exception as exc:                                  # noqa: BLE001
            # One unreachable source must not abandon the others, but the run
            # still has to exit non-zero so `instance status` shows the failure.
            print(f"  FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
            now = datetime.now(timezone.utc)
            audit_rows.append((
                batch_id, source.name, source.target.table, cfg.mode, None,
                None, None, None, None, now, now, 0.0, "FAILED",
                f"{type(exc).__name__}: {exc}"[:1000],
            ))
            failed.append(source.name)

    write_audit(spark, cfg, audit_rows)
    print(f"\nbatch {batch_id}: {len(audit_rows) - len(failed)} ok, {len(failed)} failed",
          flush=True)
    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
