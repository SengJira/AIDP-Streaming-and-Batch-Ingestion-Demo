#!/usr/bin/env python3
"""Banking streaming pipeline — Kafka -> Spark Structured Streaming -> Iceberg.

Standalone equivalent of notebooks/banking_streaming_pyspark_hdfs.ipynb, built
to run on Dell AIDP via the dell-data-processing-engine CLI:

    dell-data-processing-engine submit ... banking_streaming_job.py --stream all

Why a job instead of the notebook: the driver runs in a cluster pod sized by the
resource pool, not inside the 1 GB notebook container that was being OOM-killed.

Configuration
-------------
Endpoints are command-line arguments (each also falls back to an environment
variable). They are NOT passed as Spark properties because the AIDP CLI rejects
`spark.kubernetes.driverEnv.*` as reserved configuration.

Credentials are read from the environment only, so they can be injected by
`uploads create-secret` + `--uploaded-secrets` rather than appearing in the
submit command, which the CLI persists by default:

    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY   Glue metastore credentials.
                                                Read directly by the AWS SDK
                                                credential chain; Iceberg's
                                                GlueCatalog does NOT accept
                                                catalog-scoped client keys
                                                (apache/iceberg#10614).
    S3_ACCESS_KEY / S3_SECRET_KEY               Object storage credentials.
"""
from __future__ import annotations

import argparse
import os
import signal
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg, col, count, current_timestamp, from_json, max as smax, sum as ssum,
    to_date, when, window,
)
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType, TimestampType,
)

# ── Schemas ─────────────────────────────────────────────────────────────────
PAYMENT_SCHEMA = StructType([
    StructField("event_id",          StringType(),    True),
    StructField("event_type",        StringType(),    True),
    StructField("timestamp",         TimestampType(), True),
    StructField("transaction_id",    StringType(),    True),
    StructField("account_id",        StringType(),    True),
    StructField("amount",            DoubleType(),    True),
    StructField("currency",          StringType(),    True),
    StructField("channel",           StringType(),    True),
    StructField("merchant_category", StringType(),    True),
    StructField("status",            StringType(),    True),
    StructField("city",              StringType(),    True),
    StructField("country",           StringType(),    True),
    StructField("latitude",          DoubleType(),    True),
    StructField("longitude",         DoubleType(),    True),
    StructField("is_international",  BooleanType(),   True),
    StructField("customer_tier",     StringType(),    True),
    StructField("risk_score",        DoubleType(),    True),
])

# fraud.signals and customer.events share one consumer, so this is the union of
# both payloads (fields absent from a given message parse as NULL).
EVENT_SCHEMA = StructType([
    StructField("event_id",        StringType(),    True),
    StructField("event_type",      StringType(),    True),
    StructField("timestamp",       TimestampType(), True),
    StructField("account_id",      StringType(),    True),
    # fraud
    StructField("fraud_type",      StringType(),    True),
    StructField("ml_score",        DoubleType(),    True),
    StructField("action",          StringType(),    True),
    StructField("case_id",         StringType(),    True),
    # aml
    StructField("alert_type",      StringType(),    True),
    StructField("risk_band",       StringType(),    True),
    StructField("sar_required",    BooleanType(),   True),
    StructField("total_amount_7d", DoubleType(),    True),
    # login / profile
    StructField("channel",         StringType(),    True),
    StructField("auth_method",     StringType(),    True),
    StructField("login_success",   BooleanType(),   True),
    StructField("new_device",      BooleanType(),   True),
])

def event_routes():
    """Routing rules for stream 3: (table, predicate, columns).

    Built lazily — col() needs an active JVM, so calling it at module import
    time (before the SparkSession exists) fails with a bare AssertionError.
    """
    return (
        ("fraud_alerts", col("event_type") == "FRAUD_SIGNAL",
         ["event_id", "event_type", "timestamp", "account_id",
          "fraud_type", "ml_score", "action", "case_id", "event_date"]),
        ("aml_alerts", col("event_type") == "AML_ALERT",
         ["event_id", "event_type", "timestamp", "account_id",
          "alert_type", "risk_band", "sar_required", "total_amount_7d", "event_date"]),
        ("customer_events",
         col("event_type").isin("CUSTOMER_LOGIN", "PROFILE_CHANGE", "ACCOUNT_OPEN"),
         ["event_id", "event_type", "timestamp", "account_id",
          "channel", "auth_method", "login_success", "new_device", "event_date"]),
    )


def build_session(cfg) -> SparkSession:
    """Session with the Iceberg/Glue/S3 catalog wiring.

    Master, driver memory and executor resources are deliberately NOT set here:
    on AIDP they come from the submit flags and the resource pool.
    """
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

    catalog = cfg.catalog
    return (
        SparkSession.builder
        .appName(f"BankingStreamingJob-{cfg.stream}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        # Iceberg catalog -> Glue-compatible managed metastore
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", cfg.warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.glue.id", cfg.glue_catalog_id)
        .config(f"spark.sql.catalog.{catalog}.glue.endpoint", cfg.glue_endpoint)
        .config(f"spark.sql.catalog.{catalog}.client.region", cfg.glue_region)

        # Table data on object storage (Iceberg S3FileIO)
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", cfg.s3_endpoint)
        .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", s3_key)
        .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", s3_secret)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.http-client.type", "apache")
        .config(f"spark.sql.catalog.{catalog}.http-client.apache.connection-timeout-ms", "5000")
        .config(f"spark.sql.catalog.{catalog}.http-client.apache.socket-timeout-ms", "30000")

        # Streaming checkpoints go through Hadoop's FileSystem layer, not
        # S3FileIO, so S3A needs its own configuration.
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")

        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .getOrCreate()
    )


def read_payments(spark: SparkSession, cfg) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_brokers)
        .option("subscribe", "payments.raw")
        .option("startingOffsets", cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.max_offsets)
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), PAYMENT_SCHEMA).alias("d"))
        .select("d.*")
        .filter(col("event_id").isNotNull())
        .withColumn("ingested_at", current_timestamp())
        .withColumn("event_date", to_date(col("timestamp")))
    )


def start_payments(spark: SparkSession, cfg):
    """Stream 1 — raw payment transactions, append only."""
    return (
        read_payments(spark, cfg).writeStream
        .format("iceberg")
        .outputMode("append")
        .queryName("payments_to_iceberg")
        .trigger(processingTime=f"{cfg.trigger_seconds} seconds")
        .option("checkpointLocation", f"{cfg.checkpoint}/payment_transactions_v3")
        # payment_transactions is partitioned by (event_date, city); fanout lets
        # one batch write several partitions without pre-clustering the rows.
        .option("fanout-enabled", "true")
        .toTable(f"{cfg.catalog}.{cfg.db}.payment_transactions")
    )


def start_aggregates(spark: SparkSession, cfg):
    """Stream 2 — 5-minute tumbling window aggregates (stateful)."""
    pay_agg = (
        read_payments(spark, cfg)
        .withWatermark("timestamp", "2 minutes")
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("city"), col("channel"), col("merchant_category"),
        )
        .agg(
            count("*").alias("txn_count"),
            ssum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            smax("amount").alias("max_amount"),
            ssum(when(col("status") == "APPROVED", 1).otherwise(0)).alias("approved_count"),
            ssum(when(col("status") == "DECLINED", 1).otherwise(0)).alias("declined_count"),
            ssum(when(col("is_international"), 1).otherwise(0)).alias("intl_count"),
            avg("risk_score").alias("avg_risk_score"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"), col("channel"), col("merchant_category"),
            col("txn_count"), col("total_amount"), col("avg_amount"), col("max_amount"),
            col("approved_count"), col("declined_count"), col("intl_count"),
            col("avg_risk_score"),
            to_date(col("window.start")).alias("event_date"),
        )
    )

    return (
        pay_agg.writeStream
        .format("iceberg")
        .outputMode("append")               # watermark required for append mode
        .queryName("payment_agg_to_iceberg")
        .trigger(processingTime=f"{cfg.trigger_seconds * 2} seconds")
        .option("checkpointLocation", f"{cfg.checkpoint}/payment_agg_v2")
        .option("fanout-enabled", "true")
        .toTable(f"{cfg.catalog}.{cfg.db}.payment_agg")
    )


def start_events(spark: SparkSession, cfg):
    """Stream 3 — fan one consumer out to fraud / AML / customer tables."""
    events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_brokers)
        .option("subscribe", "fraud.signals,customer.events")
        .option("startingOffsets", cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", max(cfg.max_offsets // 2, 500))
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_date", to_date(col("timestamp")))
    )

    routes = event_routes()

    def route_to_iceberg(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        # Scanned once per route, so persist it; the default MEMORY_AND_DISK
        # level lets a large batch spill instead of pressuring the heap.
        batch_df.persist()
        try:
            for table, predicate, columns in routes:
                part = batch_df.filter(predicate).select(*columns)
                if not part.isEmpty():
                    part.writeTo(f"{cfg.catalog}.{cfg.db}.{table}").append()
                    print(f"  [batch {batch_id}] -> {table}", flush=True)
        finally:
            batch_df.unpersist()

    return (
        events.writeStream
        .foreachBatch(route_to_iceberg)
        .queryName("events_to_iceberg")
        .trigger(processingTime=f"{cfg.trigger_seconds} seconds")
        .option("checkpointLocation", f"{cfg.checkpoint}/fraud_customer")
        .start()
    )


STREAMS = {
    "payments": start_payments,
    "aggregates": start_aggregates,
    "events": start_events,
}


def parse_args(argv=None):
    def env(name, default):
        return os.environ.get(name, default)

    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    p.add_argument("--stream", default="all", choices=["all", *STREAMS],
                   help="which query to run; one per submit gives isolated "
                        "failures and independent resources (default: all)")

    # Endpoints — arguments rather than Spark properties, because the AIDP CLI
    # rejects spark.kubernetes.driverEnv.* as reserved configuration.
    p.add_argument("--kafka-brokers", default=env("KAFKA_BROKERS", "172.18.1.177:9092"))
    p.add_argument("--glue-endpoint", default=env(
        "GLUE_ENDPOINT",
        "http://managed-metastore.ddae.svc.cluster.local:8080/api/v1/glue"))
    p.add_argument("--glue-region", default=env("GLUE_REGION", "us-east-1"))
    p.add_argument("--glue-catalog-id", default=env("GLUE_CATALOG_ID", "js_banking_ice"))
    p.add_argument("--s3-endpoint", default=env("S3_ENDPOINT", "http://172.18.11.31:9020"))
    p.add_argument("--warehouse", default=env("WAREHOUSE", "s3://js-demo/warehouse/banking"))
    # Checkpoints hold broker offsets, so they are namespaced per Kafka
    # deployment. The 10.246.25.115 broker gets its own root; the old
    # s3://js-demo/warehouse/banking/checkpoints tree is left untouched.
    p.add_argument("--checkpoint", default=env(
        "CHECKPOINT", "s3://js-demo/warehouse/banking/checkpoints-10-246-25-115"))
    p.add_argument("--catalog", default=env("CATALOG", "js_financial_ice"))
    p.add_argument("--db", default=env("DB", "banking"))

    # Tuning
    p.add_argument("--trigger-seconds", type=int, default=30,
                   help="micro-batch interval; aggregates use 2x this")
    p.add_argument("--max-offsets", type=int, default=5000,
                   help="maxOffsetsPerTrigger for payments.raw (back-pressure)")
    p.add_argument("--shuffle-partitions", type=int, default=4,
                   help="keep near the Kafka partition count at demo scale")
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"],
                   help="only applies the first time a checkpoint is created")
    return p.parse_args(argv)


def main(argv=None) -> int:
    cfg = parse_args(argv)
    spark = build_session(cfg)
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark {spark.version} | {cfg.catalog}.{cfg.db} | kafka={cfg.kafka_brokers} | "
          f"s3={cfg.s3_endpoint} | glue={cfg.glue_endpoint}", flush=True)

    selected = list(STREAMS) if cfg.stream == "all" else [cfg.stream]
    queries = []
    for name in selected:
        query = STREAMS[name](spark, cfg)
        queries.append(query)
        print(f"started: {query.name} (id={query.id})", flush=True)

    # Terminate cleanly on the SIGTERM Kubernetes sends, so the in-flight
    # micro-batch commits and the checkpoint is left consistent. A hard-killed
    # driver can leave a partially written offset log on object storage.
    def shutdown(signum, _frame):
        print(f"signal {signum} — stopping {len(queries)} queries", flush=True)
        for q in queries:
            try:
                q.stop()
            except Exception as exc:                     # noqa: BLE001
                print(f"  error stopping {q.name}: {exc}", flush=True)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Raises if any query fails, so the pod exits non-zero and the failure shows
    # up in `instance status` instead of the job idling forever.
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
