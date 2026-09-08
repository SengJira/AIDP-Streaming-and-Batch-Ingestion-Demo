"""Unit tests for the batch ingestion job (jobs/batch_ingestion_job.py).

Offline: no Spark cluster, no AIDP endpoint, no MySQL. They cover the parts of
the job that are easy to get quietly wrong — the MERGE contract between reader
and target, watermark rendering, JDBC URL construction and credential hygiene.

pyspark is imported by the job module, so the whole file skips when it is not
installed (`pip install -r requirements-spark.txt`). The Spark-dependent
behaviour that these tests cannot reach — the MERGE itself, snapshot counting,
the incremental lookback — was verified against a real Iceberg warehouse; see
docs/BATCH_INGESTION_DEMO.md §"Verified behaviour".
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "jobs"))

pytest.importorskip("pyspark", reason="pip install -r requirements-spark.txt")

import batch_ingestion_job as job  # noqa: E402

WATERMARK = datetime(2026, 9, 1, 12, 30, 45, tzinfo=timezone.utc)


def cfg(**overrides):
    """Parsed defaults, overridden per test."""
    parsed = job.parse_args([])
    for key, value in overrides.items():
        setattr(parsed, key, value)
    return parsed


# --------------------------------------------------------------------------- #
# The MERGE contract: target columns, keys and DDL must agree
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("target", [job.DIM_CUSTOMER, job.DIM_ACCOUNT,
                                    job.FACT_PAYMENT_DAILY, job.AUDIT])
def test_target_columns_are_derived_from_its_ddl(target):
    """columns is what MERGE's UPDATE SET * / INSERT * match on by name."""
    assert target.columns, f"{target.table} exposes no columns"
    assert len(target.columns) == len(set(target.columns)), "duplicate column"
    for column in target.columns:
        assert re.fullmatch(r"[a-z][a-z0-9_]*", column), column
        assert re.search(rf"^\s*{column}\s+\S", target.ddl, re.MULTILINE)


@pytest.mark.parametrize("target", [job.DIM_CUSTOMER, job.DIM_ACCOUNT,
                                    job.FACT_PAYMENT_DAILY])
def test_every_target_carries_the_audit_columns(target):
    for column in ("source_system", "batch_id", "ingested_at"):
        assert column in target.columns, f"{target.table} is missing {column}"


def test_merge_key_must_exist_in_the_ddl():
    with pytest.raises(ValueError, match="merge key"):
        job.Target(table="broken", keys=("nope",), ddl="id string")


def test_watermark_and_ordering_columns_exist_in_their_target():
    for target in (job.DIM_CUSTOMER, job.DIM_ACCOUNT, job.FACT_PAYMENT_DAILY):
        for column in (target.watermark_column, target.ordering_column):
            assert column is None or column in target.columns


@pytest.mark.parametrize("created,updated", [
    ("created_at", "updated_at"),
    (job.TRINO_UTC_TIMESTAMP.format(column="created_at"),
     job.TRINO_UTC_TIMESTAMP.format(column="updated_at")),
])
def test_customer_projection_covers_every_source_column_of_dim_customer(created, updated):
    """A column added to the target without a reader change would insert nulls."""
    projected = {
        (part.split(" AS ")[-1] if " AS " in part else part).strip()
        for part in job.customer_projection(created, updated).replace("\n", " ").split(",")
    }
    # batch_id / ingested_at are added by ingest(), not by the reader.
    expected = set(job.DIM_CUSTOMER.columns) - {"source_system", "batch_id", "ingested_at"}
    assert expected == projected


def test_accounts_csv_schema_matches_the_generated_landing_file():
    """The landing schema is explicit, so it must track the generator's header."""
    from scripts.generate_customer360 import ACCOUNT_POOL_COLUMNS

    assert [f.name for f in job.ACCOUNTS_CSV_SCHEMA.fields] == list(ACCOUNT_POOL_COLUMNS)


def test_audit_schema_matches_the_audit_table_ddl():
    assert [f.name for f in job.AUDIT_SCHEMA.fields] == list(job.AUDIT.columns)


def test_sources_are_registered_with_distinct_targets():
    assert set(job.SOURCES) == {"customers", "accounts", "payments_daily"}
    tables = [source.target.table for source in job.SOURCES.values()]
    assert len(tables) == len(set(tables))


# --------------------------------------------------------------------------- #
# Watermarks
# --------------------------------------------------------------------------- #
def test_watermark_literal_is_rendered_in_utc_with_millisecond_precision():
    assert job._timestamp_literal(WATERMARK) == "2026-09-01 12:30:45.000"


def test_naive_watermark_is_treated_as_utc_not_local_time():
    """Iceberg returns naive datetimes; the session timezone is pinned to UTC.

    Interpreting them as local time would shift the incremental window by the
    host's offset and silently skip or re-read rows.
    """
    naive = WATERMARK.replace(tzinfo=None)
    assert job._timestamp_literal(naive) == job._timestamp_literal(WATERMARK)


def test_aware_watermark_in_another_zone_is_converted():
    from datetime import timedelta

    bangkok = WATERMARK.astimezone(timezone(timedelta(hours=7)))
    assert job._timestamp_literal(bangkok) == "2026-09-01 12:30:45.000"


@pytest.mark.parametrize("value,expected", [
    ("2026-09-01", datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ("2026-09-01T12:30:45", datetime(2026, 9, 1, 12, 30, 45, tzinfo=timezone.utc)),
    ("2026-09-01 12:30:45", datetime(2026, 9, 1, 12, 30, 45, tzinfo=timezone.utc)),
])
def test_since_accepts_date_and_timestamp_forms(value, expected):
    assert job._parse_since(value) == expected


def test_since_rejects_garbage():
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        job._parse_since("last tuesday")


# --------------------------------------------------------------------------- #
# Defaults and CLI
# --------------------------------------------------------------------------- #
def test_defaults_target_aidp_and_incremental_upserts():
    parsed = job.parse_args([])
    assert parsed.source == "all"
    assert parsed.mode == "incremental"
    assert parsed.catalog_type == "glue"          # AIDP, not a local warehouse
    assert parsed.customers_reader == "starburst"  # through the data catalog
    assert parsed.catalog == "js_financial_ice"
    assert parsed.db == "ingestion"
    assert parsed.dry_run is False
    assert parsed.aidp_verify_tls is True          # TLS on unless asked otherwise
    assert parsed.lookback_days >= 1               # late events must be recomputed


def test_batch_identity_is_fixed_once_per_run():
    parsed = job.parse_args([])
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{8}", parsed.batch_id)
    assert parsed.batch_ts.tzinfo is not None
    assert job.parse_args([]).batch_id != parsed.batch_id


def test_source_choices_are_the_registered_sources():
    for name in job.SOURCES:
        assert job.parse_args(["--source", name]).source == name
    with pytest.raises(SystemExit):
        job.parse_args(["--source", "elasticsearch"])


def test_tls_verification_can_be_disabled_only_explicitly():
    assert job.parse_args(["--no-aidp-verify-tls"]).aidp_verify_tls is False


# --------------------------------------------------------------------------- #
# JDBC: URL shape and credential hygiene
# --------------------------------------------------------------------------- #
class FakeReader:
    """Records the options a reader would pass to Spark's JDBC source."""

    def __init__(self):
        self.captured: dict = {}

    def format(self, _name):
        return self

    def options(self, **kwargs):
        self.captured.update(kwargs)
        return self

    def option(self, key, value):
        self.captured[key] = value
        return self

    def load(self):
        return self

    def drop(self, *_args):
        return self


class FakeSpark:
    def __init__(self):
        self.read = FakeReader()


def _request(monkeypatch, env, **overrides):
    """JDBC options + SQL for a customer read, with only ``env`` set."""
    for name in ("AIDP_USERNAME", "AIDP_PASSWORD", "MYSQL_USER", "MYSQL_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    watermark = overrides.pop("watermark", None)
    options, sql, source_system = job.customers_request(cfg(**overrides), watermark)
    return {**options, "query": sql, "source_system": source_system}


def _jdbc_options(monkeypatch, **overrides):
    """What Spark's JDBC source actually receives, via a recording fake."""
    monkeypatch.setenv("AIDP_USERNAME", "u")
    monkeypatch.setenv("AIDP_PASSWORD", "pw")
    parsed = cfg(**overrides)
    options, sql, _ = job.customers_request(parsed, None)
    spark = FakeSpark()
    job._jdbc_read(spark, options, sql, parsed)
    return spark.read.captured


def test_starburst_reader_goes_through_the_data_catalog_over_tls(monkeypatch):
    options = _request(monkeypatch, {"AIDP_USERNAME": "jirawut", "AIDP_PASSWORD": "pw"})
    assert options["driver"] == job.TRINO_DRIVER
    assert options["url"].startswith("jdbc:trino://ddae.lab9bgp.com:443?SSL=true")
    # TLS verification stays on by default: no SSLVerification=NONE.
    assert "SSLVerification" not in options["url"]
    assert "js_mysql_customer360.customer360.customers" in options["query"]
    assert options["source_system"] == "js_mysql_customer360.customer360.customers"


def test_starburst_reader_can_be_pointed_at_a_truststore(monkeypatch):
    monkeypatch.setenv("AIDP_TRUSTSTORE_PASSWORD", "changeit")
    options = _request(
        monkeypatch, {"AIDP_USERNAME": "u", "AIDP_PASSWORD": "pw"},
        aidp_truststore="/etc/ssl/titan.p12")
    assert "SSLTrustStorePath=/etc/ssl/titan.p12" in options["url"]
    assert "SSLVerification=NONE" not in options["url"]


def test_disabling_verification_is_the_only_way_to_skip_tls_checks(monkeypatch):
    options = _request(
        monkeypatch, {"AIDP_USERNAME": "u", "AIDP_PASSWORD": "pw"}, aidp_verify_tls=False)
    assert "SSLVerification=NONE" in options["url"]


def test_credentials_never_appear_in_the_sql_sent_to_the_source(monkeypatch):
    options = _request(
        monkeypatch, {"AIDP_USERNAME": "jirawut", "AIDP_PASSWORD": "s3cret"})
    assert "s3cret" not in options["query"]
    assert options["password"] == "s3cret"          # only in the JDBC property


def test_incremental_read_filters_on_the_source_change_column(monkeypatch):
    options = _request(
        monkeypatch, {"AIDP_USERNAME": "u", "AIDP_PASSWORD": "pw"}, watermark=WATERMARK)
    assert ("WHERE CAST(updated_at AT TIME ZONE 'UTC' AS timestamp(6)) > "
            "TIMESTAMP '2026-09-01 12:30:45.000'") in options["query"]


def test_trino_timestamps_are_cast_because_spark_cannot_map_them(monkeypatch):
    """Regression: MySQL TIMESTAMP reaches Trino as `timestamp(0) with time zone`.

    Spark's JDBC dialect has no mapping for TIMESTAMP_WITH_TIMEZONE, so reading
    the column raw fails the whole source with
    `[UNRECOGNIZED_SQL_TYPE] ... id: TIMESTAMP_WITH_TIMEZONE` — which is exactly
    how the first AIDP run of this source died. Both audit timestamps must be
    converted to UTC and cast to a plain timestamp in the query itself.
    """
    query = _request(monkeypatch, {"AIDP_USERNAME": "u", "AIDP_PASSWORD": "pw"})["query"]
    for column, alias in (("created_at", "src_created_at"), ("updated_at", "src_updated_at")):
        assert (f"CAST({column} AT TIME ZONE 'UTC' AS timestamp(6)) AS {alias}") in query
    # The bare column must not survive anywhere in the SELECT list.
    assert " created_at AS" not in query and " updated_at AS" not in query


def test_mysql_reader_does_not_cast_because_connector_j_maps_timestamps(monkeypatch):
    query = _request(
        monkeypatch, {"MYSQL_USER": "u", "MYSQL_PASSWORD": "pw"},
        customers_reader="mysql")["query"]
    assert "created_at AS src_created_at" in query
    assert "AT TIME ZONE" not in query


def test_full_read_has_no_predicate(monkeypatch):
    options = _request(monkeypatch, {"AIDP_USERNAME": "u", "AIDP_PASSWORD": "pw"})
    assert "WHERE" not in options["query"]


def test_mysql_reader_pins_utc_and_uses_connector_j(monkeypatch):
    options = _request(
        monkeypatch, {"MYSQL_USER": "app", "MYSQL_PASSWORD": "pw"},
        customers_reader="mysql")
    assert options["driver"] == job.MYSQL_DRIVER
    assert "connectionTimeZone=UTC" in options["url"]
    assert "customers" in options["query"]
    # MySQL's own timestamp literal form, not Trino's TIMESTAMP '...'.
    incremental = _request(
        monkeypatch, {"MYSQL_USER": "app", "MYSQL_PASSWORD": "pw"},
        customers_reader="mysql", watermark=WATERMARK)
    assert "WHERE updated_at > '2026-09-01 12:30:45.000'" in incremental["query"]


def test_missing_credentials_fail_this_source_without_killing_the_batch(monkeypatch):
    """RuntimeError, not SystemExit.

    ingest() catches Exception per source so an unreachable source is recorded
    as FAILED and the rest of the batch still runs; SystemExit would escape that
    handler and abandon the other sources. This is a real regression: the first
    AIDP run exited at `customers` and never attempted the other two.
    """
    with pytest.raises(RuntimeError, match="AIDP_USERNAME"):
        _request(monkeypatch, {})
    with pytest.raises(RuntimeError, match="MYSQL_USER"):
        _request(monkeypatch, {}, customers_reader="mysql")
    for exc_type in (SystemExit, KeyboardInterrupt):
        assert not issubclass(exc_type, Exception)  # documents why it matters


def test_parallel_reads_use_a_shard_subquery_because_query_cannot(monkeypatch):
    """Spark rejects `query` + `partitionColumn`; the shard subquery is the fix."""
    options = _jdbc_options(monkeypatch, num_partitions=4)
    assert "query" not in options
    assert options["partitionColumn"] == "_shard"
    assert options["numPartitions"] == "4"
    assert "% 4" in options["dbtable"]
    # The internal marker must never reach Spark as a JDBC option.
    assert "__shard_expr__" not in options


def test_single_partition_read_avoids_the_subquery_wrapper(monkeypatch):
    options = _jdbc_options(monkeypatch, num_partitions=1)
    assert "dbtable" not in options
    assert "__shard_expr__" not in options
    assert options["query"].startswith("SELECT")


# --------------------------------------------------------------------------- #
# Generated SQL
# --------------------------------------------------------------------------- #
class RecordingSpark:
    """Captures the SQL the job issues, without executing it."""

    def __init__(self):
        self.statements: list[str] = []

    def sql(self, statement):
        self.statements.append(" ".join(statement.split()))
        return self


class FakeStaged:
    def createOrReplaceTempView(self, _name):
        pass


def test_merge_upserts_on_the_key_and_guards_against_stale_rows():
    spark = RecordingSpark()
    job.merge(spark, "cat.db.dim_customer", FakeStaged(), job.DIM_CUSTOMER)
    statement = spark.statements[0]
    assert statement.startswith("MERGE INTO cat.db.dim_customer t USING stg_dim_customer s")
    assert "ON t.customer_id = s.customer_id" in statement
    assert "WHEN MATCHED AND s.src_updated_at >= t.src_updated_at THEN UPDATE SET *" in statement
    assert "WHEN NOT MATCHED THEN INSERT *" in statement
    # No DELETE clause: a row disappearing from the source must not silently
    # remove history from the lakehouse.
    assert "DELETE" not in statement


def test_merge_on_a_composite_key_uses_every_key_column():
    spark = RecordingSpark()
    job.merge(spark, "cat.db.fact_payment_daily", FakeStaged(), job.FACT_PAYMENT_DAILY)
    statement = spark.statements[0]
    assert "ON t.event_date = s.event_date AND t.account_id = s.account_id" in statement


def test_batch_targets_live_in_their_own_schema_not_the_streaming_one():
    """The rollup reads banking.payment_transactions but writes to ingestion."""
    parsed = job.parse_args([])
    assert parsed.db == "ingestion"
    assert parsed.payments_db == "banking"
    assert parsed.db != parsed.payments_db


def test_ensure_table_is_idempotent_and_partitions_the_fact_table():
    spark = RecordingSpark()
    name = job.ensure_table(spark, cfg(), job.FACT_PAYMENT_DAILY)
    assert name == "js_financial_ice.ingestion.fact_payment_daily"
    schema, create = spark.statements
    assert schema == "CREATE SCHEMA IF NOT EXISTS js_financial_ice.ingestion"
    assert create.startswith(
        "CREATE TABLE IF NOT EXISTS js_financial_ice.ingestion.fact_payment_daily")
    assert "USING iceberg PARTITIONED BY (event_date)" in create
    assert ", )" not in create          # trailing comma from the DDL block


def test_dimension_tables_are_not_partitioned():
    spark = RecordingSpark()
    job.ensure_table(spark, cfg(), job.DIM_CUSTOMER)
    assert "PARTITIONED BY" not in spark.statements[1]


# --------------------------------------------------------------------------- #
# Snapshot accounting
# --------------------------------------------------------------------------- #
class FakeSnapshots:
    def __init__(self, row):
        self._row = row

    def orderBy(self, *_args, **_kwargs):
        return self

    def select(self, *_args):
        return self

    def first(self):
        return self._row


class SnapshotSpark:
    def __init__(self, row):
        self._row = row

    def table(self, _name):
        return FakeSnapshots(self._row)


def test_copy_on_write_updates_are_reported_as_updates_not_inserts():
    """A MERGE update rewrites the row: one added and one deleted record."""
    row = {"snapshot_id": 7, "summary": {"added-records": "10000",
                                         "deleted-records": "10000"}}
    counts = job.snapshot_counts(SnapshotSpark(row), "t", previous=6)
    assert counts == {"snapshot_id": 7, "inserted": 0, "updated": 10000}


def test_mixed_batch_splits_inserts_from_updates():
    row = {"snapshot_id": 9, "summary": {"added-records": "150",
                                         "deleted-records": "100"}}
    counts = job.snapshot_counts(SnapshotSpark(row), "t", previous=8)
    assert counts == {"snapshot_id": 9, "inserted": 50, "updated": 100}


def test_a_merge_that_committed_nothing_reports_zeroes():
    """Iceberg does not create a snapshot when no row changed."""
    row = {"snapshot_id": 5, "summary": {"added-records": "1"}}
    counts = job.snapshot_counts(SnapshotSpark(row), "t", previous=5)
    assert counts == {"snapshot_id": 5, "inserted": 0, "updated": 0}


def test_first_load_of_an_empty_table_has_no_snapshot():
    counts = job.snapshot_counts(SnapshotSpark(None), "t", previous=None)
    assert counts == {"snapshot_id": None, "inserted": 0, "updated": 0}


# --------------------------------------------------------------------------- #
# Repository consistency: the SQL reference must match the job's model
# --------------------------------------------------------------------------- #
def test_reference_ddl_declares_the_same_tables_and_columns_as_the_job():
    """sql/Iceberg_Batch_Tables.sql is what an admin runs from Starburst."""
    sql = (REPO / "sql" / "Iceberg_Batch_Tables.sql").read_text()
    for target in (job.DIM_CUSTOMER, job.DIM_ACCOUNT, job.FACT_PAYMENT_DAILY, job.AUDIT):
        block = re.search(
            rf"CREATE TABLE IF NOT EXISTS \S+\.{target.table} \((.*?)\n\)\s*WITH",
            sql, re.DOTALL)
        assert block, f"{target.table} is missing from the reference DDL"
        declared = [
            line.split()[0] for line in
            (raw.strip() for raw in block.group(1).splitlines())
            if line and not line.startswith("--")
        ]
        assert declared == list(target.columns), target.table


def test_submit_script_passes_no_credentials_on_the_command_line():
    """The CLI persists submit configuration, so secrets must arrive as env."""
    script = (REPO / "scripts" / "submit_batch_aidp.sh").read_text()
    args_block = script.split('args+=(\n  "$APP_PATH"')[1]
    for secret in ("AIDP_PASSWORD", "MYSQL_PASSWORD", "AWS_SECRET_ACCESS_KEY"):
        assert secret not in args_block, f"{secret} is passed to the job as an argument"
    assert "--uploaded-secrets" in script
    assert "--save-configuration=false" in script
