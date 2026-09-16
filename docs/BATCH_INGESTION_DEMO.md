# AIDP Batch Ingestion Demo — three sources into one Iceberg lakehouse

Demonstrates the **Spark engine on Dell AIDP** ingesting data in batch from three
different kinds of source into Apache Iceberg tables, with idempotent upserts and
a run log. It is the batch half of the demo: `jobs/banking_streaming_job.py`
keeps the event tables live from Kafka, and `jobs/batch_ingestion_job.py` loads
the reference and rollup tables that queries join against.

```
  AIDP data catalog                Object-storage landing zone      Iceberg (streaming)
  js_mysql_customer360             s3a://js-demo/landing/accounts   banking.payment_transactions
  .customer360.customers           customer_accounts_*.csv                 |
        | (Trino JDBC)                      | (CSV, full snapshot)         | (rollup)
        v                                   v                             v
  +--------------------------------------------------------------------------------+
  |     Spark on AIDP - jobs/batch_ingestion_job.py                                 |
  |     read -> dedupe on the merge key -> MERGE INTO Iceberg -> audit row          |
  +--------------------------------------------------------------------------------+
        |                                   |                             |
        v                                   v                             v
   ingestion.dim_customer            ingestion.dim_account        ingestion.fact_payment_daily
                            ingestion.ingestion_audit  (one row per source per batch)
```

| Source | Kind | Target | Read strategy |
| --- | --- | --- | --- |
| `customers` | Starburst / AIDP data catalog over JDBC | `dim_customer` | Incremental on `updated_at` |
| `accounts` | CSV files in object storage | `dim_account` | Full snapshot every run |
| `payments_daily` | An existing Iceberg table | `fact_payment_daily` | Incremental by `event_date` with a lookback |

Targets are written to **`js_financial_ice.ingestion`** (`--db`, default
`ingestion`). The rollup reads `banking.payment_transactions` from the streaming
job's schema through its own `--payments-db`, so the two schemas stay separate
and only that one cross-schema read connects them.

---

## 1. Why these three

Together they cover the three source shapes almost every real batch pipeline
has, and each one raises a different problem the job has to solve:

* **The governed catalog.** `customers` is read through
  `js_mysql_customer360` — the same catalog the Streamlit app queries — rather
  than by opening a second connection to MySQL. One access path, one set of
  read-only credentials, one audit trail. The catalog is reached with the Trino
  JDBC driver, so Spark benefits from connector pushdown on the source side.
  (`--customers-reader mysql` bypasses it when the coordinator is unavailable —
  it reads `172.18.1.177:3306` directly, the AIDP-subnet interface of the
  dual-homed demo host; the cluster cannot route to `10.246.25.x`.)
* **The landing zone.** `accounts` has no change column, so there is nothing to
  read incrementally. It is loaded as a full snapshot and made cheap by the
  upsert: a re-drop of the same file refreshes rows instead of duplicating them.
* **The lakehouse itself.** `payments_daily` reads the Iceberg table the
  streaming job writes and rolls it up per account and day — the batch layer
  serving queries the streaming layer should not be asked to answer.

Adding a fourth source means writing one reader function and one `Target`; the
read → dedupe → MERGE → audit path is shared.

---

## 2. Idempotency: the property the whole design turns on

Every target is written with `MERGE INTO ... WHEN MATCHED THEN UPDATE SET * WHEN
NOT MATCHED THEN INSERT *` on a declared merge key:

| Target | Merge key |
| --- | --- |
| `dim_customer` | `customer_id` |
| `dim_account` | `account_id` |
| `fact_payment_daily` | `(event_date, account_id)` |

That makes a re-run safe, which is what makes the job **retryable**: a failed or
half-finished batch is fixed by running it again, with no compensating cleanup
and no risk of double counting. Measured on this environment:

| Run | rows_read | rows_inserted | rows_updated |
| --- | --- | --- | --- |
| 1st `--source accounts --mode full` | 10,000 | 10,000 | 0 |
| 2nd, identical | 10,000 | **0** | 10,000 |

Three things make it work:

1. **Dedupe before the merge.** The staged batch is reduced to one row per key
   (`row_number()` ordered by the source's change column, newest first).
   Without it, two versions of one key in the same batch abort the MERGE with
   *"a single row from the target matched multiple rows from the source"* —
   Iceberg refuses ambiguous updates rather than picking one.
2. **A stale-row guard.** `WHEN MATCHED AND s.src_updated_at >= t.src_updated_at`
   stops a replayed old batch from overwriting a newer row.
3. **No `DELETE` clause.** A row disappearing from the source does not remove
   history from the lakehouse.

`fact_payment_daily` is recomputed rather than accumulated: the merge key is the
day, so re-running a day replaces its totals instead of adding to them. This is
why the lookback window is safe.

---

## 3. Incremental loads without a watermark store

`--mode incremental` (the default) reads the high-water mark **from the target
table itself**, so there is no external state that can be lost or drift:

| Source | Watermark | Predicate sent to the source |
| --- | --- | --- |
| `customers` | `MAX(src_updated_at)` | `WHERE updated_at > TIMESTAMP '...'` |
| `payments_daily` | `MAX(last_txn_at)` | `WHERE event_date >= watermark - lookback` |
| `accounts` | — | none: full snapshot |

The **lookback matters**. The streaming job writes late-arriving events into
days that were already rolled up, so an incremental run recomputes a trailing
window (`--lookback-days`, default 1) rather than only days after the watermark.

`--mode full` re-reads everything and upserts it; `--since 2026-09-01` overrides
the watermark for a targeted backfill.

**Timestamps are UTC end to end.** The session timezone is pinned to UTC, the
watermark read back from Iceberg is tagged UTC, and the MySQL JDBC URL sets
`connectionTimeZone=UTC` (the container runs `--default-time-zone=+00:00`). A
session in local time would silently shift the incremental window and skip rows.

---

## 4. Prerequisites

* The `dell-data-processing-engine` CLI, logged in, with an uploaded secret set
* `js_mysql_customer360` catalog configured on AIDP — see
  [FEDERATED_QUERY_DEMO.md](FEDERATED_QUERY_DEMO.md) §9
* S3 credentials for the jar and landing buckets
* For `payments_daily`: `banking.payment_transactions` must exist (the streaming
  job creates and fills it)

### One-time: stage the drivers and the landing file

The AIDP Spark image ships Iceberg, hadoop-aws and aws-java-sdk-bundle but **no
JDBC drivers**, so reading the data catalog fails with `No suitable driver`.
They are too large for `--file-upload` (1 MB cap), so they go in object storage
and are fetched at startup over `s3a://`:

```bash
pip install -r requirements-spark.txt      # boto3
S3_ACCESS_KEY=... S3_SECRET_KEY=... ./scripts/stage_batch_assets.py --all
```

That publishes `trino-jdbc-476.jar` and `mysql-connector-j-8.4.0.jar` to
`s3a://js-demo/jars/` (SHA-1 verified against Maven), and drops
`data/customer_accounts.csv` into `s3a://js-demo/landing/accounts/` under a
dated name. In production an upstream system drops that file; here the script
plays that role. Re-drop it as often as you like — the MERGE deduplicates.

### One-time: credentials

The job reads credentials from the environment **only**, so they can be injected
by the secret set rather than appearing in the submit command (which the CLI
persists by default):

| Variable | Used for |
| --- | --- |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Glue metastore (AWS SDK credential chain) |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | Iceberg S3FileIO and the landing zone |
| `AIDP_USERNAME` / `AIDP_PASSWORD` | The Starburst JDBC read of the data catalog |
| `MYSQL_USER` / `MYSQL_PASSWORD` | Only for `--customers-reader mysql` |

The exact `uploads create-secret` command is in the header of
`scripts/submit_batch_aidp.sh`.

---

## 5. Running it on AIDP

```bash
export SECRET_ID=<uploaded secret set id>
export S3_ACCESS_KEY=... S3_SECRET_KEY=...
export DDPE_INSECURE=1        # or DDPE_CERT=/path/to/ca.pem

./dell-data-processing-engine/bin/dell-data-processing-engine \
  login-with-credentials --insecure --role=jirawut_demo --username=jirawut

# everything, incremental
./scripts/submit_batch_aidp.sh all

# one source at a time — isolated failures and independent sizing
./scripts/submit_batch_aidp.sh customers
./scripts/submit_batch_aidp.sh accounts
./scripts/submit_batch_aidp.sh payments_daily
```

Useful variations:

```bash
DRY_RUN=1        ./scripts/submit_batch_aidp.sh all         # read + count, no writes
MODE=full        ./scripts/submit_batch_aidp.sh customers   # ignore the watermark
SINCE=2026-09-01 ./scripts/submit_batch_aidp.sh customers   # targeted backfill
NUM_PARTITIONS=4 ./scripts/submit_batch_aidp.sh customers   # parallel JDBC readers
LOOKBACK_DAYS=3  ./scripts/submit_batch_aidp.sh payments_daily
```

Monitor it like any other AIDP instance:

```bash
dell-data-processing-engine instance list
dell-data-processing-engine instance status <instance-id>
dell-data-processing-engine instance logs   <instance-id>
```

The job is finite: it reads, upserts, writes its audit rows and exits. Schedule
it (cron, Airflow, whatever drives the CLI) rather than leaving it running. It
exits non-zero if **any** source failed, while still completing the others — one
unreachable source must not abandon the rest of the batch.

### TLS

The coordinator's certificate is issued by a private CA (`CN=TitanCA`), which
the JVM does not trust. Either point the job at a trust store containing it
(preferred) or disable verification for lab testing only:

```bash
AIDP_TRUSTSTORE=/opt/certs/titan.p12 ./scripts/submit_batch_aidp.sh customers
AIDP_VERIFY_TLS=false                ./scripts/submit_batch_aidp.sh customers
```

Verification is **on by default**, and the job prints a warning when it is off.

---

## 6. What the job produces

Four Iceberg tables in `js_financial_ice.ingestion` — a schema of their own, so
the batch targets never mix with the streaming event tables in `banking`
(created on first run;
`sql/Iceberg_Batch_Tables.sql` is the same model for review or for creating them
from Starburst):

| Table | Contents |
| --- | --- |
| `dim_customer` | Customer master data copied from the data catalog |
| `dim_account` | Account reference data from the landing zone, with `source_file` lineage |
| `fact_payment_daily` | Per-account daily payment rollup, partitioned by `event_date` |
| `ingestion_audit` | One row per source per batch |

Every ingested row carries `source_system`, `batch_id` and `ingested_at`, so
"where did this number come from" is answerable in SQL without reading the job.

`rows_inserted` / `rows_updated` in the audit table come from the **Iceberg
snapshot the MERGE produced**, not from a counter in the job, so they describe
what was actually committed. Copy-on-write reports an update as one added and
one deleted record, which is how the two are told apart.

Sample audit output from a local verification run:

```
batch_id                   source          mode         rows_read  inserted  updated  status
20260907T102745Z-7093bb25  accounts        full             10000     10000        0  OK
20260907T102821Z-e24f57d0  accounts        full             10000         0    10000  OK
20260907T102908Z-e38a547c  customers       full             10000     10000        0  OK
20260907T102948Z-9ee73633  customers       incremental          0         0        0  OK
20260907T103010Z-7858f031  customers       incremental      10000         0    10000  OK
20260907T103252Z-4cbfa91a  payments_daily  full               300       300        0  OK
20260907T103315Z-03a25dc9  payments_daily  incremental        200         0      200  OK
```

---

## 7. Validating a run

From Starburst, `sql/07_batch_ingestion_validation.sql` answers, in order:

1. What did the last batch do? (the audit row per source)
2. Is the load idempotent? (`duplicate_keys` must be 0 in all three targets)
3. Does `dim_customer` agree with the MySQL source of record?
4. Does `fact_payment_daily` agree with `payment_transactions`? (`diff_txn_count` must be 0)
5. Batch history and failures per source
6. Lineage: which landing file and batch a row came from
7. The payoff query: the batch-loaded dimension joined to the rollup **inside one
   Iceberg catalog**, so it needs no federation at query time — compare with
   `sql/02_customer_payment_360.sql`, which joins MySQL live

---

## 8. Verified behaviour

### On AIDP

Run on 2026-09-08 with `./scripts/submit_batch_aidp.sh all` into
`js_financial_ice.ingestion` (pool `default`, 2 executors x 3G x 2 cores):

| Run | Source | Result |
| --- | --- | --- |
| 1st | `customers` | 10,000 rows through `js_mysql_customer360` over Trino JDBC — **10,000 inserted** |
| 1st | `accounts` | 10,000 rows from `s3a://js-demo/landing/accounts/*.csv` — **10,000 inserted** |
| 1st | `payments_daily` | 283,256 account/day rows rolled up from `banking.payment_transactions` — **283,256 inserted** |
| 2nd | `customers` | watermark at the source's newest `updated_at` — **0 rows staged, no snapshot** |
| 2nd | `accounts` | same file re-read — **0 inserted, 10,000 updated** |
| 2nd | `payments_daily` | watermark narrowed the read to a 2-day window: 12,422 rows — **0 inserted, 12,422 updated** |

Both runs finish `3 ok, 0 failed`. The tables landed under
`s3://js-demo/warehouse/ingestion/<table>`, i.e. the Glue database's own
location rather than the job's `--warehouse` fallback.

Two bugs were found by running it, both now fixed and covered by tests:

* **A missing credential aborted the whole batch.** The credential check called
  `sys.exit()`, and `SystemExit` does not inherit from `Exception`, so it
  escaped the per-source handler: `accounts` and `payments_daily` never ran. It
  raises `RuntimeError` now, so the failing source is recorded as `FAILED` in
  `ingestion_audit` and the rest of the batch continues (the job still exits
  non-zero).
* **Spark could not read the catalog's timestamps.** MySQL `TIMESTAMP` reaches
  Trino as `timestamp(0) with time zone`, and Spark's JDBC dialect has no
  mapping for `TIMESTAMP_WITH_TIMEZONE` — the read died with
  `[UNRECOGNIZED_SQL_TYPE]`. The query now converts both audit timestamps with
  `CAST(col AT TIME ZONE 'UTC' AS timestamp(6))`, which also makes them
  independent of the coordinator's session timezone. Connector/J maps them
  natively, so the direct MySQL reader keeps the plain columns.

### Credentials used

The catalog read needs `AIDP_USERNAME`/`AIDP_PASSWORD` in the driver
environment. `--uploaded-secrets` is repeatable and `SECRET_ID` accepts a
comma-separated list, so those live in their own secret set rather than being
duplicated into the one the streaming job already uses:

```bash
dell-data-processing-engine uploads create-secret \
  --comment "batch ingestion: AIDP data catalog credentials" \
  AIDP_USERNAME=$(printf %s "jirawut" | base64 -w0) \
  AIDP_PASSWORD=$(printf %s "$AIDP_PW" | base64 -w0)

SECRET_ID=<aws/s3 secret set>,<aidp secret set> ./scripts/submit_batch_aidp.sh all
```

The pool is not optional on this cluster: with both `default` and `NVIDIA-L4`
available, a submit without `--pool` is rejected with *"More than one resource
pool available"*. Use `RESOURCE_POOL=default` for this CPU-only job.

### Locally

Verified on 2026-09-07 against a real Iceberg warehouse (Spark 3.5.7, Iceberg
1.5.0, `--catalog-type hadoop`) and the live MySQL container, with the CSV
landing file and a seeded `payment_transactions` table:

| Check | Result |
| --- | --- |
| First load of each target | 10,000 / 10,000 / 300 rows inserted |
| Re-run of the same batch | 0 inserted, all rows updated; row counts unchanged |
| Duplicate merge keys after re-runs | 0 in all three targets |
| Rollup vs a direct aggregate over the events | identical count and amount for every day |
| Incremental with no source change | 0 rows staged, no snapshot committed |
| Incremental after `--since` in the past | 10,000 rows, 0 inserted, 10,000 updated |
| Incremental rollup with 1-day lookback | 2 of 3 days recomputed, totals unchanged |
| Parallel JDBC read (`--num-partitions 4`) | same result as a serial read |
| `--source all` | 3 sources, 3 audit rows, exit 0 |

The Starburst JDBC path (`--customers-reader starburst`) is verified on AIDP
only — see the table above — since it needs cluster credentials. Locally the
same source is exercised through `--customers-reader mysql` against the same
table.

Offline tests (no Spark cluster, no AIDP, no MySQL):

```bash
pip install -r requirements-spark.txt      # pyspark, for the job module import
.venv/bin/python -m pytest tests/test_batch_ingestion.py -q     # 47 tests
```

They cover the MERGE contract (target columns match the DDL, the reader's
projection covers the target, the reference SQL matches the job), watermark
rendering including the naive-timestamp trap, JDBC URL and TLS construction,
credential hygiene, snapshot accounting, and that the submit script passes no
secret as a command-line argument.

### Local dry runs

`--catalog-type hadoop` swaps Glue and S3 for a filesystem warehouse, which is
the fastest way to exercise a change before submitting it:

```bash
pip install -r requirements-spark.txt
export PYSPARK_SUBMIT_ARGS="--jars /path/to/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/path/to/mysql-connector-j-8.4.0.jar pyspark-shell"

python jobs/batch_ingestion_job.py --source accounts \
  --catalog-type hadoop --warehouse /tmp/wh \
  --accounts-path data --accounts-pattern customer_accounts.csv --mode full
```

---

## 9. Troubleshooting

**`More than one resource pool available, please choose the resource pool`**
The cluster exposes several pools and the submit carried no `--pool`. Run
`admin resource-pools get` to list them, then `RESOURCE_POOL=default`.

**`[UNRECOGNIZED_SQL_TYPE] ... id: TIMESTAMP_WITH_TIMEZONE`**
A Trino column typed `timestamp with time zone` reached Spark's JDBC dialect,
which cannot map it. Cast it in the query —
`CAST(col AT TIME ZONE 'UTC' AS timestamp(6))` — as `TRINO_UTC_TIMESTAMP` does
for the two audit timestamps. Any new `timestamp with time zone` column in the
projection needs the same treatment.

**`No suitable driver` / `ClassNotFoundException: io.trino.jdbc.TrinoDriver`**
The jars were not staged, or `--jars` did not resolve them. Check
`s3a://js-demo/jars/` and re-run `./scripts/stage_batch_assets.py --jars`.

**`INVALID_NON_DETERMINISTIC_EXPRESSIONS` from the MERGE**
A reader introduced a non-deterministic expression (`current_timestamp()`,
`input_file_name()`, `rand()`). Iceberg may scan the merge source twice and
refuses them. Use a driver-computed literal (`lit(cfg.batch_ts)`) and
`_metadata.file_path` instead — this is why the job does.

**`a single row from the target matched multiple rows from the source`**
The batch contains two rows with the same merge key and `stage()` did not
collapse them. Either the key is not unique in the source, or the reader
bypassed `stage()`.

**`--source customers` reads 0 rows but MySQL has data**
An incremental run with a watermark already at the source's newest
`updated_at`. Confirm with query 1 in `sql/07_batch_ingestion_validation.sql`,
then use `MODE=full` or `SINCE=...`.

**Row counts differ between `dim_customer` and MySQL**
An incremental run has not caught up, or rows were deleted in MySQL — the MERGE
never deletes. Reconcile with query 3, and use `MODE=full` for a full refresh.

**`fact_payment_daily` disagrees with `payment_transactions`**
Late events landed in a day outside the lookback window. Re-run with a wider
`LOOKBACK_DAYS`, or `MODE=full` to rebuild every day.

**Certificate errors on the Starburst JDBC connection**
`PKIX path building failed` means the private CA is not trusted; see §5.

**Access denied reading the data catalog**
The JDBC read uses `AIDP_USERNAME`, which needs `SELECT` on
`js_mysql_customer360.customer360`. Check the role, then that the catalog itself
works: `SELECT COUNT(*) FROM js_mysql_customer360.customer360.customers`.

**Many small files after repeated runs**
Expected on a MERGE-heavy table. Compact from Starburst
(`ALTER TABLE ... EXECUTE optimize`) or with Iceberg's
`rewrite_data_files`/`expire_snapshots` procedures.
