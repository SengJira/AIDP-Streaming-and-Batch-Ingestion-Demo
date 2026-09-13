# AIDP Federated Query Demo — MySQL Customer 360 × Iceberg Banking Events

Demonstrates Starburst on Dell AIDP joining **customer master data that stays in
MySQL** with **real-time payment and fraud data that stays in Apache Iceberg**,
in a single query. Nothing is copied between the two systems.

---

## 1. Architecture overview

```
  synthetic generator                    Kafka (172.18.1.177:9092)
  scripts/generate_customer360.py            payments.raw
        |                                    fraud.signals
        |  data/customer_accounts.csv        customer.events
        |  (the shared ACC-TH-######## pool)       |
        |                                          | Spark Structured Streaming
        v                                          | jobs/banking_streaming_job.py
  data/customer360.csv                             v
        |                              Iceberg  js_financial_ice.banking
        | scripts/load_customer360.py    payment_transactions
        v                                fraud_alerts
   MySQL  customer360.customers          customer_events / aml_alerts / payment_agg
   (js-mysql-customer360 container)             |
        |                                       |
        +--------------- account_id ------------+
                          |
                          v
              Starburst / AIDP federated query
       js_mysql_customer360  ⋈  js_financial_ice
                          |
                          v
          fraud_analytics_app.py -> "Federated Customer 360"
```

Both sides share one **stable customer pool** (`ACC-TH-10000001` …
`ACC-TH-10010000`). That is the whole trick: the original generator invented a
random account ID per event, so cross-system joins matched essentially never.

| Layer | Technology | Location |
| --- | --- | --- |
| Customer master data | MySQL 8.0.43 | `js-mysql-customer360` container on `172.18.1.177:3306` |
| Event streams | Kafka | `172.18.1.177:9092` |
| Event storage | Apache Iceberg (Parquet on S3, Glue metastore) | `js_financial_ice.banking` |
| Query federation | Starburst Enterprise on Dell AIDP | `https://ddae.lab9bgp.com/` |
| Application | PyStarburst + Streamlit | `fraud_analytics_app.py` |

---

## 2. Prerequisites

* Docker and Docker Compose (tested with Docker 29.1.3, Compose v5.2.0)
* Python 3.12
* Network reach from this host to Kafka and to the AIDP endpoint
* An AIDP account (`jirawut`) able to query `js_financial_ice`
* An AIDP **administrator** to create the MySQL catalog (see §9)
* For restarting the Spark pipeline: the `dell-data-processing-engine` CLI, an
  uploaded secret set id (`SECRET_ID`), and S3 keys for the jar bucket

---

## 3. Network requirements

| From | To | Port | Why |
| --- | --- | --- | --- |
| AIDP **worker** nodes | `172.18.1.177` | 3306 | The MySQL connector reads from every worker, not just the coordinator |
| This host | `172.18.1.177` | 9092 | Kafka producer |
| This host | `ddae.lab9bgp.com` | 443 | Trino/PyStarburst client |
| Spark executors | `172.18.1.177` | 9092 | Kafka consumer |

The demo host is **dual-homed**: primary `10.246.25.115` (management/SSH) plus
`172.18.1.177` on the AIDP subnet. The AIDP cluster cannot route to
`10.246.25.x`, so every cluster-facing endpoint uses `172.18.1.177`. Services
still publish on `0.0.0.0`, so local clients can also reach them via
`10.246.25.115` — but AIDP must always use `172.18.1.177`.

MySQL publishes on `0.0.0.0:3306` (override with `MYSQL_BIND_ADDRESS`). It
shares the demo host that also runs the Kafka broker.

Verify reachability before blaming the catalog:

```bash
ss -ltnp | grep 3306
python -c "import socket; socket.create_connection(('172.18.1.177',3306),timeout=5); print('reachable')"
```

---

## 4. MySQL Docker installation

```bash
cd ~/jirawut-demo/repo
cp docker/mysql/.env.example docker/mysql/.env
# Edit docker/mysql/.env and set strong passwords, then:
chmod 600 docker/mysql/.env

docker compose \
  --env-file docker/mysql/.env \
  -f docker/mysql/docker-compose.yml \
  up -d

docker inspect -f '{{.State.Health.Status}}' js-mysql-customer360   # -> healthy
```

The compose file contains **no passwords**: every secret uses
`${VAR:?message}`, so the stack refuses to start rather than silently create a
blank-password account.

On first start (empty volume only) `/docker-entrypoint-initdb.d` runs:

* `init/01_create_schema.sql` — database, `customers` table, indexes. No credentials.
* `init/02_create_starburst_reader.sh` — creates `starburst_ro` with
  `GRANT SELECT ON customer360.*` and nothing else. It is a shell script because
  the MySQL entrypoint does not template variables into `.sql` files, which
  keeps the password in the container environment only.

> The init scripts run **only when the data volume is empty**. After changing
> them you must recreate the volume (destructive):
> `docker compose --env-file docker/mysql/.env -f docker/mysql/docker-compose.yml down -v`

### Why `mysql_native_password` for the reader

`starburst_ro` is created with `mysql_native_password`. MySQL 8's default
`caching_sha2_password` requires either TLS or `allowPublicKeyRetrieval=true`
over a plaintext link, which is a common first-connection failure for the Trino
MySQL connector. Note the server flag `--mysql-native-password=ON` exists only
on MySQL **8.4+**; on 8.0.x the plugin is built in and passing that flag makes
the server refuse to start with `unknown variable 'mysql-native-password=ON'`.

---

## 5. Credential setup

| Secret | Where it lives | Used by |
| --- | --- | --- |
| `MYSQL_ROOT_PASSWORD` | `docker/mysql/.env` | container init only |
| `MYSQL_APP_PASSWORD` | `docker/mysql/.env` | `scripts/load_customer360.py` (read/write) |
| `MYSQL_STARBURST_PASSWORD` | `docker/mysql/.env` | the AIDP catalog (read-only) |
| `AIDP_PASSWORD` | shell environment only | PyStarburst / Streamlit |

Rules enforced in code and tests:

* `docker/mysql/.env` is git-ignored; only `.env.example` with `replace_me` is committed.
* Passwords are wrapped in `aidp_connection.Secret`, whose `__repr__`/`__str__`/
  `__format__` return `***REDACTED***`.
* `SecretRedactingFilter` scrubs secrets from the logging subsystem.
* `scripts/validate_federated_demo.py` scans generated files for the real
  secrets and fails if any is found.
* AIDP never uses `root`.

Read the catalog password back without printing it into a shell transcript:

```bash
grep MYSQL_STARBURST_PASSWORD docker/mysql/.env
```

---

## 6. Synthetic data generation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python scripts/generate_customer360.py --customers 10000 --seed 42 --output-dir data
```

Produces `data/customer360.csv` (full master record) and
`data/customer_accounts.csv` (the pool the Kafka generator reads:
`account_id, customer_tier, city, country`).

Deterministic: the same `--seed` yields a byte-identical file. Ages, open dates
and audit timestamps derive from `--as-of-date` (default today), so pin it if
you need reproducibility across days.

**All data is synthetic.** Names are assembled from two fixed ~40-word lists,
national IDs are masked to `X-XXXX-XXXXX-XX-<digit>`, phone numbers to
`+66-XX-XXX-####`, and e-mail uses the reserved `.invalid` TLD (RFC 6761), so no
address can resolve. Customer IDs are prefixed `CUST-DEMO-`.

Distributions at 10,000 customers / seed 42:

| Attribute | Distribution |
| --- | --- |
| Tier | SILVER 40.2%, STANDARD 34.2%, GOLD 20.6%, PLATINUM 4.9% |
| Risk | LOW 68.7%, MEDIUM 26.2%, HIGH 5.1% |
| KYC | VERIFIED 85.3%, PENDING 9.8%, EXPIRED 4.9% |
| City | Bangkok 55.1%, Chiang Mai 15.1%, Pattaya 11.8%, Phuket 10.0%, Khon Kaen 8.1% |
| PEP | 2.0% |
| Sanctions | CLEAR 96.2%, PENDING_REVIEW 3.2%, MATCH_REVIEW 0.5% |

---

## 7. Customer data loading

```bash
python scripts/load_customer360.py --input data/customer360.csv
```

Idempotent: `INSERT ... ON DUPLICATE KEY UPDATE` keyed on `customer_id`. Running
it twice reports `newly inserted: 0` and the row count is unchanged.
`account_id` carries a UNIQUE constraint, because a duplicate join key would
multiply every federated aggregate.

Use `--truncate` to wipe first (destructive), `--batch-size` to tune batching.

If MySQL runs on a different host from the loader, set `MYSQL_HOST`
(the default `172.18.1.177` is the AIDP-facing address):

```bash
MYSQL_HOST=127.0.0.1 python scripts/load_customer360.py --input data/customer360.csv
```

---

## 8. Kafka generator startup

```bash
python generators/Streaming_gen.py \
  --customer-file data/customer_accounts.csv \
  --bootstrap-server 172.18.1.177:9092 \
  --seed 42
```

> Path note: the generator lives in `generators/`, matching the existing repo
> layout, rather than at the repository root.

Behaviour:

* Payments, fraud signals, AML alerts and login events all draw from the pool.
* Payment `city`, `customer_tier` and domestic `country` follow the selected
  customer's master record, so the two systems agree.
* `--unknown-rate` (default `0.01`) sends that share of events with accounts
  drawn from `ACC-TH-9########`, which can never collide with the pool. This is
  what makes the unmatched-record demo real rather than hypothetical.
* On exit it prints total events and the known/unknown split per topic.
* Topic names are unchanged. Omitting `--customer-file` restores the original
  fully-random behaviour (and warns that joins will not match).
* `--dry-run` generates and reports without touching Kafka.

Useful flags: `--duration`, `--payments-eps`, `--fraud-eps`, `--customer-eps`,
`--compression` (falls back to gzip if `python-snappy` is missing).

### Restarting the Spark streaming pipeline

Events only reach Iceberg while the Spark jobs run:

```bash
export SECRET_ID=<uploaded secret set id>
export S3_ACCESS_KEY=... S3_SECRET_KEY=...
export DDPE_INSECURE=1

./dell-data-processing-engine/bin/dell-data-processing-engine \
  login-with-credentials --insecure --role=jirawut_demo --username=jirawut

./scripts/submit_aidp.sh events
./scripts/submit_aidp.sh payments
./scripts/submit_aidp.sh aggregates
```

**If the pipeline has been stopped for a while, set `MAX_OFFSETS` high on the
first restart** — see the troubleshooting entry "Streams run but write zero
rows".

Day-to-day start/stop procedure, including how to stop the generator cleanly
and how to spot the offset gap before it wastes an hour, is in
[RUNBOOK.md](RUNBOOK.md).

---

## 9. AIDP MySQL catalog configuration

Catalog name: **`js_mysql_customer360`**

Devin does not have AIDP administrator rights, so this step is manual. Give the
administrator the template in
`catalogs/js_mysql_customer360.properties.example`:

```properties
connector.name=mysql
connection-url=jdbc:mysql://172.18.1.177:3306
connection-user=starburst_ro
connection-password=<value of MYSQL_STARBURST_PASSWORD>
```

Either drop it on the coordinator as
`<catalog-dir>/js_mysql_customer360.properties` and reload catalogs, or enter
the same four fields in the AIDP UI (Data products → Catalogs → Add catalog →
MySQL).

Critical details:

* **Do not append a database name** to `connection-url`. The connector maps
  MySQL databases to Trino schemas, giving
  `js_mysql_customer360.customer360.customers`.
* `172.18.1.177:3306` must be reachable from every **worker**, not only the coordinator.
* Use `starburst_ro`, never `root`.

Verify:

```sql
SHOW SCHEMAS FROM js_mysql_customer360;
SHOW TABLES FROM js_mysql_customer360.customer360;
SELECT COUNT(*) FROM js_mysql_customer360.customer360.customers;
```

Or run all of `sql/01_mysql_catalog_validation.sql`.

---

## 10. Federated query execution

| File | What it shows |
| --- | --- |
| `sql/01_mysql_catalog_validation.sql` | Catalog reachable, table visible, key unique and well-formed |
| `sql/02_customer_payment_360.sql` | Customers ⋈ payments: volume and value per customer |
| `sql/03_customer_fraud_360.sql` | Fraud alerts enriched with tier / KYC / PEP from MySQL |
| `sql/04_high_risk_customer_360.sql` | High-risk & PEP customers with fraud **and** payment exposure |
| `sql/05_customer_channel_profile.sql` | Stated preferred channel vs actual payment and login channels |
| `sql/06_federated_demo_validation.sql` | Match-quality metrics and the unmatched exception list |

### Avoiding multiplied totals

Files 04 and 05 pre-aggregate each Iceberg source to **one row per
`account_id`** in its own CTE before joining to the customer table. Measured on
this dataset for `ACC-TH-10007355` (5 payments, 8 fraud alerts):

| Approach | Payment rows | Payment total |
| --- | --- | --- |
| Ground truth | 5 | 336,139.59 |
| Naive `customers ⋈ payments ⋈ fraud_alerts` | 40 (**8×**) | 2,689,116.72 (**+2,352,977**) |
| Pre-aggregated CTEs (used here) | 5 | 336,139.59 |

A unit test enforces this structurally for every SQL file and app template.

---

## 11. PyStarburst application startup

```bash
export AIDP_ENDPOINT="https://ddae.lab9bgp.com/"
export AIDP_USERNAME="jirawut"
export AIDP_PASSWORD="<set-locally>"
export AIDP_CATALOG="js_financial_ice"
export AIDP_SCHEMA="banking"
export AIDP_VERIFY_TLS="true"

python scripts/validate_federated_demo.py
streamlit run fraud_analytics_app.py
```

> On this lab endpoint `AIDP_VERIFY_TLS=true` fails: the coordinator's
> certificate is issued by a private CA (`CN=TitanCA`). Install that CA
> (`export REQUESTS_CA_BUNDLE=/path/to/titan-ca.pem`) or use
> `AIDP_VERIFY_TLS=false` for lab testing only.

Pick **Federated Customer 360** in the sidebar. Supported questions:

1. Who are the customers with the highest payment amount?
2. Show high-risk customers with recent fraud alerts.
3. Which Platinum customers have fraud alerts?
4. Show PEP customers with high fraud scores.
5. Compare customer preferred channels with actual payment channels.
6. Show customers using a new device.
7. Which cities have the most fraud alerts?
8. Show payment and fraud exposure for high-risk customers.
9. How many banking events do not match a customer record?
10. Show the Customer 360 profile for account ACC-TH-10000001.

Each result shows the generated federated SQL, the result table, a per-column
breakdown of **which catalog each column came from**, a chart where meaningful,
and a plain-language note that Starburst performed the cross-catalog join.

Safety: user text never reaches SQL. Questions map to fixed reviewed templates;
only validated numerics and an account ID *rebuilt* from 8 extracted digits are
substituted, and every statement is re-checked read-only before execution.

---

## 12. Expected demo results

Measured on this environment (10,000 customers, generator at ~50 payments/s,
1% unknown-account rate):

| Check | Result |
| --- | --- |
| Customers in MySQL | 10,000 (`ACC-TH-10000001` … `ACC-TH-10010000`) |
| Payment event match rate | ~99.0% |
| Fraud alert match rate | ~99.0% |
| Customer event match rate | ~99.0% |
| Unmatched | ~1%, all `ACC-TH-9########` |
| Federated query latency | 0.4 s – 3.8 s |

`validate_federated_demo.py` should print 12 PASS and 0 FAIL.

A match rate of exactly 100% means the unknown-account path is not being
exercised; a rate near 0% means the account-ID formats have diverged.

---

## 13. Troubleshooting

**AIDP cannot reach the MySQL host**
Test from a worker, not just the coordinator. The catalog loads fine and only
fails at query time. Confirm MySQL listens on `0.0.0.0` (`ss -ltnp | grep 3306`)
rather than `127.0.0.1`, and that `MYSQL_BIND_ADDRESS` is not set to loopback.

**Port 3306 is blocked**
`python -c "import socket; socket.create_connection(('172.18.1.177',3306),timeout=5)"`.
If that hangs, open the port between the AIDP node network and this host.

**MySQL user is restricted to localhost**
`SELECT user, host FROM mysql.user WHERE user='starburst_ro';` must show `%`.
A `localhost` row is the most common cause of `Access denied` from AIDP even
though local tests pass. Recreate with
`CREATE USER 'starburst_ro'@'%' IDENTIFIED WITH mysql_native_password BY '...'`.

**AIDP catalog does not appear**
`SHOW CATALOGS;`. If absent, the properties file was not picked up or the
catalog was not reloaded. Check the coordinator log for the connector name and
confirm the file is named exactly `js_mysql_customer360.properties`.

**Access denied**
Usually one of: wrong password, `localhost`-bound user, or
`caching_sha2_password` over a plaintext link. For the last case append
`?allowPublicKeyRetrieval=true` to `connection-url`, or keep the account on
`mysql_native_password` as this demo does.

**TLS certificate validation failure**
The AIDP endpoint uses a private CA (`CN=TitanCA`). Either
`export REQUESTS_CA_BUNDLE=/path/to/titan-ca.pem` or set `AIDP_VERIFY_TLS=false`
for lab testing only. For the MySQL side, append `?sslMode=DISABLED` if the
self-signed server certificate causes trouble.

**Empty federated join**
Run `sql/06_federated_demo_validation.sql`. Then check, in order:
`SELECT COUNT(*) FROM js_mysql_customer360.customer360.customers` (is the master
data loaded?), `SELECT MAX(timestamp) FROM js_financial_ice.banking.payment_transactions`
(is the pipeline live?), and whether the generator was started **with**
`--customer-file`.

**Streams run but write zero rows**
The classic symptom: Iceberg snapshots are committed every trigger with
`changed-partition-count = 0`. Kafka retention has deleted the offsets the
checkpoint points at, and the source skips the gap at only
`maxOffsetsPerTrigger` per batch. Diagnose:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server 172.18.1.177:9092 --topic payments.raw --time earliest
```

and compare with the checkpoint under
`s3://js-demo/warehouse/banking/checkpoints-10-246-25-115/<stream>/offsets/`. If the
checkpoint is far below the earliest retained offset, resubmit with a large
window so one batch spans the gap:

```bash
MAX_OFFSETS=1000000 ./scripts/submit_aidp.sh payments
```

**Mismatched `account_id` format**
Both sides must be `ACC-TH-` plus exactly 8 digits. Check with query 6 in
`sql/01_mysql_catalog_validation.sql`. Regenerating the pool with a different
`--customers` count changes the range, so MySQL and Kafka must be regenerated
together from the same CSV.

**Duplicate customer accounts**
`SELECT COUNT(*) - COUNT(DISTINCT account_id) FROM customers;` must be 0. The
UNIQUE key normally prevents this; if it is non-zero the table was created
without the constraint — recreate the volume and reload.

**Query returns multiplied totals**
You joined two event tables in one scope. Pre-aggregate each to one row per
`account_id` first — see §10 and `sql/04_high_risk_customer_360.sql`.

**MySQL connector pushdown is not visible**
Use the app's "Pushdown diagnostics (EXPLAIN)" panel or run `EXPLAIN` manually.
Pushdown depends on connector version, predicate shape and available
statistics. **Do not claim pushdown occurred unless the plan shows it** — see §14.

**Docker volume permission issues**
`docker logs js-mysql-customer360` showing `Permission denied` or
`mysql.plugin table` errors usually means a partly-initialised volume. Recreate
it (destroys the data):
`docker compose --env-file docker/mysql/.env -f docker/mysql/docker-compose.yml down -v`
then `up -d`. Never `chown` the named volume by hand.

---

## 14. Pushdown notes

* **MySQL filters and projections may be pushed down** when the connector
  supports the predicate shape. Simple equality on an indexed column
  (`customer_tier = 'PLATINUM'`, `pep_flag = 1`) is the best candidate; the
  indexes in `01_create_schema.sql` exist for exactly this.
* **Iceberg partition pruning uses `event_date`.** Filtering on `timestamp`
  alone does not prune partitions — add an `event_date` predicate, as
  `sql/02` and `sql/04` do.
* **The final cross-catalog join always executes in Starburst.** No connector
  can join across catalogs.
* **Performance depends on filtering both sides before joining.** Narrow the
  time window and the customer segment first; the join cost is driven by how
  many rows survive each scan.
* **Large production federated joins need more**: table statistics
  (`ANALYZE`), sensible Iceberg partitioning, and workload/memory tuning. A
  10,000-row dimension table broadcasts happily; a 10-million-row one will not.
* **Verify, do not assume.** The app's EXPLAIN panel reports what the plan
  actually shows and explicitly says when no MySQL-side filter is visible.

---

## 15. Cleanup

```bash
# Stop the Kafka generator
pkill -f "generators/Streaming_gen.py"

# Stop the Spark streaming jobs (answer y at the prompt)
CLI=./dell-data-processing-engine/bin/dell-data-processing-engine
$CLI --insecure instance list
echo y | $CLI --insecure instance delete <instance-id>

# Stop MySQL but KEEP the data
docker compose --env-file docker/mysql/.env -f docker/mysql/docker-compose.yml down

# Stop MySQL and DESTROY the data volume (irreversible)
docker compose --env-file docker/mysql/.env -f docker/mysql/docker-compose.yml down -v
docker volume rm js_mysql_customer360_data   # only if it survived

# Remove generated data and local secrets
rm -f data/customer360.csv data/customer_accounts.csv
rm -f docker/mysql/.env
```

Ask the AIDP administrator to remove the `js_mysql_customer360` catalog if the
demo is finished.
