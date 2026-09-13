# Runbook — starting and stopping the streaming demo

Day-to-day operation of the Kafka event generator and the three Spark
Structured Streaming jobs that feed the Iceberg tables.

For first-time setup (MySQL, synthetic data, AIDP catalog) see
[FEDERATED_QUERY_DEMO.md](FEDERATED_QUERY_DEMO.md). The batch ingestion job is a
finite job with its own procedure — see
[BATCH_INGESTION_DEMO.md](BATCH_INGESTION_DEMO.md); it is independent of the
streaming jobs and can run whether or not they are up.

**Order matters**

| | Order | Why |
| --- | --- | --- |
| Start | Spark jobs → generator | A fresh checkpoint reads from `latest`; starting Spark first means nothing is missed |
| Stop | Generator → Spark jobs | Lets the jobs drain what is already in Kafka |

MySQL is independent of both and can stay running.

---

## 0. Session setup

Every new shell:

```bash
cd ~/jirawut-demo/repo

export SECRET_ID=s-aa4a1b9a5f6b47dd90c9661730a2a39c
export S3_ACCESS_KEY='<s3 key>'
export S3_SECRET_KEY='<s3 secret>'
export DDPE_INSECURE=1
export RESOURCE_POOL=default

CLI=./dell-data-processing-engine/bin/dell-data-processing-engine
$CLI login-with-credentials --insecure --role=jirawut_demo --username=jirawut --password='<password>'
```

The DDPE access token is refreshed only while the CLI is in use and expires
after **30 minutes idle**. `You are not logged in or your access token has
expired` simply means: run the login again.

Never put the password in a script or commit it.

---

## 1. START

### 1a. Check the Kafka offset gap — this decides one flag

Kafka retention on these topics is **24 hours** (`retention.ms=86400000`).

If the Spark jobs have been stopped for **longer than ~24 hours**, their
checkpoints point at offsets Kafka has already deleted. The Kafka source then
skips the missing range at only `maxOffsetsPerTrigger` per micro-batch, so the
jobs appear to run while writing **zero rows** — potentially for over an hour.

Earliest offsets still retained:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server 172.18.1.177:9092 --topic payments.raw --time earliest
```

> **Checkpoint namespace.** Streaming state lives under
> `s3://js-demo/warehouse/banking/checkpoints-10-246-25-115` — the Kafka broker
> at `172.18.1.177:9092` was deployed fresh, and checkpoints record broker
> offsets, so a new broker must not resume from the old broker's state. The
> previous `s3://js-demo/warehouse/banking/checkpoints` tree is kept, untouched.
> `submit_aidp.sh` passes `CHECKPOINT` through to the job as an application
> argument; override it with `CHECKPOINT=s3://... ./scripts/submit_aidp.sh ...`.

Where the checkpoints currently sit:

```bash
venv/bin/python - <<'EOF'
import boto3, os
from botocore.client import Config
s3 = boto3.client("s3", endpoint_url="http://172.18.11.31:9020",
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    config=Config(s3={"addressing_style": "path"}), region_name="us-east-1")
for cp in ("payment_transactions_v3", "fraud_customer"):
    prefix = f"warehouse/banking/checkpoints-10-246-25-115/{cp}/offsets/"
    keys = sorted(
        (o["Key"] for o in s3.list_objects_v2(Bucket="js-demo", Prefix=prefix).get("Contents", [])),
        key=lambda k: int(k.rsplit("/", 1)[-1]) if k.rsplit("/", 1)[-1].isdigit() else -1,
    )
    if keys:
        body = s3.get_object(Bucket="js-demo", Key=keys[-1])["Body"].read().decode()
        print(cp, body.strip().splitlines()[-1])
EOF
```

**Rule of thumb**

| Situation | Flag |
| --- | --- |
| Stopped < 24 h, checkpoint ≥ earliest offset | no flag needed |
| Stopped > 24 h, or checkpoint below earliest offset | `MAX_OFFSETS=1000000` |

`MAX_OFFSETS` raises `maxOffsetsPerTrigger` so a single batch spans the expired
range. It is non-destructive — preferable to deleting checkpoints.

### 1b. Submit the three Spark jobs

```bash
MAX_OFFSETS=1000000 ./scripts/submit_aidp.sh events        # fraud_alerts, customer_events, aml_alerts
MAX_OFFSETS=1000000 ./scripts/submit_aidp.sh payments      # payment_transactions
MAX_OFFSETS=1000000 ./scripts/submit_aidp.sh aggregates    # payment_agg
```

Each prints an `instanceId` such as `b-94939d88417e4b15ac4ace8200f73e9f`. Allow
roughly 60 seconds for the driver and executors to register.

Other knobs: `STARTING_OFFSETS=earliest|latest` (only honoured on a *fresh*
checkpoint), `CHECKPOINT` (default the `checkpoints-10-246-25-115` namespace),
`KAFKA_BROKERS` (default `172.18.1.177:9092`), `EXECUTOR_MEMORY`,
`NUM_EXECUTORS`, `RESOURCE_POOL` (required on this cluster — use `default`).

### 1c. Start the generator

**Foreground** — simplest; `Ctrl+C` shuts down cleanly and prints the report:

```bash
source .venv/bin/activate
python generators/Streaming_gen.py \
  --customer-file data/customer_accounts.csv \
  --bootstrap-server 172.18.1.177:9092 \
  --seed 42
```

**Background** — always pass `--pid-file` so it can be stopped precisely:

```bash
nohup .venv/bin/python generators/Streaming_gen.py \
  --customer-file data/customer_accounts.csv \
  --bootstrap-server 172.18.1.177:9092 \
  --seed 42 \
  --pid-file /tmp/streaming_gen.pid \
  > /tmp/streaming_gen.log 2>&1 &
```

Useful options:

| Option | Default | Purpose |
| --- | --- | --- |
| `--unknown-rate` | `0.01` | Share of events with accounts absent from MySQL |
| `--payments-eps` / `--fraud-eps` / `--customer-eps` | 50 / 10 / 5 | Per-topic rate |
| `--duration` | none | Stop automatically after N seconds |
| `--seed` | none | Reproducible stream |
| `--compression` | `snappy` | Falls back to gzip if python-snappy is missing |
| `--dry-run` | off | Generate and report without touching Kafka |

Omitting `--customer-file` restores the original fully-random account IDs — the
federated joins will then match ~0%.

### 1d. Verify it is genuinely working

```bash
# 1. Jobs registered
$CLI --insecure instance list

# 2. Kafka advancing - run twice, the numbers must increase
docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server 172.18.1.177:9092 --topic payments.raw

# 3. Rows actually landing in Iceberg, plus the match rate
python scripts/validate_federated_demo.py
```

> **The trap:** a job can report `RUNNING` while writing nothing. If Iceberg's
> `MAX(timestamp)` is stale, inspect the snapshot summary rather than the job
> status — `changed-partition-count = 0` on every trigger means the offset gap
> from §1a, not a crash:
>
> ```sql
> SELECT committed_at, summary
> FROM js_financial_ice.banking."payment_transactions$snapshots"
> ORDER BY committed_at DESC LIMIT 3;
> ```

---

## 2. STOP

### 2a. Generator first

**Foreground:** `Ctrl+C`.

**Background:**

```bash
kill -TERM "$(cat /tmp/streaming_gen.pid)"
```

Either signal now drains the producer and prints the final known/unknown
report; the PID file is removed on exit.

> **Historical note.** Before the signal handler was added, `kill -INT` was
> silently ignored for background runs: bash sets `SIGINT` to `SIG_IGN` for
> async children of a non-interactive shell, Python inherits that, and its
> default `KeyboardInterrupt` handler is never installed. Only `SIGTERM`
> worked, and it killed the process without flushing or reporting. The
> generator now installs explicit `SIGTERM` and `SIGINT` handlers, which
> override the inherited `SIG_IGN`.

Without a PID file, prefer the PID the generator prints at startup
(`Running against ... (pid NNNN)`). Avoid bare `pgrep -f Streaming_gen`: it
also matches the shell whose command line contains that text, and `$!` may
point at a wrapper rather than the interpreter.

Confirm production has stopped — the offsets must be identical:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 172.18.1.177:9092 --topic payments.raw
sleep 10
docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 172.18.1.177:9092 --topic payments.raw
```

### 2b. Then the Spark jobs

```bash
$CLI --insecure instance list          # collect the b-... ids, ignore NOTEBOOK rows

echo y | $CLI --insecure instance delete b-<events-id>
echo y | $CLI --insecure instance delete b-<payments-id>
echo y | $CLI --insecure instance delete b-<aggregates-id>
```

`echo y |` is required: `instance delete` prompts interactively and raises a
`NullPointerException` when stdin is empty.

Stop every batch job at once, leaving notebooks alone:

```bash
$CLI --insecure --format=json instance list 2>/dev/null | sed -n '/\[/,$p' | \
python3 -c "import sys,json;[print(i['instanceId']) for i in json.load(sys.stdin) if i['type']!='NOTEBOOK']" | \
while read -r id; do echo y | $CLI --insecure instance delete "$id"; done
```

### 2c. Verify

```bash
$CLI --insecure instance list                       # no b-... rows remain
[ -f /tmp/streaming_gen.pid ] && echo "generator still running" || echo "generator stopped"
docker inspect -f '{{.State.Health.Status}}' js-mysql-customer360   # MySQL unaffected
```

---

## 3. What stopping does and does not do

| | Effect |
| --- | --- |
| Iceberg tables | **Kept.** Everything already written stays queryable |
| MySQL Customer 360 | **Unaffected** — a separate container |
| Federated demo / Streamlit app | **Still works**, just against a frozen dataset |
| Spark checkpoints | **Kept.** Deleting an instance does not delete its checkpoint, which is why restarts resume instead of reprocessing |
| Kafka backlog | Retained for 24 h, then deleted — the cause of the §1a gap |

---

## 4. Quick reference

```bash
# START
MAX_OFFSETS=1000000 ./scripts/submit_aidp.sh events
MAX_OFFSETS=1000000 ./scripts/submit_aidp.sh payments
MAX_OFFSETS=1000000 ./scripts/submit_aidp.sh aggregates
nohup .venv/bin/python generators/Streaming_gen.py \
  --customer-file data/customer_accounts.csv --bootstrap-server 172.18.1.177:9092 \
  --seed 42 --pid-file /tmp/streaming_gen.pid > /tmp/streaming_gen.log 2>&1 &

# CHECK
python scripts/validate_federated_demo.py

# BATCH (finite job — reference tables and the daily rollup; see
# docs/BATCH_INGESTION_DEMO.md. Independent of the streaming jobs above.)
./scripts/submit_batch_aidp.sh all

# STOP
kill -TERM "$(cat /tmp/streaming_gen.pid)"
$CLI --insecure --format=json instance list 2>/dev/null | sed -n '/\[/,$p' | \
python3 -c "import sys,json;[print(i['instanceId']) for i in json.load(sys.stdin) if i['type']!='NOTEBOOK']" | \
while read -r id; do echo y | $CLI --insecure instance delete "$id"; done
```

---

## 5. Common failures

| Symptom | Cause | Fix |
| --- | --- | --- |
| Jobs `RUNNING`, Iceberg not growing, snapshots show `changed-partition-count = 0` | Checkpoint below Kafka's earliest retained offset | Resubmit with `MAX_OFFSETS=1000000` (§1a) |
| `You are not logged in or your access token has expired` | DDPE token idle > 30 min | Re-run the login (§0) |
| `instance delete` throws `NullPointerException` | Interactive prompt, empty stdin | Pipe `echo y \|` |
| `kill -INT` does nothing (older builds) | Background job inherited `SIGINT = SIG_IGN` | Use `kill -TERM`, or update to the build with signal handlers |
| `Libraries for snappy compression codec not found` | `python-snappy` missing | `pip install python-snappy`, or run with `--compression gzip` |
| Generator runs but match rate ~0% | Started without `--customer-file` | Restart with the account pool CSV |
| `instance logs` frozen at startup | It returns a one-off snapshot, not a live tail | Judge progress from Iceberg snapshots instead |
