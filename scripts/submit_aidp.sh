#!/usr/bin/env bash
# Submit the banking streaming pipeline to Dell AIDP via the
# dell-data-processing-engine CLI.
#
# The driver runs in a cluster pod sized by the resource pool — not in the
# notebook container, which is capped at 1 GB and was being OOM-killed.
#
# One-time setup
# --------------
#   0. The endpoint's TLS cert is not in the JVM trust store. Either export
#      DDPE_CERT=/path/to/ca.pem (preferred) or DDPE_INSECURE=1 (testing only).
#      Both are applied to the CLI as global options by this script.
#
#   1. Log in (access tokens expire, so this is not strictly one-time):
#        dell-data-processing-engine --insecure login
#
#   2. Upload credentials as a secret set. These are injected as environment
#      variables into the driver/executor containers BEFORE the JVM starts,
#      which is what Iceberg's GlueCatalog needs (it reads the AWS SDK
#      credential chain and ignores catalog-scoped client keys).
#      Values must be base64 encoded, and note the CLI persists submit
#      configuration by default — so never pass keys via --conf.
#
#        dell-data-processing-engine uploads create-secret \
#          --comment "banking demo credentials" \
#          AWS_ACCESS_KEY_ID=$(printf %s "$GLUE_KEY"    | base64 -w0) \
#          AWS_SECRET_ACCESS_KEY=$(printf %s "$GLUE_SECRET" | base64 -w0) \
#          AWS_REGION=$(printf %s "us-east-1"           | base64 -w0) \
#          S3_ACCESS_KEY=$(printf %s "$S3_KEY"          | base64 -w0) \
#          S3_SECRET_KEY=$(printf %s "$S3_SECRET"       | base64 -w0)
#
#      Note the returned upload id (looks like m-xxxxxxxx) and export it:
#        export SECRET_ID=m-xxxxxxxx
#
# Usage
# -----
#   SECRET_ID=m-xxxxxxxx ./scripts/submit_aidp.sh [all|payments|aggregates|events]
#
# Monitoring
# ----------
#   dell-data-processing-engine instance list
#   dell-data-processing-engine instance status <instance-id>
#   dell-data-processing-engine instance logs   <instance-id>
#   dell-data-processing-engine instance delete <instance-id>   # stops the job
set -euo pipefail

STREAM="${1:-all}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JOB_FILE="$REPO_ROOT/jobs/banking_streaming_job.py"

# Locate the CLI: PATH first, then the usual unpacked location.
CLI="${DDPE_CLI:-}"
if [[ -z "$CLI" ]]; then
  for candidate in \
    "$REPO_ROOT/dell-data-processing-engine/bin/dell-data-processing-engine" \
    "$HOME/vlo-dev/dell-data-processing-engine/bin/dell-data-processing-engine"
  do
    [[ -x "$candidate" ]] && CLI="$candidate" && break
  done
  if [[ -z "$CLI" ]] && command -v dell-data-processing-engine >/dev/null 2>&1; then
    CLI="dell-data-processing-engine"
  fi
  if [[ -z "$CLI" ]]; then
    echo "dell-data-processing-engine CLI not found — set DDPE_CLI to its path" >&2
    exit 1
  fi
fi

# Global options must precede the subcommand.
# The DDPE endpoint presents a certificate the JVM does not trust, so either
# point DDPE_CERT at the CA/server cert (preferred) or set DDPE_INSECURE=1 to
# skip validation. Without one of these the CLI fails with
# "PKIX path building failed".
GLOBAL_OPTS=()
[[ -n "${DDPE_CERT:-}" ]] && GLOBAL_OPTS+=(--cert "$DDPE_CERT")
[[ "${DDPE_INSECURE:-0}" == "1" ]] && GLOBAL_OPTS+=(--insecure)

: "${SECRET_ID:?Set SECRET_ID to the upload id from 'uploads create-secret' (see header)}"

# Executor sizing. Three queries at demo volumes are light; the point of moving
# off the notebook is the 1 GB cap, not raw throughput. Peak concurrent tasks
# is roughly queries x shuffle partitions, so 2 executors x 2 cores keeps all
# three progressing when STREAM=all.
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2G}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
NUM_EXECUTORS="${NUM_EXECUTORS:-2}"
RESOURCE_POOL="${RESOURCE_POOL:-}"

# Jars. The AIDP image ships Iceberg (/opt/spark/iceberg-jars) plus hadoop-aws
# and aws-java-sdk-bundle, but NO Kafka connector — without these four jars the
# job dies with "Failed to find data source: kafka".
#
# They cannot be shipped with --file-upload either: kafka-clients is 4.9 MB and
# the cap is 1 MB per file. So they are staged in object storage (see
# scripts/stage_kafka_jars.py) and fetched by Spark at startup over s3a://,
# which works because hadoop-aws IS in the image.
JARS_PREFIX="${JARS_PREFIX:-s3a://js-demo/jars}"
KAFKA_JARS="${JARS_PREFIX}/spark-sql-kafka-0-10_2.12-3.5.7.jar"
KAFKA_JARS+=",${JARS_PREFIX}/spark-token-provider-kafka-0-10_2.12-3.5.7.jar"
KAFKA_JARS+=",${JARS_PREFIX}/kafka-clients-3.4.1.jar"
KAFKA_JARS+=",${JARS_PREFIX}/commons-pool2-2.11.1.jar"
EXTRA_JARS="${EXTRA_JARS:-$KAFKA_JARS}"

# Credentials for the s3a:// jar download itself. This happens in spark-submit
# before the application runs, so it cannot use the job's own catalog config —
# it needs Hadoop-level properties. --save-configuration=false keeps these keys
# out of the instance configuration the CLI otherwise stores.
: "${S3_ACCESS_KEY:?Set S3_ACCESS_KEY (needed to fetch jars from ${JARS_PREFIX})}"
: "${S3_SECRET_KEY:?Set S3_SECRET_KEY (needed to fetch jars from ${JARS_PREFIX})}"
S3_ENDPOINT="${S3_ENDPOINT:-http://172.18.11.31:9020}"

# Uploaded files land under /opt/spark/uploads/<destination>/
UPLOAD_DEST="banking"
APP_PATH="local:///opt/spark/uploads/${UPLOAD_DEST}/$(basename "$JOB_FILE")"

args=(
  "${GLOBAL_OPTS[@]}"
  submit
  --name "banking-streaming-${STREAM}"
  --file-upload "${JOB_FILE}:/${UPLOAD_DEST}"
  --uploaded-secrets "${SECRET_ID}"
  --executor-memory "${EXECUTOR_MEMORY}"
  --executor-cores "${EXECUTOR_CORES}"
  --num-executors "${NUM_EXECUTORS}"
  --save-configuration=false
  --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}"
  --conf "spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY}"
  --conf "spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY}"
  --conf "spark.hadoop.fs.s3a.path.style.access=true"
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false"
)

[[ -n "$RESOURCE_POOL" ]] && args+=(--pool "$RESOURCE_POOL")
[[ -n "$EXTRA_JARS" ]] && args+=(--jars "$EXTRA_JARS")

# Endpoints go through as APPLICATION arguments, not --conf: the CLI rejects
# spark.kubernetes.driverEnv.* as reserved configuration. Credentials are never
# passed here — they arrive as env vars from the secret set.
# maxOffsetsPerTrigger. Raise this when the checkpoint is far behind Kafka's
# earliest retained offset: the source skips the expired range at only
# maxOffsetsPerTrigger per micro-batch, so the default 5000 can mean an hour of
# empty batches before live data is reached. A large value lets a single batch
# span the gap. Check for that condition with:
#   kafka-get-offsets.sh --topic payments.raw --time earliest
# and compare against the checkpoint under
#   s3://js-demo/warehouse/banking/checkpoints-10-246-25-115/<stream>/offsets/
MAX_OFFSETS="${MAX_OFFSETS:-5000}"
# earliest | latest. Only applies on a FRESH checkpoint; an existing checkpoint
# always wins, which is why the offset gap above has to be handled explicitly.
STARTING_OFFSETS="${STARTING_OFFSETS:-latest}"

# Checkpoint root for the streaming state. The default is the namespace for the
# Kafka broker on the new demo host (172.18.1.177) - checkpoints carry broker
# offsets, so a new broker needs a fresh namespace. The pre-migration
# checkpoints under s3://js-demo/warehouse/banking/checkpoints are untouched.
CHECKPOINT="${CHECKPOINT:-s3://js-demo/warehouse/banking/checkpoints-10-246-25-115}"

args+=(
  "$APP_PATH"
  --stream "$STREAM"
  --max-offsets      "${MAX_OFFSETS}"
  --starting-offsets "${STARTING_OFFSETS}"
  --kafka-brokers   "${KAFKA_BROKERS:-172.18.1.177:9092}"
  --checkpoint      "${CHECKPOINT}"
  --s3-endpoint     "${S3_ENDPOINT:-http://172.18.11.31:9020}"
  --glue-endpoint   "${GLUE_ENDPOINT:-http://managed-metastore.ddae.svc.cluster.local:8080/api/v1/glue}"
  --glue-catalog-id "${GLUE_CATALOG_ID:-js_banking_ice}"
  --catalog         "${CATALOG:-js_financial_ice}"
  --db              "${DB:-banking}"
)

echo "+ $CLI ${args[*]}"
exec "$CLI" "${args[@]}"
