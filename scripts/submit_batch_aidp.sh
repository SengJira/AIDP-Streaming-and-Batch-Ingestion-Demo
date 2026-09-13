#!/usr/bin/env bash
# Submit the batch ingestion pipeline to Dell AIDP via the
# dell-data-processing-engine CLI.
#
# Companion to scripts/submit_aidp.sh (which runs the Kafka streaming job).
# This one is a finite job: it reads three sources, upserts them into Iceberg
# and exits, so it is meant to be run on a schedule rather than left running.
#
# One-time setup
# --------------
#   0. The endpoint's TLS cert is not in the JVM trust store. Either export
#      DDPE_CERT=/path/to/ca.pem (preferred) or DDPE_INSECURE=1 (testing only).
#
#   1. Log in (access tokens expire, so this is not strictly one-time):
#        dell-data-processing-engine --insecure login
#
#   2. Stage the JDBC drivers and the landing-zone CSV:
#        S3_ACCESS_KEY=... S3_SECRET_KEY=... ./scripts/stage_batch_assets.py --all
#
#   3. Upload credentials as a secret set. They are injected as environment
#      variables into the driver/executor containers BEFORE the JVM starts,
#      which is what Iceberg's GlueCatalog needs. Values must be base64
#      encoded, and note the CLI persists submit configuration by default — so
#      never pass keys via --conf.
#
#        dell-data-processing-engine uploads create-secret \
#          --comment "batch ingestion credentials" \
#          AWS_ACCESS_KEY_ID=$(printf %s "$GLUE_KEY"    | base64 -w0) \
#          AWS_SECRET_ACCESS_KEY=$(printf %s "$GLUE_SECRET" | base64 -w0) \
#          AWS_REGION=$(printf %s "us-east-1"           | base64 -w0) \
#          S3_ACCESS_KEY=$(printf %s "$S3_KEY"          | base64 -w0) \
#          S3_SECRET_KEY=$(printf %s "$S3_SECRET"       | base64 -w0) \
#          AIDP_USERNAME=$(printf %s "$AIDP_USER"       | base64 -w0) \
#          AIDP_PASSWORD=$(printf %s "$AIDP_PW"         | base64 -w0)
#
#      AIDP_USERNAME / AIDP_PASSWORD are what the job uses to read the
#      js_mysql_customer360 data catalog over the Trino JDBC driver. Add
#      MYSQL_USER / MYSQL_PASSWORD only if you intend to run
#      CUSTOMERS_READER=mysql, which bypasses the catalog.
#
#      Note the returned upload id (looks like m-xxxxxxxx) and export it:
#        export SECRET_ID=m-xxxxxxxx
#
# Usage
# -----
#   SECRET_ID=m-xxxxxxxx ./scripts/submit_batch_aidp.sh [all|customers|accounts|payments_daily]
#
#   MODE=full        ./scripts/submit_batch_aidp.sh customers   # ignore the watermark
#   DRY_RUN=1        ./scripts/submit_batch_aidp.sh all         # read + count, no writes
#   SINCE=2026-09-01 ./scripts/submit_batch_aidp.sh customers   # explicit watermark
#
# Monitoring
# ----------
#   dell-data-processing-engine instance list
#   dell-data-processing-engine instance status <instance-id>
#   dell-data-processing-engine instance logs   <instance-id>
set -euo pipefail

SOURCE="${1:-all}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JOB_FILE="$REPO_ROOT/jobs/batch_ingestion_job.py"

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
GLOBAL_OPTS=()
[[ -n "${DDPE_CERT:-}" ]] && GLOBAL_OPTS+=(--cert "$DDPE_CERT")
[[ "${DDPE_INSECURE:-0}" == "1" ]] && GLOBAL_OPTS+=(--insecure)

: "${SECRET_ID:?Set SECRET_ID to the upload id from 'uploads create-secret' (see header)}"

# SECRET_ID may be a comma-separated list: --uploaded-secrets is repeatable, so
# the AIDP catalog credentials can live in their own secret set instead of being
# duplicated into the one the streaming job already uses.
SECRET_ARGS=()
IFS=',' read -ra _secret_ids <<<"$SECRET_ID"
for _id in "${_secret_ids[@]}"; do
  [[ -n "$_id" ]] && SECRET_ARGS+=(--uploaded-secrets "$_id")
done

# Executor sizing. The batch is small (10k customers, a daily rollup over the
# event table) but the rollup shuffles, so give it slightly more than the
# streaming job's per-executor share and let it finish quickly.
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-3G}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
NUM_EXECUTORS="${NUM_EXECUTORS:-2}"
RESOURCE_POOL="${RESOURCE_POOL:-}"

# JDBC drivers are absent from the image; see scripts/stage_batch_assets.py.
JARS_PREFIX="${JARS_PREFIX:-s3a://js-demo/jars}"
TRINO_JDBC_VERSION="${TRINO_JDBC_VERSION:-476}"
MYSQL_CONNECTOR_VERSION="${MYSQL_CONNECTOR_VERSION:-8.4.0}"
JDBC_JARS="${JARS_PREFIX}/trino-jdbc-${TRINO_JDBC_VERSION}.jar"
JDBC_JARS+=",${JARS_PREFIX}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar"
EXTRA_JARS="${EXTRA_JARS:-$JDBC_JARS}"

# Credentials for the s3a:// jar download itself. This happens in spark-submit
# before the application runs, so it cannot use the job's own catalog config.
# --save-configuration=false keeps these keys out of the stored instance config.
: "${S3_ACCESS_KEY:?Set S3_ACCESS_KEY (needed to fetch jars from ${JARS_PREFIX})}"
: "${S3_SECRET_KEY:?Set S3_SECRET_KEY (needed to fetch jars from ${JARS_PREFIX})}"
S3_ENDPOINT="${S3_ENDPOINT:-http://172.18.11.31:9020}"

# Uploaded files land under /opt/spark/uploads/<destination>/
UPLOAD_DEST="ingestion"
APP_PATH="local:///opt/spark/uploads/${UPLOAD_DEST}/$(basename "$JOB_FILE")"

args=(
  "${GLOBAL_OPTS[@]}"
  submit
  --name "batch-ingestion-${SOURCE//_/-}"
  --file-upload "${JOB_FILE}:/${UPLOAD_DEST}"
  "${SECRET_ARGS[@]}"
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

# Optional extra Spark confs as space-separated name=value pairs, e.g.
# EXTRA_CONFS="spark.kubernetes.driver.request.cores=900m" when the pool is
# short on free vcores.
for _kv in ${EXTRA_CONFS:-}; do args+=(--conf "$_kv"); done

[[ -n "$RESOURCE_POOL" ]] && args+=(--pool "$RESOURCE_POOL")
[[ -n "$EXTRA_JARS" ]] && args+=(--jars "$EXTRA_JARS")

# Endpoints go through as APPLICATION arguments, not --conf: the CLI rejects
# spark.kubernetes.driverEnv.* as reserved configuration. Credentials are never
# passed here — they arrive as env vars from the secret set.
args+=(
  "$APP_PATH"
  --source          "$SOURCE"
  --mode            "${MODE:-incremental}"
  --lookback-days   "${LOOKBACK_DAYS:-1}"
  --num-partitions  "${NUM_PARTITIONS:-1}"
  --customers-reader "${CUSTOMERS_READER:-starburst}"
  --aidp-host       "${AIDP_HOST:-ddae.lab9bgp.com}"
  --mysql-host      "${MYSQL_HOST:-172.18.1.177}"
  --mysql-port      "${MYSQL_PORT:-3306}"
  --accounts-path   "${ACCOUNTS_PATH:-s3a://js-demo/landing/accounts}"
  --s3-endpoint     "${S3_ENDPOINT}"
  --glue-endpoint   "${GLUE_ENDPOINT:-http://managed-metastore.ddae.svc.cluster.local:8080/api/v1/glue}"
  --glue-catalog-id "${GLUE_CATALOG_ID:-js_banking_ice}"
  --catalog         "${CATALOG:-js_financial_ice}"
  # Targets go to their own schema; the rollup still reads the streaming job's
  # payment_transactions out of PAYMENTS_DB.
  --db              "${DB:-ingestion}"
  --payments-db     "${PAYMENTS_DB:-banking}"
)

# The coordinator's certificate is issued by a private CA (CN=TitanCA). Point
# AIDP_TRUSTSTORE at a trust store containing it (preferred, password in
# AIDP_TRUSTSTORE_PASSWORD, which belongs in the secret set), or set
# AIDP_VERIFY_TLS=false for lab testing only.
[[ -n "${AIDP_TRUSTSTORE:-}" ]] && args+=(--aidp-truststore "$AIDP_TRUSTSTORE")
[[ "${AIDP_VERIFY_TLS:-true}" == "false" ]] && args+=(--no-aidp-verify-tls)
[[ -n "${SINCE:-}" ]] && args+=(--since "$SINCE")
[[ "${DRY_RUN:-0}" == "1" ]] && args+=(--dry-run)

echo "+ $CLI ${args[*]}"
exec "$CLI" "${args[@]}"
