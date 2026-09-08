# AIDP Real-Time Banking Fraud Analytics

> **Federated Customer 360 demo:** the app also has a second page that joins
> MySQL customer master data with the Iceberg banking tables in a single
> Starburst query, without copying either side. Setup, SQL and troubleshooting
> are in [docs/FEDERATED_QUERY_DEMO.md](docs/FEDERATED_QUERY_DEMO.md), and the
> day-to-day start/stop procedure for the Kafka generator and Spark streaming
> jobs is in [docs/RUNBOOK.md](docs/RUNBOOK.md).
>
> **Batch ingestion demo:** `jobs/batch_ingestion_job.py` runs on the AIDP Spark
> engine and ingests three different kinds of source — the
> `js_mysql_customer360` data catalog over JDBC, CSV files in the object-storage
> landing zone, and the Iceberg event table itself — into
> `js_financial_ice.ingestion` with idempotent `MERGE` upserts and a run log. See
> [docs/BATCH_INGESTION_DEMO.md](docs/BATCH_INGESTION_DEMO.md).

A Streamlit application that connects securely to a Dell AI Data Platform (AIDP /
Starburst Enterprise) endpoint with **PyStarburst** and answers a fixed,
allowlisted set of questions about the banking fraud data in
`js_financial_ice.banking`.

The application is **read-only by construction**: user text is matched against an
explainable intent registry and is never concatenated into SQL. There is no code
path that can emit `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DROP`, `CREATE`,
`ALTER`, `TRUNCATE`, `GRANT` or `REVOKE`.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export AIDP_ENDPOINT="https://ddae.lab9bgp.com/"
export AIDP_USERNAME="jirawut"
export AIDP_PASSWORD="<set-locally>"
export AIDP_CATALOG="js_financial_ice"
export AIDP_SCHEMA="banking"
export AIDP_VERIFY_TLS="true"

streamlit run fraud_analytics_app.py
```

Alternatively `cp .env.example .env` and fill in the password there. `.env` is
git-ignored and must never be committed.

> **On this lab endpoint TLS verification will fail** with `AIDP_VERIFY_TLS=true`:
> the coordinator's certificate is issued by a private CA (`CN=TitanCA`) that is
> not in the system trust store. Either install that CA
> (`export REQUESTS_CA_BUNDLE=/path/to/titan-ca.pem`) — preferred — or use
> `AIDP_VERIFY_TLS=false` for lab testing only. The default stays `true`
> deliberately; the app warns loudly whenever verification is off.

## Files

| File | Purpose |
| --- | --- |
| `fraud_analytics_app.py` | Streamlit UI: connection status, question box, suggestions, query expander, table, chart, summary |
| `aidp_connection.py` | Config loading, `Secret` wrapper, log redaction, session creation, error classification |
| `fraud_queries.py` | Intent registry, query-plan IR, SQL renderer, PyStarburst compiler, result summaries |
| `schema_inspector.py` | Table/column allowlist, live schema validation, join-match-rate measurement |
| `scripts/check_join_match_rate.py` | Standalone diagnostic for cross-table `account_id` overlap |
| `jobs/banking_streaming_job.py` | Spark Structured Streaming: Kafka -> Iceberg (runs on AIDP) |
| `jobs/batch_ingestion_job.py` | Spark batch ingestion: data catalog + landing files + Iceberg rollup -> Iceberg, upserted |
| `scripts/submit_batch_aidp.sh` | Submits the batch job to the AIDP Spark engine |
| `scripts/stage_batch_assets.py` | Stages the JDBC drivers and the landing-zone CSV in object storage |
| `sql/Iceberg_Batch_Tables.sql` | Reference DDL for the batch targets (`dim_*`, `fact_*`, `ingestion_audit`) |
| `sql/07_batch_ingestion_validation.sql` | Post-run validation: idempotency, source agreement, lineage |
| `tests/test_fraud_queries.py` | Offline unit tests (no AIDP connection required) |
| `tests/test_batch_ingestion.py` | Offline unit tests for the batch job (needs `requirements-spark.txt`) |

## Supported questions

The first version deliberately contains **no LLM**. `fraud_queries.match_intent`
is an explainable keyword/pattern matcher: each intent declares required keyword
OR-groups, score boosts and exclusions, and the match result reports exactly
which terms fired plus the runners-up.

| Question | Intent | Table |
| --- | --- | --- |
| How many fraud alerts occurred in the last hour? | `fraud_count_last_hour` | `fraud_alerts` |
| Show fraud alerts by fraud type. | `fraud_by_type` | `fraud_alerts` |
| Show fraud alerts by action. | `fraud_by_action` | `fraud_alerts` |
| Which accounts have the highest fraud scores? | `top_accounts_by_score` | `fraud_alerts` |
| Show the latest high-risk fraud alerts. | `latest_high_risk` | `fraud_alerts` |
| What is the average fraud score by fraud type? | `avg_score_by_type` | `fraud_alerts` |
| How many alerts were blocked, flagged for review, or required step-up authentication? | `action_outcome_counts` | `fraud_alerts` |
| Show fraud trends by hour. | `fraud_trend_by_hour` | `fraud_alerts` |
| Show the ten most recent fraud alerts. | `recent_fraud_alerts` | `fraud_alerts` |
| How many AML alerts exist by risk band? | `aml_by_risk_band` | `aml_alerts` |
| Show successful and failed customer logins. | `login_outcomes` | `customer_events` |
| How many customer logins used a new device? | `new_device_logins` | `customer_events` |

Anything else is refused with the list of supported questions. Requests that look
like data modification are refused with an explicit read-only message.

### Adding an LLM later

`match_intent(question) -> MatchResult` is the only natural-language component.
To add an LLM question-to-SQL stage, replace or precede it with something that
emits an intent name (or a `QueryPlan` directly). Because `QueryPlan`,
`Filter`, `GroupKey` and `Aggregate` re-validate every identifier, literal,
aggregate function, sort direction and row limit in `__post_init__`, an LLM
cannot widen the blast radius beyond the existing allowlist.

## Query rules enforced in code

* Fully qualified, double-quoted names: `"js_financial_ice"."banking"."fraud_alerts"`.
* Identifiers must be `[a-z0-9_]` and present in `schema_inspector.EXPECTED_COLUMNS`; anything else raises `SchemaValidationError`.
* String literals in predicates must come from `ALLOWED_LITERALS` (e.g. `action IN ('BLOCK_AND_ALERT','FLAG_REVIEW','STEP_UP_AUTH')`).
* "Last hour" is `"timestamp" >= current_timestamp - INTERVAL '1' HOUR`, and ignores the sidebar time range by design. All timestamps are UTC-aware (`TIMESTAMP(6) WITH TIME ZONE` on the server, `current_timestamp` evaluated server-side).
* High-risk default is `ml_score >= 0.85`, configurable in the sidebar and validated to `[0.0, 1.0]`.
* Recent alerts use `ORDER BY "timestamp" DESC`.
* Every non-scalar query carries a `LIMIT`; default 100, hard cap 1000 (`MAX_LIMIT`).
* `assert_read_only()` is the final gate: it rejects multiple statements, SQL comments, anything not starting with `SELECT`, and any forbidden keyword.
* Metadata is cached for 5 minutes (`@st.cache_data(ttl=300)`); **live fraud results are never cached**.

## Security

* The password is read **only** from `AIDP_PASSWORD`. It is never hard-coded, never a CLI argument, and never a default.
* It is held in a `Secret` wrapper whose `__repr__`, `__str__` and `__format__` return `***REDACTED***`, so accidental `print`/f-string/dataclass-repr usage cannot leak it. `AidpConfig` is safe to dump.
* `SecretRedactingFilter` is attached to the root, `aidp`, `trino`, `pystarburst`, `urllib3` and `requests` loggers and scrubs the secret from messages, args and exception arguments.
* `AIDP_ENDPOINT` must be `https://`; plain HTTP and URLs with embedded credentials (`https://user:pw@host/`) are refused. Only `host:port` is ever displayed, never a URL containing credentials.
* Server error text is summarized to one line (`_summarize_server_error`) so multi-KB Java stack traces are not dumped into the UI.
* TLS verification is **on by default** and can only be disabled with `AIDP_VERIFY_TLS=false`, which logs a warning and shows a persistent banner stating it is for lab testing only.
* `.gitignore` covers `.env`, `.env.*` (except `.env.example`), `*.pem`, `*.p12` and `secrets.*`.

## Known issue: PyStarburst 0.14.1 + pydantic 2.12.5

**Every DataFrame operation fails out of the box** on this stack with:

```
TypeError: 'MockValSer' object cannot be converted to 'SchemaSerializer'
```

PyStarburst serialises its logical plans with pydantic models that use forward
references, and `pystarburst/__init__.py` calls `model_rebuild()` on a
hand-curated list of them. That list is incomplete for the pydantic version
PyStarburst itself pins (`pydantic>=2.12.5,<2.13`): the base classes
`Expression`, `LogicalPlan`, `DataType`, `UnaryNode`, the window frame types and
~50 others are left with `__pydantic_complete__ == False`.

`aidp_connection.ensure_pystarburst_models_built()` imports every PyStarburst
submodule and rebuilds whatever is still incomplete (53 models on this stack). It
is idempotent, runs once inside `create_session()`, and only finalises type
resolution that PyStarburst intended to do itself. Remove it if a future
PyStarburst release fixes the rebuild list.

Note `session.sql(...)`, `SHOW TABLES` and `DataFrame.schema` work *without* the
shim — only plan serialisation is affected, which is why the failure looks like a
query bug rather than an import error.

## PyStarburst fallbacks

Primary data access is the PyStarburst DataFrame API:
`session.table([catalog, schema, table]).filter(...).group_by(...).agg(...).sort(...).limit(...).to_pandas()`.
All 12 questions run this way against the live cluster; the fallback below was
not needed there.

Three places need something else, all documented in code:

1. **`INTERVAL` literals.** PyStarburst 0.14.1 exposes no interval constructor, so
   the "last N hours" predicate uses `functions.sql_expr("current_timestamp -
   INTERVAL 'N' HOUR")`. `N` is an integer validated by `validate_hours()`, never
   user text.
2. **Multi-key sorting.** `DataFrame.sort()` *replaces* any previous ordering
   rather than appending to it, so `compile_plan` must pass every sort key in a
   single `sort()` call. Doing it in a loop silently degrades a two-key
   `ORDER BY` to the last key only (this was a real bug, caught against live
   data, and is now covered by
   `test_compile_plan_issues_exactly_one_sort_for_multiple_keys`).
3. **The official Trino client as an execution fallback.** PyStarburst 0.14.1
   resolves *every* DataFrame logical plan **server-side**, via either
   `/v1/dataframe/plan` or a `dataframe` table function
   (`pystarburst/_internal/analyzer/dataframe_api_client.py`). If that facility is
   not enabled on the cluster, the DataFrame API cannot run at all. The app
   detects this (`is_dataframe_api_unavailable`) and falls back **once**, for
   execution only, to `trino.dbapi` running the rendered statement via
   `execute_plan_sql()` — after `assert_read_only()` has re-validated it. The UI
   shows a notice when this happens. No parameters are bound because the
   statement contains no user text, only allowlisted identifiers and validated
   numeric/enum literals.

Metadata listing uses PyStarburst throughout: `session.sql("SHOW TABLES FROM ...")`
and `session.table(...).schema.fields`.

## Data constraint: do not correlate across tables

`generators/Streaming_gen.py` calls `rand_account()` **independently** for payment,
fraud, AML and customer events, drawing from `ACC-TH-{10000000..99999999}`
(~90 million values). Records in different tables therefore almost never describe
the same customer.

**No intent in this application performs a cross-table join**, and the app shows
this caveat in the access-check panel. Before adding any correlation, measure the
real overlap:

```bash
python scripts/check_join_match_rate.py --sample-rows 5000
```

Measured against the live cluster (5,000 distinct left-hand accounts per pair):

| Pair | Distinct left accounts | Matched | Match rate |
| --- | --- | --- | --- |
| `fraud_alerts × payment_transactions` | 5000 | 0 | 0.0000% |
| `fraud_alerts × aml_alerts` | 5000 | 0 | 0.0000% |
| `fraud_alerts × customer_events` | 5000 | 1 | 0.0200% |
| `aml_alerts × customer_events` | 5000 | 0 | 0.0000% |

This confirms the IDs are independently generated. The single match is a
birthday-style coincidence in a ~90M keyspace, not a shared customer. **Do not
present rows from different tables as the same customer.**

## Testing

```bash
.venv/bin/python -m pytest tests/ -q
```

The batch-ingestion tests import the job module, so they need PySpark; without
it they skip rather than fail:

```bash
pip install -r requirements-spark.txt
.venv/bin/python -m pytest tests/test_batch_ingestion.py -q
```

153 offline tests (no AIDP connection needed) cover: supported questions mapping
to the correct intent, every registered example matching itself, unsupported and
destructive questions being refused, generated SQL being a single read-only
`SELECT`, fully-qualified quoted names, the required `INTERVAL '1' HOUR`
predicate, the 0.85 high-risk default, score/limit/time-range validation, the
1000-row hard cap, table/column/literal allowlists, unsafe identifier rejection,
credential hygiene (`Secret`, `AidpConfig` repr, log redaction incl. format-arg
type preservation, missing password, plain HTTP, embedded credentials, TLS
default), error classification for every failure mode, `compile_plan`'s DataFrame
call sequence via a recording fake session, the Trino fallback path, and two
Streamlit `AppTest` UI smoke tests.

### Live verification

Verified against `ddae.lab9bgp.com` (Starburst Enterprise, `starburst-dataframe`
0.8.0) on 2026-09-04: connection OK, all 5 tables visible with all expected
columns, and **12/12 questions answered through the Streamlit UI** via
PyStarburst (charts rendered for the 8 aggregation intents; a `drop table ...`
input was refused without querying).

Formatting and linting:

```bash
pip install ruff
ruff format --line-length 100 .
ruff check --line-length 100 --select E,F,W,I,UP,B --ignore E501 .
```

## Troubleshooting

| Symptom | `kind` | Action |
| --- | --- | --- |
| `certificate verify failed: unable to get local issuer certificate` | `tls` | **Expected on this lab endpoint.** The coordinator presents `CN=SEP` (SAN `ddae.lab9bgp.com`, `172.18.1.72`) issued by a private CA `CN=TitanCA`, which is not in the system trust store. Obtain the TitanCA root out-of-band and `export REQUESTS_CA_BUNDLE=/path/to/titan-ca.pem`, or set `AIDP_VERIFY_TLS=false` for lab testing only. |
| `password authenticator (Keycloak) rejected these credentials` | `auth` | Re-export `AIDP_PASSWORD`. Note the coordinator returns **HTTP 500** wrapping an internal 401, not a 401. |
| `Access denied` / HTTP 403 | `authz` | RBAC: ask an admin to grant `SELECT` on `js_financial_ice.banking` to the user/role. |
| `does not exist` | `missing_object` | Check `AIDP_CATALOG`/`AIDP_SCHEMA`; create the tables with `sql/Iceberg_Table.sql`. |
| DNS / timeout / connection refused | `dns`, `timeout`, `network` | Check VPN, firewall and that `AIDP_ENDPOINT` resolves. |
| `PyStarburst ... is missing APIs` | `version` | `pip install -r requirements.txt` (pinned to `pystarburst==0.14.1`). |
| `'MockValSer' object cannot be converted to 'SchemaSerializer'` | – | Handled automatically by `ensure_pystarburst_models_built()`; see "Known issue" above. |
| Empty result table | – | Widen the sidebar time range and press **Refresh**; the stream may be idle. |
