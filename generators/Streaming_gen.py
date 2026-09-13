#!/usr/bin/env python3
"""Standalone banking event producer for Kafka.

Events are drawn from a *stable customer pool* (``data/customer_accounts.csv``,
produced by ``scripts/generate_customer360.py`` and loaded into MySQL by
``scripts/load_customer360.py``). Using the same account IDs on both sides is
what makes the AIDP federated join between MySQL Customer 360 and the Iceberg
banking tables meaningful.

A small, configurable share of events (default 1%) deliberately uses accounts
that do *not* exist in MySQL, so the demo can also show unmatched-record
detection and data-quality reporting.

    pip install kafka-python

Usage
-----
    python generators/Streaming_gen.py \\
        --customer-file data/customer_accounts.csv \\
        --bootstrap-server 172.18.1.177:9092 \\
        --seed 42

Run without ``--customer-file`` to keep the original behaviour of fully random
account IDs (backward compatible, but federated joins will not match).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import signal
import sys
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

BOOTSTRAP = "172.18.1.177:9092"

TOPIC_PAYMENTS = "payments.raw"
TOPIC_FRAUD = "fraud.signals"
TOPIC_CUSTOMER = "customer.events"

CITIES = ["Bangkok", "Chiang Mai", "Pattaya", "Phuket", "Khon Kaen"]
CITY_WEIGHTS = [0.55, 0.15, 0.12, 0.10, 0.08]
CITY_COORDS = {
    "Bangkok": (13.7563, 100.5018),
    "Chiang Mai": (18.7883, 98.9853),
    "Pattaya": (12.9236, 100.8825),
    "Phuket": (7.8804, 98.3923),
    "Khon Kaen": (16.4419, 102.8360),
}
CHANNELS = ["POS", "MOBILE_APP", "ONLINE_BANKING", "ATM", "BRANCH"]
CH_WEIGHTS = [0.40, 0.30, 0.18, 0.08, 0.04]
MCCS = [
    "RETAIL",
    "FOOD_BEVERAGE",
    "TRAVEL",
    "HEALTHCARE",
    "UTILITIES",
    "ENTERTAINMENT",
    "EDUCATION",
    "FUEL",
]
TIERS = ["PLATINUM", "GOLD", "SILVER", "STANDARD"]
TIER_WEIGHTS = [0.05, 0.20, 0.40, 0.35]
FRAUD_TYPES = [
    "CARD_NOT_PRESENT",
    "ACCOUNT_TAKEOVER",
    "IDENTITY_THEFT",
    "SYNTHETIC_IDENTITY",
]

DEFAULT_UNKNOWN_RATE = 0.01
#: Unknown accounts are drawn from a band far above the customer pool
#: (ACC-TH-10000001..) so they can never collide with a real customer.
UNKNOWN_ACCOUNT_LOW = 90_000_000
UNKNOWN_ACCOUNT_HIGH = 99_999_999


@dataclass
class PoolCustomer:
    """One row of the account pool CSV."""

    account_id: str
    customer_tier: str | None = None
    city: str | None = None
    country: str | None = None


@dataclass
class AccountStats:
    """Known-vs-unknown account usage, reported when the producer stops."""

    per_topic: Counter = field(default_factory=Counter)
    known: int = 0
    unknown: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, topic: str, is_known: bool) -> None:
        with self.lock:
            self.per_topic[topic] += 1
            if is_known:
                self.known += 1
            else:
                self.unknown += 1

    @property
    def total(self) -> int:
        return self.known + self.unknown

    def report(self) -> str:
        total = self.total
        if not total:
            return "No events were produced."
        lines = [
            f"Total events produced : {total:,}",
            f"  known accounts      : {self.known:,} ({100.0 * self.known / total:.2f}%)",
            f"  unknown accounts    : {self.unknown:,} ({100.0 * self.unknown / total:.2f}%)",
            "Per topic:",
        ]
        lines.extend(
            f"  {topic:<18} {count:,}"
            for topic, count in sorted(self.per_topic.items())
        )
        return "\n".join(lines)


class CustomerPool:
    """The stable pool of accounts shared with MySQL Customer 360.

    ``pick()`` returns ``(PoolCustomer, is_known)``. With probability
    ``unknown_rate`` it returns a synthetic account that is absent from MySQL,
    which surfaces in the demo as an unmatched record.
    """

    def __init__(
        self,
        customers: list[PoolCustomer],
        rng: random.Random,
        unknown_rate: float = DEFAULT_UNKNOWN_RATE,
    ) -> None:
        if not customers:
            raise ValueError("Customer pool is empty")
        if not 0.0 <= unknown_rate <= 1.0:
            raise ValueError(
                f"unknown-rate must be between 0.0 and 1.0, got {unknown_rate}"
            )
        self.customers = customers
        self.rng = rng
        self.unknown_rate = unknown_rate

    def __len__(self) -> int:
        return len(self.customers)

    def pick(self) -> tuple[PoolCustomer, bool]:
        if self.rng.random() < self.unknown_rate:
            number = self.rng.randint(UNKNOWN_ACCOUNT_LOW, UNKNOWN_ACCOUNT_HIGH)
            return PoolCustomer(account_id=f"ACC-TH-{number}"), False
        return self.rng.choice(self.customers), True


class RandomPool(CustomerPool):
    """Legacy behaviour: every account is random and unknown to MySQL.

    Used when ``--customer-file`` is omitted, preserving the original script's
    output shape for backward compatibility.
    """

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.customers = []
        self.unknown_rate = 1.0

    def __len__(self) -> int:
        return 0

    def pick(self) -> tuple[PoolCustomer, bool]:
        return PoolCustomer(
            account_id=f"ACC-TH-{self.rng.randint(10000000, 99999999)}"
        ), False


def load_customer_pool(path: Path) -> list[PoolCustomer]:
    """Read the account pool CSV written by generate_customer360.py."""
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Generate it first:\n"
            "  python scripts/generate_customer360.py --customers 10000 --seed 42 "
            "--output-dir data"
        )
    customers: list[PoolCustomer] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if "account_id" not in (reader.fieldnames or []):
            raise ValueError(f"{path} must contain an 'account_id' column")
        for row in reader:
            account_id = (row.get("account_id") or "").strip()
            if not account_id:
                continue
            customers.append(
                PoolCustomer(
                    account_id=account_id,
                    customer_tier=(row.get("customer_tier") or "").strip() or None,
                    city=(row.get("city") or "").strip() or None,
                    country=(row.get("country") or "").strip() or None,
                )
            )
    if not customers:
        raise ValueError(f"{path} contained no usable account_id values")
    return customers


def make_producer(bootstrap: str, compression: str = "snappy"):
    """Create the Kafka producer.

    ``kafka`` is imported lazily so the generator's pure logic stays importable
    (for --dry-run and unit tests) on machines without kafka-python installed.

    Snappy needs the optional ``python-snappy`` package; rather than failing the
    whole run we fall back to gzip so the demo still produces data.
    """
    from kafka import KafkaProducer

    def build(codec):
        return KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            compression_type=codec,
            batch_size=16384,
            linger_ms=10,
            acks="all",
            retries=3,
        )

    try:
        return build(compression)
    except Exception as exc:  # noqa: BLE001
        if compression in (None, "gzip"):
            raise
        print(
            f"[BankingProducer] WARNING: compression '{compression}' unavailable "
            f"({exc}); falling back to gzip. Install python-snappy for snappy.",
            file=sys.stderr,
        )
        return build("gzip")


def gen_payment(pool: CustomerPool, rng: random.Random) -> tuple[dict, str, bool]:
    """Payment event. City and tier follow the customer's master record."""
    customer, is_known = pool.pick()

    # Keep the event consistent with Customer 360 where we know the customer;
    # fall back to the population distribution for unknown accounts.
    city = customer.city or rng.choices(CITIES, weights=CITY_WEIGHTS)[0]
    if city not in CITY_COORDS:
        city = rng.choices(CITIES, weights=CITY_WEIGHTS)[0]
    tier = customer.customer_tier or rng.choices(TIERS, TIER_WEIGHTS)[0]
    home_country = customer.country or "TH"

    lat, lon = CITY_COORDS[city]
    etype = rng.choices(
        ["CARD_PAYMENT", "WIRE_TRANSFER", "ATM_WITHDRAWAL", "ACH_TRANSFER"],
        [0.55, 0.20, 0.15, 0.10],
    )[0]
    amount = round(
        rng.choices([rng.uniform(50, 5000), rng.uniform(5001, 500000)], [0.85, 0.15])[
            0
        ],
        2,
    )
    is_international = rng.random() < 0.12
    country = (
        rng.choices(["SG", "MY", "JP", "US"], [0.36, 0.27, 0.19, 0.18])[0]
        if is_international
        else home_country
    )

    event = {
        "event_id": f"txn_{uuid.uuid4().hex[:8]}",
        "event_type": etype,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "transaction_id": f"TXN-{datetime.now().strftime('%Y%m%d')}-{rng.randint(1000000, 9999999)}",
        "account_id": customer.account_id,
        "amount": amount,
        "currency": rng.choices(["THB", "USD", "EUR", "SGD"], [0.80, 0.10, 0.06, 0.04])[
            0
        ],
        "channel": rng.choices(CHANNELS, CH_WEIGHTS)[0],
        "merchant_category": rng.choice(MCCS),
        "status": rng.choices(
            ["APPROVED", "DECLINED", "PENDING", "REVERSED"], [0.88, 0.08, 0.03, 0.01]
        )[0],
        "city": city,
        "country": country,
        "latitude": round(lat + rng.uniform(-0.2, 0.2), 6),
        "longitude": round(lon + rng.uniform(-0.2, 0.2), 6),
        "is_international": is_international,
        "customer_tier": tier,
        "risk_score": round(rng.uniform(0, 0.35), 3),
    }
    return event, customer.account_id, is_known


def gen_fraud_signal(pool: CustomerPool, rng: random.Random) -> tuple[dict, str, bool]:
    customer, is_known = pool.pick()
    event = {
        "event_id": f"fraud_{uuid.uuid4().hex[:8]}",
        "event_type": "FRAUD_SIGNAL",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "account_id": customer.account_id,
        "fraud_type": rng.choice(FRAUD_TYPES),
        "ml_score": round(rng.uniform(0.70, 1.0), 3),
        "rule_triggers": rng.sample(
            [
                "VELOCITY_BREACH",
                "GEO_ANOMALY",
                "NEW_DEVICE",
                "UNUSUAL_AMOUNT",
                "NIGHT_TXN",
                "WATCHLIST_HIT",
            ],
            k=rng.randint(1, 3),
        ),
        "action": rng.choice(["BLOCK_AND_ALERT", "FLAG_REVIEW", "STEP_UP_AUTH"]),
        "case_id": f"CASE-{datetime.now().year}-{rng.randint(100000, 999999)}",
    }
    return event, customer.account_id, is_known


def gen_customer_event(
    pool: CustomerPool, rng: random.Random
) -> tuple[dict, str, bool]:
    """Customer-domain event: login, AML alert, profile change or account open."""
    customer, is_known = pool.pick()
    etype = rng.choices(
        ["CUSTOMER_LOGIN", "AML_ALERT", "PROFILE_CHANGE", "ACCOUNT_OPEN"],
        [0.60, 0.20, 0.15, 0.05],
    )[0]
    event = {
        "event_id": f"cust_{uuid.uuid4().hex[:8]}",
        "event_type": etype,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "account_id": customer.account_id,
    }
    if etype == "AML_ALERT":
        event.update(
            {
                "alert_type": rng.choice(["STRUCTURING", "LAYERING", "ROUND_TRIPPING"]),
                "risk_band": rng.choice(["HIGH", "MEDIUM"]),
                "sar_required": rng.random() < 0.4,
                "total_amount_7d": round(rng.uniform(100000, 1000000), 2),
            }
        )
    elif etype == "CUSTOMER_LOGIN":
        event.update(
            {
                "channel": rng.choice(["MOBILE_APP", "WEB"]),
                "auth_method": rng.choice(["BIOMETRIC", "OTP", "PASSWORD"]),
                "login_success": rng.random() < 0.95,
                "new_device": rng.random() < 0.05,
            }
        )
    return event, customer.account_id, is_known


def produce_loop(producer, topic, gen_fn, eps, stop_event, pool, rng, stats) -> None:
    interval = 1.0 / eps
    while not stop_event.is_set():
        t0 = time.time()
        event, key, is_known = gen_fn(pool, rng)
        producer.send(topic, value=event, key=key)
        stats.record(topic, is_known)
        elapsed = time.time() - t0
        if elapsed < interval:
            time.sleep(interval - elapsed)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Produce synthetic banking events to Kafka for the AIDP demo.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--customer-file",
        default=None,
        help="Account pool CSV from generate_customer360.py. Omit for the "
        "legacy fully-random behaviour (federated joins will not match).",
    )
    parser.add_argument(
        "--bootstrap-server", default=BOOTSTRAP, help="Kafka bootstrap server"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible event streams",
    )
    parser.add_argument(
        "--unknown-rate",
        type=float,
        default=DEFAULT_UNKNOWN_RATE,
        help="Share of events using accounts absent from MySQL (0.0-1.0)",
    )
    parser.add_argument(
        "--payments-eps", type=float, default=50.0, help="payments.raw events/sec"
    )
    parser.add_argument(
        "--fraud-eps", type=float, default=10.0, help="fraud.signals events/sec"
    )
    parser.add_argument(
        "--customer-eps", type=float, default=5.0, help="customer.events events/sec"
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Stop automatically after N seconds (default: run until Ctrl+C)",
    )
    parser.add_argument(
        "--compression",
        default="snappy",
        choices=["snappy", "gzip", "lz4", "none"],
        help="Kafka compression codec; falls back to gzip if unavailable",
    )
    parser.add_argument(
        "--pid-file",
        default=None,
        help="Write this process's PID here, so it can be stopped reliably with "
        "`kill -TERM $(cat <file>)`. Removed on clean exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and report events without connecting to Kafka",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    rng = random.Random(args.seed)

    if args.customer_file:
        try:
            customers = load_customer_pool(Path(args.customer_file))
        except (FileNotFoundError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        try:
            pool: CustomerPool = CustomerPool(customers, rng, args.unknown_rate)
        except ValueError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        print(
            f"[BankingProducer] Customer pool: {len(pool):,} accounts from {args.customer_file}"
        )
        print(f"[BankingProducer] Unknown-account rate: {args.unknown_rate:.2%}")
    else:
        pool = RandomPool(rng)
        print(
            "[BankingProducer] WARNING: no --customer-file given; using fully random "
            "account IDs. Federated joins against MySQL Customer 360 will NOT match."
        )

    stats = AccountStats()
    streams = (
        (TOPIC_PAYMENTS, gen_payment, args.payments_eps),
        (TOPIC_FRAUD, gen_fraud_signal, args.fraud_eps),
        (TOPIC_CUSTOMER, gen_customer_event, args.customer_eps),
    )

    if args.dry_run:
        sample = 2000
        print(
            f"[BankingProducer] Dry run: generating {sample:,} events per topic, no Kafka"
        )
        for topic, gen_fn, _ in streams:
            for _ in range(sample):
                _, _, is_known = gen_fn(pool, rng)
                stats.record(topic, is_known)
        print(stats.report())
        return 0

    try:
        producer = make_producer(
            args.bootstrap_server,
            None if args.compression == "none" else args.compression,
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"ERROR: cannot reach Kafka at {args.bootstrap_server}: {exc}",
            file=sys.stderr,
        )
        return 3

    stop = threading.Event()
    threads = [
        threading.Thread(
            target=produce_loop,
            args=(producer, topic, gen_fn, eps, stop, pool, rng, stats),
            daemon=True,
        )
        for topic, gen_fn, eps in streams
    ]
    for thread in threads:
        thread.start()

    # Shut down cleanly on both SIGTERM and SIGINT.
    #
    # Installing a SIGINT handler explicitly matters: when this script is started
    # as a background job from a NON-interactive shell (nohup ... &), bash sets
    # SIGINT to SIG_IGN for the child, Python inherits that and never installs
    # its default KeyboardInterrupt handler - so `kill -INT` is silently ignored
    # and only SIGTERM (an ungraceful kill) worked. signal.signal() overrides the
    # inherited SIG_IGN, so both signals now drain the producer and print the
    # final report.
    def _request_stop(signum, _frame) -> None:
        print(
            f"\n[BankingProducer] Received {signal.Signals(signum).name}, draining...",
            flush=True,
        )
        stop.set()

    for _sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(_sig, _request_stop)

    # A PID file avoids the usual guessing games: `pgrep -f Streaming_gen` also
    # matches the shell that launched it, and `$!` can point at a wrapper rather
    # than the interpreter.
    pid_file = Path(args.pid_file) if args.pid_file else None
    if pid_file:
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(f"{os.getpid()}\n", encoding="utf-8")

    print(
        f"[BankingProducer] Running against {args.bootstrap_server} (pid {os.getpid()}) - "
        "Ctrl+C, or `kill <pid>`, to stop",
        flush=True,
    )
    deadline = time.time() + args.duration if args.duration else None
    try:
        while not stop.is_set() and not (deadline and time.time() >= deadline):
            time.sleep(0.5)
    except KeyboardInterrupt:  # foreground Ctrl+C before the handler is installed
        pass
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=2)
        producer.flush()
        producer.close(timeout=5)
        if pid_file:
            pid_file.unlink(missing_ok=True)
        print("\n[BankingProducer] Stopped", flush=True)
        print(stats.report(), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
