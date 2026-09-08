#!/usr/bin/env python3
"""Stage what the batch ingestion job needs in object storage.

Two kinds of asset, both fetched by Spark at job startup over s3a:// (which
works because hadoop-aws IS in the AIDP image):

  jars     The AIDP Spark image ships Iceberg, hadoop-aws and aws-java-sdk-bundle
           but no JDBC drivers, so reading the Starburst data catalog fails with
           "No suitable driver". They cannot travel with --file-upload either
           (trino-jdbc is ~14 MB, and the CLI caps uploads at 1 MB per file).

  landing  The account reference CSV the file source reads. In a real deployment
           an upstream system drops files here; for the demo this script plays
           that role.

Usage (needs internet for Maven, plus S3 credentials):
    S3_ACCESS_KEY=... S3_SECRET_KEY=... ./scripts/stage_batch_assets.py --all
    S3_ACCESS_KEY=... S3_SECRET_KEY=... ./scripts/stage_batch_assets.py --landing
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

# Pinned to the versions in use: Trino 476 is the AIDP/Starburst release this
# demo was validated against, and Connector/J 8.4.0 speaks to MySQL 8.0.43.
TRINO_JDBC_VERSION = os.environ.get("TRINO_JDBC_VERSION", "476")
MYSQL_CONNECTOR_VERSION = os.environ.get("MYSQL_CONNECTOR_VERSION", "8.4.0")

MAVEN = os.environ.get("MAVEN_REPO", "https://repo1.maven.org/maven2")
BUCKET = os.environ.get("S3_BUCKET", "js-demo")
JARS_PREFIX = os.environ.get("S3_JARS_PREFIX", "jars")
LANDING_PREFIX = os.environ.get("S3_LANDING_PREFIX", "landing/accounts")
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://172.18.11.31:9020")

ARTIFACTS = [
    ("io.trino", "trino-jdbc", TRINO_JDBC_VERSION),
    ("com.mysql", "mysql-connector-j", MYSQL_CONNECTOR_VERSION),
]

DEFAULT_LANDING_FILE = "data/customer_accounts.csv"


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=300) as response:
        return response.read()


def s3_client():
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if not (access_key and secret_key):
        sys.exit("Set S3_ACCESS_KEY and S3_SECRET_KEY")
    return boto3.client(
        "s3", endpoint_url=ENDPOINT,
        aws_access_key_id=access_key, aws_secret_access_key=secret_key,
        config=Config(s3={"addressing_style": "path"}),
    )


def upload(s3, local: str, key: str, expected_size: int) -> None:
    s3.upload_file(local, BUCKET, key)
    size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    status = "ok" if size == expected_size else "SIZE MISMATCH"
    print(f"s3a://{BUCKET}/{key}  {size:>9} bytes  {status}")


def stage_jars(s3) -> None:
    with tempfile.TemporaryDirectory() as workdir:
        for group, artifact, version in ARTIFACTS:
            jar = f"{artifact}-{version}.jar"
            base = f"{MAVEN}/{group.replace('.', '/')}/{artifact}/{version}/{jar}"

            payload = fetch(base)
            # Verify against the published SHA-1 so a truncated download cannot
            # be published to the bucket and fail obscurely at job startup.
            expected = fetch(f"{base}.sha1").decode().split()[0].strip()
            actual = hashlib.sha1(payload).hexdigest()
            if expected != actual:
                sys.exit(f"SHA-1 mismatch for {jar}: expected {expected}, got {actual}")

            local = os.path.join(workdir, jar)
            Path(local).write_bytes(payload)
            upload(s3, local, f"{JARS_PREFIX}/{jar}", len(payload))


def stage_landing(s3, path: str) -> None:
    source = Path(path)
    if not source.exists():
        sys.exit(
            f"{source} not found. Generate it first:\n"
            "  python scripts/generate_customer360.py --customers 10000 --seed 42 "
            "--output-dir data"
        )
    # Dated object name: the landing zone keeps every drop, and the job reads
    # the whole prefix and upserts, so a re-drop refreshes rows instead of
    # duplicating them.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{LANDING_PREFIX}/{source.stem}_{stamp}.csv"
    upload(s3, str(source), key, source.stat().st_size)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--jars", action="store_true", help="stage the JDBC drivers")
    parser.add_argument("--landing", action="store_true",
                        help="stage the account reference CSV")
    parser.add_argument("--all", action="store_true", help="both")
    parser.add_argument("--landing-file", default=DEFAULT_LANDING_FILE)
    args = parser.parse_args()

    if not (args.jars or args.landing or args.all):
        parser.error("choose --jars, --landing or --all")

    s3 = s3_client()
    if args.jars or args.all:
        stage_jars(s3)
    if args.landing or args.all:
        stage_landing(s3, args.landing_file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
