"""MySQL connection handling for the Customer 360 side of the federated demo.

Mirrors :mod:`aidp_connection`: configuration comes only from the environment
(optionally seeded from ``docker/mysql/.env``), and passwords are wrapped in the
same :class:`~aidp_connection.Secret` type so they cannot be printed, formatted
or logged by accident.

Two roles are supported:

``app``
    ``customer_app`` - read/write, used by ``scripts/load_customer360.py``.
``starburst``
    ``starburst_ro`` - SELECT only, the account the AIDP catalog uses. The
    validation script connects as this role to prove the grant really is
    read-only.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from aidp_connection import Secret, SecretRedactingFilter

logger = logging.getLogger("mysql360")

DEFAULT_ENV_FILE = Path(__file__).resolve().parent / "docker" / "mysql" / ".env"
DEFAULT_DATABASE = "customer360"
DEFAULT_PORT = 3306
#: Host the AIDP worker nodes use. The demo host (10.246.25.115) is dual-homed;
#: the AIDP cluster cannot route to 10.246.25.x, so the address on the AIDP
#: subnet is the service endpoint. Same host also runs the Kafka broker.
DEFAULT_HOST = "172.18.1.177"

CUSTOMERS_TABLE = "customers"


class MySQLConfigError(Exception):
    """Raised when MySQL configuration is missing or invalid."""


@dataclass(frozen=True)
class MySQLConfig:
    """Connection settings. The password is never in a printable field."""

    host: str
    port: int
    database: str
    user: str
    password: Secret
    role: str = "app"

    @property
    def safe_target(self) -> str:
        """Human-readable target containing no credential material."""
        return f"mysql://{self.user}@{self.host}:{self.port}/{self.database}"


def read_env_file(path: Path | None = None) -> dict[str, str]:
    """Parse a dotenv file into a dict. Missing file yields an empty dict."""
    path = path or DEFAULT_ENV_FILE
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def load_mysql_config(
    role: str = "app",
    env_file: Path | None = None,
    environ: dict[str, str] | None = None,
) -> MySQLConfig:
    """Build a :class:`MySQLConfig` for ``role`` ("app" or "starburst").

    Real environment variables win over the dotenv file, so CI or a shell export
    can override the local development defaults.
    """
    import os

    if role not in ("app", "starburst"):
        raise MySQLConfigError(
            f"Unknown MySQL role {role!r}; expected 'app' or 'starburst'"
        )

    environ = dict(environ if environ is not None else os.environ)
    file_values = read_env_file(env_file)

    def value_for(name: str, default: str | None = None) -> str | None:
        return environ.get(name) or file_values.get(name) or default

    user_key, password_key = (
        ("MYSQL_APP_USER", "MYSQL_APP_PASSWORD")
        if role == "app"
        else ("MYSQL_STARBURST_USER", "MYSQL_STARBURST_PASSWORD")
    )

    user = value_for(user_key)
    password = value_for(password_key)
    if not user:
        raise MySQLConfigError(f"{user_key} is not set (check docker/mysql/.env)")
    if not password or password == "replace_me":
        raise MySQLConfigError(
            f"{password_key} is not set to a real value. Copy docker/mysql/.env.example "
            "to docker/mysql/.env and set secure passwords; never commit that file."
        )

    port_raw = value_for("MYSQL_PORT", str(DEFAULT_PORT))
    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        raise MySQLConfigError(
            f"MYSQL_PORT must be an integer, got {port_raw!r}"
        ) from None

    config = MySQLConfig(
        host=value_for("MYSQL_HOST", DEFAULT_HOST),
        port=port,
        database=value_for("MYSQL_DATABASE", DEFAULT_DATABASE),
        user=user,
        password=Secret(password),
        role=role,
    )
    install_log_redaction(config)
    return config


def install_log_redaction(config: MySQLConfig) -> None:
    """Scrub this password from anything reaching the logging subsystem."""
    secret_filter = SecretRedactingFilter(config.password.reveal())
    for name in ("", "mysql360", "pymysql"):
        target = logging.getLogger(name)
        if not any(isinstance(f, SecretRedactingFilter) for f in target.filters):
            target.addFilter(secret_filter)


class MySQLConnectionError(Exception):
    """A classified, credential-free MySQL connection failure."""

    def __init__(self, kind: str, message: str, remedy: str = "") -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.remedy = remedy

    def __str__(self) -> str:
        return f"{self.message} {self.remedy}".strip()


def classify_mysql_error(
    exc: BaseException, config: MySQLConfig | None = None
) -> MySQLConnectionError:
    """Map a PyMySQL/socket error to an actionable diagnosis."""
    if isinstance(exc, MySQLConnectionError):
        return exc

    text = str(exc)
    code = None
    if getattr(exc, "args", None) and isinstance(exc.args[0], int):
        code = exc.args[0]
    target = config.safe_target if config else "the MySQL server"

    if code == 1045 or "Access denied" in text:
        return MySQLConnectionError(
            "auth",
            f"Access denied for {target}.",
            "Check the user/password in docker/mysql/.env. If the user exists but is "
            "bound to 'localhost', recreate it with host '%' so off-host clients "
            "(including AIDP workers) can connect.",
        )
    if code == 1049 or "Unknown database" in text:
        return MySQLConnectionError(
            "missing_database",
            f"Database not found on {target}.",
            "Start the stack so init/01_create_schema.sql runs, or check MYSQL_DATABASE.",
        )
    if code == 1146 or "doesn't exist" in text:
        return MySQLConnectionError(
            "missing_table",
            f"Table customer360.{CUSTOMERS_TABLE} does not exist.",
            "Recreate the container so the init scripts run: "
            "docker compose --env-file docker/mysql/.env "
            "-f docker/mysql/docker-compose.yml up -d",
        )
    if code in (2003, 2002) or "Can't connect" in text or "Connection refused" in text:
        return MySQLConnectionError(
            "network",
            f"Cannot reach {target}.",
            "Is the container running and healthy? "
            "docker ps --filter name=js-mysql-customer360 - and check that TCP 3306 "
            "is open to this host.",
        )
    if "timed out" in text.lower():
        return MySQLConnectionError(
            "timeout",
            f"Timed out connecting to {target}.",
            "Check firewall rules on port 3306.",
        )
    return MySQLConnectionError(
        "unknown", f"MySQL error talking to {target}: {text}", ""
    )


def connect(config: MySQLConfig | None = None, connect_timeout: int = 10):
    """Open a PyMySQL connection, raising :class:`MySQLConnectionError` on failure."""
    config = config or load_mysql_config()
    import pymysql

    logger.info("Connecting to %s (role=%s)", config.safe_target, config.role)
    try:
        return pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password.reveal(),
            database=config.database,
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=connect_timeout,
            cursorclass=pymysql.cursors.DictCursor,
        )
    except BaseException as exc:  # noqa: BLE001 - re-raised as a classified error
        raise classify_mysql_error(exc, config) from None
