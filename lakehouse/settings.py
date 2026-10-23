"""Runtime configuration.

Everything is driven by environment variables so the same code runs in two modes:

* ``local``  – no Docker. Iceberg catalog in SQLite, table files on local disk.
               Used by the test suite, CI, and ``make local``.
* ``docker`` – the full stack from ``docker-compose.yml``: Redpanda (Kafka API),
               a Postgres-backed Iceberg catalog, and MinIO as S3 storage.

The Iceberg catalog is configured through PyIceberg's own environment variables
(``PYICEBERG_CATALOG__LAKEHOUSE__*``), so Python code and dbt read the exact same
settings.
"""

from __future__ import annotations

import os
from pathlib import Path

CATALOG_NAME = "lakehouse"
_PREFIX = f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "events.raw")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "events.dlq")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "bronze-ingestor")


def mode() -> str:
    return os.getenv("LAKEHOUSE_MODE", "local")


def apply_local_defaults(root: str | os.PathLike | None = None) -> Path:
    """Point the Iceberg catalog at a SQLite file and a local warehouse folder.

    Existing environment variables win, so tests can redirect to a temp dir.
    """
    base = Path(root or os.getenv("LAKEHOUSE_LOCAL_DIR", ".local")).resolve()
    (base / "warehouse").mkdir(parents=True, exist_ok=True)
    os.environ.setdefault(_PREFIX + "TYPE", "sql")
    os.environ.setdefault(_PREFIX + "URI", f"sqlite:///{base / 'catalog.db'}")
    os.environ.setdefault(_PREFIX + "WAREHOUSE", f"file://{base / 'warehouse'}")
    return base


def catalog():
    """Load the Iceberg catalog for the current mode."""
    from pyiceberg.catalog import load_catalog

    if mode() == "local":
        apply_local_defaults()
    # PyIceberg reads its environment once at import time, so pass the current
    # values explicitly; they also stay in os.environ for child processes (dbt).
    props = {
        key[len(_PREFIX):].lower().replace("__", ".").replace("_", "-"): value
        for key, value in os.environ.items()
        if key.startswith(_PREFIX)
    }
    return load_catalog(CATALOG_NAME, **props)
