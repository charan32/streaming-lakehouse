import os

import pytest


@pytest.fixture
def local_catalog(tmp_path, monkeypatch):
    """A fresh Iceberg catalog (SQLite + local files) in a temp directory."""
    monkeypatch.setenv("LAKEHOUSE_MODE", "local")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKEHOUSE__TYPE", "sql")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKEHOUSE__URI", f"sqlite:///{tmp_path / 'catalog.db'}")
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE", f"file://{tmp_path / 'warehouse'}")
    (tmp_path / "warehouse").mkdir()
    from lakehouse import settings

    yield settings.catalog()
    for key in [k for k in os.environ if k.startswith("PYICEBERG_CATALOG__")]:
        monkeypatch.delenv(key, raising=False)
