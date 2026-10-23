"""End to end in local mode: generate -> contracts -> Iceberg -> dbt build -> gold tables."""

import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[1]


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("lakehouse")
    env = {
        **os.environ,
        "LAKEHOUSE_MODE": "local",
        "PYICEBERG_CATALOG__LAKEHOUSE__TYPE": "sql",
        "PYICEBERG_CATALOG__LAKEHOUSE__URI": f"sqlite:///{tmp / 'catalog.db'}",
        "PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE": f"file://{tmp / 'warehouse'}",
        "DBT_DUCKDB_PATH": str(tmp / "lakehouse.duckdb"),
        "DBT_TARGET_PATH": str(tmp / "dbt-target"),
        "DBT_LOG_PATH": str(tmp / "dbt-logs"),
    }
    (tmp / "warehouse").mkdir()
    subprocess.run([sys.executable, "-m", "lakehouse.local_run", "--sessions", "800"],
                   cwd=ROOT, env=env, check=True)
    dbt = Path(sys.executable).with_name("dbt")
    proc = subprocess.run([str(dbt), "build", "--profiles-dir", "."], cwd=ROOT / "dbt", env=env,
                          capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout[-3000:]
    con = duckdb.connect(env["DBT_DUCKDB_PATH"], read_only=True)
    yield con
    con.close()


def test_gold_tables_are_populated(built):
    for table in ["gold.fct_orders", "gold.dim_customers", "gold.fct_sessions", "gold.fct_daily_kpis"]:
        assert built.sql(f"select count(*) from {table}").fetchone()[0] > 0, table


def test_duplicates_are_removed_in_staging(built):
    bronze, unique = built.sql(
        "select sum(bronze_rows), sum(bronze_rows - duplicates_removed) from monitoring.pipeline_health"
    ).fetchone()
    staged = built.sql("select count(*) from silver.stg_events").fetchone()[0]
    assert bronze > staged == unique


def test_revenue_only_counts_authorized_payments(built):
    revenue = built.sql("select sum(revenue) from gold.fct_daily_kpis").fetchone()[0]
    paid = built.sql("select sum(order_total) from gold.fct_orders where is_paid").fetchone()[0]
    assert revenue == paid > 0
