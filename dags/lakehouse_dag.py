"""Builds the silver, gold and monitoring layers every 15 minutes.

1. ``dbt_build``     - dbt builds and tests every model on top of the Iceberg bronze tables.
2. ``contract_health`` - fails the run when more than 5% of the last hour's events
                         broke their data contracts, so a bad producer release is noticed.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

HOME = os.environ.get("LAKEHOUSE_HOME", "/opt/lakehouse")
PYTHON = os.environ.get("LAKEHOUSE_PYTHON", "python")
DBT = os.environ.get("DBT_BIN", "dbt")

with DAG(
    dag_id="lakehouse_gold",
    description="dbt build on Iceberg bronze, then a data-contract health check",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "dbt", "iceberg"],
) as dag:
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {HOME}/dbt && mkdir -p $(dirname $DBT_DUCKDB_PATH) && {DBT} build --profiles-dir .",
    )

    contract_health = BashOperator(
        task_id="contract_health",
        bash_command=f"cd {HOME} && {PYTHON} -m lakehouse.health --window-minutes 60 --max-reject-rate 0.05",
    )

    dbt_build >> contract_health
