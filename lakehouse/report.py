"""Print the gold KPIs and pipeline health from the DuckDB file dbt builds.

    python -m lakehouse.report                       # local mode
    docker compose exec airflow /opt/airflow/lakehouse-venv/bin/python -m lakehouse.report
"""

from __future__ import annotations

import os

import duckdb

QUERIES = {
    "Daily KPIs": """
        select day, sessions, orders, paid_orders, revenue, avg_order_value,
               cart_rate, order_rate, conversion_rate
        from gold.fct_daily_kpis order by day""",
    "Pipeline health": """
        select day, bronze_rows, duplicates_removed, rejected_rows, reject_rate,
               p95_arrival_delay_seconds, late_events
        from monitoring.pipeline_health order by day""",
    "Top contract violations": """
        select event_type, failed_field, sum(rejected_events) as events
        from monitoring.contract_rejections group by all order by events desc limit 8""",
    "Revenue by category": """
        select category, sum(line_total) as revenue
        from gold.fct_order_items where is_paid group by 1 order by 2 desc""",
}


def main() -> None:
    path = os.getenv("DBT_DUCKDB_PATH", "dbt/target/lakehouse.duckdb")
    con = duckdb.connect(path, read_only=True)
    for title, sql in QUERIES.items():
        print(f"\n== {title} ==")
        con.sql(sql).show(max_width=140)


if __name__ == "__main__":
    main()
