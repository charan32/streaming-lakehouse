"""Contract health check, run by Airflow after each dbt build.

Fails (exit code 1) when the share of events rejected by the data contracts in
the last ``--window-minutes`` is above ``--max-reject-rate``. A spike usually
means a producer shipped a breaking change, which is something a person should
look at before the numbers in the marts are trusted.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

from pyiceberg.expressions import GreaterThanOrEqual

from . import settings
from .ingest import EVENTS_TABLE, REJECTED_TABLE


def reject_rate(window_minutes: int, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    since = (now - timedelta(minutes=window_minutes)).isoformat()
    cat = settings.catalog()
    flt = GreaterThanOrEqual("ingested_at", since)
    valid = cat.load_table(EVENTS_TABLE).scan(row_filter=flt, selected_fields=("event_id",)).to_arrow().num_rows
    rejected = cat.load_table(REJECTED_TABLE).scan(row_filter=flt, selected_fields=("raw",)).to_arrow().num_rows
    total = valid + rejected
    return {
        "window_minutes": window_minutes,
        "valid": valid,
        "rejected": rejected,
        "reject_rate": round(rejected / total, 4) if total else 0.0,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Fail if too many events break their data contracts.")
    p.add_argument("--window-minutes", type=int, default=60)
    p.add_argument("--max-reject-rate", type=float, default=0.05)
    args = p.parse_args()
    report = reject_rate(args.window_minutes)
    report["max_reject_rate"] = args.max_reject_rate
    report["healthy"] = report["reject_rate"] <= args.max_reject_rate
    print(json.dumps(report))
    sys.exit(0 if report["healthy"] else 1)


if __name__ == "__main__":
    main()
