"""Appends one line per DAG run outcome to a plain, append-only log file.

Airflow already logs every task run, but split one file per task per run —
there's no single place to `tail -f` and watch a running history across
executions. This gives that, for the one DAG where it's useful to check at
a glance (bulk_load_dimensions): how many runs happened, when, and whether
each one succeeded.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

HISTORY_FILE = Path("/opt/airflow/logs/run-history/bulk_load_dimensions.log")


def record_run(status: str, run_id: str, detail: str) -> None:
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"{timestamp} | run={run_id} | status={status} | {detail}\n"
    with HISTORY_FILE.open("a", encoding="utf-8") as fh:
        fh.write(line)
