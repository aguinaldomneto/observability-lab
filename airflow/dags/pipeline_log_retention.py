"""Keeps `pipeline_log` bounded: rows older than 7 days are deleted, and if
the table still has more than 100k rows the oldest 200 are trimmed. Runs
hourly — cheap query, and frequent enough that the row-count cap rarely
gets far past 100k even during a burst of errors.
"""
from __future__ import annotations

import datetime as dt

from airflow.operators.python import PythonOperator

from airflow import DAG

RETENTION_DAYS = 7
MAX_ROWS = 100_000
TRIM_BATCH_SIZE = 200


def _get_pyodbc_connection():
    import os

    import pyodbc

    dsn = os.environ["MSSQL_ODBC_DSN"]
    conn = pyodbc.connect(dsn, autocommit=False)
    conn.cursor().execute("SET LOCK_TIMEOUT 10000")
    conn.commit()
    return conn


def enforce_retention(**_context) -> None:
    import logging

    log = logging.getLogger("pipeline_log_retention")

    conn = _get_pyodbc_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            "DELETE FROM pipeline_log WHERE created_at < DATEADD(day, ?, SYSUTCDATETIME())",
            -RETENTION_DAYS,
        )
        if cur.rowcount > 0:
            log.info("deleted %s row(s) older than %s days", cur.rowcount, RETENTION_DAYS)

        cur.execute("SELECT COUNT(*) FROM pipeline_log")
        (count,) = cur.fetchone()
        if count > MAX_ROWS:
            cur.execute(
                "DELETE FROM pipeline_log WHERE log_id IN ("
                "SELECT TOP (?) log_id FROM pipeline_log ORDER BY created_at ASC)",
                TRIM_BATCH_SIZE,
            )
            log.info(
                "table had %s rows (over the %s cap) — trimmed the %s oldest",
                count,
                MAX_ROWS,
                TRIM_BATCH_SIZE,
            )

        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="pipeline_log_retention",
    description="Keeps pipeline_log under 7 days old and 100k rows",
    start_date=dt.datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": dt.timedelta(minutes=5)},
    tags=["housekeeping", "sql-server"],
) as dag:
    PythonOperator(task_id="enforce_retention", python_callable=enforce_retention)
