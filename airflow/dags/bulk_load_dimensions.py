"""Bulk load of >=10M synthetic rows into `order_history_fact` via `bcp`.

Rows are generated and written in fixed-size chunks (see
airflow/scripts/data_generator.py), so memory use stays flat regardless of
`total_rows`. The actual load goes through the `bcp` client utility against
`TABLOCK`, with the database in SIMPLE recovery (`db/entrypoint.sh`) and no
secondary indexes at load time — the three conditions SQL Server needs to
treat a bulk import as *minimally logged* (see README, "Carga de 10M
linhas", for the full comparison against a plain `pyodbc`/`fast_executemany`
approach and the measured numbers).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import os
import sys

from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, "/opt/airflow/scripts")
from data_generator import generate_rows  # noqa: E402

DEFAULT_TOTAL_ROWS = 10_000_000
DEFAULT_BATCH_SIZE = 50_000

# Field/row terminators for the intermediate data file bcp reads. None of
# the generated field values can contain either (see data_generator.py),
# so plain unquoted delimiting is safe.
FIELD_TERMINATOR = ","
ROW_TERMINATOR = "\n"
FORMAT_FILE = "/opt/airflow/scripts/order_history_fact.fmt"

CREATE_TABLE_IF_MISSING_SQL = """
IF OBJECT_ID('dbo.order_history_fact', 'U') IS NULL
BEGIN
    CREATE TABLE order_history_fact (
        order_history_id BIGINT IDENTITY PRIMARY KEY,
        external_order_id VARCHAR(64) NOT NULL,
        client_external_id VARCHAR(64) NOT NULL,
        order_date DATE NOT NULL,
        status VARCHAR(20) NOT NULL,
        total_amount NUMERIC(18,2) NOT NULL,
        region VARCHAR(50) NOT NULL,
        channel VARCHAR(30) NOT NULL,
        item_count INT NOT NULL,
        load_batch_id VARCHAR(50) NOT NULL,
        loaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
"""

POST_LOAD_INDEXES_SQL = [
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ohf_order_date') "
    "CREATE INDEX ix_ohf_order_date ON order_history_fact (order_date)",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ohf_region_channel') "
    "CREATE INDEX ix_ohf_region_channel ON order_history_fact (region, channel)",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ohf_load_batch') "
    "CREATE INDEX ix_ohf_load_batch ON order_history_fact (load_batch_id)",
    "UPDATE STATISTICS order_history_fact",
]


def _get_pyodbc_connection():
    import os

    import pyodbc

    dsn = os.environ["MSSQL_ODBC_DSN"]
    conn = pyodbc.connect(dsn, autocommit=False)
    # Fail fast on a blocked statement instead of hanging the task forever.
    conn.cursor().execute("SET LOCK_TIMEOUT 10000")
    conn.commit()
    return conn


def ensure_table_exists(**_context) -> None:
    conn = _get_pyodbc_connection()
    try:
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_IF_MISSING_SQL)
        conn.commit()
    finally:
        conn.close()


def _row_to_line(row: tuple) -> str:
    (
        external_order_id,
        client_external_id,
        order_date,
        status,
        total_amount,
        region,
        channel,
        item_count,
        load_batch_id,
    ) = row
    fields = (
        external_order_id,
        client_external_id,
        order_date.isoformat(),  # ISO 8601 date is parsed locale-independently
        status,
        f"{total_amount:.2f}",
        region,
        channel,
        str(item_count),
        load_batch_id,
    )
    return FIELD_TERMINATOR.join(fields) + ROW_TERMINATOR


def generate_and_load(**context) -> None:
    import logging
    import subprocess
    import time

    log = logging.getLogger("bulk_load_dimensions")

    conf = context["dag_run"].conf or {}
    total_rows = int(conf.get("total_rows", DEFAULT_TOTAL_ROWS))
    batch_size = int(conf.get("batch_size", DEFAULT_BATCH_SIZE))
    # Derived from run_id (stable across retries of the same DAG run), not a
    # fresh uuid per call — a retry after a partial bcp failure must reuse
    # the same batch id so the cleanup below can find and remove the rows
    # the failed attempt already inserted, instead of leaving them orphaned
    # under an id nothing ever queries again.
    run_id_digest = hashlib.sha1(context["dag_run"].run_id.encode()).hexdigest()[:8]
    load_batch_id = f"{context['ds']}-{run_id_digest}"

    data_file = f"/tmp/order_history_fact_{load_batch_id}.csv"
    error_file = f"/tmp/order_history_fact_{load_batch_id}.bcp.err"

    conn = _get_pyodbc_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM order_history_fact WHERE load_batch_id = ?", load_batch_id)
        if cur.rowcount > 0:
            log.info(
                "removed %s row(s) left over from a previous failed attempt of batch %s",
                cur.rowcount,
                load_batch_id,
            )
        conn.commit()
    finally:
        conn.close()

    start = time.perf_counter()
    written = 0
    try:
        with open(data_file, "w", encoding="ascii", newline="") as fh:
            for batch in generate_rows(total_rows, batch_size, load_batch_id=load_batch_id):
                fh.writelines(_row_to_line(row) for row in batch)
                written += len(batch)
                if written % (batch_size * 10) == 0 or written == total_rows:
                    elapsed = time.perf_counter() - start
                    log.info(
                        "generated %s/%s rows (%.0f rows/sec)",
                        written,
                        total_rows,
                        written / max(elapsed, 1e-6),
                    )
        generated_elapsed = time.perf_counter() - start
        log.info("data file ready: %s rows in %.1fs, handing off to bcp", written, generated_elapsed)

        # -h "TABLOCK": takes a bulk-update table lock for the duration of the
        # load instead of row/page locks, which (together with the SIMPLE
        # recovery model set in db/entrypoint.sh, and no other indexes or
        # triggers on this table at load time) is what qualifies this import
        # for minimal logging. Identity/default columns are handled by the
        # format file, not by this command line — see FORMAT_FILE and the
        # module docstring.
        bcp_cmd = [
            "bcp",
            "dbo.order_history_fact",
            "in",
            data_file,
            "-S", os.environ["MSSQL_HOST"],
            "-d", os.environ["MSSQL_DATABASE"],
            "-U", "sa",
            "-P", os.environ["MSSQL_SA_PASSWORD"],
            # -u: trust the container's self-signed cert (same reason the
            # pyodbc DSN elsewhere carries TrustServerCertificate=yes).
            "-u",
            "-f", FORMAT_FILE,
            "-b", str(batch_size),
            "-h", "TABLOCK",
            "-e", error_file,
            # 65535 is bcp's documented max packet size, but -u means this
            # connection is TLS-encrypted and TLS fragments cap at 16384 —
            # anything higher fails with "Packet size too large for SSL
            # Encrypt/Decrypt operations".
            "-a", "16384",
        ]
        bcp_timeout = int(os.environ.get("BCP_TIMEOUT_SECONDS", "7200"))
        bcp_start = time.perf_counter()
        try:
            result = subprocess.run(
                bcp_cmd, capture_output=True, text=True, timeout=bcp_timeout
            )
        except subprocess.TimeoutExpired as exc:
            # A hung bcp (e.g. the network dropping mid-transfer) would
            # otherwise block this task forever instead of failing and
            # letting Airflow's own retry pick it up.
            raise RuntimeError(
                f"bcp did not finish within {bcp_timeout}s loading {data_file}"
            ) from exc
        bcp_elapsed = time.perf_counter() - bcp_start
        log.info("bcp stdout:\n%s", result.stdout)
        if result.returncode != 0:
            error_detail = ""
            if os.path.exists(error_file):
                with open(error_file, encoding="ascii", errors="replace") as ef:
                    error_detail = ef.read()
            log.error("bcp stderr:\n%s\nbcp error file:\n%s", result.stderr, error_detail)
            raise RuntimeError(f"bcp exited with code {result.returncode} loading {data_file}")
    finally:
        for path in (data_file, error_file):
            if os.path.exists(path):
                os.remove(path)

    elapsed = time.perf_counter() - start
    log.info(
        "done: %s rows in %.1fs total (%.1fs generate + %.1fs bcp), %.0f rows/sec",
        written,
        elapsed,
        generated_elapsed,
        bcp_elapsed,
        written / elapsed,
    )
    context["ti"].xcom_push(key="load_batch_id", value=load_batch_id)
    context["ti"].xcom_push(key="rows_written", value=written)


def validate_row_count(**context) -> None:
    conf = context["dag_run"].conf or {}
    expected_min = int(conf.get("total_rows", DEFAULT_TOTAL_ROWS))
    load_batch_id = context["ti"].xcom_pull(key="load_batch_id", task_ids="generate_and_load")

    conn = _get_pyodbc_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM order_history_fact WHERE load_batch_id = ?", load_batch_id
        )
        (count,) = cur.fetchone()
    finally:
        conn.close()

    if count < expected_min:
        raise ValueError(f"expected >= {expected_min} rows for batch {load_batch_id}, found {count}")


def create_post_load_indexes(**_context) -> None:
    conn = _get_pyodbc_connection()
    try:
        cur = conn.cursor()
        for stmt in POST_LOAD_INDEXES_SQL:
            cur.execute(stmt)
            conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="bulk_load_dimensions",
    description="Chunked, memory-safe bulk load of synthetic historical order data into SQL Server",
    start_date=dt.datetime(2026, 1, 1),
    schedule=None,  # triggered manually / on demand, like a historical backfill
    catchup=False,
    max_active_runs=1,  # two concurrent runs would fight over the same TABLOCK
    default_args={"retries": 1, "retry_delay": dt.timedelta(minutes=2)},
    tags=["bulk-load", "sql-server", "performance", "bcp", "minimal-logging"],
) as dag:
    t1 = PythonOperator(task_id="ensure_table_exists", python_callable=ensure_table_exists)
    t2 = PythonOperator(task_id="generate_and_load", python_callable=generate_and_load)
    t3 = PythonOperator(task_id="validate_row_count", python_callable=validate_row_count)
    t4 = PythonOperator(task_id="create_post_load_indexes", python_callable=create_post_load_indexes)

    t1 >> t2 >> t3 >> t4
