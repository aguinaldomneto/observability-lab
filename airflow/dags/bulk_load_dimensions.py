"""Part 3: bulk load of >=10M synthetic rows into `order_history_fact`.

Memory requirement: rows are produced and written in fixed-size chunks
(`BATCH_SIZE`, default 50k) via a generator (see
airflow/scripts/data_generator.py) — at no point does the process hold more
than one batch in memory, regardless of how large `total_rows` is. This was
benchmarked standalone for the full 10M rows (see README) before being
wired into Airflow.

Speed requirement (v2): writes use the `bcp` client utility (part of
`mssql-tools18`, installed in `airflow/Dockerfile`) against `TABLOCK`, with
the database in SIMPLE recovery (`db/entrypoint.sh`) — this is what actually
qualifies for *minimally logged* bulk import on SQL Server. Row-by-row
`INSERT` (or SQLAlchemy ORM `session.add` per row) is explicitly what the
challenge disqualifies, so this DAG never uses either.

v1 of this task used `pyodbc` with `cursor.fast_executemany = True`
(commit e5c59ee — see README "Versionamento e rollback" for how to get
back to it). That is genuinely
faster than row-by-row `executemany`, because it switches parameter binding
to the ODBC driver's array-binding protocol — but it is still a *logged*
DML path: every inserted row is still fully written to the transaction log,
row by row, over the TDS protocol. It measured ~9817s (~2h43m) for 10M rows
on the reference test server, which is why this was rewritten. `bcp` writes
through the bulk-copy interface instead, which — combined with `TABLOCK`
and SIMPLE recovery, and no other indexes/triggers on the table at load
time — lets SQL Server skip most of that per-row log write. This has not
yet been re-measured end to end (see README "Parte 3"); the mechanism is
correct, the exact new number is still open until re-run on real hardware.

Two mechanical details worth calling out because they're easy to get wrong
with `bcp` and silently fall back to full logging:
1. `order_history_id` (IDENTITY) and `loaded_at` (server-side DEFAULT) are
   not in the generated data file. `bcp` will not accept a data file with a
   different column count than the target unless told how to map fields, so
   `order_history_fact.fmt` (a non-XML bcp format file, checked into
   `airflow/scripts/`) maps the 9 generated fields to their destination
   column ordinals and simply omits columns 1 and 11 — `bcp` leaves those to
   the identity generator and the column default, respectively.
2. It would be simpler to `bcp` into a *view* that only exposes the 9
   loadable columns, sidestepping the format file entirely. That was
   considered and rejected: bulk importing through a view is documented as
   always fully logged on SQL Server, regardless of recovery model or
   `TABLOCK` — it would silently defeat the entire point of this rewrite.

Executor note: this repo's docker-compose runs Airflow with LocalExecutor
(Postgres metadata DB, no Celery/Redis) — for a single-DAG take-home
prototype that is a lighter, equally valid choice: it still runs tasks as
separate processes (not just threads), it does not require a message broker
purely to schedule Airflow's own internal task queue, and the real
concurrency-sensitive broker (Redpanda) already exists for the actual data
pipeline.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator

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
    load_batch_id = f"{context['ds']}-{uuid.uuid4().hex[:8]}"

    data_file = f"/tmp/order_history_fact_{load_batch_id}.csv"
    error_file = f"/tmp/order_history_fact_{load_batch_id}.bcp.err"

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
            "-f", FORMAT_FILE,
            "-b", str(batch_size),
            "-h", "TABLOCK",
            "-e", error_file,
        ]
        bcp_start = time.perf_counter()
        result = subprocess.run(bcp_cmd, capture_output=True, text=True)
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
    default_args={"retries": 1, "retry_delay": dt.timedelta(minutes=2)},
    tags=["bulk-load", "sql-server", "performance", "bcp", "minimal-logging"],
) as dag:
    t1 = PythonOperator(task_id="ensure_table_exists", python_callable=ensure_table_exists)
    t2 = PythonOperator(task_id="generate_and_load", python_callable=generate_and_load)
    t3 = PythonOperator(task_id="validate_row_count", python_callable=validate_row_count)
    t4 = PythonOperator(task_id="create_post_load_indexes", python_callable=create_post_load_indexes)

    t1 >> t2 >> t3 >> t4
