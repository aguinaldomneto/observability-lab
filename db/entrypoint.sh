#!/usr/bin/env bash
set -euo pipefail

: "${MSSQL_HOST:?}"
: "${MSSQL_SA_PASSWORD:?}"
: "${MSSQL_DATABASE:=ecommerce}"

echo "[db] waiting for SQL Server at ${MSSQL_HOST}..."
for i in $(seq 1 60); do
    if /opt/mssql-tools18/bin/sqlcmd -C -S "${MSSQL_HOST}" -U sa -P "${MSSQL_SA_PASSWORD}" -Q "SELECT 1" >/dev/null 2>&1; then
        echo "[db] SQL Server is reachable"
        break
    fi
    sleep 2
done

echo "[db] ensuring database '${MSSQL_DATABASE}' exists"
/opt/mssql-tools18/bin/sqlcmd -C -S "${MSSQL_HOST}" -U sa -P "${MSSQL_SA_PASSWORD}" -Q \
    "IF DB_ID('${MSSQL_DATABASE}') IS NULL CREATE DATABASE [${MSSQL_DATABASE}];"

# Read Committed Snapshot Isolation: readers get a versioned snapshot instead
# of blocking behind writers' locks (and vice versa). This is the single
# highest-leverage SQL Server setting for "the pipeline must not lock the
# destination DB under concurrent writers" — it doesn't replace the small
# connection pools and short transactions in the app code, but without it
# even well-written short transactions still cause reader/writer blocking
# under enough concurrency.
echo "[db] enabling READ_COMMITTED_SNAPSHOT on '${MSSQL_DATABASE}'"
/opt/mssql-tools18/bin/sqlcmd -C -S "${MSSQL_HOST}" -U sa -P "${MSSQL_SA_PASSWORD}" -Q \
    "IF (SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = '${MSSQL_DATABASE}') = 0
     BEGIN
        ALTER DATABASE [${MSSQL_DATABASE}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
     END"

# SIMPLE recovery model is a prerequisite for minimal logging on the Parte 3
# bulk load (bcp + TABLOCK into order_history_fact, see
# airflow/dags/bulk_load_dimensions.py) — without it, every bulk-copied row
# is still fully written to the transaction log, which is most of the cost
# fast_executemany's row-by-row logged DML was already paying. This is a
# durable database setting (not toggled per-load), so it also means this
# database is not a candidate for point-in-time restore, which is an
# acceptable trade for a throwaway prototype but would not be for production
# without a real backup strategy — full/bulk-logged backups still work.
echo "[db] setting recovery model to SIMPLE on '${MSSQL_DATABASE}' for minimally logged bulk loads"
/opt/mssql-tools18/bin/sqlcmd -C -S "${MSSQL_HOST}" -U sa -P "${MSSQL_SA_PASSWORD}" -Q \
    "IF (SELECT recovery_model FROM sys.databases WHERE name = '${MSSQL_DATABASE}') <> 3
     BEGIN
        ALTER DATABASE [${MSSQL_DATABASE}] SET RECOVERY SIMPLE;
     END"

export DATABASE_URL="mssql+pyodbc://sa:${MSSQL_SA_PASSWORD}@${MSSQL_HOST}/${MSSQL_DATABASE}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

echo "[db] running alembic upgrade head"
alembic upgrade head

echo "[db] migrations complete"
