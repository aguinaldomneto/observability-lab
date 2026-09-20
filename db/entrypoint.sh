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

# Read Committed Snapshot Isolation: readers get a versioned snapshot
# instead of blocking behind writers' locks, and vice versa — this is what
# actually keeps the DB from locking up under concurrent writers, on top of
# the app's small connection pools and short transactions.
echo "[db] enabling READ_COMMITTED_SNAPSHOT on '${MSSQL_DATABASE}'"
/opt/mssql-tools18/bin/sqlcmd -C -S "${MSSQL_HOST}" -U sa -P "${MSSQL_SA_PASSWORD}" -Q \
    "IF (SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = '${MSSQL_DATABASE}') = 0
     BEGIN
        ALTER DATABASE [${MSSQL_DATABASE}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
     END"

# SIMPLE recovery is a prerequisite for the minimally logged bulk load in
# airflow/dags/bulk_load_dimensions.py (bcp + TABLOCK). Trade-off: this
# database gives up point-in-time restore, fine for this prototype but not
# for production without a real backup strategy on top.
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
