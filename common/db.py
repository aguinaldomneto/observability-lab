"""Shared SQL Server connection factory used by the ingestion consumer."""
import os
from typing import Any

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine

# Fail a blocked statement after this long instead of waiting on a lock
# forever — turns a silent hang into a catchable OperationalError.
LOCK_TIMEOUT_MS = 10_000


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    return url


def get_engine(pool_size: int = 5, max_overflow: int = 5) -> Engine:
    # fast_executemany switches pyodbc's batched inserts to the driver's
    # native array-binding protocol instead of one round-trip per row.
    # pool_size/max_overflow stay small on purpose so no single service
    # instance can flood SQL Server with connections under load.
    engine = create_engine(
        get_database_url(),
        fast_executemany=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _set_lock_timeout(dbapi_connection: Any, connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute(f"SET LOCK_TIMEOUT {LOCK_TIMEOUT_MS}")
        cursor.close()

    return engine
