"""pipeline_log: operational log for the streaming pipeline (ingestion
errors, retries exhausted, messages dropped). Bounded on purpose — see
airflow/dags/pipeline_log_retention.py, which enforces a 7-day retention
and a 100k-row hard cap so this never grows unbounded like the tables that
hold actual business data.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-20
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pipeline_log",
        sa.Column("log_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.Column("service", sa.String(50), nullable=False),
        sa.Column("level", sa.String(10), nullable=False),
        sa.Column("message", sa.String(500), nullable=False),
        sa.Column("event_id", sa.String(100), nullable=True),
    )
    # Retention deletes by age (created_at) and, for the row-count cap,
    # by oldest-first — both query patterns want this index.
    op.create_index("ix_pipeline_log_created_at", "pipeline_log", ["created_at"])


def downgrade() -> None:
    op.drop_table("pipeline_log")
