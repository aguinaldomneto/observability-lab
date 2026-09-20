"""order_history_fact: analytical dimension table for the Part 3 bulk load
(10M+ synthetic rows). Deliberately separate from the OLTP `orders` table —
loading synthetic history into the live transactional table would pollute
the streaming-ingestion demo and the two workloads have opposite index
strategies (OLTP wants indexes up front, bulk load wants them added after).

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-17
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mssql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # No secondary indexes at creation time on purpose: they would have to be
    # maintained on every one of the 10M inserted rows, which is the single
    # biggest avoidable cost in a bulk load. The DAG adds them in a dedicated
    # task after the data is in.
    op.create_table(
        "order_history_fact",
        sa.Column("order_history_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("external_order_id", sa.String(64), nullable=False),
        sa.Column("client_external_id", sa.String(64), nullable=False),
        # SQLAlchemy's generic sa.Date renders as DATETIME on the mssql
        # dialect (a long-standing quirk); mssql.DATE gets the real 3-byte
        # SQL Server DATE type, which matters at 10M+ rows and is the
        # semantically correct type for a column with no time component.
        sa.Column("order_date", mssql.DATE, nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("total_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("region", sa.String(50), nullable=False),
        sa.Column("channel", sa.String(30), nullable=False),
        sa.Column("item_count", sa.Integer, nullable=False),
        sa.Column("load_batch_id", sa.String(50), nullable=False),
        sa.Column("loaded_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
    )


def downgrade() -> None:
    op.drop_table("order_history_fact")
