"""initial schema: clients, address (SCD2), orders, order_items,
order_status_history, payment, invoices, processed_webhook_events

Revision ID: 0001
Revises:
Create Date: 2026-09-17
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "clients",
        sa.Column("client_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("external_client_id", sa.String(64), nullable=False),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("email", sa.String(200), nullable=False),
        sa.Column("document", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.UniqueConstraint("external_client_id", name="uq_clients_external_client_id"),
    )

    op.create_table(
        "address",
        sa.Column("address_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("client_id", sa.BigInteger, sa.ForeignKey("clients.client_id"), nullable=False),
        sa.Column("external_address_id", sa.String(64), nullable=False),
        sa.Column("street", sa.String(200), nullable=False),
        sa.Column("number", sa.String(20), nullable=False),
        sa.Column("complement", sa.String(100), nullable=True),
        sa.Column("neighborhood", sa.String(100), nullable=False),
        sa.Column("city", sa.String(100), nullable=False),
        sa.Column("state", sa.String(2), nullable=False),
        sa.Column("postal_code", sa.String(16), nullable=False),
        sa.Column("country", sa.String(2), nullable=False, server_default="BR"),
        sa.Column("valid_from", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.Column("valid_to", sa.DateTime, nullable=True),
        sa.Column("is_current", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
    )
    op.create_index("ix_address_client_external", "address", ["client_id", "external_address_id"])
    # Filtered unique index: only one CURRENT version per (client, logical address).
    # This is SQL-Server-specific (WHERE clause on the index) and is exactly what
    # enforces the SCD2 invariant at the database level, not just in application code.
    op.execute(
        """
        CREATE UNIQUE INDEX uq_address_current_per_client
        ON address (client_id, external_address_id)
        WHERE is_current = 1
        """
    )

    op.create_table(
        "orders",
        sa.Column("order_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("external_order_id", sa.String(64), nullable=False),
        sa.Column("client_id", sa.BigInteger, sa.ForeignKey("clients.client_id"), nullable=False),
        sa.Column("address_id", sa.BigInteger, sa.ForeignKey("address.address_id"), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
        sa.Column("total_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False, server_default="BRL"),
        sa.Column("order_date", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.Column("closed_at", sa.DateTime, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.UniqueConstraint("external_order_id", name="uq_orders_external_order_id"),
        sa.CheckConstraint(
            "status IN ('PENDING','APPROVED','SHIPPED','DELIVERED','CANCELLED','CLOSED')",
            name="ck_orders_status",
        ),
    )
    op.create_index("ix_orders_client_id", "orders", ["client_id"])
    op.create_index("ix_orders_status", "orders", ["status"])

    op.create_table(
        "order_items",
        sa.Column("order_item_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.BigInteger, sa.ForeignKey("orders.order_id"), nullable=False),
        sa.Column("sku", sa.String(64), nullable=False),
        sa.Column("description", sa.String(200), nullable=False),
        sa.Column("quantity", sa.BigInteger, nullable=False),
        sa.Column("unit_price", sa.Numeric(18, 2), nullable=False),
    )
    op.create_index("ix_order_items_order_id", "order_items", ["order_id"])

    op.create_table(
        "order_status_history",
        sa.Column("history_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.BigInteger, sa.ForeignKey("orders.order_id"), nullable=False),
        sa.Column("previous_status", sa.String(20), nullable=True),
        sa.Column("new_status", sa.String(20), nullable=False),
        sa.Column("changed_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.Column("changed_by", sa.String(100), nullable=False),
        sa.Column("source_event_id", sa.String(100), nullable=True),
    )
    op.create_index("ix_order_status_history_order_id", "order_status_history", ["order_id"])

    op.create_table(
        "payment",
        sa.Column("payment_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("external_payment_id", sa.String(64), nullable=False),
        sa.Column("order_id", sa.BigInteger, sa.ForeignKey("orders.order_id"), nullable=False),
        sa.Column("method", sa.String(30), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("paid_at", sa.DateTime, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.UniqueConstraint("external_payment_id", name="uq_payment_external_payment_id"),
        sa.CheckConstraint(
            "status IN ('PENDING','CONFIRMED','FAILED','REFUNDED')", name="ck_payment_status"
        ),
    )
    op.create_index("ix_payment_order_id", "payment", ["order_id"])

    op.create_table(
        "invoices",
        sa.Column("invoice_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("external_invoice_id", sa.String(64), nullable=False),
        sa.Column("order_id", sa.BigInteger, sa.ForeignKey("orders.order_id"), nullable=False),
        sa.Column("invoice_number", sa.String(50), nullable=False),
        sa.Column("issued_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="ISSUED"),
        sa.Column("pdf_url", sa.String(500), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.UniqueConstraint("external_invoice_id", name="uq_invoices_external_invoice_id"),
        sa.CheckConstraint("status IN ('ISSUED','CANCELLED')", name="ck_invoice_status"),
    )
    op.create_index("ix_invoices_order_id", "invoices", ["order_id"])

    op.create_table(
        "processed_webhook_events",
        sa.Column("event_id", sa.String(100), primary_key=True),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("received_at", sa.DateTime, nullable=False, server_default=sa.func.sysutcdatetime()),
        sa.Column("processed_at", sa.DateTime, nullable=True),
    )

    # Business rule: once an order reaches a terminal status, it is closed for
    # retroactive edits. Enforcing this only in application code means every
    # future service (including this one, months from now) has to remember
    # the rule; a trigger makes it impossible to violate regardless of caller.
    op.execute(
        """
        CREATE TRIGGER trg_orders_block_retro_update
        ON orders
        AFTER UPDATE
        AS
        BEGIN
            SET NOCOUNT ON;
            SET XACT_ABORT ON;
            IF EXISTS (
                SELECT 1
                FROM deleted d
                WHERE d.status IN ('DELIVERED', 'CANCELLED', 'CLOSED')
            )
            BEGIN
                RAISERROR (
                    'Order is in a terminal status and cannot be modified retroactively.',
                    16, 1
                );
                ROLLBACK TRANSACTION;
            END
        END
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_orders_block_retro_update")
    op.drop_table("processed_webhook_events")
    op.drop_table("invoices")
    op.drop_table("payment")
    op.drop_table("order_status_history")
    op.drop_table("order_items")
    op.drop_table("orders")
    op.execute("DROP INDEX IF EXISTS uq_address_current_per_client ON address")
    op.drop_table("address")
    op.drop_table("clients")
