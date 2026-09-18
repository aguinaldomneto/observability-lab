"""SQLAlchemy models — source of truth for the schema (Alembic migrations mirror these).

Design notes (see README for the full rationale):
  * Surrogate keys are BIGINT IDENTITY, not GUID/UNIQUEIDENTIFIER. SQL Server's
    clustered index is the table's physical row order; random GUID inserts
    fragment it under high write concurrency, which is exactly the failure
    mode this challenge is testing for. Every external system's natural key
    (external_order_id, external_payment_id, ...) is kept as a separate
    UNIQUE column and doubles as the idempotency key.
  * `address` is SCD Type 2: a client's address history is preserved, and an
    order stores a foreign key to the *specific version* of the address that
    was valid when the order was placed, so a later address edit never
    changes a historical order's snapshot.
  * `orders` is protected against retroactive mutation once it reaches a
    terminal status via an AFTER UPDATE trigger (see migration 0001), not by
    application code alone — a business rule this important should not
    depend on every future service remembering to enforce it.
"""
from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()


class Client(Base):
    __tablename__ = "clients"

    client_id = Column(BigInteger, primary_key=True, autoincrement=True)
    external_client_id = Column(String(64), nullable=False, unique=True)
    name = Column(String(200), nullable=False)
    email = Column(String(200), nullable=False)
    document = Column(String(32), nullable=False)
    created_at = Column(DateTime, nullable=False, default=utcnow)
    updated_at = Column(DateTime, nullable=False, default=utcnow, onupdate=utcnow)

    addresses = relationship("Address", back_populates="client")
    orders = relationship("Order", back_populates="client")


class Address(Base):
    """SCD Type 2: one row per version. `is_current=1` marks the active row."""

    __tablename__ = "address"

    address_id = Column(BigInteger, primary_key=True, autoincrement=True)
    client_id = Column(BigInteger, ForeignKey("clients.client_id"), nullable=False)
    external_address_id = Column(String(64), nullable=False)
    street = Column(String(200), nullable=False)
    number = Column(String(20), nullable=False)
    complement = Column(String(100), nullable=True)
    neighborhood = Column(String(100), nullable=False)
    city = Column(String(100), nullable=False)
    state = Column(String(2), nullable=False)
    postal_code = Column(String(16), nullable=False)
    country = Column(String(2), nullable=False, default="BR")
    valid_from = Column(DateTime, nullable=False, default=utcnow)
    valid_to = Column(DateTime, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=utcnow)

    client = relationship("Client", back_populates="addresses")

    __table_args__ = (
        Index("ix_address_client_external", "client_id", "external_address_id"),
    )


class Order(Base):
    __tablename__ = "orders"

    order_id = Column(BigInteger, primary_key=True, autoincrement=True)
    external_order_id = Column(String(64), nullable=False, unique=True)
    client_id = Column(BigInteger, ForeignKey("clients.client_id"), nullable=False)
    address_id = Column(BigInteger, ForeignKey("address.address_id"), nullable=False)
    status = Column(String(20), nullable=False, default="PENDING")
    total_amount = Column(Numeric(18, 2), nullable=False)
    currency = Column(String(3), nullable=False, default="BRL")
    order_date = Column(DateTime, nullable=False, default=utcnow)
    closed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utcnow)
    updated_at = Column(DateTime, nullable=False, default=utcnow, onupdate=utcnow)

    client = relationship("Client", back_populates="orders")
    address = relationship("Address")
    items = relationship("OrderItem", back_populates="order")
    payments = relationship("Payment", back_populates="order")
    invoices = relationship("Invoice", back_populates="order")

    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING','APPROVED','SHIPPED','DELIVERED','CANCELLED','CLOSED')",
            name="ck_orders_status",
        ),
    )


class OrderItem(Base):
    """Line items — not in the mandatory table list, but required to give the
    Part 4 payload ("itens do pedido") something real to carry."""

    __tablename__ = "order_items"

    order_item_id = Column(BigInteger, primary_key=True, autoincrement=True)
    order_id = Column(BigInteger, ForeignKey("orders.order_id"), nullable=False)
    sku = Column(String(64), nullable=False)
    description = Column(String(200), nullable=False)
    quantity = Column(BigInteger, nullable=False)
    unit_price = Column(Numeric(18, 2), nullable=False)

    order = relationship("Order", back_populates="items")


class OrderStatusHistory(Base):
    """Historical audit trail of order status transitions."""

    __tablename__ = "order_status_history"

    history_id = Column(BigInteger, primary_key=True, autoincrement=True)
    order_id = Column(BigInteger, ForeignKey("orders.order_id"), nullable=False)
    previous_status = Column(String(20), nullable=True)
    new_status = Column(String(20), nullable=False)
    changed_at = Column(DateTime, nullable=False, default=utcnow)
    changed_by = Column(String(100), nullable=False)
    source_event_id = Column(String(100), nullable=True)


class Payment(Base):
    __tablename__ = "payment"

    payment_id = Column(BigInteger, primary_key=True, autoincrement=True)
    external_payment_id = Column(String(64), nullable=False, unique=True)
    order_id = Column(BigInteger, ForeignKey("orders.order_id"), nullable=False)
    method = Column(String(30), nullable=False)
    status = Column(String(20), nullable=False, default="PENDING")
    amount = Column(Numeric(18, 2), nullable=False)
    paid_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=utcnow)
    updated_at = Column(DateTime, nullable=False, default=utcnow, onupdate=utcnow)

    order = relationship("Order", back_populates="payments")

    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING','CONFIRMED','FAILED','REFUNDED')",
            name="ck_payment_status",
        ),
    )


class Invoice(Base):
    __tablename__ = "invoices"

    invoice_id = Column(BigInteger, primary_key=True, autoincrement=True)
    external_invoice_id = Column(String(64), nullable=False, unique=True)
    order_id = Column(BigInteger, ForeignKey("orders.order_id"), nullable=False)
    invoice_number = Column(String(50), nullable=False)
    issued_at = Column(DateTime, nullable=False, default=utcnow)
    amount = Column(Numeric(18, 2), nullable=False)
    status = Column(String(20), nullable=False, default="ISSUED")
    pdf_url = Column(String(500), nullable=True)
    created_at = Column(DateTime, nullable=False, default=utcnow)

    order = relationship("Order", back_populates="invoices")

    __table_args__ = (
        CheckConstraint("status IN ('ISSUED','CANCELLED')", name="ck_invoice_status"),
    )


class ProcessedWebhookEvent(Base):
    """Idempotency ledger. The consumer inserts here (or upserts) in the same
    transaction as the business write; a PK violation means the event was
    already processed and the message is safely dropped/acked."""

    __tablename__ = "processed_webhook_events"

    event_id = Column(String(100), primary_key=True)
    event_type = Column(String(50), nullable=False)
    received_at = Column(DateTime, nullable=False, default=utcnow)
    processed_at = Column(DateTime, nullable=True)

    __table_args__ = (UniqueConstraint("event_id", name="uq_processed_event_id"),)
