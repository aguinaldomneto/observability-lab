"""All SQL Server writes performed by the ingestion consumer.

Every function takes an open `Connection` that is already inside a
transaction (`engine.begin()` in consumer.py) — idempotency depends on the
event-ledger insert and the business writes committing or rolling back
together as a single unit.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

TERMINAL_STATUSES = {"DELIVERED", "CANCELLED", "CLOSED"}


class DuplicateEvent(Exception):
    """Raised when the event_id was already processed."""


class RetroactiveUpdateRejected(Exception):
    """Raised when a status-change event targets an order that is already
    terminal — the DB trigger enforces this, this is the app-level mirror."""


def claim_event(conn: Connection, event_id: str, event_type: str) -> None:
    """Insert into the idempotency ledger. Raises DuplicateEvent on replay."""
    try:
        conn.execute(
            text(
                "INSERT INTO processed_webhook_events (event_id, event_type, received_at) "
                "VALUES (:event_id, :event_type, SYSUTCDATETIME())"
            ),
            {"event_id": event_id, "event_type": event_type},
        )
    except IntegrityError as exc:
        raise DuplicateEvent(event_id) from exc


def mark_event_processed(conn: Connection, event_id: str) -> None:
    conn.execute(
        text(
            "UPDATE processed_webhook_events SET processed_at = SYSUTCDATETIME() "
            "WHERE event_id = :event_id"
        ),
        {"event_id": event_id},
    )


def upsert_client(conn: Connection, client: dict[str, Any]) -> int:
    row = conn.execute(
        text(
            """
            MERGE clients AS target
            USING (SELECT :external_client_id AS external_client_id) AS src
            ON target.external_client_id = src.external_client_id
            WHEN MATCHED THEN
                UPDATE SET name = :name, email = :email, document = :document,
                           updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (external_client_id, name, email, document, created_at, updated_at)
                VALUES (:external_client_id, :name, :email, :document,
                        SYSUTCDATETIME(), SYSUTCDATETIME())
            OUTPUT inserted.client_id;
            """
        ),
        {
            "external_client_id": client["external_client_id"],
            "name": client["name"],
            "email": client["email"],
            "document": client["document"],
        },
    ).first()
    return int(row.client_id)


def upsert_current_address(conn: Connection, client_id: int, address: dict[str, Any]) -> int:
    """SCD2 upsert: reuse the current version if unchanged, otherwise expire
    it and insert a new version. Returns the address_id an order should
    reference (the current version's surrogate key at this point in time)."""
    fields = ["street", "number", "complement", "neighborhood", "city", "state", "postal_code", "country"]
    current = conn.execute(
        text(
            """
            SELECT address_id, street, number, complement, neighborhood, city, state,
                   postal_code, country
            FROM address
            WHERE client_id = :client_id AND external_address_id = :external_address_id
                  AND is_current = 1
            """
        ),
        {"client_id": client_id, "external_address_id": address["external_address_id"]},
    ).mappings().first()

    if current is not None:
        unchanged = all(
            (current[f] or None) == (address.get(f) or None) for f in fields
        )
        if unchanged:
            return int(current["address_id"])

        conn.execute(
            text(
                "UPDATE address SET valid_to = SYSUTCDATETIME(), is_current = 0 "
                "WHERE address_id = :address_id"
            ),
            {"address_id": current["address_id"]},
        )

    row = conn.execute(
        text(
            """
            INSERT INTO address
                (client_id, external_address_id, street, number, complement,
                 neighborhood, city, state, postal_code, country,
                 valid_from, is_current, created_at)
            OUTPUT inserted.address_id
            VALUES
                (:client_id, :external_address_id, :street, :number, :complement,
                 :neighborhood, :city, :state, :postal_code, :country,
                 SYSUTCDATETIME(), 1, SYSUTCDATETIME())
            """
        ),
        {
            "client_id": client_id,
            "external_address_id": address["external_address_id"],
            **{f: address.get(f) for f in fields},
        },
    ).first()
    return int(row.address_id)


def insert_order(
    conn: Connection, client_id: int, address_id: int, order: dict[str, Any]
) -> int:
    row = conn.execute(
        text(
            """
            INSERT INTO orders
                (external_order_id, client_id, address_id, status, total_amount,
                 currency, order_date, created_at, updated_at)
            OUTPUT inserted.order_id
            VALUES
                (:external_order_id, :client_id, :address_id, 'PENDING', :total_amount,
                 :currency, :order_date, SYSUTCDATETIME(), SYSUTCDATETIME())
            """
        ),
        {
            "external_order_id": order["external_order_id"],
            "client_id": client_id,
            "address_id": address_id,
            "total_amount": order["total_amount"],
            "currency": order.get("currency", "BRL"),
            "order_date": order.get("order_date") or dt.datetime.utcnow(),
        },
    ).first()
    return int(row.order_id)


def insert_order_items(conn: Connection, order_id: int, items: list[dict[str, Any]]) -> None:
    if not items:
        return
    conn.execute(
        text(
            """
            INSERT INTO order_items (order_id, sku, description, quantity, unit_price)
            VALUES (:order_id, :sku, :description, :quantity, :unit_price)
            """
        ),
        [{"order_id": order_id, **item} for item in items],
    )


def record_status_history(
    conn: Connection,
    order_id: int,
    previous_status: str | None,
    new_status: str,
    source_event_id: str,
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO order_status_history
                (order_id, previous_status, new_status, changed_at, changed_by, source_event_id)
            VALUES (:order_id, :previous_status, :new_status, SYSUTCDATETIME(),
                    'ingestion-consumer', :source_event_id)
            """
        ),
        {
            "order_id": order_id,
            "previous_status": previous_status,
            "new_status": new_status,
            "source_event_id": source_event_id,
        },
    )


def update_order_status(conn: Connection, external_order_id: str, new_status: str) -> tuple[int, str] | None:
    """Returns (order_id, previous_status), or None if the order isn't known
    yet (an out-of-order webhook arrived before ORDER_CREATED).

    SQL Server forbids `OUTPUT ... ` without an `INTO` clause on a table that
    has an enabled AFTER trigger for the same statement type — and `orders`
    has trg_orders_block_retro_update (AFTER UPDATE). Routing OUTPUT into a
    table variable first sidesteps that restriction; it does not weaken the
    trigger, which still fires and can still roll the whole batch back for a
    terminal order.
    """
    row = conn.execute(
        text(
            """
            DECLARE @updated TABLE (order_id BIGINT, previous_status VARCHAR(20));

            UPDATE orders
            SET status = :new_status,
                updated_at = SYSUTCDATETIME(),
                closed_at = CASE WHEN :new_status IN ('DELIVERED','CANCELLED','CLOSED')
                                  THEN SYSUTCDATETIME() ELSE closed_at END
            OUTPUT inserted.order_id, deleted.status INTO @updated
            WHERE external_order_id = :external_order_id;

            SELECT order_id, previous_status FROM @updated;
            """
        ),
        {"new_status": new_status, "external_order_id": external_order_id},
    ).first()
    if row is None:
        return None
    return int(row.order_id), row.previous_status


def fetch_order_snapshot(conn: Connection, order_id: int) -> dict[str, Any]:
    """Full denormalized payload for the Part 4 outbound integration."""
    order = conn.execute(
        text(
            """
            SELECT o.order_id, o.external_order_id, o.status, o.total_amount, o.currency,
                   o.order_date,
                   c.external_client_id, c.name AS client_name, c.email AS client_email,
                   c.document AS client_document,
                   a.street, a.number, a.complement, a.neighborhood, a.city, a.state,
                   a.postal_code, a.country
            FROM orders o
            JOIN clients c ON c.client_id = o.client_id
            JOIN address a ON a.address_id = o.address_id
            WHERE o.order_id = :order_id
            """
        ),
        {"order_id": order_id},
    ).mappings().first()

    items = conn.execute(
        text("SELECT sku, description, quantity, unit_price FROM order_items WHERE order_id = :order_id"),
        {"order_id": order_id},
    ).mappings().all()

    return {**dict(order), "items": [dict(i) for i in items]}


def upsert_payment(conn: Connection, order_id: int, payment: dict[str, Any]) -> None:
    conn.execute(
        text(
            """
            MERGE payment AS target
            USING (SELECT :external_payment_id AS external_payment_id) AS src
            ON target.external_payment_id = src.external_payment_id
            WHEN MATCHED THEN
                UPDATE SET status = :status, amount = :amount, paid_at = :paid_at,
                           updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (external_payment_id, order_id, method, status, amount, paid_at,
                        created_at, updated_at)
                VALUES (:external_payment_id, :order_id, :method, :status, :amount, :paid_at,
                        SYSUTCDATETIME(), SYSUTCDATETIME());
            """
        ),
        {
            "external_payment_id": payment["external_payment_id"],
            "order_id": order_id,
            "method": payment["method"],
            "status": payment.get("status", "CONFIRMED"),
            "amount": payment["amount"],
            "paid_at": payment.get("paid_at"),
        },
    )


def upsert_invoice(conn: Connection, order_id: int, invoice: dict[str, Any]) -> None:
    conn.execute(
        text(
            """
            MERGE invoices AS target
            USING (SELECT :external_invoice_id AS external_invoice_id) AS src
            ON target.external_invoice_id = src.external_invoice_id
            WHEN MATCHED THEN
                UPDATE SET status = :status, amount = :amount, pdf_url = :pdf_url
            WHEN NOT MATCHED THEN
                INSERT (external_invoice_id, order_id, invoice_number, issued_at, amount,
                        status, pdf_url, created_at)
                VALUES (:external_invoice_id, :order_id, :invoice_number, :issued_at, :amount,
                        :status, :pdf_url, SYSUTCDATETIME());
            """
        ),
        {
            "external_invoice_id": invoice["external_invoice_id"],
            "order_id": order_id,
            "invoice_number": invoice["invoice_number"],
            "issued_at": invoice.get("issued_at"),
            "amount": invoice["amount"],
            "status": invoice.get("status", "ISSUED"),
            "pdf_url": invoice.get("pdf_url"),
        },
    )


def resolve_order_id(conn: Connection, external_order_id: str) -> int | None:
    row = conn.execute(
        text("SELECT order_id FROM orders WHERE external_order_id = :eid"),
        {"eid": external_order_id},
    ).first()
    return int(row.order_id) if row else None
