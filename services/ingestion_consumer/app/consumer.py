"""Idempotent Kafka -> SQL Server consumer.

Concurrency / backpressure story (see README for the full writeup):
  * A small, bounded SQLAlchemy connection pool (see common/db.py) caps how
    many concurrent connections this process can ever open against SQL
    Server, no matter how fast Kafka can hand us messages.
  * Messages are consumed in small batches (`getmany`, `max_batch_size`) and
    each event is processed inside its own short transaction — no
    long-running transactions holding locks while we wait on I/O.
  * Kafka offsets are committed manually, only *after* the DB transaction for
    that message has committed. If this process crashes mid-batch, the
    un-committed messages are simply re-delivered on restart — which is safe
    because processing is idempotent (see claim_event in db_ops.py).
  * Scaling out is just running more instances of this container in the same
    consumer group: Kafka rebalances partitions across them automatically,
    and each partition is only ever owned by one consumer at a time, so
    there is no risk of two processes racing to write the same order.
"""
import asyncio
import logging
import os

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.exc import SQLAlchemyError

import db_ops
from common.db import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestion-consumer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_WEBHOOK_EVENTS = os.environ.get("TOPIC_WEBHOOK_EVENTS", "webhook-events")
TOPIC_ORDER_APPROVED = os.environ.get("TOPIC_ORDER_APPROVED", "order-approved")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "ingestion-consumer")
BATCH_MAX_RECORDS = int(os.environ.get("BATCH_MAX_RECORDS", "200"))
BATCH_TIMEOUT_MS = int(os.environ.get("BATCH_TIMEOUT_MS", "500"))

engine = get_engine(pool_size=10, max_overflow=10)


def process_event(conn, event: dict) -> dict | None:
    """Runs inside an open transaction. Returns an outbox event to publish
    *after* commit, or None."""
    event_id = event["event_id"]
    event_type = event["event_type"]
    data = event["data"]

    db_ops.claim_event(conn, event_id, event_type)

    if event_type == "ORDER_CREATED":
        client_id = db_ops.upsert_client(conn, data["client"])
        address_id = db_ops.upsert_current_address(conn, client_id, data["address"])
        order_id = db_ops.insert_order(conn, client_id, address_id, data)
        db_ops.insert_order_items(conn, order_id, data.get("items", []))
        db_ops.record_status_history(conn, order_id, None, "PENDING", event_id)
        return None

    if event_type == "ORDER_STATUS_CHANGED":
        result = db_ops.update_order_status(conn, data["external_order_id"], data["new_status"])
        if result is None:
            log.warning(
                "ORDER_STATUS_CHANGED for unknown order %s (event %s) — dropping; "
                "a real deployment would route this to a dead-letter topic and retry "
                "once ORDER_CREATED has been ingested.",
                data["external_order_id"],
                event_id,
            )
            return None
        order_id, previous_status = result
        db_ops.record_status_history(conn, order_id, previous_status, data["new_status"], event_id)
        if data["new_status"] == "APPROVED":
            snapshot = db_ops.fetch_order_snapshot(conn, order_id)
            return {"event_type": "ORDER_APPROVED", "order": snapshot}
        return None

    if event_type == "PAYMENT_CONFIRMED":
        order_id = db_ops.resolve_order_id(conn, data["external_order_id"])
        if order_id is None:
            log.warning("PAYMENT_CONFIRMED for unknown order %s — dropping", data["external_order_id"])
            return None
        db_ops.upsert_payment(conn, order_id, data)
        return None

    if event_type == "INVOICE_ISSUED":
        order_id = db_ops.resolve_order_id(conn, data["external_order_id"])
        if order_id is None:
            log.warning("INVOICE_ISSUED for unknown order %s — dropping", data["external_order_id"])
            return None
        db_ops.upsert_invoice(conn, order_id, data)
        return None

    log.warning("Unknown event_type %s (event %s) — dropping", event_type, event_id)
    return None


async def handle_message(msg, producer: AIOKafkaProducer) -> None:
    event = orjson.loads(msg.value)
    event_id = event.get("event_id", "<unknown>")

    def _run_in_txn():
        with engine.begin() as conn:
            outbox = process_event(conn, event)
            db_ops.mark_event_processed(conn, event_id)
            return outbox

    try:
        outbox = await asyncio.to_thread(_run_in_txn)
    except db_ops.DuplicateEvent:
        log.info("Duplicate event %s — already processed, skipping (idempotent no-op)", event_id)
        return
    except SQLAlchemyError as exc:
        # Covers real DB errors (e.g. the trigger rejecting a retroactive
        # update to a terminal order) and driver/result-handling errors alike
        # (SQLAlchemyError is the common base of DBAPIError and things like
        # ResourceClosedError) — a single bad message must never take down
        # the whole consumer process.
        log.error("DB error processing event %s: %s", event_id, exc)
        return

    if outbox is not None and outbox["event_type"] == "ORDER_APPROVED":
        await producer.send_and_wait(
            TOPIC_ORDER_APPROVED,
            key=str(outbox["order"]["order_id"]).encode(),
            value=orjson.dumps(outbox["order"]),
        )
        log.info("Published ORDER_APPROVED for order_id=%s", outbox["order"]["order_id"])


async def main() -> None:
    consumer = AIOKafkaConsumer(
        TOPIC_WEBHOOK_EVENTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=BATCH_MAX_RECORDS,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP, acks="all", enable_idempotence=True
    )

    await consumer.start()
    await producer.start()
    log.info("Consumer started: topic=%s group=%s", TOPIC_WEBHOOK_EVENTS, CONSUMER_GROUP)
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=BATCH_TIMEOUT_MS, max_records=BATCH_MAX_RECORDS)
            if not batches:
                continue
            for tp, messages in batches.items():
                for msg in messages:
                    await handle_message(msg, producer)
                await consumer.commit({tp: messages[-1].offset + 1})
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
