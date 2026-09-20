"""Idempotent Kafka -> SQL Server consumer.

Offsets are committed manually, only after the DB transaction for that
message has committed — a crash mid-batch just replays already-idempotent
messages. See README for the full concurrency/scaling story.
"""
import asyncio
import decimal
import logging
import os
from typing import Any

import db_ops
import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from prometheus_client import Counter, start_http_server
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from common.db import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestion-consumer")

METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))
events_processed_total = Counter(
    "events_processed_total", "Webhook events processed", ["event_type", "outcome"]
)


def _json_default(value: Any) -> float:
    """orjson has no native Decimal support (SQL Server NUMERIC columns come
    back from pyodbc as decimal.Decimal); this is the fallback orjson calls
    for anything it doesn't recognize."""
    if isinstance(value, decimal.Decimal):
        return float(value)
    raise TypeError(f"Type is not JSON serializable: {type(value)}")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_WEBHOOK_EVENTS = os.environ.get("TOPIC_WEBHOOK_EVENTS", "webhook-events")
TOPIC_ORDER_APPROVED = os.environ.get("TOPIC_ORDER_APPROVED", "order-approved")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "ingestion-consumer")
BATCH_MAX_RECORDS = int(os.environ.get("BATCH_MAX_RECORDS", "200"))
BATCH_TIMEOUT_MS = int(os.environ.get("BATCH_TIMEOUT_MS", "500"))

engine = get_engine(pool_size=10, max_overflow=10)


def process_event(conn: Connection, event: dict[str, Any]) -> dict[str, Any] | None:
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


async def handle_message(msg: ConsumerRecord, producer: AIOKafkaProducer) -> None:
    try:
        event = orjson.loads(msg.value)
        event_id = event["event_id"]
    except Exception as exc:
        # Malformed JSON or a missing event_id — not something a retry
        # would ever fix. Log and move on instead of crashing every
        # partition this consumer owns over one poison message.
        log.exception("Malformed message at offset %s — skipping", msg.offset)
        db_ops.log_pipeline_event(
            engine, "ingestion-consumer", "ERROR", f"malformed message at offset {msg.offset}: {exc}"
        )
        events_processed_total.labels(event_type="unknown", outcome="malformed").inc()
        return

    # OperationalError covers deadlocks, lock-wait timeouts (see
    # common.db's SET LOCK_TIMEOUT) and dropped connections — genuinely
    # transient conditions worth a few quick retries. reraise=True means
    # tenacity re-raises this same exception type once attempts are
    # exhausted, not tenacity.RetryError — the except clause below relies
    # on that.
    @retry(
        retry=retry_if_exception_type(OperationalError),
        wait=wait_exponential_jitter(initial=0.2, max=2),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _run_in_txn() -> dict[str, Any] | None:
        with engine.begin() as conn:
            outbox = process_event(conn, event)
            db_ops.mark_event_processed(conn, event_id)
            return outbox

    try:
        outbox = await asyncio.to_thread(_run_in_txn)
    except db_ops.DuplicateEvent:
        log.info("Duplicate event %s — already processed, skipping (idempotent no-op)", event_id)
        events_processed_total.labels(event_type=event["event_type"], outcome="duplicate").inc()
        return
    except OperationalError as exc:
        log.error("DB still unavailable/blocked after retries for event %s: %s", event_id, exc)
        db_ops.log_pipeline_event(
            engine, "ingestion-consumer", "ERROR", f"DB unavailable after retries: {exc}", event_id
        )
        events_processed_total.labels(event_type=event["event_type"], outcome="db_unavailable").inc()
        return
    except SQLAlchemyError as exc:
        # Not retried: data errors (e.g. truncation) or a rejected write
        # (e.g. the retroactive-update trigger) will fail identically
        # every time. A single bad message must never take down the
        # whole consumer process.
        log.error("DB error processing event %s: %s", event_id, exc)
        db_ops.log_pipeline_event(engine, "ingestion-consumer", "ERROR", f"DB error: {exc}", event_id)
        events_processed_total.labels(event_type=event["event_type"], outcome="db_error").inc()
        return
    except Exception as exc:
        # Same principle for anything process_event itself can raise on a
        # malformed but valid-JSON payload (e.g. a missing "data" field) —
        # a bad message must never take the whole consumer down.
        log.exception("Unexpected error processing event %s — skipping", event_id)
        db_ops.log_pipeline_event(
            engine, "ingestion-consumer", "ERROR", f"unexpected error: {exc}", event_id
        )
        events_processed_total.labels(event_type="unknown", outcome="unexpected_error").inc()
        return

    events_processed_total.labels(event_type=event["event_type"], outcome="success").inc()

    if outbox is not None and outbox["event_type"] == "ORDER_APPROVED":
        try:
            await producer.send_and_wait(
                TOPIC_ORDER_APPROVED,
                key=str(outbox["order"]["order_id"]).encode(),
                value=orjson.dumps(outbox["order"], default=_json_default),
            )
            log.info("Published ORDER_APPROVED for order_id=%s", outbox["order"]["order_id"])
        except Exception as exc:
            # The DB write already committed — the order IS approved. A
            # failed publish must not crash the consumer (see README,
            # "transactional outbox" under limitations, for the known gap).
            order_id = outbox["order"]["order_id"]
            log.exception(
                "Failed to publish ORDER_APPROVED for order_id=%s — order is committed in the "
                "DB but the external-integration event was NOT published",
                order_id,
            )
            db_ops.log_pipeline_event(
                engine,
                "ingestion-consumer",
                "ERROR",
                f"ORDER_APPROVED publish failed for order_id={order_id}: {exc}",
                event_id,
            )
            events_processed_total.labels(event_type="ORDER_APPROVED", outcome="publish_failed").inc()


async def main() -> None:
    start_http_server(METRICS_PORT)
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
