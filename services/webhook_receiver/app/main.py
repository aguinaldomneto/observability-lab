"""Thin, stateless webhook edge: validates the payload and hands it to Kafka.
Never touches SQL Server — see README for why that split matters under load.
"""
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Literal

import orjson
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Response
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic import BaseModel, Field

from common.telemetry import inject_kafka_headers, setup_tracing

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("webhook-receiver")
tracer = setup_tracing("webhook-receiver")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_WEBHOOK_EVENTS = os.environ.get("TOPIC_WEBHOOK_EVENTS", "webhook-events")

webhooks_received_total = Counter(
    "webhooks_received_total", "Webhooks accepted and published to Kafka", ["event_type"]
)
webhook_publish_failures_total = Counter(
    "webhook_publish_failures_total", "Webhooks that failed to publish to Kafka"
)

producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        enable_idempotence=True,
        linger_ms=5,
        compression_type="lz4",
        max_batch_size=64 * 1024,
    )
    await producer.start()
    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(title="webhook-receiver", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app, excluded_urls="health,metrics")

EventType = Literal[
    "ORDER_CREATED", "ORDER_STATUS_CHANGED", "PAYMENT_CONFIRMED", "INVOICE_ISSUED"
]


class WebhookEnvelope(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=100)
    event_type: EventType
    occurred_at: str
    data: dict[str, Any]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/webhooks/events", status_code=202)
async def receive_webhook(envelope: WebhookEnvelope) -> dict[str, str]:
    if producer is None:
        raise HTTPException(status_code=503, detail="producer not ready")

    # Partition key = the entity's natural id when present, so all events for
    # the same order land on the same partition and are processed in order
    # by a single consumer instance (order-of-events matters: you cannot
    # apply PAYMENT_CONFIRMED before ORDER_CREATED has landed).
    partition_key = (
        envelope.data.get("external_order_id")
        or envelope.data.get("external_client_id")
        or envelope.event_id
    )

    value = orjson.dumps(
        {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "occurred_at": envelope.occurred_at,
            "received_at": time.time(),
            "data": envelope.data,
        }
    )

    # PRODUCER span: its context rides in the Kafka headers, so the consumer's
    # span becomes a child of this one even though they share no process.
    with tracer.start_as_current_span(
        f"{TOPIC_WEBHOOK_EVENTS} publish", kind=trace.SpanKind.PRODUCER
    ) as span:
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute("messaging.destination.name", TOPIC_WEBHOOK_EVENTS)
        span.set_attribute("event.id", envelope.event_id)
        span.set_attribute("event.type", envelope.event_type)
        try:
            await producer.send_and_wait(
                TOPIC_WEBHOOK_EVENTS,
                key=partition_key.encode("utf-8"),
                value=value,
                headers=inject_kafka_headers(),
            )
        except Exception:
            log.exception("Failed to publish event %s to Kafka", envelope.event_id)
            webhook_publish_failures_total.inc()
            raise HTTPException(status_code=503, detail="event broker unavailable, retry later") from None
    webhooks_received_total.labels(event_type=envelope.event_type).inc()
    return {"status": "accepted", "event_id": envelope.event_id}
