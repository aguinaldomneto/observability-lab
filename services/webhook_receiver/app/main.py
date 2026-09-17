"""Thin, stateless webhook edge.

This service has exactly one job: accept the partner webhook over HTTP,
validate its shape, and hand it to Kafka/Redpanda as fast as possible. It
never touches SQL Server. That is the whole performance argument for
choosing Kafka over a FastAPI-does-everything design (see README): under a
burst of thousands of req/s, this process can never be the thing that blocks
on a database lock or connection-pool exhaustion, because it has no
database connection to exhaust. Backpressure is inherited for free from the
Kafka producer's send buffer — if Redpanda (or the network to it) is slow,
`producer.send_and_wait` simply takes longer to return, which naturally
throttles the caller instead of piling up unbounded work in this process.
"""
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Literal

import orjson
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_WEBHOOK_EVENTS = os.environ.get("TOPIC_WEBHOOK_EVENTS", "webhook-events")

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


@app.post("/webhooks/events", status_code=202)
async def receive_webhook(envelope: WebhookEnvelope, request: Request) -> dict[str, str]:
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

    await producer.send_and_wait(
        TOPIC_WEBHOOK_EVENTS,
        key=partition_key.encode("utf-8"),
        value=value,
    )
    return {"status": "accepted", "event_id": envelope.event_id}
