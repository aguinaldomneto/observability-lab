"""Consumes `order-approved` (published the instant an order flips to
APPROVED, no polling) and POSTs it to the external ERP. Exhausted retries go
to a dead-letter topic instead of being dropped or blocking the partition.
"""
import asyncio
import logging
import os

import httpx
import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("order-approved-worker")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_ORDER_APPROVED = os.environ.get("TOPIC_ORDER_APPROVED", "order-approved")
TOPIC_DLQ = os.environ.get("TOPIC_ORDER_APPROVED_DLQ", "order-approved-dlq")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "order-approved-worker")
EXTERNAL_API_URL = os.environ.get("EXTERNAL_API_URL", "http://mock-external-api:9000/erp/orders")


class TransientDeliveryError(Exception):
    pass


@retry(
    retry=retry_if_exception_type(TransientDeliveryError),
    wait=wait_exponential_jitter(initial=0.5, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def deliver(client: httpx.AsyncClient, payload: dict) -> None:
    try:
        resp = await client.post(EXTERNAL_API_URL, json=payload, timeout=5.0)
    except httpx.TransportError as exc:
        raise TransientDeliveryError(str(exc)) from exc

    if resp.status_code >= 500:
        raise TransientDeliveryError(f"upstream returned {resp.status_code}")
    resp.raise_for_status()


async def main() -> None:
    consumer = AIOKafkaConsumer(
        TOPIC_ORDER_APPROVED,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, acks="all")

    await consumer.start()
    await producer.start()
    log.info("order-approved-worker started, target=%s", EXTERNAL_API_URL)

    async with httpx.AsyncClient() as client:
        try:
            async for msg in consumer:
                payload = orjson.loads(msg.value)
                order_id = payload.get("order_id")
                try:
                    await deliver(client, payload)
                    log.info("Delivered order_id=%s to external ERP", order_id)
                except RetryError:
                    log.error(
                        "Exhausted retries delivering order_id=%s — routing to DLQ", order_id
                    )
                    await producer.send_and_wait(TOPIC_DLQ, value=msg.value, key=msg.key)
                await consumer.commit()
        finally:
            await consumer.stop()
            await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
