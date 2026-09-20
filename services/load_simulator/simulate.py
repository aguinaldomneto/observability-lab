"""Load/idempotency test harness for the webhook receiver.

Fires a burst of synthetic ORDER_CREATED -> ORDER_STATUS_CHANGED(APPROVED)
-> PAYMENT_CONFIRMED -> INVOICE_ISSUED sequences at the webhook receiver,
with a configurable fraction of events re-sent verbatim (same event_id) to
prove the pipeline is idempotent: re-sending must not create duplicate rows
downstream. Run this against a live `docker compose up` stack.

Usage:
    python simulate.py --orders 2000 --concurrency 200 --duplicate-rate 0.1
"""
import argparse
import asyncio
import random
import time
import uuid
from typing import Any

import httpx

WEBHOOK_URL_DEFAULT = "http://localhost:8000/webhooks/events"


def make_order_created(order_seq: int) -> tuple[dict[str, Any], str]:
    external_order_id = f"ORD-{order_seq:08d}"
    external_client_id = f"CLI-{order_seq % 5000:06d}"
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data": {
            "external_order_id": external_order_id,
            "client": {
                "external_client_id": external_client_id,
                "name": f"Client {order_seq % 5000}",
                "email": f"client{order_seq % 5000}@example.com",
                "document": f"{(order_seq % 99999999):011d}",
            },
            "address": {
                "external_address_id": f"ADDR-{order_seq % 5000:06d}",
                "street": "Rua Synthetic",
                "number": str(100 + order_seq % 900),
                "complement": None,
                "neighborhood": "Centro",
                "city": "Sao Paulo",
                "state": "SP",
                "postal_code": "01000-000",
                "country": "BR",
            },
            "items": [
                {"sku": f"SKU-{order_seq % 100}", "description": "Widget", "quantity": 2, "unit_price": 49.9}
            ],
            "total_amount": 99.8,
            "currency": "BRL",
            "order_date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    }, external_order_id


def make_status_changed(external_order_id: str, new_status: str) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_STATUS_CHANGED",
        "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data": {"external_order_id": external_order_id, "new_status": new_status},
    }


def make_payment_confirmed(order_seq: int, external_order_id: str) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "PAYMENT_CONFIRMED",
        "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data": {
            "external_payment_id": f"PAY-{order_seq:08d}",
            "external_order_id": external_order_id,
            "method": "PIX",
            "status": "CONFIRMED",
            "amount": 99.8,
            "paid_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    }


async def send(
    client: httpx.AsyncClient, url: str, payload: dict[str, Any], results: list[tuple[int, float]]
) -> None:
    start = time.perf_counter()
    try:
        resp = await client.post(url, json=payload, timeout=10.0)
        results.append((resp.status_code, time.perf_counter() - start))
    except httpx.HTTPError as exc:
        results.append((0, time.perf_counter() - start))
        print(f"request failed: {exc}")


async def run(args: argparse.Namespace) -> None:
    sem = asyncio.Semaphore(args.concurrency)
    results: list[tuple[int, float]] = []

    async def bounded_send(client: httpx.AsyncClient, url: str, payload: dict[str, Any]) -> None:
        async with sem:
            await send(client, url, payload, results)

    async def run_order(client: httpx.AsyncClient, seq: int) -> bool:
        # Each order's own events are sent strictly in causal order (created
        # -> approved -> payment) — a real partner would never fire
        # ORDER_STATUS_CHANGED before the order it refers to exists. Ordering
        # is only guaranteed *within* one order's sequence; different orders
        # still run fully concurrently against each other via the semaphore,
        # so this doesn't reduce overall throughput.
        created_event, external_order_id = make_order_created(seq)
        await bounded_send(client, args.url, created_event)

        approved_event = make_status_changed(external_order_id, "APPROVED")
        await bounded_send(client, args.url, approved_event)

        payment_event = make_payment_confirmed(seq, external_order_id)
        await bounded_send(client, args.url, payment_event)

        if random.random() < args.duplicate_rate:
            await bounded_send(client, args.url, dict(created_event))
            return True
        return False

    async with httpx.AsyncClient() as client:
        start = time.perf_counter()
        outcomes = await asyncio.gather(*(run_order(client, seq) for seq in range(args.orders)))
        elapsed = time.perf_counter() - start

    duplicates_sent = sum(outcomes)

    ok = sum(1 for status, _ in results if 200 <= status < 300)
    total = len(results)
    latencies = sorted(lat for _, lat in results)
    p50 = latencies[len(latencies) // 2] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99) - 1] if latencies else 0

    print(f"orders simulated:     {args.orders}")
    print(f"duplicate replays:    {duplicates_sent}")
    print(f"total requests:       {total}")
    print(f"successful (2xx):     {ok} ({ok/total:.1%})" if total else "no requests sent")
    print(f"elapsed:              {elapsed:.2f}s ({total/elapsed:,.0f} req/s)")
    print(f"latency p50 / p99:    {p50*1000:.1f}ms / {p99*1000:.1f}ms")
    print(
        "\nNext: check the target DB — `orders` count should equal --orders, and "
        "`processed_webhook_events` count should be less than total requests sent "
        "(the duplicated ORDER_CREATED events must be no-ops)."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=WEBHOOK_URL_DEFAULT)
    parser.add_argument("--orders", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--duplicate-rate", type=float, default=0.1)
    asyncio.run(run(parser.parse_args()))
