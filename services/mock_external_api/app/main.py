"""Fictitious external ERP/logistics operator.

Deliberately flaky (`FAILURE_RATE`, default 30%) so the order-approved
worker's retry policy has something real to prove against — the challenge
explicitly asks for resilience to *temporary* failures of the external API.
"""
import os
import random
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

app = FastAPI(title="mock-external-erp")
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.3"))

received: list[dict[str, Any]] = []

erp_orders_total = Counter("erp_orders_total", "Orders received by the mock ERP", ["outcome"])


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/erp/orders")
async def receive_order(request: Request) -> dict[str, Any]:
    if random.random() < FAILURE_RATE:
        erp_orders_total.labels(outcome="simulated_failure").inc()
        raise HTTPException(status_code=503, detail="simulated transient failure")
    payload: dict[str, Any] = await request.json()
    received.append(payload)
    erp_orders_total.labels(outcome="received").inc()
    return {"status": "received", "external_order_id": payload.get("external_order_id")}


@app.get("/erp/orders/_debug")
async def debug_received() -> dict[str, Any]:
    """Test-only introspection endpoint to verify delivery in the demo."""
    return {"count": len(received), "orders": received[-20:]}
