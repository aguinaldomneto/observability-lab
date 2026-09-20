"""Fictitious external ERP/logistics operator.

Deliberately flaky (`FAILURE_RATE`, default 30%) so the order-approved
worker's retry policy has something real to prove against — the challenge
explicitly asks for resilience to *temporary* failures of the external API.
"""
import os
import random
from typing import Any

from fastapi import FastAPI, HTTPException, Request

app = FastAPI(title="mock-external-erp")
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.3"))

received: list[dict[str, Any]] = []


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/erp/orders")
async def receive_order(request: Request) -> dict[str, Any]:
    if random.random() < FAILURE_RATE:
        raise HTTPException(status_code=503, detail="simulated transient failure")
    payload: dict[str, Any] = await request.json()
    received.append(payload)
    return {"status": "received", "external_order_id": payload.get("external_order_id")}


@app.get("/erp/orders/_debug")
async def debug_received() -> dict[str, Any]:
    """Test-only introspection endpoint to verify delivery in the demo."""
    return {"count": len(received), "orders": received[-20:]}
