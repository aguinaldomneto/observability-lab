"""Best-effort Telegram notifications for DAG completion.

No-ops (with a log warning) if TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID aren't
set, and never raises on a delivery failure — a missing or broken
notification must never be the reason a DAG run shows as failed.
"""
from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger("notify")


def send_telegram_message(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log.warning("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set — skipping notification: %s", text)
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception:
        log.exception("Failed to send Telegram notification")
