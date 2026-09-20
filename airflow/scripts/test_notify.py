"""Unit tests for notify.py — mocks the HTTP call, no real Telegram API."""
from unittest.mock import Mock, patch

import notify


def test_skips_silently_without_credentials(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    with patch("notify.requests.post") as mock_post:
        notify.send_telegram_message("hello")
    mock_post.assert_not_called()


def test_posts_to_the_right_chat_when_configured(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:abc")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "456")
    with patch("notify.requests.post") as mock_post:
        mock_post.return_value = Mock(raise_for_status=Mock())
        notify.send_telegram_message("done")
    url, kwargs = mock_post.call_args
    assert url[0] == "https://api.telegram.org/bot123:abc/sendMessage"
    assert kwargs["json"] == {"chat_id": "456", "text": "done"}


def test_delivery_failure_does_not_raise(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:abc")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "456")
    with patch("notify.requests.post", side_effect=ConnectionError("network down")):
        notify.send_telegram_message("done")  # must not raise
