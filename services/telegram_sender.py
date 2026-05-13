"""Small Telegram Bot API helper used by the live worker.

Kept separate from core logic so the scanner can still run offline in tests.
"""
from __future__ import annotations

import requests
from dataclasses import dataclass
from typing import Any


@dataclass
class TelegramSender:
    bot_token: str
    chat_id: str
    timeout: int = 15

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    @property
    def api_base(self) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}"

    def send_message(
        self,
        text: str,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict:
        if not self.enabled:
            return {"ok": False, "skipped": True, "reason": "telegram_not_configured"}
        payload: dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": text[:3900],
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            response = requests.post(f"{self.api_base}/sendMessage", json=payload, timeout=self.timeout)
            return response.json() if response.content else {"ok": response.ok}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def get_updates(self, offset: int | None = None, timeout_seconds: int = 0) -> dict:
        if not self.enabled:
            return {"ok": False, "skipped": True, "reason": "telegram_not_configured"}
        params = {"timeout": timeout_seconds}
        if offset is not None:
            params["offset"] = offset
        try:
            response = requests.get(f"{self.api_base}/getUpdates", params=params, timeout=self.timeout)
            return response.json() if response.content else {"ok": response.ok, "result": []}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "result": []}

    def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> dict:
        if not self.enabled or not callback_query_id:
            return {"ok": False, "skipped": True}
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text[:180]
        try:
            response = requests.post(f"{self.api_base}/answerCallbackQuery", json=payload, timeout=self.timeout)
            return response.json() if response.content else {"ok": response.ok}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
