"""Small Telegram Bot API helper used by the live worker.

Kept separate from core logic so the scanner can still run offline in tests.
"""
from __future__ import annotations

import requests
from dataclasses import dataclass
from typing import Any


TELEGRAM_SAFE_CHUNK_SIZE = 3500
TELEGRAM_HARD_LIMIT = 4096


def split_long_message(text: str, limit: int = TELEGRAM_SAFE_CHUNK_SIZE) -> list[str]:
    """Split long Telegram messages safely on line boundaries when possible.

    Telegram has a 4096-character hard limit. We keep a lower limit to leave
    room for part headers and avoid API rejection. The splitter prefers newline
    boundaries so report cards are not cut in the middle of a line.
    """
    text = str(text or "")
    if not text:
        return [""]
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for raw_line in text.splitlines(keepends=True):
        line = raw_line
        # A single very long line still needs hard splitting.
        while len(line) > limit:
            if current:
                chunks.append("".join(current).rstrip())
                current = []
                current_len = 0
            chunks.append(line[:limit].rstrip())
            line = line[limit:]

        if current_len + len(line) > limit and current:
            chunks.append("".join(current).rstrip())
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += len(line)

    if current:
        chunks.append("".join(current).rstrip())

    return [chunk for chunk in chunks if chunk] or [""]


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

    def _post_message(self, payload: dict[str, Any]) -> dict:
        try:
            response = requests.post(f"{self.api_base}/sendMessage", json=payload, timeout=self.timeout)
            data = response.json() if response.content else {"ok": response.ok}
            if not response.ok and "ok" not in data:
                data["ok"] = False
            return data
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def send_message(
        self,
        text: str,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict:
        if not self.enabled:
            return {"ok": False, "skipped": True, "reason": "telegram_not_configured"}

        chunks = split_long_message(str(text or ""), TELEGRAM_SAFE_CHUNK_SIZE)
        total = len(chunks)
        results: list[dict] = []

        for index, chunk in enumerate(chunks, start=1):
            if total > 1:
                header = f"ðŸ“„ Part {index}/{total}\n"
                chunk_text = header + chunk
            else:
                chunk_text = chunk

            payload: dict[str, Any] = {
                "chat_id": self.chat_id,
                "text": chunk_text[:TELEGRAM_HARD_LIMIT],
                "disable_web_page_preview": True,
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode
            # Telegram reply_markup should be sent once only. Keep it on the
            # final part so command/report buttons stay below the last message.
            if reply_markup and index == total:
                payload["reply_markup"] = reply_markup

            result = self._post_message(payload)

            # If HTML/Markdown parsing fails, retry the same chunk as plain text
            # instead of silently losing the report.
            if not result.get("ok") and parse_mode:
                payload.pop("parse_mode", None)
                result = self._post_message(payload)

            results.append(result)

        return {
            "ok": all(bool(item.get("ok")) for item in results),
            "parts": total,
            "results": results,
        }

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
