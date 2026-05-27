"""Small Telegram Bot API helper used by the live worker.

Kept separate from core logic so the scanner can still run offline in tests.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


# =========================================================
# Shared Session With Retry Protection
# =========================================================

_session = requests.Session()

_retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET", "POST"]),
)

_adapter = HTTPAdapter(max_retries=_retries)

_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


# =========================================================
# Async Telegram Queue
# =========================================================

telegram_queue: Queue = Queue()
_worker_started = False


@dataclass
class TelegramTask:
    method: str
    url: str
    kwargs: dict[str, Any]


def _telegram_worker() -> None:
    logger.info("Telegram worker started")

    while True:
        task: TelegramTask = telegram_queue.get()

        try:
            response = _session.request(
                method=task.method,
                url=task.url,
                **task.kwargs,
            )

            if not response.ok:
                logger.warning(
                    "Telegram API returned non-200 response",
                    extra={
                        "status_code": response.status_code,
                        "url": task.url,
                    },
                )

        except Exception:
            logger.exception("Telegram worker failed")

        finally:
            telegram_queue.task_done()


def _ensure_worker_started() -> None:
    global _worker_started

    if _worker_started:
        return

    worker = Thread(
        target=_telegram_worker,
        daemon=True,
        name="telegram-worker",
    )

    worker.start()

    _worker_started = True


@dataclass
class TelegramSender:
    bot_token: str
    chat_id: str
    timeout: int = 15

    def __post_init__(self) -> None:
        _ensure_worker_started()

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    @property
    def api_base(self) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}"

    # =========================================================
    # Internal Queue Helper
    # =========================================================

    def _enqueue(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> dict:
        if not self.enabled:
            return {
                "ok": False,
                "skipped": True,
                "reason": "telegram_not_configured",
            }

        try:
            telegram_queue.put(
                TelegramTask(
                    method=method,
                    url=f"{self.api_base}/{endpoint}",
                    kwargs=kwargs,
                )
            )

            return {
                "ok": True,
                "queued": True,
            }

        except Exception:
            logger.exception(
                "Failed to enqueue telegram task",
                extra={
                    "endpoint": endpoint,
                },
            )

            return {
                "ok": False,
                "queued": False,
            }

    # =========================================================
    # Send Message
    # =========================================================

    def send_message(
        self,
        text: str,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict:
        payload: dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": text[:3900],
            "disable_web_page_preview": True,
        }

        if parse_mode:
            payload["parse_mode"] = parse_mode

        if reply_markup:
            payload["reply_markup"] = reply_markup

        return self._enqueue(
            method="POST",
            endpoint="sendMessage",
            json=payload,
            timeout=self.timeout,
        )

    # =========================================================
    # Send Document
    # =========================================================

    def send_document(
        self,
        file_path: str,
        caption: str | None = None,
    ) -> dict:
        if not self.enabled:
            return {
                "ok": False,
                "skipped": True,
                "reason": "telegram_not_configured",
            }

        try:
            path = Path(file_path)
            filename = path.name or "report_export.dat"
            with open(path, "rb") as fh:
                content = fh.read()

            data: dict[str, Any] = {
                "chat_id": self.chat_id,
            }

            if caption:
                data["caption"] = caption[:1024]

            # Telegram uses the multipart filename. Sending raw bytes without a
            # filename makes clients download it as document.json/document.csv.
            return self._enqueue(
                method="POST",
                endpoint="sendDocument",
                data=data,
                files={
                    "document": (filename, content),
                },
                timeout=max(self.timeout, 60),
            )

        except Exception:
            logger.exception(
                "Failed to queue telegram document",
                extra={
                    "file_path": file_path,
                },
            )

            return {
                "ok": False,
                "queued": False,
            }

    # =========================================================
    # Get Updates
    # =========================================================

    def get_updates(
        self,
        offset: int | None = None,
        timeout_seconds: int = 0,
    ) -> dict:
        if not self.enabled:
            return {
                "ok": False,
                "skipped": True,
                "reason": "telegram_not_configured",
            }

        params = {
            "timeout": timeout_seconds,
        }

        if offset is not None:
            params["offset"] = offset

        try:
            response = _session.get(
                f"{self.api_base}/getUpdates",
                params=params,
                timeout=self.timeout,
            )

            return (
                response.json()
                if response.content
                else {"ok": response.ok, "result": []}
            )

        except Exception:
            logger.exception("Telegram getUpdates failed")

            return {
                "ok": False,
                "result": [],
            }

    # =========================================================
    # Answer Callback Query
    # =========================================================

    def answer_callback_query(
        self,
        callback_query_id: str,
        text: str | None = None,
    ) -> dict:
        if not self.enabled or not callback_query_id:
            return {
                "ok": False,
                "skipped": True,
            }

        payload: dict[str, Any] = {
            "callback_query_id": callback_query_id,
        }

        if text:
            payload["text"] = text[:180]

        return self._enqueue(
            method="POST",
            endpoint="answerCallbackQuery",
            json=payload,
            timeout=self.timeout,
        )
