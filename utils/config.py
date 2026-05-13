"""Environment-backed config. Keeps main.py orchestration-only."""
from __future__ import annotations

import os
from dataclasses import dataclass

from .constants import MAX_EXECUTION_POSITIONS


@dataclass(frozen=True)
class Settings:
    bot_token: str = os.getenv("BOT_TOKEN", "")
    chat_id: str = os.getenv("CHAT_ID", "")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    okx_base_url: str = os.getenv("OKX_BASE_URL", "https://www.okx.com")
    timeframe: str = os.getenv("TIMEFRAME", "15m")
    scan_limit: int = int(os.getenv("SCAN_LIMIT", "200"))
    min_normal_score: float = float(os.getenv("MIN_NORMAL_SCORE", "6.2"))
    min_strong_score: float = float(os.getenv("MIN_STRONG_SCORE", "7.5"))
    min_execution_score: float = float(os.getenv("MIN_EXECUTION_SCORE", "6.6"))
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "15"))
    max_execution_positions: int = int(os.getenv("MAX_OPEN_POSITIONS", str(MAX_EXECUTION_POSITIONS)))
    reminder_first_minutes: int = 15
    reminder_second_minutes: int = 15
    reminder_third_minutes: int = 10


def get_settings() -> Settings:
    return Settings()
