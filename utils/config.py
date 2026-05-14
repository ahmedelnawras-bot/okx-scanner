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
    min_execution_score: float = float(os.getenv("MIN_EXECUTION_SCORE", "6.5"))
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "15"))
    scan_interval_seconds: int = int(os.getenv("SCAN_INTERVAL_SECONDS", "90"))
    market_mode_guard_interval_seconds: int = int(os.getenv("MARKET_MODE_GUARD_INTERVAL_SECONDS", "45"))
    max_execution_positions: int = int(os.getenv("MAX_OPEN_POSITIONS", str(MAX_EXECUTION_POSITIONS)))
    telegram_enabled: bool = os.getenv("TELEGRAM_ENABLED", "1").lower() in ("1", "true", "yes", "on")
    send_normal_signals: bool = os.getenv("SEND_NORMAL_SIGNALS", "1").lower() in ("1", "true", "yes", "on")
    send_mode_status_each_scan: bool = os.getenv("SEND_MODE_STATUS_EACH_SCAN", "0").lower() in ("1", "true", "yes", "on")
    execution_enabled: bool = os.getenv("EXECUTION_ENABLED", "0").lower() in ("1", "true", "yes", "on")
    okx_place_orders: bool = os.getenv("OKX_PLACE_ORDERS", "0").lower() in ("1", "true", "yes", "on")
    okx_simulated: bool = os.getenv("OKX_SIMULATED", "1").lower() in ("1", "true", "yes", "on")
    allow_live_trading: bool = os.getenv("ALLOW_LIVE_TRADING", "0").lower() in ("1", "true", "yes", "on")
    okx_api_key: str = os.getenv("OKX_API_KEY", "")
    okx_api_secret: str = os.getenv("OKX_API_SECRET", "")
    okx_passphrase: str = os.getenv("OKX_PASSPHRASE", "")
    default_leverage: int = int(os.getenv("DEFAULT_LEVERAGE", "15"))
    paper_margin_usdt: float = float(os.getenv("PAPER_MARGIN_USDT", "35"))
    okx_td_mode: str = os.getenv("OKX_TD_MODE", "cross")
    verbose_logs: bool = os.getenv("VERBOSE_LOGS", "0").lower() in ("1", "true", "yes", "on")
    reminder_first_minutes: int = 15
    reminder_second_minutes: int = 15
    reminder_third_minutes: int = 10


def get_settings() -> Settings:
    return Settings()
