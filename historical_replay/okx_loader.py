from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests


@dataclass(frozen=True)
class HistoricalCandle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def timeframe_to_ms(timeframe: str) -> int:
    tf = str(timeframe or "15m").strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60_000
    if tf.endswith("h"):
        return int(tf[:-1]) * 3_600_000
    if tf.endswith("d"):
        return int(tf[:-1]) * 86_400_000
    return 15 * 60_000


def fetch_swap_tickers(base_url: str, timeout: int = 15) -> list[dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/api/v5/market/tickers"
    resp = requests.get(url, params={"instType": "SWAP"}, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return data if isinstance(data, list) else []


def select_top_usdt_swap_symbols(tickers: list[dict[str, Any]], limit: int = 200) -> list[str]:
    excluded = ("USDC", "USDE", "FDUSD", "TUSD", "BUSD", "DAI")
    rows: list[tuple[float, str]] = []
    for item in tickers or []:
        symbol = str(item.get("instId") or item.get("symbol") or "")
        if not symbol.endswith("-USDT-SWAP"):
            continue
        if any(x in symbol for x in excluded):
            continue
        turnover = _safe_float(item.get("volCcy24h") or item.get("volCcyQuote24h") or item.get("quoteVolume"))
        rows.append((turnover, symbol))
    rows.sort(reverse=True)
    return [symbol for _, symbol in rows[: max(1, int(limit))]]


def _parse_candle(row: Any) -> HistoricalCandle | None:
    if not isinstance(row, (list, tuple)) or len(row) < 5:
        return None
    ts = _safe_int(row[0])
    open_ = _safe_float(row[1])
    high = _safe_float(row[2])
    low = _safe_float(row[3])
    close = _safe_float(row[4])
    volume = _safe_float(row[5] if len(row) > 5 else 0.0)
    # OKX swap candles usually include quote volume around index 7.
    quote_volume = _safe_float(row[7] if len(row) > 7 else (row[6] if len(row) > 6 else 0.0))
    if ts <= 0 or open_ <= 0 or high <= 0 or low <= 0 or close <= 0:
        return None
    return HistoricalCandle(ts=ts, open=open_, high=high, low=low, close=close, volume=volume, quote_volume=quote_volume)


def fetch_historical_candles(
    base_url: str,
    symbol: str,
    bar: str = "15m",
    days: int = 45,
    timeout: int = 15,
    pause_seconds: float = 0.06,
    max_pages: int = 120,
) -> list[HistoricalCandle]:
    """Fetch OKX historical candles, oldest first.

    Uses only OKX public market data. This function is isolated from live bot
    state and can be called by a replay worker/job safely.
    """
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = int((datetime.now(timezone.utc) - timedelta(days=int(days))).timestamp() * 1000)
    url = f"{base_url.rstrip('/')}/api/v5/market/history-candles"
    before: int | None = None
    by_ts: dict[int, HistoricalCandle] = {}

    for _ in range(max(1, int(max_pages))):
        params: dict[str, str] = {"instId": symbol, "bar": bar, "limit": "100"}
        if before is not None:
            params["before"] = str(before)
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", []) if isinstance(payload, dict) else []
        if not data:
            break

        oldest_seen: int | None = None
        for row in data:
            candle = _parse_candle(row)
            if candle is None:
                continue
            if start_ms <= candle.ts <= end_ms:
                by_ts[candle.ts] = candle
            oldest_seen = candle.ts if oldest_seen is None else min(oldest_seen, candle.ts)

        if oldest_seen is None or oldest_seen <= start_ms:
            break
        before = oldest_seen
        if pause_seconds > 0:
            time.sleep(float(pause_seconds))

    return sorted(by_ts.values(), key=lambda c: c.ts)
