"""Redis-backed persistence for trades and execution checks.

Keeps reports stable across Railway restarts/redeploys. Redis is optional at
runtime: if the library/server is unavailable, the bot continues in memory-only
mode and logs a clear warning instead of crashing.
"""
from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from .models import TrackedTrade

try:  # pragma: no cover - redis may not be installed locally during static checks.
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

RETENTION_DAYS = 35
RETENTION_SECONDS = RETENTION_DAYS * 24 * 60 * 60
PREFIX = "okx:longbot:v130"
OPEN_SET = f"{PREFIX}:trades:open"
HISTORY_SET = f"{PREFIX}:trades:history"
EXEC_CHECKS_LIST = f"{PREFIX}:execution:checks"


def _to_iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def trade_to_dict(trade: TrackedTrade) -> dict[str, Any]:
    data = asdict(trade) if is_dataclass(trade) else dict(trade)
    for key, value in list(data.items()):
        data[key] = _to_iso(value)
    return data


def trade_from_dict(data: dict[str, Any]) -> TrackedTrade | None:
    try:
        allowed = set(TrackedTrade.__dataclass_fields__.keys())
        clean = {k: v for k, v in dict(data).items() if k in allowed}
        for dt_key in ("opened_at", "updated_at", "closed_at"):
            if dt_key in clean:
                parsed = _parse_dt(clean.get(dt_key))
                if parsed is not None:
                    clean[dt_key] = parsed
                elif dt_key == "closed_at":
                    clean[dt_key] = None
        list_fields = ("execution_setup_tags", "warnings")
        for key in list_fields:
            if key in clean and not isinstance(clean[key], list):
                clean[key] = []
        return TrackedTrade(**clean)
    except Exception:
        return None


class RedisTradeStore:
    def __init__(self, redis_url: str | None):
        self.redis_url = redis_url or ""
        self.client = None
        self.enabled = False
        if redis is None or not self.redis_url:
            return
        try:
            self.client = redis.from_url(self.redis_url, decode_responses=True)
            self.client.ping()
            self.enabled = True
        except Exception as exc:
            print(f"⚠️ Redis persistence disabled: {exc}", flush=True)
            self.client = None
            self.enabled = False

    def _trade_key(self, trade_id: str) -> str:
        return f"{PREFIX}:trade:{trade_id}"

    def load_trades(self) -> list[TrackedTrade]:
        if not self.enabled or not self.client:
            return []
        trades: list[TrackedTrade] = []
        try:
            ids = set(self.client.smembers(OPEN_SET) or []) | set(self.client.smembers(HISTORY_SET) or [])
            missing_open: list[str] = []
            missing_history: list[str] = []
            for trade_id in ids:
                raw = self.client.get(self._trade_key(trade_id))
                if not raw:
                    if self.client.sismember(OPEN_SET, trade_id):
                        missing_open.append(trade_id)
                    if self.client.sismember(HISTORY_SET, trade_id):
                        missing_history.append(trade_id)
                    continue
                try:
                    trade = trade_from_dict(json.loads(raw))
                except Exception:
                    trade = None
                if trade:
                    trades.append(trade)
            if missing_open:
                self.client.srem(OPEN_SET, *missing_open)
            if missing_history:
                self.client.srem(HISTORY_SET, *missing_history)
        except Exception as exc:
            print(f"⚠️ Redis load_trades failed: {exc}", flush=True)
        return trades

    def save_trades(self, trades: list[TrackedTrade]) -> None:
        if not self.enabled or not self.client:
            return
        try:
            pipe = self.client.pipeline()
            for trade in trades:
                if not trade.trade_id:
                    continue
                key = self._trade_key(trade.trade_id)
                payload = json.dumps(trade_to_dict(trade), ensure_ascii=False)
                if trade.is_closed:
                    pipe.setex(key, RETENTION_SECONDS, payload)
                    pipe.srem(OPEN_SET, trade.trade_id)
                    pipe.sadd(HISTORY_SET, trade.trade_id)
                    pipe.expire(HISTORY_SET, RETENTION_SECONDS)
                else:
                    # Open trades should survive redeploy/restart. Keep a long TTL as a safety net.
                    pipe.setex(key, RETENTION_SECONDS * 3, payload)
                    pipe.sadd(OPEN_SET, trade.trade_id)
                    pipe.srem(HISTORY_SET, trade.trade_id)
            pipe.execute()
        except Exception as exc:
            print(f"⚠️ Redis save_trades failed: {exc}", flush=True)

    def append_execution_checks(self, execution_results: list[dict[str, Any]], limit: int = 10000) -> None:
        if not self.enabled or not self.client or not execution_results:
            return
        try:
            now = datetime.now(timezone.utc).isoformat()
            pipe = self.client.pipeline()
            for item in execution_results:
                payload = dict(item or {})
                payload["ts"] = now
                # Remove nested dataclass-like values if any.
                try:
                    encoded = json.dumps(payload, ensure_ascii=False, default=str)
                except Exception:
                    encoded = json.dumps({"status": payload.get("status"), "reason": payload.get("reason"), "path": payload.get("path"), "ts": now}, ensure_ascii=False)
                pipe.lpush(EXEC_CHECKS_LIST, encoded)
            pipe.ltrim(EXEC_CHECKS_LIST, 0, max(0, limit - 1))
            pipe.expire(EXEC_CHECKS_LIST, RETENTION_SECONDS)
            pipe.execute()
        except Exception as exc:
            print(f"⚠️ Redis append_execution_checks failed: {exc}", flush=True)

    def load_execution_checks(self) -> list[dict[str, Any]]:
        if not self.enabled or not self.client:
            return []
        try:
            rows = self.client.lrange(EXEC_CHECKS_LIST, 0, -1) or []
            out = []
            for raw in reversed(rows):
                try:
                    out.append(json.loads(raw))
                except Exception:
                    continue
            return out
        except Exception as exc:
            print(f"⚠️ Redis load_execution_checks failed: {exc}", flush=True)
            return []

    def soft_restart_safe_note(self) -> str:
        return f"Redis persistence: {'ON' if self.enabled else 'OFF'} | retention {RETENTION_DAYS}d"
