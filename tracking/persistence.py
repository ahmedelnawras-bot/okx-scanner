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

# Current namespace used by this build.
PREFIX = "okx:longbot:v130"

# Deep clean should wipe all historical OKX long-bot namespaces created by prior builds.
# This prevents old v123/v134/v147 execution checks/trades from leaking into fresh reports.
DEEP_CLEAN_PATTERNS = [
    "okx:longbot:*",
]

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

    def _scan_keys_for_patterns(self, patterns: list[str]) -> list[str]:
        """Return unique Redis keys matching one or more scan patterns."""
        if not self.enabled or not self.client:
            return []
        keys: set[str] = set()
        for pattern in patterns:
            try:
                for key in self.client.scan_iter(pattern):
                    keys.add(str(key))
            except Exception:
                continue
        return sorted(keys)

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

    def _parse_execution_check_ts(self, item: dict[str, Any]) -> datetime | None:
        return _parse_dt(item.get("ts") or item.get("created_at") or item.get("time"))

    def clean_preview(self, mode: str = "soft") -> dict[str, Any]:
        """Return a safe preview for Redis cleanup commands.

        soft: removes stale open trades and old rejected/check rows only.
        deep: deletes all Redis keys under all okx:longbot namespaces and starts a clean baseline.
        """
        stats: dict[str, Any] = {
            "enabled": bool(self.enabled and self.client),
            "mode": mode,
            "prefix": PREFIX,
            "deep_patterns": ", ".join(DEEP_CLEAN_PATTERNS),
            "open_set": 0,
            "history_set": 0,
            "trade_keys": 0,
            "execution_checks": 0,
            "stale_open_candidates": 0,
            "old_execution_checks": 0,
            "keys_to_delete": 0,
        }
        if not self.enabled or not self.client:
            return stats
        try:
            open_ids = set(self.client.smembers(OPEN_SET) or [])
            history_ids = set(self.client.smembers(HISTORY_SET) or [])
            stats["open_set"] = len(open_ids)
            stats["history_set"] = len(history_ids)
            stats["trade_keys"] = sum(1 for _ in self.client.scan_iter(f"{PREFIX}:trade:*"))
            rows = self.client.lrange(EXEC_CHECKS_LIST, 0, -1) or []
            stats["execution_checks"] = len(rows)
            now = datetime.now(timezone.utc)
            stale_cutoff = now.timestamp() - (24 * 60 * 60)
            old_check_cutoff = now.timestamp() - (7 * 24 * 60 * 60)
            stale = 0
            for trade_id in open_ids:
                raw = self.client.get(self._trade_key(trade_id))
                if not raw:
                    stale += 1
                    continue
                try:
                    trade = trade_from_dict(json.loads(raw))
                except Exception:
                    trade = None
                if not trade:
                    stale += 1
                    continue
                opened = _parse_dt(getattr(trade, "opened_at", None)) or _parse_dt(getattr(trade, "updated_at", None))
                if opened and opened.timestamp() < stale_cutoff and not getattr(trade, "protected_runner", False):
                    stale += 1
            old_checks = 0
            for raw in rows:
                try:
                    item = json.loads(raw)
                except Exception:
                    old_checks += 1
                    continue
                ts = self._parse_execution_check_ts(item)
                if ts and ts.timestamp() < old_check_cutoff:
                    old_checks += 1
            stats["stale_open_candidates"] = stale
            stats["old_execution_checks"] = old_checks
            if mode == "deep":
                stats["keys_to_delete"] = len(self._scan_keys_for_patterns(DEEP_CLEAN_PATTERNS))
        except Exception as exc:
            stats["error"] = str(exc)
        return stats

    def soft_clean(self) -> dict[str, Any]:
        """Clean stale test data without wiping the whole history.

        - removes malformed/dangling open set members
        - removes non-protected open trades older than 24h
        - trims old execution checks older than 7d
        """
        preview = self.clean_preview("soft")
        stats = dict(preview)
        stats.update({"deleted_trade_keys": 0, "removed_open_members": 0, "kept_execution_checks": 0, "removed_execution_checks": 0})
        if not self.enabled or not self.client:
            return stats
        try:
            now = datetime.now(timezone.utc)
            stale_cutoff = now.timestamp() - (24 * 60 * 60)
            old_check_cutoff = now.timestamp() - (7 * 24 * 60 * 60)
            open_ids = set(self.client.smembers(OPEN_SET) or [])
            pipe = self.client.pipeline()
            for trade_id in open_ids:
                key = self._trade_key(trade_id)
                raw = self.client.get(key)
                remove = False
                delete_key = False
                if not raw:
                    remove = True
                else:
                    try:
                        trade = trade_from_dict(json.loads(raw))
                    except Exception:
                        trade = None
                    if not trade:
                        remove = True
                        delete_key = True
                    else:
                        opened = _parse_dt(getattr(trade, "opened_at", None)) or _parse_dt(getattr(trade, "updated_at", None))
                        if opened and opened.timestamp() < stale_cutoff and not getattr(trade, "protected_runner", False):
                            remove = True
                            delete_key = True
                if remove:
                    pipe.srem(OPEN_SET, trade_id)
                    stats["removed_open_members"] += 1
                if delete_key:
                    pipe.delete(key)
                    stats["deleted_trade_keys"] += 1
            rows = self.client.lrange(EXEC_CHECKS_LIST, 0, -1) or []
            kept: list[str] = []
            removed = 0
            for raw in rows:
                try:
                    item = json.loads(raw)
                except Exception:
                    removed += 1
                    continue
                ts = self._parse_execution_check_ts(item)
                if ts and ts.timestamp() < old_check_cutoff:
                    removed += 1
                else:
                    kept.append(raw)
            pipe.delete(EXEC_CHECKS_LIST)
            if kept:
                # lrange returns newest -> oldest. Preserve the same order.
                for raw in reversed(kept):
                    pipe.lpush(EXEC_CHECKS_LIST, raw)
                pipe.ltrim(EXEC_CHECKS_LIST, 0, 9999)
                pipe.expire(EXEC_CHECKS_LIST, RETENTION_SECONDS)
            pipe.execute()
            stats["kept_execution_checks"] = len(kept)
            stats["removed_execution_checks"] = removed
        except Exception as exc:
            stats["error"] = str(exc)
        return stats

    def deep_clean(self) -> dict[str, Any]:
        """Delete all Redis keys for all OKX long-bot namespaces.

        This is intentionally wider than PREFIX because older deployments used
        different namespaces such as okx:longbot:v123/v134/v147. Use only after
        explicit confirmation.
        """
        preview = self.clean_preview("deep")
        stats = dict(preview)
        stats.update({"deleted_keys": 0})
        if not self.enabled or not self.client:
            return stats
        try:
            keys = self._scan_keys_for_patterns(DEEP_CLEAN_PATTERNS)
            if keys:
                for i in range(0, len(keys), 500):
                    self.client.delete(*keys[i:i + 500])
            stats["deleted_keys"] = len(keys)
        except Exception as exc:
            stats["error"] = str(exc)
        return stats

    def soft_restart_safe_note(self) -> str:
        return f"Redis persistence: {'ON' if self.enabled else 'OFF'} | retention {RETENTION_DAYS}d"
