"""Redis-backed persistence for trades and execution checks.

Keeps reports stable across Railway restarts/redeploys. Redis is optional at
runtime: if the library/server is unavailable, the bot continues in memory-only
mode and logs a clear warning instead of crashing.

Managed-execution compatibility:
- Preserves exchange/order metadata stored on TrackedTrade
- Preserves attached SL / TP order ids and managed trade plan payloads
- Safe with older saved trades because unknown fields are ignored on load
"""
from __future__ import annotations

import hashlib
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
SIGNAL_FP_PREFIX = f"{PREFIX}:signals:fp"
SIGNAL_FP_TTL_SECONDS = 6 * 60 * 60


def _to_json_safe(value: Any) -> Any:
    """Recursively convert values into JSON-safe structures.

    Important for managed execution payloads because they may contain nested
    dict/list structures that include datetimes.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_json_safe(v) for v in value]
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
    return {key: _to_json_safe(value) for key, value in data.items()}


def trade_from_dict(data: dict[str, Any]) -> TrackedTrade | None:
    try:
        allowed = set(TrackedTrade.__dataclass_fields__.keys())
        clean = {k: v for k, v in dict(data).items() if k in allowed}

        datetime_fields = (
            "opened_at",
            "updated_at",
            "closed_at",
            "last_exchange_sync_at",
            "last_sl_amend_at",
            "tp1_hit_at",
            "tp2_hit_at",
            "sl_move_to_entry_at",
            "sl_move_to_tp1_at",
            "trailing_started_at",
            "trailing_tightened_at",
        )
        nullable_datetime_fields = {
            "closed_at",
            "last_exchange_sync_at",
            "last_sl_amend_at",
            "tp1_hit_at",
            "tp2_hit_at",
            "sl_move_to_entry_at",
            "sl_move_to_tp1_at",
            "trailing_started_at",
            "trailing_tightened_at",
        }
        for dt_key in datetime_fields:
            if dt_key in clean:
                parsed = _parse_dt(clean.get(dt_key))
                if parsed is not None:
                    clean[dt_key] = parsed
                elif dt_key in nullable_datetime_fields:
                    clean[dt_key] = None

        list_fields = (
            "execution_setup_tags",
            "warnings",
            "last_3_candles",
        )
        for key in list_fields:
            if key in clean and not isinstance(clean[key], list):
                clean[key] = []

        dict_fields = (
            "entry_order_payload",
            "managed_trade_plan",
        )
        for key in dict_fields:
            if key in clean and clean[key] is not None and not isinstance(clean[key], dict):
                clean[key] = {}

        # sl_attached_payload is a list[dict], not a dict — handle separately
        if "sl_attached_payload" in clean and not isinstance(clean["sl_attached_payload"], list):
            clean["sl_attached_payload"] = []

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

    def _current_namespace_keys(self) -> list[str]:
        """Collect all keys that belong to the active namespace.

        Includes exact well-known keys so deep_clean still works even when scan
        results are partial or delayed.
        """
        if not self.enabled or not self.client:
            return []
        keys = set(self._scan_keys_for_patterns([
            f"{PREFIX}:*",
            f"{SIGNAL_FP_PREFIX}:*",
        ]))
        keys.update({OPEN_SET, HISTORY_SET, EXEC_CHECKS_LIST})
        return sorted(k for k in keys if k)

    def _deep_clean_keys(self) -> list[str]:
        """Collect every long-bot key we know how to wipe."""
        if not self.enabled or not self.client:
            return []
        keys = set(self._scan_keys_for_patterns(DEEP_CLEAN_PATTERNS))
        keys.update(self._current_namespace_keys())
        return sorted(k for k in keys if k)

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
                    pipe.set(key, payload, ex=RETENTION_SECONDS)
                    pipe.srem(OPEN_SET, trade.trade_id)
                    pipe.sadd(HISTORY_SET, trade.trade_id)
                    pipe.expire(HISTORY_SET, RETENTION_SECONDS)
                else:
                    # Open trades should survive redeploy/restart. Keep a long TTL as a safety net.
                    pipe.set(key, payload, ex=RETENTION_SECONDS * 3)
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
                try:
                    encoded = json.dumps(_to_json_safe(payload), ensure_ascii=False, default=str)
                except Exception:
                    encoded = json.dumps(
                        {
                            "status": payload.get("status"),
                            "reason": payload.get("reason"),
                            "path": payload.get("path"),
                            "ts": now,
                        },
                        ensure_ascii=False,
                    )
                pipe.lpush(EXEC_CHECKS_LIST, encoded)
            pipe.ltrim(EXEC_CHECKS_LIST, 0, max(0, limit - 1))
            pipe.expire(EXEC_CHECKS_LIST, RETENTION_SECONDS)
            pipe.execute()
        except Exception as exc:
            print(f"⚠️ Redis append_execution_checks failed: {exc}", flush=True)

    def load_execution_checks(self, limit: int | None = None) -> list[dict[str, Any]]:
        if not self.enabled or not self.client:
            return []
        try:
            end = -1 if limit is None or limit <= 0 else max(0, int(limit) - 1)
            rows = self.client.lrange(EXEC_CHECKS_LIST, 0, end) or []
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

    def delete_trade_records(self, trade_ids: list[str] | set[str] | tuple[str, ...]) -> int:
        """Delete specific live trade records from Redis without touching simulation keys.

        Used by scoped report reset commands. This only touches the current live
        namespace trade keys and their open/history set memberships. Simulation
        has its own namespace and is intentionally untouched here.
        """
        if not self.enabled or not self.client or not trade_ids:
            return 0
        ids = [str(tid) for tid in trade_ids if str(tid or "").strip()]
        if not ids:
            return 0
        try:
            pipe = self.client.pipeline()
            for trade_id in ids:
                pipe.delete(self._trade_key(trade_id))
                pipe.srem(OPEN_SET, trade_id)
                pipe.srem(HISTORY_SET, trade_id)
            result = pipe.execute() or []
            deleted = 0
            for idx in range(0, len(result), 3):
                try:
                    deleted += int(result[idx] or 0)
                except Exception:
                    continue
            return deleted
        except Exception as exc:
            print(f"⚠️ Redis delete_trade_records failed: {exc}", flush=True)
            return 0

    def clear_execution_checks(self) -> int:
        """Clear live execution check history only.

        Simulation execution checks are stored under SIMULATION_REDIS_PREFIX in
        main.py, so this does not affect simulation reports.
        """
        if not self.enabled or not self.client:
            return 0
        try:
            return int(self.client.delete(EXEC_CHECKS_LIST) or 0)
        except Exception as exc:
            print(f"⚠️ Redis clear_execution_checks failed: {exc}", flush=True)
            return 0

    def _parse_execution_check_ts(self, item: dict[str, Any]) -> datetime | None:
        return _parse_dt(item.get("ts") or item.get("created_at") or item.get("time"))

    def _signal_fingerprint_key(self, fingerprint: str) -> str:
        digest = hashlib.sha1(str(fingerprint or "").encode("utf-8")).hexdigest()
        return f"{SIGNAL_FP_PREFIX}:{digest}"

    def mark_signal_fingerprint(self, fingerprint: str, ttl_seconds: int = SIGNAL_FP_TTL_SECONDS) -> bool:
        """Return True when fingerprint was already sent recently; otherwise mark it.

        This persists duplicate protection across restarts but expires automatically,
        so a real fresh setup is not blocked forever.
        """
        if not fingerprint:
            return False
        if not self.enabled or not self.client:
            return False
        try:
            key = self._signal_fingerprint_key(fingerprint)
            created = self.client.set(key, "1", nx=True, ex=max(60, int(ttl_seconds)))
            return not bool(created)
        except Exception as exc:
            print(f"⚠️ Redis mark_signal_fingerprint failed: {exc}", flush=True)
            return False

    def health_snapshot(self) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "enabled": bool(self.enabled and self.client),
            "prefix": PREFIX,
            "open_set": 0,
            "history_set": 0,
            "trade_keys": 0,
            "execution_checks": 0,
            "signal_fingerprints": 0,
        }
        if not self.enabled or not self.client:
            return stats
        try:
            stats["open_set"] = len(self.client.smembers(OPEN_SET) or [])
            stats["history_set"] = len(self.client.smembers(HISTORY_SET) or [])
            stats["trade_keys"] = sum(1 for _ in self.client.scan_iter(f"{PREFIX}:trade:*"))
            stats["execution_checks"] = int(self.client.llen(EXEC_CHECKS_LIST) or 0)
            stats["signal_fingerprints"] = sum(1 for _ in self.client.scan_iter(f"{SIGNAL_FP_PREFIX}:*"))
        except Exception as exc:
            stats["error"] = str(exc)
        return stats

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
            "signal_fingerprints": 0,
            "stale_open_candidates": 0,
            "old_execution_checks": 0,
            "keys_to_delete": 0,
            "current_namespace_keys": 0,
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
            stats["signal_fingerprints"] = sum(1 for _ in self.client.scan_iter(f"{SIGNAL_FP_PREFIX}:*"))
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
                last_seen = _parse_dt(getattr(trade, "updated_at", None)) or _parse_dt(getattr(trade, "opened_at", None))
                if last_seen and last_seen.timestamp() < stale_cutoff and not getattr(trade, "protected_runner", False):
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
                current_keys = self._current_namespace_keys()
                deep_keys = self._deep_clean_keys()
                stats["current_namespace_keys"] = len(current_keys)
                stats["keys_to_delete"] = len(deep_keys)
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
                        last_seen = _parse_dt(getattr(trade, "updated_at", None)) or _parse_dt(getattr(trade, "opened_at", None))
                        if last_seen and last_seen.timestamp() < stale_cutoff and not getattr(trade, "protected_runner", False):
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
        stats.update({
            "deleted_keys": 0,
            "delete_attempted": False,
            "remaining_keys": 0,
        })
        if not self.enabled or not self.client:
            return stats
        try:
            keys = self._deep_clean_keys()
            stats["delete_attempted"] = True

            deleted = 0
            if keys:
                for i in range(0, len(keys), 500):
                    deleted += int(self.client.delete(*keys[i:i + 500]) or 0)

            deleted += int(self.client.delete(OPEN_SET, HISTORY_SET, EXEC_CHECKS_LIST) or 0)

            stats["deleted_keys"] = deleted
            stats["remaining_keys"] = len(self._deep_clean_keys())
        except Exception as exc:
            stats["error"] = str(exc)
        return stats

    def soft_restart_safe_note(self) -> str:
        return f"Redis persistence: {'ON' if self.enabled else 'OFF'} | retention {RETENTION_DAYS}d"
