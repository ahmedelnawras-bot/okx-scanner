from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPLAY_PREFIX = "replay:"
STATUS_KEY = f"{REPLAY_PREFIX}status"
STOP_KEY = f"{REPLAY_PREFIX}stop_requested"
DATA_KEY = f"{REPLAY_PREFIX}snapshots"
LOG_KEY = f"{REPLAY_PREFIX}log"
MAX_LOG_ITEMS = 200
LOCAL_STATUS_PATH = Path(os.getenv("REPLAY_STATUS_PATH", "data/replay_status.json"))
LOCAL_DATA_PATH = Path(os.getenv("REPLAY_OUTPUT_PATH", "data/replay_signals_dataset.jsonl"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json_default(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        return "<unserializable>"


def _encode(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=_json_default, separators=(",", ":"))


def _decode(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        value = json.loads(str(raw))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def default_status() -> dict[str, Any]:
    return {
        "running": False,
        "state": "idle",
        "run_id": "",
        "days": 30,
        "symbols_limit": 200,
        "timeframe": "15m",
        "expected_candles_per_symbol": 2880,
        "min_required_candles": 1872,
        "current_symbol": "",
        "current_symbol_candles": 0,
        "started_at": "",
        "updated_at": utc_now_iso(),
        "completed_at": "",
        "progress_pct": 0.0,
        "symbols_total": 0,
        "symbols_done": 0,
        "records": 0,
        "normal": 0,
        "quality_candidates": 0,
        "execution_candidates": 0,
        "blocked_by_limits": 0,
        "message": "Historical replay is idle.",
        "data_store": "Redis" if False else "file/env fallback",
        "output_path": str(LOCAL_DATA_PATH),
    }


def _redis_available(redis_client: Any | None) -> bool:
    return redis_client is not None


def get_status(redis_client: Any | None = None) -> dict[str, Any]:
    if _redis_available(redis_client):
        data = _decode(redis_client.get(STATUS_KEY))
        if data:
            data.setdefault("data_store", "Redis")
            data.setdefault("output_path", str(LOCAL_DATA_PATH))
            return data
    if LOCAL_STATUS_PATH.exists():
        try:
            data = json.loads(LOCAL_STATUS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("data_store", "file/env fallback")
                data.setdefault("output_path", str(LOCAL_DATA_PATH))
                return data
        except Exception:
            pass
    status = default_status()
    status["data_store"] = "Redis" if _redis_available(redis_client) else "file/env fallback"
    return status


def set_status(status: dict[str, Any], redis_client: Any | None = None) -> dict[str, Any]:
    status = dict(status or {})
    status["updated_at"] = utc_now_iso()
    status.setdefault("output_path", str(LOCAL_DATA_PATH))
    if _redis_available(redis_client):
        status["data_store"] = "Redis"
        redis_client.set(STATUS_KEY, _encode(status))
    else:
        status["data_store"] = "file/env fallback"
        LOCAL_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOCAL_STATUS_PATH.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    return status


def request_stop(redis_client: Any | None = None) -> dict[str, Any]:
    status = get_status(redis_client)
    if _redis_available(redis_client):
        redis_client.set(STOP_KEY, "1")
    status.update({"state": "stopping", "running": bool(status.get("running")), "message": "Stop requested. The replay worker will stop at the next safe checkpoint."})
    return set_status(status, redis_client)


def stop_requested(redis_client: Any | None = None) -> bool:
    if _redis_available(redis_client):
        raw = redis_client.get(STOP_KEY)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return str(raw or "").strip() in {"1", "true", "yes", "on"}
    return False


def clear_stop(redis_client: Any | None = None) -> None:
    if _redis_available(redis_client):
        redis_client.delete(STOP_KEY)


def append_log(message: str, redis_client: Any | None = None) -> None:
    payload = _encode({"ts": utc_now_iso(), "message": str(message)})
    if _redis_available(redis_client):
        redis_client.lpush(LOG_KEY, payload)
        redis_client.ltrim(LOG_KEY, 0, MAX_LOG_ITEMS - 1)


def get_log(redis_client: Any | None = None, limit: int = 10) -> list[dict[str, Any]]:
    if not _redis_available(redis_client):
        return []
    rows = redis_client.lrange(LOG_KEY, 0, max(0, int(limit) - 1)) or []
    return [_decode(row) for row in rows]


def clear_replay(redis_client: Any | None = None, clear_local: bool = True) -> dict[str, Any]:
    deleted = 0
    if _redis_available(redis_client):
        for key in (STATUS_KEY, STOP_KEY, DATA_KEY, LOG_KEY):
            try:
                deleted += int(redis_client.delete(key) or 0)
            except Exception:
                pass
    local_deleted = []
    if clear_local:
        for path in (LOCAL_STATUS_PATH, LOCAL_DATA_PATH):
            try:
                if path.exists():
                    path.unlink()
                    local_deleted.append(str(path))
            except Exception:
                pass
    return {"ok": True, "redis_keys_deleted": deleted, "local_deleted": local_deleted}


def data_count(redis_client: Any | None = None) -> int:
    if _redis_available(redis_client):
        try:
            return int(redis_client.llen(DATA_KEY) or 0)
        except Exception:
            return 0
    if LOCAL_DATA_PATH.exists():
        try:
            return sum(1 for _ in LOCAL_DATA_PATH.open("r", encoding="utf-8"))
        except Exception:
            return 0
    return 0
