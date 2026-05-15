"""Technical dataset capture for AI/gate research.

This module is intentionally passive:
- it records normal signals and execution-candidate decisions,
- it does not change scoring, filters, TP/SL, market modes, or execution behavior,
- it keeps legacy gates as observed metadata for later comparison.
"""
from __future__ import annotations

import json
import os
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SNAPSHOT_REDIS_KEY = "okx_long_bot:technical_snapshot:enabled"
SNAPSHOT_REDIS_RECORDS_KEY = "okx_long_bot:technical_snapshot:records"
DEFAULT_REDIS_RECORD_LIMIT = 50000


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _data_path(value: str | None, default: str) -> Path:
    return Path(value or default)


def snapshot_path(settings: Any | None = None) -> Path:
    return _data_path(getattr(settings, "snapshot_output_path", None), os.getenv("SNAPSHOT_OUTPUT_PATH", "data/technical_snapshots.jsonl"))


def flag_path(settings: Any | None = None) -> Path:
    return _data_path(getattr(settings, "technical_snapshot_flag_path", None), os.getenv("TECHNICAL_SNAPSHOT_FLAG_PATH", "data/technical_snapshot.flag"))


def redis_record_limit() -> int:
    try:
        return max(1000, int(os.getenv("SNAPSHOT_REDIS_MAX_RECORDS", str(DEFAULT_REDIS_RECORD_LIMIT))))
    except Exception:
        return DEFAULT_REDIS_RECORD_LIMIT


def _decode_redis_value(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return str(value)
    return str(value)


def _redis_get_snapshot_flag(redis_client: Any | None) -> bool | None:
    if not redis_client:
        return None
    try:
        value = redis_client.get(SNAPSHOT_REDIS_KEY)
        if value is None:
            return None
        value = _decode_redis_value(value).strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False
    except Exception:
        return None
    return None


def _redis_set_snapshot_flag(redis_client: Any | None, enabled: bool) -> bool:
    if not redis_client:
        return False
    try:
        redis_client.set(SNAPSHOT_REDIS_KEY, "on" if enabled else "off")
        return True
    except Exception:
        return False


def is_snapshot_enabled(settings: Any | None = None, redis_client: Any | None = None) -> bool:
    """Redis runtime flag overrides local flag/env when available.

    Redis keeps ON/OFF persistent across Railway restarts/redeploys.
    Local flag remains as a fallback for local runs or Redis outages.
    """
    redis_value = _redis_get_snapshot_flag(redis_client)
    if redis_value is not None:
        return redis_value

    flag = flag_path(settings)
    try:
        if flag.exists():
            value = flag.read_text(encoding="utf-8").strip().lower()
            if value in {"1", "true", "yes", "on"}:
                return True
            if value in {"0", "false", "no", "off"}:
                return False
    except Exception:
        pass
    return bool(getattr(settings, "technical_snapshot_enabled", _env_bool("TECHNICAL_SNAPSHOT_ENABLED", "0")))


def set_snapshot_enabled(enabled: bool, settings: Any | None = None, redis_client: Any | None = None) -> dict:
    redis_ok = _redis_set_snapshot_flag(redis_client, enabled)

    flag = flag_path(settings)
    file_ok = False
    file_error = ""
    try:
        flag.parent.mkdir(parents=True, exist_ok=True)
        flag.write_text("on" if enabled else "off", encoding="utf-8")
        file_ok = True
    except Exception as exc:
        file_error = str(exc)

    if redis_ok or file_ok:
        return {
            "ok": True,
            "enabled": enabled,
            "source": "redis" if redis_ok else "file",
            "redis_key": SNAPSHOT_REDIS_KEY if redis_ok else "",
            "flag_path": str(flag),
        }

    return {
        "ok": False,
        "enabled": is_snapshot_enabled(settings, redis_client=redis_client),
        "error": file_error or "Redis and local flag file are unavailable",
        "redis_key": SNAPSHOT_REDIS_KEY,
        "flag_path": str(flag),
    }


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_clean(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _signal_id(scan_id: str, signal: Any) -> str:
    base = f"{scan_id}|{getattr(signal, 'symbol', '')}|{getattr(signal, 'market_mode', '')}|{getattr(signal, 'entry', '')}|{getattr(signal, 'score', '')}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


def build_signal_snapshot(scan_id: str, signal: Any, execution: dict | None, market_context: dict | None = None) -> dict:
    meta = getattr(signal, "meta", {}) or {}
    execution = execution or {}
    gate = execution.get("gate") or {}
    status = str(execution.get("status") or "")
    reason = str(execution.get("reason") or gate.get("reason") or "")
    quality_candidate = bool(gate.get("allowed") or status in {"accepted_preview", "pending_pullback_preview", "rejected_limit"})
    execution_candidate = status in {"accepted_preview", "pending_pullback_preview"}
    blocked_by_limit = status == "rejected_limit" or reason in {"max_positions_reached", "recovery_cycle_full"}
    signal_level = "candidate" if quality_candidate else "normal"

    entry = _as_float(getattr(signal, "entry", 0.0))
    sl = _as_float(getattr(signal, "sl", 0.0))
    tp1 = _as_float(getattr(signal, "tp1", 0.0))
    tp2 = _as_float(getattr(signal, "tp2", 0.0))
    risk = max(entry - sl, 0.0)

    record = {
        "event": "live_signal_snapshot",
        "schema_version": 2,
        "captured_at": _utc_now(),
        "scan_id": scan_id,
        "signal_id": _signal_id(scan_id, signal),
        "symbol": getattr(signal, "symbol", ""),
        "mode": getattr(signal, "market_mode", ""),
        "signal_level": signal_level,
        "normal_signal": True,
        "quality_candidate": quality_candidate,
        "execution_candidate": execution_candidate,
        "blocked_by_limit": blocked_by_limit,
        "block_reason": reason if blocked_by_limit else "",
        "execution_status": status,
        "execution_path": execution.get("path") or gate.get("path") or "",
        "legacy_gate_passed": bool(gate.get("allowed", False)),
        "legacy_gate_reason": gate.get("reason") or reason,
        "legacy_gate": _clean(gate),
        "trade_plan": {
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp1_pct": ((tp1 / entry) - 1.0) * 100.0 if entry else 0.0,
            "tp2_pct": ((tp2 / entry) - 1.0) * 100.0 if entry else 0.0,
            "sl_pct": ((sl / entry) - 1.0) * 100.0 if entry else 0.0,
            "rr1": ((tp1 - entry) / risk) if risk else 0.0,
            "rr2": ((tp2 - entry) / risk) if risk else 0.0,
        },
        "features": {
            "score": _as_float(getattr(signal, "score", 0.0)),
            "effective_score": _as_float(meta.get("effective_score"), _as_float(getattr(signal, "score", 0.0))),
            "raw_score": _as_float(meta.get("raw_score"), _as_float(getattr(signal, "score", 0.0))),
            "setup_type": getattr(signal, "setup_type", ""),
            "entry_timing": getattr(signal, "entry_timing", ""),
            "setup_tags": list(getattr(signal, "execution_setup_tags", []) or []),
            "pair_tags": list(meta.get("pair_tags", []) or []),
            "turnover_usdt": _as_float(meta.get("turnover_usdt")),
            "change_pct": _as_float(meta.get("change_pct")),
            "vol_ratio": _as_float(meta.get("vol_ratio"), 1.0),
            "mtf_confirmed": bool(meta.get("mtf_confirmed")),
            "dist_ma": _as_float(meta.get("dist_ma")),
            "breakout": bool(meta.get("breakout")),
            "pre_breakout": bool(meta.get("pre_breakout")),
            "breakout_quality": meta.get("breakout_quality") or "",
            "setup_weight": _as_float(meta.get("setup_weight")),
            "resistance_warning": meta.get("resistance_warning") or meta.get("rejection_context") or "",
            "btc_bounce_pct": _as_float(meta.get("btc_bounce_pct")),
            "symbol_bounce_pct": _as_float(meta.get("symbol_bounce_pct")),
            "bounce_ratio_vs_btc": _as_float(meta.get("bounce_ratio_vs_btc")),
            "recovery_relative_bounce": bool(meta.get("recovery_relative_bounce")),
            "warnings": list(getattr(signal, "warnings", []) or []),
        },
        "market_context": _clean(market_context or {}),
        "outcome": None,
    }
    return _clean(record)


def _append_records_to_local_jsonl(records: list[dict], settings: Any | None = None) -> dict:
    path = snapshot_path(settings)
    if not records:
        return {"ok": True, "path": str(path), "written": 0}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        return {"ok": True, "path": str(path), "written": len(records)}
    except Exception as exc:
        return {"ok": False, "path": str(path), "written": 0, "error": str(exc)}


def _append_records_to_redis(records: list[dict], redis_client: Any | None = None) -> dict:
    if not redis_client:
        return {"ok": False, "written": 0, "error": "Redis unavailable"}
    if not records:
        return {"ok": True, "written": 0, "redis_key": SNAPSHOT_REDIS_RECORDS_KEY}
    try:
        limit = redis_record_limit()
        encoded = [json.dumps(record, ensure_ascii=False, sort_keys=True, default=str) for record in records]
        pipe = redis_client.pipeline()
        # LPUSH keeps newest records first. load_snapshot_records reverses them back to chronological order.
        pipe.lpush(SNAPSHOT_REDIS_RECORDS_KEY, *encoded)
        pipe.ltrim(SNAPSHOT_REDIS_RECORDS_KEY, 0, max(0, limit - 1))
        pipe.execute()
        return {"ok": True, "written": len(records), "redis_key": SNAPSHOT_REDIS_RECORDS_KEY, "limit": limit}
    except Exception as exc:
        return {"ok": False, "written": 0, "redis_key": SNAPSHOT_REDIS_RECORDS_KEY, "error": str(exc)}


def append_signal_snapshot(record: dict, settings: Any | None = None, redis_client: Any | None = None) -> dict:
    return append_many_signal_snapshots([record], settings=settings, redis_client=redis_client)


def append_many_signal_snapshots(records: list[dict], settings: Any | None = None, redis_client: Any | None = None) -> dict:
    """Persist snapshots to Redis first, with local JSONL as a mirror/fallback.

    Redis is the durable store for Railway restart/redeploy safety.
    The local JSONL file remains useful for local development and quick inspection,
    but it should not be treated as the source of truth on ephemeral hosts.
    """
    local_result = _append_records_to_local_jsonl(records, settings=settings)
    redis_result = _append_records_to_redis(records, redis_client=redis_client)

    if redis_result.get("ok"):
        return {
            "ok": True,
            "store": "redis",
            "written": redis_result.get("written", 0),
            "redis_key": SNAPSHOT_REDIS_RECORDS_KEY,
            "local_mirror": local_result,
        }
    if local_result.get("ok"):
        return {
            "ok": True,
            "store": "local_file_fallback",
            "written": local_result.get("written", 0),
            "path": local_result.get("path"),
            "redis_error": redis_result.get("error"),
        }
    return {
        "ok": False,
        "written": 0,
        "redis_error": redis_result.get("error"),
        "file_error": local_result.get("error"),
        "path": local_result.get("path"),
        "redis_key": SNAPSHOT_REDIS_RECORDS_KEY,
    }


def _load_records_from_redis(redis_client: Any | None = None, limit: int = 5000) -> list[dict]:
    if not redis_client:
        return []
    try:
        end = max(0, int(limit) - 1)
        rows = redis_client.lrange(SNAPSHOT_REDIS_RECORDS_KEY, 0, end) or []
        records: list[dict] = []
        for raw in reversed(rows):
            try:
                records.append(json.loads(_decode_redis_value(raw)))
            except Exception:
                continue
        return records
    except Exception:
        return []


def _load_records_from_local_jsonl(settings: Any | None = None, limit: int = 5000) -> list[dict]:
    path = snapshot_path(settings)
    if not path.exists():
        return []
    records: list[dict] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in lines[-max(limit, 1):]:
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                continue
    except Exception:
        return []
    return records


def load_snapshot_records(settings: Any | None = None, limit: int = 5000, redis_client: Any | None = None) -> list[dict]:
    records = _load_records_from_redis(redis_client=redis_client, limit=limit)
    if records:
        return records
    return _load_records_from_local_jsonl(settings=settings, limit=limit)


def snapshot_storage_status(settings: Any | None = None, redis_client: Any | None = None) -> dict:
    path = snapshot_path(settings)
    local_size_kb = round(path.stat().st_size / 1024, 1) if path.exists() else 0.0
    redis_available = bool(redis_client)
    redis_count = 0
    redis_error = ""
    if redis_client:
        try:
            redis_count = int(redis_client.llen(SNAPSHOT_REDIS_RECORDS_KEY) or 0)
        except Exception as exc:
            redis_available = False
            redis_error = str(exc)
    return {
        "redis_available": redis_available,
        "redis_key": SNAPSHOT_REDIS_RECORDS_KEY,
        "redis_count": redis_count,
        "redis_limit": redis_record_limit(),
        "redis_error": redis_error,
        "local_path": str(path),
        "local_size_kb": local_size_kb,
        "state_store": "Redis" if _redis_get_snapshot_flag(redis_client) is not None else "file/env fallback",
        "data_store": "Redis" if redis_available else "local file fallback",
    }


def build_technical_dataset_status(settings: Any | None = None, redis_client: Any | None = None) -> str:
    records = load_snapshot_records(settings, limit=20000, redis_client=redis_client)
    storage = snapshot_storage_status(settings, redis_client=redis_client)
    by_level = Counter(str(r.get("signal_level") or "unknown") for r in records)
    by_mode = Counter(str(r.get("mode") or "unknown") for r in records)
    candidates = sum(1 for r in records if r.get("quality_candidate"))
    executions = sum(1 for r in records if r.get("execution_candidate"))
    blocked = sum(1 for r in records if r.get("blocked_by_limit"))
    lines = [
        "🧠 Technical Dataset Status",
        "┄┄┄┄┄┄┄┄",
        f"Capture: {'ON' if is_snapshot_enabled(settings, redis_client=redis_client) else 'OFF'}",
        f"State Store: {storage.get('state_store')}",
        f"Data Store: {storage.get('data_store')}",
        f"Redis Records: {storage.get('redis_count')} / {storage.get('redis_limit')}",
        f"Local Mirror: {storage.get('local_path')}",
        f"Local Size: {storage.get('local_size_kb')} KB",
        f"Records loaded: {len(records)}",
        "",
        f"Normal: {by_level.get('normal', 0)}",
        f"Quality Candidates: {candidates}",
        f"Execution Candidates: {executions}",
        f"Blocked by limits: {blocked}",
        "",
        "By Mode:",
    ]
    if by_mode:
        lines.extend([f"- {mode}: {count}" for mode, count in by_mode.most_common(8)])
    else:
        lines.append("- no data yet")
    if storage.get("redis_error"):
        lines += ["", f"⚠️ Redis error: {storage.get('redis_error')}"]
    return "\n".join(lines)


def build_technical_dataset_export(settings: Any | None = None, redis_client: Any | None = None) -> str:
    storage = snapshot_storage_status(settings, redis_client=redis_client)
    records = load_snapshot_records(settings, limit=5, redis_client=redis_client)
    last = records[-1] if records else {}
    last_line = ""
    if last:
        last_line = f"Last: {last.get('symbol', '')} | {last.get('mode', '')} | {last.get('signal_level', '')}"
    return "\n".join([
        "📦 Technical Dataset Export",
        "┄┄┄┄┄┄┄┄",
        f"Primary AI store: {storage.get('data_store')}",
        f"Redis key: {storage.get('redis_key')}",
        f"Redis records: {storage.get('redis_count')} / {storage.get('redis_limit')}",
        f"Local mirror file: {snapshot_path(settings)}",
        f"Capture state: {storage.get('state_store')}",
        "Format: JSONL-compatible records — one signal snapshot per record.",
        "Redis is the source of truth on Railway; the local file is only a mirror/fallback.",
        last_line,
    ]).strip()


def clear_snapshot_records(settings: Any | None = None, redis_client: Any | None = None, clear_local: bool = True) -> dict:
    """Clear only Technical Snapshot AI data.

    This is intentionally separate from Admin/Hard Reset. It does not touch
    trades, execution checks, signal fingerprints, cooldowns, or bot state.
    The ON/OFF capture flag is preserved.
    """
    result = {
        "ok": False,
        "redis_deleted": False,
        "local_deleted": False,
        "redis_key": SNAPSHOT_REDIS_RECORDS_KEY,
        "local_path": str(snapshot_path(settings)),
        "error": "",
    }

    redis_ok = False
    if redis_client:
        try:
            redis_client.delete(SNAPSHOT_REDIS_RECORDS_KEY)
            redis_ok = True
            result["redis_deleted"] = True
        except Exception as exc:
            result["error"] = str(exc)

    local_ok = False
    if clear_local:
        try:
            path = snapshot_path(settings)
            if path.exists():
                path.unlink()
            local_ok = True
            result["local_deleted"] = True
        except Exception as exc:
            if not result["error"]:
                result["error"] = str(exc)
    else:
        local_ok = True

    result["ok"] = bool(redis_ok or local_ok)
    if result["ok"] and not result["error"]:
        result["error"] = ""
    return result


def build_clear_snapshot_result(settings: Any | None = None, redis_client: Any | None = None) -> str:
    result = clear_snapshot_records(settings=settings, redis_client=redis_client, clear_local=True)
    if not result.get("ok"):
        return "⚠️ لم أستطع مسح Technical Snapshot data: " + str(result.get("error") or "unknown error")
    return "\n".join([
        "🧹 Technical Snapshot Data Cleared",
        "┄┄┄┄┄┄┄┄",
        "✅ تم مسح داتا الـ AI snapshots فقط.",
        "✅ لم يتم لمس الصفقات أو tracking أو execution state.",
        f"Redis key: {result.get('redis_key')}",
        f"Local mirror: {result.get('local_path')}",
        "Capture ON/OFF لم يتغير.",
    ])


def build_gate_suggestions_report(settings: Any | None = None, redis_client: Any | None = None) -> str:
    records = load_snapshot_records(settings, limit=20000, redis_client=redis_client)
    if not records:
        return "🧠 Gate Suggestions\n┄┄┄┄┄┄┄┄\nلا توجد بيانات كافية حتى الآن."
    grouped: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        grouped[str(record.get("mode") or "unknown")].append(record)
    lines = ["🧠 Gate Suggestions — Passive", "┄┄┄┄┄┄┄┄", "هذه قراءة مبدئية من الداتا الخام، وليست قواعد تنفيذ.", ""]
    for mode, items in grouped.items():
        cand = [r for r in items if r.get("quality_candidate")]
        blocked = [r for r in items if r.get("blocked_by_limit")]
        avg_score = sum(float((r.get("features") or {}).get("effective_score") or 0) for r in items) / max(len(items), 1)
        avg_vol = sum(float((r.get("features") or {}).get("vol_ratio") or 0) for r in items) / max(len(items), 1)
        lines += [
            f"{mode}",
            f"- signals={len(items)} | quality_candidates={len(cand)} | blocked_limits={len(blocked)}",
            f"- avg_effective_score={avg_score:.2f} | avg_vol_ratio={avg_vol:.2f}",
        ]
    lines += ["", "📌 الحكم الحقيقي يحتاج outcome تاريخي/لاحق قبل اعتماد أي Gate."]
    return "\n".join(lines)
