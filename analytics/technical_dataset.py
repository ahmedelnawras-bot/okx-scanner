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


def is_snapshot_enabled(settings: Any | None = None) -> bool:
    """Runtime flag overrides env when present; env remains the default."""
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


def set_snapshot_enabled(enabled: bool, settings: Any | None = None) -> dict:
    flag = flag_path(settings)
    try:
        flag.parent.mkdir(parents=True, exist_ok=True)
        flag.write_text("on" if enabled else "off", encoding="utf-8")
        return {"ok": True, "enabled": enabled, "flag_path": str(flag)}
    except Exception as exc:
        return {"ok": False, "enabled": is_snapshot_enabled(settings), "error": str(exc), "flag_path": str(flag)}


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
        "schema_version": 1,
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


def append_signal_snapshot(record: dict, settings: Any | None = None) -> dict:
    path = snapshot_path(settings)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        return {"ok": True, "path": str(path)}
    except Exception as exc:
        return {"ok": False, "path": str(path), "error": str(exc)}


def append_many_signal_snapshots(records: list[dict], settings: Any | None = None) -> dict:
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


def load_snapshot_records(settings: Any | None = None, limit: int = 5000) -> list[dict]:
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


def build_technical_dataset_status(settings: Any | None = None) -> str:
    records = load_snapshot_records(settings, limit=20000)
    by_level = Counter(str(r.get("signal_level") or "unknown") for r in records)
    by_mode = Counter(str(r.get("mode") or "unknown") for r in records)
    candidates = sum(1 for r in records if r.get("quality_candidate"))
    executions = sum(1 for r in records if r.get("execution_candidate"))
    blocked = sum(1 for r in records if r.get("blocked_by_limit"))
    path = snapshot_path(settings)
    size_kb = round(path.stat().st_size / 1024, 1) if path.exists() else 0.0
    lines = [
        "🧠 Technical Dataset Status",
        "┄┄┄┄┄┄┄┄",
        f"Capture: {'ON' if is_snapshot_enabled(settings) else 'OFF'}",
        f"File: {path}",
        f"Size: {size_kb} KB",
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
    return "\n".join(lines)


def build_technical_dataset_export(settings: Any | None = None) -> str:
    return "\n".join([
        "📦 Technical Dataset Export",
        "┄┄┄┄┄┄┄┄",
        f"AI raw file: {snapshot_path(settings)}",
        "Format: JSONL — one signal snapshot per line.",
        "Use this file for AI analysis / future mode-gate design.",
    ])


def build_gate_suggestions_report(settings: Any | None = None) -> str:
    records = load_snapshot_records(settings, limit=20000)
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
