"""AI Research Data Export Layer — v1.0

Zero changes to trading logic, scoring, filters, or market modes.
Adds structured JSONL/JSON export for AI analysis pipelines.

Output structure:
  data/ai_reports/
    simulation/
      YYYY-MM-DD_trades.jsonl         ← صفقات المحاكاة
      YYYY-MM-DD_rejections.jsonl     ← المرفوضات
    execution/
      YYYY-MM-DD_trades.jsonl         ← صفقات التنفيذ
      YYYY-MM-DD_rejections.jsonl     ← المرفوضات
    daily_snapshots/
      YYYY-MM-DD_simulation.json      ← snapshot يومي كامل
      YYYY-MM-DD_execution.json       ← snapshot يومي كامل

Format:
  JSONL → trades & rejections  (streaming, appendable)
  JSON  → daily snapshots      (complete picture, overwrite)

Consumer-ready for: GPT, Claude, Gemini, Local LLM, Python Analytics
"""
from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

# قوائم الحالات الموحّدة — نفس المصدر اللي يستخدمه report_simulation عشان
# يبقى عدّ الرابحين/الخاسرين متطابق 100% بين تقرير المحاكاة وملفات JSON.
try:
    from reporting.report_format import WIN_STATUSES as _WIN_STATUSES, LOSS_STATUSES as _LOSS_STATUSES
except Exception:
    _WIN_STATUSES = {"tp1_partial", "tp2_partial", "runner", "trailing_hit", "closed_win", "breakeven_after_tp1"}
    _LOSS_STATUSES = {"closed_loss"}


# =========================================================
# Config
# =========================================================

AI_REPORTS_BASE_DIR = Path(os.environ.get("AI_REPORTS_DIR", "./data/ai_reports"))
EXPORT_VERSION = "1.0"
MAX_JSONL_ROWS_PER_FILE = 50_000   # safety cap per day file


# =========================================================
# Helpers
# =========================================================

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _today_str(now: datetime | None = None) -> str:
    return (now or _now_utc()).strftime("%Y-%m-%d")


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    try:
        return str(value)
    except Exception:
        return None


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return default


def _b(value: Any) -> bool:
    return bool(value)


def _s(value: Any) -> str:
    return str(value or "")


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _safe_dict(value: Any) -> dict:
    """Return a dict safely from dict-like values."""
    try:
        return dict(value or {})
    except Exception:
        return {}


def _safe_list(value: Any) -> list:
    """Return a list safely from list-like values."""
    try:
        return list(value or [])
    except Exception:
        return []


def _duration_minutes(start: Any, end: Any) -> float | None:
    """Calculate minutes between datetimes safely."""
    if not start or not end:
        return None
    try:
        s = start if getattr(start, "tzinfo", None) else start.replace(tzinfo=timezone.utc)
        e = end if getattr(end, "tzinfo", None) else end.replace(tzinfo=timezone.utc)
        return round((e - s).total_seconds() / 60, 1)
    except Exception:
        return None


def _calc_exit_efficiency(realized_pnl_pct: float, mfe_pct: float) -> float | None:
    """How much of max favorable excursion was captured."""
    try:
        if mfe_pct <= 0:
            return None
        return round(max(0.0, min(100.0, (realized_pnl_pct / mfe_pct) * 100.0)), 2)
    except Exception:
        return None


def _classify_rejection_reason(reason: str, status: str = "") -> str:
    """Map raw rejection reason into a stable analytics category."""
    r = (reason or status or "").lower()
    if not r:
        return "unknown"
    if "not_whitelisted" in r or "whitelist" in r:
        return "not_whitelisted"
    if "max_positions" in r or "slot" in r or "position" in r:
        return "max_positions"
    if "cooldown" in r or "recovery_cycle_full" in r or "loss_streak" in r:
        return "cooldown"
    if "market" in r or "drawdown" in r or "guard" in r or "protection" in r:
        return "market_protection"
    if "execution" in r or "weak_drift" in r or "api" in r or "order" in r:
        return "execution_block"
    if "nour" in r or "pa_" in r or "bearish_reversal" in r or "mtf" in r or "resistance" in r or "overextended" in r:
        return "technical_filter"
    return "other"


def _build_decision_trace_id(symbol: str, exported_at: str, setup: str = "") -> str:
    base = exported_at.replace(":", "").replace("-", "").replace("+", "Z").replace(".", "_")
    safe_symbol = (symbol or "unknown").replace("/", "_").replace(":", "_")
    safe_setup = (setup or "unknown").replace("/", "_").replace(":", "_")
    return f"scan_{base}_{safe_symbol}_{safe_setup}"


def _append_jsonl(path: Path, record: dict) -> None:
    """Append one JSON record to a JSONL file safely."""
    _ensure_dir(path.parent)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def _write_json(path: Path, data: dict) -> None:
    """Overwrite a JSON file safely."""
    _ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, default=str)


# =========================================================
# Trade Record Builder
# =========================================================

def _build_trade_record(trade: Any, source: str, exported_at: str) -> dict:
    """Build complete trade record from TrackedTrade for AI analysis."""
    meta = dict(getattr(trade, "meta", {}) or {})
    sl = _f(getattr(trade, "sl", 0.0))
    entry = _f(getattr(trade, "entry", 0.0))
    tp1 = _f(getattr(trade, "tp1", 0.0))
    tp2 = _f(getattr(trade, "tp2", 0.0))
    margin = _f(getattr(trade, "simulation_margin_usdt", 0.0) or getattr(trade, "used_margin_usdt", 0.0))
    leverage = _i(getattr(trade, "leverage", 1) or 1) or 1
    opened_at = getattr(trade, "opened_at", None)
    closed_at = getattr(trade, "closed_at", None)

    holding_minutes = None
    if opened_at and closed_at:
        try:
            oa = opened_at if opened_at.tzinfo else opened_at.replace(tzinfo=timezone.utc)
            ca = closed_at if closed_at.tzinfo else closed_at.replace(tzinfo=timezone.utc)
            holding_minutes = round((ca - oa).total_seconds() / 60, 1)
        except Exception:
            pass

    risk_pct = None
    reward_tp1_pct = None
    reward_tp2_pct = None
    rr_tp1 = None
    rr_tp2 = None
    if entry > 0 and sl > 0:
        risk_pct = round(((entry - sl) / entry) * 100, 4)
    if entry > 0 and sl > 0 and tp1 > 0:
        reward_tp1_pct = round(((tp1 - entry) / entry) * 100, 4)
        if risk_pct and risk_pct > 0:
            rr_tp1 = round(reward_tp1_pct / risk_pct, 2)
    if entry > 0 and sl > 0 and tp2 > 0:
        reward_tp2_pct = round(((tp2 - entry) / entry) * 100, 4)
        if risk_pct and risk_pct > 0:
            rr_tp2 = round(reward_tp2_pct / risk_pct, 2)

    symbol = _s(getattr(trade, "symbol", ""))
    setup_type = _s(getattr(trade, "setup_type", ""))
    realized_pnl_pct = _f(getattr(trade, "realized_pnl_pct", 0.0))
    mfe_pct = _f(getattr(trade, "max_favorable_pct", 0.0))
    mae_pct = _f(getattr(trade, "max_adverse_pct", 0.0))
    exit_efficiency_pct = _calc_exit_efficiency(realized_pnl_pct, mfe_pct)
    decision_trace_id = _s(meta.get("decision_trace_id")) or _build_decision_trace_id(symbol, exported_at, setup_type)

    return {
        # ── Meta
        "record_type": "trade",
        "export_version": EXPORT_VERSION,
        "exported_at": exported_at,
        "source": source,

        # ── Identity
        "trade_id": _s(getattr(trade, "trade_id", "")),
        "symbol": _s(getattr(trade, "symbol", "")),
        "market_mode": _s(getattr(trade, "market_mode", "") or meta.get("market_mode", "")),
        "execution_path": _s(getattr(trade, "execution_path", "")),
        "trade_source": _s(getattr(trade, "trade_source", "")),
        "tracking_bucket": _s(getattr(trade, "tracking_bucket", "")),

        # ── Timing
        "opened_at": _iso(opened_at),
        "closed_at": _iso(closed_at),
        "updated_at": _iso(getattr(trade, "updated_at", None)),
        "holding_minutes": holding_minutes,
        "date": _today_str(opened_at) if opened_at else None,

        # ── Setup & Score
        "setup_type": _s(getattr(trade, "setup_type", "")),
        "execution_setup_tags": list(getattr(trade, "execution_setup_tags", []) or []),
        "score": _f(getattr(trade, "score", 0.0)),
        "score_breakdown": {
            "boost_score": _f(meta.get("boost_score")),
            "effective_score": _f(meta.get("effective_score")),
            "base_score": _f(meta.get("base_score")),
            "display_score": _f(meta.get("display_score")),
        },
        "execution_confidence": _f(meta.get("execution_confidence")),

        # ── Prices
        "entry_price": entry,
        "stop_loss": sl,
        "tp1": tp1,
        "tp2": tp2,
        "current_price": _f(getattr(trade, "current_price", 0.0)),
        "highest_price": _f(getattr(trade, "highest_price", 0.0)),

        # ── Risk/Reward
        "risk_pct": risk_pct,
        "reward_tp1_pct": reward_tp1_pct,
        "reward_tp2_pct": reward_tp2_pct,
        "rr_ratio_tp1": rr_tp1,
        "rr_ratio_tp2": rr_tp2,

        # ── Sizing
        "leverage": leverage,
        "margin_usdt": margin,
        "position_size_usdt": round(margin * leverage, 4) if margin > 0 else 0.0,
        "tp1_close_pct": _f(getattr(trade, "tp1_close_pct", 40.0)),
        "tp2_close_pct": _f(getattr(trade, "tp2_close_pct", 40.0)),
        "runner_close_pct": _f(getattr(trade, "runner_close_pct", 20.0)),

        # ── Status & Lifecycle
        "status": _s(getattr(trade, "status", "")),
        "is_closed": _b(getattr(trade, "is_closed", False)),
        "tp1_hit": _b(getattr(trade, "tp1_hit", False)),
        "tp2_hit": _b(getattr(trade, "tp2_hit", False)),
        "runner_active": _b(getattr(trade, "runner_active", False)),
        "sl_moved_to_entry": _b(getattr(trade, "sl_moved_to_entry", False)),
        "sl_moved_to_tp1": _b(getattr(trade, "sl_moved_to_tp1", False)),
        "protected_runner": _b(getattr(trade, "protected_runner", False)),
        "protected_sl": _f(getattr(trade, "protected_sl", 0.0)),
        "trailing_active": _b(getattr(trade, "trailing_active", False)),
        "trailing_tightened": _b(getattr(trade, "trailing_tightened", False)),

        # ── PnL
        "pnl_pct": _f(getattr(trade, "pnl_pct", 0.0)),
        "realized_pnl_pct": _f(getattr(trade, "realized_pnl_pct", 0.0)),
        "runner_pnl_pct": _f(getattr(trade, "runner_pnl_pct", 0.0)),
        "floating_pnl_pct": _f(getattr(trade, "floating_pnl_pct", 0.0)),
        "closed_portion_pct": _f(getattr(trade, "closed_portion_pct", 0.0)),

        # ── MFE / MAE
        "mfe": _f(getattr(trade, "max_favorable_pct", 0.0)),
        "mae": _f(getattr(trade, "max_adverse_pct", 0.0)),

        # ── Market Context at Entry
        "btc_context": {
            "btc_change_15m": _f(meta.get("btc_change_15m")),
            "btc_change_1h": _f(meta.get("btc_change_1h")),
            "btc_1h_ma5_gap_pct": _f(meta.get("btc_1h_ma5_gap_pct")),
            "hourly_ma5_pressure": _b(meta.get("hourly_ma5_pressure")),
        },
        "market_context": {
            "avg_change_15m": _f(meta.get("avg_change_15m")),
            "red_ratio_15m": _f(meta.get("red_ratio_15m")),
            "strong_coins_count": _i(meta.get("strong_coins_count")),
            "market_guard_valid_count": _i(meta.get("market_guard_valid_count")),
        },

        # ── Volume & Volatility
        "volume_metrics": {
            "vol_ratio": _f(meta.get("vol_ratio")),
            "volume_15m": _f(meta.get("volume_15m")),
            "turnover_usdt": _f(meta.get("turnover_usdt")),
            "volume_spike": _b(meta.get("volume_spike")),
        },
        "volatility_metrics": {
            "atr_pct": _f(meta.get("atr_pct")),
            "range_pct": _f(meta.get("range_pct")),
            "dist_ma": _f(meta.get("dist_ma")),
            "overextended": _b(meta.get("overextended")),
        },
        "relative_strength_metrics": {
            "rs_vs_btc": _f(meta.get("rs_vs_btc")),
            "rs_vs_market": _f(meta.get("rs_vs_market")),
            "relative_strength_tag": _s(meta.get("relative_strength_tag")),
        },

        # ── Gate Results
        "gate_results": {
            "whitelist_passed": _b(meta.get("strict_allowed") or meta.get("normal_extra_allowed")),
            "strict_allowed": _b(meta.get("strict_allowed")),
            "normal_extra_allowed": _b(meta.get("normal_extra_allowed")),
            "elite_allowed": _b(meta.get("elite_allowed")),
            "recovery_allowed": _b(meta.get("recovery_allowed")),
            "weak_drift_passed": _b(meta.get("weak_drift_passed")),
            "complete_plan": _b(meta.get("complete_plan")),
            "pa_gate_passed": _b(meta.get("pa_gate_passed")),
            "pa_score": _f(meta.get("pa_score")),
            "market_context_status": _s(meta.get("market_context_status")),
        },
        "nour_filter_results": {
            "name": _s(meta.get("nour_filter_name")),
            "passed": meta.get("nour_filter_passed"),
            "reason": _s(meta.get("nour_filter_reason")),
        },
        "mtf_confirmed": _b(meta.get("mtf_confirmed")),
        "resistance_warning": _b(meta.get("resistance_warning")),
        "near_resistance_warning": _b(meta.get("near_resistance_warning")),
        "resistance_4h": dict(meta.get("resistance_4h_context") or {}),

        # ── Exchange
        "exchange_order_ok": _b(getattr(trade, "exchange_order_ok", False)),
        "entry_order_id": _s(getattr(trade, "entry_order_id", "")),
        "exchange_sync_state": _s(getattr(trade, "exchange_sync_state", "")),

        # ── Slot
        "slot_exempt": _b(getattr(trade, "slot_exempt", False)),
        "slot_exempt_reason": _s(getattr(trade, "slot_exempt_reason", "")),
        "execution_trade": _b(getattr(trade, "execution_trade", False)),
        "telegram_announced": _b(getattr(trade, "telegram_announced", False)),

        # ── AI Development Schema #1: Unified trade analytics
        "strategy_version": _s(meta.get("strategy_version") or meta.get("bot_version") or meta.get("config_version")),
        "config_hash": _s(meta.get("config_hash")),
        "decision_trace_id": decision_trace_id,
        "entry_decision": {
            "decision_trace_id": decision_trace_id,
            "entry_reason": _s(meta.get("entry_reason") or meta.get("reason") or meta.get("signal_reason")),
            "acceptance_path": _s(meta.get("acceptance_path") or meta.get("execution_path")),
            "risk_mode": _s(meta.get("risk_mode") or meta.get("drawdown_mode") or meta.get("market_context_status")),
            "entry_quality_label": _s(meta.get("entry_quality_label")),
            "passed_filters": _safe_list(meta.get("passed_filters")),
            "weak_but_allowed_filters": _safe_list(meta.get("weak_but_allowed_filters")),
        },
        "candles": {
            "entry_pattern": _s(meta.get("entry_pattern") or meta.get("candlestick_pattern") or meta.get("pa_pattern")),
            "bullish_pattern": _s(meta.get("bullish_pattern")),
            "bearish_reversal_found": _b(meta.get("bearish_reversal_found") or meta.get("bearish_reversal_detected")),
            "bearish_reversal_type": _s(meta.get("bearish_reversal_type") or meta.get("reversal_type")),
            "wick_ratio": _f(meta.get("wick_ratio")),
            "upper_wick_ratio": _f(meta.get("upper_wick_ratio")),
            "lower_wick_ratio": _f(meta.get("lower_wick_ratio")),
            "body_ratio": _f(meta.get("body_ratio")),
            "candle_strength": _f(meta.get("candle_strength")),
            "last_3_candles": _safe_list(meta.get("last_3_candles")),
            "raw": _safe_dict(meta.get("candles") or meta.get("candle_context")),
        },
        "market_snapshot": {
            "market_mode": _s(getattr(trade, "market_mode", "") or meta.get("market_mode", "")),
            "btc_change_15m": _f(meta.get("btc_change_15m")),
            "btc_change_1h": _f(meta.get("btc_change_1h")),
            "btc_1h_ma5_gap_pct": _f(meta.get("btc_1h_ma5_gap_pct")),
            "hourly_ma5_pressure": _b(meta.get("hourly_ma5_pressure")),
            "avg_change_15m": _f(meta.get("avg_change_15m")),
            "red_ratio_15m": _f(meta.get("red_ratio_15m")),
            "strong_coins_count": _i(meta.get("strong_coins_count")),
            "market_guard_valid_count": _i(meta.get("market_guard_valid_count")),
            "market_context_status": _s(meta.get("market_context_status")),
            "market_context_reason": _s(meta.get("market_context_reason")),
        },
        "entry_quality": {
            "distance_from_vwap_pct": _f(meta.get("distance_from_vwap_pct") or meta.get("vwap_distance_pct")),
            "distance_from_ema20_pct": _f(meta.get("distance_from_ema20_pct") or meta.get("ema20_distance_pct")),
            "volume_spike": _b(meta.get("volume_spike")),
            "vol_ratio": _f(meta.get("vol_ratio")),
            "spread_pct": _f(meta.get("spread_pct")),
            "slippage_pct": _f(meta.get("slippage_pct")),
            "liquidity_score": _f(meta.get("liquidity_score")),
        },
        "timeline": {
            "opened_at": _iso(opened_at),
            "tp1_hit_at": _iso(getattr(trade, "tp1_hit_at", None) or meta.get("tp1_hit_at")),
            "tp2_hit_at": _iso(getattr(trade, "tp2_hit_at", None) or meta.get("tp2_hit_at")),
            "sl_moved_to_entry_at": _iso(getattr(trade, "sl_moved_to_entry_at", None) or meta.get("sl_moved_to_entry_at")),
            "sl_moved_to_tp1_at": _iso(getattr(trade, "sl_moved_to_tp1_at", None) or meta.get("sl_moved_to_tp1_at")),
            "trailing_started_at": _iso(getattr(trade, "trailing_started_at", None) or meta.get("trailing_started_at")),
            "trailing_tightened_at": _iso(getattr(trade, "trailing_tightened_at", None) or meta.get("trailing_tightened_at")),
            "closed_at": _iso(closed_at),
            "time_to_tp1_minutes": _f(meta.get("time_to_tp1_minutes"), -1.0) if meta.get("time_to_tp1_minutes") is not None else None,
            "time_to_tp2_minutes": _f(meta.get("time_to_tp2_minutes"), -1.0) if meta.get("time_to_tp2_minutes") is not None else None,
            "holding_minutes": holding_minutes,
        },
        "management": {
            "tp1_order_sent": _b(getattr(trade, "tp1_order_sent", False) or meta.get("tp1_order_sent")),
            "tp2_order_sent": _b(getattr(trade, "tp2_order_sent", False) or meta.get("tp2_order_sent")),
            "sl_order_sent": _b(getattr(trade, "sl_order_sent", False) or meta.get("sl_order_sent")),
            "sl_moved_to_entry_after_tp2": _b(meta.get("sl_moved_to_entry_after_tp2") or (getattr(trade, "tp2_hit", False) and getattr(trade, "sl_moved_to_entry", False))),
            "trailing_active": _b(getattr(trade, "trailing_active", False)),
            "trailing_distance_pct": _f(getattr(trade, "trailing_distance_pct", 0.0) or meta.get("trailing_distance_pct")),
            "protected_runner": _b(getattr(trade, "protected_runner", False)),
        },
        "performance": {
            "pnl_pct": _f(getattr(trade, "pnl_pct", 0.0)),
            "realized_pnl_pct": realized_pnl_pct,
            "runner_pnl_pct": _f(getattr(trade, "runner_pnl_pct", 0.0)),
            "floating_pnl_pct": _f(getattr(trade, "floating_pnl_pct", 0.0)),
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
            "exit_efficiency_pct": exit_efficiency_pct,
            "missed_runner_profit_pct": round(max(0.0, mfe_pct - realized_pnl_pct), 4) if mfe_pct > 0 else 0.0,
            "risk_reward_actual": round(realized_pnl_pct / risk_pct, 4) if risk_pct and risk_pct > 0 else None,
        },
        "post_exit": {
            "price_after_5m_pct": _f(meta.get("price_after_5m_pct")),
            "price_after_15m_pct": _f(meta.get("price_after_15m_pct")),
            "price_after_1h_pct": _f(meta.get("price_after_1h_pct")),
            "would_have_hit_extra_runner": _b(meta.get("would_have_hit_extra_runner")),
        },
        "diagnosis": {
            "result_class": _s(meta.get("result_class") or meta.get("diagnosis_class")),
            "main_issue": _s(meta.get("main_issue")),
            "suggestion": _s(meta.get("suggestion")),
            "exit_efficiency_pct": exit_efficiency_pct,
            "notes": _s(meta.get("diagnosis_notes")),
        },

        # ── Raw meta (full)
        "_meta_raw": meta,
    }



# =========================================================
# Protection Intelligence Helpers
# =========================================================

def _minutes_text_ar(minutes: Any) -> str:
    value = max(0, _i(minutes))
    if value <= 0:
        return "انتهت فترة التهدئة أو لا يوجد وقت متبقٍ"
    if value < 60:
        return f"{value} دقيقة"
    hours = value // 60
    mins = value % 60
    if mins:
        return f"{hours} ساعة و {mins} دقيقة"
    return f"{hours} ساعة"


def _loss_streak_message_ar(guard: dict | None) -> str:
    guard = _safe_dict(guard)
    cooldown = _i(guard.get("cooldown_minutes"), 120)
    limit = _i(guard.get("limit"), 5)
    remaining = _i(guard.get("remaining_minutes"), 0)
    base = (
        f"🛡️ تم إيقاف فتح صفقات جديدة لمدة {cooldown} دقيقة بسبب {limit} صفقات متتالية لم تحقق TP1. "
        "هذا إجراء وقائي يهدف إلى الحد من التداول أثناء فترات ضعف أداء السوق."
    )
    if remaining > 0:
        base += f" ⏳ الوقت المتبقي: {_minutes_text_ar(remaining)}."
    return base


def _build_loss_streak_protection(guard: dict | None) -> dict | None:
    guard = _safe_dict(guard)
    active = _b(guard.get("active"))
    streak = _i(guard.get("streak"))
    limit = _i(guard.get("limit"), 5)
    if not active and streak <= 0:
        return None
    severity = 3 if active else (2 if streak >= max(1, limit - 1) else 1)
    return {
        "type": "loss_streak_guard",
        "active": active,
        "severity": severity,
        "level": severity,
        "streak": streak,
        "limit": limit,
        "cooldown_minutes": _i(guard.get("cooldown_minutes"), 120),
        "remaining_minutes": _i(guard.get("remaining_minutes")),
        "cooldown_until": _s(guard.get("cooldown_until")),
        "last_loss_at": _s(guard.get("last_loss_at")),
        "symbols": _safe_list(guard.get("symbols")),
        "reason": _s(guard.get("reason") or "loss_streak_no_tp1_guard"),
        "message_ar": _loss_streak_message_ar(guard) if active else f"⚠️ سلسلة خسائر قبل TP1: {streak}/{limit}. لم يتم تفعيل الإيقاف الوقائي بعد.",
    }


def _build_drawdown_protection(drawdown: dict | None) -> dict | None:
    dd = _safe_dict(drawdown)
    level = _i(dd.get("level"))
    if not dd:
        return None
    active = bool(level > 0 or not _b(dd.get("allowed", True)))
    if not active:
        return None
    label = "تحذير" if level == 1 else "إيقاف مرن" if level == 2 else "إيقاف كامل" if level >= 3 else "طبيعي"
    return {
        "type": "daily_drawdown_guard",
        "active": active,
        "severity": level,
        "level": level,
        "allowed": _b(dd.get("allowed")),
        "reason": _s(dd.get("reason")),
        "drawdown_pct": _f(dd.get("drawdown_pct")),
        "drawdown_usdt": _f(dd.get("drawdown_usdt")),
        "current_equity": _f(dd.get("current_equity")),
        "start_of_day_balance": _f(dd.get("start_of_day_balance")),
        "remaining_minutes": 0,
        "message_ar": _s(dd.get("message_ar")) or f"🛡️ تم تفعيل حماية الخسارة اليومية — المستوى {level}: {label}.",
    }


def _build_market_mode_protection(mode_context: dict | None) -> dict | None:
    ctx = _safe_dict(mode_context)
    current = _s(ctx.get("protection_current"))
    if not current or current.lower() == "inactive":
        return None
    level = _i(ctx.get("protection_level") or ctx.get("level"))
    if level <= 0:
        # Existing main.py stores level in text only; infer it safely from protection_current.
        lowered = current.lower()
        if "level 3" in lowered:
            level = 3
        elif "level 2" in lowered:
            level = 2
        elif "level 1" in lowered:
            level = 1
    remaining = _i(ctx.get("remaining_minutes"))
    message = (
        f"🛡️ حماية السوق نشطة — {current}. "
        f"المرحلة التالية: {_s(ctx.get('protection_next') or 'غير محدد')}."
    )
    if remaining > 0:
        message += f" ⏳ الوقت المتبقي للمرحلة الحالية: {_minutes_text_ar(remaining)}."
    return {
        "type": "market_block_protection",
        "active": True,
        "severity": max(1, level),
        "level": level,
        "current": current,
        "next": _s(ctx.get("protection_next")),
        "remaining_minutes": remaining,
        "mode": _s(ctx.get("mode")),
        "message_ar": message,
    }


def _build_active_protections(
    *,
    mode_context: dict | None,
    drawdown: dict | None,
    loss_streak_guard: dict | None,
) -> list[dict]:
    protections: list[dict] = []
    for item in (
        _build_loss_streak_protection(loss_streak_guard),
        _build_drawdown_protection(drawdown),
        _build_market_mode_protection(mode_context),
    ):
        if item:
            protections.append(item)
    return sorted(protections, key=lambda x: _i(x.get("severity")), reverse=True)


def _build_risk_protection_summary(active_protections: list[dict]) -> dict:
    highest = max((_i(item.get("severity")) for item in active_protections), default=0)
    active_now = [item for item in active_protections if _b(item.get("active"))]
    primary = active_now[0] if active_now else (active_protections[0] if active_protections else {})
    return {
        "has_active_protection": bool(active_now),
        "active_count": len(active_now),
        "tracked_count": len(active_protections),
        "highest_level": highest,
        "primary_type": _s(primary.get("type")),
        "primary_message_ar": _s(primary.get("message_ar")),
        "total_remaining_minutes": sum(_i(item.get("remaining_minutes")) for item in active_now),
        "protection_types": [_s(item.get("type")) for item in active_protections],
    }


def _build_rejection_protection_context(exec_result: dict | None) -> dict:
    exec_result = _safe_dict(exec_result)
    status = _s(exec_result.get("status"))
    reason = _s(exec_result.get("reason"))
    slot_scope = _s(exec_result.get("slot_scope"))
    is_loss_guard = bool(
        "loss_streak" in status.lower()
        or "loss_streak" in reason.lower()
        or slot_scope == "loss_streak_guard"
    )
    is_drawdown = bool(
        "drawdown" in reason.lower()
        or slot_scope == "drawdown"
        or _i(exec_result.get("drawdown_level")) > 0
    )
    if is_loss_guard:
        cooldown = _i(exec_result.get("cooldown_minutes"), 120)
        limit = _i(exec_result.get("loss_streak_limit"), 5)
        remaining = _i(exec_result.get("cooldown_remaining_minutes"))
        message = (
            f"🛡️ تم إيقاف فتح صفقات جديدة لمدة {cooldown} دقيقة بسبب {limit} صفقات متتالية لم تحقق TP1. "
            "هذا إجراء وقائي يهدف إلى الحد من التداول أثناء فترات ضعف أداء السوق."
        )
        if remaining > 0:
            message += f" ⏳ الوقت المتبقي: {_minutes_text_ar(remaining)}."
        return {
            "protection_active": True,
            "protection_type": "loss_streak_guard",
            "protection_remaining_minutes": remaining,
            "protection_level": 3,
            "human_reason": message,
        }
    if is_drawdown:
        level = _i(exec_result.get("drawdown_level"))
        message = _s(exec_result.get("drawdown_message")) or "🛡️ تم تفعيل حماية الخسارة اليومية وفق مستوى الـ Daily Drawdown الحالي."
        return {
            "protection_active": bool(level > 0 or status.startswith("rejected")),
            "protection_type": "daily_drawdown_guard",
            "protection_remaining_minutes": 0,
            "protection_level": level,
            "human_reason": message,
        }
    return {
        "protection_active": False,
        "protection_type": "",
        "protection_remaining_minutes": 0,
        "protection_level": 0,
        "human_reason": "",
    }

# =========================================================
# Rejection Record Builder
# =========================================================

def _build_rejection_record(signal_item: dict, source: str, exported_at: str) -> dict:
    """Build rejection record from signal_item for AI analysis."""
    signal = signal_item.get("signal")
    exec_result = dict(signal_item.get("execution") or {})
    meta = dict(getattr(signal, "meta", {}) or {}) if signal else {}
    symbol = _s(getattr(signal, "symbol", "")) if signal else ""
    setup_type = _s(getattr(signal, "setup_type", "")) if signal else ""
    reason = _s(exec_result.get("reason"))
    status = _s(exec_result.get("status"))
    decision_trace_id = _s(meta.get("decision_trace_id") or exec_result.get("decision_trace_id")) or _build_decision_trace_id(symbol, exported_at, setup_type)
    rejection_category = _s(exec_result.get("rejection_category") or meta.get("rejection_category")) or _classify_rejection_reason(reason, status)
    protection_context = _build_rejection_protection_context(exec_result)
    if protection_context.get("protection_active"):
        rejection_category = "protection_pause"

    return {
        # ── Meta
        "record_type": "rejection",
        "export_version": EXPORT_VERSION,
        "exported_at": exported_at,
        "source": source,
        "timestamp": exported_at,

        # ── Identity
        "symbol": _s(getattr(signal, "symbol", "")) if signal else "",
        "market_mode": _s(getattr(signal, "market_mode", "") or meta.get("market_mode", "")),
        "setup_type": _s(getattr(signal, "setup_type", "")) if signal else "",
        "execution_setup_tags": list(getattr(signal, "execution_setup_tags", []) or []) if signal else [],

        # ── Score
        "score": _f(getattr(signal, "score", 0.0)) if signal else 0.0,
        "boost_score": _f(meta.get("boost_score")),
        "effective_score": _f(meta.get("effective_score")),

        # ── Prices
        "entry_price": _f(getattr(signal, "entry", 0.0)) if signal else 0.0,
        "stop_loss": _f(getattr(signal, "sl", 0.0)) if signal else 0.0,
        "tp1": _f(getattr(signal, "tp1", 0.0)) if signal else 0.0,
        "tp2": _f(getattr(signal, "tp2", 0.0)) if signal else 0.0,

        # ── Rejection Details
        "exec_status": _s(exec_result.get("status")),
        "rejection_reason": _s(exec_result.get("reason")),
        "rejection_path": _s(exec_result.get("path")),
        "slot_scope": _s(exec_result.get("slot_scope")),
        "drawdown_level": _i(exec_result.get("drawdown_level")),

        # ── Protection Context (clear reason for risk pauses)
        "protection_active": _b(protection_context.get("protection_active")),
        "protection_type": _s(protection_context.get("protection_type")),
        "protection_remaining_minutes": _i(protection_context.get("protection_remaining_minutes")),
        "protection_level": _i(protection_context.get("protection_level")),
        "human_reason": _s(protection_context.get("human_reason")),

        # ── Gate State at Rejection
        "gate_snapshot": {
            "allowed": _b(exec_result.get("allowed")),
            "strict_allowed": _b(exec_result.get("strict_allowed")),
            "normal_extra_allowed": _b(exec_result.get("normal_extra_allowed")),
            "elite_allowed": _b(exec_result.get("elite_allowed")),
            "recovery_allowed": _b(exec_result.get("recovery_allowed")),
            "weak_drift_passed": _b(exec_result.get("weak_drift_passed")),
            "complete_plan": _b(exec_result.get("complete_plan")),
            "pa_gate_passed": _b(exec_result.get("pa_gate_passed")),
            "pa_score": _f(exec_result.get("pa_score")),
            "nour_filter_name": _s(exec_result.get("nour_filter_name")),
            "nour_filter_passed": exec_result.get("nour_filter_passed"),
            "nour_filter_reason": _s(exec_result.get("nour_filter_reason")),
            "market_context_status": _s(exec_result.get("market_context_status")),
            "market_context_reason": _s(exec_result.get("market_context_reason")),
            "near_resistance_warning": _b(exec_result.get("near_resistance_warning")),
            "pending_pullback": _b(exec_result.get("pending_pullback")),
        },

        # ── Market Context at Rejection
        "btc_change_15m": _f(meta.get("btc_change_15m")),
        "avg_change_15m": _f(meta.get("avg_change_15m")),
        "red_ratio_15m": _f(meta.get("red_ratio_15m")),
        "vol_ratio": _f(meta.get("vol_ratio")),
        "dist_ma": _f(meta.get("dist_ma")),
        "mtf_confirmed": _b(meta.get("mtf_confirmed")),
        "resistance_4h": dict(meta.get("resistance_4h_context") or {}),

        # ── AI Development Schema #2: Rejection/decision quality
        "decision_trace_id": decision_trace_id,
        "rejection_category": rejection_category,
        "missed_opportunity_score": _f(exec_result.get("missed_opportunity_score") or meta.get("missed_opportunity_score")),
        "post_rejection_tracking": {
            "price_after_5m_pct": _f(exec_result.get("price_after_5m_pct") or meta.get("price_after_5m_pct")),
            "price_after_15m_pct": _f(exec_result.get("price_after_15m_pct") or meta.get("price_after_15m_pct")),
            "price_after_1h_pct": _f(exec_result.get("price_after_1h_pct") or meta.get("price_after_1h_pct")),
            "would_hit_tp1": _b(exec_result.get("would_hit_tp1") or meta.get("would_hit_tp1")),
            "would_hit_tp2": _b(exec_result.get("would_hit_tp2") or meta.get("would_hit_tp2")),
            "would_hit_sl": _b(exec_result.get("would_hit_sl") or meta.get("would_hit_sl")),
            "max_pump_after_rejection_pct": _f(exec_result.get("max_pump_after_rejection_pct") or meta.get("max_pump_after_rejection_pct")),
            "max_dump_after_rejection_pct": _f(exec_result.get("max_dump_after_rejection_pct") or meta.get("max_dump_after_rejection_pct")),
        },
        "rejection_verdict": {
            "was_correct": exec_result.get("rejection_was_correct", meta.get("rejection_was_correct")),
            "reason": _s(exec_result.get("rejection_verdict_reason") or meta.get("rejection_verdict_reason")),
        },

        # ── Full exec_result for deep analysis
        "_exec_result_raw": exec_result,
    }


# =========================================================
# Daily Snapshot Builder
# =========================================================

def _build_daily_snapshot(
    source: str,
    trades: list,
    execution_results: list,
    signal_items: list,
    wallet: dict | None,
    daily_log: list,
    mode_context: dict | None,
    portfolio_state: Any,
    drawdown_status: Any,
    loss_streak_guard: dict | None,
    scan_stats: dict | None,
    exported_at: str,
    today: str,
) -> dict:
    """Build comprehensive daily snapshot for AI analysis."""

    # ── تصنيف الصفقات (موحّد مع report_simulation تماماً) ────────────────────────
    # رابح = status في WIN_STATUSES أو ضرب TP1 (أمّن ربح، حتى لو runner مفتوح).
    # خاسر = status في LOSS_STATUSES (خسارة كاملة) فقط.
    # الباقي (تعادل مثل protected_entry_exit، أو مفتوح لم يضرب TP1) لا يُحتسب.
    # نفس منطق _closed_wr_parts في report_simulation عشان التطابق 100%.
    def _status_l(t) -> str:
        return str(getattr(t, "status", "") or "").strip().lower()

    def _is_winner(t) -> bool:
        return _status_l(t) in _WIN_STATUSES or _b(getattr(t, "tp1_hit", False))

    def _is_loser(t) -> bool:
        return (not _is_winner(t)) and _status_l(t) in _LOSS_STATUSES

    def _is_decided(t) -> bool:
        # محسومة = رابحة (TP1) أو خاسرة كاملة. (التعادل والمفتوح مش محسومين)
        return _is_winner(t) or _is_loser(t)

    open_trades = [t for t in trades if not _is_decided(t)]
    closed_trades = [t for t in trades if _is_decided(t)]
    winners = [t for t in closed_trades if _is_winner(t)]
    losers = [t for t in closed_trades if _is_loser(t)]

    # Execution statistics
    accepted = [r for r in execution_results if r.get("status") in {"accepted_preview", "pending_pullback_preview"}]
    rejected = [r for r in execution_results if str(r.get("status", "")).startswith("rejected") or r.get("status") == "candidate_only"]
    rejection_reasons: dict[str, int] = {}
    for r in rejected:
        key = _s(r.get("reason") or r.get("status") or "unknown")
        rejection_reasons[key] = rejection_reasons.get(key, 0) + 1

    # Market mode distribution from signal_items
    mode_distribution: dict[str, int] = {}
    for item in signal_items:
        signal = item.get("signal")
        mode = _s(getattr(signal, "market_mode", "") if signal else "")
        if mode:
            mode_distribution[mode] = mode_distribution.get(mode, 0) + 1

    # Setup performance
    setup_stats: dict[str, dict] = {}
    for t in closed_trades:
        setup = _s(getattr(t, "setup_type", "")) or "unknown"
        if setup not in setup_stats:
            setup_stats[setup] = {"count": 0, "wins": 0, "pnl_sum": 0.0}
        setup_stats[setup]["count"] += 1
        pnl = _f(getattr(t, "realized_pnl_pct", 0.0))
        if pnl > 0:
            setup_stats[setup]["wins"] += 1
        setup_stats[setup]["pnl_sum"] = round(setup_stats[setup]["pnl_sum"] + pnl, 4)

    # Portfolio state fields
    ps = portfolio_state
    ps_dict = {}
    if ps is not None:
        for field in ("reference_portfolio", "start_of_day_balance", "current_equity", "realized_pnl_usdt", "unrealized_pnl_usdt", "drawdown_pct", "drawdown_usdt", "trades_opened_today"):
            try:
                ps_dict[field] = _f(getattr(ps, field, 0.0))
            except Exception:
                pass

    # Drawdown
    dd_dict = {}
    if drawdown_status is not None:
        for field in ("level", "allowed", "reason", "drawdown_pct", "drawdown_usdt", "current_equity", "start_of_day_balance", "message_ar"):
            try:
                dd_dict[field] = getattr(drawdown_status, field, None)
            except Exception:
                pass

    # Symbol performance rankings
    symbol_stats: dict[str, dict] = {}
    for t in closed_trades:
        sym = _s(getattr(t, "symbol", "")) or "unknown"
        if sym not in symbol_stats:
            symbol_stats[sym] = {"count": 0, "wins": 0, "pnl_sum": 0.0, "best": -999999.0, "worst": 999999.0}
        pnl = _f(getattr(t, "realized_pnl_pct", 0.0))
        symbol_stats[sym]["count"] += 1
        symbol_stats[sym]["wins"] += 1 if pnl > 0 else 0
        symbol_stats[sym]["pnl_sum"] = round(symbol_stats[sym]["pnl_sum"] + pnl, 4)
        symbol_stats[sym]["best"] = max(symbol_stats[sym]["best"], pnl)
        symbol_stats[sym]["worst"] = min(symbol_stats[sym]["worst"], pnl)

    symbol_rows = [
        {
            "symbol": sym,
            "count": v["count"],
            "wins": v["wins"],
            "win_rate": round(v["wins"] / max(1, v["count"]) * 100, 2),
            "total_pnl_pct": round(v["pnl_sum"], 4),
            "avg_pnl_pct": round(v["pnl_sum"] / max(1, v["count"]), 4),
            "best_trade_pct": round(v["best"], 4),
            "worst_trade_pct": round(v["worst"], 4),
        }
        for sym, v in symbol_stats.items()
    ]

    # Enhanced setup ranking
    setup_ranking: dict[str, dict] = {}
    for setup, v in setup_stats.items():
        setup_trades = [t for t in closed_trades if (_s(getattr(t, "setup_type", "")) or "unknown") == setup]
        gross_win = sum(max(0.0, _f(getattr(t, "realized_pnl_pct", 0.0))) for t in setup_trades)
        gross_loss = abs(sum(min(0.0, _f(getattr(t, "realized_pnl_pct", 0.0))) for t in setup_trades))
        tp2_count = sum(1 for t in setup_trades if getattr(t, "tp2_hit", False))
        runner_count = sum(1 for t in setup_trades if getattr(t, "runner_active", False) or str(getattr(t, "status", "")) == "trailing_hit")
        setup_ranking[setup] = {
            "count": v["count"],
            "wins": v["wins"],
            "win_rate": round(v["wins"] / max(1, v["count"]) * 100, 2),
            "avg_pnl_pct": round(v["pnl_sum"] / max(1, v["count"]), 4),
            "total_pnl_pct": round(v["pnl_sum"], 4),
            "profit_factor": round(gross_win / max(0.0001, gross_loss), 4) if gross_loss > 0 else None,
            "tp2_rate": round(tp2_count / max(1, len(setup_trades)) * 100, 2),
            "runner_rate": round(runner_count / max(1, len(setup_trades)) * 100, 2),
        }

    # Exit analysis
    exit_efficiencies = []
    missed_runner_profit = 0.0
    for t in closed_trades:
        realized = _f(getattr(t, "realized_pnl_pct", 0.0))
        mfe = _f(getattr(t, "max_favorable_pct", 0.0))
        eff = _calc_exit_efficiency(realized, mfe)
        if eff is not None:
            exit_efficiencies.append(eff)
        if mfe > realized:
            missed_runner_profit += max(0.0, mfe - realized)

    # Filter and decision quality from current execution results
    filter_buckets: dict[str, int] = {}
    rejection_categories: dict[str, int] = {}
    correct_rejections = wrong_rejections = unknown_rejections = 0
    wrong_rejection_rows: list[dict] = []
    correct_rejection_rows: list[dict] = []
    filter_costs: dict[str, float] = {}
    filter_saves: dict[str, float] = {}
    for r in rejected:
        reason = _s(r.get("reason") or r.get("status") or "unknown")
        category = _s(r.get("rejection_category")) or _classify_rejection_reason(reason, _s(r.get("status")))
        rejection_categories[category] = rejection_categories.get(category, 0) + 1
        if "mtf" in reason.lower():
            filter_buckets["mtf_confirmation"] = filter_buckets.get("mtf_confirmation", 0) + 1
        if "pa_" in reason.lower() or "breakout" in reason.lower() or "bearish_reversal" in reason.lower():
            filter_buckets["pa_gate"] = filter_buckets.get("pa_gate", 0) + 1
        if "nour" in reason.lower():
            filter_buckets["nour_filter"] = filter_buckets.get("nour_filter", 0) + 1

        pump = _f(r.get("max_pump_after_rejection_pct"))
        dump = _f(r.get("max_dump_after_rejection_pct"))
        one_hour = _f(r.get("price_after_1h_pct"))
        verdict = r.get("rejection_was_correct")
        if verdict is True:
            correct_rejections += 1
            saved = abs(min(dump, one_hour, 0.0))
            filter_saves[category] = filter_saves.get(category, 0.0) + saved
            correct_rejection_rows.append({
                "symbol": _s(r.get("symbol")),
                "setup_type": _s(r.get("setup_type")),
                "reason": reason,
                "category": category,
                "saved_dump_pct": round(saved, 4),
                "price_after_1h_pct": one_hour,
            })
        elif verdict is False:
            wrong_rejections += 1
            cost = max(pump, one_hour, 0.0)
            filter_costs[category] = filter_costs.get(category, 0.0) + cost
            wrong_rejection_rows.append({
                "symbol": _s(r.get("symbol")),
                "setup_type": _s(r.get("setup_type")),
                "reason": reason,
                "category": category,
                "missed_pump_pct": round(cost, 4),
                "would_hit_tp1": _b(r.get("would_hit_tp1")),
                "would_hit_tp2": _b(r.get("would_hit_tp2")),
            })
        else:
            unknown_rejections += 1

    most_expensive_filter = None
    if filter_costs:
        k, v = max(filter_costs.items(), key=lambda kv: kv[1])
        most_expensive_filter = {"category": k, "missed_pump_pct": round(v, 4)}
    most_protective_filter = None
    if filter_saves:
        k, v = max(filter_saves.items(), key=lambda kv: kv[1])
        most_protective_filter = {"category": k, "saved_dump_pct": round(v, 4)}

    bot_health = {
        "execution_errors": sum(1 for r in execution_results if "error" in _s(r.get("status")).lower() or "error" in _s(r.get("reason")).lower()),
        "sync_errors": sum(1 for t in trades if "sync" in _s(getattr(t, "exchange_sync_state", "")).lower() and "error" in _s(getattr(t, "exchange_sync_state", "")).lower()),
        "order_failures": sum(1 for t in trades if getattr(t, "exchange_order_ok", True) is False),
        "api_latency_avg_ms": _f(mode_context.get("api_latency_avg_ms") if mode_context else 0.0),
    }

    lessons_today = []
    if setup_ranking:
        best_setup = max(setup_ranking.items(), key=lambda kv: kv[1].get("total_pnl_pct", 0.0))
        lessons_today.append(f"Best setup today: {best_setup[0]} total_pnl={best_setup[1].get('total_pnl_pct')}%")
    if symbol_rows:
        best_symbol = max(symbol_rows, key=lambda x: x.get("total_pnl_pct", 0.0))
        worst_symbol = min(symbol_rows, key=lambda x: x.get("total_pnl_pct", 0.0))
        lessons_today.append(f"Best symbol today: {best_symbol['symbol']} total_pnl={best_symbol['total_pnl_pct']}%")
        lessons_today.append(f"Worst symbol today: {worst_symbol['symbol']} total_pnl={worst_symbol['total_pnl_pct']}%")
    if rejection_reasons:
        top_rejection = max(rejection_reasons.items(), key=lambda kv: kv[1])
        lessons_today.append(f"Top rejection reason: {top_rejection[0]} count={top_rejection[1]}")
    if exit_efficiencies:
        lessons_today.append(f"Average exit efficiency: {round(sum(exit_efficiencies) / max(1, len(exit_efficiencies)), 2)}%")

    active_protections = _build_active_protections(
        mode_context=mode_context,
        drawdown=dd_dict,
        loss_streak_guard=loss_streak_guard,
    )
    risk_protection_summary = _build_risk_protection_summary(active_protections)

    return {
        "record_type": "daily_snapshot",
        "export_version": EXPORT_VERSION,
        "exported_at": exported_at,
        "source": source,
        "date": today,

        # ── Wallet
        "wallet": dict(wallet or {}),
        "equity_curve": list(daily_log or []),

        # ── Trades Summary
        "trades_summary": {
            "total": len(trades),
            "open": len(open_trades),
            "closed": len(closed_trades),
            "winners": len(winners),
            "losers": len(losers),
            "win_rate": round(len(winners) / max(1, len(closed_trades)) * 100, 2),
            "tp1_hit": sum(1 for t in closed_trades if getattr(t, "tp1_hit", False)),
            "tp2_hit": sum(1 for t in closed_trades if getattr(t, "tp2_hit", False)),
            "trailing_hit": sum(1 for t in closed_trades if str(getattr(t, "status", "")) == "trailing_hit"),
            "direct_sl": sum(1 for t in closed_trades if str(getattr(t, "status", "")) == "closed_loss" and not getattr(t, "tp1_hit", False)),
            "avg_realized_pnl_pct": round(sum(_f(getattr(t, "realized_pnl_pct", 0.0)) for t in closed_trades) / max(1, len(closed_trades)), 4),
            "total_realized_pnl_pct": round(sum(_f(getattr(t, "realized_pnl_pct", 0.0)) for t in closed_trades), 4),
        },

        # ── Open Trades Summary
        "open_trades_summary": [
            {
                "trade_id": _s(getattr(t, "trade_id", "")),
                "symbol": _s(getattr(t, "symbol", "")),
                "status": _s(getattr(t, "status", "")),
                "pnl_pct": _f(getattr(t, "pnl_pct", 0.0)),
                "tp1_hit": _b(getattr(t, "tp1_hit", False)),
                "tp2_hit": _b(getattr(t, "tp2_hit", False)),
                "setup_type": _s(getattr(t, "setup_type", "")),
                "score": _f(getattr(t, "score", 0.0)),
                "margin_usdt": _f(getattr(t, "simulation_margin_usdt", 0.0) or getattr(t, "used_margin_usdt", 0.0)),
                "opened_at": _iso(getattr(t, "opened_at", None)),
            }
            for t in open_trades[:50]
        ],

        # ── Execution Statistics
        "execution_statistics": {
            "checked": len(execution_results),
            "accepted": len(accepted),
            "rejected": len(rejected),
            "accept_rate": round(len(accepted) / max(1, len(execution_results)) * 100, 2),
        },

        # ── Rejection Statistics
        "rejection_statistics": {
            "total": len(rejected),
            "by_reason": dict(sorted(rejection_reasons.items(), key=lambda x: -x[1])[:20]),
        },

        # ── Market Mode Distribution
        "market_mode_distribution": mode_distribution,
        "current_mode": dict(mode_context or {}),

        # ── Unified Protection Intelligence
        "active_protections": active_protections,
        "protection_status": {
            "active": bool(risk_protection_summary.get("has_active_protection")),
            "primary_type": risk_protection_summary.get("primary_type"),
            "highest_level": risk_protection_summary.get("highest_level"),
            "remaining_minutes": risk_protection_summary.get("total_remaining_minutes"),
            "message_ar": risk_protection_summary.get("primary_message_ar"),
        },
        "risk_protection_summary": risk_protection_summary,
        "protection_history": active_protections,

        # ── AI Development Schema #3: Snapshot intelligence
        "comparison": {
            "win_rate_vs_yesterday": None,
            "profit_vs_yesterday": None,
            "accept_rate_vs_yesterday": None,
            "note": "requires previous daily snapshot loader",
        },
        "symbol_performance": {
            "top_winners": sorted(symbol_rows, key=lambda x: -x["total_pnl_pct"])[:10],
            "top_losers": sorted(symbol_rows, key=lambda x: x["total_pnl_pct"])[:10],
        },
        "setup_ranking": dict(sorted(setup_ranking.items(), key=lambda kv: -kv[1].get("total_pnl_pct", 0.0))),
        "exit_analysis": {
            "avg_exit_efficiency_pct": round(sum(exit_efficiencies) / max(1, len(exit_efficiencies)), 2) if exit_efficiencies else None,
            "missed_runner_profit_pct": round(missed_runner_profit, 4),
            "sample_size": len(exit_efficiencies),
        },
        "filter_analysis": {
            "by_filter_family": filter_buckets,
            "by_rejection_category": rejection_categories,
            "most_common_filter_family": max(filter_buckets.items(), key=lambda kv: kv[1])[0] if filter_buckets else None,
        },
        "decision_quality": {
            "correct_rejections": correct_rejections,
            "wrong_rejections": wrong_rejections,
            "unknown_rejections": unknown_rejections,
            "top_wrong_rejections": sorted(wrong_rejection_rows, key=lambda x: -_f(x.get("missed_pump_pct")))[:10],
            "top_correct_rejections": sorted(correct_rejection_rows, key=lambda x: -_f(x.get("saved_dump_pct")))[:10],
            "most_expensive_filter": most_expensive_filter,
            "most_protective_filter": most_protective_filter,
            "note": "post_rejection_tracking_v1 uses scan checkpoint prices",
        },
        "bot_health": bot_health,
        "lessons_today": lessons_today,

        # ── Setup Performance
        "setup_performance": {
            k: {
                "count": v["count"],
                "wins": v["wins"],
                "win_rate": round(v["wins"] / max(1, v["count"]) * 100, 2),
                "avg_pnl_pct": round(v["pnl_sum"] / max(1, v["count"]), 4),
            }
            for k, v in setup_stats.items()
        },

        # ── Portfolio State
        "portfolio_state": ps_dict,

        # ── Drawdown
        "drawdown": dd_dict,

        # ── Loss Streak Guard
        "loss_streak_guard": dict(loss_streak_guard or {}),

        # ── Scan Stats
        "scan_stats": dict(scan_stats or {}),
    }


# =========================================================
# Main Export Function
# =========================================================

def export_ai_snapshot(
    result: dict,
    source: str,
    *,
    base_dir: Path | None = None,
    now: datetime | None = None,
) -> dict:
    """Export AI snapshot from a run_once result dict.

    source: "simulation" or "execution"
    Returns a dict with export stats (never raises).
    """
    stats = {
        "ok": False,
        "source": source,
        "trades_written": 0,
        "rejections_written": 0,
        "daily_snapshot_written": False,
        "error": None,
    }

    try:
        base = base_dir or AI_REPORTS_BASE_DIR
        now = now or _now_utc()
        today = _today_str(now)
        exported_at = now.isoformat()

        src_dir = base / source
        snapshot_dir = base / "daily_snapshots"

        trades_path = src_dir / f"{today}_trades.jsonl"
        rejections_path = src_dir / f"{today}_rejections.jsonl"
        snapshot_path = snapshot_dir / f"{today}_{source}.json"

        # ── Select correct trades and results based on source
        if source == "simulation":
            trades = list(result.get("simulation_trades", []) or [])
            execution_results = list(result.get("simulation_execution_results", []) or [])
            signal_items = list(result.get("simulation_signal_items", []) or result.get("signal_items", []) or [])
            wallet = dict(result.get("simulation_wallet") or {})
            daily_log = list(result.get("simulation_daily_log") or [])
        else:
            trades = list(result.get("trades", []) or [])
            execution_results = list(result.get("execution_results", []) or [])
            signal_items = list(result.get("signal_items", []) or [])
            wallet = {}
            daily_log = []

        mode_context = dict(result.get("mode_context") or {})
        portfolio_state = result.get("portfolio_state")
        drawdown_status = result.get("drawdown_status")
        loss_streak_guard = dict(result.get("loss_streak_guard") or {})
        scan_stats = dict(result.get("scan_stats") or {})

        # ── 1. Write trades JSONL (closed trades only for day file, avoid duplicates)
        written_ids: set[str] = set()
        try:
            existing_ids: set[str] = set()
            if trades_path.exists():
                with open(trades_path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            row = json.loads(line)
                            tid = row.get("trade_id", "")
                            if tid:
                                existing_ids.add(tid)
                        except Exception:
                            pass

            for trade in trades:
                tid = _s(getattr(trade, "trade_id", ""))
                is_closed = getattr(trade, "is_closed", False) or str(getattr(trade, "status", "")) in {"closed_win", "closed_loss", "breakeven_after_tp1", "trailing_hit", "expired"}
                if is_closed and tid and tid not in existing_ids:
                    record = _build_trade_record(trade, source, exported_at)
                    _append_jsonl(trades_path, record)
                    written_ids.add(tid)
                    stats["trades_written"] += 1
        except Exception as exc:
            stats["error"] = f"trades_write: {exc}"

        # ── 2. Write rejections JSONL (current scan only)
        try:
            for item in signal_items:
                exec_result = dict(item.get("execution") or {})
                status = _s(exec_result.get("status"))
                is_rejection = status.startswith("rejected") or status == "candidate_only"
                if is_rejection:
                    record = _build_rejection_record(item, source, exported_at)
                    _append_jsonl(rejections_path, record)
                    stats["rejections_written"] += 1
        except Exception as exc:
            stats["error"] = (stats["error"] or "") + f" | rejections_write: {exc}"

        # ── 3. Write daily snapshot JSON (overwrite = latest state)
        try:
            snapshot = _build_daily_snapshot(
                source=source,
                trades=trades,
                execution_results=execution_results,
                signal_items=signal_items,
                wallet=wallet,
                daily_log=daily_log,
                mode_context=mode_context,
                portfolio_state=portfolio_state,
                drawdown_status=drawdown_status,
                loss_streak_guard=loss_streak_guard,
                scan_stats=scan_stats,
                exported_at=exported_at,
                today=today,
            )
            _write_json(snapshot_path, snapshot)
            stats["daily_snapshot_written"] = True
        except Exception as exc:
            stats["error"] = (stats["error"] or "") + f" | snapshot_write: {exc}"

        stats["ok"] = True

    except Exception as exc:
        stats["error"] = f"export_ai_snapshot: {exc}\n{traceback.format_exc()[-500:]}"

    return stats
