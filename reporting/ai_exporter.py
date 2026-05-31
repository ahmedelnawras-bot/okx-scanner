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

        # ── Raw meta (full)
        "_meta_raw": meta,
    }


# =========================================================
# Rejection Record Builder
# =========================================================

def _build_rejection_record(signal_item: dict, source: str, exported_at: str) -> dict:
    """Build rejection record from signal_item for AI analysis."""
    signal = signal_item.get("signal")
    exec_result = dict(signal_item.get("execution") or {})
    meta = dict(getattr(signal, "meta", {}) or {}) if signal else {}

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

    open_trades = [t for t in trades if not getattr(t, "is_closed", False) and str(getattr(t, "status", "")) not in {"closed_win", "closed_loss", "breakeven_after_tp1", "trailing_hit", "expired"}]
    closed_trades = [t for t in trades if getattr(t, "is_closed", False) or str(getattr(t, "status", "")) in {"closed_win", "closed_loss", "breakeven_after_tp1", "trailing_hit", "expired"}]
    winners = [t for t in closed_trades if _f(getattr(t, "realized_pnl_pct", 0.0)) > 0]
    losers = [t for t in closed_trades if _f(getattr(t, "realized_pnl_pct", 0.0)) <= 0]

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
