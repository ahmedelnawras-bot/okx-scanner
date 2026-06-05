from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from analysis.models import SignalCandidate
from utils.constants import MODE_RECOVERY_LONG
from .models import TrackedTrade


# Execution trade here means "count as an actually opened execution path"
# and not just a preview/candidate shown in Telegram.
#
# Important:
# - pending_pullback_preview is NOT an opened trade
# - candidate_only / rejected_* are NOT execution trades
# - accepted_preview keeps the current project behavior as the execution-tracking path
_EXECUTION_OPEN_STATUSES = {
    "accepted_preview",
    "executed",
    "open",
    "tp1",
    "tp2",
    "trailing",
    "runner",
    "tp1_partial",
    "tp2_partial",
}

_NON_FILLED_PREVIEW_STATUSES = {
    # pending_pullback_preview is an execution-intent trade waiting for entry,
    # so it must stay in the execution lifecycle / slot / same-symbol guards.
    "candidate_only",
    "normal_signal_only",
    "rejected_quality",
    "rejected_limit",
    "rejected_risk",
    "rejected_same_symbol",
}


def is_execution_trade_status(status: str | None) -> bool:
    return str(status or "").strip().lower() in _EXECUTION_OPEN_STATUSES


def _target_plan_for(signal: SignalCandidate, execution_path: str) -> tuple[str, float, float, float]:
    if execution_path == "recovery" or signal.market_mode == MODE_RECOVERY_LONG:
        return "recovery_50_25_25", 50.0, 25.0, 25.0

    return "standard_40_40_20", 40.0, 40.0, 20.0


def _resolve_entry_price(
    signal: SignalCandidate,
    execution_result: dict,
) -> float:
    """
    Resolve the actual filled entry price if available.

    Priority:
    1) normalized filled_entry
    2) avg_fill_price
    3) OKX avgPx
    4) fallback to signal.entry
    """
    try:
        raw_entry = (
            execution_result.get("filled_entry")
            or execution_result.get("avg_fill_price")
            or execution_result.get("avgPx")
            or signal.entry
        )
        return float(raw_entry)
    except Exception:
        return float(signal.entry)


def _resolve_execution_trade_flag(execution_status: str) -> bool:
    normalized = str(execution_status or "").strip().lower()

    if normalized in _NON_FILLED_PREVIEW_STATUSES:
        return False

    return normalized in _EXECUTION_OPEN_STATUSES


def _resolve_tracking_bucket(execution_trade: bool) -> str:
    return "execution" if execution_trade else "normal"


def _preview_only_lifecycle_fields(
    execution_status: str,
    now: datetime,
) -> dict[str, Any]:
    """
    Non-filled previews / rejected execution checks should not come back as
    currently-open trades after deep_clean or after the next scan.

    We still keep them in history through execution_status / execution_reason,
    but archive them immediately as expired observations.
    """
    preview_reason = str(execution_status or "normal_signal_only").strip().lower() or "normal_signal_only"

    return {
        "status": "expired",
        "closed_at": now,
        "slot_exempt": True,
        "slot_exempt_reason": f"preview_only:{preview_reason}",
        "daily_open_risk_exempt": True,
        "same_symbol_block_exempt": True,
        "runner_active": False,
        "trailing_active": False,
        "protected_runner": False,
        "exchange_sync_state": "not_submitted",
    }


def _execution_lifecycle_fields(execution_result: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Actually opened execution-path trades start as live/open positions.

    Exchange-specific IDs may still be blank at registration time because the
    order gets sent later in main.py, then attached back onto the trade.
    """
    execution_result = execution_result or {}
    exchange_order_ok = bool(execution_result.get("exchange_order_ok"))
    exchange_sync_state = str(execution_result.get("exchange_sync_state") or "queued_for_submission")
    if exchange_order_ok and exchange_sync_state == "queued_for_submission":
        exchange_sync_state = "submitted"

    return {
        "status": "open",
        "closed_at": None,
        "slot_exempt": False,
        "slot_exempt_reason": "",
        "daily_open_risk_exempt": False,
        "same_symbol_block_exempt": False,
        "exchange_order_ok": exchange_order_ok,
        "exchange_order_reason": str(execution_result.get("exchange_order_reason") or ""),
        "exchange_sync_state": exchange_sync_state,
        "last_exchange_error": str(execution_result.get("last_exchange_error") or ""),
    }


def _extract_managed_exchange_fields(execution_result: dict[str, Any] | None) -> dict[str, Any]:
    execution_result = execution_result or {}
    managed_trade_plan = execution_result.get("managed_trade_plan") or execution_result.get("plan") or {}
    entry_payload = execution_result.get("entry_order_payload") or execution_result.get("payload") or {}
    sl_payload = execution_result.get("sl_attached_payload") or []

    return {
        "entry_order_id": str(execution_result.get("entry_order_id") or ""),
        "entry_client_order_id": str(execution_result.get("entry_client_order_id") or ""),
        "entry_order_payload": entry_payload if isinstance(entry_payload, dict) else {},
        "sl_attached_on_entry": bool(execution_result.get("sl_attached_on_entry")),
        "sl_attached_payload": sl_payload if isinstance(sl_payload, list) else [],
        "live_stop_loss_px": float(execution_result.get("live_stop_loss_px") or 0.0),
        "tp_split_ok": bool(execution_result.get("tp_split_ok")),
        "tp_split_reason": str(execution_result.get("tp_split_reason") or ""),
        "tp1_order_id": str(execution_result.get("tp1_order_id") or ""),
        "tp2_order_id": str(execution_result.get("tp2_order_id") or ""),
        "tp1_client_order_id": str(execution_result.get("tp1_client_order_id") or ""),
        "tp2_client_order_id": str(execution_result.get("tp2_client_order_id") or ""),
        "runner_expected_size": str(execution_result.get("runner_expected_size") or ""),
        "runner_requires_trailing_after_tp2": bool(execution_result.get("runner_requires_trailing_after_tp2")),
        "runner_algo_id": str(execution_result.get("runner_algo_id") or ""),
        "runner_algo_client_order_id": str(execution_result.get("runner_algo_client_order_id") or ""),
        "managed_trade_plan": managed_trade_plan if isinstance(managed_trade_plan, dict) else {},
    }




def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_bool(value: Any) -> bool:
    return bool(value)


def _signal_meta(signal: SignalCandidate) -> dict[str, Any]:
    meta = getattr(signal, "meta", {}) or {}
    return meta if isinstance(meta, dict) else {}


def _first_value(*values: Any, default: Any = "") -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        return value
    return default


def _list_value(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _build_decision_trace_id(signal: SignalCandidate, execution_result: dict[str, Any], now: datetime) -> str:
    explicit = _first_value(
        execution_result.get("decision_trace_id"),
        execution_result.get("trace_id"),
        getattr(signal, "decision_trace_id", ""),
        default="",
    )
    if explicit:
        return str(explicit)
    symbol = str(getattr(signal, "symbol", "") or "")
    return f"trade_{now.strftime('%Y%m%dT%H%M%S%f')}_{symbol}_{uuid.uuid4().hex[:8]}"


def _extract_ai_research_fields(
    signal: SignalCandidate,
    execution_result: dict[str, Any],
    execution_path: str,
    now: datetime,
) -> dict[str, Any]:
    """Extract export-only analytics fields for TrackedTrade.

    These fields do not affect scoring, filtering, order placement, or lifecycle logic.
    They only preserve evidence for later AI/research reports.
    """
    meta = _signal_meta(signal)
    candle_gate = execution_result.get("candle_gate") or execution_result.get("candle_reversal_gate") or meta.get("candle_gate") or meta.get("candle_reversal_gate") or {}
    if not isinstance(candle_gate, dict):
        candle_gate = {}

    decision_trace_id = _build_decision_trace_id(signal, execution_result, now)

    entry_pattern = str(_first_value(
        execution_result.get("entry_pattern"),
        candle_gate.get("entry_pattern"),
        candle_gate.get("pattern"),
        meta.get("entry_pattern"),
        meta.get("pattern"),
        default="",
    ) or "")

    reversal_type = str(_first_value(
        execution_result.get("reversal_type"),
        candle_gate.get("reversal_type"),
        candle_gate.get("pattern"),
        meta.get("reversal_type"),
        default=entry_pattern,
    ) or "")

    bullish_reversal = _safe_bool(_first_value(
        candle_gate.get("bullish_reversal_detected"),
        meta.get("bullish_reversal_detected"),
        default=False,
    ))
    bearish_reversal = _safe_bool(_first_value(
        candle_gate.get("bearish_reversal_detected"),
        meta.get("bearish_reversal_detected"),
        default=False,
    ))

    return {
        "decision_trace_id": decision_trace_id,
        "strategy_version": str(_first_value(execution_result.get("strategy_version"), meta.get("strategy_version"), default="") or ""),
        "config_hash": str(_first_value(execution_result.get("config_hash"), meta.get("config_hash"), default="") or ""),
        "entry_reason": str(_first_value(execution_result.get("entry_reason"), execution_result.get("reason"), meta.get("entry_reason"), default="") or ""),
        "acceptance_path": str(_first_value(execution_result.get("acceptance_path"), execution_result.get("path"), execution_path, default="") or ""),
        "risk_mode": str(_first_value(execution_result.get("risk_mode"), meta.get("risk_mode"), getattr(signal, "market_mode", ""), default="") or ""),

        "entry_pattern": entry_pattern,
        "reversal_detected": bool(bullish_reversal or bearish_reversal),
        "reversal_type": reversal_type,
        "wick_ratio": _safe_float(_first_value(candle_gate.get("wick_ratio"), meta.get("wick_ratio"), default=0.0)),
        "body_ratio": _safe_float(_first_value(candle_gate.get("body_ratio"), meta.get("body_ratio"), default=0.0)),
        "candle_strength": _safe_float(_first_value(candle_gate.get("candle_strength"), candle_gate.get("reversal_strength"), meta.get("candle_strength"), default=0.0)),
        "last_3_candles": _list_value(_first_value(candle_gate.get("last_3_candles"), meta.get("last_3_candles"), default=[])),

        "volume_spike_ratio": _safe_float(_first_value(meta.get("volume_spike_ratio"), meta.get("vol_ratio"), meta.get("volume_ratio"), default=0.0)),
        "spread_pct": _safe_float(_first_value(execution_result.get("spread_pct"), meta.get("spread_pct"), default=0.0)),
        "slippage_pct": _safe_float(_first_value(execution_result.get("slippage_pct"), meta.get("slippage_pct"), default=0.0)),
        "distance_from_vwap_pct": _safe_float(_first_value(meta.get("distance_from_vwap_pct"), meta.get("dist_vwap"), default=0.0)),
        "distance_from_ema20_pct": _safe_float(_first_value(meta.get("distance_from_ema20_pct"), meta.get("dist_ema20"), meta.get("dist_ma"), default=0.0)),
    }


def _extract_position_sizing_fields(
    execution_result: dict[str, Any],
    execution_trade: bool,
) -> dict[str, Any]:
    """Extract per-trade margin/notional fields for accurate wallet reports.

    This is reporting/persistence metadata only. It does not change position
    sizing, order placement, TP/SL logic, or risk decisions.
    """
    margin = _safe_float(
        _first_value(
            execution_result.get("used_margin_usdt"),
            execution_result.get("simulation_margin_usdt"),
            execution_result.get("margin_usdt"),
            execution_result.get("allocated_margin_usdt"),
            execution_result.get("position_margin_usdt"),
            execution_result.get("margin"),
            default=0.0,
        )
    )

    notional = _safe_float(
        _first_value(
            execution_result.get("position_notional_usdt"),
            execution_result.get("notional_usdt"),
            execution_result.get("position_size_usdt"),
            default=0.0,
        )
    )

    leverage = _safe_float(
        _first_value(
            execution_result.get("effective_leverage"),
            execution_result.get("leverage"),
            default=0.0,
        )
    )

    if notional <= 0 and margin > 0 and leverage > 0:
        notional = round(margin * leverage, 8)

    balance_reference = _safe_float(
        _first_value(
            execution_result.get("simulation_balance_reference"),
            execution_result.get("balance_reference"),
            execution_result.get("reference_balance"),
            default=0.0,
        )
    )

    return {
        "used_margin_usdt": margin if execution_trade else 0.0,
        "simulation_margin_usdt": 0.0 if execution_trade else margin,
        "margin_usdt": margin,
        "allocated_margin_usdt": margin,
        "position_notional_usdt": notional,
        "simulation_balance_reference": balance_reference,
        "effective_leverage": leverage,
    }


def register_trade(
    signal: SignalCandidate,
    execution_result: dict | None = None,
) -> TrackedTrade:
    """
    Register a signal into the correct tracking path.

    Important separation:
    - Every signal keeps its execution-check metadata.
    - Only actually opened execution-path trades remain open.
    - pending_pullback_preview stays preview-only and does not become
      an actually opened execution trade.
    - Rejected / candidate-only / normal-signal-only items are archived
      immediately so they do not refill open reports after deep_clean.
    """
    execution_result = execution_result or {}

    execution_status = str(
        execution_result.get("status") or "normal_signal_only"
    )

    execution_reason = str(
        execution_result.get("reason") or ""
    )

    execution_path = str(
        execution_result.get("path") or ""
    )

    execution_trade = _resolve_execution_trade_flag(
        execution_status
    )

    target_model, tp1_pct, tp2_pct, runner_pct = _target_plan_for(
        signal,
        execution_path,
    )

    now = datetime.now(timezone.utc)

    resolved_entry = _resolve_entry_price(
        signal,
        execution_result,
    )

    tracking_bucket = _resolve_tracking_bucket(
        execution_trade
    )

    lifecycle_fields = (
        _execution_lifecycle_fields(execution_result)
        if execution_trade
        else _preview_only_lifecycle_fields(
            execution_status,
            now,
        )
    )

    managed_exchange_fields = _extract_managed_exchange_fields(execution_result)
    ai_research_fields = _extract_ai_research_fields(
        signal,
        execution_result,
        execution_path,
        now,
    )
    position_sizing_fields = _extract_position_sizing_fields(
        execution_result,
        execution_trade,
    )

    return TrackedTrade(
        trade_id=str(uuid.uuid4()),
        symbol=signal.symbol,

        # =====================================================
        # Actual Filled Entry (preferred)
        # Falls back safely to signal.entry
        # =====================================================
        entry=resolved_entry,

        sl=signal.sl,
        tp1=signal.tp1,
        tp2=signal.tp2,

        setup_type=signal.setup_type,
        market_mode=signal.market_mode,
        score=signal.score,
        execution_setup_tags=list(signal.execution_setup_tags),
        warnings=list(signal.warnings),

        trade_source="execution" if execution_trade else "normal",
        tracking_bucket=tracking_bucket,
        execution_checked=bool(execution_result),
        execution_status=execution_status,
        execution_reason=execution_reason,
        execution_path=execution_path,
        execution_trade=execution_trade,

        target_model=target_model,
        tp1_close_pct=tp1_pct,
        tp2_close_pct=tp2_pct,
        runner_close_pct=runner_pct,

        opened_at=now,
        updated_at=now,
        current_price=resolved_entry,
        highest_price=resolved_entry,

        **managed_exchange_fields,
        **lifecycle_fields,
        **ai_research_fields,
        **position_sizing_fields,
    )
