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
    "pending_pullback_preview",
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
    )
