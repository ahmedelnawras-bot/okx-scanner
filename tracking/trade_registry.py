from __future__ import annotations

import uuid
from datetime import datetime, timezone

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


def register_trade(
    signal: SignalCandidate,
    execution_result: dict | None = None,
) -> TrackedTrade:
    """
    Register a signal into the correct tracking path.

    Important separation:
    - Every normal signal can be tracked normally.
    - Execution check result is stored as metadata.
    - Rejected execution checks never turn the trade into an execution trade.
    - pending_pullback_preview stays preview-only and does not become
      an actually opened execution trade.
    - Execution reports/wallet/open-execution only see execution_trade=True.
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
    )
