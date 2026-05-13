from __future__ import annotations

from analysis.models import SignalCandidate
from .models import TrackedTrade


_EXECUTION_OPEN_STATUSES = {
    "accepted_preview",
    "pending_pullback_preview",
    "executed",
    "open",
    "tp1",
    "tp2",
    "trailing",
}


def is_execution_trade_status(status: str | None) -> bool:
    return str(status or "") in _EXECUTION_OPEN_STATUSES


def register_trade(signal: SignalCandidate, execution_result: dict | None = None) -> TrackedTrade:
    """Register a signal into the correct tracking path.

    v124 important separation:
    - Every normal signal is tracked as normal.
    - Execution check result is stored as metadata.
    - Rejected execution checks never turn the trade into an execution trade.
    - Execution reports/wallet/open-execution only see execution_trade=True.
    """
    execution_result = execution_result or {}
    execution_status = str(execution_result.get("status") or "normal_signal_only")
    execution_reason = str(execution_result.get("reason") or "")
    execution_path = str(execution_result.get("path") or "")
    execution_trade = is_execution_trade_status(execution_status)

    return TrackedTrade(
        symbol=signal.symbol,
        entry=signal.entry,
        sl=signal.sl,
        tp1=signal.tp1,
        tp2=signal.tp2,
        setup_type=signal.setup_type,
        market_mode=signal.market_mode,
        score=signal.score,
        execution_setup_tags=list(signal.execution_setup_tags),
        warnings=list(signal.warnings),
        trade_source="execution" if execution_trade else "normal",
        tracking_bucket="execution" if execution_trade else "normal",
        execution_checked=bool(execution_result),
        execution_status=execution_status,
        execution_reason=execution_reason,
        execution_path=execution_path,
        execution_trade=execution_trade,
        current_price=signal.entry,
    )
