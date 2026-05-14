from __future__ import annotations

import uuid
from datetime import datetime, timezone

from analysis.models import SignalCandidate
from utils.constants import MODE_RECOVERY_LONG
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


def _target_plan_for(signal: SignalCandidate, execution_path: str) -> tuple[str, float, float, float]:
    if execution_path == "recovery" or signal.market_mode == MODE_RECOVERY_LONG:
        return "recovery_50_25_25", 50.0, 25.0, 25.0
    return "standard_40_40_20", 40.0, 40.0, 20.0


def register_trade(signal: SignalCandidate, execution_result: dict | None = None) -> TrackedTrade:
    """Register a signal into the correct tracking path.

    Important separation:
    - Every normal signal can be tracked normally.
    - Execution check result is stored as metadata.
    - Rejected execution checks never turn the trade into an execution trade.
    - Execution reports/wallet/open-execution only see execution_trade=True.
    """
    execution_result = execution_result or {}
    execution_status = str(execution_result.get("status") or "normal_signal_only")
    execution_reason = str(execution_result.get("reason") or "")
    execution_path = str(execution_result.get("path") or "")
    execution_trade = is_execution_trade_status(execution_status)
    target_model, tp1_pct, tp2_pct, runner_pct = _target_plan_for(signal, execution_path)
    now = datetime.now(timezone.utc)

    return TrackedTrade(
        trade_id=str(uuid.uuid4()),
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
        target_model=target_model,
        tp1_close_pct=tp1_pct,
        tp2_close_pct=tp2_pct,
        runner_close_pct=runner_pct,
        opened_at=now,
        updated_at=now,
        current_price=signal.entry,
        highest_price=signal.entry,
    )
