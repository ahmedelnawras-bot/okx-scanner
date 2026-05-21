from __future__ import annotations

from analysis.models import SignalCandidate
from utils.constants import MODE_RECOVERY_LONG


def _target_plan_for(signal: SignalCandidate, execution_path: str = "") -> tuple[str, float, float, float]:
    """
    Resolve the managed exit split model.

    Recovery path uses 50/25/25.
    Everything else defaults to 40/40/20.
    """
    if execution_path == "recovery" or getattr(signal, "market_mode", "") == MODE_RECOVERY_LONG:
        return "recovery_50_25_25", 50.0, 25.0, 25.0

    return "standard_40_40_20", 40.0, 40.0, 20.0


def build_managed_trade_plan(
    signal: SignalCandidate,
    execution_path: str = "",
) -> dict:
    """
    Build a normalized managed-execution plan that can be passed through
    execution, registry, persistence, reports, and live exchange adapters.
    """
    target_model, tp1_close_pct, tp2_close_pct, runner_close_pct = _target_plan_for(
        signal,
        execution_path=execution_path,
    )

    entry_mode = "pullback_pending" if getattr(signal, "entry_timing", "") == "pullback" else "market"

    return {
        "symbol": signal.symbol,
        "entry": signal.entry,
        "sl": signal.sl,
        "tp1": signal.tp1,
        "tp2": signal.tp2,
        "entry_mode": entry_mode,
        "status": "preview_only",
        "execution_path": execution_path or "",
        "target_model": target_model,
        "tp1_close_pct": tp1_close_pct,
        "tp2_close_pct": tp2_close_pct,
        "runner_close_pct": runner_close_pct,
        "managed_execution": {
            "attach_sl_on_entry": True,
            "place_tp1_partial": True,
            "place_tp2_partial": True,
            "runner_after_tp2": True,
            "runner_requires_trailing_after_tp2": True,
            "runner_requires_block_sl_sync": True,
        },
        "partials": [
            {
                "label": "tp1",
                "target_price": signal.tp1,
                "close_pct": tp1_close_pct,
                "order_type": "reduce_only_tp",
            },
            {
                "label": "tp2",
                "target_price": signal.tp2,
                "close_pct": tp2_close_pct,
                "order_type": "reduce_only_tp",
            },
            {
                "label": "runner",
                "target_price": None,
                "close_pct": runner_close_pct,
                "order_type": "trailing_runner_after_tp2",
            },
        ],
    }


def build_preview_order(
    signal: SignalCandidate,
    execution_path: str = "",
) -> dict:
    """
    Backward-compatible preview builder.

    Previous versions returned only entry/sl/tp1/tp2 preview fields.
    We now return the same core fields plus the managed trade plan metadata
    required for live OKX execution wiring.
    """
    return build_managed_trade_plan(
        signal,
        execution_path=execution_path,
    )
