from __future__ import annotations

from analysis.models import SignalCandidate
from analysis.execution_candidate import decide_execution_candidate
from utils.constants import (
    MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE,
    MAX_RECOVERY_TRADES_PER_CYCLE,
)
from .risk_manager import evaluate_execution_risk
from .order_builder import build_preview_order
from .models import TrackedTrade


def _get_execution_score(signal: SignalCandidate) -> float:
    """
    v128c execution architecture
    ───────────────────────────
    IMPORTANT:
    execution/risk MUST use boost_score
    NOT display_score.

    signal.score = UI display only.
    """

    meta = signal.meta or {}

    boost_score = meta.get("boost_score")

    if boost_score is not None:

        try:
            return float(boost_score)

        except Exception:
            pass

    # emergency fallback only
    return float(signal.score)


def process_trade_candidate(
    signal: SignalCandidate,
    open_trades: list[TrackedTrade] | None = None,
    current_open_positions: int = 0,
    max_open_positions: int = 10,
    min_execution_score: float = 6.6,
    recovery_slots_remaining: int | None = None,
    block_open_positions: int = 0,
    max_block_positions: int = MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE,
    recovery_open_positions: int = 0,
    max_recovery_positions: int = MAX_RECOVERY_TRADES_PER_CYCLE,
) -> dict:

    # ─────────────────────────────────────────
    # Execution intelligence gate
    # ─────────────────────────────────────────
    gate = decide_execution_candidate(
        signal,
        recovery_slots_remaining=recovery_slots_remaining,
    )

    # ─────────────────────────────────────────
    # Candidate rejected by execution gate
    # ─────────────────────────────────────────
    if not gate["allowed"]:

        status = "candidate_only"

        if gate["reason"] in {
            "late_risky_execution_context",
            "weak_drift_execution_block",
            "recovery_quality_not_confirmed",
        }:
            status = "rejected_quality"

        if gate["reason"] in {
            "recovery_cycle_full",
        }:
            status = "rejected_limit"

        return {
            "status": status,
            "reason": gate["reason"],
            "path": gate["path"],
            "gate": gate,

            "nour_filter_name": gate.get(
                "nour_filter_name"
            ),

            "nour_filter_passed": gate.get(
                "nour_filter_passed"
            ),

            "nour_filter_reason": gate.get(
                "nour_filter_reason"
            ),
        }

    # ─────────────────────────────────────────
    # Same symbol protection
    # ─────────────────────────────────────────
    open_trades = open_trades or []

    for trade in open_trades:

        if (
            trade.symbol == signal.symbol
            and not trade.same_symbol_block_exempt
        ):

            return {
                "status": "rejected_same_symbol",
                "reason": "same_symbol_active_trade",
                "existing_trade_status": trade.status,
            }

    # ─────────────────────────────────────────
    # IMPORTANT:
    # execution layer uses boost_score
    # NOT display_score
    # ─────────────────────────────────────────
    execution_score = _get_execution_score(signal)

    path = str(gate.get("path") or "")

    # ─────────────────────────────────────────
    # BLOCK exception routing
    # ─────────────────────────────────────────
    if path == "block_exception":

        risk = evaluate_execution_risk(
            execution_score=execution_score,
            max_open_positions=max_block_positions,
            current_open_positions=block_open_positions,
            min_execution_score=min_execution_score,
        )

        slot_scope = "block_exception"

    # ─────────────────────────────────────────
    # RECOVERY routing
    # ─────────────────────────────────────────
    elif path == "recovery":

        risk = evaluate_execution_risk(
            execution_score=execution_score,
            max_open_positions=max_recovery_positions,
            current_open_positions=recovery_open_positions,
            min_execution_score=min_execution_score,
        )

        slot_scope = "recovery"

    # ─────────────────────────────────────────
    # NORMAL / STRONG routing
    # ─────────────────────────────────────────
    else:

        risk = evaluate_execution_risk(
            execution_score=execution_score,
            max_open_positions=max_open_positions,
            current_open_positions=current_open_positions,
            min_execution_score=min_execution_score,
        )

        slot_scope = "general"

    # ─────────────────────────────────────────
    # Risk rejected
    # ─────────────────────────────────────────
    if not risk["allowed"]:

        return {

            "status": (
                "rejected_limit"
                if risk["reason"] == "max_positions_reached"
                else "rejected_risk"
            ),

            "reason": risk["reason"],

            "path": gate["path"],

            "slot_scope": slot_scope,

            "slots": risk["slots"],

            "gate": gate,

            "nour_filter_name": gate.get(
                "nour_filter_name"
            ),

            "nour_filter_passed": gate.get(
                "nour_filter_passed"
            ),

            "nour_filter_reason": gate.get(
                "nour_filter_reason"
            ),

            # debug visibility
            "execution_score": round(
                execution_score,
                2,
            ),

            "display_score": round(
                float(signal.score),
                2,
            ),
        }

    # ─────────────────────────────────────────
    # Final acceptance state
    # ─────────────────────────────────────────
    status = (
        "pending_pullback_preview"
        if gate["pending_pullback"]
        else "accepted_preview"
    )

    return {

        "status": status,

        "reason": gate["reason"],

        "path": gate["path"],

        "slot_scope": slot_scope,

        "order": build_preview_order(signal),

        "slots": risk["slots"],

        "gate": gate,

        "nour_filter_name": gate.get(
            "nour_filter_name"
        ),

        "nour_filter_passed": gate.get(
            "nour_filter_passed"
        ),

        "nour_filter_reason": gate.get(
            "nour_filter_reason"
        ),

        # debug visibility
        "execution_score": round(
            execution_score,
            2,
        ),

        "display_score": round(
            float(signal.score),
            2,
        ),
    }
