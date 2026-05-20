from __future__ import annotations

from analysis.models import SignalCandidate
from analysis.execution_candidate import decide_execution_candidate

from utils.constants import (
    MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE,
    MAX_RECOVERY_TRADES_PER_CYCLE,
)

from .risk_manager import evaluate_execution_risk
from .order_builder import build_preview_order


# ─────────────────────────────────────────
# Same-symbol active protection
# يمنع إعادة الدخول لنفس العملة
# طالما الصفقة القديمة لم تنتهِ بالكامل
#
# يسمح بإعادة الدخول فقط بعد:
# - TP2
# - closed
# - stopped
# ─────────────────────────────────────────
ACTIVE_BLOCKING_STATUSES = {
    "open",
    "tp1_hit",
    "runner_active",
    "breakeven_runner",
    "protected_runner",
    "partial_runner",
}

NON_BLOCKING_FINAL_STATUSES = {
    "closed",
    "stopped",
    "tp2_hit",
    "take_profit_2_hit",
}


def _get_execution_score(signal: SignalCandidate) -> float:
    """
    v129 execution architecture
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


def _trade_reached_tp2(trade) -> bool:
    return bool(
        getattr(trade, "tp2_hit", False)
        or getattr(trade, "take_profit_2_hit", False)
        or getattr(trade, "fully_closed_at_tp2", False)
    )


def _trade_is_terminal(trade) -> bool:
    trade_status = str(
        getattr(trade, "status", "")
    ).lower()

    if bool(getattr(trade, "is_closed", False)):
        return True

    if _trade_reached_tp2(trade):
        return True

    if trade_status in NON_BLOCKING_FINAL_STATUSES:
        return True

    return False


def _same_symbol_trade_is_active(trade, signal: SignalCandidate) -> tuple[bool, str]:
    trade_symbol = getattr(
        trade,
        "symbol",
        None,
    )

    if trade_symbol != signal.symbol:
        return False, ""

    if _trade_is_terminal(trade):
        return False, ""

    trade_status = str(
        getattr(trade, "status", "")
    ).lower()

    if trade_status in ACTIVE_BLOCKING_STATUSES:
        return True, trade_status

    # fallback protection:
    # أي صفقة لنفس العملة ليست terminal
    # تعتبر ما زالت نشطة حتى لو status غير متوقع
    fallback_status = trade_status or "active_unfinished_trade"
    return True, fallback_status


def process_trade_candidate(
    signal: SignalCandidate,
    open_trades: list | None = None,
    current_open_positions: int = 0,
    max_open_positions: int = 10,
    min_execution_score: float = 6.6,
    recovery_slots_remaining: int | None = None,
    block_open_positions: int = 0,
    max_block_positions: int = MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE,
    recovery_open_positions: int = 0,
    max_recovery_positions: int = MAX_RECOVERY_TRADES_PER_CYCLE,
    drawdown_status=None,
) -> dict:

    # ─────────────────────────────────────────
    # Execution intelligence gate
    # IMPORTANT:
    # Nour filter + execution quality
    # must stay inside execution layer
    # NOT scoring architecture
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
            "velocity_instability",
            "expansion_exhaustion",
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
    # يمنع إعادة الدخول لنفس العملة
    # قبل انتهاء الصفقة بالكامل
    #
    # TP2 = trade closed
    # ─────────────────────────────────────────
    open_trades = open_trades or []

    for trade in open_trades:
        is_active, trade_status = _same_symbol_trade_is_active(
            trade,
            signal,
        )

        if is_active:
            return {

                "status": "rejected_same_symbol",

                "reason": (
                    "same_symbol_active_trade"
                ),

                "existing_trade_status": (
                    trade_status
                ),
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
            score=execution_score,
            max_open_positions=max_block_positions,
            current_open_positions=block_open_positions,
            min_execution_score=min_execution_score,
            drawdown_status=drawdown_status,
        )

        slot_scope = "block_exception"

    # ─────────────────────────────────────────
    # RECOVERY routing
    # ─────────────────────────────────────────
    elif path == "recovery":

        risk = evaluate_execution_risk(
            score=execution_score,
            max_open_positions=max_recovery_positions,
            current_open_positions=recovery_open_positions,
            min_execution_score=min_execution_score,
            drawdown_status=drawdown_status,
        )

        slot_scope = "recovery"

    # ─────────────────────────────────────────
    # NORMAL / STRONG routing
    # ─────────────────────────────────────────
    else:

        risk = evaluate_execution_risk(
            score=execution_score,
            max_open_positions=max_open_positions,
            current_open_positions=current_open_positions,
            min_execution_score=min_execution_score,
            drawdown_status=drawdown_status,
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

            "drawdown_level": risk.get(
                "drawdown_level"
            ),

            "drawdown_pct": risk.get(
                "drawdown_pct"
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

        "drawdown_level": risk.get(
            "drawdown_level"
        ),

        "drawdown_pct": risk.get(
            "drawdown_pct"
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
