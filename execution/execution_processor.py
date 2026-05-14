from __future__ import annotations

from analysis.models import SignalCandidate
from analysis.execution_candidate import decide_execution_candidate
from utils.constants import MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE, MAX_RECOVERY_TRADES_PER_CYCLE
from .risk_manager import evaluate_execution_risk
from .order_builder import build_preview_order


def process_trade_candidate(
    signal: SignalCandidate,
    current_open_positions: int = 0,
    max_open_positions: int = 10,
    min_execution_score: float = 6.6,
    recovery_slots_remaining: int | None = None,
    block_open_positions: int = 0,
    max_block_positions: int = MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE,
    recovery_open_positions: int = 0,
    max_recovery_positions: int = MAX_RECOVERY_TRADES_PER_CYCLE,
) -> dict:
    gate = decide_execution_candidate(signal, recovery_slots_remaining=recovery_slots_remaining)
    if not gate["allowed"]:
        status = "candidate_only"
        if gate["reason"] in {"late_risky_execution_context", "weak_drift_execution_block", "recovery_quality_not_confirmed"}:
            status = "rejected_quality"
        if gate["reason"] in {"recovery_cycle_full"}:
            status = "rejected_limit"
        return {
            "status": status,
            "reason": gate["reason"],
            "path": gate["path"],
            "gate": gate,
        }

    path = str(gate.get("path") or "")
    if path == "block_exception":
        risk = evaluate_execution_risk(
            signal.score,
            max_open_positions=max_block_positions,
            current_open_positions=block_open_positions,
            min_execution_score=min_execution_score,
        )
        slot_scope = "block_exception"
    elif path == "recovery":
        risk = evaluate_execution_risk(
            signal.score,
            max_open_positions=max_recovery_positions,
            current_open_positions=recovery_open_positions,
            min_execution_score=min_execution_score,
        )
        slot_scope = "recovery"
    else:
        risk = evaluate_execution_risk(
            signal.score,
            max_open_positions=max_open_positions,
            current_open_positions=current_open_positions,
            min_execution_score=min_execution_score,
        )
        slot_scope = "general"

    if not risk["allowed"]:
        return {
            "status": "rejected_limit" if risk["reason"] == "max_positions_reached" else "rejected_risk",
            "reason": risk["reason"],
            "path": gate["path"],
            "slot_scope": slot_scope,
            "slots": risk["slots"],
            "gate": gate,
        }

    status = "pending_pullback_preview" if gate["pending_pullback"] else "accepted_preview"
    return {
        "status": status,
        "reason": gate["reason"],
        "path": gate["path"],
        "slot_scope": slot_scope,
        "order": build_preview_order(signal),
        "slots": risk["slots"],
        "gate": gate,
    }
