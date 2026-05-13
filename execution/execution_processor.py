from __future__ import annotations

from analysis.models import SignalCandidate
from analysis.execution_candidate import decide_execution_candidate
from .risk_manager import evaluate_execution_risk
from .order_builder import build_preview_order



def process_trade_candidate(
    signal: SignalCandidate,
    current_open_positions: int = 0,
    max_open_positions: int = 7,
    min_execution_score: float = 6.6,
    recovery_slots_remaining: int | None = None,
) -> dict:
    gate = decide_execution_candidate(signal, recovery_slots_remaining=recovery_slots_remaining)
    if not gate["allowed"]:
        status = "candidate_only"
        if gate["reason"] in {"late_risky_execution_context", "weak_drift_execution_block"}:
            status = "rejected_quality"
        return {
            "status": status,
            "reason": gate["reason"],
            "path": gate["path"],
            "gate": gate,
        }

    risk = evaluate_execution_risk(
        signal.score,
        max_open_positions=max_open_positions,
        current_open_positions=current_open_positions,
        min_execution_score=min_execution_score,
    )
    if not risk["allowed"]:
        return {
            "status": "rejected_limit" if risk["reason"] == "max_positions_reached" else "rejected_risk",
            "reason": risk["reason"],
            "path": gate["path"],
            "slots": risk["slots"],
            "gate": gate,
        }

    status = "pending_pullback_preview" if gate["pending_pullback"] else "accepted_preview"
    return {
        "status": status,
        "reason": gate["reason"],
        "path": gate["path"],
        "order": build_preview_order(signal),
        "slots": risk["slots"],
        "gate": gate,
    }
