from __future__ import annotations


def evaluate_execution_risk(score: float, max_open_positions: int, current_open_positions: int, min_execution_score: float) -> dict:
    remaining = max(0, max_open_positions - current_open_positions)
    if current_open_positions >= max_open_positions:
        return {
            "allowed": False,
            "reason": "max_positions_reached",
            "slots": {"allowed": max_open_positions, "counted": current_open_positions, "remaining": remaining},
        }
    if score < min_execution_score:
        return {
            "allowed": False,
            "reason": "score_too_low",
            "slots": {"allowed": max_open_positions, "counted": current_open_positions, "remaining": remaining},
        }
    return {
        "allowed": True,
        "reason": "risk_pass",
        "slots": {"allowed": max_open_positions, "counted": current_open_positions, "remaining": remaining},
    }
