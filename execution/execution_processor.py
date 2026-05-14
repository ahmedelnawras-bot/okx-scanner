from __future__ import annotations

from analysis.models import SignalCandidate
from analysis.execution_candidate import decide_execution_candidate
from utils.constants import MAX_BLOCK_EXCEPTION_TRADES_PER_CYCLE, MAX_RECOVERY_TRADES_PER_CYCLE
from .risk_manager import evaluate_execution_risk
from .order_builder import build_preview_order


def _effective_min_execution_score(signal: SignalCandidate, default_min: float) -> float:
    """v135 aggressive execution threshold: 6.5 normally, 6.2 only for clean RS/volume entries."""
    meta = signal.meta or {}
    tags = {str(t).lower() for t in (signal.execution_setup_tags or [])}
    tags.update(str(t).lower() for t in (meta.get("pair_tags", []) or []))
    score = float(signal.score or 0.0)
    vol_ratio = float(meta.get("vol_ratio") or 1.0)
    clean_entry = str(meta.get("entry_maturity") or "").lower() in {"healthy", "pullback_first"}
    rs = bool({"relative_strength_vs_btc", "rs_btc"} & tags)
    has_volume = vol_ratio >= 1.15
    strong_setup = bool({"wave_3", "retest_breakout_confirmed", "vwap_reclaim"} & tags)
    if score >= 6.2 and rs and has_volume and clean_entry and strong_setup:
        return min(float(default_min or 6.5), 6.2)
    return float(default_min or 6.5)


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
    effective_min_score = _effective_min_execution_score(signal, min_execution_score)
    if path == "block_exception":
        risk = evaluate_execution_risk(
            signal.score,
            max_open_positions=max_block_positions,
            current_open_positions=block_open_positions,
            min_execution_score=effective_min_score,
        )
        slot_scope = "block_exception"
    elif path == "recovery":
        risk = evaluate_execution_risk(
            signal.score,
            max_open_positions=max_recovery_positions,
            current_open_positions=recovery_open_positions,
            min_execution_score=effective_min_score,
        )
        slot_scope = "recovery"
    else:
        risk = evaluate_execution_risk(
            signal.score,
            max_open_positions=max_open_positions,
            current_open_positions=current_open_positions,
            min_execution_score=effective_min_score,
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
