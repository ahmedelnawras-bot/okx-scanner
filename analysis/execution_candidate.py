"""Execution gate stays downstream from normal signal formation.

This version moves closer to the old reference behavior:
- strict whitelist for execution
- NORMAL_LONG may additionally allow a small local extra whitelist
- BLOCK exceptions stay separate
- Weak Drift is execution-only
- late/danger context blocks execution but not the signal itself
"""
from __future__ import annotations

from analysis.models import SignalCandidate
from utils.constants import MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG, MODE_BLOCK_LONGS

STRICT_WHITELIST = {
    "vwap_reclaim",
    "retest_breakout_confirmed",
    "wave_3",
    "relative_strength_vs_btc",
}
NORMAL_LONG_EXTRA_WHITELIST = {
    "failed_breakdown_trap",
    "higher_low_continuation",
    "support_bounce_confirmed",
    "liquidity_sweep_reclaim",
}
ELITE_TAGS = {"elite", "wave_3", "relative_strength_vs_btc", "retest_breakout_confirmed"}



def has_complete_execution_plan(signal: SignalCandidate) -> bool:
    return all(v and v > 0 for v in (signal.entry, signal.sl, signal.tp1, signal.tp2))



def _is_late_risky_execution_context(signal: SignalCandidate) -> bool:
    joined = "|".join(
        [
            str(signal.setup_type or "").lower(),
            str(signal.meta.get("entry_maturity") or "").lower(),
            str(signal.meta.get("wave_context") or "").lower(),
            str(signal.meta.get("maturity_label") or "").lower(),
        ]
    )
    risky_tokens = (
        "wave_5_late",
        "danger_late",
        "overextended",
        "late_without_mtf",
        "hard_late_entry",
    )
    return any(token in joined for token in risky_tokens)



def _passes_weak_drift_execution_quality(signal: SignalCandidate) -> bool:
    meta = signal.meta or {}
    if signal.market_mode == MODE_BLOCK_LONGS:
        return True
    vol_ratio = float(meta.get("vol_ratio") or 1.0)
    mtf = bool(meta.get("mtf_confirmed"))
    dist_ma = float(meta.get("dist_ma") or 0.0)
    near_res = bool(meta.get("rejection_context") == "near_resistance_warning")
    weak_ctx = signal.market_mode in (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG) and not mtf and vol_ratio < 1.1
    low_momentum = (not mtf) and vol_ratio < 1.05
    soft_chase = dist_ma > 3.2 and vol_ratio < 1.30 and signal.setup_type not in ("wave_3", "retest_breakout_confirmed")
    near_res_weak = near_res and vol_ratio < 1.25 and not mtf
    return not any([weak_ctx, low_momentum, soft_chase, near_res_weak])



def decide_execution_candidate(signal: SignalCandidate, recovery_slots_remaining: int | None = None) -> dict:
    tags = set(signal.execution_setup_tags)
    strict_allowed = bool(tags & STRICT_WHITELIST)
    normal_extra_allowed = bool(tags & NORMAL_LONG_EXTRA_WHITELIST)
    elite_allowed = bool(tags & ELITE_TAGS) or bool(signal.meta.get("is_elite_setup"))
    recovery_allowed = signal.market_mode == MODE_RECOVERY_LONG and "recovery_execution" in tags
    block_exception = signal.market_mode == MODE_BLOCK_LONGS and "block_exception" in tags
    complete_plan = has_complete_execution_plan(signal)
    near_resistance_warning = signal.meta.get("rejection_context") == "near_resistance_warning"
    weak_drift_passed = _passes_weak_drift_execution_quality(signal)
    late_risky = _is_late_risky_execution_context(signal)

    allowed = False
    path = "candidate_only"
    reason = "not_whitelisted"
    pending_pullback = signal.entry_timing == "pullback" and signal.score < 7.3

    if late_risky:
        return {
            "allowed": False,
            "path": "blocked",
            "reason": "late_risky_execution_context",
            "complete_plan": complete_plan,
            "strict_allowed": strict_allowed,
            "normal_extra_allowed": normal_extra_allowed,
            "elite_allowed": elite_allowed,
            "recovery_allowed": recovery_allowed,
            "pending_pullback": pending_pullback,
            "near_resistance_warning": near_resistance_warning,
            "weak_drift_passed": weak_drift_passed,
        }

    if signal.market_mode == MODE_NORMAL_LONG and complete_plan and weak_drift_passed and (strict_allowed or normal_extra_allowed):
        allowed, path, reason = True, "whitelist", "normal_whitelist_pass"
        if normal_extra_allowed and not strict_allowed:
            reason = "normal_extra_whitelist_pass"
        if near_resistance_warning and signal.score < 7.2:
            pending_pullback = True
            reason = "pullback_first_instead_of_reject"
    elif signal.market_mode == MODE_STRONG_LONG_ONLY and complete_plan and weak_drift_passed and (elite_allowed or strict_allowed) and signal.score >= 7.5:
        allowed, path, reason = True, "elite_or_whitelist", "strong_execution_pass"
        if near_resistance_warning and signal.score < 7.8:
            pending_pullback = True
            reason = "strong_pullback_first"
    elif recovery_allowed and complete_plan and weak_drift_passed:
        if recovery_slots_remaining is not None and recovery_slots_remaining <= 0:
            return {
                "allowed": False,
                "path": "recovery",
                "reason": "recovery_cycle_full",
                "complete_plan": complete_plan,
                "strict_allowed": strict_allowed,
                "normal_extra_allowed": normal_extra_allowed,
                "recovery_allowed": True,
                "pending_pullback": pending_pullback,
                "near_resistance_warning": near_resistance_warning,
                "weak_drift_passed": weak_drift_passed,
            }
        allowed, path, reason = True, "recovery", "recovery_execution_pass"
    elif block_exception and complete_plan:
        allowed, path, reason = True, "block_exception", "block_exception_pass"
    elif not weak_drift_passed:
        path, reason = "blocked", "weak_drift_execution_block"

    return {
        "allowed": allowed,
        "path": path,
        "reason": reason,
        "complete_plan": complete_plan,
        "strict_allowed": strict_allowed,
        "normal_extra_allowed": normal_extra_allowed,
        "elite_allowed": elite_allowed,
        "recovery_allowed": recovery_allowed,
        "pending_pullback": pending_pullback,
        "near_resistance_warning": near_resistance_warning,
        "weak_drift_passed": weak_drift_passed,
    }
