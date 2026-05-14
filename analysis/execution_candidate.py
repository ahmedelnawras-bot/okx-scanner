"""Execution gate stays downstream from normal signal formation.

v127 restores the old NORMAL_LONG Weak Drift spirit:
- Weak Drift exists in NORMAL_LONG, but it is execution-only.
- Normal Telegram signals are never blocked by Weak Drift.
- NORMAL_LONG can allow detector-approved extra setups when quality is enough.
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

SETUP_WEIGHTS = {
    "vwap_reclaim": 3,
    "retest_breakout_confirmed": 3,
    "liquidity_sweep_reclaim": 2,
    "relative_strength_vs_btc": 2,
    "wave_3": 2,
    "support_bounce_confirmed": 2,
    "failed_breakdown_trap": 2,
    "higher_low_continuation": 2,
}


def _normalize_execution_tag(value) -> str:
    try:
        tag = str(value or "").strip().lower().replace(" ", "_").replace("-", "_")
        while "__" in tag:
            tag = tag.replace("__", "_")
        return tag.strip("_|")
    except Exception:
        return ""


def _signal_tags(signal: SignalCandidate) -> set[str]:
    tags = {_normalize_execution_tag(t) for t in (signal.execution_setup_tags or [])}
    tags.add(_normalize_execution_tag(signal.setup_type))
    for t in (signal.meta or {}).get("pair_tags", []) or []:
        tags.add(_normalize_execution_tag(t))
    return {t for t in tags if t}


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
        "hard_late_entry",
        "متأخر جدًا",
        "موجة خامسة",
    )
    return any(token in joined for token in risky_tokens)


def _has_normal_long_execution_setup(signal: SignalCandidate) -> bool:
    """NORMAL_LONG-only extension copied conceptually from the old code."""
    tags = _signal_tags(signal)
    if tags & NORMAL_LONG_EXTRA_WHITELIST:
        return True
    try:
        if int((signal.meta or {}).get("wave_estimate") or 0) == 3:
            return True
    except Exception:
        pass
    return False


def _setup_weight(signal: SignalCandidate) -> int:
    tags = _signal_tags(signal)
    weight = max((SETUP_WEIGHTS.get(t, 0) for t in tags), default=0)
    try:
        weight = max(weight, int((signal.meta or {}).get("setup_weight") or 0))
    except Exception:
        pass
    return weight


def get_weak_trend_drift_status(signal: SignalCandidate) -> dict:
    """Execution-only weak drift detector.

    Weak Drift means upward drift without enough confirmation. It never blocks
    normal signal registration/reporting; it only controls execution preview/badge.
    """
    result = {"active": False, "reason": "", "details": {}}
    try:
        meta = signal.meta or {}
        mode = signal.market_mode or MODE_NORMAL_LONG
        if mode == MODE_BLOCK_LONGS:
            return result

        score = float(meta.get("effective_score") or signal.score or 0.0)
        vol_ratio = float(meta.get("vol_ratio") or 1.0)
        mtf_confirmed = bool(meta.get("mtf_confirmed"))
        dist_ma = float(meta.get("dist_ma") or 0.0)
        breakout = bool(meta.get("breakout"))
        pre_breakout = bool(meta.get("pre_breakout"))
        resistance_warning = bool(meta.get("resistance_warning") or meta.get("rejection_context") == "near_resistance_warning")
        tags = _signal_tags(signal)
        setup_weight = _setup_weight(signal)

        entry_text = "|".join([
            str(signal.entry_timing or ""),
            str(meta.get("entry_maturity") or ""),
            str(meta.get("wave_context") or ""),
            str(meta.get("fib_position") or ""),
        ]).lower()
        hard_late_or_danger = any(token in entry_text for token in (
            "danger", "danger_late", "hard_late", "overextended", "متأخر جدًا", "موجة خامسة"
        ))

        # Old behavior: NORMAL/MIXED is not weak by itself. Only true risk/weak labels are weak context.
        market_state = str(meta.get("market_state") or "").lower()
        market_state_label = str(meta.get("market_state_label") or "").lower()
        market_bias_label = str(meta.get("market_bias_label") or "").lower()
        btc_mode = str(meta.get("btc_mode") or "")
        alt_mode = str(meta.get("alt_mode") or "")
        weak_market_context = (
            "risk_off" in market_state
            or "btc_leading" in market_state
            or "weak" in market_state_label
            or "ضعيف" in market_state_label
            or "weak" in market_bias_label
            or "ضعيف" in market_bias_label
            or btc_mode in ("🔴 هابط",)
            or alt_mode in ("🔴 ضعيف",)
        )

        low_momentum = (not mtf_confirmed and vol_ratio < 1.05)
        drifting_range = (weak_market_context and vol_ratio < 1.15 and not mtf_confirmed)
        soft_chase = (dist_ma > 3.2 and vol_ratio < 1.30 and not (breakout or pre_breakout))
        near_resistance_without_force = resistance_warning and vol_ratio < 1.25 and not mtf_confirmed

        active = bool(low_momentum or drifting_range or soft_chase or near_resistance_without_force or hard_late_or_danger)
        if not active:
            return result

        reasons: list[str] = []
        if low_momentum:
            reasons.append("low_momentum")
        if drifting_range:
            reasons.append("weak_market_drift")
        if soft_chase:
            reasons.append("soft_chase")
        if near_resistance_without_force:
            reasons.append("near_resistance_without_force")
        if hard_late_or_danger:
            reasons.append("late_or_danger")

        result.update({
            "active": True,
            "reason": ",".join(reasons),
            "details": {
                "mode": mode,
                "score": score,
                "vol_ratio": vol_ratio,
                "dist_ma": dist_ma,
                "mtf_confirmed": mtf_confirmed,
                "setup_weight": setup_weight,
                "tags": sorted(tags),
            },
        })
        return result
    except Exception:
        return result


def _candidate_passes_weak_drift_execution_quality(signal: SignalCandidate) -> bool:
    """Smart Weak Drift gate for execution only.

    Keeps true weak/drift cases blocked, but allows NORMAL_LONG detector-approved
    setups when score/volume/MTF/setup quality is enough.
    """
    drift = get_weak_trend_drift_status(signal)
    if not drift.get("active"):
        return True

    meta = signal.meta or {}
    mode = signal.market_mode or MODE_NORMAL_LONG
    score = float(meta.get("effective_score") or signal.score or 0.0)
    vol_ratio = float(meta.get("vol_ratio") or 1.0)
    mtf_confirmed = bool(meta.get("mtf_confirmed"))
    breakout_quality = str(meta.get("breakout_quality") or "").strip().lower()
    resistance_warning = bool(meta.get("resistance_warning") or meta.get("rejection_context") == "near_resistance_warning")
    setup_weight = _setup_weight(signal)
    tags = _signal_tags(signal)
    strict_whitelist = bool(tags & STRICT_WHITELIST)
    normal_extra = _has_normal_long_execution_setup(signal)
    has_whitelist = strict_whitelist or (mode == MODE_NORMAL_LONG and normal_extra)

    entry_text = "|".join([
        str(signal.entry_timing or ""),
        str(meta.get("entry_maturity") or ""),
        str(meta.get("wave_context") or ""),
    ]).lower()
    hard_late_or_danger = any(token in entry_text for token in (
        "danger", "danger_late", "hard_late", "overextended", "متأخر جدًا", "موجة خامسة"
    ))
    soft_late_warning = any(token in entry_text for token in ("late", "متأخر", "امتداد سعري", "نهاية موجة"))

    if hard_late_or_danger or not has_whitelist:
        return False

    if mode == MODE_NORMAL_LONG:
        if setup_weight >= 3 and score >= 6.5 and mtf_confirmed and vol_ratio >= 1.05:
            return True
        if setup_weight >= 2 and score >= 7.2 and mtf_confirmed and vol_ratio >= 1.10:
            return True
        if setup_weight >= 2 and score >= 7.8 and vol_ratio >= 1.25:
            return True
        if soft_late_warning and setup_weight >= 3 and score >= 6.8 and mtf_confirmed and vol_ratio >= 1.08:
            return True

    if breakout_quality == "strong" and mtf_confirmed and vol_ratio >= 1.10 and score >= 7.2:
        return True
    if setup_weight >= 3 and score >= 7.3 and mtf_confirmed and vol_ratio >= 1.10:
        return True
    if setup_weight >= 3 and score >= 7.7 and vol_ratio >= 1.25:
        return True
    if setup_weight >= 2 and score >= 7.5 and mtf_confirmed and vol_ratio >= 1.15:
        return True

    dynamic_score = 0
    if mtf_confirmed:
        dynamic_score += 2
    if vol_ratio >= 1.50:
        dynamic_score += 3
    elif vol_ratio >= 1.25:
        dynamic_score += 2
    elif vol_ratio >= 1.15:
        dynamic_score += 1
    if breakout_quality == "strong":
        dynamic_score += 2
    elif breakout_quality in ("ok", "good"):
        dynamic_score += 1
    dynamic_score += setup_weight
    if soft_late_warning:
        dynamic_score -= 1
    if resistance_warning:
        dynamic_score -= 1

    return dynamic_score >= 5 and score >= 7.6 and vol_ratio >= 1.15




def _recovery_quality_gate(signal: SignalCandidate) -> tuple[bool, dict]:
    """RECOVERY_LONG-only quality gate.

    Recovery is not a standalone mandatory mode; it is a temporary alternative
    to STRONG during fast rebound. Therefore it should not be blocked by the
    normal Weak Drift gate alone, but it must show real rebound quality.
    """
    meta = signal.meta or {}
    tags = _signal_tags(signal)
    score = float(meta.get("effective_score") or signal.score or 0.0)
    vol_ratio = float(meta.get("vol_ratio") or 1.0)
    mtf_confirmed = bool(meta.get("mtf_confirmed"))
    setup_weight = _setup_weight(signal)
    breakout_quality = str(meta.get("breakout_quality") or "").lower()
    resistance_warning = bool(meta.get("resistance_warning") or meta.get("rejection_context") == "near_resistance_warning")
    bounce_fast = bool(meta.get("bounce_faster_than_btc") or meta.get("recovery_relative_bounce"))

    checks = {
        "rs_vs_btc": "relative_strength_vs_btc" in tags or "rs_btc" in tags,
        "bounce_faster_than_btc": bounce_fast,
        "reclaim_or_mtf": mtf_confirmed or breakout_quality in {"ok", "good", "strong"},
        "micro_break_or_setup": setup_weight >= 2 or bool(tags & {"wave_3", "retest_breakout_confirmed", "support_bounce_confirmed", "liquidity_sweep_reclaim", "higher_low_continuation"}),
        "volume_confirmation": vol_ratio >= 1.12,
        "score_quality": score >= 6.55,
        "rr_not_blocked_by_resistance": not resistance_warning or (score >= 7.2 and vol_ratio >= 1.20),
    }
    points = sum(1 for v in checks.values() if v)

    # Strong leading rebound combo can pass with fewer generic points.
    strong_combo = checks["bounce_faster_than_btc"] and checks["rs_vs_btc"] and checks["reclaim_or_mtf"]
    passed = bool((points >= 4 and checks["rr_not_blocked_by_resistance"]) or (strong_combo and points >= 3))
    return passed, {
        "passed": passed,
        "points": points,
        "checks": checks,
        "score": score,
        "vol_ratio": vol_ratio,
        "bounce_ratio_vs_btc": meta.get("bounce_ratio_vs_btc"),
        "btc_bounce_pct": meta.get("btc_bounce_pct"),
        "symbol_bounce_pct": meta.get("symbol_bounce_pct"),
    }

def decide_execution_candidate(signal: SignalCandidate, recovery_slots_remaining: int | None = None) -> dict:
    tags = _signal_tags(signal)
    strict_allowed = bool(tags & STRICT_WHITELIST)
    normal_extra_allowed = _has_normal_long_execution_setup(signal)
    elite_allowed = bool(tags & ELITE_TAGS) or bool(signal.meta.get("is_elite_setup"))
    recovery_allowed = signal.market_mode == MODE_RECOVERY_LONG and "recovery_execution" in tags
    block_exception = signal.market_mode == MODE_BLOCK_LONGS and "block_exception" in tags
    complete_plan = has_complete_execution_plan(signal)
    near_resistance_warning = bool(signal.meta.get("rejection_context") == "near_resistance_warning" or signal.meta.get("resistance_warning"))
    weak_drift_passed = _candidate_passes_weak_drift_execution_quality(signal)
    recovery_quality_passed, recovery_quality = _recovery_quality_gate(signal) if recovery_allowed else (False, {})
    # Recovery has a softened weak-drift rule: real rebound quality can override
    # normal weak drift, but only in RECOVERY_LONG and only for recovery tags.
    recovery_soft_passed = bool(recovery_allowed and recovery_quality_passed)
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
            "weak_drift": get_weak_trend_drift_status(signal),
            "recovery_quality_passed": recovery_quality_passed,
            "recovery_quality": recovery_quality,
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
    elif recovery_allowed and complete_plan:
        if not recovery_quality_passed:
            path, reason = "recovery", "recovery_quality_not_confirmed"
        elif recovery_slots_remaining is not None and recovery_slots_remaining <= 0:
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
                "weak_drift": get_weak_trend_drift_status(signal),
                "recovery_quality_passed": recovery_quality_passed,
                "recovery_quality": recovery_quality,
            }
        else:
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
        "weak_drift": get_weak_trend_drift_status(signal),
        "recovery_quality_passed": recovery_quality_passed,
        "recovery_quality": recovery_quality,
    }
