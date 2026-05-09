"""
Long execution gate for OKX scanner.

This module decides whether an already accepted LONG signal may enter the
execution preview layer. It does not score signals, does not build TP/SL, and
must not block normal Telegram/tracking alerts.

Design:
- NORMAL_LONG keeps the existing execution behaviour passed from main.py.
- STRONG_LONG_ONLY allows execution through either the existing whitelist path
  or a strict Elite Opportunity path.
- BLOCK_LONGS keeps the existing block-exception behaviour passed from main.py.
"""

MODE_NORMAL_LONG = "NORMAL_LONG"
MODE_STRONG_LONG_ONLY = "STRONG_LONG_ONLY"
MODE_BLOCK_LONGS = "BLOCK_LONGS"
MODE_RECOVERY_LONG = "RECOVERY_LONG"

ELITE_EXECUTION_TAG = "elite_long_opportunity"
ELITE_PASS_SCORE = 7.5
ELITE_HIGH_CONVICTION_SCORE = 8.5


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _norm_tag(value):
    try:
        tag = str(value or "").strip().lower()
        tag = tag.replace(" ", "_").replace("-", "_")
        while "__" in tag:
            tag = tag.replace("__", "_")
        return tag.strip("_| ")
    except Exception:
        return ""


def _listify(value):
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return str(value).replace(",", "|").replace(";", "|").split("|")


def _collect_candidate_tags(candidate):
    tags = set()
    data = candidate or {}
    diagnostics = data.get("diagnostics", {}) or {}
    keys = (
        "setup_type", "setup_type_base", "primary_extra_setup", "extra_setup",
        "extra_setup_name", "extra_setup_names", "extra_setups",
        "context", "context_setup", "context_setups", "wave_label",
        "wave_estimate", "execution_setup_tags",
    )
    for source in (data, diagnostics):
        for key in keys:
            for item in _listify(source.get(key)):
                tag = _norm_tag(item)
                if tag:
                    tags.add(tag)
    if _as_bool(data.get("relative_strength_vs_btc")) or _as_bool(diagnostics.get("relative_strength_vs_btc")):
        tags.add("relative_strength_vs_btc")
    return tags


def _entry_text(candidate):
    data = candidate or {}
    parts = [
        data.get("entry_timing", ""),
        data.get("entry_maturity", ""),
        data.get("wave_label", ""),
        data.get("fib_position", ""),
    ]
    return "|".join(str(p or "") for p in parts).lower()


def _has_late_chase(candidate):
    text = _entry_text(candidate)
    return any(token in text for token in (
        "hard_late", "danger", "overextended", "امتداد سعري",
        "متأخر جدًا", "نهاية موجة", "موجة خامسة", "مطاردة حركة",
    ))


def _has_soft_late(candidate):
    text = _entry_text(candidate)
    return any(token in text for token in ("late", "متأخر", "امتداد"))


def _has_resistance_warning(candidate):
    value = (candidate or {}).get("resistance_warning")
    if isinstance(value, bool):
        return value
    return bool(str(value or "").strip())


def evaluate_elite_long_opportunity(candidate):
    """Return a strict but compact Elite score for STRONG_LONG_ONLY execution.

    Elite is not a signal exception. main.py must call this only after the
    normal signal was accepted and the execution plan is complete.
    """
    data = candidate or {}
    tags = _collect_candidate_tags(data)

    score = _safe_float(data.get("effective_score", data.get("score", 0.0)), 0.0)
    vol_ratio = _safe_float(data.get("vol_ratio", 0.0), 0.0)
    dist_ma = _safe_float(data.get("dist_ma", 0.0), 0.0)
    vwap_distance = abs(_safe_float(data.get("vwap_distance", 0.0), 0.0))
    mtf_confirmed = _as_bool(data.get("mtf_confirmed"))
    breakout = _as_bool(data.get("breakout"))
    pre_breakout = _as_bool(data.get("pre_breakout"))
    gaining_strength = _as_bool(data.get("gaining_strength"))
    breakout_quality = str(data.get("breakout_quality", "") or "").strip().lower()
    market_state = str(data.get("market_state", "") or "").lower()
    market_state_label = str(data.get("market_state_label", "") or "").lower()
    market_bias_label = str(data.get("market_bias_label", "") or "").lower()
    btc_mode = str(data.get("btc_mode", "") or "")
    alt_mode = str(data.get("alt_mode", "") or "")

    reasons = []
    warnings = []
    elite_score = 0.0

    # Kill switches: keep Elite from becoming a backdoor for bad entries.
    if score < 7.5:
        return {"passed": False, "score": 0.0, "level": "none", "reasons": [], "warnings": ["score_below_7_5"]}
    if _has_late_chase(data):
        return {"passed": False, "score": 0.0, "level": "none", "reasons": [], "warnings": ["late_chase_or_danger"]}
    if _has_resistance_warning(data) and vol_ratio < 1.45 and not mtf_confirmed:
        return {"passed": False, "score": 0.0, "level": "none", "reasons": [], "warnings": ["near_resistance_without_force"]}
    if "risk_off" in market_state or "هابط" in btc_mode:
        return {"passed": False, "score": 0.0, "level": "none", "reasons": [], "warnings": ["market_context_against_elite"]}

    # 1) Relative Strength — up to 3 points.
    rel_strength = (
        _as_bool(data.get("relative_strength_vs_btc"))
        or "relative_strength_vs_btc" in tags
        or _safe_float(data.get("relative_strength_short", 0.0), 0.0) > 0
        or _safe_float(data.get("relative_strength_24", 0.0), 0.0) > 0
    )
    top_momentum = "top_momentum" in tags or "strong_momentum" in tags or gaining_strength
    if rel_strength and top_momentum:
        elite_score += 3.0
        reasons.append("relative_strength_leader")
    elif rel_strength:
        elite_score += 2.2
        reasons.append("relative_strength")
    elif top_momentum:
        elite_score += 1.3
        reasons.append("top_momentum")
    else:
        warnings.append("no_clear_relative_strength")

    # 2) Clean Entry — up to 2 points.
    clean_entry_tags = {
        "vwap_reclaim", "retest_breakout_confirmed", "higher_low_continuation",
        "support_bounce_confirmed", "failed_breakdown_trap", "liquidity_sweep_reclaim",
        "compression_breakout", "pullback_continuation", "golden_pullback",
    }
    clean_entry = bool(tags & clean_entry_tags) or (vwap_distance <= 2.0 and dist_ma <= 3.2 and not _has_soft_late(data))
    if clean_entry and not _has_soft_late(data):
        elite_score += 2.0
        reasons.append("clean_entry")
    elif clean_entry:
        elite_score += 1.1
        reasons.append("clean_entry_with_minor_late_warning")
    else:
        warnings.append("entry_not_clean_enough")

    # 3) Volume Quality — up to 1.5 points.
    if vol_ratio >= 1.8:
        elite_score += 1.5
        reasons.append("strong_volume")
    elif vol_ratio >= 1.45:
        elite_score += 1.2
        reasons.append("good_volume")
    elif vol_ratio >= 1.20:
        elite_score += 0.8
        reasons.append("acceptable_volume")
    else:
        warnings.append("volume_not_elite")

    # 4) Structure — up to 1.5 points.
    structure_tags = {
        "wave_3", "breakout", "pre_breakout", "retest_breakout_confirmed",
        "higher_low_continuation", "compression_breakout", "vwap_reclaim",
    }
    if breakout_quality == "strong" or (breakout and mtf_confirmed):
        elite_score += 1.5
        reasons.append("strong_structure")
    elif breakout or pre_breakout or bool(tags & structure_tags):
        elite_score += 1.0
        reasons.append("clear_structure")
    else:
        warnings.append("structure_not_clear")

    # 5) Room to Move — up to 1 point.
    if not _has_resistance_warning(data):
        elite_score += 1.0
        reasons.append("room_to_move")
    elif vol_ratio >= 1.60 and mtf_confirmed:
        elite_score += 0.5
        reasons.append("resistance_warning_but_forceful")
    else:
        warnings.append("room_to_move_limited")

    # 6) Market Compatibility — up to 1 point.
    weak_but_not_bad = (
        "risk_off" not in market_state
        and "ضعيف جدًا" not in market_state_label
        and "ضعيف جدًا" not in market_bias_label
        and "🔴" not in alt_mode
    )
    if mtf_confirmed and weak_but_not_bad:
        elite_score += 1.0
        reasons.append("market_compatible")
    elif weak_but_not_bad:
        elite_score += 0.6
        reasons.append("market_not_against")
    else:
        warnings.append("market_compatibility_weak")

    elite_score = round(elite_score, 2)
    passed = elite_score >= ELITE_PASS_SCORE
    level = "high_conviction_elite" if elite_score >= ELITE_HIGH_CONVICTION_SCORE else ("elite" if passed else "none")
    return {
        "passed": passed,
        "score": elite_score,
        "level": level,
        "reasons": reasons,
        "warnings": warnings,
        "tag": ELITE_EXECUTION_TAG if passed else "",
    }


def _stamp_elite(candidate, elite_result):
    if not isinstance(candidate, dict) or not elite_result.get("passed"):
        return candidate
    candidate["elite_execution_passed"] = True
    candidate["elite_execution_score"] = elite_result.get("score", 0.0)
    candidate["elite_execution_level"] = elite_result.get("level", "elite")
    candidate["elite_execution_reasons"] = elite_result.get("reasons", [])
    candidate["elite_execution_warnings"] = elite_result.get("warnings", [])

    raw_tags = candidate.get("execution_setup_tags") or []
    if isinstance(raw_tags, str):
        raw_tags = raw_tags.replace(",", "|").split("|")
    if not isinstance(raw_tags, list):
        raw_tags = list(raw_tags) if isinstance(raw_tags, (tuple, set)) else []
    if ELITE_EXECUTION_TAG not in {_norm_tag(t) for t in raw_tags}:
        raw_tags.append(ELITE_EXECUTION_TAG)
    candidate["execution_setup_tags"] = raw_tags
    return candidate


def decide_long_execution_gate(
    candidate,
    market_mode,
    base_execution_allowed=False,
    strict_setup_allowed=False,
    block_mode_allowed=False,
    has_complete_plan=False,
    weak_drift_passed=True,
    mutate=False,
):
    """Decide whether an accepted long signal may enter execution preview."""
    mode = str(market_mode or MODE_NORMAL_LONG).strip().upper() or MODE_NORMAL_LONG
    data = candidate or {}

    result = {
        "allowed": False,
        "path": "blocked",
        "reason": "execution_gate_blocked",
        "elite": {"passed": False, "score": 0.0, "level": "none", "reasons": [], "warnings": []},
    }

    if not has_complete_plan:
        result["reason"] = "missing_or_invalid_entry_sl_tp"
        return result
    if not weak_drift_passed:
        result["reason"] = "weak_drift_execution_quality_failed"
        return result

    # Keep NORMAL exactly as main.py previously decided it.
    if mode == MODE_NORMAL_LONG:
        result.update({
            "allowed": bool(base_execution_allowed),
            "path": "normal" if base_execution_allowed else "blocked",
            "reason": "normal_long_execution_allowed" if base_execution_allowed else "normal_long_base_execution_rejected",
        })
        return result

    # Keep existing block exception behaviour passed from main.py.
    if mode == MODE_BLOCK_LONGS:
        allowed = bool(block_mode_allowed and base_execution_allowed)
        result.update({
            "allowed": allowed,
            "path": "block_exception" if allowed else "blocked",
            "reason": "block_longs_execution_exception" if allowed else "block_longs_no_execution",
        })
        return result

    if mode == MODE_STRONG_LONG_ONLY:
        if strict_setup_allowed:
            result.update({
                "allowed": True,
                "path": "whitelist",
                "reason": "strong_long_whitelist_allowed",
            })
            return result

        elite = evaluate_elite_long_opportunity(data)
        result["elite"] = elite
        if elite.get("passed"):
            if mutate:
                _stamp_elite(data, elite)
            result.update({
                "allowed": True,
                "path": elite.get("level", "elite"),
                "reason": "strong_long_elite_allowed",
            })
            return result

        result.update({
            "allowed": False,
            "path": "candidate_only",
            "reason": "strong_long_not_whitelist_or_elite",
        })
        return result

    # Recovery currently keeps base behaviour unless main.py decides otherwise.
    if mode == MODE_RECOVERY_LONG:
        result.update({
            "allowed": bool(base_execution_allowed),
            "path": "recovery" if base_execution_allowed else "blocked",
            "reason": "recovery_long_base_execution_allowed" if base_execution_allowed else "recovery_long_base_execution_rejected",
        })
        return result

    result.update({
        "allowed": bool(base_execution_allowed),
        "path": "base" if base_execution_allowed else "blocked",
        "reason": "base_execution_allowed" if base_execution_allowed else "base_execution_rejected",
    })
    return result
