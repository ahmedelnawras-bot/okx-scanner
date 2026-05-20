"""
Signal formation preserves old philosophy: momentum, continuation,
reclaim, and rebound can all form normal signals.

Execution-specific strictness stays downstream and never suppresses
the normal signal itself.

v129 FIXED execution intelligence update
──────────────────────────────────────────────────────────────────────────────
FIXES:

1) Nour Filter V2 moved to execution layer philosophy
   - scoring preserved
   - signal formation preserved
   - no upstream suppression

2) Adaptive target engine improved
   - more realistic TP2
   - safer SL behavior
   - bounded execution geometry

3) quality_meta restored
   - dist_ma restored
   - compatibility preserved

IMPORTANT:
- scoring architecture NOT changed
- boost_score still authoritative
- display_score still UI-only
- no future leakage
- no mode architecture changes
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from .models import PairCandidate, SignalCandidate

from utils.constants import (
    MODE_NORMAL_LONG,
    MODE_STRONG_LONG_ONLY,
    MODE_RECOVERY_LONG,
    MODE_BLOCK_LONGS,
)

WHITELIST_SETUPS = {
    "vwap_reclaim",
    "retest_breakout_confirmed",
    "wave_3",
    "relative_strength_vs_btc",
}

ELITE_SETUPS = {
    "retest_breakout_confirmed",
    "wave_3",
    "relative_strength_vs_btc",
}

BLOCK_EXCEPTION_SETUPS = {
    "relative_strength_vs_btc",
    "retest_breakout_confirmed",
}

SMART_SL_MIN_PCT = 1.15
SMART_SL_MAX_PCT = 3.40

SMART_TP1_MIN_RR = 1.15
SMART_TP1_MAX_RR = 2.10

SMART_TP2_MIN_RR = 1.90
SMART_TP2_MAX_RR = 3.20


def _clamp(
    value: float,
    low: float,
    high: float,
) -> float:

    return max(
        float(low),
        min(float(high), float(value)),
    )


def _soft_cap_score(
    boost_score: float,
) -> float:

    score = float(boost_score)

    if score <= 8.5:
        ui_score = score

    elif score <= 10:
        ui_score = 8.5 + (
            (score - 8.5) * 0.55
        )

    elif score <= 12:
        ui_score = 9.33 + (
            (score - 10) * 0.22
        )

    else:
        ui_score = 9.77 + min(
            (score - 12) * 0.05,
            0.22,
        )

    return round(
        min(ui_score, 9.99),
        2,
    )


def _clamp_vol_ratio(
    vol_ratio: float,
) -> float:

    return round(
        _clamp(
            vol_ratio,
            1.00,
            2.20,
        ),
        2,
    )


# ─────────────────────────────────────────
# Execution Stability Intelligence
# IMPORTANT:
# informational only
# NOT hard rejection
# ─────────────────────────────────────────
def _calculate_execution_stability(
    pair: PairCandidate,
    setup_type: str,
    market_mode: str,
) -> dict:

    tags = set(pair.tags or [])

    turnover = float(
        pair.turnover_usdt or 0.0
    )

    change_pct = abs(
        float(pair.change_pct or 0.0)
    )

    stability_score = 1.00
    instability_score = 0.0

    # positive stability
    if "liquid" in tags:
        stability_score += 0.20

    if "major" in tags:
        stability_score += 0.12

    if "rs_btc" in tags:
        stability_score += 0.15

    if turnover >= 5_000_000:
        stability_score += 0.12

    if turnover >= 20_000_000:
        stability_score += 0.15

    if setup_type in ELITE_SETUPS:
        stability_score += 0.18

    # instability
    if change_pct >= 2.5:
        instability_score += 0.25

    if change_pct >= 4.0:
        instability_score += 0.35

    if "rebound" in tags:
        instability_score += 0.10

    if "compression" in tags:
        instability_score += 0.08

    if (
        "momentum" in tags
        and "breakout" not in tags
    ):
        instability_score += 0.10

    if turnover < 800_000:
        instability_score += 0.18

    if turnover < 250_000:
        instability_score += 0.20

    if "near_resistance" in tags:
        instability_score += 0.12

    exhausted_move = bool(
        change_pct >= 4.5
        and "breakout" not in tags
    )

    if exhausted_move:
        instability_score += 0.25

    execution_stability = round(
        max(
            0.0,
            stability_score - instability_score,
        ),
        2,
    )

    # IMPORTANT:
    # no hard rejection here anymore
    nour_passed = (
        execution_stability >= 0.90
        and not exhausted_move
    )

    nour_reason = (
        "stable_execution_context"
        if nour_passed
        else "unstable_execution_context"
    )

    return {

        "execution_stability":
            execution_stability,

        "stability_score":
            round(stability_score, 2),

        "instability_score":
            round(instability_score, 2),

        "nour_filter_passed":
            nour_passed,

        "nour_filter_reason":
            nour_reason,

        "exhausted_move":
            exhausted_move,
    }


# ─────────────────────────────────────────
# Adaptive Target Geometry
# FIXED:
# more realistic TP2
# ─────────────────────────────────────────
def _calculate_adaptive_targets(
    pair: PairCandidate,
    setup_type: str,
    entry_timing: str,
    market_mode: str,
    execution_stability: float,
) -> dict:

    tags = set(pair.tags or [])

    turnover = float(
        pair.turnover_usdt or 0.0
    )

    change_abs = abs(
        float(pair.change_pct or 0.0)
    )

    # base SL
    if entry_timing == "pullback":
        risk_pct = 1.75
    else:
        risk_pct = 1.45

    if market_mode == MODE_RECOVERY_LONG:
        risk_pct += 0.08

    if "rebound" in tags:
        risk_pct += 0.15

    if change_abs >= 2.5:
        risk_pct += 0.20

    if execution_stability >= 1.45:
        risk_pct -= 0.12

    if execution_stability <= 0.95:
        risk_pct += 0.20

    if "major" in tags:
        risk_pct -= 0.08

    if turnover < 500_000:
        risk_pct += 0.15

    risk_pct = round(
        _clamp(
            risk_pct,
            SMART_SL_MIN_PCT,
            SMART_SL_MAX_PCT,
        ),
        4,
    )

    # TP1
    tp1_rr = 1.40

    if execution_stability >= 1.30:
        tp1_rr += 0.20

    if execution_stability >= 1.55:
        tp1_rr += 0.15

    if "breakout" in tags:
        tp1_rr += 0.10

    if "rs_btc" in tags:
        tp1_rr += 0.10

    if "rebound" in tags:
        tp1_rr -= 0.12

    tp1_rr = round(
        _clamp(
            tp1_rr,
            SMART_TP1_MIN_RR,
            SMART_TP1_MAX_RR,
        ),
        2,
    )

    # TP2
    tp2_rr = 2.25

    if execution_stability >= 1.40:
        tp2_rr += 0.25

    if execution_stability >= 1.65:
        tp2_rr += 0.20

    if setup_type == "wave_3":
        tp2_rr += 0.15

    if "rebound" in tags:
        tp2_rr -= 0.20

    if turnover < 500_000:
        tp2_rr -= 0.15

    if change_abs >= 4.0:
        tp2_rr -= 0.20

    tp2_rr = round(
        _clamp(
            tp2_rr,
            SMART_TP2_MIN_RR,
            SMART_TP2_MAX_RR,
        ),
        2,
    )

    return {

        "model":
            "adaptive_execution_geometry_v2_fixed",

        "risk_pct":
            risk_pct,

        "rr1":
            tp1_rr,

        "rr2":
            tp2_rr,

        "sl_min_pct":
            SMART_SL_MIN_PCT,

        "sl_max_pct":
            SMART_SL_MAX_PCT,
    }


def _infer_setup(
    pair: PairCandidate,
    market_mode: str,
) -> tuple[str, str, list[str], list[str]]:

    warnings: list[str] = []

    pair_tags = set(pair.tags)

    if market_mode == MODE_RECOVERY_LONG:

        tags = [
            "recovery_execution",
            "relative_strength_vs_btc",
            "whitelist",
        ]

        if "breakout" in pair_tags:
            tags.append("elite")

        return (
            "relative_strength_vs_btc",
            "market",
            tags,
            warnings,
        )

    if {
        "breakout",
        "momentum",
        "rs_btc",
    }.issubset(pair_tags):

        return (
            "wave_3",
            "market",
            [
                "wave_3",
                "relative_strength_vs_btc",
                "elite",
                "whitelist",
            ],
            warnings,
        )

    if (
        "breakout" in pair_tags
        and "momentum" in pair_tags
    ):

        return (
            "retest_breakout_confirmed",
            "market",
            [
                "retest_breakout_confirmed",
                "elite",
                "whitelist",
            ],
            warnings,
        )

    if (
        "rs_btc" in pair_tags
        and "continuation" in pair_tags
    ):

        return (
            "relative_strength_vs_btc",
            "market",
            [
                "relative_strength_vs_btc",
                "whitelist",
            ],
            warnings,
        )

    if "momentum" in pair_tags:

        if "near_resistance" in pair_tags:
            warnings.append(
                "مقاومة قريبة قبل TP1"
            )

        return (
            "vwap_reclaim",
            "market",
            [
                "vwap_reclaim",
                "whitelist",
            ],
            warnings,
        )

    if "rebound" in pair_tags:

        warnings.append(
            "ارتداد مبكر يحتاج تأكيد"
        )

        return (
            "support_bounce_confirmed",
            "pullback",
            [
                "support_bounce_confirmed",
            ],
            warnings,
        )

    warnings.append(
        "حركة متابعة — ليست أفضل إعداد تنفيذ"
    )

    return (
        "higher_low_continuation",
        "pullback",
        [
            "higher_low_continuation",
        ],
        warnings,
    )


def _infer_quality_context(
    pair: PairCandidate,
    setup_type: str,
    entry_timing: str,
    boost_score: float,
    display_score: float,
    raw_score: float,
    market_mode: str,
    warnings: list[str],
    execution_stability: dict,
) -> dict:

    tags = set(pair.tags or [])

    turnover = float(
        pair.turnover_usdt or 0.0
    )

    change = float(
        pair.change_pct or 0.0
    )

    vol_ratio = 1.00

    if "liquid" in tags:
        vol_ratio += 0.06

    if turnover >= 5_000_000:
        vol_ratio += 0.08

    if turnover >= 20_000_000:
        vol_ratio += 0.12

    if turnover >= 60_000_000:
        vol_ratio += 0.10

    if "momentum" in tags:
        vol_ratio += 0.12

    if "breakout" in tags:
        vol_ratio += 0.16

    if "rs_btc" in tags:
        vol_ratio += 0.10

    if "rebound" in tags:
        vol_ratio += 0.05

    vol_ratio = _clamp_vol_ratio(
        vol_ratio
    )

    mtf_confirmed = bool(
        "rs_btc" in tags
        or setup_type in {
            "wave_3",
            "retest_breakout_confirmed",
            "relative_strength_vs_btc",
        }
        or (
            "major" in tags
            and change >= 0.75
        )
    )

    breakout_quality = ""

    if (
        setup_type == "wave_3"
        and vol_ratio >= 1.12
    ):
        breakout_quality = "strong"

    elif (
        setup_type in {
            "retest_breakout_confirmed",
            "vwap_reclaim",
            "relative_strength_vs_btc",
        }
        and vol_ratio >= 1.08
    ):
        breakout_quality = "good"

    elif setup_type in {
        "support_bounce_confirmed",
        "higher_low_continuation",
    }:
        breakout_quality = "ok"

    resistance_warning = (
        "near_resistance_before_tp1"
        if any(
            "مقاومة" in str(w)
            for w in warnings
        )
        or "near_resistance" in tags
        else ""
    )

    effective_score = round(
        boost_score - (
            0.15
            if resistance_warning
            else 0.0
        ),
        2,
    )

    # restored compatibility
    dist_ma = round(
        min(
            abs(change) * 0.35,
            4.5,
        ),
        2,
    )

    return {

        "effective_score":
            round(effective_score, 2),

        "display_score":
            round(display_score, 2),

        "boost_score":
            round(boost_score, 2),

        "raw_score":
            round(raw_score, 2),

        "score_scale":
            "boost_score_execution_scale",

        "vol_ratio":
            vol_ratio,

        "dist_ma":
            dist_ma,

        "mtf_confirmed":
            mtf_confirmed,

        "breakout_quality":
            breakout_quality,

        "resistance_warning":
            resistance_warning,

        "execution_stability":
            execution_stability[
                "execution_stability"
            ],

        "stability_score":
            execution_stability[
                "stability_score"
            ],

        "instability_score":
            execution_stability[
                "instability_score"
            ],

        "nour_filter_passed":
            execution_stability[
                "nour_filter_passed"
            ],

        "nour_filter_reason":
            execution_stability[
                "nour_filter_reason"
            ],

        "exhausted_move":
            execution_stability[
                "exhausted_move"
            ],
    }


def build_signal_candidate(
    pair: PairCandidate,
    market_mode: str,
    min_normal_score: float,
    min_strong_score: float,
) -> SignalCandidate | None:

    raw_score = (
        pair.score_hint
        + pair.rebound_hint
    )

    boost_score = raw_score

    pair_tags = set(pair.tags)

    if "near_resistance" in pair_tags:
        boost_score -= 0.25

    if "rs_btc" in pair_tags:
        boost_score += 0.30

    if "rebound" in pair_tags:
        boost_score += 0.12

    if "major" in pair_tags:
        boost_score += 0.08

    (
        setup_type,
        entry_timing,
        tags,
        warnings,
    ) = _infer_setup(
        pair,
        market_mode,
    )

    rejection_reason = ""

    if market_mode == MODE_BLOCK_LONGS:

        if (
            setup_type
            not in BLOCK_EXCEPTION_SETUPS
            or pair.turnover_usdt < 5_000_000
        ):
            return None

        tags.append(
            "block_exception"
        )

        warnings.append(
            "Block exception only"
        )

        boost_score += 0.20

    threshold = min_normal_score

    if market_mode == MODE_STRONG_LONG_ONLY:

        threshold = min_strong_score

        if (
            setup_type
            not in WHITELIST_SETUPS
            and boost_score
            < (
                min_strong_score + 0.30
            )
        ):
            return None

    elif market_mode == MODE_RECOVERY_LONG:

        threshold = (
            min_normal_score + 0.10
        )

    if (
        boost_score < threshold
        or pair.last_price <= 0
    ):
        return None

    # IMPORTANT:
    # informational only now
    stability_context = (
        _calculate_execution_stability(
            pair=pair,
            setup_type=setup_type,
            market_mode=market_mode,
        )
    )

    display_score = _soft_cap_score(
        boost_score
    )

    target_profile = (
        _calculate_adaptive_targets(
            pair=pair,
            setup_type=setup_type,
            entry_timing=entry_timing,
            market_mode=market_mode,
            execution_stability=(
                stability_context[
                    "execution_stability"
                ]
            ),
        )
    )

    rr1 = float(
        target_profile["rr1"]
    )

    rr2 = float(
        target_profile["rr2"]
    )

    risk_pct = float(
        target_profile["risk_pct"]
    )

    entry = pair.last_price

    sl = entry * (
        1.0 - risk_pct / 100.0
    )

    risk_amount = entry - sl

    tp1 = entry + (
        risk_amount * rr1
    )

    tp2 = entry + (
        risk_amount * rr2
    )

    if (
        tp1 <= entry
        or tp2 <= tp1
        or sl >= entry
    ):
        return None

    quality_meta = (
        _infer_quality_context(
            pair=pair,
            setup_type=setup_type,
            entry_timing=entry_timing,
            boost_score=boost_score,
            display_score=display_score,
            raw_score=raw_score,
            market_mode=market_mode,
            warnings=warnings,
            execution_stability=(
                stability_context
            ),
        )
    )

    return SignalCandidate(

        symbol=pair.symbol,

        entry=round(entry, 8),

        sl=round(sl, 8),

        tp1=round(tp1, 8),

        tp2=round(tp2, 8),

        # UI ONLY
        score=display_score,

        setup_type=setup_type,

        entry_timing=entry_timing,

        market_mode=market_mode,

        execution_setup_tags=tags,

        warnings=warnings,

        notes=[
            "v129 fixed",
            "adaptive targets improved",
            "nour filter moved downstream",
        ],

        meta={

            "turnover_usdt":
                pair.turnover_usdt,

            "change_pct":
                pair.change_pct,

            "pair_tags":
                list(pair.tags),

            "rr1":
                rr1,

            "rr2":
                rr2,

            "risk_pct":
                risk_pct,

            "target_model":
                target_profile.get(
                    "model"
                ),

            "smart_sl_min_pct":
                target_profile.get(
                    "sl_min_pct"
                ),

            "smart_sl_max_pct":
                target_profile.get(
                    "sl_max_pct"
                ),

            "is_elite_setup": (
                setup_type
                in ELITE_SETUPS
                or "elite" in tags
            ),

            "rejection_context":
                rejection_reason,

            "threshold_used":
                round(threshold, 2),

            **quality_meta,
        },
    )
