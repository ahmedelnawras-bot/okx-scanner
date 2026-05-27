"""
Signal formation preserves old philosophy: momentum, continuation,
reclaim, and rebound can all form normal signals.

Execution-specific strictness stays downstream and never suppresses
the normal signal itself.

v129 FIXED execution intelligence update
â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
"""

from __future__ import annotations

from .models import PairCandidate, SignalCandidate

try:
    from .price_action_evidence import build_smart_evidence
except Exception:
    def build_smart_evidence(*args, **kwargs) -> dict:
        return {
            "available": False,
            "reason": "price_action_evidence_import_failed",
            "displacement_hint": False,
            "compression_release_hint": False,
            "sweep_reclaim_hint": False,
            "failed_breakout_risk": False,
            "auction_acceptance_hint": False,
        }

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

def _confirmation_bonus(
    pair: PairCandidate,
    setup_type: str,
) -> float:

    tags = set(pair.tags or [])
    bonus = 0.0

    if "rs_btc" in tags:
        bonus += 0.18

    if "breakout" in tags:
        bonus += 0.10

    if "liquid" in tags:
        bonus += 0.08

    if "continuation" in tags:
        bonus += 0.08

    if setup_type in {
        "retest_breakout_confirmed",
        "vwap_reclaim",
        "relative_strength_vs_btc",
    }:
        bonus += 0.10

    if "major" in tags:
        bonus += 0.06

    return round(bonus, 3)


def _freshness_bonus(
    pair: PairCandidate,
    setup_type: str,
    entry_timing: str,
) -> float:

    tags = set(pair.tags or [])
    change_abs = abs(float(pair.change_pct or 0.0))
    bonus = 0.0

    if change_abs <= 1.20:
        bonus += 0.18
    elif change_abs <= 2.20:
        bonus += 0.10
    elif change_abs <= 3.20:
        bonus += 0.03

    if setup_type in {
        "retest_breakout_confirmed",
        "vwap_reclaim",
        "higher_low_continuation",
        "liquidity_sweep_reclaim",
        "support_bounce_confirmed",
    }:
        bonus += 0.10

    if entry_timing == "pullback":
        bonus += 0.12

    if "compression" in tags:
        bonus += 0.08

    if "rebound" in tags and change_abs <= 2.20:
        bonus += 0.05

    return round(bonus, 3)


def _heat_penalty(
    pair: PairCandidate,
    setup_type: str,
) -> float:

    tags = set(pair.tags or [])
    turnover = float(pair.turnover_usdt or 0.0)
    change_abs = abs(float(pair.change_pct or 0.0))
    penalty = 0.0

    if change_abs >= 2.80:
        penalty += 0.18

    if change_abs >= 4.00:
        penalty += 0.32

    if "near_resistance" in tags:
        penalty += 0.18

    if turnover >= 20_000_000 and change_abs >= 2.50:
        penalty += 0.12

    if turnover >= 60_000_000 and change_abs >= 4.00:
        penalty += 0.12

    if {"breakout", "momentum", "rs_btc"}.issubset(tags) and change_abs >= 3.00:
        penalty += 0.15

    if setup_type == "wave_3" and change_abs >= 4.00:
        penalty += 0.10

    return round(penalty, 3)



def _build_btc_control_context(pair: PairCandidate) -> dict:
    """Lightweight BTC control context for alt execution.

    Uses btc_bounce_pct attached in main.py from market snapshot.
    Display + post-Nour execution context only.
    """
    try:
        btc_15m = float(getattr(pair, "btc_bounce_pct", 0.0) or 0.0)
    except Exception:
        btc_15m = 0.0

    abs_move = abs(btc_15m)

    if abs_move >= 0.85:
        status = "risk"
        label = "Dominance Risk"
        icon = "ðŸ”´"
    elif abs_move >= 0.35:
        status = "active"
        label = "Active BTC"
        icon = "ðŸŸ¡"
    else:
        status = "calm"
        label = "Calm BTC"
        icon = "ðŸŸ¢"

    return {
        "status": status,
        "label": label,
        "icon": icon,
        "btc_15m_move": round(btc_15m, 3),
    }


def _build_4h_resistance_meta(pair: PairCandidate) -> dict:
    ctx = getattr(pair, "resistance_4h_context", None)
    if isinstance(ctx, dict):
        return dict(ctx)
    return {
        "status": "unknown",
        "distance_pct": None,
        "resistance": None,
        "reason": "not_attached",
    }



def _build_trade_context_meta(
    pair: PairCandidate,
    setup_type: str,
    entry_timing: str,
    smart_evidence: dict | None = None,
) -> dict:
    """Build lightweight display metadata for Telegram Trade Details.

    Display-only context enrichment:
    - Does not change score.
    - Does not change execution decisions.
    - Uses only data already available on the pair/smart_evidence.
    """

    tags = set(getattr(pair, "tags", []) or [])
    smart_evidence = smart_evidence or {}
    candles = list(getattr(pair, "recent_candles", []) or [])

    # 1) Wave / setup state
    if setup_type == "wave_3":
        wave = "Wave 3"
    elif setup_type == "retest_breakout_confirmed":
        wave = "Breakout Retest"
    elif setup_type == "relative_strength_vs_btc":
        wave = "RS Continuation"
    elif setup_type == "liquidity_sweep_reclaim":
        wave = "Sweep Reclaim"
    elif setup_type == "support_bounce_confirmed":
        wave = "Support Bounce"
    elif setup_type == "vwap_reclaim":
        wave = "VWAP Reclaim"
    else:
        wave = str(setup_type or "-").replace("_", " ").title()

    # 2) Volume / pressure state
    # We do not rely on raw exchange volume here because the PA candle helper
    # currently passes OHLC only. This is a price-action pressure proxy.
    volume_state = "Normal"
    try:
        if len(candles) >= 4:
            ranges = [
                abs(float(c.get("high", 0.0) or 0.0) - float(c.get("low", 0.0) or 0.0))
                for c in candles[-4:]
            ]
            avg_prev_range = sum(ranges[:-1]) / max(1, len(ranges[:-1]))
            last_range = ranges[-1]
            if avg_prev_range > 0 and last_range >= avg_prev_range * 1.45:
                volume_state = "Expansion"
            elif avg_prev_range > 0 and last_range <= avg_prev_range * 0.70:
                volume_state = "Compression"
    except Exception:
        volume_state = "Normal"

    if smart_evidence.get("compression_release_hint"):
        volume_state = "Compression Release"
    elif smart_evidence.get("displacement_hint"):
        volume_state = "Expansion"
    elif smart_evidence.get("failed_breakout_risk"):
        volume_state = "Exhaustion Risk"

    # 3) HTF / broader confirmation proxy
    # Uses stable tags + PA hints. This is not a hard filter.
    if smart_evidence.get("failed_breakout_risk"):
        htf_confirmation = "Caution"
    elif smart_evidence.get("auction_acceptance_hint") and smart_evidence.get("displacement_hint"):
        htf_confirmation = "Bullish"
    elif "rs_btc" in tags and ("continuation" in tags or "breakout" in tags):
        htf_confirmation = "Bullish Bias"
    elif "rebound" in tags or smart_evidence.get("sweep_reclaim_hint"):
        htf_confirmation = "Reclaim Bias"
    else:
        htf_confirmation = "Neutral"

    return {
        "wave": wave,
        "volume_state": volume_state,
        "htf_confirmation": htf_confirmation,
        "entry_context": "Market" if entry_timing == "market" else "Pullback",
    }


def _calculate_pa_score(
    smart_evidence: dict,
    market_mode: str,
) -> dict:
    """Price Action sub-score.

    Surgical scoring layer:
    - Runs before Nour.
    - Adjusts boost_score only within a small bounded range.
    - Does NOT change market mode logic.
    - Does NOT hard reject by itself.
    """

    if not isinstance(smart_evidence, dict) or not smart_evidence.get("available"):
        return {
            "pa_score": 0.0,
            "pa_score_raw": 0.0,
            "pa_score_model": "pa_score_v1",
            "pa_score_reason": smart_evidence.get("reason", "not_available") if isinstance(smart_evidence, dict) else "not_available",
            "pa_score_flags": {},
        }

    expansion = bool(smart_evidence.get("displacement_hint"))
    acceptance = bool(smart_evidence.get("auction_acceptance_hint"))
    compression = bool(smart_evidence.get("compression_release_hint"))
    sweep = bool(smart_evidence.get("sweep_reclaim_hint"))
    weak_breakout = bool(smart_evidence.get("failed_breakout_risk"))

    raw = 0.0
    flags: dict[str, bool] = {
        "expansion": expansion,
        "acceptance": acceptance,
        "compression": compression,
        "sweep": sweep,
        "weak_breakout": weak_breakout,
    }

    if expansion:
        raw += 0.22

    if acceptance:
        raw += 0.22

    if compression:
        raw += 0.12

    if sweep:
        raw += 0.10

    if weak_breakout:
        raw -= 0.35

    # Mode-aware weighting.
    # The same PA event does not mean the same thing in every mode.
    if market_mode == MODE_STRONG_LONG_ONLY:
        if expansion:
            raw += 0.08
        if acceptance:
            raw += 0.05
        if weak_breakout and not acceptance:
            raw -= 0.12

    elif market_mode == MODE_RECOVERY_LONG:
        if sweep:
            raw += 0.15
        if acceptance:
            raw += 0.08
        if expansion and not (sweep or acceptance):
            raw -= 0.05

    elif market_mode == MODE_BLOCK_LONGS:
        if acceptance:
            raw += 0.06
        if weak_breakout:
            raw -= 0.20
        if not acceptance:
            raw -= 0.08

    pa_score = round(
        _clamp(
            raw,
            -0.65,
            0.55,
        ),
        3,
    )

    reason_parts: list[str] = []

    if expansion:
        reason_parts.append("expansion")

    if acceptance:
        reason_parts.append("acceptance")

    if compression:
        reason_parts.append("compression")

    if sweep:
        reason_parts.append("sweep")

    if weak_breakout:
        reason_parts.append("weak_breakout")

    if not reason_parts:
        reason_parts.append("neutral")

    return {
        "pa_score": pa_score,
        "pa_score_raw": round(raw, 3),
        "pa_score_model": "pa_score_v1_mode_aware",
        "pa_score_reason": ",".join(reason_parts),
        "pa_score_flags": flags,
    }


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Execution Stability Intelligence
# IMPORTANT:
# informational only
# NOT hard rejection
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Adaptive Target Geometry
# FIXED:
# more realistic TP2
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
                "Ù…Ù‚Ø§ÙˆÙ…Ø© Ù‚Ø±ÙŠØ¨Ø© Ù‚Ø¨Ù„ TP1"
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
            "Ø§Ø±ØªØ¯Ø§Ø¯ Ù…Ø¨ÙƒØ± ÙŠØ­ØªØ§Ø¬ ØªØ£ÙƒÙŠØ¯"
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
        "Ø­Ø±ÙƒØ© Ù…ØªØ§Ø¨Ø¹Ø© â€” Ù„ÙŠØ³Øª Ø£ÙØ¶Ù„ Ø¥Ø¹Ø¯Ø§Ø¯ ØªÙ†ÙÙŠØ°"
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
            "Ù…Ù‚Ø§ÙˆÙ…Ø©" in str(w)
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

    base_score = float(pair.score_hint or 0.0) + float(pair.rebound_hint or 0.0)

    pair_tags = set(pair.tags)

    (
        setup_type,
        entry_timing,
        tags,
        warnings,
    ) = _infer_setup(
        pair,
        market_mode,
    )

    confirmation_bonus = _confirmation_bonus(
        pair,
        setup_type,
    )

    freshness_bonus = _freshness_bonus(
        pair,
        setup_type,
        entry_timing,
    )

    heat_penalty = _heat_penalty(
        pair,
        setup_type,
    )

    raw_score = round(
        base_score
        + confirmation_bonus
        + freshness_bonus
        - heat_penalty,
        3,
    )

    boost_score = raw_score

    if "near_resistance" in pair_tags:
        boost_score -= 0.18

    if "rs_btc" in pair_tags:
        boost_score += 0.18

    if "rebound" in pair_tags:
        boost_score += 0.08

    if "major" in pair_tags:
        boost_score += 0.05

    if {"breakout", "momentum", "rs_btc"}.issubset(pair_tags) and abs(float(pair.change_pct or 0.0)) >= 3.0:
        boost_score -= 0.12

    if entry_timing == "pullback":
        boost_score += 0.06

    smart_evidence = build_smart_evidence(
        candles=getattr(pair, "recent_candles", []) or [],
        pair=pair,
        signal_setup=setup_type,
        market_mode=market_mode,
    )

    pa_score_context = _calculate_pa_score(
        smart_evidence=smart_evidence,
        market_mode=market_mode,
    )

    trade_context_meta = _build_trade_context_meta(
        pair=pair,
        setup_type=setup_type,
        entry_timing=entry_timing,
        smart_evidence=smart_evidence,
    )

    btc_control_context = _build_btc_control_context(pair)
    resistance_4h_context = _build_4h_resistance_meta(pair)

    # PA sub-score is intentionally small and bounded.
    # It improves ranking/eligibility before Nour without becoming a hard filter.
    boost_score += float(pa_score_context.get("pa_score") or 0.0)

    print(
        f"SMART_EVIDENCE | "
        f"{pair.symbol} | "
        f"available={smart_evidence.get('available')} | "
        f"disp={smart_evidence.get('displacement_hint')} | "
        f"sweep={smart_evidence.get('sweep_reclaim_hint')} | "
        f"compress={smart_evidence.get('compression_release_hint')} | "
        f"failed={smart_evidence.get('failed_breakout_risk')} | "
        f"accept={smart_evidence.get('auction_acceptance_hint')} | "
        f"pa_score={pa_score_context.get('pa_score')} | "
        f"pa_reason={pa_score_context.get('pa_score_reason')}",
        flush=True,
    )

    if (
        market_mode == MODE_RECOVERY_LONG
        and "recovery_execution" not in tags
    ):
        tags.append("recovery_execution")

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

    threshold = max(
        float(min_normal_score),
        6.95,
    )

    if market_mode == MODE_STRONG_LONG_ONLY:

        threshold = max(
            float(min_strong_score),
            7.45,
        )

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

        threshold = max(
            float(min_normal_score) + 0.10,
            7.05,
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
            "global anti-chase score refinement",
            "pa sub-score pre-nour enabled",
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

            "freshness_bonus":
                freshness_bonus,

            "heat_penalty":
                heat_penalty,

            "confirmation_bonus":
                confirmation_bonus,

            "wave":
                trade_context_meta.get("wave"),

            "volume_state":
                trade_context_meta.get("volume_state"),

            "htf_confirmation":
                trade_context_meta.get("htf_confirmation"),

            "entry_context":
                trade_context_meta.get("entry_context"),

            "btc_control":
                btc_control_context,

            "btc_control_status":
                btc_control_context.get("status"),

            "btc_15m_move":
                btc_control_context.get("btc_15m_move"),

            "resistance_4h":
                resistance_4h_context,

            "resistance_4h_status":
                resistance_4h_context.get("status"),

            "resistance_4h_distance_pct":
                resistance_4h_context.get("distance_pct"),

            "smart_evidence":
                smart_evidence,

            "pa_score":
                pa_score_context.get("pa_score"),

            "pa_score_raw":
                pa_score_context.get("pa_score_raw"),

            "pa_score_model":
                pa_score_context.get("pa_score_model"),

            "pa_score_reason":
                pa_score_context.get("pa_score_reason"),

            "pa_score_flags":
                pa_score_context.get("pa_score_flags"),

            **quality_meta,
        },
    )
