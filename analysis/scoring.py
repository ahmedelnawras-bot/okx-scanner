"""Signal formation preserves old philosophy: momentum, continuation, reclaim, and rebound can all form normal signals.
Execution-specific strictness stays downstream and never suppresses the normal signal itself.

v135 note:
- Pair selection / 24h ticker ranking is only an attention score.
- The displayed/decision score is a normalized aggressive signal_score (0..10.5)
  built from momentum, volume, RS vs BTC, setup, market mode context, and light risk penalties.
- TP/SL uses a hybrid RR model with wider crypto-safe SL bands.
"""
from __future__ import annotations

from .models import PairCandidate, SignalCandidate
from utils.constants import MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG, MODE_BLOCK_LONGS


WHITELIST_SETUPS = {"vwap_reclaim", "retest_breakout_confirmed", "wave_3", "relative_strength_vs_btc"}
ELITE_SETUPS = {"retest_breakout_confirmed", "wave_3", "relative_strength_vs_btc"}
BLOCK_EXCEPTION_SETUPS = {"relative_strength_vs_btc", "retest_breakout_confirmed"}


_SETUP_WEIGHTS = {
    "vwap_reclaim": 3,
    "retest_breakout_confirmed": 3,
    "liquidity_sweep_reclaim": 2,
    "relative_strength_vs_btc": 2,
    "wave_3": 2,
    "support_bounce_confirmed": 2,
    "failed_breakdown_trap": 2,
    "higher_low_continuation": 2,
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value or 0.0)))


def _infer_setup(pair: PairCandidate, market_mode: str) -> tuple[str, str, list[str], list[str]]:
    warnings: list[str] = []
    pair_tags = set(pair.tags)

    if market_mode == MODE_RECOVERY_LONG:
        tags = ["recovery_execution", "relative_strength_vs_btc", "whitelist"]
        if "breakout" in pair_tags:
            tags.append("elite")
        return "relative_strength_vs_btc", "market", tags, warnings

    if {"breakout", "momentum", "rs_btc"}.issubset(pair_tags):
        return "wave_3", "market", ["wave_3", "relative_strength_vs_btc", "elite", "whitelist"], warnings
    if "breakout" in pair_tags and "momentum" in pair_tags:
        return "retest_breakout_confirmed", "market", ["retest_breakout_confirmed", "elite", "whitelist"], warnings
    if "rs_btc" in pair_tags and "continuation" in pair_tags:
        return "relative_strength_vs_btc", "market", ["relative_strength_vs_btc", "whitelist"], warnings
    if "momentum" in pair_tags:
        if "near_resistance" in pair_tags:
            warnings.append("مقاومة قريبة قبل TP1 — warning فقط")
        return "vwap_reclaim", "market", ["vwap_reclaim", "whitelist"], warnings
    if "rebound" in pair_tags:
        warnings.append("ارتداد مبكر يحتاج تأكيد")
        return "support_bounce_confirmed", "pullback", ["support_bounce_confirmed"], warnings
    warnings.append("حركة متابعة — ليست أفضل إعداد تنفيذ")
    return "higher_low_continuation", "pullback", ["higher_low_continuation"], warnings


def _mtf_proxy(pair: PairCandidate, setup_type: str) -> bool:
    tags = set(pair.tags or [])
    change = float(pair.change_pct or 0.0)
    turnover = float(pair.turnover_usdt or 0.0)
    return bool(
        "rs_btc" in tags
        or setup_type in {"wave_3", "retest_breakout_confirmed", "relative_strength_vs_btc"}
        or ("major" in tags and change >= 0.75)
        or (change >= 2.2 and turnover >= 10_000_000)
    )


def _volume_ratio_proxy(pair: PairCandidate, setup_type: str) -> float:
    tags = set(pair.tags or [])
    turnover = float(pair.turnover_usdt or 0.0)
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
    if setup_type == "wave_3":
        vol_ratio += 0.06
    return round(_clamp(vol_ratio, 0.85, 2.20), 2)


def _calculate_signal_score(pair: PairCandidate, setup_type: str, entry_timing: str, market_mode: str, warnings: list[str]) -> tuple[float, dict]:
    """Aggressive normalized score.

    It intentionally does NOT use pair.score_hint/rebound_hint for acceptance; those are
    attention/ranking only. Components map to the approved v135 weights and stay capped.
    """
    tags = set(pair.tags or [])
    change = float(pair.change_pct or 0.0)
    turnover = float(pair.turnover_usdt or 0.0)
    vol_ratio = _volume_ratio_proxy(pair, setup_type)
    mtf_confirmed = _mtf_proxy(pair, setup_type)

    # 8.0-point aggressive base.
    trend = 0.25
    if change > 0:
        trend += min(change / 3.0, 0.55)
    if "continuation" in tags:
        trend += 0.25
    if "momentum" in tags:
        trend += 0.25
    if setup_type in {"wave_3", "retest_breakout_confirmed", "relative_strength_vs_btc"}:
        trend += 0.20
    trend = _clamp(trend, 0.0, 1.25)

    momentum = 0.15
    if "momentum" in tags:
        momentum += 0.45
    if "breakout" in tags:
        momentum += 0.35
    if 0.75 <= change <= 5.5:
        momentum += 0.25
    elif change > 5.5:
        momentum += 0.15
    if "rebound" in tags:
        momentum += 0.20
    momentum = _clamp(momentum, 0.0, 1.25)

    volume = _clamp((vol_ratio - 0.85) / 0.75 * 1.25, 0.0, 1.25)
    rs = 1.50 if "rs_btc" in tags or setup_type == "relative_strength_vs_btc" else (0.55 if "major" in tags else 0.25)
    rs = _clamp(rs, 0.0, 1.50)

    breakout = 0.20
    if setup_type == "wave_3":
        breakout += 0.75
    elif setup_type in {"retest_breakout_confirmed", "vwap_reclaim"}:
        breakout += 0.65
    elif setup_type in {"relative_strength_vs_btc", "higher_low_continuation"}:
        breakout += 0.40
    elif setup_type == "support_bounce_confirmed":
        breakout += 0.30
    if "breakout" in tags:
        breakout += 0.30
    breakout = _clamp(breakout, 0.0, 1.25)

    market_context = {
        MODE_NORMAL_LONG: 0.75,
        MODE_STRONG_LONG_ONLY: 0.48,
        MODE_RECOVERY_LONG: 0.55,
        MODE_BLOCK_LONGS: 0.25,
    }.get(market_mode, 0.50)
    mtf = 0.50 if mtf_confirmed else 0.15
    risk_quality = 0.25 if "near_resistance" not in tags else 0.12

    base = trend + momentum + volume + rs + breakout + market_context + mtf + risk_quality

    # Setup bonuses, capped at +1.5.
    bonus = 0.0
    if setup_type == "wave_3":
        bonus += 0.45
    if "rs_btc" in tags or setup_type == "relative_strength_vs_btc":
        bonus += 0.40
    if setup_type == "vwap_reclaim":
        bonus += 0.30
    if setup_type == "retest_breakout_confirmed":
        bonus += 0.30
    if setup_type == "liquidity_sweep_reclaim":
        bonus += 0.25
    if setup_type == "higher_low_continuation":
        bonus += 0.20
    if setup_type == "support_bounce_confirmed":
        bonus += 0.20
    bonus = _clamp(bonus, 0.0, 1.50)

    # Light penalties, capped at -1.2. These are warnings/quality shaping, not hard blocks.
    penalty = 0.0
    if change >= 8.0 and "breakout" not in tags:
        penalty += 0.35  # late entry / chase risk
    if "near_resistance" in tags:
        penalty += 0.30
    if change >= 10.0:
        penalty += 0.25  # overheated proxy
    if market_mode == MODE_STRONG_LONG_ONLY and not mtf_confirmed:
        penalty += 0.20
    if turnover < 1_500_000:
        penalty += 0.25
    penalty = _clamp(penalty, 0.0, 1.20)

    score = _clamp(base + bonus - penalty, 0.0, 10.50)
    components = {
        "attention_score": round(float(pair.score_hint or 0.0) + float(pair.rebound_hint or 0.0), 2),
        "signal_score_components": {
            "trend_ema_direction": round(trend, 2),
            "momentum_candle_strength": round(momentum, 2),
            "volume_expansion": round(volume, 2),
            "rs_vs_btc": round(rs, 2),
            "breakout_vwap_reclaim": round(breakout, 2),
            "btc_market_context": round(market_context, 2),
            "mtf_confirmation": round(mtf, 2),
            "risk_distance_rr": round(risk_quality, 2),
            "setup_bonus": round(bonus, 2),
            "penalty": round(penalty, 2),
        },
        "vol_ratio": vol_ratio,
        "mtf_confirmed": mtf_confirmed,
    }
    return round(score, 2), components


def _hybrid_risk_pct(pair: PairCandidate, setup_type: str, entry_timing: str, market_mode: str) -> float:
    """Crypto-safe wider SL bands, using an ATR-like proxy until full candles are wired here."""
    change_abs = abs(float(pair.change_pct or 0.0))
    tags = set(pair.tags or [])
    if market_mode == MODE_RECOVERY_LONG:
        lo, hi = 1.0, 1.8
    elif market_mode == MODE_BLOCK_LONGS:
        lo, hi = 1.0, 1.6
    elif entry_timing == "pullback" or setup_type in {"retest_breakout_confirmed", "higher_low_continuation"}:
        lo, hi = 1.4, 2.6
    else:
        lo, hi = 1.2, 2.2

    atr_proxy = _clamp(0.65 + change_abs * 0.10, 0.70, 1.80)
    base = lo + (hi - lo) * 0.45
    risk_pct = max(lo, min(hi, base + atr_proxy * 0.35))
    if "breakout" in tags or setup_type == "wave_3":
        risk_pct += 0.10
    if "near_resistance" in tags:
        risk_pct -= 0.05
    return round(_clamp(risk_pct, lo, min(hi, 2.8)), 3)


def _rr_targets(pair: PairCandidate, setup_type: str, market_mode: str) -> tuple[float, float, str]:
    tags = set(pair.tags or [])
    if market_mode == MODE_RECOVERY_LONG:
        rr1, rr2 = 1.10, 1.85
    elif setup_type == "support_bounce_confirmed":
        rr1, rr2 = 1.25, 2.25
    elif setup_type in {"wave_3", "relative_strength_vs_btc"}:
        rr1, rr2 = 1.55, 2.90
    elif setup_type in {"retest_breakout_confirmed", "vwap_reclaim"}:
        rr1, rr2 = 1.50, 2.75
    else:
        rr1, rr2 = 1.45, 2.55

    resistance_context = ""
    if "near_resistance" in tags:
        # v135: do not hard reject unless TP1 space is truly broken. With current
        # ticker-only proxy we treat it as 0.95R warning and reshape TP1 slightly.
        resistance_r_estimate = 0.95
        if resistance_r_estimate < 0.75:
            resistance_context = "near_resistance_before_tp1_hard"
        else:
            resistance_context = "near_resistance_warning"
            rr1 = max(1.15, min(rr1, resistance_r_estimate - 0.05))
            rr2 = max(2.05, rr2 - 0.20)
    return round(rr1, 2), round(rr2, 2), resistance_context


def _infer_quality_context(pair: PairCandidate, setup_type: str, entry_timing: str, score: float, market_mode: str, warnings: list[str], score_components: dict | None = None) -> dict:
    """Build old-style execution quality metadata from the lightweight rebuild inputs."""
    tags = set(pair.tags or [])
    turnover = float(pair.turnover_usdt or 0.0)
    change = float(pair.change_pct or 0.0)

    score_components = score_components or {}
    vol_ratio = float(score_components.get("vol_ratio") or _volume_ratio_proxy(pair, setup_type))
    mtf_confirmed = bool(score_components.get("mtf_confirmed") if "mtf_confirmed" in score_components else _mtf_proxy(pair, setup_type))

    if setup_type == "wave_3" and vol_ratio >= 1.12:
        breakout_quality = "strong"
    elif setup_type in {"retest_breakout_confirmed", "vwap_reclaim", "relative_strength_vs_btc"} and vol_ratio >= 1.08:
        breakout_quality = "good"
    elif setup_type in {"support_bounce_confirmed", "higher_low_continuation"}:
        breakout_quality = "ok"
    else:
        breakout_quality = ""

    dist_ma = round(min(abs(change) * 0.55, 4.5), 2)
    resistance_warning = "near_resistance_before_tp1" if any("مقاومة" in str(w) for w in warnings) or "near_resistance" in tags else ""
    setup_weight = _SETUP_WEIGHTS.get(setup_type, 0)
    if "elite" in tags or setup_type in ELITE_SETUPS:
        setup_weight = max(setup_weight, 3)

    effective_score = round(score, 2)
    if resistance_warning:
        effective_score = round(effective_score - 0.10, 2)

    btc_bounce_pct = float(getattr(pair, "btc_bounce_pct", 0.0) or 0.0)
    symbol_bounce_pct = max(float(change or 0.0), 0.0)
    btc_bounce_positive = max(btc_bounce_pct, 0.0)
    bounce_ratio = (symbol_bounce_pct / btc_bounce_positive) if btc_bounce_positive > 0 else 0.0
    bounce_faster_than_btc = bool(
        btc_bounce_positive > 0
        and symbol_bounce_pct > 0
        and (bounce_ratio >= 1.5 or symbol_bounce_pct >= btc_bounce_positive + 0.40)
    )

    return {
        "effective_score": effective_score,
        "raw_score": round(score, 2),
        "attention_score": score_components.get("attention_score", round(float(pair.score_hint or 0.0) + float(pair.rebound_hint or 0.0), 2)),
        "signal_score_components": score_components.get("signal_score_components", {}),
        "vol_ratio": vol_ratio,
        "mtf_confirmed": mtf_confirmed,
        "dist_ma": dist_ma,
        "breakout": "breakout" in tags or setup_type in {"wave_3", "retest_breakout_confirmed"},
        "pre_breakout": setup_type in {"vwap_reclaim", "higher_low_continuation", "support_bounce_confirmed"},
        "breakout_quality": breakout_quality,
        "setup_weight": setup_weight,
        "resistance_warning": resistance_warning,
        "entry_maturity": "healthy" if entry_timing == "market" and not resistance_warning else "pullback_first" if entry_timing == "pullback" else "watch_resistance",
        "wave_estimate": 3 if setup_type == "wave_3" else 0,
        "wave_context": "wave_3" if setup_type == "wave_3" else setup_type,
        "volume_state": f"vol_ratio_{vol_ratio:.2f}",
        "htf_confirmation": "yes" if mtf_confirmed else "no",
        "btc_bounce_pct": round(btc_bounce_pct, 3),
        "symbol_bounce_pct": round(symbol_bounce_pct, 3),
        "bounce_ratio_vs_btc": round(bounce_ratio, 2),
        "bounce_faster_than_btc": bounce_faster_than_btc,
        "recovery_relative_bounce": bounce_faster_than_btc,
    }


def build_signal_candidate(pair: PairCandidate, market_mode: str, min_normal_score: float, min_strong_score: float) -> SignalCandidate | None:
    setup_type, entry_timing, tags, warnings = _infer_setup(pair, market_mode)
    pair_tags = set(pair.tags)
    score, score_components = _calculate_signal_score(pair, setup_type, entry_timing, market_mode, warnings)

    rejection_reason = ""
    if market_mode == MODE_BLOCK_LONGS:
        if setup_type not in BLOCK_EXCEPTION_SETUPS or pair.turnover_usdt < 5_000_000:
            return None
        tags.append("block_exception")
        warnings.append("Block exception only")
        score = min(10.5, score + 0.20)

    threshold = min_normal_score
    strong_exception = setup_type in {"wave_3", "relative_strength_vs_btc", "vwap_reclaim"} and score >= 7.2
    aggressive_normal_exception = setup_type in {"wave_3", "relative_strength_vs_btc", "retest_breakout_confirmed", "vwap_reclaim"} and score >= 6.0

    if market_mode == MODE_STRONG_LONG_ONLY:
        threshold = min_strong_score
        if strong_exception:
            threshold = min(threshold, 7.2)
        if setup_type not in WHITELIST_SETUPS and score < (threshold + 0.30):
            return None
    elif market_mode == MODE_RECOVERY_LONG:
        threshold = min_normal_score + 0.10
    elif market_mode == MODE_NORMAL_LONG and aggressive_normal_exception:
        threshold = min(threshold, 6.0)

    if score < threshold or pair.last_price <= 0:
        return None

    rr1, rr2, resistance_context = _rr_targets(pair, setup_type, market_mode)
    risk_pct = _hybrid_risk_pct(pair, setup_type, entry_timing, market_mode)
    if resistance_context == "near_resistance_before_tp1_hard":
        return None
    if resistance_context:
        rejection_reason = resistance_context
        if resistance_context == "near_resistance_warning":
            warnings.append("TP1 reshaped before nearby resistance")

    entry = pair.last_price
    sl = entry * (1.0 - risk_pct / 100.0)
    risk_amount = entry - sl
    tp1 = entry + (risk_amount * rr1)
    tp2 = entry + (risk_amount * rr2)

    if tp1 <= entry or tp2 <= tp1:
        return None

    quality_meta = _infer_quality_context(pair, setup_type, entry_timing, score, market_mode, warnings, score_components)
    if rejection_reason:
        quality_meta["rejection_context"] = rejection_reason
        if not quality_meta.get("resistance_warning"):
            quality_meta["resistance_warning"] = rejection_reason

    return SignalCandidate(
        symbol=pair.symbol,
        entry=round(entry, 8),
        sl=round(sl, 8),
        tp1=round(tp1, 8),
        tp2=round(tp2, 8),
        score=round(score, 2),
        setup_type=setup_type,
        entry_timing=entry_timing,
        market_mode=market_mode,
        execution_setup_tags=tags,
        warnings=warnings,
        notes=["attention_score is ranking-only; signal_score is normalized aggressive quality"],
        meta={
            "turnover_usdt": pair.turnover_usdt,
            "change_pct": pair.change_pct,
            "pair_tags": list(pair.tags),
            "rr1": rr1,
            "rr2": rr2,
            "risk_pct": risk_pct,
            "sl_model": "hybrid_support_atr_proxy_crypto_safe",
            "is_elite_setup": setup_type in ELITE_SETUPS or "elite" in tags,
            "block_exception": "block_exception" in tags,
            "recovery_execution": "recovery_execution" in tags,
            **quality_meta,
        },
    )
