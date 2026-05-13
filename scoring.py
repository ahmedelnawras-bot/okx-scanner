"""Signal formation preserves old philosophy: momentum, continuation, reclaim, and rebound can all form normal signals.
Execution-specific strictness stays downstream and never suppresses the normal signal itself.
"""
from __future__ import annotations

from .models import PairCandidate, SignalCandidate
from utils.constants import MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG, MODE_BLOCK_LONGS


WHITELIST_SETUPS = {"vwap_reclaim", "retest_breakout_confirmed", "wave_3", "relative_strength_vs_btc"}
ELITE_SETUPS = {"retest_breakout_confirmed", "wave_3", "relative_strength_vs_btc"}
BLOCK_EXCEPTION_SETUPS = {"relative_strength_vs_btc", "retest_breakout_confirmed"}



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
            warnings.append("مقاومة قريبة قبل TP1")
        return "vwap_reclaim", "market", ["vwap_reclaim", "whitelist"], warnings
    if "rebound" in pair_tags:
        warnings.append("ارتداد مبكر يحتاج تأكيد")
        return "support_bounce_confirmed", "pullback", ["support_bounce_confirmed"], warnings
    warnings.append("حركة متابعة — ليست أفضل إعداد تنفيذ")
    return "higher_low_continuation", "pullback", ["higher_low_continuation"], warnings



def build_signal_candidate(pair: PairCandidate, market_mode: str, min_normal_score: float, min_strong_score: float) -> SignalCandidate | None:
    score = pair.score_hint + pair.rebound_hint
    setup_type, entry_timing, tags, warnings = _infer_setup(pair, market_mode)
    pair_tags = set(pair.tags)

    if "near_resistance" in pair_tags:
        score -= 0.25
    if "rs_btc" in pair_tags:
        score += 0.30
    if "rebound" in pair_tags:
        score += 0.12
    if "major" in pair_tags:
        score += 0.08
    if "compression" in pair_tags and setup_type == "higher_low_continuation":
        score -= 0.10

    rejection_reason = ""
    if market_mode == MODE_BLOCK_LONGS:
        if setup_type not in BLOCK_EXCEPTION_SETUPS or pair.turnover_usdt < 5_000_000:
            return None
        tags.append("block_exception")
        warnings.append("Block exception only")
        score += 0.20

    threshold = min_normal_score
    if market_mode == MODE_STRONG_LONG_ONLY:
        threshold = min_strong_score
        # Strong mode raises quality but should not become a mini-block.
        if setup_type not in WHITELIST_SETUPS and score < (min_strong_score + 0.30):
            return None
    elif market_mode == MODE_RECOVERY_LONG:
        threshold = min_normal_score + 0.10

    if score < threshold or pair.last_price <= 0:
        return None

    rr1 = 1.25 if setup_type == "support_bounce_confirmed" else 1.55
    rr2 = 2.25 if setup_type == "support_bounce_confirmed" else 2.85
    risk_pct = 0.92 if entry_timing == "market" else 1.10
    if market_mode == MODE_RECOVERY_LONG:
        risk_pct = 0.78
        rr1 = 1.10
        rr2 = 1.85
    elif "near_resistance" in pair_tags:
        rr1 = max(1.2, rr1 - 0.15)
        rr2 = max(2.0, rr2 - 0.25)
        rejection_reason = "near_resistance_warning"

    entry = pair.last_price
    sl = entry * (1.0 - risk_pct / 100.0)
    risk_amount = entry - sl
    tp1 = entry + (risk_amount * rr1)
    tp2 = entry + (risk_amount * rr2)

    if tp1 <= entry or tp2 <= tp1:
        return None

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
        notes=["old-core behavior preserved: signal forms before execution decision"],
        meta={
            "turnover_usdt": pair.turnover_usdt,
            "change_pct": pair.change_pct,
            "pair_tags": list(pair.tags),
            "rr1": rr1,
            "rr2": rr2,
            "is_elite_setup": setup_type in ELITE_SETUPS,
            "rejection_context": rejection_reason,
        },
    )
