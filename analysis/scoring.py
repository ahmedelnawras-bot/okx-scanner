"""Signal formation preserves old philosophy: momentum, continuation, reclaim, and rebound can all form normal signals.
Execution-specific strictness stays downstream and never suppresses the normal signal itself.

v128b score architecture fix:
──────────────────────────────────────────────────────────────────────────────
المشكلة اللي اتحلت:
  - الـ normalization الـ aggressive (raw/14*10) كانت بتغير الـ perception scale كامل
  - الـ gate decisions كانت بتستخدم display_score وده مخالف للفلسفة

الحل:
  ┌───────────────┬──────────────────────────────────────────────┐
  │ raw_score     │ pair.score_hint + rebound_hint               │
  │               │ الـ scale الأصلي — للـ reference فقط        │
  ├───────────────┼──────────────────────────────────────────────┤
  │ boost_score   │ raw + context boosts                         │
  │               │ يُستخدم في: market acceptance + gate logic  │
  │               │ نفس scale الـ config thresholds (6.2, 7.5)  │
  ├───────────────┼──────────────────────────────────────────────┤
  │ display_score │ boost_score مع soft cap عند 10 فقط          │
  │               │ يُستخدم في: UI والتقارير فقط                │
  │               │ 0→10 يفضل كما هو | فوق 10 → clamp at 10    │
  ├───────────────┼──────────────────────────────────────────────┤
  │effective_score│ display_score - resistance penalty           │
  │               │ للـ analytics/reporting فقط                 │
  └───────────────┴──────────────────────────────────────────────┘

القاعدة الأساسية المحمية:
  - Gate decisions  → boost_score  (same scale as config thresholds)
  - UI / Telegram   → display_score (soft-capped, familiar scale)
  - مفيش تغيير في الـ thresholds ولا في الـ behavior الأصلي
  - المستخدم يشوف: 6.2 = جيد | 7.5 = قوي | 10 = أقصى
──────────────────────────────────────────────────────────────────────────────
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

SMART_SL_MIN_PCT = 1.20
SMART_SL_MAX_PCT = 2.80
SMART_TP1_RR = 1.20
SMART_TP2_RR = 2.00

# ── Score Architecture Constants ───────────────────────────────────────────────
# Soft cap فقط — مش scaling كامل
# 0→10: يفضل كما هو | فوق 10: clamp
_SCORE_DISPLAY_CAP = 10.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(float(low), min(float(high), float(value)))


def _soft_cap_score(boost_score: float) -> float:
    """Soft cap للـ display فقط — يحافظ على الـ scale الطبيعي.

    - boost_score <= 10 → يفضل كما هو بدون تغيير
    - boost_score > 10  → يتعمله clamp عند 10
    - الـ gate decisions مش بتستخدم الدالة دي
    """
    return round(min(float(boost_score), _SCORE_DISPLAY_CAP), 2)


def _clamp_vol_ratio(vol_ratio: float) -> float:
    """Vol ratio — الـ minimum الفعلي 1.0 بدل 0.85 اللي كان dead code."""
    return round(_clamp(vol_ratio, 1.00, 2.20), 2)


def _smart_target_profile(
    pair: PairCandidate,
    setup_type: str,
    entry_timing: str,
    market_mode: str,
) -> dict[str, float | str]:
    """TP/SL profile — منطق محمي ومش بيتغير."""
    tags = set(pair.tags or [])
    change_abs = abs(float(pair.change_pct or 0.0))
    turnover = float(pair.turnover_usdt or 0.0)

    if market_mode == MODE_RECOVERY_LONG:
        risk_pct = 1.45
    elif entry_timing == "pullback":
        risk_pct = 1.70
    else:
        risk_pct = 1.55

    if setup_type == "support_bounce_confirmed":
        risk_pct += 0.15
    if "rebound" in tags:
        risk_pct += 0.15
    if "momentum" in tags or "breakout" in tags:
        risk_pct += 0.15
    if change_abs >= 0.80:
        risk_pct += 0.15
    if change_abs >= 1.40:
        risk_pct += 0.25
    if change_abs >= 2.20:
        risk_pct += 0.25
    if turnover > 0 and turnover < 250_000:
        risk_pct += 0.20
    if "major" in tags:
        risk_pct -= 0.10
    if "near_resistance" in tags:
        risk_pct -= 0.10

    risk_pct = round(_clamp(risk_pct, SMART_SL_MIN_PCT, SMART_SL_MAX_PCT), 4)
    return {
        "model": "smart_sl_1p2_2p8_tp1_1p2r_tp2_2r",
        "risk_pct": risk_pct,
        "rr1": SMART_TP1_RR,
        "rr2": SMART_TP2_RR,
        "sl_min_pct": SMART_SL_MIN_PCT,
        "sl_max_pct": SMART_SL_MAX_PCT,
    }


def _infer_setup(
    pair: PairCandidate,
    market_mode: str,
) -> tuple[str, str, list[str], list[str]]:
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


def _infer_quality_context(
    pair: PairCandidate,
    setup_type: str,
    entry_timing: str,
    boost_score: float,    # ✅ للـ gate logic — نفس scale الـ thresholds
    display_score: float,  # ✅ للـ UI فقط — soft-capped
    raw_score: float,      # ✅ للـ reference
    market_mode: str,
    warnings: list[str],
) -> dict:
    """Quality metadata للـ execution gate والـ analytics.

    ملاحظة معمارية مهمة:
    ─────────────────────
    effective_score = display_score - penalty
    display_score = soft_cap(boost_score)

    لما boost_score <= 10 (الحالة الطبيعية):
      display_score = boost_score → effective_score = boost_score - penalty
      ∴ الـ gate يشتغل على نفس الـ scale الأصلي ✅

    لما boost_score > 10 (حالات نادرة جداً — elite + كل الشروط):
      display_score = 10 → effective_score = 10 - penalty
      ∴ الـ gate بيشتغل على 10 كأقصى درجة ✅

    في الحالتين الـ gate behavior سليم ومتوافق مع الـ thresholds الموجودة.
    """
    tags = set(pair.tags or [])
    turnover = float(pair.turnover_usdt or 0.0)
    change = float(pair.change_pct or 0.0)

    # ── Vol ratio ──────────────────────────────────────────────────────────────
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
    vol_ratio = _clamp_vol_ratio(vol_ratio)

    # ── MTF / Breakout quality ─────────────────────────────────────────────────
    mtf_confirmed = bool(
        "rs_btc" in tags
        or setup_type in {"wave_3", "retest_breakout_confirmed", "relative_strength_vs_btc"}
        or ("major" in tags and change >= 0.75)
        or (change >= 2.2 and turnover >= 10_000_000)
    )

    if setup_type == "wave_3" and vol_ratio >= 1.12:
        breakout_quality = "strong"
    elif setup_type in {"retest_breakout_confirmed", "vwap_reclaim", "relative_strength_vs_btc"} and vol_ratio >= 1.08:
        breakout_quality = "good"
    elif setup_type in {"support_bounce_confirmed", "higher_low_continuation"}:
        breakout_quality = "ok"
    else:
        breakout_quality = ""

    dist_ma = round(min(abs(change) * 0.55, 4.5), 2)
    resistance_warning = (
        "near_resistance_before_tp1"
        if any("مقاومة" in str(w) for w in warnings) or "near_resistance" in tags
        else ""
    )
    setup_weight = _SETUP_WEIGHTS.get(setup_type, 0)
    if "elite" in tags or setup_type in ELITE_SETUPS:
        setup_weight = max(setup_weight, 3)

    # ✅ effective_score على نفس scale المألوف — مع penalty بس
    effective_score = round(display_score - (0.15 if resistance_warning else 0.0), 2)

    # ── Recovery bounce proxy ──────────────────────────────────────────────────
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
        # ── 4 scores واضحة بوظيفة كل واحدة ──────────────────────────────────
        "effective_score":  effective_score,           # display - penalty | analytics
        "display_score":    round(display_score, 2),   # soft-capped | UI فقط
        "boost_score":      round(boost_score, 2),     # raw + boosts | gate logic
        "raw_score":        round(raw_score, 2),       # قبل boosts | reference
        "score_scale":      "boost_scale_soft_capped_at_10",

        "vol_ratio":        vol_ratio,
        "mtf_confirmed":    mtf_confirmed,
        "dist_ma":          dist_ma,
        "breakout":         "breakout" in tags or setup_type in {"wave_3", "retest_breakout_confirmed"},
        "pre_breakout":     setup_type in {"vwap_reclaim", "higher_low_continuation", "support_bounce_confirmed"},
        "breakout_quality": breakout_quality,
        "setup_weight":     setup_weight,
        "resistance_warning": resistance_warning,
        "entry_maturity": (
            "healthy" if entry_timing == "market" and not resistance_warning
            else "pullback_first" if entry_timing == "pullback"
            else "watch_resistance"
        ),
        "wave_estimate":    3 if setup_type == "wave_3" else 0,
        "wave_context":     "wave_3" if setup_type == "wave_3" else setup_type,
        "volume_state":     f"vol_ratio_{vol_ratio:.2f}",
        "htf_confirmation": "yes" if mtf_confirmed else "no",
        "btc_bounce_pct":   round(btc_bounce_pct, 3),
        "symbol_bounce_pct": round(symbol_bounce_pct, 3),
        "bounce_ratio_vs_btc": round(bounce_ratio, 2),
        "bounce_faster_than_btc": bounce_faster_than_btc,
        "recovery_relative_bounce": bounce_faster_than_btc,
    }


def build_signal_candidate(
    pair: PairCandidate,
    market_mode: str,
    min_normal_score: float,
    min_strong_score: float,
) -> SignalCandidate | None:

    # ── Step 1: raw_score — قبل أي boosts ──────────────────────────────────────
    raw_score = pair.score_hint + pair.rebound_hint

    # ── Step 2: context boosts — منطق محمي ومش بيتغير ─────────────────────────
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

    setup_type, entry_timing, tags, warnings = _infer_setup(pair, market_mode)

    if "compression" in pair_tags and setup_type == "higher_low_continuation":
        boost_score -= 0.10

    rejection_reason = ""
    if market_mode == MODE_BLOCK_LONGS:
        if setup_type not in BLOCK_EXCEPTION_SETUPS or pair.turnover_usdt < 5_000_000:
            return None
        tags.append("block_exception")
        warnings.append("Block exception only")
        boost_score += 0.20

    # ── Step 3: threshold على boost_score مباشرة — نفس scale الـ config ────────
    # ✅ مفيش normalization — 6.2 يعني 6.2، 7.5 يعني 7.5
    threshold = min_normal_score
    if market_mode == MODE_STRONG_LONG_ONLY:
        threshold = min_strong_score
        if setup_type not in WHITELIST_SETUPS and boost_score < (min_strong_score + 0.30):
            return None
    elif market_mode == MODE_RECOVERY_LONG:
        threshold = min_normal_score + 0.10

    if boost_score < threshold or pair.last_price <= 0:
        return None

    # ── Step 4: soft cap للـ display فقط ───────────────────────────────────────
    # ✅ 0→10 يفضل كما هو | فوق 10 → clamp at 10
    # الـ gate فات بالفعل على boost_score — display للـ UI بس
    display_score = _soft_cap_score(boost_score)

    # ── Step 5: TP/SL ──────────────────────────────────────────────────────────
    target_profile = _smart_target_profile(pair, setup_type, entry_timing, market_mode)
    rr1 = float(target_profile["rr1"])
    rr2 = float(target_profile["rr2"])
    risk_pct = float(target_profile["risk_pct"])
    if "near_resistance" in pair_tags:
        rejection_reason = "near_resistance_warning"

    entry = pair.last_price
    sl = entry * (1.0 - risk_pct / 100.0)
    risk_amount = entry - sl
    tp1 = entry + (risk_amount * rr1)
    tp2 = entry + (risk_amount * rr2)

    if tp1 <= entry or tp2 <= tp1:
        return None

    # ── Step 6: quality context ────────────────────────────────────────────────
    quality_meta = _infer_quality_context(
        pair, setup_type, entry_timing,
        boost_score=boost_score,
        display_score=display_score,
        raw_score=raw_score,
        market_mode=market_mode,
        warnings=warnings,
    )
    if rejection_reason and not quality_meta.get("resistance_warning"):
        quality_meta["resistance_warning"] = rejection_reason

    return SignalCandidate(
        symbol=pair.symbol,
        entry=round(entry, 8),
        sl=round(sl, 8),
        tp1=round(tp1, 8),
        tp2=round(tp2, 8),
        # ✅ signal.score = display_score (soft-capped | familiar scale للمستخدم)
        score=display_score,
        setup_type=setup_type,
        entry_timing=entry_timing,
        market_mode=market_mode,
        execution_setup_tags=tags,
        warnings=warnings,
        notes=["v128b: soft_cap@10 | gate on boost_score | display only capped"],
        meta={
            "turnover_usdt":    pair.turnover_usdt,
            "change_pct":       pair.change_pct,
            "pair_tags":        list(pair.tags),
            "rr1":              rr1,
            "rr2":              rr2,
            "risk_pct":         risk_pct,
            "target_model":     target_profile.get("model"),
            "smart_sl_min_pct": target_profile.get("sl_min_pct"),
            "smart_sl_max_pct": target_profile.get("sl_max_pct"),
            "is_elite_setup":   setup_type in ELITE_SETUPS or "elite" in tags,
            "rejection_context": rejection_reason,
            "threshold_used":   round(threshold, 2),
            **quality_meta,
        },
    )
