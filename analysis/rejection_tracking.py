# analysis/rejection_tracking.py
"""
Rejected Candidates Tracking for OKX Scanner Bot - LONG.

هذا الملف مسؤول عن:
- تسجيل الفرص التي تم رفضها أثناء الفحص.
- حفظها مؤقتًا في Redis لمدة 7 أيام.
- بناء تقرير /report_rejections لمعرفة أكثر أسباب الرفض.
- تحسين مسميات أسباب الرفض عربيًا بدون تغيير reason الأصلي المستخدم في الكود.
- تصنيف أسباب الرفض إلى مجموعات تحليلية: توقيت / زخم / فوليوم / مقاومة / مود السوق / سكور / إلخ.

الفكرة:
بدل ما كل continue يضيع بدون أثر، نسجل سبب الرفض ونحلله لاحقًا.
"""

import json
import time
import html
import logging
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional


logger = logging.getLogger("okx-scanner")


# =========================
# CONFIG
# =========================
REJECTED_KEY_PREFIX = "rejected:long"
REJECTED_TTL_SECONDS = 7 * 24 * 3600
REJECTED_REPORT_LIMIT = 700


# =========================
# REASON LABELS / CATEGORIES
# =========================
REJECTION_REASON_LABELS = {
    # Timing / late entry
    "hard_late_entry": "دخول متأخر عام",
    "wave_5_no_pullback": "موجة خامسة بدون Pullback",
    "overextended_late_entry": "دخول متأخر بعد امتداد سعري",
    "late_entry_simple": "دخول متأخر بعيد عن المتوسط",
    "late_without_mtf": "دخول متأخر بدون تأكيد 1H",
    "late_move_without_breakout": "حركة متأخرة بدون Breakout",
    "late_high_risk_low_volume": "متأخر + مخاطرة عالية + فوليوم ضعيف",
    "chasing_4h_move": "مطاردة صعود قوي على 4H",

    # Recovery / weak rebound
    "post_dump_weak_rebound": "ارتداد ضعيف بعد هبوط",
    "weak_recovery_below_ma": "تعافي ضعيف أسفل MA",
    "no_structure_break": "لا يوجد كسر هيكل واضح",
    "low_volume_bounce": "ارتداد بفوليوم ضعيف",

    # Market mode / market guard
    "strong_only_no_valid_setup": "STRONG_ONLY بدون إعداد قوي",
    "strong_only_mtf_not_confirmed": "STRONG_ONLY بدون تأكيد 1H",
    "strong_only_low_volume": "STRONG_ONLY فوليوم ضعيف",
    "strong_only_weak_breakout": "STRONG_ONLY كسر ضعيف",
    "strong_only_late_entry": "STRONG_ONLY دخول متأخر",
    "late_guard_should_block": "Late Guard منع الإشارة",
    "market_block_longs": "Market Mode يمنع اللونج",

    # Momentum / indicators
    "rsi_momentum_weak": "RSI مرتفع أو الزخم يضعف",
    "macd_negative": "MACD سلبي بدون كسر",
    "macd_momentum_falling": "زخم MACD يتراجع",
    "momentum_exhaustion_trap": "فخ نهاية الزخم",
    "exhausted_long_move": "الحركة الصاعدة مرهقة",

    # Volume
    "low_volume_no_breakout": "فوليوم ضعيف بدون Breakout",
    "low_volume": "فوليوم ضعيف",

    # VWAP / MA / overextension
    "vwap_overextended_bull_market": "بعيد عن VWAP في سوق صاعد",
    "near_resistance": "مقاومة قريبة قبل TP1",

    # Breakout / retest
    "late_breakout_guard_blocked": "Late Breakout Guard منع الإشارة",
    "retest_required": "يحتاج Retest قبل الدخول",
    "weak_breakout": "Breakout ضعيف",

    # Score / threshold / ranking
    "final_threshold": "السكور أقل من الحد النهائي",
    "weak_historical_setup": "Setup ضعيف تاريخيًا",
    "top_momentum_filter": "تم استبعاده من Top Momentum",
    "top_momentum_min_score": "Top Momentum: السكور أقل من الحد",
    "top_momentum_plain_continuation_score": "Top Momentum: استمرار عادي بسكور غير كافٍ",
    "top_momentum_rank_cut": "Top Momentum: خارج أفضل الترتيب",
    "top_momentum_new_listing_limit": "Top Momentum: حد العملات الجديدة",
    "new_listing_filter": "فلتر العملات الجديدة رفض الإشارة",

    # Candle / duplicate / timing
    "invalid_candle_timing": "توقيت الشمعة غير صالح",
    "duplicate_candle": "نفس الشمعة مرسلة سابقًا",
    "cooldown": "العملة داخل فترة Cooldown",
    "local_same_candle_cache": "مكرر محليًا لنفس الشمعة",
    "local_recent_send_cache": "إرسال حديث محليًا",

    # Entry maturity
    "entry_maturity_block": "Entry Maturity منع الإشارة",
    "early_without_confirmation": "إشارة مبكرة بدون تأكيد كافٍ",

    # Reverse / falling knife
    "falling_knife": "خطر Falling Knife",
    "oversold_reversal_not_confirmed": "Oversold Reversal غير مؤكد",
}


REJECTION_REASON_CATEGORIES = {
    # Timing
    "hard_late_entry": "timing_late",
    "wave_5_no_pullback": "timing_late",
    "overextended_late_entry": "timing_late",
    "late_entry_simple": "timing_late",
    "late_without_mtf": "timing_late",
    "late_move_without_breakout": "timing_late",
    "late_high_risk_low_volume": "timing_late",
    "chasing_4h_move": "timing_late",

    # Weak rebound
    "post_dump_weak_rebound": "weak_rebound",
    "weak_recovery_below_ma": "weak_rebound",
    "no_structure_break": "weak_rebound",
    "low_volume_bounce": "weak_rebound",

    # Market mode
    "strong_only_no_valid_setup": "market_mode",
    "strong_only_mtf_not_confirmed": "market_mode",
    "strong_only_low_volume": "market_mode",
    "strong_only_weak_breakout": "market_mode",
    "strong_only_late_entry": "market_mode",
    "late_guard_should_block": "market_mode",
    "market_block_longs": "market_mode",

    # Momentum
    "rsi_momentum_weak": "momentum",
    "macd_negative": "momentum",
    "macd_momentum_falling": "momentum",
    "momentum_exhaustion_trap": "momentum",
    "exhausted_long_move": "momentum",

    # Volume
    "low_volume_no_breakout": "volume",
    "low_volume": "volume",

    # Extension / resistance
    "vwap_overextended_bull_market": "overextension",
    "near_resistance": "resistance",

    # Breakout
    "late_breakout_guard_blocked": "breakout_quality",
    "retest_required": "breakout_quality",
    "weak_breakout": "breakout_quality",

    # Score
    "final_threshold": "score_threshold",
    "weak_historical_setup": "historical_setup",
    "top_momentum_filter": "top_momentum",
    "top_momentum_min_score": "top_momentum",
    "top_momentum_plain_continuation_score": "top_momentum",
    "top_momentum_rank_cut": "top_momentum",
    "top_momentum_new_listing_limit": "top_momentum",
    "new_listing_filter": "new_listing",

    # Candle / duplicate
    "invalid_candle_timing": "data_timing",
    "duplicate_candle": "duplicate",
    "cooldown": "duplicate",
    "local_same_candle_cache": "duplicate",
    "local_recent_send_cache": "duplicate",

    # Entry maturity
    "entry_maturity_block": "entry_maturity",
    "early_without_confirmation": "confirmation",

    # Reverse
    "falling_knife": "reverse_risk",
    "oversold_reversal_not_confirmed": "reverse_risk",
}


CATEGORY_LABELS = {
    "timing_late": "توقيت دخول متأخر",
    "weak_rebound": "ارتداد / تعافي ضعيف",
    "market_mode": "قيود Market Mode",
    "momentum": "ضعف الزخم",
    "volume": "الفوليوم",
    "overextension": "امتداد زائد",
    "resistance": "مقاومة قريبة",
    "breakout_quality": "جودة الكسر / Retest",
    "score_threshold": "السكور والحد النهائي",
    "historical_setup": "أداء تاريخي ضعيف",
    "top_momentum": "فلتر Top Momentum",
    "new_listing": "فلتر عملة جديدة",
    "data_timing": "توقيت / بيانات الشمعة",
    "duplicate": "منع التكرار",
    "entry_maturity": "Entry Maturity",
    "confirmation": "نقص التأكيد",
    "reverse_risk": "مخاطر Reversal",
    "other": "أسباب أخرى",
}


# =========================
# BASIC HELPERS
# =========================
def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        value = float(value)
        if value != value:
            return default
        if value in (float("inf"), float("-inf")):
            return default
        return value
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _avg(values: List[Any]) -> float:
    try:
        cleaned = []
        for v in values or []:
            try:
                f = float(v)
                if f == f:
                    cleaned.append(f)
            except Exception:
                continue

        if not cleaned:
            return 0.0

        return sum(cleaned) / len(cleaned)
    except Exception:
        return 0.0


def _limit_telegram_message(text: str, limit: int = 3900) -> str:
    try:
        text = str(text or "")
        if len(text) <= limit:
            return text

        return (
            text[:limit - 200]
            + "\n\n⚠️ تم اختصار التقرير لأن حجمه أكبر من حد Telegram."
        )
    except Exception:
        return "❌ فشل اختصار الرسالة"


def clean_symbol_for_message(symbol: str) -> str:
    try:
        return str(symbol or "UNKNOWN").replace("-SWAP", "")
    except Exception:
        return "UNKNOWN"


def normalize_rejection_reason(reason: Any) -> str:
    try:
        value = str(reason or "unknown").strip()
        if not value:
            return "unknown"
        return value
    except Exception:
        return "unknown"


def get_reason_label(reason: Any) -> str:
    normalized = normalize_rejection_reason(reason)
    return REJECTION_REASON_LABELS.get(normalized, normalized)


def get_reason_category(reason: Any) -> str:
    normalized = normalize_rejection_reason(reason)
    return REJECTION_REASON_CATEGORIES.get(normalized, "other")


def get_category_label(category: Any) -> str:
    value = str(category or "other").strip() or "other"
    return CATEGORY_LABELS.get(value, value)


def _json_safe(value: Any) -> Any:
    """
    يحاول جعل البيانات قابلة للتخزين في JSON.
    مهم لأن بعض القيم قد تأتي من pandas/numpy أو objects غير قابلة للتسلسل.
    """
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except Exception:
        pass

    if isinstance(value, dict):
        safe_dict = {}
        for k, v in value.items():
            safe_dict[str(k)] = _json_safe(v)
        return safe_dict

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    try:
        return float(value)
    except Exception:
        return str(value)


def _extract_extra_tags(extra: Any, max_tags: int = 5) -> List[str]:
    """
    يستخرج Tags مفيدة من extra لعرضها في آخر حالات الرفض.
    لا يغير البيانات المخزنة، فقط للعرض.
    """
    tags: List[str] = []

    try:
        if not isinstance(extra, dict):
            return tags

        if extra.get("breakout_quality"):
            tags.append(f"BQ={extra.get('breakout_quality')}")

        if extra.get("guard_reason"):
            tags.append(f"guard={str(extra.get('guard_reason'))[:28]}")

        if extra.get("upper_wick_ratio") is not None:
            tags.append(f"wick={_safe_float(extra.get('upper_wick_ratio'), 0):.2f}")

        if extra.get("nearest_resistance") is not None:
            tags.append(f"res={_safe_float(extra.get('nearest_resistance'), 0):.6g}")

        if extra.get("resistance_warning"):
            tags.append(str(extra.get("resistance_warning"))[:28])

        if extra.get("dynamic_threshold") is not None:
            tags.append(f"dyn={_safe_float(extra.get('dynamic_threshold'), 0):.2f}")

        if extra.get("required_min_score") is not None:
            tags.append(f"req={_safe_float(extra.get('required_min_score'), 0):.2f}")

        if extra.get("early_priority"):
            tags.append(f"early={extra.get('early_priority')}")

        if extra.get("strong_bull_pullback") is not None:
            tags.append(f"pullback={bool(extra.get('strong_bull_pullback'))}")

        if extra.get("late_guard_reasons"):
            reasons = extra.get("late_guard_reasons") or []
            if isinstance(reasons, list) and reasons:
                tags.append("late=" + ",".join(str(x)[:12] for x in reasons[:2]))

        if extra.get("trap_reasons"):
            reasons = extra.get("trap_reasons") or []
            if isinstance(reasons, list) and reasons:
                tags.append("trap=" + ",".join(str(x)[:12] for x in reasons[:2]))

        if extra.get("adjustments_log"):
            names = []
            for adj in extra.get("adjustments_log") or []:
                if isinstance(adj, dict) and adj.get("name"):
                    names.append(str(adj.get("name")))
            if names:
                tags.append("adj=" + ",".join(names[:2]))

        return tags[:max_tags]

    except Exception:
        return tags[:max_tags]


# =========================
# KEY HELPERS
# =========================
def get_rejected_key(symbol: str, candle_time: int, reason: str) -> str:
    safe_symbol = str(symbol or "UNKNOWN").replace(":", "_")
    safe_reason = normalize_rejection_reason(reason).replace(":", "_")
    safe_candle_time = _safe_int(candle_time, int(time.time()))

    return f"{REJECTED_KEY_PREFIX}:{safe_reason}:{safe_symbol}:{safe_candle_time}"


# =========================
# WRITE
# =========================
def log_rejected_candidate(
    redis_client,
    symbol: str,
    reason: str,
    candle_time: Optional[int] = None,
    score: Optional[float] = None,
    raw_score: Optional[float] = None,
    final_threshold: Optional[float] = None,
    market_state: str = "",
    current_mode: str = "",
    setup_type: str = "",
    entry_timing: str = "",
    opportunity_type: str = "",
    dist_ma: Optional[float] = None,
    rsi_now: Optional[float] = None,
    vol_ratio: Optional[float] = None,
    vwap_distance: Optional[float] = None,
    mtf_confirmed: Optional[bool] = None,
    breakout: Optional[bool] = None,
    pre_breakout: Optional[bool] = None,
    is_reverse: Optional[bool] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    يسجل فرصة مرفوضة في Redis.

    يرجع:
    - True لو تم التسجيل
    - False لو Redis غير متصل أو حدث خطأ

    ملاحظة:
    هذه الدالة لا يجب أن توقف البوت أبدًا.
    """
    if not redis_client:
        return False

    try:
        now_ts = int(time.time())
        candle_time = _safe_int(candle_time, now_ts)
        normalized_reason = normalize_rejection_reason(reason)
        reason_label = get_reason_label(normalized_reason)
        reason_category = get_reason_category(normalized_reason)

        payload = {
            "created_ts": now_ts,
            "symbol": str(symbol or "UNKNOWN"),
            "reason": normalized_reason,
            "reason_label": reason_label,
            "reason_category": reason_category,
            "reason_category_label": get_category_label(reason_category),
            "candle_time": candle_time,
            "score": None if score is None else _safe_float(score, 0.0),
            "raw_score": None if raw_score is None else _safe_float(raw_score, 0.0),
            "final_threshold": None if final_threshold is None else _safe_float(final_threshold, 0.0),
            "market_state": str(market_state or ""),
            "current_mode": str(current_mode or ""),
            "setup_type": str(setup_type or ""),
            "entry_timing": str(entry_timing or ""),
            "opportunity_type": str(opportunity_type or ""),
            "dist_ma": None if dist_ma is None else _safe_float(dist_ma, 0.0),
            "rsi_now": None if rsi_now is None else _safe_float(rsi_now, 0.0),
            "vol_ratio": None if vol_ratio is None else _safe_float(vol_ratio, 0.0),
            "vwap_distance": None if vwap_distance is None else _safe_float(vwap_distance, 0.0),
            "mtf_confirmed": None if mtf_confirmed is None else bool(mtf_confirmed),
            "breakout": None if breakout is None else bool(breakout),
            "pre_breakout": None if pre_breakout is None else bool(pre_breakout),
            "is_reverse": None if is_reverse is None else bool(is_reverse),
            "extra": _json_safe(extra or {}),
        }

        key = get_rejected_key(symbol, candle_time, normalized_reason)

        redis_client.set(
            key,
            json.dumps(payload, ensure_ascii=False),
            ex=REJECTED_TTL_SECONDS,
        )

        return True

    except Exception as e:
        logger.warning(f"log_rejected_candidate error: {e}")
        return False


# =========================
# READ
# =========================
def load_rejected_candidates(
    redis_client,
    limit: int = REJECTED_REPORT_LIMIT,
) -> List[Dict[str, Any]]:
    """
    تحميل آخر الفرص المرفوضة من Redis باستخدام scan_iter وليس keys.
    """
    items: List[Dict[str, Any]] = []

    if not redis_client:
        return items

    try:
        count = 0

        for key in redis_client.scan_iter(f"{REJECTED_KEY_PREFIX}:*"):
            if count >= int(limit):
                break

            try:
                raw = redis_client.get(key)
                if not raw:
                    continue

                data = json.loads(raw)
                if isinstance(data, dict):
                    reason = normalize_rejection_reason(data.get("reason", "unknown"))
                    reason_category = data.get("reason_category") or get_reason_category(reason)

                    data["reason"] = reason
                    data["reason_label"] = data.get("reason_label") or get_reason_label(reason)
                    data["reason_category"] = reason_category
                    data["reason_category_label"] = data.get("reason_category_label") or get_category_label(reason_category)
                    data["_redis_key"] = key

                    items.append(data)
                    count += 1

            except Exception:
                continue

        items.sort(
            key=lambda x: _safe_int(x.get("created_ts"), 0),
            reverse=True,
        )

        return items

    except Exception as e:
        logger.error(f"load_rejected_candidates error: {e}")
        return items


# =========================
# REPORT HELPERS
# =========================
def _format_counter_block(title: str, counter: Counter, max_items: int = 8) -> List[str]:
    lines = [f"<b>{html.escape(title)}</b>"]

    if not counter:
        lines.append("• لا توجد بيانات")
        return lines

    for key, count in counter.most_common(max_items):
        safe_key = html.escape(str(key or "unknown"))
        lines.append(f"• {safe_key}: {count}")

    return lines


def _format_category_counter_block(counter: Counter, max_items: int = 8) -> List[str]:
    lines = ["<b>🧭 أسباب الرفض حسب التصنيف:</b>"]

    if not counter:
        lines.append("• لا توجد بيانات")
        return lines

    for category, count in counter.most_common(max_items):
        label = get_category_label(category)
        lines.append(f"• {html.escape(label)}: {count}")

    return lines


def _build_reason_stats(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "total": 0,
        "label": "",
        "category": "",
        "category_label": "",
        "scores": [],
        "raw_scores": [],
        "thresholds": [],
        "gaps": [],
        "dist_ma": [],
        "rsi": [],
        "vol": [],
        "vwap": [],
        "mtf_yes": 0,
        "breakout_yes": 0,
        "pre_breakout_yes": 0,
        "reverse_yes": 0,
    })

    for item in items:
        reason = normalize_rejection_reason(item.get("reason", "unknown"))
        s = stats[reason]
        s["total"] += 1
        s["label"] = item.get("reason_label") or get_reason_label(reason)
        s["category"] = item.get("reason_category") or get_reason_category(reason)
        s["category_label"] = item.get("reason_category_label") or get_category_label(s["category"])

        score = item.get("score")
        raw_score = item.get("raw_score")
        threshold = item.get("final_threshold")

        if score is not None:
            s["scores"].append(_safe_float(score, 0.0))

        if raw_score is not None:
            s["raw_scores"].append(_safe_float(raw_score, 0.0))

        if threshold is not None:
            s["thresholds"].append(_safe_float(threshold, 0.0))

        if score is not None and threshold is not None:
            s["gaps"].append(_safe_float(score, 0.0) - _safe_float(threshold, 0.0))

        if item.get("dist_ma") is not None:
            s["dist_ma"].append(_safe_float(item.get("dist_ma"), 0.0))

        if item.get("rsi_now") is not None:
            s["rsi"].append(_safe_float(item.get("rsi_now"), 0.0))

        if item.get("vol_ratio") is not None:
            s["vol"].append(_safe_float(item.get("vol_ratio"), 0.0))

        if item.get("vwap_distance") is not None:
            s["vwap"].append(_safe_float(item.get("vwap_distance"), 0.0))

        if item.get("mtf_confirmed") is True:
            s["mtf_yes"] += 1

        if item.get("breakout") is True:
            s["breakout_yes"] += 1

        if item.get("pre_breakout") is True:
            s["pre_breakout_yes"] += 1

        if item.get("is_reverse") is True:
            s["reverse_yes"] += 1

    return stats


def _format_reason_stats(items: List[Dict[str, Any]], max_items: int = 10) -> List[str]:
    reason_stats = _build_reason_stats(items)

    sorted_reasons = sorted(
        reason_stats.items(),
        key=lambda x: x[1].get("total", 0),
        reverse=True,
    )

    lines = ["<b>📌 أكثر أسباب الرفض بالتفصيل:</b>"]

    if not sorted_reasons:
        lines.append("• لا توجد بيانات")
        return lines

    for reason, info in sorted_reasons[:max_items]:
        total = int(info.get("total", 0) or 0)

        avg_score = _avg(info.get("scores", []))
        avg_gap = _avg(info.get("gaps", []))
        avg_dist = _avg(info.get("dist_ma", []))
        avg_rsi = _avg(info.get("rsi", []))
        avg_vol = _avg(info.get("vol", []))
        avg_vwap = _avg(info.get("vwap", []))

        label = info.get("label") or get_reason_label(reason)
        category_label = info.get("category_label") or get_category_label(info.get("category"))

        details = [
            f"• <b>{html.escape(str(label))}</b>",
            f"count={total}",
            f"cat={html.escape(str(category_label))}",
        ]

        if str(label) != str(reason):
            details.append(f"code={html.escape(str(reason))}")

        if info.get("scores"):
            details.append(f"AvgScore={avg_score:.2f}")

        if info.get("gaps"):
            details.append(f"Gap={avg_gap:+.2f}")

        if info.get("dist_ma"):
            details.append(f"DistMA={avg_dist:+.2f}%")

        if info.get("rsi"):
            details.append(f"RSI={avg_rsi:.1f}")

        if info.get("vol"):
            details.append(f"Vol={avg_vol:.2f}x")

        if info.get("vwap"):
            details.append(f"VWAP={avg_vwap:+.2f}%")

        total_safe = max(1, total)
        mtf_rate = (int(info.get("mtf_yes", 0)) / total_safe) * 100
        breakout_rate = (int(info.get("breakout_yes", 0)) / total_safe) * 100
        pre_breakout_rate = (int(info.get("pre_breakout_yes", 0)) / total_safe) * 100
        reverse_rate = (int(info.get("reverse_yes", 0)) / total_safe) * 100

        details.append(f"MTF={mtf_rate:.0f}%")
        details.append(f"BO={breakout_rate:.0f}%")
        details.append(f"PreBO={pre_breakout_rate:.0f}%")
        details.append(f"Rev={reverse_rate:.0f}%")

        lines.append(" | ".join(details))

    return lines


def _format_latest_rejections(items: List[Dict[str, Any]], max_items: int = 10) -> List[str]:
    lines = ["<b>🔹 آخر حالات رفض:</b>"]

    if not items:
        lines.append("• لا توجد بيانات")
        return lines

    for item in items[:max_items]:
        symbol = clean_symbol_for_message(str(item.get("symbol", "UNKNOWN")))
        reason = normalize_rejection_reason(item.get("reason", "unknown"))
        reason_label = item.get("reason_label") or get_reason_label(reason)
        score = item.get("score")
        threshold = item.get("final_threshold")
        entry_timing = str(item.get("entry_timing", ""))[:35]
        market_state = str(item.get("market_state", ""))[:20]
        current_mode = str(item.get("current_mode", ""))[:25]
        extra_tags = _extract_extra_tags(item.get("extra", {}), max_tags=4)

        score_text = ""
        try:
            if score is not None:
                score_text += f" | Score={float(score):.2f}"
            if threshold is not None:
                score_text += f"/{float(threshold):.2f}"
        except Exception:
            pass

        line = (
            f"• {html.escape(symbol)} | "
            f"{html.escape(str(reason_label))}"
            f"{score_text}"
        )

        if str(reason_label) != str(reason):
            line += f" | {html.escape(reason)}"

        if entry_timing:
            line += f" | {html.escape(entry_timing)}"

        if market_state:
            line += f" | {html.escape(market_state)}"

        if current_mode:
            line += f" | {html.escape(current_mode)}"

        if extra_tags:
            line += " | " + html.escape(" / ".join(extra_tags))

        lines.append(line)

    return lines


def _format_quick_insights(items: List[Dict[str, Any]]) -> List[str]:
    """
    ملخص تحليلي سريع يساعدك تعرف هل المشكلة Over-filtering ولا ضعف حقيقي.
    """
    lines = ["<b>🧠 قراءة سريعة:</b>"]

    if not items:
        lines.append("• لا توجد بيانات كافية")
        return lines

    total = len(items)
    total_safe = max(1, total)

    final_threshold_count = sum(1 for x in items if normalize_rejection_reason(x.get("reason")) == "final_threshold")
    near_resistance_count = sum(1 for x in items if normalize_rejection_reason(x.get("reason")) == "near_resistance")
    late_count = sum(
        1 for x in items
        if get_reason_category(x.get("reason")) == "timing_late"
    )
    top_momentum_count = sum(
        1 for x in items
        if get_reason_category(x.get("reason")) == "top_momentum"
    )
    market_mode_count = sum(
        1 for x in items
        if get_reason_category(x.get("reason")) == "market_mode"
    )
    momentum_count = sum(
        1 for x in items
        if get_reason_category(x.get("reason")) == "momentum"
    )

    lines.append(f"• final_threshold: {(final_threshold_count / total_safe) * 100:.1f}% من الرفض")
    lines.append(f"• timing_late: {(late_count / total_safe) * 100:.1f}%")
    lines.append(f"• top_momentum: {(top_momentum_count / total_safe) * 100:.1f}%")
    lines.append(f"• market_mode: {(market_mode_count / total_safe) * 100:.1f}%")
    lines.append(f"• momentum_weak: {(momentum_count / total_safe) * 100:.1f}%")
    lines.append(f"• near_resistance: {(near_resistance_count / total_safe) * 100:.1f}%")

    if final_threshold_count / total_safe >= 0.35:
        lines.append("• ملاحظة: نسبة كبيرة مرفوضة بسبب الحد النهائي؛ راقب هل الفلترة شديدة زيادة.")
    if late_count / total_safe >= 0.25:
        lines.append("• ملاحظة: الرفض المتأخر عالي؛ Entry Maturity / Late Guard شغالين بقوة.")
    if top_momentum_count / total_safe >= 0.25:
        lines.append("• ملاحظة: Top Momentum يقطع عدد كبير؛ راجع TOP_MOMENTUM_PERCENT والحد الأدنى.")
    if near_resistance_count / total_safe >= 0.15:
        lines.append("• ملاحظة: مقاومات TP1 قريبة كثيرًا؛ راجع Smart TP round/resistance logic.")

    return lines


# =========================
# MAIN REPORT
# =========================
def build_rejections_report_message(
    redis_client,
    limit: int = REJECTED_REPORT_LIMIT,
) -> str:
    """
    يبني رسالة Telegram لتقرير الفرص المرفوضة.

    يستخدم هكذا من main.py:
        build_rejections_report_message(r)
    """
    try:
        items = load_rejected_candidates(redis_client, limit=limit)

        if not items:
            return "ℹ️ لا توجد بيانات رفض مسجلة خلال آخر 7 أيام"

        total = len(items)

        reason_counter = Counter(
            str(x.get("reason_label") or get_reason_label(x.get("reason", "unknown")))
            for x in items
        )

        reason_code_counter = Counter(
            normalize_rejection_reason(x.get("reason", "unknown"))
            for x in items
        )

        category_counter = Counter(
            str(x.get("reason_category", get_reason_category(x.get("reason", "unknown"))) or "other")
            for x in items
        )

        mode_counter = Counter(
            str(x.get("current_mode", "unknown") or "unknown")
            for x in items
        )

        market_counter = Counter(
            str(x.get("market_state", "unknown") or "unknown")
            for x in items
        )

        setup_counter = Counter(
            str(x.get("setup_type", "unknown") or "unknown")
            for x in items
            if str(x.get("setup_type", "") or "").strip()
        )

        entry_timing_counter = Counter(
            str(x.get("entry_timing", "unknown") or "unknown")
            for x in items
            if str(x.get("entry_timing", "") or "").strip()
        )

        opportunity_counter = Counter(
            str(x.get("opportunity_type", "unknown") or "unknown")
            for x in items
            if str(x.get("opportunity_type", "") or "").strip()
        )

        mtf_yes = sum(1 for x in items if x.get("mtf_confirmed") is True)
        breakout_yes = sum(1 for x in items if x.get("breakout") is True)
        pre_breakout_yes = sum(1 for x in items if x.get("pre_breakout") is True)
        reverse_yes = sum(1 for x in items if x.get("is_reverse") is True)

        score_values = [
            _safe_float(x.get("score"), 0.0)
            for x in items
            if x.get("score") is not None
        ]

        gap_values = []
        for x in items:
            score = x.get("score")
            threshold = x.get("final_threshold")
            if score is not None and threshold is not None:
                gap_values.append(_safe_float(score, 0.0) - _safe_float(threshold, 0.0))

        avg_score = _avg(score_values)
        avg_gap = _avg(gap_values)

        total_safe = max(1, total)

        lines = [
            "🚫 <b>Rejected Candidates Report - LONG</b>",
            "",
            f"• إجمالي الرفض المسجل: {total}",
            "• الفترة: آخر 7 أيام تقريبًا",
            "",
            "<b>📊 ملخص سريع:</b>",
            f"• MTF Confirmed: {(mtf_yes / total_safe) * 100:.1f}%",
            f"• Breakout: {(breakout_yes / total_safe) * 100:.1f}%",
            f"• Pre-Breakout: {(pre_breakout_yes / total_safe) * 100:.1f}%",
            f"• Reverse: {(reverse_yes / total_safe) * 100:.1f}%",
        ]

        if score_values:
            lines.append(f"• Avg Score: {avg_score:.2f}")

        if gap_values:
            lines.append(f"• Avg Score Gap: {avg_gap:+.2f}")

        lines.append("")
        lines.extend(_format_quick_insights(items))

        lines.append("")
        lines.extend(_format_category_counter_block(category_counter, max_items=8))

        lines.append("")
        lines.extend(_format_reason_stats(items, max_items=10))

        lines.append("")
        lines.extend(_format_counter_block("🏷 أكثر أكواد الرفض:", reason_code_counter, max_items=8))

        lines.append("")
        lines.extend(_format_counter_block("⚙️ حسب المود:", mode_counter, max_items=6))

        lines.append("")
        lines.extend(_format_counter_block("🌍 حسب حالة السوق:", market_counter, max_items=6))

        if opportunity_counter:
            lines.append("")
            lines.extend(_format_counter_block("🧠 حسب نوع الفرصة:", opportunity_counter, max_items=6))

        if setup_counter:
            lines.append("")
            lines.extend(_format_counter_block("🧬 حسب setup_type:", setup_counter, max_items=6))

        if entry_timing_counter:
            lines.append("")
            lines.extend(_format_counter_block("📍 حسب entry_timing:", entry_timing_counter, max_items=6))

        lines.append("")
        lines.extend(_format_latest_rejections(items, max_items=10))

        return _limit_telegram_message("\n".join(lines))

    except Exception as e:
        logger.exception(f"build_rejections_report_message error: {e}")
        return f"❌ حصل خطأ أثناء بناء تقرير الرفض\n{html.escape(str(e))}"


# =========================
# OPTIONAL CLEANUP
# =========================
def clear_rejected_candidates(redis_client, max_delete: int = 5000) -> int:
    """
    دالة اختيارية لو احتجت تصفير سجل الرفض يدويًا لاحقًا.
    لا تستدعيها من main.py إلا لو هتعمل أمر admin مخصوص.
    """
    if not redis_client:
        return 0

    deleted = 0

    try:
        for key in redis_client.scan_iter(f"{REJECTED_KEY_PREFIX}:*"):
            if deleted >= max_delete:
                break
            try:
                redis_client.delete(key)
                deleted += 1
            except Exception:
                continue

        return deleted

    except Exception as e:
        logger.error(f"clear_rejected_candidates error: {e}")
        return deleted
