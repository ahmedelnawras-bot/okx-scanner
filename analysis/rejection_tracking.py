# analysis/rejection_tracking.py
"""
Rejected Candidates Tracking for OKX Scanner Bot - LONG.

هذا الملف مسؤول عن:
- تسجيل الفرص التي تم رفضها أثناء الفحص.
- حفظها مؤقتًا في Redis لمدة 7 أيام.
- بناء تقرير /report_rejections لمعرفة أكثر أسباب الرفض.

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
# BASIC HELPERS
# =========================
def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        value = float(value)
        if value != value:  # NaN
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


# =========================
# KEY HELPERS
# =========================
def get_rejected_key(symbol: str, candle_time: int, reason: str) -> str:
    safe_symbol = str(symbol or "UNKNOWN").replace(":", "_")
    safe_reason = str(reason or "unknown").replace(":", "_")
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

        payload = {
            "created_ts": now_ts,
            "symbol": symbol,
            "reason": str(reason or "unknown"),
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

        key = get_rejected_key(symbol, candle_time, reason)
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


def _build_reason_stats(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "total": 0,
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
        reason = str(item.get("reason", "unknown") or "unknown")
        s = stats[reason]
        s["total"] += 1

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

        details = [f"• <b>{html.escape(str(reason))}</b>: {total}"]

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
        reason = str(item.get("reason", "unknown"))
        score = item.get("score")
        threshold = item.get("final_threshold")
        entry_timing = str(item.get("entry_timing", ""))[:35]
        market_state = str(item.get("market_state", ""))[:20]
        current_mode = str(item.get("current_mode", ""))[:25]

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
            f"{html.escape(reason)}"
            f"{score_text}"
        )

        if entry_timing:
            line += f" | {html.escape(entry_timing)}"

        if market_state:
            line += f" | {html.escape(market_state)}"

        if current_mode:
            line += f" | {html.escape(current_mode)}"

        lines.append(line)

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
            str(x.get("reason", "unknown") or "unknown")
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

        lines.extend(_format_reason_stats(items, max_items=10))

        lines.append("")
        lines.extend(_format_counter_block("⚙️ حسب المود:", mode_counter, max_items=6))

        lines.append("")
        lines.extend(_format_counter_block("🌍 حسب حالة السوق:", market_counter, max_items=6))

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
