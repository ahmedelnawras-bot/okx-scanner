"""Candle Reversal Gate — Execution Layer Only.

فلسفة:
- Normal Mode:  منع شراء القمم (bearish reversal patterns)
- Recovery Mode: منع شراء Falling Knife (يشترط bullish reversal)

قواعد ثابتة:
- يعمل داخل Execution Layer فقط
- لا يغير الـ Score أو الـ Ranking
- يستخدم آخر شموع مغلقة على 15m فقط
- لا HTF / لا 1H / لا 4H
- لا Market Structure معقد
- خفيف وسريع
"""
from __future__ import annotations

from typing import Any


# ─────────────────────────────────────────
# Thresholds
# ─────────────────────────────────────────

# النسبة الدنيا لحجم body يُعتبر معنوي
MIN_BODY_PCT = 0.15        # 0.15% من السعر

# نسبة الـ wick للـ range عشان يُعتبر rejection
UPPER_WICK_REJECTION_RATIO = 0.55   # upper wick > 55% من total range
LOWER_WICK_REVERSAL_RATIO  = 0.55   # lower wick > 55% من total range

# الحد الأدنى للـ body في engulfing
MIN_ENGULF_BODY_PCT = 0.20

# Recovery: يكفي pattern واحد
# Normal: أي bearish pattern يمنع


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default




def _candle_color(metric: dict) -> str:
    if not metric:
        return "unknown"
    return "green" if bool(metric.get("is_bullish")) else "red"


def _body_ratio(metric: dict) -> float:
    total_range = _safe_float(metric.get("total_range"), 0.0) if metric else 0.0
    body = _safe_float(metric.get("body"), 0.0) if metric else 0.0
    return body / total_range if total_range > 0 else 0.0


def _candle_export_fields(
    curr: dict | None = None,
    closed: list[dict] | None = None,
    *,
    pattern: str = "",
    reversal_strength: float = 0.0,
    reversal_kind: str = "",
) -> dict:
    """Return export-only candle analytics without changing gate logic."""
    curr = curr or {}
    closed = closed or []

    upper_wick_ratio = _safe_float(curr.get("upper_wick_ratio"), 0.0)
    lower_wick_ratio = _safe_float(curr.get("lower_wick_ratio"), 0.0)

    if reversal_kind == "bullish":
        wick_ratio = lower_wick_ratio
    elif reversal_kind == "bearish":
        wick_ratio = upper_wick_ratio
    else:
        wick_ratio = max(upper_wick_ratio, lower_wick_ratio)

    return {
        "entry_pattern": str(pattern or ""),
        "reversal_type": str(pattern or ""),
        "wick_ratio": wick_ratio,
        "body_ratio": _body_ratio(curr),
        "body_pct": _safe_float(curr.get("body_pct"), 0.0),
        "upper_wick_ratio": upper_wick_ratio,
        "lower_wick_ratio": lower_wick_ratio,
        "candle_strength": _safe_float(reversal_strength, 0.0),
        "last_3_candles": [_candle_color(c) for c in list(closed or [])[:3]],
    }


def _candle_metrics(c: dict) -> dict:
    """احسب المقاييس الأساسية لشمعة واحدة."""
    o = _safe_float(c.get("open"))
    h = _safe_float(c.get("high"))
    l = _safe_float(c.get("low"))
    cl = _safe_float(c.get("close"))

    if o <= 0 or h <= l:
        return {}

    total_range = h - l
    body = abs(cl - o)
    upper_wick = h - max(cl, o)
    lower_wick = min(cl, o) - l
    is_bullish = cl >= o
    mid = (h + l) / 2.0
    close_position = (cl - l) / total_range if total_range > 0 else 0.5
    body_pct = (body / o) * 100.0 if o > 0 else 0.0

    return {
        "open": o, "high": h, "low": l, "close": cl,
        "body": body,
        "body_pct": body_pct,
        "upper_wick": upper_wick,
        "lower_wick": lower_wick,
        "total_range": total_range,
        "is_bullish": is_bullish,
        "mid": mid,
        "close_position": close_position,  # 0=low end, 1=high end
        "upper_wick_ratio": upper_wick / total_range if total_range > 0 else 0,
        "lower_wick_ratio": lower_wick / total_range if total_range > 0 else 0,
    }


# ─────────────────────────────────────────
# BEARISH PATTERNS — Normal Mode
# ─────────────────────────────────────────

def _is_shooting_star(curr: dict) -> tuple[bool, str]:
    """Shooting Star: جسم صغير في الأسفل، ذيل علوي طويل."""
    if not curr:
        return False, ""
    upper = curr.get("upper_wick_ratio", 0)
    body = curr.get("body_pct", 0)
    close_pos = curr.get("close_position", 0.5)

    if upper >= 0.60 and body <= MIN_BODY_PCT * 2 and close_pos <= 0.35:
        return True, "shooting_star"
    return False, ""


def _is_bearish_engulfing(curr: dict, prev: dict) -> tuple[bool, str]:
    """Bearish Engulfing: شمعة حمراء تبتلع الخضراء السابقة."""
    if not curr or not prev:
        return False, ""
    if curr.get("is_bullish") or not prev.get("is_bullish"):
        return False, ""

    # current body يغطي previous body
    curr_open = curr.get("open", 0)
    curr_close = curr.get("close", 0)
    prev_open = prev.get("open", 0)
    prev_close = prev.get("close", 0)

    engulfs = curr_open >= prev_close and curr_close <= prev_open
    meaningful = curr.get("body_pct", 0) >= MIN_ENGULF_BODY_PCT

    if engulfs and meaningful:
        return True, "bearish_engulfing"
    return False, ""


def _is_long_upper_wick_rejection(curr: dict) -> tuple[bool, str]:
    """ذيل علوي طويل مع إغلاق قرب القاع."""
    if not curr:
        return False, ""
    upper = curr.get("upper_wick_ratio", 0)
    close_pos = curr.get("close_position", 0.5)

    if upper >= UPPER_WICK_REJECTION_RATIO and close_pos <= 0.40:
        return True, "long_upper_wick_rejection"
    return False, ""


def _is_failed_breakout(curr: dict, prev: dict) -> tuple[bool, str]:
    """وصل لقمة جديدة لكن أغلق أسفل الإغلاق السابق."""
    if not curr or not prev:
        return False, ""
    new_high = curr.get("high", 0) > prev.get("high", 0)
    close_below_prev = curr.get("close", 0) < prev.get("close", 0)
    bearish_close = not curr.get("is_bullish", True)

    if new_high and close_below_prev and bearish_close:
        return True, "failed_breakout"
    return False, ""


def _is_strong_rejection_after_expansion(curr: dict, prev: dict) -> tuple[bool, str]:
    """شمعة خضراء كبيرة يعقبها شمعة حمراء بجسم معنوي."""
    if not curr or not prev:
        return False, ""
    prev_big_green = prev.get("is_bullish") and prev.get("body_pct", 0) >= 0.80
    curr_red = not curr.get("is_bullish") and curr.get("body_pct", 0) >= 0.40

    if prev_big_green and curr_red:
        return True, "strong_rejection_after_expansion"
    return False, ""


# ─────────────────────────────────────────
# BULLISH PATTERNS — Recovery Mode
# ─────────────────────────────────────────

def _is_hammer(curr: dict) -> tuple[bool, str]:
    """Hammer: جسم صغير في الأعلى، ذيل سفلي طويل."""
    if not curr:
        return False, ""
    lower = curr.get("lower_wick_ratio", 0)
    body = curr.get("body_pct", 0)
    close_pos = curr.get("close_position", 0.5)

    if lower >= 0.55 and body <= MIN_BODY_PCT * 2.5 and close_pos >= 0.60:
        return True, "hammer"
    return False, ""


def _is_bullish_engulfing(curr: dict, prev: dict) -> tuple[bool, str]:
    """Bullish Engulfing: شمعة خضراء تبتلع الحمراء السابقة."""
    if not curr or not prev:
        return False, ""
    if not curr.get("is_bullish") or prev.get("is_bullish"):
        return False, ""

    curr_open = curr.get("open", 0)
    curr_close = curr.get("close", 0)
    prev_open = prev.get("open", 0)
    prev_close = prev.get("close", 0)

    engulfs = curr_open <= prev_close and curr_close >= prev_open
    meaningful = curr.get("body_pct", 0) >= MIN_ENGULF_BODY_PCT

    if engulfs and meaningful:
        return True, "bullish_engulfing"
    return False, ""


def _is_sweep_and_reclaim(curr: dict, prev: dict) -> tuple[bool, str]:
    """ذهب تحت القاع السابق ثم أغلق فوقه."""
    if not curr or not prev:
        return False, ""
    swept_below = curr.get("low", 0) < prev.get("low", 0)
    reclaimed = curr.get("close", 0) > prev.get("low", 0)
    bullish = curr.get("is_bullish", False)

    if swept_below and reclaimed and bullish:
        return True, "sweep_and_reclaim"
    return False, ""


def _is_strong_lower_wick(curr: dict) -> tuple[bool, str]:
    """ذيل سفلي قوي مع إغلاق في upper 40%."""
    if not curr:
        return False, ""
    lower = curr.get("lower_wick_ratio", 0)
    close_pos = curr.get("close_position", 0.5)

    if lower >= LOWER_WICK_REVERSAL_RATIO and close_pos >= 0.60:
        return True, "strong_lower_wick"
    return False, ""


def _is_compression_release(candles: list[dict]) -> tuple[bool, str]:
    """شموع صغيرة ثم كاندل خضراء قوية."""
    if len(candles) < 3:
        return False, ""

    curr = candles[0]
    prev_candles = candles[1:4]

    # آخر 3 شموع كانت صغيرة (range < 0.5%)
    small = all(
        (c.get("body_pct", 1.0) < 0.50) for c in prev_candles if c
    )
    curr_breakout = curr.get("is_bullish") and curr.get("body_pct", 0) >= 0.40

    if small and curr_breakout:
        return True, "compression_release"
    return False, ""


# ─────────────────────────────────────────
# MAIN GATE FUNCTION
# ─────────────────────────────────────────

def evaluate_candle_reversal_gate(
    signal: Any,
    risk_mode: str = "normal",
) -> dict:
    """Evaluate candle patterns for execution gate.

    Args:
        signal: SignalCandidate — يقرأ من signal.meta["raw_candles"]
        risk_mode: "normal" أو "recovery" أو غيرهم

    Returns:
        {
            "bullish_reversal_detected": bool,
            "bearish_reversal_detected": bool,
            "reversal_strength": float,
            "execution_allowed": bool,
            "reason": str,
            "pattern": str,
        }
    """
    meta = getattr(signal, "meta", {}) or {}
    raw_candles = list(
        meta.get("raw_candles")
        or meta.get("recent_candles")
        or []
    )
    symbol = str(getattr(signal, "symbol", "-") or "-")

    # لو مفيش candles → اسمح بالتنفيذ (لا تبلوك بدون بيانات)
    if not raw_candles:
        print(f"🕯 CANDLE_GATE | {symbol} | no_candle_data → allowed", flush=True)
        return {
            "bullish_reversal_detected": False,
            "bearish_reversal_detected": False,
            "reversal_strength": 0.0,
            "execution_allowed": True,
            "reason": "no_candle_data",
            "pattern": "",
            **_candle_export_fields(pattern="", reversal_strength=0.0, reversal_kind=""),
        }

    # احسب metrics لآخر شمعتين مغلقتين
    # raw_candles[0] = أحدث شمعة (قد تكون لسه بتتشكل)
    # raw_candles[1] = الشمعة المغلقة الأخيرة ← المرجع الأساسي
    # raw_candles[2] = الشمعة قبلها

    closed = [_candle_metrics(c) for c in raw_candles[1:4] if c]

    if not closed:
        return {
            "bullish_reversal_detected": False,
            "bearish_reversal_detected": False,
            "reversal_strength": 0.0,
            "execution_allowed": True,
            "reason": "no_closed_candles",
            "pattern": "",
            **_candle_export_fields(pattern="", reversal_strength=0.0, reversal_kind=""),
        }

    curr = closed[0]                          # آخر شمعة مغلقة
    prev = closed[1] if len(closed) > 1 else {}  # الشمعة قبلها

    mode = str(risk_mode or "normal").strip().lower()
    is_recovery = mode in {"recovery_long", "recovery"}

    # ─────────────────────────────────────────
    # NORMAL MODE — كشف Bearish Patterns
    # ─────────────────────────────────────────
    if not is_recovery:
        bearish_found = False
        bearish_pattern = ""
        bearish_strength = 0.0

        checks = [
            _is_shooting_star(curr),
            _is_bearish_engulfing(curr, prev),
            _is_long_upper_wick_rejection(curr),
            _is_failed_breakout(curr, prev),
            _is_strong_rejection_after_expansion(curr, prev),
        ]

        strengths = {
            "shooting_star": 0.75,
            "bearish_engulfing": 0.90,
            "long_upper_wick_rejection": 0.65,
            "failed_breakout": 0.85,
            "strong_rejection_after_expansion": 0.80,
        }

        for found, pattern in checks:
            if found:
                bearish_found = True
                bearish_pattern = pattern
                bearish_strength = strengths.get(pattern, 0.70)
                break  # أول pattern يكفي للمنع

        print(
            f"🕯 CANDLE_GATE | {symbol} | mode=normal | "
            f"bearish={bearish_found} | pattern={bearish_pattern or 'none'} | "
            f"allowed={not bearish_found}",
            flush=True,
        )
        return {
            "bullish_reversal_detected": False,
            "bearish_reversal_detected": bearish_found,
            "reversal_strength": bearish_strength,
            "execution_allowed": not bearish_found,
            "reason": f"bearish_reversal:{bearish_pattern}" if bearish_found else "candle_ok",
            "pattern": bearish_pattern,
            **_candle_export_fields(
                curr,
                closed,
                pattern=bearish_pattern,
                reversal_strength=bearish_strength,
                reversal_kind="bearish" if bearish_found else "",
            ),
        }

    # ─────────────────────────────────────────
    # RECOVERY MODE — يشترط Bullish Pattern
    # ─────────────────────────────────────────
    bullish_found = False
    bullish_pattern = ""
    bullish_strength = 0.0

    checks = [
        _is_hammer(curr),
        _is_bullish_engulfing(curr, prev),
        _is_sweep_and_reclaim(curr, prev),
        _is_strong_lower_wick(curr),
        _is_compression_release(closed),
    ]

    strengths = {
        "hammer": 0.70,
        "bullish_engulfing": 0.90,
        "sweep_and_reclaim": 0.85,
        "strong_lower_wick": 0.65,
        "compression_release": 0.75,
    }

    for found, pattern in checks:
        if found:
            bullish_found = True
            bullish_pattern = pattern
            bullish_strength = strengths.get(pattern, 0.70)
            break

    print(
        f"🕯 CANDLE_GATE | {symbol} | mode=recovery | "
        f"bullish={bullish_found} | pattern={bullish_pattern or 'none'} | "
        f"allowed={bullish_found}",
        flush=True,
    )
    return {
        "bullish_reversal_detected": bullish_found,
        "bearish_reversal_detected": False,
        "reversal_strength": bullish_strength,
        "execution_allowed": bullish_found,
        "reason": f"bullish_reversal:{bullish_pattern}" if bullish_found else "no_bullish_reversal_in_recovery",
        "pattern": bullish_pattern,
        **_candle_export_fields(
            curr,
            closed,
            pattern=bullish_pattern,
            reversal_strength=bullish_strength,
            reversal_kind="bullish" if bullish_found else "",
        ),
    }
