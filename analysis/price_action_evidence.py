from __future__ import annotations

from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _body_pct(candle: dict) -> float:
    open_ = _safe_float(candle.get("open"))
    close = _safe_float(candle.get("close"))
    high = _safe_float(candle.get("high"))
    low = _safe_float(candle.get("low"))

    rng = high - low
    if rng <= 0:
        return 0.0

    return abs(close - open_) / rng


def _close_position(candle: dict) -> float:
    close = _safe_float(candle.get("close"))
    high = _safe_float(candle.get("high"))
    low = _safe_float(candle.get("low"))

    rng = high - low
    if rng <= 0:
        return 0.5

    return (close - low) / rng


def build_smart_evidence(
    candles: list[dict] | None = None,
    pair: Any | None = None,
    signal_setup: str | None = None,
    market_mode: str | None = None,
) -> dict:
    """
    Price Action evidence layer.

    IMPORTANT:
    - Does NOT change score.
    - Does NOT change mode.
    - Does NOT approve/reject execution.
    - Only returns soft evidence for Nour / reports later.
    """

    candles = candles or []
    recent = candles[-8:] if len(candles) >= 8 else candles

    if len(recent) < 3:
        return {
            "available": False,
            "reason": "not_enough_candles",
            "displacement_hint": False,
            "compression_release_hint": False,
            "sweep_reclaim_hint": False,
            "failed_breakout_risk": False,
            "auction_acceptance_hint": False,
        }

    last = recent[-1]
    prev = recent[-2]
    prev_prev = recent[-3]

    last_body = _body_pct(last)
    last_close_pos = _close_position(last)

    last_high = _safe_float(last.get("high"))
    last_low = _safe_float(last.get("low"))
    last_close = _safe_float(last.get("close"))

    prev_high = _safe_float(prev.get("high"))
    prev_low = _safe_float(prev.get("low"))
    prev_close = _safe_float(prev.get("close"))

    prev_prev_high = _safe_float(prev_prev.get("high"))
    prev_prev_low = _safe_float(prev_prev.get("low"))

    # 1) Displacement:
    # شمعة جسمها قوي وقفلت قريب من الهاي
    displacement_hint = bool(
        last_body >= 0.58
        and last_close_pos >= 0.68
        and last_close > prev_high
    )

    # 2) Compression Release:
    # آخر شمعة كسرت نطاق صغير سابق
    prior_ranges = []
    for c in recent[:-1]:
        h = _safe_float(c.get("high"))
        l = _safe_float(c.get("low"))
        if h > l:
            prior_ranges.append(h - l)

    avg_prior_range = (
        sum(prior_ranges) / len(prior_ranges)
        if prior_ranges
        else 0.0
    )

    last_range = last_high - last_low

    compression_release_hint = bool(
        avg_prior_range > 0
        and last_range >= avg_prior_range * 1.35
        and last_close > max(prev_high, prev_prev_high)
    )

    # 3) Sweep Reclaim:
    # السعر نزل تحت low قريب ورجع قفل فوقه
    sweep_reclaim_hint = bool(
        last_low < min(prev_low, prev_prev_low)
        and last_close > prev_low
        and last_close_pos >= 0.60
    )

    # 4) Failed Breakout Risk:
    # السعر طلع فوق الهاي وفشل يقفل بقوة فوقه
    failed_breakout_risk = bool(
        last_high > max(prev_high, prev_prev_high)
        and last_close <= prev_high
        and last_close_pos <= 0.55
    )

    # 5) Auction Acceptance:
    # قفل فوق آخر نطاق + مش failed breakout
    auction_acceptance_hint = bool(
        last_close > max(prev_high, prev_prev_high)
        and last_close_pos >= 0.62
        and not failed_breakout_risk
    )

    return {
        "available": True,
        "model": "price_action_evidence_v1",
        "signal_setup": signal_setup or "",
        "market_mode": market_mode or "",

        "displacement_hint": displacement_hint,
        "compression_release_hint": compression_release_hint,
        "sweep_reclaim_hint": sweep_reclaim_hint,
        "failed_breakout_risk": failed_breakout_risk,
        "auction_acceptance_hint": auction_acceptance_hint,

        "details": {
            "last_body_pct": round(last_body, 3),
            "last_close_position": round(last_close_pos, 3),
            "last_range": round(last_range, 8),
            "avg_prior_range": round(avg_prior_range, 8),
        },
    }
