from __future__ import annotations

from typing import Any


def evaluate_trade_outcome(
    future_candles: list[Any],
    entry: float,
    tp1: float,
    tp2: float,
    sl: float,
    horizon_bars: int = 96,
) -> dict:
    """Evaluate simple long TP/SL outcome from future candles.

    If TP and SL are both touched inside the same candle, the conservative label
    marks SL first. This avoids overstating historical quality.
    """
    entry = float(entry or 0.0)
    tp1 = float(tp1 or 0.0)
    tp2 = float(tp2 or 0.0)
    sl = float(sl or 0.0)
    if entry <= 0:
        return {}

    max_gain = 0.0
    max_drawdown = 0.0
    hit_tp1 = False
    hit_tp2 = False
    hit_sl = False
    time_to_tp1_min: int | None = None
    time_to_sl_min: int | None = None
    first_event = ""

    rows = list(future_candles or [])[: max(1, int(horizon_bars))]
    for idx, candle in enumerate(rows, start=1):
        high = float(getattr(candle, "high", 0.0) or 0.0)
        low = float(getattr(candle, "low", 0.0) or 0.0)
        if high > 0:
            max_gain = max(max_gain, ((high / entry) - 1.0) * 100.0)
        if low > 0:
            max_drawdown = min(max_drawdown, ((low / entry) - 1.0) * 100.0)

        # Conservative intrabar ordering for long trades.
        if not hit_sl and sl > 0 and low <= sl:
            hit_sl = True
            time_to_sl_min = idx * 15
            if not first_event:
                first_event = "sl"
        if not hit_tp1 and tp1 > 0 and high >= tp1:
            hit_tp1 = True
            time_to_tp1_min = idx * 15
            if not first_event:
                first_event = "tp1"
        if not hit_tp2 and tp2 > 0 and high >= tp2:
            hit_tp2 = True
            if not first_event:
                first_event = "tp2"

    if hit_sl and not hit_tp1:
        label = "stopped"
    elif hit_tp2:
        label = "win_tp2"
    elif hit_tp1:
        label = "win_tp1"
    elif max_gain >= 0.6 and max_drawdown > -0.8:
        label = "small_followthrough"
    elif max_drawdown <= -1.0:
        label = "weak_or_late"
    else:
        label = "flat"

    return {
        "horizon_bars": len(rows),
        "hit_tp1": hit_tp1,
        "hit_tp2": hit_tp2,
        "hit_sl": hit_sl,
        "first_event": first_event,
        "max_gain_24h": round(max_gain, 4),
        "max_drawdown_24h": round(max_drawdown, 4),
        "time_to_tp1_min": time_to_tp1_min,
        "time_to_sl_min": time_to_sl_min,
        "final_label": label,
    }
