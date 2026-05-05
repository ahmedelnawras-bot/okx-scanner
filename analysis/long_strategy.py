"""Basic long-side strategy helpers for OKX scanner.

The goal of this module is intentionally small: provide a robust early bullish
boolean signal that main.py/scoring.py can use without raising KeyErrors or
breaking on missing/dirty candle data.
"""


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def early_bullish_signal(df):
    """Return True when the latest candle shows early bullish intent.

    This keeps the original lightweight logic, but makes it safer and a bit more
    selective by requiring at least two weak confirmations instead of allowing a
    single green candle to pass alone.
    """
    try:
        if df is None or getattr(df, "empty", True) or len(df) < 25:
            return False

        last = df.iloc[-1]
        prev = df.iloc[-2]

        close = _safe_float(last.get("close"))
        open_ = _safe_float(last.get("open"))
        high = _safe_float(last.get("high"))
        low = _safe_float(last.get("low"))
        volume = _safe_float(last.get("volume"))
        prev_volume = _safe_float(prev.get("volume"))
        rsi = _safe_float(last.get("rsi"), default=None) if "rsi" in df.columns else None

        if close <= 0 or open_ <= 0 or high <= 0 or low <= 0:
            return False

        score = 0

        # Green candle / buyer control.
        if close > open_:
            score += 1

        # RSI reclaim/healthy momentum, when RSI exists.
        if rsi is not None and rsi > 50:
            score += 1

        # Volume expansion versus previous candle.
        if volume > 0 and prev_volume > 0 and volume > prev_volume:
            score += 1

        # Close near candle high means less upper-wick rejection.
        candle_range = high - low
        if candle_range > 0:
            close_position = (close - low) / candle_range
            if close_position >= 0.55:
                score += 1

        return score >= 2

    except Exception:
        return False
