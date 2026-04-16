def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        # NaN check
        if value != value:
            return default
        return float(value)
    except Exception:
        return default


def _get_signal_and_prev_rows(df):
    """
    يرجّع:
    - signal_row: آخر شمعة confirmed
    - prev_row: الشمعة اللي قبلها
    """
    if df is None or df.empty or len(df) < 3:
        return None, None

    try:
        if "confirm" in df.columns:
            last_confirm = str(int(_safe_float(df.iloc[-1]["confirm"], 0)))
            if last_confirm == "1":
                signal_idx = len(df) - 1
            else:
                signal_idx = len(df) - 2
        else:
            signal_idx = len(df) - 2

        prev_idx = max(0, signal_idx - 1)

        return df.iloc[signal_idx], df.iloc[prev_idx]
    except Exception:
        return None, None


def is_breakout(df, lookback=20):
    try:
        if df is None or df.empty or len(df) < (lookback + 2):
            return False

        signal_row, _ = _get_signal_and_prev_rows(df)
        if signal_row is None:
            return False

        signal_idx = int(signal_row.name)
        if signal_idx <= lookback:
            return False

        current_close = _safe_float(signal_row["close"], 0.0)
        recent_high = _safe_float(df["high"].iloc[:signal_idx].rolling(lookback).max().iloc[-1], 0.0)

        return current_close > recent_high and recent_high > 0
    except Exception:
        return False


def classify_funding_simple(funding):
    if funding < -0.0005:
        return "🟢 سلبي"
    elif funding > 0.0005:
        return "🔴 إيجابي"
    else:
        return "🟡 محايد"


def classify_signal(score):
    if score >= 8.5:
        return "🔥 نار"
    elif score >= 7.0:
        return "✅ جيد"
    else:
        return "⚡ عادي"


def calculate_long_score(df, mtf_confirmed, btc_mode, breakout, is_new, funding=0.0):
    funding_label = classify_funding_simple(funding)

    if df is None or df.empty or len(df) < 25:
        return {
            "score": 0.0,
            "reasons": [],
            "fake_signal": True,
            "signal": False,
            "funding_label": funding_label,
            "signal_rating": "⚡ عادي",
        }

    signal_row, prev_row = _get_signal_and_prev_rows(df)
    if signal_row is None or prev_row is None:
        return {
            "score": 0.0,
            "reasons": [],
            "fake_signal": True,
            "signal": False,
            "funding_label": funding_label,
            "signal_rating": "⚡ عادي",
        }

    score = 0.0
    reasons = []

    rsi = _safe_float(signal_row.get("rsi"), 50.0)
    close = _safe_float(signal_row.get("close"), 0.0)
    open_ = _safe_float(signal_row.get("open"), close)
    ma = _safe_float(signal_row.get("ma"), close)

    high = _safe_float(signal_row.get("high"), close)
    low = _safe_float(signal_row.get("low"), close)

    last_volume = _safe_float(signal_row.get("volume"), 0.0)
    prev_volume = _safe_float(prev_row.get("volume"), 0.0)

    vol_ratio = (last_volume / prev_volume) if prev_volume > 0 else 1.0

    body = abs(close - open_)
    full = max(high - low, 0.0)
    upper_wick = max(high - max(open_, close), 0.0)
    candle_strength = (body / full) if full > 0 else 0.0
    rejection = full > 0 and upper_wick > body * 1.5

    # RSI
    if 52 <= rsi <= 62:
        score += 1.5
        reasons.append("RSI صحي")
    elif 50 <= rsi < 52:
        score += 0.7
    elif 62 < rsi <= 68:
        score += 0.5
    elif rsi > 68:
        score -= 0.5
    else:
        score -= 0.7

    # Volume
    if vol_ratio >= 2.0:
        score += 2.5
        reasons.append("فوليوم انفجار")
    elif vol_ratio >= 1.5:
        score += 1.8
        reasons.append("فوليوم قوي")
    elif vol_ratio >= 1.2:
        score += 0.8
        reasons.append("فوليوم داعم")
    elif vol_ratio < 0.85:
        score -= 0.5

    # Trend
    if close > ma:
        score += 1.0
        reasons.append("فوق MA")
    else:
        score -= 0.8

    # Candle
    if candle_strength >= 0.7:
        score += 1.6
        reasons.append("شمعة قوية")
    elif candle_strength >= 0.5:
        score += 0.9
        reasons.append("شمعة جيدة")
    elif candle_strength >= 0.35:
        score += 0.2
    else:
        score -= 0.3

    # Rejection
    if rejection:
        score -= 1.0

    # Breakout
    if breakout:
        score += 2.2
        reasons.append("اختراق")
    else:
        score -= 0.1

    # MTF
    if mtf_confirmed:
        score += 1.8
        reasons.append("تأكيد 1H")

    # BTC
    if "🟢" in btc_mode:
        score += 0.7
        reasons.append("BTC داعم")
    elif "🔴" in btc_mode:
        score -= 0.5

    # Funding
    if funding < -0.0005:
        score += 0.6
        reasons.append("تمويل سلبي")
    elif funding > 0.0005:
        score -= 0.5

    # New listing
    if is_new:
        score += 0.3
        reasons.append("عملة جديدة")

    # Fake signal (مخفف)
    fake_signal = False

    if score < 4.5:
        fake_signal = True

    if rejection and candle_strength < 0.5:
        fake_signal = True

    if close <= ma and not breakout:
        fake_signal = True

    if rsi < 48 and not breakout:
        fake_signal = True

    if score >= 8.5 and vol_ratio < 1.2:
        fake_signal = True

    score = max(0.0, min(9.5, score))
    score = round(score, 1)

    return {
        "score": score,
        "reasons": list(dict.fromkeys(reasons)),
        "fake_signal": fake_signal,
        "signal": score >= 5.5,
        "funding_label": funding_label,
        "signal_rating": classify_signal(score),
    }
