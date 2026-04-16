def is_breakout(df, lookback=20):
    try:
        recent_high = df["high"].rolling(lookback).max().iloc[-2]
        current_close = df["close"].iloc[-1]
        return current_close > recent_high
    except Exception:
        return False


def calculate_long_score(df, mtf_confirmed, btc_mode, breakout, is_new):
    score = 0.0
    reasons = []
    flags = []

    last = df.iloc[-1]
    prev = df.iloc[-2]

    rsi = float(last["rsi"])
    close = float(last["close"])
    open_ = float(last["open"])
    ma = float(last["ma"]) if last["ma"] == last["ma"] else close  # avoid nan

    prev_volume = float(prev["volume"])
    last_volume = float(last["volume"])

    if prev_volume > 0:
        vol_ratio = last_volume / prev_volume
    else:
        vol_ratio = 1.0

    body = abs(close - open_)
    full = float(last["high"]) - float(last["low"])
    upper_wick = float(last["high"]) - max(open_, close)

    # RSI
    if 50 < rsi < 65:
        score += 2
        reasons.append("RSI healthy")
        flags.append("RSI")
    elif 45 < rsi <= 50:
        score += 1
    elif rsi > 65:
        score += 0.5
    else:
        score -= 1

    # Volume
    if vol_ratio > 1.3:
        score += 2
        reasons.append("Strong volume")
        flags.append("Vol")
    elif vol_ratio > 1:
        score += 1

    # Trend
    if close > ma:
        score += 2
        reasons.append("Above MA")
    else:
        score -= 1

    # Candle strength
    if full > 0:
        ratio = body / full
        if ratio > 0.6:
            score += 1.5
            reasons.append("Strong candle")
        elif ratio > 0.4:
            score += 0.5

    # Rejection
    rejection = False
    if full > 0 and upper_wick > body * 1.5:
        rejection = True
        score -= 1

    # Breakout
    if breakout:
        score += 1.5
        reasons.append("Breakout")
        flags.append("BO")

    # MTF
    if mtf_confirmed:
        score += 1.5
        flags.append("MTF")

    # BTC mode
    if "🟢" in btc_mode:
        score += 1
    elif "🔴" in btc_mode:
        score -= 1

    # New listing
    if is_new:
        score += 0.5
        flags.append("NEW")

    # Fake filter
    fake_signal = False

    if score < 4.5:
        fake_signal = True

    if rejection and score < 6:
        fake_signal = True

    if vol_ratio < 0.7 and rsi < 45:
        fake_signal = True

    if score >= 7:
        fake_signal = False

    score = max(0.0, score)

    signal = score >= 5.5

    return {
        "score": round(score, 1),
        "reasons": reasons,
        "flags": flags,
        "fake_signal": fake_signal,
        "signal": signal
    }
