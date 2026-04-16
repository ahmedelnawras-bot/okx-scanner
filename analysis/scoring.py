def is_breakout(df, lookback=20):
    try:
        recent_high = df["high"].rolling(lookback).max().iloc[-2]
        current_close = df["close"].iloc[-1]
        return current_close > recent_high
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
    elif score >= 6.5:
        return "✅ جيد"
    else:
        return "⚡ عادي"


def calculate_long_score(df, mtf_confirmed, btc_mode, breakout, is_new, funding=0.0):
    score = 0.0
    reasons = []

    last = df.iloc[-1]
    prev = df.iloc[-2]

    rsi = float(last["rsi"])
    close = float(last["close"])
    open_ = float(last["open"])
    ma = float(last["ma"]) if last["ma"] == last["ma"] else close

    prev_volume = float(prev["volume"])
    last_volume = float(last["volume"])
    vol_ratio = (last_volume / prev_volume) if prev_volume > 0 else 1.0

    body = abs(close - open_)
    full = float(last["high"]) - float(last["low"])
    upper_wick = float(last["high"]) - max(open_, close)

    # RSI
    if 50 < rsi < 65:
        score += 2.5
        reasons.append("RSI صحي")
    elif 45 < rsi <= 50:
        score += 1.0
    elif rsi > 65:
        score += 0.5
    else:
        score -= 1.0

    # Volume
    if vol_ratio > 1.3:
        score += 2.5
        reasons.append("فوليوم قوي")
    elif vol_ratio > 1:
        score += 1.0
        reasons.append("فوليوم داعم")

    # Trend
    if close > ma:
        score += 2.0
        reasons.append("فوق MA")
    else:
        score -= 1.0

    # Candle strength
    if full > 0:
        ratio = body / full
        if ratio > 0.6:
            score += 1.5
            reasons.append("شمعة قوية")
        elif ratio > 0.4:
            score += 0.5
            reasons.append("شمعة متوسطة")

    # Rejection
    rejection = False
    if full > 0 and upper_wick > body * 1.5:
        rejection = True
        score -= 1.0

    # Breakout
    if breakout:
        score += 1.5
        reasons.append("اختراق")

    # MTF
    if mtf_confirmed:
        score += 1.5
        reasons.append("تأكيد 1H")

    # BTC mode
    if "🟢" in btc_mode:
        score += 1.0
    elif "🔴" in btc_mode:
        score -= 1.0

    # Funding
    funding_label = classify_funding_simple(funding)
    if funding < -0.0005:
        score += 1.0
    elif funding > 0.0005:
        score -= 1.0

    # New listing
    if is_new:
        score += 0.5

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
    score = round(score, 1)

    return {
        "score": score,
        "reasons": list(dict.fromkeys(reasons)),
        "fake_signal": fake_signal,
        "signal": score >= 5.0,
        "funding_label": funding_label,
        "signal_rating": classify_signal(score),
    }
