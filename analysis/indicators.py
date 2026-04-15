def early_bullish_signal(df):
    if df.empty or len(df) < 20:
        return None

    price = df["close"].iloc[-1]
    prev_close = df["close"].iloc[-2]

    ma20 = df["ma20"].iloc[-1]
    rsi = df["rsi"].iloc[-1]

    vol = df["volume"].iloc[-1]
    avg_vol = df["volume"].rolling(20).mean().iloc[-1]

    if avg_vol == 0:
        return None

    momentum = (price - prev_close) / prev_close
    vol_ratio = vol / avg_vol

    if not (
        price > ma20 and
        momentum > 0.006 and
        vol_ratio > 2 and
        45 < rsi < 70
    ):
        return None

    score = 0
    reasons = []

    if rsi > 55:
        score += 1.5
        reasons.append("RSI قوي")

    if vol_ratio > 3:
        score += 2
        reasons.append("فوليوم انفجار")

    prev_highs = df["high"].iloc[-4:-1].max()
    if price > prev_highs:
        score += 1
        reasons.append("كسر مقاومة")

    candle_body = abs(df["close"].iloc[-1] - df["open"].iloc[-1])
    candle_range = df["high"].iloc[-1] - df["low"].iloc[-1]

    if candle_range > 0 and (candle_body / candle_range) > 0.7:
        score += 1.5
        reasons.append("شمعة قوية")

    return {
        "score": score,
        "reasons": reasons,
        "price": price,
        "rsi": rsi,
        "vol_ratio": vol_ratio
    }
