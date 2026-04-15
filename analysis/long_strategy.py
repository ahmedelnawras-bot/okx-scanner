def early_bullish_signal(df):
    price = df['c'].iloc[-1]
    prev_close = df['c'].iloc[-2]

    ma20 = df['ma20'].iloc[-1]
    rsi = df['rsi'].iloc[-1]

    vol = df['vol'].iloc[-1]
    avg_vol = df['vol'].rolling(20).mean().iloc[-1]

    momentum = (price - prev_close) / prev_close
    vol_ratio = vol / avg_vol

    # الشروط الأساسية
    if not (
        price > ma20 and
        momentum > 0.006 and
        vol_ratio > 2 and
        45 < rsi < 70
    ):
        return None

    # =====================
    # 🔥 Score System
    # =====================
    score = 0
    reasons = []

    # RSI قوي
    if rsi > 55:
        score += 1.5
        reasons.append("RSI قوي")

    # Volume عالي
    if vol_ratio > 3:
        score += 2
        reasons.append("Volume انفجار")

    # Break Structure
    prev_highs = df['h'].iloc[-4:-1].max()
    if price > prev_highs:
        score += 1
        reasons.append("Break مقاومة")

    # شمعة قوية
    candle_body = abs(df['c'].iloc[-1] - df['o'].iloc[-1])
    candle_range = df['h'].iloc[-1] - df['l'].iloc[-1]

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
