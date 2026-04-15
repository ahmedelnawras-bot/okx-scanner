def calculate_long_score(df):
    score = 0.0

    if df.empty or len(df) < 30:
        return 0.0

    last = df.iloc[-1]
    prev = df.iloc[-2]

    close_price = float(last["close"])
    ma20 = float(last["ma20"])
    rsi = float(last["rsi"])

    # 1) السعر فوق ma20
    if close_price > ma20:
        score += 2.5

    # 2) RSI إيجابي
    if 55 <= rsi <= 68:
        score += 2.5
    elif 50 <= rsi < 55:
        score += 1.5
    elif 68 < rsi <= 75:
        score += 1.0

    # 3) آخر شمعة أقوى من اللي قبلها
    if float(last["close"]) > float(prev["close"]):
        score += 2.0

    # 4) ATR موجود
    if "atr" in df.columns:
        atr = float(last["atr"])
        if atr > 0:
            score += 1.5

    # 5) المسافة فوق ma20 منطقية
    if ma20 > 0:
        distance_pct = ((close_price - ma20) / ma20) * 100
        if 0.2 <= distance_pct <= 2.5:
            score += 1.5

    if score > 10:
        score = 10.0

    return round(score, 1)
