def early_bullish_signal(df):
    if df is None or df.empty or len(df) < 30:
        return False

    last = df.iloc[-1]
    prev = df.iloc[-2]

    avg_volume_20 = df["volume"].rolling(20).mean().iloc[-1]
    recent_high_5 = df["high"].iloc[-6:-1].max()

    # ======================
    # شروط أساسية (لازم)
    # ======================
    cond_trend = last["close"] > last["ma20"]
    cond_rsi = last["rsi"] > 50
    cond_not_overextended = last["close"] < (last["ma20"] * 1.1)

    # ======================
    # شروط تعزيز (مش كلها لازم)
    # ======================
    cond_ma_slope = last["ma20"] > prev["ma20"]

    cond_green = last["close"] > last["open"]

    candle_body = last["close"] - last["open"]
    candle_range = last["high"] - last["low"]
    cond_body = candle_range > 0 and (candle_body / candle_range) >= 0.3

    cond_volume = last["volume"] > (avg_volume_20 * 1.0)

    cond_breakout = last["close"] > recent_high_5
    cond_momentum = last["close"] > prev["close"]

    # ======================
    # نحسب عدد الشروط المحققة
    # ======================
    confirmations = [
        cond_ma_slope,
        cond_green,
        cond_body,
        cond_volume,
        cond_breakout,
        cond_momentum,
    ]

    score = sum(confirmations)

    # ======================
    # القرار النهائي
    # ======================
    return cond_trend and cond_rsi and cond_not_overextended and score >= 3
