def early_bullish_signal(df):
    if df is None or df.empty or len(df) < 30:
        return False

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # متوسط الحجم
    avg_volume_20 = df["volume"].rolling(20).mean().iloc[-1]

    # أعلى قمة في آخر 5 شمعات قبل الحالية
    recent_high_5 = df["high"].iloc[-6:-1].max()

    # 1) الاتجاه
    cond_trend = last["close"] > last["ma20"]

    # 2) MA20 نفسه صاعد
    cond_ma_slope = last["ma20"] > prev["ma20"]

    # 3) الشمعة الحالية إيجابية وقوية
    candle_body = last["close"] - last["open"]
    candle_range = last["high"] - last["low"]

    cond_green_candle = last["close"] > last["open"]
    cond_body_strength = candle_range > 0 and (candle_body / candle_range) >= 0.4

    # 4) RSI مناسب للصعود — مخفف
    cond_rsi = last["rsi"] > 50

    # 5) الحجم أعلى من المتوسط — مخفف
    cond_volume = last["volume"] > (avg_volume_20 * 1.05)

    # 6) كسر قريب أو استمرار قوي
    cond_breakout = last["close"] > recent_high_5
    cond_momentum = last["close"] > prev["close"] and last["high"] > prev["high"]

    # 7) منع الدخول المتأخر جدًا
    cond_not_overextended = last["close"] < (last["ma20"] * 1.08)

    return (
        cond_trend
        and cond_ma_slope
        and cond_green_candle
        and cond_body_strength
        and cond_rsi
        and cond_volume
        and (cond_breakout or cond_momentum)
        and cond_not_overextended
    )
