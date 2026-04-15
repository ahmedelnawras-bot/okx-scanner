def early_bullish_signal(df):
    try:
        if df is None or df.empty or len(df) < 20:
            return False

        last = df.iloc[-1]
        prev = df.iloc[-2]

        score = 0

        # ✅ نحل مشكلة ma
        ma = last.get("ma", None)

        if ma is not None:
            if last["close"] > ma:
                score += 1

        # RSI
        if "rsi" in df.columns:
            if last["rsi"] > 50:
                score += 1

        # ATR
        if "atr" in df.columns:
            if last["atr"] >= prev["atr"]:
                score += 1

        # شمعة خضراء
        if last["close"] > last["open"]:
            score += 1

        # حجم
        if last["volume"] > prev["volume"]:
            score += 1

        # مومنتوم
        if last["close"] > prev["close"]:
            score += 1

        return score >= 3   # خففناها عشان الإشارات ترجع

    except Exception as e:
        print(f"Strategy error: {e}")
        return False
