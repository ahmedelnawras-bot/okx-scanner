def early_bullish_signal(df):
    if df is None or df.empty:
        return False

    # تأكد إن الأعمدة موجودة
    required_cols = ["open", "high", "low", "close"]
    for col in required_cols:
        if col not in df.columns:
            return False

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # شمعة خضراء
    bullish = last["close"] > last["open"]

    # اختراق أعلى من الشمعة السابقة
    breakout = last["high"] > prev["high"]

    # زخم صعودي بسيط
    momentum = last["close"] > prev["close"]

    return bullish and breakout and momentum
