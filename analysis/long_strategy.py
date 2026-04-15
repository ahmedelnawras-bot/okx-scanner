def early_bullish_signal(df):
    """
    استراتيجية لونج خفيفة ومناسبة لفحص عدد كبير من الأزواج.
    """

    try:
        if df is None or df.empty or len(df) < 20:
            return False

        last = df.iloc[-1]
        prev = df.iloc[-2]

        score = 0

        ma = last.get("ma", None)
        if ma is not None and last["close"] > ma:
            score += 1

        if "rsi" in df.columns and last["rsi"] > 50:
            score += 1

        if "atr" in df.columns and last["atr"] >= prev["atr"]:
            score += 1

        if last["close"] > last["open"]:
            score += 1

        if last["volume"] > prev["volume"]:
            score += 1

        if last["close"] > prev["close"]:
            score += 1

        return score >= 3

    except Exception as e:
        print(f"Strategy error: {e}")
        return False
