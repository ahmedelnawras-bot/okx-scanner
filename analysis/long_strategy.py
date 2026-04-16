def early_bullish_signal(df):
    try:
        if df is None or df.empty or len(df) < 25:
            return False

        last = df.iloc[-1]
        prev = df.iloc[-2]

        score = 0

        if float(last["close"]) > float(last["open"]):
            score += 1

        if "rsi" in df.columns and float(last["rsi"]) > 50:
            score += 1

        if float(last["volume"]) > float(prev["volume"]):
            score += 1

        return score >= 1

    except Exception:
        return False
