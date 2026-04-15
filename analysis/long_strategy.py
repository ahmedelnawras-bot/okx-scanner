def early_bullish_signal(df):
    if df is None or df.empty or len(df) < 20:
        return False

    last = df.iloc[-1]
    avg_volume = df["volume"].rolling(20).mean().iloc[-1]

    cond_trend = last["close"] > last["ma20"]
    cond_candle = last["close"] > last["open"]
    cond_rsi = last["rsi"] > 50
    cond_volume = last["volume"] > avg_volume

    return cond_trend and cond_candle and (cond_rsi or cond_volume)
