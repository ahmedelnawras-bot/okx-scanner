def calculate_long_score(df, mtf_confirmed=False, btc_mode="🟡 محايد", breakout=False, is_new=False):
    result = {
        "score": 0.0,
        "reasons": [],
        "flags": [],
        "fake_signal": False,
    }

    try:
        if df is None or df.empty or len(df) < 25:
            result["fake_signal"] = True
            return result

        last = df.iloc[-1]
        prev = df.iloc[-2]

        close = float(last["close"])
        open_ = float(last["open"])
        ma = float(last.get("ma", close))
        rsi = float(last.get("rsi", 50))
        rsi_prev = float(prev.get("rsi", rsi))
        atr = float(last.get("atr", 0))

        candle_strength = get_candle_strength(df)
        volume_spike = is_volume_spike(df, 1.2)
        bull_reject = bullish_rejection(df)
        bear_reject = bearish_rejection(df)

        score = 0.0

        # الاتجاه
        if close > ma:
            score += 2
            result["reasons"].append("فوق MA")
        else:
            score -= 0.5   # ❗ بدل -2

        # RSI
        if rsi > 55:
            score += 2
            result["flags"].append("RSI ↑")
        elif rsi > 50:
            score += 1
        else:
            score -= 0.5

        if rsi > rsi_prev:
            score += 0.5

        # الشمعة
        if close > open_:
            if candle_strength > 0.5:
                score += 1.5
                result["flags"].append("Strong Candle")
            else:
                score += 0.5
        else:
            score -= 0.5   # ❗ بدل -2

        # الفوليوم
        if volume_spike:
            score += 1.5
            result["flags"].append("Vol ↑")

        # breakout
        if breakout:
            score += 1

        # rejection
        if bull_reject:
            score += 0.5
        if bear_reject:
            score -= 0.5   # ❗ بدل -1

        # MTF
        if mtf_confirmed:
            score += 1

        # BTC
        if "🟢" in btc_mode:
            score += 0.5
        elif "🔴" in btc_mode:
            score -= 0.5   # ❗ بدل -1

        # new listing boost
        if is_new:
            score += 0.3

        # ❗ fake filter خفيف جدًا
        if atr <= 0:
            result["fake_signal"] = True

        # ❗ الشرط ده اتشال (كان بيقص السوق كله)
        # if close <= ma and rsi < 45 and not volume_spike:
        #     result["fake_signal"] = True

        score = max(0.0, min(10.0, score))
        result["score"] = round(score, 1)

        if score < 3:
            result["fake_signal"] = True   # ❗ بدل شروط قاسية

        return result

    except Exception:
        result["fake_signal"] = True
        return result
