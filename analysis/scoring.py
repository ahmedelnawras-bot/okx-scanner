def is_breakout(df, lookback=20):
    try:
        if df is None or df.empty or len(df) < lookback + 2:
            return False

        recent_high = df["high"].rolling(lookback).max().iloc[-2]
        last_close = float(df["close"].iloc[-1])
        return last_close > float(recent_high)
    except Exception:
        return False


def get_candle_strength(df):
    try:
        c = df.iloc[-1]
        body = abs(float(c["close"]) - float(c["open"]))
        total_range = float(c["high"]) - float(c["low"])
        if total_range <= 0:
            return 0.0
        return body / total_range
    except Exception:
        return 0.0


def bullish_rejection(df):
    try:
        c = df.iloc[-1]
        body = abs(float(c["close"]) - float(c["open"]))
        lower_wick = min(float(c["open"]), float(c["close"])) - float(c["low"])
        return lower_wick > (body * 1.5)
    except Exception:
        return False


def bearish_rejection(df):
    try:
        c = df.iloc[-1]
        body = abs(float(c["close"]) - float(c["open"]))
        upper_wick = float(c["high"]) - max(float(c["open"]), float(c["close"]))
        return upper_wick > (body * 1.5)
    except Exception:
        return False


def is_volume_spike(df, multiplier=1.2):
    try:
        if df is None or df.empty or len(df) < 20:
            return False

        last_volume = float(df["volume"].iloc[-1])
        avg_volume_20 = float(df["volume"].rolling(20).mean().iloc[-1])

        if avg_volume_20 <= 0:
            return False

        return last_volume >= (avg_volume_20 * multiplier)
    except Exception:
        return False


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

        # 1) MA trend
        if close > ma:
            score += 2.0
            result["reasons"].append("فوق MA20")
        else:
            score -= 2.0

        # 2) RSI zones الذكية
        if 55 <= rsi <= 68:
            score += 2.0
            result["reasons"].append("RSI قوي")
            result["flags"].append("RSI ↑")
        elif 50 <= rsi < 55:
            score += 1.0
            result["flags"].append("RSI ↗")
        elif 68 < rsi <= 75:
            score += 1.0
            result["flags"].append("RSI Hot")
        else:
            if rsi < 50:
                score -= 2.0
            else:
                score -= 0.5

        if rsi > rsi_prev:
            score += 0.5

        # 3) candle strength
        if close > open_:
            if candle_strength >= 0.60:
                score += 2.0
                result["reasons"].append("شمعة قوية")
                result["flags"].append("Candle Strong")
            elif candle_strength >= 0.45:
                score += 1.0
                result["flags"].append("Candle OK")
            else:
                score -= 0.5
        else:
            score -= 2.0

        # 4) volume
        if volume_spike:
            score += 1.5
            result["reasons"].append("فوليوم قوي")
            result["flags"].append("Vol ↑")
        else:
            score -= 0.5

        # 5) breakout
        if breakout:
            score += 1.0
            result["reasons"].append("اختراق")
            result["flags"].append("Break ✔")

        # 6) rejection
        if bull_reject:
            score += 0.5
            result["flags"].append("Reject ↓")

        if bear_reject:
            score -= 1.5
            result["flags"].append("Upper Wick")

        # 7) MTF
        if mtf_confirmed:
            score += 1.0
            result["reasons"].append("تأكيد 1H")
            result["flags"].append("MTF ✔")
        else:
            score -= 0.5

        # 8) BTC mode
        if "🟢" in btc_mode:
            score += 0.5
        elif "🔴" in btc_mode:
            score -= 1.0

        # 9) new listing
        if is_new and score >= 7.0:
            score += 0.2
            result["flags"].append("New")

        # 10) fake filters مخففة
        if candle_strength < 0.35 and not volume_spike:
            score -= 1.5

        if bear_reject and not mtf_confirmed:
            score -= 2.0

        if atr <= 0:
            result["fake_signal"] = True

        if close <= ma and rsi < 45 and not volume_spike:
            result["fake_signal"] = True

        score = max(0.0, min(10.0, score))
        result["score"] = round(score, 1)
        result["reasons"] = list(dict.fromkeys(result["reasons"]))
        result["flags"] = list(dict.fromkeys(result["flags"]))

        if not result["reasons"]:
            result["reasons"] = ["زخم مبكر"]

        return result

    except Exception:
        result["fake_signal"] = True
        return result
