# ================= BREAKOUT =================
def is_breakout(df, lookback=20):
    try:
        recent_high = df["high"].rolling(lookback).max().iloc[-2]
        current_close = df["close"].iloc[-1]

        return current_close > recent_high
    except:
        return False


# ================= SCORING =================
def calculate_long_score(df, mtf_confirmed, btc_mode, breakout, is_new):
    score = 0
    reasons = []
    flags = []

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # ================= RSI =================
    rsi = last["rsi"]

    if 50 < rsi < 65:
        score += 2
        reasons.append("RSI صحي")
        flags.append("RSI")
    elif 45 < rsi <= 50:
        score += 1
    elif rsi > 65:
        score += 0.5
    else:
        score -= 1

    # ================= VOLUME =================
    if last["volume"] > prev["volume"] * 1.3:
        score += 2
        reasons.append("Volume قوي")
        flags.append("Vol")
    elif last["volume"] > prev["volume"]:
        score += 1

    # ================= TREND =================
    if last["close"] > last["ma"]:
        score += 2
        reasons.append("فوق الموفنج")
    else:
        score -= 1

    # ================= CANDLE STRENGTH =================
    body = abs(last["close"] - last["open"])
    full = last["high"] - last["low"]

    if full > 0:
        ratio = body / full
        if ratio > 0.6:
            score += 1.5
            reasons.append("شمعة قوية")

    # ================= REJECTION =================
    upper_wick = last["high"] - max(last["open"], last["close"])
    if upper_wick > body * 1.5:
        score -= 1.5

    # ================= BREAKOUT =================
    if breakout:
        score += 1.5
        reasons.append("Breakout")
        flags.append("BO")

    # ================= MTF =================
    if mtf_confirmed:
        score += 1.5
        flags.append("MTF")

    # ================= BTC MODE =================
    if "🟢" in btc_mode:
        score += 1
    elif "🔴" in btc_mode:
        score -= 1

    # ================= NEW LISTING =================
    if is_new:
        score += 0.5
        flags.append("NEW")

    # ================= FAKE SIGNAL FILTER =================
    fake_signal = False

    # ضعيف جدًا
    if score < 3:
        fake_signal = True

    # مفيش حجم + RSI ضعيف
    if last["volume"] < prev["volume"] and rsi < 50:
        fake_signal = True

    # شمعة ضعيفة جدًا
    if full > 0 and (body / full) < 0.3:
        fake_signal = True

    return {
        "score": round(score, 1),
        "reasons": reasons,
        "flags": flags,
        "fake_signal": fake_signal
    }
