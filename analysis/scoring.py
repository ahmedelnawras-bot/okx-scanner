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
    vol_ratio = last["volume"] / prev["volume"] if prev["volume"] > 0 else 1

    if vol_ratio > 1.3:
        score += 2
        reasons.append("Volume قوي")
        flags.append("Vol")
    elif vol_ratio > 1:
        score += 1

    # ================= TREND =================
    if last["close"] > last["ma"]:
        score += 2
        reasons.append("فوق الموفنج")
    else:
        score -= 1

    # ================= CANDLE =================
    body = abs(last["close"] - last["open"])
    full = last["high"] - last["low"]

    if full > 0:
        ratio = body / full

        if ratio > 0.6:
            score += 1.5
            reasons.append("شمعة قوية")
        elif ratio > 0.4:
            score += 0.5

    # ================= REJECTION =================
    upper_wick = last["high"] - max(last["open"], last["close"])

    rejection = False
    if full > 0 and upper_wick > body * 1.5:
        rejection = True
        score -= 1

    # ================= BREAKOUT =================
    if breakout:
        score += 1.5
        reasons.append("Breakout")
        flags.append("BO")

    # ================= MTF =================
    if mtf_confirmed:
        score += 1.5
        flags.append("MTF")

    # ================= BTC =================
    if "🟢" in btc_mode:
        score += 1
    elif "🔴" in btc_mode:
        score -= 1

    # ================= NEW =================
    if is_new:
        score += 0.5
        flags.append("NEW")

    # ================= SMART FAKE FILTER =================
    fake_signal = False

    # ❌ فقط لو سيء جدًا
    if score < 3:
        fake_signal = True

    # ❌ رفض قوي لو في rejection + ضعف
    if rejection and score < 6:
        fake_signal = True

    # ❌ ضعف شديد في الحجم + RSI ضعيف
    if vol_ratio < 0.7 and rsi < 45:
        fake_signal = True

    # ❗ مفيش رفض للسكورات العالية
    if score >= 7:
        fake_signal = False

    return {
        "score": round(score, 1),
        "reasons": reasons,
        "flags": flags,
        "fake_signal": fake_signal
    }١١
