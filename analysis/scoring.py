def get_signal_index(df):
    """
    يرجّع index آخر شمعة confirmed.
    لو آخر شمعة غير مؤكدة، نستخدم اللي قبلها.
    """
    try:
        if df is None or df.empty:
            return -1

        if "confirm" not in df.columns or len(df) < 2:
            return -1

        last_confirm = str(int(float(df.iloc[-1]["confirm"])))
        if last_confirm == "1":
            return -1

        return -2
    except Exception:
        return -1


def is_breakout(df, lookback=20):
    try:
        if df is None or df.empty or len(df) < (lookback + 2):
            return False

        signal_idx = get_signal_index(df)
        current_close = float(df.iloc[signal_idx]["close"])

        highs_before = df["high"].iloc[:signal_idx]
        if len(highs_before) < lookback:
            return False

        recent_high = highs_before.rolling(lookback).max().iloc[-1]
        return current_close > float(recent_high)
    except Exception:
        return False


def classify_funding_simple(funding):
    if funding < -0.0005:
        return "🟢 سلبي"
    elif funding > 0.0005:
        return "🔴 إيجابي"
    else:
        return "🟡 محايد"


def classify_signal(score):
    if score >= 8.5:
        return "🔥 نار"
    elif score >= 7.5:
        return "✅ جيد"
    else:
        return "⚡ عادي"


def calculate_long_score(df, mtf_confirmed, btc_mode, breakout, is_new, funding=0.0):
    score = 0.0
    reasons = []

    if df is None or df.empty or len(df) < 25:
        return {
            "score": 0.0,
            "reasons": [],
            "fake_signal": True,
            "signal": False,
            "funding_label": classify_funding_simple(funding),
            "signal_rating": "⚡ عادي",
        }

    signal_idx = get_signal_index(df)

    try:
        last = df.iloc[signal_idx]
        prev = df.iloc[signal_idx - 1]
    except Exception:
        return {
            "score": 0.0,
            "reasons": [],
            "fake_signal": True,
            "signal": False,
            "funding_label": classify_funding_simple(funding),
            "signal_rating": "⚡ عادي",
        }

    rsi = float(last["rsi"])
    close = float(last["close"])
    open_ = float(last["open"])
    ma = float(last["ma"]) if last["ma"] == last["ma"] else close

    prev_volume = float(prev["volume"])
    last_volume = float(last["volume"])
    vol_ratio = (last_volume / prev_volume) if prev_volume > 0 else 1.0

    high = float(last["high"])
    low = float(last["low"])

    body = abs(close - open_)
    full = high - low
    upper_wick = high - max(open_, close)

    candle_strength = (body / full) if full > 0 else 0.0
    rejection = full > 0 and upper_wick > body * 1.5

    # RSI
    if 52 <= rsi <= 62:
        score += 1.5
        reasons.append("RSI صحي")
    elif 50 <= rsi < 52:
        score += 0.7
    elif 62 < rsi <= 68:
        score += 0.5
    elif rsi > 68:
        score -= 0.5
    else:
        score -= 1.2

    # Volume
    if vol_ratio >= 2.2:
        score += 2.5
        reasons.append("فوليوم انفجار")
    elif vol_ratio >= 1.6:
        score += 1.8
        reasons.append("فوليوم قوي")
    elif vol_ratio >= 1.2:
        score += 0.8
        reasons.append("فوليوم داعم")
    elif vol_ratio < 0.85:
        score -= 0.8

    # MA / Trend (أهدى من قبل)
    if close > ma:
        score += 0.8
        reasons.append("فوق MA")
    else:
        score -= 1.2

    # Candle strength
    if candle_strength >= 0.7:
        score += 1.6
        reasons.append("شمعة قوية")
    elif candle_strength >= 0.5:
        score += 0.9
        reasons.append("شمعة جيدة")
    elif candle_strength >= 0.35:
        score += 0.2
    else:
        score -= 0.5

    # Rejection wick
    if rejection:
        score -= 1.5

    # Breakout (أهم)
    if breakout:
        score += 2.2
        reasons.append("اختراق")
    else:
        score -= 0.4

    # MTF (أهم)
    if mtf_confirmed:
        score += 1.8
        reasons.append("تأكيد 1H")
    elif not is_new:
        score -= 1.0

    # BTC
    if "🟢" in btc_mode:
        score += 0.7
        reasons.append("BTC داعم")
    elif "🔴" in btc_mode:
        score -= 0.7

    # Funding
    funding_label = classify_funding_simple(funding)
    if funding < -0.0005:
        score += 0.6
        reasons.append("تمويل سلبي")
    elif funding > 0.0005:
        score -= 0.7

    # New listing
    if is_new:
        score += 0.2
        reasons.append("عملة جديدة")

        if breakout and vol_ratio >= 1.8:
            score += 0.7
            reasons.append("زخم جديد قوي")
        elif breakout or vol_ratio >= 1.8:
            score += 0.2

    # Quality gates
    strong_structure = breakout or mtf_confirmed
    strong_momentum = vol_ratio >= 1.6 or candle_strength >= 0.6
    premium_quality = breakout and mtf_confirmed
    premium_momentum = vol_ratio >= 1.6 and candle_strength >= 0.5

    if not strong_structure and not strong_momentum:
        score -= 1.8

    if close > ma and 50 <= rsi <= 62 and vol_ratio < 1.2 and not breakout and not mtf_confirmed:
        score -= 1.5

    if score >= 8.0 and not (premium_quality or premium_momentum):
        score -= 1.3

    if score >= 8.8 and not (breakout and mtf_confirmed and vol_ratio >= 1.4):
        score -= 1.0

    # Fake signal filter
    fake_signal = False

    if score < 5.5:
        fake_signal = True

    if rejection and candle_strength < 0.55:
        fake_signal = True

    if vol_ratio < 0.9 and not breakout:
        fake_signal = True

    if not is_new and not mtf_confirmed and not breakout and score < 7.0:
        fake_signal = True

    if close <= ma and not breakout:
        fake_signal = True

    if rsi < 50 and not breakout:
        fake_signal = True

    if score >= 8.5 and not (breakout or mtf_confirmed):
        fake_signal = True

    if score >= 8.5 and vol_ratio < 1.3:
        fake_signal = True

    # سقف السكور
    score = max(0.0, min(9.2, score))
    score = round(score, 1)

    return {
        "score": score,
        "reasons": list(dict.fromkeys(reasons)),
        "fake_signal": fake_signal,
        "signal": score >= 6.0,
        "funding_label": funding_label,
        "signal_rating": classify_signal(score),
    }
