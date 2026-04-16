def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        if value != value:
            return default
        return float(value)
    except Exception:
        return default


def _get_signal_and_prev_rows(df):
    if df is None or df.empty or len(df) < 3:
        return None, None

    try:
        if "confirm" in df.columns:
            last_confirm = str(int(_safe_float(df.iloc[-1]["confirm"], 0)))
            signal_idx = len(df) - 1 if last_confirm == "1" else len(df) - 2
        else:
            signal_idx = len(df) - 2

        prev_idx = max(0, signal_idx - 1)
        return df.iloc[signal_idx], df.iloc[prev_idx]
    except Exception:
        return None, None


def is_breakout(df, lookback=20):
    try:
        if df is None or len(df) < lookback + 2:
            return False

        signal_row, _ = _get_signal_and_prev_rows(df)
        if signal_row is None:
            return False

        idx = int(signal_row.name)
        if idx <= lookback:
            return False

        close = _safe_float(signal_row["close"])
        high = _safe_float(
            df["high"].iloc[:idx].rolling(lookback).max().iloc[-1]
        )

        return close > high and high > 0
    except:
        return False


def classify_funding_simple(funding):
    if funding < -0.0005:
        return "🟢 سلبي"
    elif funding > 0.0005:
        return "🔴 إيجابي"
    return "🟡 محايد"


def classify_signal(score):
    if score >= 8.7:
        return "🔥 نار"
    elif score >= 7.0:
        return "✅ جيد"
    return "⚡ عادي"


def calculate_long_score(df, mtf_confirmed, btc_mode, breakout, is_new, funding=0.0):
    funding_label = classify_funding_simple(funding)

    signal_row, prev_row = _get_signal_and_prev_rows(df)
    if signal_row is None:
        return {"score": 0.0, "fake_signal": True, "signal": False}

    score = 0.0
    reasons = []

    close = _safe_float(signal_row["close"])
    open_ = _safe_float(signal_row["open"])
    high = _safe_float(signal_row["high"])
    low = _safe_float(signal_row["low"])
    ma = _safe_float(signal_row.get("ma"), close)
    rsi = _safe_float(signal_row.get("rsi"), 50)

    vol = _safe_float(signal_row["volume"])
    prev_vol = _safe_float(prev_row["volume"])
    vol_ratio = vol / prev_vol if prev_vol > 0 else 1.0

    # Candle
    body = abs(close - open_)
    full = max(high - low, 0.0)
    candle_strength = body / full if full > 0 else 0

    # =========================
    # BASIC SCORE (زي ما هو)
    # =========================

    if 52 <= rsi <= 62:
        score += 1.5; reasons.append("RSI صحي")
    elif rsi > 68:
        score -= 0.5
    else:
        score -= 0.7

    if vol_ratio >= 2:
        score += 2.5; reasons.append("فوليوم انفجار")
    elif vol_ratio >= 1.5:
        score += 1.8; reasons.append("فوليوم قوي")

    if close > ma:
        score += 1.0; reasons.append("فوق MA")
    else:
        score -= 0.8

    if candle_strength >= 0.7:
        score += 1.6; reasons.append("شمعة قوية")
    elif candle_strength >= 0.5:
        score += 0.9

    if breakout:
        score += 2.2; reasons.append("اختراق")

    if mtf_confirmed:
        score += 1.8; reasons.append("تأكيد 1H")

    if "🟢" in btc_mode:
        score += 0.7; reasons.append("BTC داعم")
    elif "🔴" in btc_mode:
        score -= 0.5

    if funding < -0.0005:
        score += 0.6; reasons.append("تمويل سلبي")

    # =========================
    # 🔥 NEW INTELLIGENCE
    # =========================

    # 📏 distance from MA
    dist_ma = ((close - ma) / ma) * 100 if ma > 0 else 0

    if 0.3 <= dist_ma <= 2.5:
        score += 0.5
        reasons.append("بداية ترند")

    elif dist_ma > 4:
        score -= 0.7
        reasons.append("بعيد عن MA")

    # 📈 breakout extension
    if breakout:
        recent_high = df["high"].rolling(20).max().iloc[-2]
        ext = ((close - recent_high) / recent_high) * 100 if recent_high > 0 else 0

        if ext <= 1.5:
            score += 0.4
            reasons.append("اختراق مبكر")
        elif ext > 3:
            score -= 0.6
            reasons.append("متأخر بعد الاختراق")

    # 🔥 RSI overheat
    if rsi > 72:
        score -= 0.8
        reasons.append("RSI عالي")

    # 🚀 premium
    if breakout and mtf_confirmed and vol_ratio >= 1.8 and candle_strength >= 0.5:
        score += 0.4
        reasons.append("توافق قوي")

    # =========================
    # FINAL
    # =========================

    score = max(0, min(9.5, score))
    score = round(score, 1)

    fake_signal = False
    if score < 4.5:
        fake_signal = True

    return {
        "score": score,
        "reasons": list(dict.fromkeys(reasons)),
        "fake_signal": fake_signal,
        "signal": score >= 5.5,
        "funding_label": funding_label,
        "signal_rating": classify_signal(score),
    }
