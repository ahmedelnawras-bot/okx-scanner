def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        if value != value:  # NaN
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
        recent_high = _safe_float(
            df["high"].iloc[:idx].rolling(lookback).max().iloc[-1]
        )

        return close > recent_high and recent_high > 0
    except Exception:
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


def get_btc_dominance_proxy(btc_mode: str) -> str:
    if "🔴 هابط" in btc_mode:
        return "🟢 داعم للألت"
    if "🟢 صاعد" in btc_mode:
        return "🔴 ضد الألت"
    return "🟡 محايد"


def calculate_long_score(
    df,
    mtf_confirmed,
    btc_mode,
    breakout,
    is_new,
    funding=0.0,
    btc_dominance_proxy=None,
    vol_ratio=None,
    pre_breakout=False,
):
    funding_label = classify_funding_simple(funding)

    if btc_dominance_proxy is None:
        btc_dominance_proxy = get_btc_dominance_proxy(btc_mode)

    signal_row, prev_row = _get_signal_and_prev_rows(df)
    if signal_row is None or prev_row is None:
        return {
            "score": 0.0,
            "reasons": [],
            "fake_signal": True,
            "signal": False,
            "funding_label": funding_label,
            "signal_rating": "⚡ عادي",
        }

    score = 0.0
    reasons = []

    close = _safe_float(signal_row["close"])
    open_ = _safe_float(signal_row["open"])
    high = _safe_float(signal_row["high"])
    low = _safe_float(signal_row["low"])
    ma = _safe_float(signal_row.get("ma"), close)
    rsi = _safe_float(signal_row.get("rsi"), 50)
    prev_rsi = _safe_float(prev_row.get("rsi"), 50)
    rsi_momentum = rsi - prev_rsi

    vol = _safe_float(signal_row["volume"])
    prev_vol = _safe_float(prev_row["volume"])

    if vol_ratio is None:
        vol_ratio = vol / prev_vol if prev_vol > 0 else 1.0

    body = abs(close - open_)
    full = max(high - low, 0.0)
    upper_wick = max(high - max(open_, close), 0.0)
    candle_strength = body / full if full > 0 else 0.0
    rejection = full > 0 and upper_wick > body * 1.5

    # ===================== BASIC SCORING =====================
    if 52 <= rsi <= 70 and rsi_momentum > 3:
        score += 1.8
        reasons.append("RSI صاعد بقوة")
    elif 52 <= rsi <= 62:
        score += 1.2
        reasons.append("RSI في منطقة صحية")
    elif 62 < rsi <= 68:
        score += 0.7
        reasons.append("RSI جيد")
    elif 68 < rsi <= 72 and rsi_momentum > 4:
        score += 0.5
        reasons.append("RSI مرتفع بزخم")
    elif rsi > 72:
        score -= 0.9
        reasons.append("RSI عالي (تشبع شراء)")
    elif rsi < 48:
        score -= 0.7

    if vol_ratio >= 2.0:
        score += 2.5
        reasons.append("فوليوم انفجاري")
    elif vol_ratio >= 1.5:
        score += 1.8
        reasons.append("فوليوم قوي")
    elif vol_ratio >= 1.2:
        score += 0.9
        reasons.append("فوليوم داعم")

    if close > ma:
        score += 1.0
        reasons.append("فوق المتوسط")
    else:
        score -= 0.8

    if candle_strength >= 0.65:
        score += 1.4
        reasons.append("شمعة قوية")
    elif candle_strength >= 0.45:
        score += 0.8
        reasons.append("شمعة جيدة")

    if rejection:
        score -= 1.0

    if breakout:
        score += 2.2
        reasons.append("اختراق")

    if pre_breakout and not breakout:
        score += 1.2
        reasons.append("زخم مبكر تحت المقاومة 🎯")

    if mtf_confirmed:
        score += 1.8
        reasons.append("تأكيد فريم الساعة")

    if "🟢" in btc_mode:
        score += 0.7
        reasons.append("BTC داعم")
    elif "🔴" in btc_mode:
        score -= 0.5

    if btc_dominance_proxy == "🟢 داعم للألت":
        score += 0.4
        reasons.append("هيمنة داعمة للألت")
    elif btc_dominance_proxy == "🔴 ضد الألت":
        score -= 0.4
        reasons.append("هيمنة ضد الألت")

    if funding < -0.0005:
        score += 0.6
        reasons.append("تمويل سلبي (داعم للشراء)")
    elif funding > 0.0005:
        score -= 0.5

    if is_new:
        score += 0.3
        reasons.append("عملة جديدة")

    # ===================== EARLY MOVE INTELLIGENCE =====================
    dist_ma = ((close - ma) / ma) * 100 if ma > 0 else 0.0

    if 0.2 <= dist_ma <= 2.8:
        score += 0.7
        reasons.append("بداية ترند مبكرة")
    elif dist_ma > 4.5:
        score -= 0.8
        reasons.append("بعيد عن المتوسط (دخول متأخر)")

    if dist_ma > 6.0:
        score -= 1.2
        reasons.append("ممتد زيادة")

    if breakout:
        try:
            idx = int(signal_row.name)
            if idx > 20:
                recent_high = _safe_float(
                    df["high"].iloc[:idx].rolling(20).max().iloc[-1]
                )
                ext = ((close - recent_high) / recent_high) * 100 if recent_high > 0 else 0.0

                if ext <= 1.8:
                    score += 0.6
                    reasons.append("اختراق مبكر")
                elif 1.8 < ext <= 3.0:
                    score += 0.2
                elif ext > 3.5:
                    score -= 0.7
                    reasons.append("اختراق متأخر")
        except Exception:
            pass

    if breakout and mtf_confirmed and vol_ratio >= 1.6 and candle_strength >= 0.45:
        score += 0.5
        reasons.append("اختراق قوي مؤكد")

    if score >= 8.8:
        if not ((breakout or pre_breakout) and mtf_confirmed and vol_ratio >= 1.5):
            score -= 0.8

    # ===================== FINAL =====================
    score = max(0.0, min(9.2, score))
    score = round(score, 1)

    # ===================== Fake signal =====================
    fake_signal = False

    if score < 4.5:
        fake_signal = True

    if rejection and candle_strength < 0.48:
        fake_signal = True

    if close <= ma * 0.997 and not breakout and not pre_breakout:
        fake_signal = True

    if rsi < 46 and not breakout and not pre_breakout:
        fake_signal = True

    if score >= 8.5 and vol_ratio < 1.15:
        fake_signal = True

    return {
        "score": score,
        "reasons": list(dict.fromkeys(reasons)),
        "fake_signal": fake_signal,
        "signal": score >= 5.5,
        "funding_label": funding_label,
        "signal_rating": classify_signal(score),
    }
