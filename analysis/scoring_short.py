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


def is_breakdown(df, lookback=20):
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
        recent_low = _safe_float(df["low"].iloc[max(0, idx - lookback):idx].min())

        if recent_low <= 0:
            return False

        return close < recent_low
    except Exception:
        return False


def classify_funding_simple_short(funding):
    if funding > 0.0005:
        return "🟢 إيجابي"
    elif funding < -0.0005:
        return "🔴 سلبي"
    return "🟡 محايد"


def classify_signal(score):
    if score >= 8.5:
        return "🔥 نار"
    elif score >= 6.8:
        return "✅ جيد"
    return "⚡ عادي"


def get_btc_short_bias_proxy(btc_mode: str) -> str:
    if "🔴 هابط" in btc_mode:
        return "🟢 داعم للشورت"
    if "🟢 صاعد" in btc_mode:
        return "🔴 ضد الشورت"
    return "🟡 محايد"


def calculate_short_score(
    df,
    mtf_confirmed,
    btc_mode,
    breakdown,
    is_new,
    funding=0.0,
    btc_short_bias_proxy=None,
    vol_ratio=None,
    pre_breakdown=False,
    market_state=None,
    alt_mode=None,
    market_bias_label=None,
):
    funding_label = classify_funding_simple_short(funding)

    if btc_short_bias_proxy is None:
        btc_short_bias_proxy = get_btc_short_bias_proxy(btc_mode)

    signal_row, prev_row = _get_signal_and_prev_rows(df)
    if signal_row is None or prev_row is None:
        return {
            "score": 0.0,
            "reasons": [],
            "warning_reasons": [],
            "risk_level": "🟢 منخفضة",
            "fake_signal": True,
            "signal": False,
            "funding_label": funding_label,
            "signal_rating": "⚡ عادي",
        }

    score = 0.0
    reasons = []
    warning_reasons = []

    close = _safe_float(signal_row["close"])
    open_ = _safe_float(signal_row["open"])
    high = _safe_float(signal_row["high"])
    low = _safe_float(signal_row["low"])
    ma = _safe_float(signal_row.get("ma"), close)
    rsi = _safe_float(signal_row.get("rsi"), 50)
    prev_rsi = _safe_float(prev_row.get("rsi"), 50)
    rsi_momentum = prev_rsi - rsi

    vol = _safe_float(signal_row["volume"])
    prev_vol = _safe_float(prev_row["volume"])

    if vol_ratio is None:
        vol_ratio = vol / prev_vol if prev_vol > 0 else 1.0

    body = abs(close - open_)
    full = max(high - low, 0.0)
    lower_wick = max(min(open_, close) - low, 0.0)
    candle_strength = body / full if full > 0 else 0.0
    downside_rejection = full > 0 and lower_wick > body * 1.5

    dist_ma = ((ma - close) / ma) * 100 if ma > 0 else 0.0

    # ===================== BASIC SCORING =====================
    if 30 <= rsi <= 48 and rsi_momentum > 2.5:
        score += 1.7
        reasons.append("RSI هابط بقوة")
    elif 38 <= rsi <= 49:
        score += 1.1
        reasons.append("RSI في منطقة ضعف")
    elif 32 <= rsi < 38:
        score += 0.8
        reasons.append("RSI ضعيف")
    elif 28 <= rsi < 32 and rsi_momentum > 3.0:
        score += 0.5
        reasons.append("RSI منخفض بزخم")
    elif rsi < 28:
        score -= 0.8
        reasons.append("RSI منخفض جدًا (تشبع بيع)")
        warning_reasons.append("RSI منخفض جدًا (تشبع بيع)")
    elif rsi > 56:
        score -= 0.7
        warning_reasons.append("RSI غير داعم للشورت")

    if vol_ratio >= 2.0:
        score += 2.1
        reasons.append("فوليوم بيعي انفجاري")
    elif vol_ratio >= 1.5:
        score += 1.5
        reasons.append("فوليوم بيعي قوي")
    elif vol_ratio >= 1.2:
        score += 0.8
        reasons.append("فوليوم داعم")
    elif vol_ratio >= 1.08:
        score += 0.3
        reasons.append("فوليوم مقبول")

    if close < ma:
        score += 1.0
        reasons.append("أسفل المتوسط")
    else:
        score -= 0.8
        warning_reasons.append("فوق المتوسط")

    if candle_strength >= 0.65 and close < open_:
        score += 1.3
        reasons.append("شمعة هبوط قوية")
    elif candle_strength >= 0.45 and close < open_:
        score += 0.8
        reasons.append("شمعة هبوط جيدة")
    elif candle_strength >= 0.30 and close < open_:
        score += 0.3
        reasons.append("شمعة هبوط خفيفة")

    if downside_rejection:
        score -= 0.8
        warning_reasons.append("رفض سعري سفلي")

    if breakdown:
        score += 1.5
        reasons.append("كسر دعم")

    if pre_breakdown and not breakdown:
        score += 1.1
        reasons.append("ضغط بيعي تحت الدعم 🎯")

    if mtf_confirmed:
        score += 1.6
        reasons.append("تأكيد فريم الساعة")

    # ===================== MARKET CONTEXT =====================
    if market_state == "risk_off":
        score += 0.9
        reasons.append("السوق ضعيف ويدعم الشورت")
    elif market_state == "btc_leading":
        score += 0.5
        reasons.append("السيولة خارجة من الألت")
    elif market_state == "bull_market":
        score -= 0.7
        warning_reasons.append("السوق صاعد والشورت أصعب")
    elif market_state == "alt_season":
        score -= 0.8
        warning_reasons.append("الألت قوي ضد الشورت")
    else:
        if "🔴" in btc_mode:
            score += 0.6
            reasons.append("BTC هابط")
        elif "🟢" in btc_mode:
            score -= 0.4
            warning_reasons.append("BTC غير داعم للشورت")

        if btc_short_bias_proxy == "🟢 داعم للشورت":
            score += 0.4
            reasons.append("BTC داعم للشورت")
        elif btc_short_bias_proxy == "🔴 ضد الشورت":
            score -= 0.6
            warning_reasons.append("BTC ضد الشورت")

    if funding > 0.0005:
        score += 0.5
        reasons.append("تمويل إيجابي (داعم للشورت)")
    elif funding < -0.0005:
        score -= 0.4
        warning_reasons.append("تمويل سلبي (ضغط محتمل)")

    if is_new:
        score += 0.3
        reasons.append("عملة جديدة")

    # ===================== EARLY MOVE INTELLIGENCE =====================
    if 0.2 <= dist_ma <= 2.8:
        score += 0.7
        reasons.append("بداية هبوط مبكرة")
    elif 2.8 < dist_ma <= 4.5:
        score += 0.1
    elif dist_ma > 4.5:
        score -= 0.6
        reasons.append("بعيد عن المتوسط (دخول متأخر)")
        warning_reasons.append("بعيد عن المتوسط (دخول متأخر)")

    if dist_ma > 6.0:
        score -= 0.8
        reasons.append("ممتد هبوطًا")
        warning_reasons.append("ممتد هبوطًا")

    if breakdown:
        try:
            idx = int(signal_row.name)
            if idx > 20:
                recent_low = _safe_float(df["low"].iloc[max(0, idx - 20):idx].min())
                ext = ((recent_low - close) / recent_low) * 100 if recent_low > 0 else 0.0

                if ext <= 1.8:
                    score += 0.6
                    reasons.append("كسر مبكر")
                elif 1.8 < ext <= 3.0:
                    score += 0.2
                elif ext > 3.5:
                    score -= 0.5
                    reasons.append("كسر متأخر")
                    warning_reasons.append("كسر متأخر")
        except Exception:
            pass

    if breakdown and mtf_confirmed and vol_ratio >= 1.45 and candle_strength >= 0.45:
        score += 1.0
        reasons.append("كسر قوي مؤكد")

    if score >= 8.8:
        if not ((breakdown or pre_breakdown) and mtf_confirmed and vol_ratio >= 1.35):
            score -= 0.6

    # ===================== FINAL =====================
    score = max(0.0, min(9.2, score))
    score = round(score, 1)

    # ===================== Fake signal =====================
    fake_signal = False

    # إشارات ضعيفة جدًا
    if score < 4.2:
        fake_signal = True

    # رفض سفلي واضح + شمعة ضعيفة
    if downside_rejection and candle_strength < 0.42:
        fake_signal = True

    # السعر فوق المتوسط بوضوح بدون أي setup قوي
    if close >= ma * 1.004 and not breakdown and not pre_breakdown:
        if rsi > 54:
            fake_signal = True

    # RSI عالي ضد الشورت من غير كسر
    if rsi > 60 and not breakdown and not pre_breakdown:
        fake_signal = True

    # سكور عالي بدون فوليوم كفاية = تضخم
    if score >= 8.4 and vol_ratio < 1.10:
        fake_signal = True

    # لو السوق ضدك جدًا، ما نخليش ده fake مباشرة
    # فقط في الحالات الضعيفة جدًا وغير المؤكدة
    if market_state == "bull_market" and not breakdown and not pre_breakdown:
        if score < 6.1 and not mtf_confirmed:
            fake_signal = True

    if market_state == "alt_season" and not breakdown and not pre_breakdown:
        if score < 6.3 and not mtf_confirmed:
            fake_signal = True

    if btc_short_bias_proxy == "🔴 ضد الشورت" and not breakdown and not pre_breakdown:
        if score < 6.0 and rsi > 54:
            fake_signal = True

    # ===================== Risk Level (display only) =====================
    unique_warnings = list(dict.fromkeys(warning_reasons))

    risk_points = len(unique_warnings)

    if downside_rejection:
        risk_points += 1
    if funding < -0.0005:
        risk_points += 1
    if btc_short_bias_proxy == "🔴 ضد الشورت":
        risk_points += 1
    if dist_ma > 6.0:
        risk_points += 1
    if market_state == "bull_market":
        risk_points += 1
    if market_state == "alt_season":
        risk_points += 1

    if risk_points >= 4:
        risk_level = "🔴 عالية"
    elif risk_points >= 2:
        risk_level = "🟡 متوسطة"
    else:
        risk_level = "🟢 منخفضة"

    return {
        "score": score,
        "reasons": list(dict.fromkeys(reasons)),
        "warning_reasons": unique_warnings,
        "risk_level": risk_level,
        "fake_signal": fake_signal,
        "signal": score >= 5.3,
        "funding_label": funding_label,
        "signal_rating": classify_signal(score),
    }
