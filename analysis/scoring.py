def is_breakout(df, lookback=20):
    try:
        if df is None or df.empty or len(df) < (lookback + 2):
            return False

        recent_high = df["high"].rolling(lookback).max().iloc[-2]
        current_close = float(df["close"].iloc[-1])

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

    try:
        if df is None or df.empty or len(df) < 25:
            return {
                "score": 0.0,
                "reasons": [],
                "fake_signal": True,
                "signal": False,
                "funding_label": classify_funding_simple(funding),
                "signal_rating": "⚡ عادي",
            }

        # اختيار شمعة مؤكدة
        if "confirm" in df.columns and len(df) >= 2:
            last = df.iloc[-1]
            if str(int(float(last["confirm"]))) == "1":
                signal_row = last
                prev_row = df.iloc[-2]
            else:
                signal_row = df.iloc[-2]
                prev_row = df.iloc[-3] if len(df) >= 3 else df.iloc[-2]
        else:
            signal_row = df.iloc[-2]
            prev_row = df.iloc[-3] if len(df) >= 3 else df.iloc[-2]

        rsi = float(signal_row["rsi"])
        close = float(signal_row["close"])
        open_ = float(signal_row["open"])
        ma = float(signal_row["ma"]) if signal_row["ma"] == signal_row["ma"] else close

        prev_volume = float(prev_row["volume"])
        last_volume = float(signal_row["volume"])
        vol_ratio = (last_volume / prev_volume) if prev_volume > 0 else 1.0

        high = float(signal_row["high"])
        low = float(signal_row["low"])

        body = abs(close - open_)
        full = high - low
        upper_wick = high - max(open_, close)
        candle_strength = (body / full) if full > 0 else 0.0

        rejection = False
        if full > 0 and upper_wick > body * 1.5:
            rejection = True

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

        # Trend
        if close > ma:
            score += 0.8
            reasons.append("فوق MA")
        else:
            score -= 1.2

        # Candle
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

        if rejection:
            score -= 1.5

        # Breakout
        if breakout:
            score += 2.2
            reasons.append("اختراق")
        else:
            score -= 0.4

        # MTF
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

        # Quality
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

        # =========================
        # Fake signal (مخفف)
        # =========================
        fake_signal = False

        if score < 5.0:
            fake_signal = True

        if rejection and candle_strength < 0.55:
            fake_signal = True

        # ❌ اتشال شرط الفوليوم الضعيف

        if not is_new and not mtf_confirmed and not breakout and score < 5.5:
            fake_signal = True

        if close <= ma and not breakout:
            fake_signal = True

        if rsi < 50 and not breakout:
            fake_signal = True

        if score >= 8.5 and not (breakout or mtf_confirmed):
            fake_signal = True

        if score >= 8.5 and vol_ratio < 1.3:
            fake_signal = True

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

    except Exception:
        return {
            "score": 0.0,
            "reasons": [],
            "fake_signal": True,
            "signal": False,
            "funding_label": classify_funding_simple(funding),
            "signal_rating": "⚡ عادي",
        }
