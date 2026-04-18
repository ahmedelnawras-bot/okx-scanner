from analysis.scoring_short import calculate_short_score, is_breakdown
from tracking.performance import register_trade

# باقي الاستيرادات زي main بالظبط

SCAN_LIMIT = 150


def build_short_message(symbol, price, score_result, stop_loss):
    return f"""
🔴 SHORT | {symbol}

💰 السعر: {price:.6f}
⭐ سكور: {score_result['score']}

🛑 SL: {stop_loss:.6f}

📊 الأسباب:
- {' | '.join(score_result.get("reasons", []))}

⚠️ تحذيرات:
- {' | '.join(score_result.get("warning_reasons", []))}

⚖️ المخاطرة: {score_result.get("risk_level")}
"""


def run_short_scanner():
    pairs = get_ranked_pairs()[:SCAN_LIMIT]

    for pair in pairs:
        symbol = pair["instId"]

        candles = get_candles(symbol)
        df = to_dataframe(candles)

        if df is None or len(df) < 30:
            continue

        breakdown = is_breakdown(df)
        mtf_confirmed = is_higher_timeframe_confirmed(symbol)
        funding = get_funding_rate(symbol)

        score_result = calculate_short_score(
            df=df,
            mtf_confirmed=mtf_confirmed,
            btc_mode=get_btc_mode(),
            breakdown=breakdown,
            is_new=False,
            funding=funding,
        )

        if score_result["fake_signal"]:
            continue

        if score_result["score"] < 6.3:
            continue

        signal_row = get_signal_row(df)
        price = float(signal_row["close"])

        # SL فوق القمة
        high = float(signal_row["high"])
        stop_loss = round(high * 1.003, 6)

        message = build_short_message(
            symbol,
            price,
            score_result,
            stop_loss
        )

        send_telegram_message(message)

        register_trade(
            redis_client=r,
            symbol=symbol,
            market_type="futures",
            side="short",   # 🔥 المهم
            candle_time=get_signal_candle_time(df),
            entry=price,
            sl=stop_loss,
            score=score_result["score"],
        )
