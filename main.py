import sys
import os
import time
import redis

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.okx_client import get_tickers, get_candles
from services.telegram_sender import send_telegram_message
from analysis.indicators import to_dataframe, add_ma, add_rsi, add_atr
from analysis.long_strategy import early_bullish_signal
from analysis.scoring import calculate_long_score

COOLDOWN_SECONDS = 3600   # ساعة كاملة للفيوتشر
MAX_ALERTS_PER_RUN = 3
SCAN_LIMIT = 200

REDIS_URL = os.environ.get("REDIS_URL")

r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        print("✅ Redis connected")
    except Exception as e:
        print(f"❌ Redis connection error: {e}")
        r = None
else:
    print("⚠️ REDIS_URL not found")


def is_volume_spike(df, multiplier=1.2):
    if df is None or df.empty or len(df) < 20:
        return False

    last_volume = df["volume"].iloc[-1]
    avg_volume_20 = df["volume"].rolling(20).mean().iloc[-1]

    if avg_volume_20 == 0:
        return False

    return last_volume >= (avg_volume_20 * multiplier)


def get_last_candle_time(df):
    try:
        ts = df["ts"].iloc[-1]
        ts = int(ts)
        if ts > 10_000_000_000:
            ts = ts // 1000
        return ts
    except Exception as e:
        print(f"⚠️ candle time error: {e}")
        return 0


def get_same_candle_key(symbol, candle_time, signal_type="long"):
    return f"sent:{signal_type}:{symbol}:{candle_time}"


def get_cooldown_key(symbol, signal_type="long"):
    return f"cooldown:{signal_type}:{symbol}"


def already_sent_same_candle(symbol, candle_time, signal_type="long"):
    if not r:
        return False

    key = get_same_candle_key(symbol, candle_time, signal_type)
    try:
        return bool(r.exists(key))
    except Exception as e:
        print(f"Redis exists error (same candle): {e}")
        return False


def in_cooldown(symbol, signal_type="long"):
    if not r:
        return False

    key = get_cooldown_key(symbol, signal_type)
    try:
        return bool(r.exists(key))
    except Exception as e:
        print(f"Redis exists error (cooldown): {e}")
        return False


def mark_sent(symbol, candle_time, signal_type="long"):
    if not r:
        return

    same_candle_key = get_same_candle_key(symbol, candle_time, signal_type)
    cooldown_key = get_cooldown_key(symbol, signal_type)

    try:
        r.set(same_candle_key, "1", ex=7200)  # نفس الشمعة محفوظة ساعتين
        r.set(cooldown_key, "1", ex=COOLDOWN_SECONDS)  # كولداون ساعة
        print(f"✅ Redis saved for {symbol} | candle={candle_time}")
    except Exception as e:
        print(f"Redis save error: {e}")


def get_btc_mode():
    """
    تحليل بسيط للبيتكوين على 1H
    """
    try:
        candles = get_candles("BTC-USDT-SWAP", "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return "🟡 محايد"

        df = add_ma(df)
        df = add_rsi(df)

        last = df.iloc[-1]
        ma_value = last.get("ma", None)
        rsi_value = float(last.get("rsi", 50))

        if ma_value is not None:
            if last["close"] > ma_value and rsi_value >= 55:
                return "🟢 صاعد (داعم)"
            elif last["close"] < ma_value and rsi_value <= 45:
                return "🔴 هابط (ضاغط)"
            else:
                return "🟡 محايد"

        return "🟡 محايد"

    except Exception as e:
        print(f"BTC mode error: {e}")
        return "🟡 محايد"


def is_higher_timeframe_confirmed(symbol):
    """
    تأكيد 1H مرن:
    - فوق MA20 = نقطة
    - RSI > 50 = نقطة
    ويكفي نقطة واحدة
    """
    try:
        candles = get_candles(symbol, "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return False

        df = add_ma(df)
        df = add_rsi(df)

        last = df.iloc[-1]
        ma_value = last.get("ma", None)

        score = 0

        if ma_value is not None and last["close"] > ma_value:
            score += 1

        if float(last.get("rsi", 0)) > 50:
            score += 1

        return score >= 1

    except Exception as e:
        print(f"MTF error on {symbol}: {e}")
        return False


def is_breakout(df, lookback=20):
    try:
        if df is None or df.empty or len(df) < lookback + 2:
            return False

        recent_high = df["high"].rolling(lookback).max().iloc[-2]
        last_close = df["close"].iloc[-1]
        return bool(last_close > recent_high)
    except Exception as e:
        print(f"Breakout error: {e}")
        return False


def calculate_stop_loss(price, atr_value):
    try:
        return round(float(price) - (float(atr_value) * 1.2), 6)
    except Exception:
        return round(float(price), 6)


def clean_symbol_for_message(symbol):
    return symbol.replace("-SWAP", "")


def build_tradingview_link(symbol):
    """
    TradingView futures on OKX perp format:
    SPACE-USDT-SWAP -> OKX:SPACEUSDT.P
    BTC-USDT-SWAP   -> OKX:BTCUSDT.P
    """
    base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "").replace("-", "")
    tv_symbol = f"OKX:{base}USDT.P"
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"


def run():
    print("🚀 Bot Started...")

    btc_mode = get_btc_mode()
    print(f"BTC mode: {btc_mode}")

    futures = get_tickers("SWAP")
    print(f"Fetched {len(futures)} futures pairs")

    usdt_pairs = [
        p for p in futures
        if "USDT" in p["instId"]
        and not p["instId"].startswith((
            "USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP"
        ))
    ]

    print(f"USDT pairs: {len(usdt_pairs)}")

    tested = 0
    collected_keys = set()
    candidates = []

    for pair_data in usdt_pairs[:SCAN_LIMIT]:
        tested += 1
        symbol = pair_data["instId"]

        try:
            candles = get_candles(symbol, "15m", 100)
            df = to_dataframe(candles)

            if df is None or df.empty:
                print(f"{symbol} → empty dataframe")
                continue

            df = add_ma(df)
            df = add_rsi(df)
            df = add_atr(df)

            signal = early_bullish_signal(df)
            volume_spike = is_volume_spike(df, multiplier=1.2)
            mtf_confirmed = is_higher_timeframe_confirmed(symbol)
            breakout = is_breakout(df, lookback=20)

            if signal:
                score = calculate_long_score(df)

                if volume_spike:
                    score += 0.3
                else:
                    score -= 1.0

                if mtf_confirmed:
                    score += 0.5
                else:
                    score -= 0.5

                if breakout:
                    score += 0.5

                if "🔴" in btc_mode:
                    score -= 1.0
                elif "🟢" in btc_mode:
                    score += 0.3

                if score < 0:
                    score = 0
                if score > 10:
                    score = 10
            else:
                score = 0

            print(
                f"{symbol} → signal: {signal} | "
                f"score: {score} | "
                f"volume_spike: {volume_spike} | "
                f"mtf: {mtf_confirmed} | "
                f"breakout: {breakout}"
            )

            if not signal:
                continue

            if score < 7.5:
                continue

            if score < 8 and not volume_spike:
                continue

            candle_time = get_last_candle_time(df)
            if candle_time == 0:
                print(f"{symbol} → skipped (invalid candle time)")
                continue

            same_candle_key = get_same_candle_key(symbol, candle_time, "long")

            if same_candle_key in collected_keys:
                print(f"{symbol} → skipped (already collected in this run)")
                continue

            if already_sent_same_candle(symbol, candle_time, "long"):
                print(f"{symbol} → skipped (same candle in Redis)")
                continue

            if in_cooldown(symbol, "long"):
                print(f"{symbol} → skipped (cooldown in Redis)")
                continue

            price = float(df["close"].iloc[-1])
            atr_value = float(df["atr"].iloc[-1])
            stop_loss = calculate_stop_loss(price, atr_value)
            tv_link = build_tradingview_link(symbol)
            msg_symbol = clean_symbol_for_message(symbol)

            reasons = ["زخم مبكر"]
            flags = []

            if volume_spike:
                reasons.append("فوليوم قوي")
                flags.append("Vol ↑")

            if breakout:
                reasons.append("اختراق")
                flags.append("Break ✔")

            if float(df["rsi"].iloc[-1]) > 50:
                flags.append("RSI ↑")

            if mtf_confirmed:
                reasons.append("تأكيد 1H")
                flags.append("MTF ✔")

            reason_line = " + ".join(reasons)
            flags_line = " | ".join(flags) if flags else "Setup"

            message = f"""🚀 لونج فيوتشر | {msg_symbol}

💰 {round(price, 6)} | ⏱ 15m
⭐ {round(score, 1)} / 10 | 🛑 {stop_loss}

🪙 BTC: {btc_mode}

📊 {reason_line}

🔥 {flags_line}

🔗 TradingView
{tv_link}
"""

            candidates.append({
                "symbol": symbol,
                "score": float(score),
                "volume_spike": bool(volume_spike),
                "message": message,
                "candle_time": candle_time,
            })

            collected_keys.add(same_candle_key)

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    candidates.sort(
        key=lambda x: (x["score"], x["volume_spike"]),
        reverse=True
    )

    top_candidates = candidates[:MAX_ALERTS_PER_RUN]

    sent_count = 0

    for candidate in top_candidates:
        sent_ok = send_telegram_message(candidate["message"])

        if sent_ok:
            mark_sent(
                symbol=candidate["symbol"],
                candle_time=candidate["candle_time"],
                signal_type="long",
            )
            sent_count += 1
            print(
                f'SENT → {candidate["symbol"]} | '
                f'score: {candidate["score"]} | '
                f'volume_spike: {candidate["volume_spike"]}'
            )
        else:
            print(f'FAILED SEND → {candidate["symbol"]}')

    print(f"Candidates found: {len(candidates)}")
    print(f"Sent alerts this run: {sent_count}")
    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    while True:
        try:
            run()
        except Exception as e:
            print(f"Fatal error: {e}")

        print("Sleeping 60 seconds...")
        time.sleep(60)
