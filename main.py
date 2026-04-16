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

COOLDOWN_SECONDS = 900   # 15 دقيقة
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
    """
    نجيب وقت آخر شمعة بشكل ثابت قدر الإمكان.
    """
    if "timestamp" in df.columns:
        try:
            value = df["timestamp"].iloc[-1]
            if hasattr(value, "timestamp"):
                return int(value.timestamp())
            value_str = str(value).strip()
            if value_str.isdigit():
                value_int = int(value_str)
                if value_int > 10_000_000_000:
                    return value_int // 1000
                return value_int
            return value_str
        except Exception:
            pass

    if "ts" in df.columns:
        try:
            value = df["ts"].iloc[-1]
            if isinstance(value, (int, float)):
                if value > 10_000_000_000:
                    return int(value // 1000)
                return int(value)

            value_str = str(value).strip()
            if value_str.isdigit():
                value_int = int(value_str)
                if value_int > 10_000_000_000:
                    return value_int // 1000
                return value_int
            return value_str
        except Exception:
            pass

    try:
        idx_value = df.index[-1]
        if hasattr(idx_value, "timestamp"):
            return int(idx_value.timestamp())
        value_str = str(idx_value).strip()
        if value_str.isdigit():
            value_int = int(value_str)
            if value_int > 10_000_000_000:
                return value_int // 1000
            return value_int
        return value_str
    except Exception:
        pass

    # fallback: bucket 15m
    now = int(time.time())
    return (now // 900) * 900


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
        # نفس الشمعة نخليها محفوظة شوية أطول من 15 دقيقة
        r.set(same_candle_key, "1", ex=3600)
        # الكولداون 15 دقيقة
        r.set(cooldown_key, "1", ex=COOLDOWN_SECONDS)
        print(f"✅ Redis saved for {symbol} | candle={candle_time}")
    except Exception as e:
        print(f"Redis save error: {e}")


def run():
    print("🚀 Bot Started...")

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

            if signal:
                score = calculate_long_score(df)

                if not volume_spike:
                    score -= 1.5

                if score < 0:
                    score = 0
            else:
                score = 0

            print(f"{symbol} → signal: {signal} | score: {score} | volume_spike: {volume_spike}")

            if not signal:
                continue

            if score < 7.5:
                continue

            if score < 8 and not volume_spike:
                continue

            candle_time = get_last_candle_time(df)
            same_candle_key = get_same_candle_key(symbol, candle_time, "long")

            # منع التكرار داخل نفس run
            if same_candle_key in collected_keys:
                print(f"{symbol} → skipped (already collected in this run)")
                continue

            # منع التكرار من Redis لنفس الشمعة
            if already_sent_same_candle(symbol, candle_time, "long"):
                print(f"{symbol} → skipped (same candle in Redis)")
                continue

            # منع إعادة نفس الزوج خلال الكولداون
            if in_cooldown(symbol, "long"):
                print(f"{symbol} → skipped (cooldown in Redis)")
                continue

            price = df["close"].iloc[-1]
            volume_line = "💥 Volume Spike" if volume_spike else "📊 Volume عادي"

            message = f"""🚀 لونج فيوتشر

{symbol}

💰 {price}
⏱ 15m

⭐ {score} / 10
🪙 BTC: --

📊 إشارة لونج أولية
{volume_line}
🔥 Long detected
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
