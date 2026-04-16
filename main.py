import sys
import os
import time
import json
import redis

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.okx_client import get_tickers, get_candles
from services.telegram_sender import send_telegram_message
from analysis.indicators import to_dataframe, add_ma, add_rsi, add_atr
from analysis.long_strategy import early_bullish_signal
from analysis.scoring import calculate_long_score

COOLDOWN_SECONDS = 900   # 15 دقيقة
MAX_ALERTS_PER_RUN = 3
REDIS_KEY = "alert_state"

REDIS_URL = os.environ.get("REDIS_URL")

r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        print("✅ Redis connected")
    except Exception as e:
        print(f"❌ Redis connection error: {e}")
        r = None
else:
    print("⚠️ REDIS_URL not found")


def load_state():
    default_state = {
        "last_sent_at": {},
        "last_fingerprint": {},
    }

    if not r:
        return default_state

    try:
        data = r.get(REDIS_KEY)
        if data:
            return json.loads(data)
    except Exception as e:
        print(f"Redis load error: {e}")

    return default_state


def save_state(state):
    if not r:
        return

    try:
        r.set(REDIS_KEY, json.dumps(state))
    except Exception as e:
        print(f"Redis save error: {e}")


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
    يجيب وقت آخر شمعة من الداتا نفسها
    """
    if "timestamp" in df.columns:
        try:
            value = df["timestamp"].iloc[-1]
            if hasattr(value, "timestamp"):
                return int(value.timestamp())
            return str(value)
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
                    return int(value_int // 1000)
                return value_int

            return value_str
        except Exception:
            pass

    try:
        idx_value = df.index[-1]
        if hasattr(idx_value, "timestamp"):
            return int(idx_value.timestamp())
        return str(idx_value)
    except Exception:
        pass

    now = time.time()
    return int(now // (15 * 60))


def get_fingerprint(df, symbol, signal_type="long"):
    candle_time = get_last_candle_time(df)
    return f"{symbol}_{signal_type}_{candle_time}"


def should_send_alert(state, signal_key, fingerprint, now, cooldown_seconds):
    # منع تكرار نفس الشمعة
    if state["last_fingerprint"].get(signal_key) == fingerprint:
        return False, "same candle"

    # منع إعادة التنبيه بسرعة
    last_time = float(state["last_sent_at"].get(signal_key, 0))
    if now - last_time < cooldown_seconds:
        return False, "cooldown"

    return True, "ok"


def mark_alert_sent(state, signal_key, fingerprint, now):
    state["last_sent_at"][signal_key] = now
    state["last_fingerprint"][signal_key] = fingerprint
    save_state(state)


def run():
    state = load_state()

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
    collected_fingerprints = set()
    candidates = []

    for pair_data in usdt_pairs[:200]:
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

            now = time.time()
            price = df["close"].iloc[-1]

            signal_key = f"{symbol}_long"
            fingerprint = get_fingerprint(df, symbol, "long")

            if fingerprint in collected_fingerprints:
                print(f"{symbol} → skipped (already collected in this run)")
                continue

            allowed, reason = should_send_alert(
                state=state,
                signal_key=signal_key,
                fingerprint=fingerprint,
                now=now,
                cooldown_seconds=COOLDOWN_SECONDS,
            )

            if not allowed:
                print(f"{symbol} → skipped ({reason})")
                continue

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
                "signal_key": signal_key,
                "fingerprint": fingerprint,
                "now": now,
            })

            collected_fingerprints.add(fingerprint)

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    candidates.sort(
        key=lambda x: (x["score"], x["volume_spike"]),
        reverse=True
    )

    top_candidates = candidates[:MAX_ALERTS_PER_RUN]

    sent_count = 0

    for candidate in top_candidates:
        send_telegram_message(candidate["message"])

        mark_alert_sent(
            state=state,
            signal_key=candidate["signal_key"],
            fingerprint=candidate["fingerprint"],
            now=candidate["now"],
        )

        sent_count += 1
        print(
            f'SENT → {candidate["symbol"]} | '
            f'score: {candidate["score"]} | '
            f'volume_spike: {candidate["volume_spike"]}'
        )

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
