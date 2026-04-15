import sys
import os
import time
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.okx_client import get_tickers, get_candles
from services.telegram_sender import send_telegram_message
from analysis.indicators import to_dataframe, add_ma, add_rsi, add_atr
from analysis.long_strategy import early_bullish_signal
from analysis.scoring import calculate_long_score

COOLDOWN_SECONDS = 900  # 15 دقيقة
COOLDOWN_FILE = "cooldown.json"


def load_cooldown():
    if os.path.exists(COOLDOWN_FILE):
        try:
            with open(COOLDOWN_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cooldown(data):
    try:
        with open(COOLDOWN_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving cooldown file: {e}")


def is_volume_spike(df, multiplier=1.5):
    if df is None or df.empty or len(df) < 20:
        return False

    last_volume = df["volume"].iloc[-1]
    avg_volume_20 = df["volume"].rolling(20).mean().iloc[-1]

    if avg_volume_20 == 0:
        return False

    return last_volume >= (avg_volume_20 * multiplier)


last_signals = load_cooldown()


def run():
    global last_signals

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
    sent_in_this_run = set()

    for pair_data in usdt_pairs[:200]:
        tested += 1
        symbol = pair_data["instId"]

        try:
            candles = get_candles(symbol, "15m", 100)
            df = to_dataframe(candles)

            if df.empty:
                print(f"{symbol} → empty dataframe")
                continue

            df = add_ma(df)
            df = add_rsi(df)
            df = add_atr(df)

            volume_spike = is_volume_spike(df, multiplier=1.5)

            signal = early_bullish_signal(df)

            if signal:
                score = calculate_long_score(df)
            else:
                score = 0

            print(f"{symbol} → signal: {signal} | score: {score} | volume_spike: {volume_spike}")

            if not (signal and score >= 7.5 and volume_spike):
                continue

            price = df["close"].iloc[-1]
            now = time.time()

            current_candle_time = str(df.index[-1])
            key = f"{symbol}_long_{current_candle_time}"

            if key in sent_in_this_run:
                print(f"{symbol} → skipped (already sent in run)")
                continue

            last_time = float(last_signals.get(key, 0))
            if now - last_time < COOLDOWN_SECONDS:
                print(f"{symbol} → skipped (cooldown)")
                continue

            message = f"""🚀 لونج فيوتشر

{symbol}

💰 {price}
⏱ 15m

⭐ {score} / 10
🪙 BTC: --

📊 إشارة لونج أولية
💥 Volume Spike
🔥 Long detected
"""

            send_telegram_message(message)

            last_signals[key] = now
            save_cooldown(last_signals)
            sent_in_this_run.add(key)

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    run()
