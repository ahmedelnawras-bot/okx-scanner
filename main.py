import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
from services.okx_client import get_tickers, get_candles
from services.telegram_sender import send_telegram_message
from analysis.indicators import to_dataframe, add_ma, add_rsi, add_atr
from analysis.long_strategy import early_bullish_signal
from analysis.scoring import calculate_long_score

last_alert_time = {}


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
    cooldown = 3600  # ساعة

    for pair_data in usdt_pairs[:100]:
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

            signal = early_bullish_signal(df)

            if signal:
                score = calculate_long_score(df)
            else:
                score = 0

            print(f"{symbol} → signal: {signal} | score: {score}")

            price = df["close"].iloc[-1]
            now = time.time()

            if signal and score >= 7.5:
                if symbol in last_alert_time:
                    if now - last_alert_time[symbol] < cooldown:
                        print(f"{symbol} → skipped (cooldown)")
                        continue

                message = f"""
🚀 لونج فيوتشر

{symbol}

💰 {price}
⏱ 15m

⭐ {score} / 10
🪙 BTC: --

📊 إشارة لونج أولية
🔥 Long detected
"""

                send_telegram_message(message)
                last_alert_time[symbol] = now

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    run()
