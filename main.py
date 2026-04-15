import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
from services.okx_client import get_tickers, get_candles
from services.telegram_sender import send_telegram_message
from analysis.indicators import to_dataframe, add_ma, add_rsi, add_atr
from analysis.long_strategy import early_bullish_signal
from analysis.scoring import calculate_long_score


def run():
    print("🚀 Bot Started...")

    send_telegram_message("✅ Test message from bot")

    futures = get_tickers("SWAP")
    print(f"Fetched {len(futures)} futures pairs")

    usdt_pairs = [p for p in futures if "USDT" in p["instId"]]
    print(f"USDT pairs: {len(usdt_pairs)}")

    tested = 0

    for pair_data in usdt_pairs[:20]:
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
            print(f"{symbol} → signal: {signal}")

            score = calculate_long_score(df)
            price = df["close"].iloc[-1]

            if signal:
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

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    run()
