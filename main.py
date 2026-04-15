import time
from data import get_tickers, get_candles, to_dataframe
from indicators import add_ma, add_rsi, add_atr
from strategy import early_bullish_signal
from telegram_utils import send_telegram_message
from analysis.scoring import calculate_long_score


def run():
    print("🚀 Bot Started...")

    # test message
    send_telegram_message("✅ Test message from bot")

    futures = get_tickers("SWAP")
    print(f"Fetched {len(futures)} futures pairs")

    usdt_pairs = [p for p in futures if "USDT" in p["instId"]]
    print(f"USDT pairs: {len(usdt_pairs)}")

    tested = 0

    for pair_data in usdt_pairs[:20]:
        symbol = pair_data["instId"]

        try:
            candles = get_candles(symbol, "15m")
            df = to_dataframe(candles)

            if df.empty:
                continue

            df = add_ma(df, 20)
            df = add_rsi(df, 14)
            df = add_atr(df, 14)

            signal = early_bullish_signal(df)

            if signal:
                price = df.iloc[-1]["close"]
                score = calculate_long_score(df)

                message = (
                    f"\u200E🚀 <b>LONG FUTURES</b>\n"
                    f"\u200E<b>{symbol}</b>\n\n"
                    f"\u200E💰 <code>{price}</code>|⏱15m\n\n"
                    f"\u200E⭐ {score} / 10\n"
                    f"\u200E🛑 --\n\n"
                    f"\u200E🪙 BTC: --\n\n"
                    f"\u200E📊 إشارة لونج أولية\n"
                    f"\u200E🔥 <b>Long detected</b>"
                )

                print(message)
                send_telegram_message(message)

            tested += 1

        except Exception as e:
            print(f"Error with {symbol}: {e}")
            continue

    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    run()
