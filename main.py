from services.okx_client import get_tickers, get_candles
from services.telegram_sender import send_telegram_message
from analysis.indicators import to_dataframe, add_ma, add_rsi, add_atr
from analysis.long_strategy import early_bullish_signal


def run():
    print("🚀 Bot Started...")

    futures = get_tickers("SWAP")
    print(f"Fetched {len(futures)} futures pairs")

    usdt_pairs = [p for p in futures if "USDT" in p["instId"]]
    print(f"USDT pairs: {len(usdt_pairs)}")

    tested = 0

    for pair_data in usdt_pairs[:20]:
        symbol = pair_data["instId"]

        candles = get_candles(symbol, timeframe="15m", limit=100)
        if not candles:
            continue

        df = to_dataframe(candles)
        if df.empty:
            continue

        df = add_ma(df, 20)
        df = add_rsi(df, 14)
        df = add_atr(df, 14)

        signal = early_bullish_signal(df)

        if signal:
            price = df.iloc[-1]["close"]

            message = (
                f"🚀 <b>LONG SIGNAL</b>\n\n"
                f"💎 <b>Pair:</b> {symbol}\n"
                f"💰 <b>Price:</b> <code>{price}</code>\n"
                f"⏱ <b>Timeframe:</b> 15m\n"
            )

            print(message)
            send_telegram_message(message)

        tested += 1

    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    run()
