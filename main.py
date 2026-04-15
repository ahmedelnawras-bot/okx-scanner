from services.okx_client import get_tickers, get_candles
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
        pair = pair_data["instId"]

        candles = get_candles(pair, timeframe="15m", limit=100)
        if not candles:
            continue

        df = to_dataframe(candles)
        df = add_ma(df, 20)
        df = add_rsi(df, 14)
        df = add_atr(df, 14)

        signal = early_bullish_signal(df)

        if signal:
            print("🚀 LONG SIGNAL")
            print(f"Pair: {pair}")
            print(f"Price: {signal['price']}")
            print(f"Score: {signal['score']}")
            print(f"RSI: {signal['rsi']}")
            print(f"Vol Ratio: {signal['vol_ratio']}")
            print(f"Reasons: {', '.join(signal['reasons'])}")
            print("-" * 40)

        tested += 1

    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    run()
