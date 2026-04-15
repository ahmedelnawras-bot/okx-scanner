# main.py

from services.okx_client import get_tickers

def run():
    print("🚀 Bot Started...")

    futures = get_tickers("SWAP")
    print(f"Fetched {len(futures)} futures pairs")

    usdt_pairs = [p for p in futures if "USDT" in p["instId"]]
    print(f"USDT pairs: {len(usdt_pairs)}")

    for pair in usdt_pairs[:10]:
        print(pair["instId"], pair["last"])

if __name__ == "__main__":
    run()
