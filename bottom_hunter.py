import requests
import time
import pandas as pd
import numpy as np

TELEGRAM_TOKEN = "8626651293:AAGTVnwdW36qLsoZdmC2ngKoYUGMeYZyjsg"
CHAT_ID = "5523662724"

EXCLUDE = ["USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","BRL1","WIN","SHIB","USDG","SPURS","NFT","USDP","USDD","FRAX","LUSD","GUSD","HUSD","SUSD","CUSD","ZUSD","USDX","USDN","USDK","USDQ","USDB","SPY","TSLA","AAPL","AMZN","GOOGL","MSFT","NVDA","META","NFLX","XAU","XAUT","XAG","PAXG","OIL","CRUDE","USDG"]

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"})

def get_pairs(inst_type):
    url = f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}"
    r = requests.get(url).json()
    if inst_type == "SPOT":
        pairs = [x["instId"] for x in r["data"] if x["instId"].endswith("-USDT") and x["instId"].split("-")[0] not in EXCLUDE]
    else:
        pairs = [x["instId"] for x in r["data"] if "USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
    return pairs

def get_candles(symbol, bar, limit=100):
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
    r = requests.get(url).json()
    if "data" not in r or len(r["data"]) < 30:
        return None
    df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
    df = df.astype({"close": float, "vol": float, "high": float, "low": float, "open": float})
    df = df.iloc[::-1].reset_index(drop=True)
    return df

def check_rsi(df, period=14):
    close = df["close"]
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = -delta.where(delta < 0, 0).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def check_volume_spike(df, multiplier=3):
    avg_vol = df["vol"].iloc[-20:-1].mean()
    last_vol = df["vol"].iloc[-1]
    return last_vol > avg_vol * multiplier

def check_drop_50(df_1d):
    if len(df_1d) < 30:
        return False, 0
    high_30d = df_1d["high"].iloc[-30:].max()
    current = df_1d["close"].iloc[-1]
    drop = (high_30d - current) / high_30d * 100
    return drop >= 50, round(drop, 1)

def check_rsi_recovery(df_1d):
    rsi = check_rsi(df_1d)
    prev_rsi = rsi.iloc[-2]
    curr_rsi = rsi.iloc[-1]
    was_oversold = prev_rsi < 30
    recovering = curr_rsi > 35
    return was_oversold and recovering, round(curr_rsi, 1)

def check_candle_pattern(df_4h):
    last = df_4h.iloc[-1]
    prev = df_4h.iloc[-2]
    body = abs(last["close"] - last["open"])
    total = last["high"] - last["low"]
    if total == 0:
        return False, ""
    lower_wick = min(last["open"], last["close"]) - last["low"]
    is_hammer = lower_wick > body * 2 and last["close"] > last["open"]
    is_engulfing = (last["close"] > last["open"] and
                    prev["close"] < prev["open"] and
                    last["close"] > prev["open"] and
                    last["open"] < prev["close"])
    if is_hammer:
        return True, "🔨 Hammer"
    elif is_engulfing:
        return True, "💚 Bullish Engulfing"
    return False, ""

def scan(inst_type):
    label = "سبوت 🟢" if inst_type == "SPOT" else "فيوتشر 🔴"
    bar = "1D" if inst_type == "SPOT" else "1D"
    print(f"Bottom Hunter scanning {inst_type}...")
    pairs = get_pairs(inst_type)
    print(f"Found {len(pairs)} pairs")

    for symbol in pairs:
        try:
            df_1d = get_candles(symbol, "1D", limit=60)
            df_4h = get_candles(symbol, "4H", limit=100)

            if df_1d is None or df_4h is None:
                continue

            dropped, drop_pct = check_drop_50(df_1d)
            if not dropped:
                continue

            vol_spike = check_volume_spike(df_4h)
            if not vol_spike:
                continue

            rsi_ok, rsi_val = check_rsi_recovery(df_1d)
            if not rsi_ok:
                continue

            pattern_ok, pattern_name = check_candle_pattern(df_4h)
            if not pattern_ok:
                continue

            msg = (
                f"🎯 <b>Bottom Hunter</b>\n"
                f"{symbol} | {label}\n"
                f"📉 نزل {drop_pct}% في 30 يوم\n"
                f"🔵 Volume Spike\n"
                f"📊 RSI 1D: {rsi_val} صاعد من oversold\n"
                f"{pattern_name}\n"
                f"⚡️ ارتداد محتمل من القاع"
            )
            send_telegram(msg)
            print(f"BOTTOM: {symbol}")

            time.sleep(0.3)

        except Exception as e:
            print(f"Error {symbol}: {e}")
            continue

    print(f"Bottom Hunter {inst_type} complete")

while True:
    scan("SPOT")
    scan("SWAP")
    print("Waiting 4 hours...")
    time.sleep(14400)
