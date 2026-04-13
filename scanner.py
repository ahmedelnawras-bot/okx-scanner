import requests
import time
import pandas as pd
import numpy as np

TELEGRAM_TOKEN = "8626651293:AAGTVnwdW36qLsoZdmC2ngKoYUGMeYZyjsg"
CHAT_ID = "5523662724"

EXCLUDE = ["USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","BRL1","WIN","SHIB","USDG","SPURS","NFT","USDP","USDD","FRAX","LUSD","GUSD","HUSD","SUSD","CUSD","ZUSD","USDX","USDN","USDK","USDQ","USDB"]

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

def get_candles(symbol, limit=50):
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=4H&limit={limit}"
    r = requests.get(url).json()
    if "data" not in r or len(r["data"]) < 20:
        return None
    df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
    df = df.astype({"close": float, "vol": float, "high": float, "low": float})
    df = df.iloc[::-1].reset_index(drop=True)
    return df

def check_volume_spike(df, multiplier=3):
    avg_vol = df["vol"].iloc[-20:-1].mean()
    last_vol = df["vol"].iloc[-1]
    return last_vol > avg_vol * multiplier

def check_bb_squeeze(df, period=20, threshold=0.03):
    close = df["close"]
    ma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = ma + 2 * std
    lower = ma - 2 * std
    bandwidth = (upper - lower) / ma
    return bandwidth.iloc[-1] < threshold

def scan(inst_type):
    label = "سبوت 🟢" if inst_type == "SPOT" else "فيوتشر 🔴"
    print(f"Scanning {inst_type}...")
    pairs = get_pairs(inst_type)
    print(f"Found {len(pairs)} pairs")
    for symbol in pairs:
        try:
            df = get_candles(symbol)
            if df is None:
                continue
            vol_spike = check_volume_spike(df)
            bb_squeeze = check_bb_squeeze(df)
            if vol_spike and bb_squeeze:
                msg = f"🚨 <b>STRONG SIGNAL</b>\n{symbol}\n{label}\n✅ Volume Spike + BB Squeeze\nالاتنين مع بعض - فرصة قوية"
                send_telegram(msg)
            elif vol_spike:
                msg = f"🔵 <b>Volume Spike</b>\n{symbol}\n{label}\nارتفاع حجم تداول غير عادي على 4H"
                send_telegram(msg)
            elif bb_squeeze:
                msg = f"🟡 <b>BB Squeeze</b>\n{symbol}\n{label}\nضغط في السعر - انتظر الانفجار على 4H"
                send_telegram(msg)
            time.sleep(0.3)
        except Exception as e:
            print(f"Error {symbol}: {e}")
            continue
    print(f"{inst_type} scan complete")

while True:
    scan("SPOT")
    scan("SWAP")
    print("Waiting 4 hours...")
    time.sleep(14400)
