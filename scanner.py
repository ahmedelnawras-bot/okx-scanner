import requests
import time
import pandas as pd
import numpy as np

TELEGRAM_TOKEN = "8626651293:AAGTVnwdW36qLsoZdmC2ngKoYUGMeYZyjsg"
CHAT_ID = "5523662724"

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"})

def get_okx_pairs():
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    r = requests.get(url).json()
    pairs = [x["instId"] for x in r["data"] if x["instId"].endswith("-USDT")]
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

def scan():
    print("Starting scan...")
    pairs = get_okx_pairs()
    print(f"Found {len(pairs)} pairs")
    for symbol in pairs:
        try:
            df = get_candles(symbol)
            if df is None:
                continue
            vol_spike = check_volume_spike(df)
            bb_squeeze = check_bb_squeeze(df)
            if vol_spike and bb_squeeze:
                msg = f"🚨 <b>STRONG SIGNAL</b>\n{symbol}\n✅ Volume Spike + BB Squeeze\nالاتنين مع بعض - فرصة قوية"
                send_telegram(msg)
                print(f"STRONG: {symbol}")
            elif vol_spike:
                msg = f"🔵 <b>Volume Spike</b>\n{symbol}\nارتفاع حجم تداول غير عادي على 4H"
                send_telegram(msg)
                print(f"VOL: {symbol}")
            elif bb_squeeze:
                msg = f"🟡 <b>BB Squeeze</b>\n{symbol}\nضغط في السعر - انتظر الانفجار على 4H"
                send_telegram(msg)
                print(f"SQZ: {symbol}")
            time.sleep(0.3)
        except Exception as e:
            print(f"Error {symbol}: {e}")
            continue
    print("Scan complete")

while True:
    scan()
    print("Waiting 4 hours...")
    time.sleep(14400)
