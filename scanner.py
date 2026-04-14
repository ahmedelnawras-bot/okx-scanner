
import requests
import time
import pandas as pd
import numpy as np

TELEGRAM_TOKEN = "8626651293:AAGTVnwdW36qLsoZdmC2ngKoYUGMeYZyjsg"
CHAT_ID = "5523662724"

EXCLUDE = ["USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","BRL1","WIN","SHIB","USDG","SPURS","NFT","USDP","USDD","FRAX","LUSD","GUSD","HUSD","SUSD","CUSD","ZUSD","USDX","USDN","USDK","USDQ","USDB","SPY","TSLA","AAPL","AMZN","GOOGL","MSFT","NVDA","META","NFLX","XAU","XAUT","XAG","PAXG","OIL","CRUDE"]

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"})

def get_btc_trend():
    url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=4H&limit=10"
    r = requests.get(url).json()
    if "data" not in r:
        return "غير معروف"
    closes = [float(x[4]) for x in r["data"]]
    closes.reverse()
    if closes[-1] > closes[-3]:
        return "إيجابي ✅"
    else:
        return "سلبي ⚠️"

def get_pairs(inst_type):
    url = f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}"
    r = requests.get(url).json()
    if inst_type == "SPOT":
        pairs = [(x["instId"], float(x["last"])) for x in r["data"] if x["instId"].endswith("-USDT") and x["instId"].split("-")[0] not in EXCLUDE]
    else:
        pairs = [(x["instId"], float(x["last"])) for x in r["data"] if "USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
    return pairs

def get_candles(symbol, limit=100):
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=4H&limit={limit}"
    r = requests.get(url).json()
    if "data" not in r or len(r["data"]) < 30:
        return None
    df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
    df = df.astype({"close": float, "vol": float, "high": float, "low": float, "open": float})
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

def check_rsi(df, period=14):
    close = df["close"]
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = -delta.where(delta < 0, 0).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

def check_above_ma20(df):
    ma20 = df["close"].rolling(20).mean().iloc[-1]
    return df["close"].iloc[-1] > ma20

def calc_score(vol_spike, bb_squeeze, rsi_ok, above_ma20, btc_positive):
    score = 0
    if vol_spike: score += 2
    if bb_squeeze: score += 2
    if rsi_ok: score += 2
    if above_ma20: score += 2
    if btc_positive: score += 2
    return score

def calc_entry_stop(df, inst_type):
    last_close = df["close"].iloc[-1]
    last_low = df["low"].iloc[-5:].min()
    entry = round(last_close, 6)
    stop = round(last_low * 0.98, 6)
    return entry, stop

def scan(inst_type, top_signals):
    label = "سبوت 🟢" if inst_type == "SPOT" else "فيوتشر 🔴"
    print(f"Scanning {inst_type}...")
    pairs = get_pairs(inst_type)
    btc_trend = get_btc_trend()
    btc_positive = "إيجابي" in btc_trend

    for symbol, price in pairs:
        try:
            df = get_candles(symbol)
            if df is None:
                continue

            vol_spike = check_volume_spike(df)
            bb_squeeze = check_bb_squeeze(df)
            rsi = check_rsi(df)
            above_ma20 = check_above_ma20(df)
            rsi_ok = 45 <= rsi <= 60

            if not (vol_spike or bb_squeeze):
                continue
            if not (rsi_ok and above_ma20):
                continue

            score = calc_score(vol_spike, bb_squeeze, rsi_ok, above_ma20, btc_positive)
            entry, stop = calc_entry_stop(df, inst_type)

            if vol_spike and bb_squeeze:
                signal_type = "🚨 <b>STRONG SIGNAL</b>"
            elif vol_spike:
                signal_type = "🔵 <b>Volume Spike</b>"
            else:
                signal_type = "🟡 <b>BB Squeeze</b>"

            msg = (
                f"{signal_type}\n"
                f"{symbol} | {label}\n"
                f"💰 السعر: {price}\n"
                f"📊 RSI: {rsi:.1f} | فوق MA20\n"
                f"🎯 دخول: {entry}\n"
                f"🛑 ستوب: {stop}\n"
                f"₿ BTC: {btc_trend}\n"
                f"🔥 تقييم الفرصة: {score}/10"
            )
            send_telegram(msg)

            top_signals.append({
                "symbol": symbol,
                "score": score,
                "label": label,
                "price": price,
                "entry": entry,
                "stop": stop
            })

            time.sleep(0.3)

        except Exception as e:
            print(f"Error {symbol}: {e}")
            continue

    print(f"{inst_type} scan complete")

def send_top10(top_signals):
    if not top_signals:
        return
    sorted_signals = sorted(top_signals, key=lambda x: x["score"], reverse=True)[:10]
    msg = "🏆 <b>أفضل الفرص الحالية:</b>\n\n"
    for i, s in enumerate(sorted_signals, 1):
        msg += f"{i}. {s['symbol']} ({s['label']}) - تقييم: {s['score']}/10\n"
        msg += f"   💰 {s['price']} | 🎯 {s['entry']} | 🛑 {s['stop']}\n\n"
    send_telegram(msg)

while True:
    top_signals = []
    scan("SPOT", top_signals)
    scan("SWAP", top_signals)
    send_top10(top_signals)
    print("Waiting 4 hours...")
    time.sleep(14400)
اضغط
ق
