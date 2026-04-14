import requests
import time
import pandas as pd
import numpy as np

# --- الإعدادات الأساسية ---
TELEGRAM_TOKEN = "8626651293:AAGTVnwdW36qLsoZdmC2ngKoYUGMeYZyjsg"
CHAT_ID = "5523662724"
EXCLUDE = ["USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","WIN","SHIB"]

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"})
    except:
        pass

def get_candles(symbol, bar='1H', limit=100):
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
    try:
        r = requests.get(url).json()
        if "data" not in r or len(r["data"]) < 30:
            return None
        df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df = df.astype({"close": float, "vol": float, "high": float, "low": float, "open": float})
        return df.iloc[::-1].reset_index(drop=True)
    except:
        return None

def get_btc_status():
    df_btc = get_candles("BTC-USDT", bar='4H', limit=30)
    if df_btc is None:
        return "غير معروف ⚠️"
    ma20 = df_btc["close"].rolling(20).mean().iloc[-1]
    current_price = df_btc["close"].iloc[-1]
    return "إيجابي ✅" if current_price > ma20 else "هابط (حذر) ⚠️"

def get_oi_value(symbol):
    try:
        url = f"https://www.okx.com/api/v5/public/open-interest?instId={symbol}"
        r = requests.get(url).json()
        if "data" in r and len(r["data"]) > 0:
            return float(r["data"][0]["oiCcy"])
    except:
        return 0
    return 0

def get_levels(df):
    cp = df["close"].iloc[-1]
    recent_low = df["low"].iloc[-2:].min()
    sl = recent_low * 0.997 
    risk = cp - sl
    tp = cp + (risk * 2)
    return cp, sl, tp

def scan(inst_type):
    label = "فيوتشر 🔴" if inst_type == "SWAP" else "سبوت 🟢"
    btc_status = get_btc_status()
    
    try:
        url_tickers = f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}"
        r = requests.get(url_tickers).json()
        tickers = r.get("data", [])
        pairs = [x["instId"] for x in tickers if "-USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
    except:
        return

    for symbol in pairs:
        try:
            df = get_candles(symbol, bar='1H')
            if df is None: continue
            
            close = df["close"]
            ma20 = close.rolling(20).mean().iloc[-1]
            curr_price = close.iloc[-1]
            
            delta = close.diff()
            gain = delta.where(delta > 0, 0).rolling(14).mean()
            loss = -delta.where(delta < 0, 0).rolling(14).mean()
            rsi = 100 - (100 / (1 + (gain.iloc[-1] / loss.iloc[-1])))
            
            std = close.rolling(20).std().iloc[-1]
            bandwidth = (4 * std) / ma20
            avg_vol = df["vol"].iloc[-20:-1].mean()
            vol_spike = df["vol"].iloc[-1] > avg_vol * 2.5
            
            if (bandwidth < 0.02 or vol_spike) and (45 <= rsi <= 62) and (curr_price > ma20):
                cp, sl, tp = get_levels(df)
                oi = get_oi_value(symbol)
                
                oi_desc = "سيولة قوية 🔥" if oi > 5000000 else "سيولة متوسطة ⚠️" if oi > 1000000 else "سيولة ضعيفة ❄️"
                rsi_desc = "منطقة تجمع ممتازة ✅" if rsi <= 55 else "بداية زخم صعودي 🚀"
                
                msg = (f"🚀 <b>فرصة جديدة (1H)</b>\n"
                       f"الزوج: <code>{symbol}</code> | {label}\n\n"
                       f"📍 دخول: {cp:.4f}\n"
                       f"🚫 ستوب: {sl:.4f}\n"
                       f"🎯 هدف: {tp:.4f}\n"
                       f"---------------------------\n"
                       f"📊 <b>التحذيرات الاسترشادية:</b>\n"
                       f"🟠 اتجاه البيتكوين: {btc_status}\n"
                       f"💰 سيولة العقود (OI): {oi:,.0f} USDT ({oi_desc})\n"
                       f"---------------------------\n"
                       f"RSI: {rsi:.1f} ({rsi_desc})\n"
                       f"النطاق: {bandwidth:.2%}")
                
                send_telegram(msg)
                time.sleep(0.5)
        except:
            continue

if __name__ == "__main__":
    while True:
        try:
            scan("SWAP")
            time.sleep(3600)
        except Exception as e:
            print(f"Main Loop Error: {e}")
            time.sleep(60)
