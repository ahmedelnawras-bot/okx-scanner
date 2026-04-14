import requests
import time
import pandas as pd
import threading

# --- الإعدادات الأساسية ---
TELEGRAM_TOKEN = "8626651293:AAGTVnwdW36qLsoZdmC2ngKoYUGMeYZyjsg"
CHAT_ID = "5523662724"
EXCLUDE = ["USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","WIN","SHIB"]

def send_telegram(message, symbol=None, is_summary=False):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    if symbol and not is_summary:
        trading_url = f"https://www.okx.com/trading-market/spot/{symbol.lower()}"
        payload["reply_markup"] = {"inline_keyboard": [[{"text": "🔗 فتح التداول (OKX)", "url": trading_url}]]}
    try:
        requests.post(url, json=payload)
    except:
        pass

def get_candles(symbol, bar='1H', limit=100):
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
    try:
        r = requests.get(url).json()
        if "data" not in r or not r["data"]: return None
        df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df = df.astype({"close": float, "vol": float, "high": float, "low": float})
        return df.iloc[::-1].reset_index(drop=True)
    except:
        return None

def get_market_data(symbol):
    try:
        f_url = f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol}"
        funding = float(requests.get(f_url).json()["data"][0]["fundingRate"])
        oi_url = f"https://www.okx.com/api/v5/public/open-interest?instId={symbol}"
        oi = float(requests.get(oi_url).json()["data"][0]["oiCcy"])
        return funding, oi
    except:
        return 0, 0

def get_btc_status():
    df = get_candles("BTC-USDT", bar='4H', limit=30)
    if df is None: return "DOWN"
    ma20 = df["close"].rolling(20).mean().iloc[-1]
    return "UP" if df["close"].iloc[-1] > ma20 else "DOWN"

def analyze_logic(symbol, btc_trend):
    df = get_candles(symbol)
    if df is None or len(df) < 20: return None
    close = df["close"]
    ma20 = close.rolling(20).mean().iloc[-1]
    curr_price = close.iloc[-1]
    
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean().iloc[-1]
    loss = -delta.where(delta < 0, 0).rolling(14).mean().iloc[-1]
    rsi = 100 - (100 / (1 + (gain / loss))) if loss != 0 else 100
    
    vol_spike = df["vol"].iloc[-1] > df["vol"].iloc[-20:-1].mean() * 2.0 
    bandwidth = (4 * close.rolling(20).std().iloc[-1]) / ma20
    
    if (bandwidth < 0.04 or vol_spike) and (40 <= rsi <= 70) and (curr_price > ma20 * 0.995):
        funding, oi = get_market_data(symbol)
        score = 0
        if btc_trend == "UP": score += 3
        if oi > 5000000: score += 3 
        elif oi > 500000: score += 1
        if 45 <= rsi <= 55: score += 2
        elif 55 < rsi <= 65: score += 1
        if funding <= 0.01: score += 1
        
        oi_d = "تمركز مؤسسي ضخم 🔥" if oi > 10000000 else "تمركز متوسط ✅" if oi > 1000000 else "سيولة عادية"
        rsi_d = "تجميع/انطلاق ✅" if rsi < 55 else "تسارع صاعد 🔥" if rsi < 65 else "تشبع شرائي ⚠️"
        f_d = "سوق متوازن ✅" if funding <= 0.01 else "طمع شرايين ⚠️"
        
        msg = (f"📡 <b>Nour's Market Scanner (Score: {score}/10)</b>\n"
               f"الزوج: <code>{symbol}</code>\n"
               f"السعر: {curr_price:.4f}\n"
               f"---------------------------\n"
               f"💰 <b>السيولة:</b> {oi_d}\n"
               f"📈 <b>حالة الـ RSI:</b> {rsi_d}\n"
               f"🟠 <b>اتجاه البيتكوين:</b> {'صاعد ✅' if btc_trend == 'UP' else 'هابط ⚠️'}")
        return {"symbol": symbol, "score": score, "msg": msg}
    return None

def telegram_listener():
    last_id = 0
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", params={"limit": 1}).json()
        if r.get("result"): last_id = r["result"][-1]["update_id"]
    except: pass

    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            r = requests.get(url, params={"offset": last_id + 1, "timeout": 20}).json()
            for update in r.get("result", []):
                last_id = update["update_id"]
                if "message" in update and "text" in update["message"]:
                    msg = update["message"]["text"].lower().replace(" ", "")
                    if "/help" in msg or
