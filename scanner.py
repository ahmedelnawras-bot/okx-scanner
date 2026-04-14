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
        requests.post(url, json=payload, timeout=10)
    except: pass

def get_candles(symbol, bar='1H', limit=100):
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
    try:
        r = requests.get(url, timeout=10).json()
        if "data" not in r: return None
        df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df = df.astype({"close": float, "vol": float, "high": float, "low": float})
        return df.iloc[::-1].reset_index(drop=True)
    except: return None

def get_market_data(symbol):
    try:
        f_url = f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol}"
        funding = float(requests.get(f_url, timeout=10).json()["data"][0]["fundingRate"])
        oi_url = f"https://www.okx.com/api/v5/public/open-interest?instId={symbol}"
        oi = float(requests.get(oi_url, timeout=10).json()["data"][0]["oiCcy"])
        return funding, oi
    except: return 0, 0

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
    
    vol_spike = df["vol"].iloc[-1] > df["vol"].iloc[-20:-1].mean() * 1.5 
    
    if (vol_spike or curr_price > ma20) and (40 <= rsi <= 70):
        funding, oi = get_market_data(symbol)
        score = 0
        if btc_trend == "UP": score += 3
        if oi > 1000000: score += 3 
        if 45 <= rsi <= 60: score += 2
        if funding <= 0.01: score += 2
        
        msg = (f"📡 <b>Nour's Market Scanner (Score: {score}/10)</b>\n"
               f"الزوج: <code>{symbol}</code>\n"
               f"السعر الحالي: {curr_price:.4f}\n"
               f"---------------------------\n"
               f"💰 <b>السيولة:</b> {'عالية 🔥' if oi > 5000000 else 'متوسطة ✅'}\n"
               f"📈 <b>الـ RSI:</b> {'إيجابي ✅' if rsi < 60 else 'تشبع شرائي ⚠️'}\n"
               f"🟠 <b>البيتكوين:</b> {'صاعد ✅' if btc_trend == 'UP' else 'هابط ⚠️'}")
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
                    msg = update["message"]["text"].lower()
                    if "/help" in msg or "شرح" in msg:
                        send_telegram("📖 <b>Nour's Scanner Help:</b>\n- تنبيهات لتقييم 5 وأعلى.\n- فحص البيتكوين آلياً.\n- اطلب /top10 للمسح اليدوي.", is_summary=True)
                    elif "/top10" in msg or "افضل" in msg:
                        threading.Thread(target=get_top_10_report).start()
        except: time.sleep(5)

def get_top_10_report():
    send_telegram("⏳ ثواني يا نور.. بحدث لك القائمة بأفضل الفرص.", is_summary=True)
    btc_trend = get_btc_status()
    try:
        tickers = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP").json().get("data", [])
        all_p = [x["instId"] for x in tickers if "-USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
        res = []
        for s in all_p[:60]: # فحص سريع لأول 60 عملة
            analysis = analyze_logic(s, btc_trend)
            if analysis: res.append(analysis)
        top = sorted(res, key=lambda x: x['score'], reverse=True)[:10]
        report = "🏆 <b>أفضل الفرص الحالية (5+):</b>\n\n" + "\n".join([f"{x['symbol']} (Score: {x['score']}/10)" for x in top])
        send_telegram(report if top else "لا توجد فرص محققة حالياً.", is_summary=True)
    except: pass

if __name__ == "__main__":
    threading.Thread(target=telegram_listener, daemon=True).start()
    while True:
        try:
            btc_trend = get_btc_status()
            tickers = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP").json().get("data", [])
            pairs = [x["instId"] for x in tickers if "-USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
            for s in pairs:
                res = analyze_logic(s, btc_trend)
                if res and res['score'] >= 5: # التنبيه بيبدأ من 5
                    send_telegram(res['msg'], symbol=s)
            time.sleep(900) # فحص كل 15 دقيقة
        except: time.sleep(60)
