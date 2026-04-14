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
        payload["reply_markup"] = {"inline_keyboard": [[{"text": "🔗 فتح التداول (Manual)", "url": trading_url}]]}
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
    
    # حساب RSI يدوي دقيق
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean().iloc[-1]
    loss = -delta.where(delta < 0, 0).rolling(14).mean().iloc[-1]
    rsi = 100 - (100 / (1 + (gain / loss))) if loss != 0 else 100
    
    vol_spike = df["vol"].iloc[-1] > df["vol"].iloc[-20:-1].mean() * 2.5
    bandwidth = (4 * close.rolling(20).std().iloc[-1]) / ma20
    
    if (bandwidth < 0.02 or vol_spike) and (45 <= rsi <= 65) and (curr_price > ma20):
        funding, oi = get_market_data(symbol)
        score = 0
        if btc_trend == "UP": score += 3
        if oi > 10000000: score += 3
        elif oi > 1000000: score += 1
        if 50 <= rsi <= 55: score += 2
        elif 55 < rsi <= 60: score += 1
        if funding <= 0.01: score += 1
        
        oi_d = "تمركز مؤسسي ضخم 🔥" if oi > 10000000 else "تمركز متوسط ✅" if oi > 1000000 else "سيولة ضحلة ⚠️"
        rsi_d = "تجميع هادئ ✅" if rsi < 50 else "اختراق زخم 🚀" if rsi < 55 else "تسارع صاعد 🔥" if rsi < 60 else "إجهاد شرائي ⚠️"
        f_d = "سوق متوازن ✅" if funding <= 0.01 else "طمع شرايين ⚠️" if funding > 0.03 else "ضغوط بيعية 🚀"
        
        msg = (f"🔥 <b>تقييم الفرصة: {score}/10</b>\n"
               f"الزوج: <code>{symbol}</code>\n"
               f"📍 دخول: {curr_price:.4f}\n"
               f"🚫 ستوب: {df['low'].iloc[-2:].min() * 0.997:.4f}\n"
               f"---------------------------\n"
               f"💰 <b>السيولة:</b> {oi_d}\n"
               f"💳 <b>الفائدة:</b> {f_d}\n"
               f"📈 <b>الـ RSI:</b> {rsi_d}\n"
               f"🟠 <b>البيتكوين:</b> {'إيجابي ✅' if btc_trend == 'UP' else 'هابط ⚠️'}")
        return {"symbol": symbol, "score": score, "msg": msg}
    return None

def telegram_listener():
    last_id = 0
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", params={"offset": last_id + 1, "timeout": 10}).json()
            for update in r.get("result", []):
                last_id = update["update_id"]
                msg = update.get("message", {}).get("text", "").lower().replace(" ", "")
                if "/help" in msg or "شرح" in msg:
                    send_telegram("📖 (شرح البوت): التقييم 1-10، السيولة تعني الحيتان، RSI بنزين العملة، والبيتكوين بوصلتك.", is_summary=True)
                elif "/top10" in msg or "افضل" in msg:
                    # تنفيذ مسح سريع للـ Top 10
                    btc_trend = get_btc_status()
                    tickers = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP").json().get("data", [])
                    res = []
                    for s in [x["instId"] for x in tickers if "-USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE][:50]: # فحص أول 50 لتوفير الوقت
                        analysis = analyze_logic(s, btc_trend)
                        if analysis: res.append(analysis)
                    top = sorted(res, key=lambda x: x['score'], reverse=True)[:10]
                    report = "🏆 <b>أفضل الفرص الحالية:</b>\n\n" + "\n".join([f"{i+1}. {x['symbol']} ({x['score']}/10)" for i, x in enumerate(top)])
                    send_telegram(report if res else "لم يتم العثور على فرص حالياً.", is_summary=True)
        except: pass
        time.sleep(3)

if __name__ == "__main__":
    threading.Thread(target=telegram_listener, daemon=True).start()
    while True:
        try:
            btc_trend = get_btc_status()
            tickers = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP").json().get("data", [])
            pairs = [x["instId"] for x in tickers if "-USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
            for s in pairs:
                res = analyze_logic(s, btc_trend)
                if res and res['score'] >= 7:
                    send_telegram(res['msg'], symbol=s)
            time.sleep(3600)
        except: time.sleep(60)
