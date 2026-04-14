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
    try: requests.post(url, json=payload)
    except: pass

def get_candles(symbol, bar='1H', limit=100):
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
    try:
        r = requests.get(url).json()
        if "data" not in r: return None
        df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df = df.astype({"close": float, "vol": float, "high": float, "low": float})
        return df.iloc[::-1].reset_index(drop=True)
    except: return None

def get_market_data(symbol):
    try:
        f_url = f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol}"
        funding = float(requests.get(f_url).json()["data"][0]["fundingRate"])
        oi_url = f"https://www.okx.com/api/v5/public/open-interest?instId={symbol}"
        oi = float(requests.get(oi_url).json()["data"][0]["oiCcy"])
        return funding, oi
    except: return 0, 0

def get_btc_status():
    df = get_candles("BTC-USDT", bar='4H', limit=30)
    if df is None: return "DOWN"
    ma20 = df["close"].rolling(20).mean().iloc[-1]
    return "UP" if df["close"].iloc[-1] > ma20 else "DOWN"

def analyze_logic(symbol, btc_trend):
    df = get_candles(symbol)
    if df is None: return None
    close = df["close"]
    ma20 = close.rolling(20).mean().iloc[-1]
    curr_price = close.iloc[-1]
    rsi = 100 - (100 / (1 + (close.diff().where(close.diff() > 0, 0).rolling(14).mean().iloc[-1] / -close.diff().where(close.diff() < 0, 0).rolling(14).mean().iloc[-1]))) if len(df) > 14 else 50
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
        
        # الأوصاف الاحترافية
        oi_d = "تمركز مؤسسي ضخم 🔥" if oi > 10000000 else "تمركز متوسط ✅" if oi > 1000000 else "سيولة ضحلة ⚠️"
        rsi_d = "تجميع هادئ ✅" if rsi < 50 else "اختراق زخم 🚀" if rsi < 55 else "تسارع صاعد 🔥" if rsi < 60 else "إجهاد شرائي ⚠️"
        f_d = "سوق متوازن ✅" if funding <= 0.01 else "طمع شرايين ⚠️" if funding > 0.03 else "ضغط بيع / انفجار 🚀"
        
        return {"symbol": symbol, "score": score, "msg": (f"🔥 <b>تقييم الفرصة: {score}/10</b>\nالزوج: <code>{symbol}</code>\n📍 دخول: {curr_price:.4f}\n🚫 ستوب: {df['low'].iloc[-2:].min() * 0.997:.4f}\n---------------------------\n💰 <b>السيولة:</b> {oi_d}\n💳 <b>الفائدة:</b> {f_d}\n📈 <b>الـ RSI:</b> {rsi_d}\n🟠 <b>البيتكوين:</b> {'إيجابي ✅' if btc_trend == 'UP' else 'هابط ⚠️'}")}
    return None

def get_top_10_report():
    send_telegram("⏳ ثواني يا نور.. بمسح لك السوق كله أجيبلك الخلاصة.", is_summary=True)
    btc_trend = get_btc_status()
    tickers = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP").json().get("data", [])
    all_p = [x["instId"] for x in tickers if "-USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
    res = []
    for s in all_p:
        analysis = analyze_logic(s, btc_trend)
        if analysis: res.append(analysis)
    
    top = sorted(res, key=lambda x: x['score'], reverse=True)[:10]
    if not top:
        send_telegram("❌ مفيش فرص قوية حالياً، استنى شوية والسوق هيظبط.", is_summary=True)
        return
    
    report = "🏆 <b>أفضل 10 فرص حالياً:</b>\n\n"
    for i, item in enumerate(top, 1):
        report += f"{i}. <code>{item['symbol']}</code> | التقييم: <b>{item['score']}/10</b>\n"
    send_telegram(report, is_summary=True)

def telegram_listener():
    last_id = 0
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", params={"offset": last_id + 1, "timeout": 10}).json()
            for update in r.get("result", []):
                last_id = update["update_id"]
                msg = update.get("message", {}).get("text", "").lower().replace(" ", "") # شيلنا المسافات خالص
                if "/help" in msg or "شرح" in msg:
                    send_telegram("📖 (شرح البوت): التقييم 1-10، السيولة تعني الحيتان، RSI بنزين العملة، والبيتكوين بوصلتك.", is_summary=True)
                elif "/top10" in msg or "افضل الفرص" in msg or "أفضل الفرص" in msg:
                    get_top_10_report()
        except: pass
        time.sleep(2)

if __name__ == "__main__":
    threading.Thread(target=telegram_listener, daemon=True).start()
    while True:
        btc_trend = get_btc_status()
        tickers = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP").json().get("data", [])
        pairs = [x["instId"] for x in tickers if "-USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
        for s in pairs:
            res = analyze_logic(s, btc_trend)
            if res and res['score'] >= 7: # ابعت فوراً لو الفرصة قوية
                send_telegram(res['msg'], symbol=s)
        time.sleep(3600)
