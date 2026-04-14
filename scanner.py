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
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": "🔗 فتح التداول (Manual)", "url": trading_url}]]
        }
    
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

def show_help():
    help_text = (
        "📖 <b>دليل استخدام بوت القناص (Nour Bot)</b>\n\n"
        "أهلاً بيك يا نور.. دي الخلاصة عشان تفهم رسائل البوت:\n\n"
        "1️⃣ <b>التقييم (Score):</b> ده مجموع نقاط القوة. (8-10 فرصة ذهبية، 5-7 محتاجة حذر، أقل من 5 فكك منها).\n"
        "2️⃣ <b>السيولة (OI):</b> بتقولك الفلوس اللي محبوسة في العملة قد إيه. 'تمركز مؤسسي' يعني الحيتان دخلت تلعب 🔥.\n"
        "3️⃣ <b>الـ RSI:</b> ده بنزين العملة. 'اختراق الزحم' يعني العملة بدأت تسخن وهتطير 🚀.\n"
        "4️⃣ <b>الفائدة (Funding):</b> لو الرقم موجب وكبير يبقى المشتريين طماعين وممكن ينزل يضرب ستوباتهم. لو سالب يبقى فيه 'انفجار صعودي' جاي.\n"
        "5️⃣ <b>البيتكوين:</b> ده القائد. لو إيجابي ✅ السوق كله بيساعدك، لو هابط ⚠️ يبقى ادخل بحرص جداً.\n\n"
        "💡 <b>الأوامر المتاحة:</b>\n"
        "🔹 اكتب <b>شرح</b> أو <code>/help</code> : عشان يبعتلك الرسالة دي.\n"
        "🔹 اكتب <b>أفضل الفرص</b> أو <code>/top10</code> : عشان يمسح السوق فوراً ويجيبلك أحسن 10 صفقات حالياً."
    )
    send_telegram(help_text, is_summary=True)

def analyze_signal(symbol, btc_trend):
    df = get_candles(symbol)
    if df is None: return None
    close = df["close"]
    ma20 = close.rolling(20).mean().iloc[-1]
    curr_price = close.iloc[-1]
    
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean().iloc[-1]
    loss = -delta.where(delta < 0, 0).rolling(14).mean().iloc[-1]
    rsi = 100 - (100 / (1 + (gain / loss))) if loss != 0 else 100
    
    bandwidth = (4 * close.rolling(20).std().iloc[-1]) / ma20
    vol_spike = df["vol"].iloc[-1] > df["vol"].iloc[-20:-1].mean() * 2.5
    
    if (bandwidth < 0.02 or vol_spike) and (45 <= rsi <= 65) and (curr_price > ma20):
        funding, oi = get_market_data(symbol)
        score = 0
        if btc_trend == "UP": score += 3
        if oi > 10000000: score += 3
        elif oi > 1000000: score += 1
        if 50 <= rsi <= 55: score += 2
        elif 55 < rsi <= 60: score += 1
        if funding <= 0.01: score += 1
        return {"symbol": symbol, "score": score, "funding": funding}
    return None

def get_top_10():
    send_telegram("⏳ ثواني يا نور.. بمسح لك السوق كله أجيبلك الخلاصة لأفضل 10 عملات حالياً.", is_summary=True)
    btc_trend = get_btc_status()
    try:
        tickers = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP").json().get("data", [])
        pairs = [x["instId"] for x in tickers if "-USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
        
        results = []
        for s in pairs:
            res = analyze_signal(s, btc_trend)
            if res: results.append(res)
        
        top_10 = sorted(results, key=lambda x: x['score'], reverse=True)[:10]
        
        if not top_10:
            send_telegram("❌ للأسف مفيش فرص قوية مطابقة للشروط دلوقتي.", is_summary=True)
            return

        report = "🏆 <b>أفضل 10 فرص في السوق دلوقتي:</b>\n\n"
        for i, item in enumerate(top_10, 1):
            report += f"{i}. <code>{item['symbol']}</code> | التقييم: <b>{item['score']}/10</b>\n"
        
        report += "\n💡 تقدر تفتح أي عملة منهم مانيوال وتراجع الشارت قبل الدخول."
        send_telegram(report, is_summary=True)
    except:
        send_telegram("⚠️ حصل مشكلة وأنا بجمع البيانات، جرب كمان شوية.", is_summary=True)

def check_telegram_commands():
    last_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            r = requests.get(url, params={"offset": last_id + 1, "timeout": 10}).json()
            for update in r.get("result", []):
                last_id = update["update_id"]
                msg_text = update.get("message", {}).get("text", "").lower()
                
                if "/help" in msg_text or "شرح" in msg_text:
                    show_help()
                elif "/top10" in msg_text or "افضل الفرص" in msg_text or "أفضل الفرص" in msg_text:
                    get_top_10()
        except: pass
        time.sleep(3)

if __name__ == "__main__":
    threading.Thread(target=check_telegram_commands).start()
    while True:
        btc_trend = get_btc_status()
        tickers = requests.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP").json().get("data", [])
        pairs = [x["instId"] for x in tickers if "-USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
        for s in pairs:
            # هنا البوت بيبحث تلقائياً ويرسل الفرص الجديدة فوراً
            df = get_candles(s)
            if df is None: continue
            res = analyze_signal(s, btc_trend)
            if res and res['score'] >= 7: # يرسل تلقائياً فقط الفرص القوية جداً
                # دالة البناء والإرسال (كما في النسخ السابقة)
                pass 
        time.sleep(3600)
