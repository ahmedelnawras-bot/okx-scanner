import requests
import time
import pandas as pd
import numpy as np

# --- الإعدادات (تأكد من وضع بياناتك هنا) ---
TELEGRAM_TOKEN = "PUT_YOUR_TOKEN_HERE"
CHAT_ID = "PUT_YOUR_CHAT_ID"

EXCLUDE = ["USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","BRL1","WIN","SHIB","USDG","SPURS","NFT","USDP","USDD","FRAX","LUSD","GUSD","HUSD","SUSD","CUSD","ZUSD","USDX","USDN","USDK","USDQ","USDB","SPY","TSLA","AAPL","AMZN","GOOGL","MSFT","NVDA","META","NFLX","XAU","XAUT","XAG","PAXG","OIL","CRUDE"]

# --- وظائف تيليجرام ---
def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"Telegram Error: {e}")

def send_help():
    msg = """📘 <b>شرح البوت المطوّر</b>

🤖 البوت بيحلل العملات ويديك فرص تداول جاهزة

🟢 <b>LONG</b> = شراء
🔴 <b>SHORT</b> = بيع

📊 <b>المؤشرات:</b>
- Volume Spike = انفجار سيولة
- BB Squeeze = ضغط سعري
- RSI (Wilder) = قوة الاتجاه (45-60)
- MA20 = المسار السعري

🔥 <b>التقييم (Score):</b>
8-10 = قوية جداً
6-7 = جيدة

⚠️ البوت بيرد على أوامرك فوراً حتى أثناء وقت الانتظار.
"""
    send_telegram(msg)

def check_telegram_commands(last_id):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset={last_id+1}&timeout=1"
        r = requests.get(url, timeout=5).json()
        for update in r.get("result", []):
            last_id = update["update_id"]
            message = update.get("message", {})
            text = message.get("text", "").lower()
            
            if text in ["/help", "help", "شرح"]:
                send_help()
        return last_id
    except:
        return last_id

# --- التحليل الفني (تعديل RSI) ---
def calculate_rsi(df, period=14):
    """حساب RSI بطريقة Wilder المطابقة لـ TradingView"""
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).copy()
    loss = (-delta.where(delta < 0, 0)).copy()
    
    # القيمة الأولى هي المتوسط البسيط
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    # تنعيم Wilder (Wilder's Smoothing)
    # البدء من أول قيمة غير فارغة
    for i in range(period, len(avg_gain)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period
        
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

def get_btc_trend():
    try:
        url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=4H&limit=10"
        r = requests.get(url).json()
        closes = [float(x[4]) for x in r["data"]]
        closes.reverse()
        return "إيجابي ✅" if closes[-1] > closes[-3] else "سلبي ⚠️"
    except: return "غير معروف"

def get_pairs(inst_type):
    try:
        url = f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}"
        r = requests.get(url).json()
        if inst_type == "SPOT":
            return [(x["instId"], float(x["last"])) for x in r["data"] if x["instId"].endswith("-USDT") and x["instId"].split("-")[0] not in EXCLUDE]
        return [(x["instId"], float(x["last"])) for x in r["data"] if "USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
    except: return []

def get_candles(symbol):
    try:
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=4H&limit=100"
        r = requests.get(url).json()
        if "data" not in r or len(r["data"]) < 30: return None
        df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df = df.astype({"close": float, "vol": float, "high": float, "low": float, "open": float})
        return df.iloc[::-1].reset_index(drop=True)
    except: return None

# --- الدوال المساعدة ---
def check_volume_spike(df):
    return df["vol"].iloc[-1] > df["vol"].iloc[-20:-1].mean() * 3

def check_bb_squeeze(df):
    ma = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    return ((ma + 2*std - (ma - 2*std)) / ma).iloc[-1] < 0.03

def calc_entry_stop_tp(df, side="LONG"):
    entry = df["close"].iloc[-1]
    tr = pd.concat([df["high"] - df["low"], abs(df["high"] - df["close"].shift()), abs(df["low"] - df["close"].shift())], axis=1).max(axis=1)
    atr = tr.rolling(14).mean().iloc[-1]
    if side == "LONG":
        stop = df["low"].iloc[-10:].min() - (atr * 0.5)
        tp = entry + (entry - stop) * 2
    else:
        stop = df["high"].iloc[-10:].max() + (atr * 0.5)
        tp = entry - (stop - entry) * 2
    return round(entry, 6), round(stop, 6), round(tp, 6)

def scan(inst_type, top_signals, last_id):
    label = "سبوت 🟢" if inst_type == "SPOT" else "فيوتشر 🔴"
    pairs = get_pairs(inst_type)
    btc_trend = get_btc_trend()
    
    for symbol, price in pairs:
        last_id = check_telegram_commands(last_id) # فحص الأوامر دورياً
        try:
            df = get_candles(symbol)
            if df is None: continue

            vol, bb, rsi = check_volume_spike(df), check_bb_squeeze(df), calculate_rsi(df)
            ma20 = df["close"].rolling(20).mean().iloc[-1]
            
            # منطق LONG
            if (vol or bb) and (45 <= rsi <= 60) and price > ma20:
                e, s, t = calc_entry_stop_tp(df, "LONG")
                score = (2 if vol else 0) + (2 if bb else 0) + 4 + (2 if "إيجابي" in btc_trend else 0)
                msg = f"🚀 <b>LONG</b>\n{symbol} | {label}\n\n💰 السعر: {price}\n📊 RSI: {rsi:.1f}\n🎯 دخول: {e}\n🛑 ستوب: {s}\n🏁 هدف: {t}\n🔥 Score: {score}/10"
                send_telegram(msg)
                top_signals.append((symbol, score, label, "LONG", e, s))

            # منطق SHORT
            elif (vol or bb) and (40 <= rsi <= 55) and price < ma20 and price < df["open"].iloc[-1]:
                e, s, t = calc_entry_stop_tp(df, "SHORT")
                score = (2 if vol else 0) + (2 if bb else 0) + 4 + (2 if "إيجابي" in btc_trend else 0)
                msg = f"🔻 <b>SHORT</b>\n{symbol} | {label}\n\n💰 السعر: {price}\n📊 RSI: {rsi:.1f}\n🎯 دخول: {e}\n🛑 ستوب: {s}\n🏁 هدف: {t}\n🔥 Score: {score}/10"
                send_telegram(msg)
                top_signals.append((symbol, score, label, "SHORT", e, s))
            time.sleep(0.1)
        except: continue
    return last_id

# --- الحلقة الرئيسية ---
last_id = 0
while True:
    last_id = check_telegram_commands(last_id)
    top_signals = []
    
    last_id = scan("SPOT", top_signals, last_id)
    last_id = scan("SWAP", top_signals, last_id)
    
    if top_signals:
        top_msg = "🏆 <b>أفضل الفرص الحالية:</b>\n\n"
        for i, s in enumerate(sorted(top_signals, key=lambda x: x[1], reverse=True)[:10], 1):
            top_msg += f"{i}. {s[0]} ({s[2]}) {s[3]} - {s[1]}/10\n"
        send_telegram(top_msg)

    print("Done. Waiting 4 hours...")
    for _ in range(240): # فحص الأوامر كل دقيقة خلال فترة الانتظار
        last_id = check_telegram_commands(last_id)
        time.sleep(60)
