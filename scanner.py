import requests
import time
import pandas as pd
import numpy as np

# --- 1. الإعدادات الأساسية ---
TELEGRAM_TOKEN = "PUT_YOUR_TOKEN_HERE"
CHAT_ID = "PUT_YOUR_CHAT_ID"

EXCLUDE = ["USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","BRL1","WIN","SHIB","USDG","SPURS","NFT","USDP","USDD","FRAX","LUSD","GUSD","HUSD","SUSD","CUSD","ZUSD","USDX","USDN","USDK","USDQ","USDB","SPY","TSLA","AAPL","AMZN","GOOGL","MSFT","NVDA","META","NFLX","XAU","XAUT","XAG","PAXG","OIL","CRUDE"]

# --- 2. وظائف تيليجرام ---
def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
    except:
        pass

def send_help():
    msg = """📘 <b>شرح البوت المطوّر (نسخة الاستجابة السريعة)</b>

🤖 البوت بيفحص العملات كل 4 ساعات وبيرد على أوامرك فوراً.

📊 <b>المؤشرات:</b>
- <b>RSI (Wilder):</b> مطور ليعطي نتائج TradingView بدقة.
- <b>BB Squeeze:</b> بيكتشف ضغط السعر قبل الانفجار.
- <b>Volume Spike:</b> بيكتشف دخول الحيتان والسيولة.

⚡ <b>الأوامر المتاحة:</b>
- أرسل <b>help</b> لمشاهدة هذه الرسالة.
- البوت بيبعت "أفضل 10 فرص" تلقائياً بعد كل مسح.
"""
    send_telegram(msg)

def check_telegram_commands(last_id):
    """دالة فحص الأوامر - تم تحسينها لعدم تفويت أي رسالة"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset={last_id+1}&timeout=1"
        r = requests.get(url, timeout=5).json()
        if "result" in r:
            for update in r["result"]:
                last_id = update["update_id"]
                if "message" in update and "text" in update["message"]:
                    text = update["message"]["text"].lower()
                    if "help" in text:
                        send_help()
        return last_id
    except:
        return last_id

# --- 3. التحليل الفني العميق ---
def calculate_rsi(df, period=14):
    """حساب RSI بطريقة Wilder's Smoothing المطابقة لـ TradingView"""
    try:
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).copy()
        loss = (-delta.where(delta < 0, 0)).copy()
        
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # Wilder's Smoothing logic
        for i in range(period, len(avg_gain)):
            avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
            avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period
            
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]
    except:
        return 50

def get_candles(symbol):
    try:
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=4H&limit=100"
        r = requests.get(url, timeout=10).json()
        if "data" not in r or len(r["data"]) < 30: return None
        df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df = df.astype({"close": float, "vol": float, "high": float, "low": float, "open": float})
        return df.iloc[::-1].reset_index(drop=True)
    except:
        return None

def calc_entry_stop_tp(df, side="LONG"):
    entry = df["close"].iloc[-1]
    # حساب ATR بسيط للستوب لوز
    tr = pd.concat([df["high"] - df["low"], abs(df["high"] - df["close"].shift()), abs(df["low"] - df["close"].shift())], axis=1).max(axis=1)
    atr = tr.rolling(14).mean().iloc[-1]
    
    if side == "LONG":
        stop = df["low"].iloc[-10:].min() - (atr * 0.5)
        tp = entry + (entry - stop) * 1.5
    else:
        stop = df["high"].iloc[-10:].max() + (atr * 0.5)
        tp = entry - (stop - entry) * 1.5
    return round(entry, 6), round(stop, 6), round(tp, 6)

# --- 4. محرك فحص السوق ---
def scan_market(inst_type, last_id, top_signals):
    label = "سبوت 🟢" if inst_type == "SPOT" else "فيوتشر 🔴"
    url = f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}"
    
    try:
        r = requests.get(url, timeout=10).json()
        pairs = [x for x in r['data'] if "USDT" in x['instId'] and x['instId'].split("-")[0] not in EXCLUDE]
    except:
        return last_id

    for item in pairs:
        symbol = item['instId']
        price = float(item['last'])
        
        # التعديل الأهم: فحص الأوامر "أثناء" المسح لضمان الرد الفوري
        last_id = check_telegram_commands(last_id)
        
        df = get_candles(symbol)
        if df is None: continue

        # حساب المؤشرات
        rsi = calculate_rsi(df)
        ma20 = df["close"].rolling(20).mean().iloc[-1]
        std = df["close"].rolling(20).std().iloc[-1]
        bandwidth = (4 * std) / ma20
        vol_avg = df["vol"].iloc[-20:-1].mean()
        
        is_vol_spike = df["vol"].iloc[-1] > vol_avg * 2.5
        is_bb_squeeze = bandwidth < 0.03
        
        # منطق الـ LONG
        if (is_vol_spike or is_bb_squeeze) and (45 <= rsi <= 60) and price > ma20:
            e, s, t = calc_entry_stop_tp(df, "LONG")
            score = (3 if is_vol_spike else 0) + (3 if is_bb_squeeze else 0) + 4
            msg = f"🚀 <b>LONG | {symbol}</b>\nنوع: {label}\n\n💰 السعر: {price}\n📊 RSI: {rsi:.1f}\n🎯 دخول: {e}\n🛑 ستوب: {s}\n🏁 هدف: {t}\n🔥 التقييم: {score}/10"
            send_telegram(msg)
            top_signals.append((symbol, score, label, "LONG"))

        # منطق الـ SHORT
        elif (is_vol_spike or is_bb_squeeze) and (40 <= rsi <= 55) and price < ma20:
            e, s, t = calc_entry_stop_tp(df, "SHORT")
            score = (3 if is_vol_spike else 0) + (3 if is_bb_squeeze else 0) + 4
            msg = f"🔻 <b>SHORT | {symbol}</b>\nنوع: {label}\n\n💰 السعر: {price}\n📊 RSI: {rsi:.1f}\n🎯 دخول: {e}\n🛑 ستوب: {s}\n🏁 هدف: {t}\n🔥 التقييم: {score}/10"
            send_telegram(msg)
            top_signals.append((symbol, score, label, "SHORT"))

        time.sleep(0.05) # سرعة فحص رشيقة
    return last_id

# --- 5. حلقة التشغيل الرئيسية ---
last_id = 0
# تنظيف الرسايل القديمة عند البداية
try:
    requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset=-1")
except:
    pass

send_telegram("✅ <b>تم تشغيل البوت بنجاح!</b>\nالبوت الآن يفحص السوق، يمكنك إرسال <b>help</b> في أي وقت.")
print("Bot Started...")

while True:
    last_id = check_telegram_commands(last_id)
    
    top_signals = []
    # فحص Spot ثم Futures
    last_id = scan_market("SPOT", last_id, top_signals)
    last_id = scan_market("SWAP", last_id, top_signals)
    
    # إرسال ملخص أفضل الفرص
    if top_signals:
        summary = "🏆 <b>ملخص أفضل فرص المسح الحالي:</b>\n\n"
        for i, sig in enumerate(sorted(top_signals, key=lambda x: x[1], reverse=True)[:10], 1):
            summary += f"{i}. {sig[0]} ({sig[2]}) - تقييم {sig[1]}/10\n"
        send_telegram(summary)

    print("Scan finished. Waiting 4 hours...")
    # النوم الذكي: فحص الأوامر كل ثانية خلال الـ 4 ساعات
    for _ in range(14400):
        last_id = check_telegram_commands(last_id)
        time.sleep(1)
