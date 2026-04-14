import requests
import time
import pandas as pd
import numpy as np

TELEGRAM_TOKEN = "PUT_YOUR_TOKEN_HERE"
CHAT_ID = "PUT_YOUR_CHAT_ID"

EXCLUDE = ["USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","BRL1","WIN","SHIB","USDG","SPURS","NFT","USDP","USDD","FRAX","LUSD","GUSD","HUSD","SUSD","CUSD","ZUSD","USDX","USDN","USDK","USDQ","USDB","SPY","TSLA","AAPL","AMZN","GOOGL","MSFT","NVDA","META","NFLX","XAU","XAUT","XAG","PAXG","OIL","CRUDE"]

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"})

# 📘 رسالة شرح البوت
def send_help():
    msg = """📘 <b>شرح البوت</b>

🤖 البوت بيحلل العملات ويديك فرص تداول جاهزة

🟢 LONG = شراء (توقع صعود)
🔴 SHORT = بيع (توقع هبوط)

📊 المؤشرات المستخدمة:
- Volume Spike = دخول سيولة
- BB Squeeze = ضغط سعري
- RSI = قوة الحركة
- MA20 = الاتجاه

🎯 دخول = سعر الدخول
🛑 ستوب = وقف الخسارة
🏁 هدف = جني الأرباح

🔥 Score من 10:
8-10 = قوية جداً
6-7 = جيدة
أقل = ضعيفة

₿ حالة السوق:
إيجابي = السوق مساعد
سلبي = خليك حذر

🏆 Top 10 = أفضل الفرص الحالية

⚠️ نصيحة:
لا تدخل كل الصفقات - ركز على الأقوى فقط
"""
    send_telegram(msg)

def get_btc_trend():
    url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=4H&limit=10"
    r = requests.get(url).json()
    if "data" not in r:
        return "غير معروف"
    closes = [float(x[4]) for x in r["data"]]
    closes.reverse()
    return "إيجابي ✅" if closes[-1] > closes[-3] else "سلبي ⚠️"

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

def check_volume_spike(df):
    avg = df["vol"].iloc[-20:-1].mean()
    return df["vol"].iloc[-1] > avg * 3

def check_bb_squeeze(df):
    ma = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    bandwidth = (ma + 2*std - (ma - 2*std)) / ma
    return bandwidth.iloc[-1] < 0.03

def check_rsi(df):
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = -delta.where(delta < 0, 0).rolling(14).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs)).iloc[-1]

def check_above_ma20(df):
    return df["close"].iloc[-1] > df["close"].rolling(20).mean().iloc[-1]

def check_below_ma20(df):
    return df["close"].iloc[-1] < df["close"].rolling(20).mean().iloc[-1]

def is_bearish(df):
    return df["close"].iloc[-1] < df["open"].iloc[-1]

def calculate_atr(df):
    tr = pd.concat([
        df["high"] - df["low"],
        abs(df["high"] - df["close"].shift()),
        abs(df["low"] - df["close"].shift())
    ], axis=1).max(axis=1)
    return tr.rolling(14).mean().iloc[-1]

def calc_entry_stop(df):
    entry = df["close"].iloc[-1]
    support = df["low"].iloc[-10:].min()
    atr = calculate_atr(df)
    stop = support - (atr * 0.5)
    return round(entry,6), round(stop,6)

def calc_entry_stop_sell(df):
    entry = df["close"].iloc[-1]
    resistance = df["high"].iloc[-10:].max()
    atr = calculate_atr(df)
    stop = resistance + (atr * 0.5)
    return round(entry,6), round(stop,6)

def calc_tp(entry, stop):
    return round(entry + (entry - stop)*2,6)

def calc_tp_sell(entry, stop):
    return round(entry - (stop - entry)*2,6)

def calc_score(vol, bb, rsi_ok, trend, btc):
    score = 0
    if vol: score += 2
    if bb: score += 2
    if rsi_ok: score += 2
    if trend: score += 2
    if btc: score += 2
    return score

def check_telegram_commands(last_update_id):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset={last_update_id+1}"
    r = requests.get(url).json()

    for update in r.get("result", []):
        last_update_id = update["update_id"]
        msg = update.get("message", {}).get("text", "")

        if msg.lower() == "help":
            send_help()

    return last_update_id

def scan(inst_type, top_signals):
    label = "سبوت 🟢" if inst_type == "SPOT" else "فيوتشر 🔴"
    pairs = get_pairs(inst_type)
    btc_trend = get_btc_trend()
    btc_positive = "إيجابي" in btc_trend

    for symbol, price in pairs:
        try:
            df = get_candles(symbol)
            if df is None:
                continue

            vol = check_volume_spike(df)
            bb = check_bb_squeeze(df)
            rsi = check_rsi(df)
            above = check_above_ma20(df)
            below = check_below_ma20(df)
            bearish = is_bearish(df)

            # LONG
            if (vol or bb) and (45 <= rsi <= 60) and above:
                entry, stop = calc_entry_stop(df)
                tp = calc_tp(entry, stop)
                score = calc_score(vol, bb, True, above, btc_positive)

                msg = f"""🚀 <b>LONG</b>
{symbol} | {label}

💰 السعر: {price}
📊 RSI: {rsi:.1f}

🎯 دخول: {entry}
🛑 ستوب: {stop}
🏁 هدف: {tp}

₿ السوق: {btc_trend}
🔥 Score: {score}/10
"""
                send_telegram(msg)

                top_signals.append((symbol, score, label, "LONG", entry, stop))

            # SHORT
            if (vol or bb) and (40 <= rsi <= 55) and below and bearish:
                entry, stop = calc_entry_stop_sell(df)
                tp = calc_tp_sell(entry, stop)
                score = calc_score(vol, bb, True, below, btc_positive)

                msg = f"""🔻 <b>SHORT</b>
{symbol} | {label}

💰 السعر: {price}
📊 RSI: {rsi:.1f}

🎯 دخول: {entry}
🛑 ستوب: {stop}
🏁 هدف: {tp}

₿ السوق: {btc_trend}
🔥 Score: {score}/10
"""
                send_telegram(msg)

                top_signals.append((symbol, score, label, "SHORT", entry, stop))

            time.sleep(0.3)

        except Exception as e:
            print("Error:", e)

def send_top10(top_signals):
    if not top_signals:
        return

    sorted_signals = sorted(top_signals, key=lambda x: x[1], reverse=True)[:10]

    msg = "🏆 <b>أفضل الفرص:</b>\n\n"
    for i, s in enumerate(sorted_signals, 1):
        msg += f"{i}. {s[0]} ({s[2]}) [{s[3]}] - {s[1]}/10\n"
        msg += f"🎯 {s[4]} | 🛑 {s[5]}\n\n"

    send_telegram(msg)

last_update_id = 0

while True:
    last_update_id = check_telegram_commands(last_update_id)

    top_signals = []
    scan("SPOT", top_signals)
    scan("SWAP", top_signals)
    send_top10(top_signals)

    print("Waiting 4 hours...")
    time.sleep(14400)
