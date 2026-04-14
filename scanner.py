import os
import time
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import threading
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ================== إعدادات من Railway ==================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "5523662724")

EXCLUDE = {
    "USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","WIN","SHIB",
    "USDG","NFT","USDP","USDD","FRAX","LUSD","GUSD","HUSD","SUSD","CUSD",
    "ZUSD","USDX","USDN","USDK","USDQ","USDB","SPY","TSLA","AAPL","AMZN",
    "GOOGL","MSFT","NVDA","META","NFLX","XAU","XAUT","XAG","PAXG","OIL","CRUDE"
}

top_spot, top_long, top_short = [], [], []

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True  # هذا السطر سيلغي المعاينة والصندوق الإنجليزي
    }
    try:
        requests.post(url, data=payload, timeout=10)
        time.sleep(0.7)
    except Exception as e:
        print(f"Telegram error: {e}")

def get_btc_trend():
    try:
        url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=4H&limit=10"
        r = requests.get(url, timeout=10).json()
        closes = [float(x[4]) for x in r.get("data", [])]
        closes.reverse()
        return "إيجابي ✅" if len(closes) >= 4 and closes[-1] > closes[-3] else "سلبي ⚠️"
    except:
        return "غير معروف"

def get_pairs(inst_type):
    try:
        url = f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}"
        r = requests.get(url, timeout=10).json()
        data = r.get("data", [])
        if inst_type == "SPOT":
            return [(x["instId"], float(x["last"])) for x in data 
                    if x["instId"].endswith("-USDT") and x["instId"].split("-")[0] not in EXCLUDE]
        else:
            return [(x["instId"], float(x["last"])) for x in data 
                    if "USDT" in x["instId"] and x["instId"].split("-")[0] not in EXCLUDE]
    except:
        return []

def get_candles(symbol, timeframe, limit=100):
    try:
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={timeframe}&limit={limit}"
        r = requests.get(url, timeout=15).json()
        if "data" not in r or len(r["data"]) < 40: return None
        df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df = df.astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        return df
    except:
        return None

def calc_atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean().iloc[-1]

def check_volume_spike(df, multiplier=3.0):
    avg_vol = df["vol"].iloc[-30:-1].mean()
    return df["vol"].iloc[-1] > avg_vol * multiplier

def check_bb_squeeze(df, period=20):
    close = df["close"]
    ma = close.rolling(period).mean()
    std = close.rolling(period).std()
    bandwidth = (ma + 2*std - (ma - 2*std)) / ma
    current = bandwidth.iloc[-1]
    avg = bandwidth.iloc[-60:-10].mean() if len(bandwidth) > 60 else bandwidth.mean()
    return current < avg * 0.65

def check_rsi(df, period=14):
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.where(delta < 0, 0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss
    return (100 - (100 / (1 + rs))).iloc[-1]

def calc_score(vol, bb, rsi_ok, ma_ok, btc_ok):
    score = 0
    if vol: score += 3.5
    if bb: score += 3.0
    if rsi_ok: score += 2.0
    if ma_ok: score += 1.0
    if btc_ok: score += 0.5
    return round(min(score, 10), 1)

# ===================== Commands =====================
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📖 <b>الدليل الفني وتشغيل البوت:</b>\n\n"
        "1️⃣ <b>الفريمات:</b> سبوت (4H)، فيوتشر (1H).\n"
        "2️⃣ <b>اللوجيك:</b> اختراق MA20 + فوليوم عالي أو انضغاط بولنجر + RSI مثالي.\n"
        "3️⃣ <b>التكرار:</b> المسح الشامل يتم كل ساعة.\n\n"
        "/top10 - أفضل 30 فرصة مقسمة\n"
        "/help - الدليل الفني"
    )
    await update.message.reply_html(text, disable_web_page_preview=True)

async def top10_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🏆 <b>قائمة الـ Sniper Top 10:</b>\n\n"
    categories = [("🟢 سبوت (4H)", top_spot, 240, ""), ("🔵 لونج (1H)", top_long, 60, ".P"), ("🔴 شورت (1H)", top_short, 60, ".P")]
    for title, data, tf, suffix in categories:
        msg += f"<b>{title}:</b>\n"
        if not data: msg += "<i>جاري الجمع...</i>\n"
        for i, s in enumerate(sorted(data, key=lambda x: x["score"], reverse=True)[:10], 1):
            tv_link = f"https://www.tradingview.com/chart/?symbol=OKX%3A{s['symbol'].split('-')[0]}USDT{suffix}&interval={tf}"
            msg += f"{i}. {s['symbol']} | {s['score']}/10 | {s['price']} | <a href='{tv_link}'>📈</a>\n"
        msg += "\n"
    await update.message.reply_html(msg, disable_web_page_preview=True)

# ===================== Scan Function =====================
def scan_process():
    global top_spot, top_long, top_short
    while True:
        try:
            temp_spot, temp_long, temp_short = [], [], []
            btc_trend = get_btc_trend()
            btc_positive = "إيجابي" in btc_trend

            # --- SPOT (4H) ---
            for symbol, price in get_pairs("SPOT"):
                df = get_candles(symbol, "4H")
                if df is not None:
                    rsi, vol_s, bb_s = check_rsi(df), check_volume_spike(df), check_bb_squeeze(df)
                    ma20 = df["close"].rolling(20).mean().iloc[-1]
                    if (vol_s or bb_s) and 42 <= rsi <= 62 and df["close"].iloc[-1] > ma20:
                        sc = calc_score(vol_s, bb_s, True, True, btc_positive)
                        atr = calc_atr(df)
                        temp_spot.append({"symbol": symbol, "score": sc, "price": price})
                        tv_link = f"https://www.tradingview.com/chart/?symbol=OKX%3A{symbol.split('-')[0]}USDT&interval=240"
                        msg = (f"<b>{'🔵 Vol Spike' if vol_s else '🟡 BB Squeeze'} | سبوت 🟢</b>\n"
                               f"{symbol}\n💰 السعر: {price}\n📊 RSI: {rsi:.1f} | MA20 فوق ✅\n"
                               f"🎯 دخول: {price}\n🛑 ستوب: {round(price-(atr*1.5), 6)}\n"
                               f"₿ BTC: {btc_trend}\n🔥 التقييم: {sc}/10\n"
                               f"🔗 <a href='{tv_link}'>افتح الشارت (4H)</a>")
                        send_telegram(msg)
                time.sleep(0.1)

            # --- SWAP (1H) ---
            for symbol, price in get_pairs("SWAP"):
                df = get_candles(symbol, "1H")
                if df is not None:
                    rsi, vol_s, bb_s = check_rsi(df), check_volume_spike(df), check_bb_squeeze(df)
                    ma20 = df["close"].rolling(20).mean().iloc[-1]
                    # Long
                    if (vol_s or bb_s) and 42 <= rsi <= 62 and df["close"].iloc[-1] > ma20:
                        sc = calc_score(vol_s, bb_s, True, True, btc_positive)
                        atr = calc_atr(df)
                        temp_long.append({"symbol": symbol, "score": sc, "price": price})
                        tv_link = f"https://www.tradingview.com/chart/?symbol=OKX%3A{symbol.split('-')[0]}USDT.P&interval=60"
                        msg = (f"<b>🔵 LONG SIGNAL | فيوتشر 🔴</b>\n"
                               f"{symbol}\n💰 السعر: {price}\n📊 RSI: {rsi:.1f} | MA20 فوق ✅\n"
                               f"🎯 دخول: {price}\n🛑 ستوب: {round(price-(atr*1.5), 6)}\n"
                               f"₿ BTC: {btc_trend}\n🔥 التقييم: {sc}/10\n"
                               f"🔗 <a href='{tv_link}'>افتح الشارت (1H)</a>")
                        send_telegram(msg)
                    # Short
                    if (vol_s or bb_s) and rsi > 65 and df["close"].iloc[-1] < ma20:
                        sc = calc_score(vol_s, bb_s, True, True, not btc_positive)
                        temp_short.append({"symbol": symbol, "score": sc, "price": price})
                        tv_link = f"https://www.tradingview.com/chart/?symbol=OKX%3A{symbol.split('-')[0]}USDT.P&interval=60"
                        msg = (f"<b>🔴 SHORT SIGNAL | فيوتشر 🔴</b>\n"
                               f"{symbol}\n💰 السعر: {price}\n📊 RSI: {rsi:.1f} | MA20 تحت ⚠️\n"
                               f"🎯 دخول: {price}\n₿ BTC: {btc_trend}\n🔥 التقييم: {sc}/10\n"
                               f"🔗 <a href='{tv_link}'>افتح الشارت (1H)</a>")
                        send_telegram(msg)
                time.sleep(0.1)

            top_spot, top_long, top_short = temp_spot, temp_long, temp_short
            time.sleep(3600)
        except: time.sleep(300)

def main():
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("top10", top10_command))
    threading.Thread(target=scan_process, daemon=True).start()
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
