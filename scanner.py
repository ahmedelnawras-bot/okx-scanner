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

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, data=payload, timeout=10)
        time.sleep(0.8) # تأخير بسيط لتجنب الـ Spam
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

def create_alert_msg(symbol, price, rsi, ma_status, btc_trend, score, strat_label, tf_text, tv_link, atr=None):
    stop_text = f"🛑 ستوب: {round(price-(atr*1.5), 6)}" if atr else ""
    return (f"<b>{strat_label}</b>\n"
            f"{symbol}\n💰 السعر: {price}\n📊 RSI: {rsi:.1f} | MA20 {ma_status}\n"
            f"🎯 دخول: {price}\n{stop_text}\n"
            f"₿ BTC: {btc_trend}\n🔥 التقييم: {score}/10\n"
            f"🔗 <a href='{tv_link}'>افتح الشارت ({tf_text})</a>")

# ===================== Scan Function =====================
def scan_process():
    while True:
        try:
            btc_trend = get_btc_trend()
            btc_positive = "إيجابي" in btc_trend

            # --- المسح ---
            markets = [("SPOT", "4H", "240", "🟢 سبوت"), ("SWAP", "1H", "60", "🔴 فيوتشر")]
            
            for inst_type, tf, tf_link_val, market_label in markets:
                suffix = ".P" if inst_type == "SWAP" else ""
                for symbol, price in get_pairs(inst_type):
                    df = get_candles(symbol, tf)
                    if df is not None:
                        rsi = check_rsi(df)
                        vol_s = check_volume_spike(df)
                        bb_s = check_bb_squeeze(df)
                        ma20 = df["close"].rolling(20).mean().iloc[-1]
                        curr_price = df["close"].iloc[-1]
                        atr = calc_atr(df)
                        
                        tv_link = f"https://www.tradingview.com/chart/?symbol=OKX%3A{symbol.split('-')[0]}USDT{suffix}&interval={tf_link_val}"

                        # 1. حالة الاجتماع (كلاهما معاً)
                        if vol_s and bb_s:
                            if (42 <= rsi <= 62 and curr_price > ma20) or (rsi > 65 and curr_price < ma20):
                                label = f"🔥 🔵 Vol Spike & 🟡 BB Squeeze | {market_label}"
                                msg = create_alert_msg(symbol, price, rsi, "فوق ✅" if curr_price > ma20 else "تحت ⚠️", btc_trend, 9.5, label, tf, tv_link, atr)
                                send_telegram(msg)
                                continue # لا ترسل التنبيهات المنفردة لو اجتمعوا

                        # 2. حالة الفوليوم فقط
                        if vol_s:
                            if (42 <= rsi <= 62 and curr_price > ma20) or (rsi > 65 and curr_price < ma20):
                                label = f"🔵 Volume Spike | {market_label}"
                                msg = create_alert_msg(symbol, price, rsi, "فوق ✅" if curr_price > ma20 else "تحت ⚠️", btc_trend, 7.5, label, tf, tv_link, atr)
                                send_telegram(msg)

                        # 3. حالة البولنجر فقط
                        if bb_s:
                            if (42 <= rsi <= 62 and curr_price > ma20) or (rsi > 65 and curr_price < ma20):
                                label = f"🟡 BB Squeeze | {market_label}"
                                msg = create_alert_msg(symbol, price, rsi, "فوق ✅" if curr_price > ma20 else "تحت ⚠️", btc_trend, 7.0, label, tf, tv_link, atr)
                                send_telegram(msg)

                    time.sleep(0.1)
            time.sleep(3600)
        except: time.sleep(300)

def main():
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    threading.Thread(target=scan_process, daemon=True).start()
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
