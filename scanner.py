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

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN مش موجود في Environment Variables على Railway!")

EXCLUDE = {
    "USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","WIN","SHIB",
    "USDG","NFT","USDP","USDD","FRAX","LUSD","GUSD","HUSD","SUSD","CUSD",
    "ZUSD","USDX","USDN","USDK","USDQ","USDB","SPY","TSLA","AAPL","AMZN",
    "GOOGL","MSFT","NVDA","META","NFLX","XAU","XAUT","XAG","PAXG","OIL","CRUDE"
}

top_signals_global = []

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
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


def get_candles(symbol, limit=120):
    try:
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=4H&limit={limit}"
        r = requests.get(url, timeout=15).json()
        if "data" not in r or len(r["data"]) < 40:
            return None
        df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df = df.astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        return df.iloc[1:] if len(df) > 1 else df
    except:
        return None


def calc_atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean().iloc[-1]


def check_volume_spike(df, multiplier=3.0):
    if len(df) < 30: return False
    avg_vol = df["vol"].iloc[-30:-1].mean()
    return df["vol"].iloc[-1] > avg_vol * multiplier


def check_bb_squeeze(df, period=20):
    if len(df) < period + 20: return False
    close = df["close"]
    ma = close.rolling(period).mean()
    std = close.rolling(period).std()
    bandwidth = (ma + 2*std - (ma - 2*std)) / ma
    current = bandwidth.iloc[-1]
    avg = bandwidth.iloc[-60:-10].mean() if len(bandwidth) > 60 else bandwidth.mean()
    return current < avg * 0.65


def check_rsi(df, period=14):
    if len(df) < period + 10: 
        return 50.0
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.where(delta < 0, 0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss
    return (100 - (100 / (1 + rs))).iloc[-1]


def check_above_ma20(df):
    if len(df) < 25: return False
    return df["close"].iloc[-1] > df["close"].rolling(20).mean().iloc[-1]


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
        "🚀 <b>OKX 4H Scanner Bot</b>\n\n"
        "البوت بيسكان السوق كل 4 ساعات ويبعت إشارات قوية.\n\n"
        "<b>أنواع الإشارات:</b>\n"
        "🚨 STRONG SIGNAL → Long قوي جداً\n"
        "🔵 Volume Spike → Long\n"
        "🟡 BB Squeeze → Long\n"
        "🔴 SHORT SIGNAL → Short (فيوتشر فقط)\n\n"
        "/help - هذه الرسالة\n"
        "/top10 - أفضل 10 إشارات حالية"
    )
    await update.message.reply_html(text)


async def top10_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not top_signals_global:
        await update.message.reply_html("❌ مفيش إشارات حالية دلوقتي.")
        return

    sorted_signals = sorted(top_signals_global, key=lambda x: x["score"], reverse=True)[:10]
    msg = "🏆 <b>أفضل 10 إشارات حالياً:</b>\n\n"
    for i, s in enumerate(sorted_signals, 1):
        msg += f"{i}. <b>{s['symbol']}</b> ({s.get('label', '')}) — <b>{s['score']}/10</b>\n"
        msg += f"   💰 {s['price']:,.6f} | 🎯 {s['entry']} | 🛑 {s['stop']}\n\n"
    await update.message.reply_html(msg)


# ===================== Scan Function =====================
def scan(inst_type):
    global top_signals_global
    label = "سبوت 🟢" if inst_type == "SPOT" else "فيوتشر 🔴"
    print(f"[{datetime.now().strftime('%H:%M')}] Scanning {inst_type}...")

    pairs = get_pairs(inst_type)
    btc_trend = get_btc_trend()
    btc_positive = "إيجابي" in btc_trend

    for symbol, ticker_price in pairs:
        try:
            df = get_candles(symbol)
            if df is None or len(df) < 40:
                continue

            current_price = df["close"].iloc[-1]
            vol_spike = check_volume_spike(df)
            bb_squeeze = check_bb_squeeze(df)
            rsi = check_rsi(df)
            above_ma20 = check_above_ma20(df)
            rsi_ok = 42 <= rsi <= 62

            # === Long Signals ===
            if (vol_spike or bb_squeeze) and rsi_ok and above_ma20:
                score = calc_score(vol_spike, bb_squeeze, rsi_ok, above_ma20, btc_positive)
                atr = calc_atr(df)
                entry = round(current_price, 6)
                stop = round(current_price - (atr * 1.8), 6)

                if vol_spike and bb_squeeze:
                    signal_type = "🚨 <b>STRONG SIGNAL</b>"
                elif vol_spike:
                    signal_type = "🔵 <b>Volume Spike</b>"
                else:
                    signal_type = "🟡 <b>BB Squeeze</b>"

                tv_symbol = f"OKX:{symbol.split('-')[0]}USDT" + (".P" if inst_type != "SPOT" else "")
                tv_link = f"https://www.tradingview.com/chart/?symbol={tv_symbol.replace(':', '%3A')}&interval=240"

                msg = (
                    f"{signal_type}\n"
                    f"{symbol} | {label}\n"
                    f"💰 السعر الحالي: {current_price:,.6f}\n"
                    f"📊 RSI: {rsi:.1f} | فوق MA20\n"
                    f"🎯 دخول: {entry}\n"
                    f"🛑 ستوب: {stop}\n"
                    f"₿ BTC: {btc_trend}\n"
                    f"🔥 تقييم الإشارة: {score}/10\n\n"
                    f"📈 <a href=\"{tv_link}\">افتح الشارت على TradingView (4H)</a>"
                )
                send_telegram(msg)

                top_signals_global.append({
                    "symbol": symbol, "score": score, "label": label,
                    "price": current_price, "entry": entry, "stop": stop
                })

            # === Short Signal - فقط في الفيوتشر ===
            if inst_type == "SWAP":
                short_rsi = check_rsi(df)
                below_ma20 = current_price < df["close"].rolling(20).mean().iloc[-1]
                if short_rsi > 65 and below_ma20 and (vol_spike or bb_squeeze):
                    score = calc_score(vol_spike, bb_squeeze, short_rsi > 65, not above_ma20, not btc_positive)
                    atr = calc_atr(df)
                    entry = round(current_price, 6)
                    stop = round(current_price + (atr * 1.8), 6)

                    signal_type = "🔴 <b>SHORT SIGNAL</b>"

                    tv_symbol = f"OKX:{symbol.split('-')[0]}USDT.P"
                    tv_link = f"https://www.tradingview.com/chart/?symbol={tv_symbol.replace(':', '%3A')}&interval=240"

                    msg = (
                        f"{signal_type}\n"
                        f"{symbol} | {label}\n"
                        f"💰 السعر الحالي: {current_price:,.6f}\n"
                        f"📊 RSI: {short_rsi:.1f} | تحت MA20\n"
                        f"🎯 دخول شورت: {entry}\n"
                        f"🛑 ستوب: {stop}\n"
                        f"₿ BTC: {btc_trend}\n"
                        f"🔥 تقييم الإشارة: {score}/10\n\n"
                        f"📈 <a href=\"{tv_link}\">افتح الشارت على TradingView (4H)</a>"
                    )
                    send_telegram(msg)

                    top_signals_global.append({
                        "symbol": symbol, "score": score, "label": "شورت فيوتشر 🔴",
                        "price": current_price, "entry": entry, "stop": stop
                    })

            time.sleep(0.35)

        except Exception as e:
            continue

    print(f"{inst_type} scan finished.")


# ===================== Main =====================
def main():
    print("🚀 OKX Scanner Bot بدأ على Railway...")

    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("top10", top10_command))

    # تشغيل السكان في thread منفصل
    def scanner_loop():
        while True:
            try:
                global top_signals_global
                top_signals_global = []
                scan("SPOT")
                scan("SWAP")
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Scan complete")
                time.sleep(14400)  # 4 ساعات
            except Exception as e:
                print(f"Scanner error: {e}")
                time.sleep(300)

    threading.Thread(target=scanner_loop, daemon=True).start()

    # تشغيل الـ Bot
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
