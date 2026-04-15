import os
import time
import requests
import threading
import pandas as pd
import numpy as np

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# =====================
# CONFIG
# =====================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

bot_active = True

# =====================
# TELEGRAM SEND
# =====================
def send_telegram(message):
    if not TOKEN or not bot_active:
        return

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, data=payload, timeout=15)
    except:
        pass

# =====================
# TV LINK
# =====================
def get_tv_link(symbol, bar="1H"):
    clean = symbol.replace("-SWAP", "").replace("-USDT", "USDT")
    interval = "60" if bar == "1H" else "240"
    return f"https://www.tradingview.com/chart/?symbol=OKX:{clean}&interval={interval}"

# =====================
# BTC STATUS
# =====================
def get_btc_status():
    try:
        url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=1H&limit=20"
        data = requests.get(url).json().get("data", [])
        df = pd.DataFrame(data, columns=['ts','o','h','l','c','v','v2','v3','v4'])
        df = df.iloc[::-1]
        df['c'] = df['c'].astype(float)

        price = df['c'].iloc[-1]
        ma = df['c'].rolling(20).mean().iloc[-1]

        return "🟢 صاعد" if price > ma else "🔴 هابط"
    except:
        return "غير معروف"

# =====================
# INDICATORS
# =====================
def indicators(df):
    df['c'] = df['c'].astype(float)
    df['h'] = df['h'].astype(float)
    df['l'] = df['l'].astype(float)
    df['v'] = df['v'].astype(float)

    delta = df['c'].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()

    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    df['ma20'] = df['c'].rolling(20).mean()

    return df

# =====================
# EARLY ENTRY ENGINE
# =====================
def early_entry(df):
    price = df['c'].iloc[-1]
    prev = df['c'].iloc[-3]

    vol = df['v'].iloc[-1]
    avg_vol = df['v'].rolling(20).mean().iloc[-1]

    momentum = (price - prev) / prev

    if momentum > 0.01 and vol > avg_vol * 1.2:
        return "LONG"
    if momentum < -0.01 and vol > avg_vol * 1.2:
        return "SHORT"

    return None

# =====================
# VOLUME SPIKE
# =====================
def volume_spike(df):
    return df['v'].iloc[-1] > df['v'].rolling(20).mean().iloc[-1] * 1.5

# =====================
# STRENGTH
# =====================
def strength(score):
    if score >= 8.5:
        return "💎 VIP"
    elif score >= 7:
        return "🟢 قوي"
    elif score >= 5:
        return "🟡 متوسط"
    else:
        return "🔴 ضعيف"

# =====================
# SCAN ENGINE
# =====================
def scan(inst_type):
    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP" if inst_type == "SWAP" else \
          "https://www.okx.com/api/v5/market/tickers?instType=SPOT"

    data = requests.get(url).json().get("data", [])

    for p in data[:75]:
        try:
            symbol = p["instId"]

            if not symbol.endswith("USDT") and not symbol.endswith("USDT-SWAP"):
                continue

            bar = "1H" if inst_type == "SWAP" else "4H"

            url_c = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit=50"
            res = requests.get(url_c).json().get("data", [])

            if len(res) < 30:
                continue

            df = pd.DataFrame(res, columns=['ts','o','h','l','c','v','v2','v3','v4'])
            df = df.iloc[::-1]
            df = indicators(df)

            price = df['c'].iloc[-1]
            rsi = df['rsi'].iloc[-1]

            direction = early_entry(df)
            if not direction:
                continue

            vol_ok = volume_spike(df)

            conf = 6 + (0.7 if vol_ok else 0) + (0.3 if 30 < rsi < 70 else 0)
            conf = round(conf, 2)

            low = df['l'].tail(3).min()
            high = df['h'].tail(3).max()

            stop = low * 0.985 if direction == "LONG" else high * 1.015

            link = get_tv_link(symbol, bar)

            vip_tag = "💎 VIP SIGNAL 🔥🔥\n" if conf >= 8.5 else ""

            msg = (
                f"{vip_tag}"
                f"🧠 إشارة ذكية | {'🟢 LONG' if direction=='LONG' else '🔴 SHORT'}\n"
                f"⚡ EARLY ENTRY 🚀\n"
                f"────────────\n"
                f"🪙 {symbol}\n"
                f"💰 السعر: {price}\n"
                f"📊 RSI: {round(rsi,1)}\n"
                f"🎯 دخول: {price}\n"
                f"🛑 وقف: {round(stop,6)}\n"
                f"�ى القوة: {conf}/10 | {strength(conf)}\n"
                f"────────────\n"
                f"📈 <a href='{link}'>TradingView</a>"
            )

            send_telegram(msg)
            time.sleep(0.05)

        except:
            continue

# =====================
# TOP 10 SIMPLE
# =====================
def send_top(inst_type):
    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP" if inst_type == "SWAP" else \
          "https://www.okx.com/api/v5/market/tickers?instType=SPOT"

    data = requests.get(url).json().get("data", [])

    msg = f"🚀 TOP 10 {inst_type}\n\n"

    for i, p in enumerate(data[:10], 1):
        symbol = p["instId"]
        link = get_tv_link(symbol, "1H" if inst_type == "SWAP" else "4H")
        msg += f"{i}. <a href='{link}'>{symbol}</a>\n"

    send_telegram(msg)

# =====================
# COMMANDS
# =====================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global bot_active
    bot_active = True
    await update.message.reply_text("🚀 Bot Started\n⚡ Early Entry Active")

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global bot_active
    bot_active = False
    await update.message.reply_text("⛔ Bot Stopped")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🧠 Bot System:\n"
        "⚡ Early Entry Momentum\n"
        "💎 VIP Signals\n"
        "📊 Futures 1H / Spot 4H\n"
        "/start /stop /help /top"
    )

async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    send_top("SWAP")
    send_top("SPOT")
    await update.message.reply_text("🚀 TOP 10 Sent")

# =====================
# LOOPS
# =====================
def futures_loop():
    while True:
        scan("SWAP")
        send_top("SWAP")
        time.sleep(3600)

def spot_loop():
    while True:
        scan("SPOT")
        send_top("SPOT")
        time.sleep(14400)

# =====================
# MAIN
# =====================
def main():
    if not TOKEN:
        return

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("top", top_cmd))

    threading.Thread(target=futures_loop, daemon=True).start()
    threading.Thread(target=spot_loop, daemon=True).start()

    print("🚀 Bot Running - Early Entry System")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
