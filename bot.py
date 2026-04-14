import os
import time
import requests
import threading
import pandas as pd
import numpy as np
from telegram.ext import Application, CommandHandler, ContextTypes

# =====================
# CONFIG
# =====================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

bot_active = True  # START ON DEFAULT

# =====================
# TELEGRAM SENDER
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
# TRADINGVIEW LINK
# =====================
def get_tv_link(symbol, bar="1H"):
    clean = symbol.replace("-SWAP", "").replace("-USDT", "USDT")
    interval_map = {"1H": "60", "4H": "240"}
    interval = interval_map.get(bar, "60")
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
# AI MOMENTUM (EARLY)
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
# VIP SYSTEM
# =====================
def is_vip(conf, vol_ok):
    return conf >= 8.5 and vol_ok

# =====================
# SMART FILTER (soft now)
# =====================
def volume_spike(df):
    return df['v'].iloc[-1] > df['v'].rolling(20).mean().iloc[-1] * 1.5

# =====================
# PRIORITY + STRENGTH
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
# SCAN ENGINE (UPDATED EARLY)
# =====================
def scan(inst_type, btc_status):
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

            # SCORE SIMPLE
            conf = 6 + (0.5 if vol_ok else 0) + (0.5 if rsi < 40 or rsi > 60 else 0)
            conf = round(conf, 2)

            vip = is_vip(conf, vol_ok)

            low = df['l'].tail(3).min()
            high = df['h'].tail(3).max()

            stop = low * 0.985 if direction == "LONG" else high * 1.015

            link = get_tv_link(symbol, bar)

            # TYPE LABEL
            type_label = "⚡ EARLY ENTRY 🚀"
            vip_tag = "💎 VIP SIGNAL 🔥🔥\n" if vip else ""

            msg = (
                f"{vip_tag}"
                f"🧠 إشارة ذكية | {'🟢 LONG' if direction=='LONG' else '🔴 SHORT'}\n"
                f"{type_label}\n"
                f"────────────\n"
                f"🪙 {symbol}\n"
                f"💰 السعر: {price}\n"
                f"📊 RSI: {round(rsi,1)}\n"
                f"🎯 دخول: {price}\n"
                f"🛑 وقف: {round(stop,6)}\n"
                f"🔥 القوة: {conf}/10 | {strength(conf)}\n"
                f"₿ BTC: {btc_status}\n"
                f"────────────\n"
                f"📈 <a href='{link}'>TradingView</a>"
            )

            send_telegram(msg)

            time.sleep(0.05)

        except:
            continue

# =====================
# TOP REPORT (SIMPLE KEEP)
# =====================
def send_top(inst_type, btc_status):
    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP" if inst_type == "SWAP" else \
          "https://www.okx.com/api/v5/market/tickers?instType=SPOT"

    data = requests.get(url).json().get("data", [])

    coins = []

    for p in data[:75]:
        symbol = p["instId"]
        coins.append(symbol)

    msg = f"🚀 TOP 10 ({inst_type})\n📊 BTC: {btc_status}\n\n"

    for i, c in enumerate(coins[:10], 1):
        link = get_tv_link(c, "1H" if inst_type == "SWAP" else "4H")
        msg += f"{i}. <a href='{link}'>{c}</a>\n"

    send_telegram(msg)

# =====================
# COMMANDS
# =====================
async def start_cmd(update: ContextTypes.DEFAULT_TYPE, context: ContextTypes.DEFAULT_TYPE):
    global bot_active
    bot_active = True
    await update.message.reply_text("🚀 Bot Started\n🧠 Early Entry Active")

async def stop_cmd(update: ContextTypes.DEFAULT_TYPE, context: ContextTypes.DEFAULT_TYPE):
    global bot_active
    bot_active = False
    await update.message.reply_text("⛔ Bot Stopped")

async def help_cmd(update: ContextTypes.DEFAULT_TYPE, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🧠 Bot Philosophy:\n"
        "⚡ Early Entry Momentum Detection\n"
        "💎 VIP Signals Only\n"
        "📊 Futures 1H / Spot 4H\n"
        "/start /stop /help /top"
    )

async def top_cmd(update: ContextTypes.DEFAULT_TYPE, context: ContextTypes.DEFAULT_TYPE):
    btc = get_btc_status()
    send_top("SWAP", btc)
    send_top("SPOT", btc)
    await update.message.reply_text("🚀 Top 10 Sent")

# =====================
# LOOPS
# =====================
def futures_loop():
    while True:
        btc = get_btc_status()
        scan("SWAP", btc)
        send_top("SWAP", btc)
        time.sleep(3600)

def spot_loop():
    while True:
        btc = get_btc_status()
        scan("SPOT", btc)
        send_top("SPOT", btc)
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

    print("🚀 Bot Running (Early Entry Mode)")
    app.run_polling()

if __name__ == "__main__":
    main()
