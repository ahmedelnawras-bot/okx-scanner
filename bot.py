import os
import time
import requests
import threading
import pandas as pd
import numpy as np

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

bot_active = True

# =====================
# TELEGRAM SAFE SEND
# =====================
def send_telegram(message):
    if not TOKEN or not bot_active:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={
                "chat_id": CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            },
            timeout=10
        )
    except:
        pass

# =====================
# CLEAN TELEGRAM SESSION 🔥
# =====================
def clean_telegram():
    try:
        requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
        time.sleep(2)
        requests.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates?offset=-1")
        time.sleep(2)
    except:
        pass

# =====================
# TRADINGVIEW
# =====================
def get_tv_link(symbol, bar="1H"):
    clean = symbol.replace("-SWAP", "").replace("-USDT", "USDT")
    interval = "60" if bar == "1H" else "240"
    return f"https://www.tradingview.com/chart/?symbol=OKX:{clean}&interval={interval}"

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
# EARLY ENTRY
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

def volume_spike(df):
    return df['v'].iloc[-1] > df['v'].rolling(20).mean().iloc[-1] * 1.5

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
# TOP 75 (USDT ONLY)
# =====================
def get_top75(inst_type):
    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP" if inst_type == "SWAP" else \
          "https://www.okx.com/api/v5/market/tickers?instType=SPOT"

    data = requests.get(url).json().get("data", [])

    coins = []
    for p in data:
        symbol = p["instId"]

        if not symbol.endswith("USDT") and not symbol.endswith("USDT-SWAP"):
            continue
        if "USDC" in symbol or "EUR" in symbol:
            continue

        vol = float(p.get("vol24h", 0))
        coins.append({"symbol": symbol, "vol": vol})

    coins = sorted(coins, key=lambda x: x["vol"], reverse=True)
    return [c["symbol"] for c in coins[:75]]

# =====================
# SCAN
# =====================
def scan(inst_type, results):
    pairs = get_top75(inst_type)

    for symbol in pairs:
        try:
            bar = "1H" if inst_type == "SWAP" else "4H"

            url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit=50"
            data = requests.get(url).json().get("data", [])

            if len(data) < 30:
                continue

            df = pd.DataFrame(data, columns=['ts','o','h','l','c','v','v2','v3','v4'])
            df = df.iloc[::-1]
            df = indicators(df)

            price = df['c'].iloc[-1]
            rsi = df['rsi'].iloc[-1]

            direction = early_entry(df)
            if not direction:
                continue

            vol_ok = volume_spike(df)

            score = 6 + (0.7 if vol_ok else 0) + (0.3 if 30 < rsi < 70 else 0)
            score = round(score, 2)

            low = df['l'].tail(3).min()
            high = df['h'].tail(3).max()

            stop = low * 0.985 if direction == "LONG" else high * 1.015
            link = get_tv_link(symbol, bar)

            msg = (
                f"🧠 {'🟢 LONG' if direction=='LONG' else '🔴 SHORT'}\n"
                f"🪙 {symbol}\n"
                f"💰 {price}\n"
                f"🔥 {score}/10 | {strength(score)}\n"
                f"📈 <a href='{link}'>TradingView</a>"
            )

            send_telegram(msg)

            results.append({
                "symbol": symbol,
                "score": score,
                "type": direction,
                "link": link,
                "price": price
            })

            time.sleep(0.05)

        except:
            continue

# =====================
# TOP REPORT
# =====================
def send_top(results, title, inst_type="SWAP"):
    if not results:
        return

    msg = f"🚀 <b>{title}</b>\n\n"

    longs = sorted([r for r in results if r["type"] == "LONG"], key=lambda x: x["score"], reverse=True)[:10]
    shorts = sorted([r for r in results if r["type"] == "SHORT"], key=lambda x: x["score"], reverse=True)[:10]

    if longs:
        msg += "🟢 LONG:\n"
        for r in longs:
            msg += f"{r['symbol']} 💰 {r['price']} 🔥 {r['score']}\n"

    if inst_type == "SWAP" and shorts:
        msg += "\n🔴 SHORT:\n"
        for r in shorts:
            msg += f"{r['symbol']} 💰 {r['price']} 🔥 {r['score']}\n"

    send_telegram(msg)

# =====================
# COMMANDS
# =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global bot_active
    bot_active = True
    await update.message.reply_text("🚀 Started")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global bot_active
    bot_active = False
    await update.message.reply_text("⛔ Stopped")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("/start /stop /top")

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    results = []
    scan("SWAP", results)
    send_top(results, "Futures Top 10")

    await update.message.reply_text("Done")

# =====================
# LOOPS
# =====================
def loop():
    while True:
        try:
            results = []
            scan("SWAP", results)
            send_top(results, "Auto Report")
            time.sleep(3600)
        except:
            time.sleep(60)

# =====================
# MAIN
# =====================
def main():
    if not TOKEN:
        return

    clean_telegram()  # 🔥 الحل النهائي

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("top", top))

    threading.Thread(target=loop, daemon=True).start()

    print("🚀 Running Stable...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
