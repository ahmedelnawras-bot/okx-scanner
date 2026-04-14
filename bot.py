import os
import time
import requests
import threading
import pandas as pd
import numpy as np

# =====================
# CONFIG
# =====================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# =====================
# TELEGRAM
# =====================
def send_telegram(message):
    if not TOKEN:
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
# TRADINGVIEW
# =====================
def get_tv_link(symbol):
    clean = symbol.replace("-SWAP", "").replace("-USDT", "USDT")
    return f"https://www.tradingview.com/chart/?symbol=OKX:{clean}"

# =====================
# BTC STATUS
# =====================
def get_btc_status():
    try:
        url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=1H&limit=20"
        data = requests.get(url).json().get("data", [])
        if not data:
            return "غير معروف"
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
# AI (Simple Model)
# =====================
def ai_predict(df):
    last = df.iloc[-1]
    ma = df['ma20'].iloc[-1]
    rsi = df['rsi'].iloc[-1]

    prob = 0.5

    if last['c'] > ma:
        prob += 0.25
    else:
        prob -= 0.25

    if 30 < rsi < 70:
        prob += 0.1
    else:
        prob -= 0.1

    return max(0, min(1, prob))

# =====================
# SMART MONEY
# =====================
def liquidity_sweep(df):
    high = df['h'].iloc[-20:-2].max()
    low = df['l'].iloc[-20:-2].min()

    last_high = df['h'].iloc[-1]
    last_low = df['l'].iloc[-1]
    close = df['c'].iloc[-1]

    return (last_high > high and close < high,
            last_low < low and close > low)


def volume_spike(df):
    return df['v'].iloc[-1] > df['v'].rolling(20).mean().iloc[-1] * 1.5


def structure_break(df):
    high = df['h'].iloc[-20:-2].max()
    low = df['l'].iloc[-20:-2].min()
    close = df['c'].iloc[-1]

    return close > high, close < low


def smart_filter(df, prob):
    sweep_high, sweep_low = liquidity_sweep(df)
    vol = volume_spike(df)
    bos_up, bos_down = structure_break(df)

    direction = None
    boost = 0

    if sweep_low and bos_up:
        direction = "LONG"
        boost += 0.2

    if sweep_high and bos_down:
        direction = "SHORT"
        boost += 0.2

    if vol:
        boost += 0.1

    if prob > 0.78:
        base = "LONG"
    elif prob < 0.22:
        base = "SHORT"
    else:
        return None

    if direction and direction != base:
        return None

    return {
        "direction": base,
        "confidence": prob + boost
    }

# =====================
# PRIORITY + STRENGTH
# =====================
def get_priority(score):
    if score >= 7.5:
        return "🚨 HIGH"
    elif score >= 5:
        return "⚡ MEDIUM"
    else:
        return "💤 LOW"


def strength_level(score):
    if score >= 7:
        return "🟢 قوي"
    elif score >= 5:
        return "🟡 متوسط"
    else:
        return "🔴 ضعيف"

# =====================
# VIP SYSTEM
# =====================
def is_vip(prob, score, vol, smart):
    return score >= 8.5 and vol and smart and (prob >= 0.8 or prob <= 0.2)

# =====================
# RANKING SCORE
# =====================
def get_score(symbol):
    try:
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=1H&limit=40"
        data = requests.get(url).json().get("data", [])

        if len(data) < 30:
            return 0

        df = pd.DataFrame(data, columns=['ts','o','h','l','c','v','v2','v3','v4'])
        df = df.iloc[::-1]

        df['c'] = df['c'].astype(float)
        df['h'] = df['h'].astype(float)
        df['l'] = df['l'].astype(float)
        df['v'] = df['v'].astype(float)

        liquidity = df['v'].mean()
        volatility = (df['h'] - df['l']).mean()

        ma20 = df['c'].rolling(20).mean().iloc[-1]
        price = df['c'].iloc[-1]
        trend = abs(price - ma20) / ma20

        score = (
            np.log1p(liquidity) * 0.4 +
            np.log1p(volatility) * 0.3 +
            trend * 100 * 0.3
        )

        return score

    except:
        return 0


def get_top75():
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    data = requests.get(url).json().get("data", [])

    coins = []

    for p in data:
        symbol = p["instId"]

        if not symbol.endswith("USDT"):
            continue

        score = get_score(symbol)

        coins.append({"symbol": symbol, "score": score})

    coins = sorted(coins, key=lambda x: x["score"], reverse=True)

    return [c["symbol"] for c in coins[:75]]

# =====================
# SCAN ENGINE
# =====================
def scan(inst_type, results, btc_status):
    pairs = get_top75()

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
            ma = df['ma20'].iloc[-1]

            prob = ai_predict(df)
            decision = smart_filter(df, prob)

            if not decision:
                continue

            direction = decision["direction"]
            conf = round(decision["confidence"] * 10, 2)

            vol_ok = volume_spike(df)
            smart_ok = True

            vip = is_vip(prob, conf, vol_ok, smart_ok)

            priority = get_priority(conf)
            strength = strength_level(conf)

            low = df['l'].tail(3).min()
            high = df['h'].tail(3).max()

            stop = low * 0.985 if direction == "LONG" else high * 1.015

            link = get_tv_link(symbol)

            vip_tag = "💎 VIP SIGNAL 🔥🔥\n" if vip else ""

            msg = (
                f"{vip_tag}"
                f"🧠 إشارة ذكية | {'🟢 LONG' if direction=='LONG' else '🔴 SHORT'}\n"
                f"📊 النوع: {'🚀 FUTURES (1H)' if inst_type=='SWAP' else '💎 SPOT (4H)'}\n"
                f"────────────\n"
                f"🪙 {symbol}\n"
                f"💰 السعر: {price}\n"
                f"📊 RSI: {round(rsi,1)}\n"
                f"🎯 دخول: {price}\n"
                f"🛑 وقف: {round(stop,6)}\n"
                f"🔥 القوة: {conf}/10 | {strength}\n"
                f"📌 التصنيف: {priority}\n"
                f"₿ BTC: {btc_status}\n"
                f"────────────\n"
                f"📈 <a href='{link}'>TradingView</a>"
            )

            send_telegram(msg)

            results.append({
                "symbol": symbol,
                "score": conf,
                "type": direction,
                "link": link
            })

            time.sleep(0.05)

        except:
            continue

# =====================
# TOP REPORT
# =====================
def send_top(results, title, btc_status):
    if not results:
        return

    longs = sorted([r for r in results if r["type"] == "LONG"], key=lambda x: x["score"], reverse=True)[:10]
    shorts = sorted([r for r in results if r["type"] == "SHORT"], key=lambda x: x["score"], reverse=True)[:10]

    msg = f"🚀 <b>{title}</b>\n📊 BTC: {btc_status}\n\n"

    for i, r in enumerate(longs, 1):
        msg += f"🟢 {i}. <a href='{r['link']}'>{r['symbol']}</a> 🔥 {r['score']}\n"

    for i, r in enumerate(shorts, 1):
        msg += f"🔴 {i}. <a href='{r['link']}'>{r['symbol']}</a> 🔥 {r['score']}\n"

    send_telegram(msg)

# =====================
# LOOPS
# =====================
def futures_loop():
    while True:
        btc = get_btc_status()
        results = []
        scan("SWAP", results, btc)
        send_top(results, "FUTURES TOP 10 (1H)", btc)
        time.sleep(3600)


def spot_loop():
    while True:
        btc = get_btc_status()
        results = []
        scan("SPOT", results, btc)
        send_top(results, "SPOT TOP 10 (4H)", btc)
        time.sleep(14400)

# =====================
# MAIN
# =====================
def main():
    if not TOKEN:
        return

    requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")

    threading.Thread(target=futures_loop, daemon=True).start()
    threading.Thread(target=spot_loop, daemon=True).start()

    print("🚀 Bot Running...")
    while True:
        time.sleep(999999)

if __name__ == "__main__":
    main()
