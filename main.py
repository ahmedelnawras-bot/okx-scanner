import os
import time
import requests
import threading
import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- الإعدادات (تأكد من ضبطها في Railway Variables) ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "5523662724")

def send_telegram(message):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try: requests.post(url, data=payload, timeout=15)
    except: pass

def get_tv_link(symbol, inst_type):
    clean_symbol = symbol.replace("-USDT", "USDT").replace("-SWAP", "")
    return f"https://www.tradingview.com/chart/?symbol=OKX:{clean_symbol}"

def calculate_indicators(df):
    delta = df['c'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    df['ma20'] = df['c'].rolling(window=20).mean()
    return df

def scan(inst_type, all_signals, min_score):
    url_pairs = f"https://www.okx.com/api/v5/public/instruments?instType={inst_type}"
    try:
        pairs_data = requests.get(url_pairs).json().get('data', [])[:50]
        for p in pairs_data:
            symbol = p['instId']
            bar_frame = "1H" if inst_type == "SWAP" else "4H"
            url_candles = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar_frame}&limit=50"
            res = requests.get(url_candles).json().get('data', [])
            if not res: continue
            
            df = pd.DataFrame(res, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'v2', 'v3', 'v4'])
            df['c'] = df['c'].astype(float)
            df = df.iloc[::-1]
            df = calculate_indicators(df)
            
            curr_price = df['c'].iloc[-1]
            rsi = df['rsi'].iloc[-1]
            ma20 = df['ma20'].iloc[-1]
            
            # --- معادلة السكور ---
            score = 0
            if curr_price > ma20: score += 3
            if rsi < 30 or rsi > 70: score += 5 
            elif 35 > rsi or rsi > 65: score += 3
            if (curr_price > ma20 and rsi < 45): score += 2
            score = min(score, 10)
            
            signal_type = "LONG" if curr_price > ma20 else "SHORT"
            link = get_tv_link(symbol, inst_type)

            # --- التنبيه الفوري (سكور 8 للفيوتشر و 9 للسبوت) ---
            alert_threshold = 8 if inst_type == "SWAP" else 9
            
            if score >= alert_threshold:
                alert_type = "🔥 فرصة قوية" if score == 8 else "🌟 فرصة ذهبية"
                alert_msg = (
                    f"{alert_type} <b>({inst_type})</b>\n\n"
                    f"💰 العملة: <a href='{link}'>{symbol}</a>\n"
                    f"📊 السكور: {score}/10\n"
                    f"📈 الاتجاه: {signal_type}\n"
                    f"🏷️ السعر: {curr_price}\n"
                    f"⚡ RSI: {round(rsi, 2)}"
                )
                send_telegram(alert_msg)

            if score >= min_score:
                all_signals.append({
                    "symbol": symbol, "score": score, "type": signal_type,
                    "market": inst_type, "link": link
                })
            time.sleep(0.1)
    except: pass

def send_top10(all_signals, category):
    if not all_signals: return
    longs = sorted([s for s in all_signals if s["type"] == "LONG"], key=lambda x: x["score"], reverse=True)[:10]
    shorts = sorted([s for s in all_signals if s["type"] == "SHORT"], key=lambda x: x["score"], reverse=True)[:10]

    header = "🚀 <b>TOP 10 FUTURE (1H)</b>" if category == "FUTURE" else "💎 <b>TOP 10 SPOT (4H)</b>"
    msg = f"{header}\n\n"

    if longs:
        msg += "🟢 <b>أفضل LONG:</b>\n"
        for i, s in enumerate(longs, 1):
            msg += f"{i}. <a href='{s['link']}'>{s['symbol']}</a> - 🔥 {s['score']}/10\n"
    
    if shorts and category == "FUTURE":
        msg += "\n🔴 <b>أفضل SHORT:</b>\n"
        for i, s in enumerate(shorts, 1):
            msg += f"{i}. <a href='{s['link']}'>{s['symbol']}</a> - 🔥 {s['score']}/10\n"

    send_telegram(msg)

def main_logic_loop():
    last_spot_time = 0
    while True:
        try:
            # فحص الفيوتشر (كل ساعة)
            future_signals = []
            scan("SWAP", future_signals, min_score=6)
            send_top10(future_signals, "FUTURE")

            # فحص السبوت (كل 4 ساعات)
            current_time = time.time()
            if current_time - last_spot_time >= 14400:
                spot_signals = []
                scan("SPOT", spot_signals, min_score=7)
                send_top10(spot_signals, "SPOT")
                last_spot_time = current_time
            
            time.sleep(3600) 
        except: time.sleep(60)

def main():
    if not TOKEN: 
        print("Error: TELEGRAM_TOKEN not found!")
        return

    # --- السطر المطلوب لتنظيف الـ Webhook ومنع الـ Conflict ---
    requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
    
    # تشغيل المنطق في Thread منفصل
    threading.Thread(target=main_logic_loop, daemon=True).start()
    
    app = Application.builder().token(TOKEN).build()
    print("Bot Sniper is Online and Cleaning updates...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
