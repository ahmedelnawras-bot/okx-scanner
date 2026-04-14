import os
import time
import requests
import threading
import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- الإعدادات ---
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

def get_btc_status():
    try:
        url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=1H&limit=20"
        res = requests.get(url).json().get('data', [])
        if not res: return "⚠️ غير معروف"
        curr_price = float(res[0][4])
        ma20 = sum([float(x[4]) for x in res]) / 20
        return "🟢 إيجابي (صاعد)" if curr_price > ma20 else "🔴 سلبي (هابط)"
    except: return "⚠️ فحص BTC فشل"

def calculate_indicators(df):
    delta = df['c'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    df['ma20'] = df['c'].rolling(window=20).mean()
    return df

def scan(inst_type, all_signals, min_score, btc_status):
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
            df['h'] = df['h'].astype(float)
            df['l'] = df['l'].astype(float)
            df = df.iloc[::-1]
            df = calculate_indicators(df)
            
            curr_price = df['c'].iloc[-1]
            rsi = df['rsi'].iloc[-1]
            ma20 = df['ma20'].iloc[-1]
            
            # --- حساب الستوب لوس (Stop Loss) ---
            # في الـ Long: الستوب هو أقل سعر في آخر 3 شموع
            # في الـ Short: الستوب هو أعلى سعر في آخر 3 شموع
            low_3 = df['l'].tail(3).min()
            high_3 = df['h'].tail(3).max()
            
            score = 0
            if curr_price > ma20: score += 3
            if rsi < 35 or rsi > 65: score += 5
            score = min(score, 10)
            
            direction = "LONG" if curr_price > ma20 else "SHORT"
            stop_loss = round(low_3 * 0.99, 4) if direction == "LONG" else round(high_3 * 1.01, 4)
            ma_status = "فوق ✅" if curr_price > ma20 else "تحت ❌"
            link = get_tv_link(symbol, inst_type)

            alert_threshold = 8 if inst_type == "SWAP" else 9
            if score >= alert_threshold:
                msg = (
                    f"🟡 <b>BB Squeeze | {direction}</b>\n"
                    f"{symbol}\n"
                    f"💰 السعر: {curr_price}\n"
                    f"📊 RSI: {round(rsi, 1)} | MA20 {ma_status}\n"
                    f"🎯 دخول: {curr_price}\n"
                    f"🛑 ستوب: {stop_loss}\n"
                    f"🔥 التقييم: {score}/10\n"
                    f"₿ BTC: {btc_status}\n"
                    f"🔗 <a href='{link}'>افتح الشارت ({bar_frame})</a>"
                )
                send_telegram(msg)

            if score >= min_score:
                all_signals.append({"symbol": symbol, "score": score, "type": direction, "market": inst_type, "link": link})
            time.sleep(0.1)
    except: pass

# ... بقية الدوال (send_top10, main_logic_loop, main) تظل كما هي ...
