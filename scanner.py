import requests
import time
import pandas as pd
import numpy as np

# --- الإعدادات الأساسية ---
TELEGRAM_TOKEN = "8626651293:AAGTVnwdW36qLsoZdmC2ngKoYUGMeYZyjsg"
CHAT_ID = "5523662724"
# قائمة العملات المستبعدة (العملات المستقرة وغيرها)
EXCLUDE = ["USDC","USDT","BUSD","TUSD","DAI","PYUSD","FDUSD","TRY","BRL","WIN","SHIB"]

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"})
    except Exception as e:
        print(f"Error sending Telegram message: {e}")

def get_candles(symbol, bar='1H', limit=100):
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
    try:
        r = requests.get(url).json()
        if "data" not in r or len(r["data"]) < 30:
            return None
        df = pd.DataFrame(r["data"], columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df = df.astype({"close": float, "vol": float, "high": float, "low": float, "open": float})
        # ترتيب البيانات من الأقدم للأحدث
        return df.iloc[::-1].reset_index(drop=True)
    except:
        return None

# 1. فلتر اتجاه البيتكوين (كتحذير استرشادي)
def get_btc_status():
    df_btc = get_candles("BTC-USDT", bar='4H', limit=30)
    if df_btc is None:
        return "غير معروف ⚠️"
    ma20 = df_btc["close"].rolling(20).mean().iloc[-1]
    current_price = df_btc["close"].iloc[-1]
    return "إيجابي ✅" if current_price > ma20 else "هابط (حذر) ⚠️"

# 2. جلب السيولة المفتوحة (Open Interest)
def get_oi_value(symbol):
    try:
        url = f"https://www.okx.com/api/v5/public/open-interest?instId={symbol}"
        r = requests.get(url).json()
        if "data" in r and len(r["data"]) > 0:
            # القيمة بالعملة المقابلة (عادة USDT في العقود الدائمة)
            return float(r["data"][0]["oiCcy"])
    except:
        return 0
    return 0

# 3. حساب مستويات الدخول، الستوب، والهدف
def get_levels(df):
    cp = df["close"].iloc[-1]
    # الستوب لوز تحت أقل سعر في آخر شمعتين بخصم بسيط للأمان
    recent_low = df["low"].iloc[-2:].min()
    sl = recent_low * 0.997 
    # الهدف ضعف المخاطرة (Risk:Reward 1:2)
    risk = cp - sl
    tp = cp + (risk * 2)
    return cp, sl, tp

def scan(inst_type):
    label = "فيوتشر 🔴" if inst_type == "SWAP" else "سبوت 🟢"
    btc_status = get_btc_status()
    
    # جلب قائمة العملات المتاحة
    try:
        url_tickers = f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}"
        tickers
