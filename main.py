import os
import asyncio
import requests
import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- الإعدادات الأساسية ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "5523662724")
STABLE_COINS = ['USDC', 'FDUSD', 'DAI', 'TUSD', 'EUR', 'GBP', 'BUSD']

# --- أوامر الشات ---
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """الرد على أمر المساعدة فوراً"""
    help_text = "🤖 **بوت القناص V7 جاهز!**\n\n"
    help_text += "✅ البوت شغال دلوقتي بنظام مهام منظم.\n"
    help_text += "📊 التنبيهات بتوصلك آلياً أول ما السكور يعلى.\n"
    help_text += "🔗 اللينكات متعدلة عشان تفتح الشارت فوراً."
    await update.message.reply_text(help_text, parse_mode="HTML")

# --- وظائف المساعدة ---
def get_tv_link(symbol):
    clean_symbol = symbol.replace("-USDT", "USDT").replace("-SWAP", "")
    return f"https://www.tradingview.com/chart/index.html?symbol=OKX:{clean_symbol}"

async def get_btc_status():
    try:
        url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=1H&limit=20"
        res = requests.get(url, timeout=10).json().get('data', [])
        if not res: return "⚠️ غير معروف"
        curr_price = float(res[0][4])
        ma20 = sum([float(x[4]) for x in res]) / 20
        return "🟢 إيجابي" if curr_price > ma20 else "🔴 سلبي"
    except: return "⚠️ فحص BTC فشل"

# --- مهمة الفحص الدوري (تدار بواسطة البوت) ---
async def run_scanner(context: ContextTypes.DEFAULT_TYPE):
    """دالة الفحص اللي البوت هيشغلها كل ساعة تلقائياً"""
    try:
        btc_status = await get_btc_status()
        url_pairs = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
        pairs_data = requests.get(url_pairs, timeout=10).json().get('data', [])
        
        for p in pairs_data:
            symbol = p['instId']
            if "-USDT" not in symbol or any(s in symbol for s in STABLE_COINS): continue

            url_candles = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=1H&limit=50"
            res = requests.get(url_candles, timeout=10).json().get('data', [])
            if not res or len(res) < 30: continue
            
            df = pd.DataFrame(res, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'v2', 'v3', 'v4'])
            df['c'] = df['c'].astype(float)
            df['h'] = df['h'].astype(float)
            df['l'] = df['l'].astype(float)
            df = df.iloc[::-1]
            
            # حساب MA20
            df['ma20'] = df['c'].rolling(window=20).mean()
            curr_price = df['c'].iloc[-1]
            ma20 = df['ma20'].iloc[-1]
            
            # شرط التنبيه (مثال للسكور)
            if abs(curr_price - ma20) / ma20 < 0.012: # منطقة تجميع
                direction = "LONG" if curr_price > ma20 else "SHORT"
                stop_loss = round(df['l'].tail(3).min() * 0.988, 6) if direction == "LONG" else round(df['h'].tail(3).max() * 1.012, 6)
                
                msg = (
                    f"🟡 <b>BB Squeeze | {direction}</b>\n"
                    f"<code>{symbol}</code>\n"
                    f"💰 السعر: {curr_price}\n"
                    f"🎯 دخول: {curr_price}\n"
                    f"🛑 ستوب: {stop_loss}\n"
                    f"₿ BTC: {btc_status}\n"
                    f"🔗 <a href='{get_tv_link(symbol)}'>افتح الشارت (1H)</a>"
                )
                await context.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML", disable_web_page_preview=True)
            
            await asyncio.sleep(0.1) # سرعة الفحص
    except Exception as e:
        print(f"Error in scanner: {e}")

# --- تشغيل البوت ---
def main():
    if not TOKEN: return
    
    # 1. بناء التطبيق
    app = Application.builder().token(TOKEN).build()
    
    # 2. إضافة الأوامر
    app.add_handler(CommandHandler("help", help_command))
    
    # 3. جدولة مهمة الفحص (تشتغل فوراً ثم كل ساعة)
    if app.job_queue:
        app.job_queue.run_repeating(run_scanner, interval=3600, first=5)
    
    print("Sniper Bot V7 is starting...")
    
    # 4. حل مشكلة الـ Conflict نهائياً بالمسح الفوري للتحديثات المتراكمة
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
