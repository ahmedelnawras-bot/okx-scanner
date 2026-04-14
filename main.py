import os
import asyncio
import requests
import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- الإعدادات ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "5523662724")
STABLE_COINS = ['USDC', 'FDUSD', 'DAI', 'TUSD', 'EUR', 'GBP', 'BUSD']

# --- أوامر البوت ---
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = "🤖 **بوت قناص العملات V6**\n\n"
    help_text += "✅ البوت يعمل الآن بنظام مستقر\n"
    help_text += "📊 التنبيهات تصلك آلياً عند وجود فرص قوية\n"
    await update.message.reply_text(help_text, parse_mode="HTML")

# --- وظائف التحليل ---
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

async def scanner_task(context: ContextTypes.DEFAULT_TYPE):
    """هذه الدالة تعمل كخلفية للفحص الدوري"""
    while True:
        try:
            btc_status = await get_btc_status()
            url_pairs = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
            pairs_data = requests.get(url_pairs, timeout=10).json().get('data', [])
            
            for p in pairs_data:
                symbol = p['instId']
                if "-USDT" not in symbol: continue
                if any(stable in symbol for stable in STABLE_COINS): continue

                url_candles = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=1H&limit=50"
                res = requests.get(url_candles, timeout=10).json().get('data', [])
                if not res or len(res) < 25: continue
                
                df = pd.DataFrame(res, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'v2', 'v3', 'v4'])
                df['c'] = df['c'].astype(float)
                df['h'] = df['h'].astype(float)
                df['l'] = df['l'].astype(float)
                df = df.iloc[::-1]
                
                # حساب المؤشرات
                df['ma20'] = df['c'].rolling(window=20).mean()
                curr_price = df['c'].iloc[-1]
                ma20 = df['ma20'].iloc[-1]
                
                # شرط الدخول (BB Squeeze بسيط كمثال)
                direction = "LONG" if curr_price > ma20 else "SHORT"
                score = 8 if abs(curr_price - ma20) / ma20 < 0.01 else 5 # سكور افتراضي
                
                if score >= 8:
                    stop_loss = round(df['l'].tail(3).min() * 0.985, 6) if direction == "LONG" else round(df['h'].tail(3).max() * 1.015, 6)
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
                
                await asyncio.sleep(0.1) # لمنع حظر الـ API
            
            await asyncio.sleep(3600) # فحص كل ساعة
        except Exception as e:
            print(f"Scanner Error: {e}")
            await asyncio.sleep(60)

# --- التشغيل الرئيسي ---
def main():
    if not TOKEN: return
    
    # بناء التطبيق بنظام الـ JobQueue لضمان عدم التعارض
    app = Application.builder().token(TOKEN).build()
    
    # إضافة الأوامر
    app.add_handler(CommandHandler("help", help_command))
    
    # إضافة مهمة الفحص في الخلفية
    if app.job_queue:
        app.job_queue.run_once(scanner_task, when=1)
    
    print("Bot V6 Gold is Starting...")
    # استخدام drop_pending_updates لحل مشكلة الـ Conflict نهائياً
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
