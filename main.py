import os
import time
import requests
import threading
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "5523662724")

def send_telegram(message):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        requests.post(url, data=payload, timeout=10)
        time.sleep(1) # تأخير بسيط لتجنب سبام تليجرام
    except: pass

def get_bottom_coins():
    """وظيفة فحص العملات من OKX مع حماية من الأخطاء"""
    symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "AVAX-USDT", "ADA-USDT"] # أضف بقية العملات هنا
    results = []
    
    for symbol in symbols:
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=1H&limit=100"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200: continue
            
            res_json = response.json()
            # الحل الجذري لمشكلة الـ KeyError: 'data'
            data = res_json.get('data', [])
            
            if not data:
                print(f"⚠️ لا توجد بيانات للعملة {symbol}")
                continue
            
            # هنا تضع معادلة القاع الخاصة بك (مثال بسيط):
            # الكاندل عبارة عن [ts, open, high, low, close, vol, ...]
            current_price = float(data[0][4])
            results.append(f"✅ {symbol}: {current_price}")
            
        except Exception as e:
            print(f"🛑 خطأ في فحص {symbol}: {e}")
            continue # تخطي العملة اللي فيها مشكلة وكمل الباقي
            
        time.sleep(0.5) # تأخير بسيط عشان المنصة متعملش Block
    return results

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "📖 <b>دليل Sniper:</b>\n• سبوت (4H) 🟢\n• فيوتشر (1H) 🔴\n\n/top10 - قائمة التميز\n/help - الدليل"
    await update.message.reply_html(text)

async def top10_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html("🏆 <b>Top 10:</b>\n<i>جاري جمع البيانات...</i>")

def scan_loop():
    print("Bot is running... Scanning for potential bottom coins on OKX...")
    while True:
        try:
            found_coins = get_bottom_coins()
            if found_coins:
                message = "🔍 <b>نتائج الفحص الجديد:</b>\n\n" + "\n".join(found_coins)
                send_telegram(message)
            
            # انتظر ساعة قبل الفحص القادم (أو عدلها حسب رغبتك)
            time.sleep(3600) 
        except Exception as e:
            print(f"Loop Error: {e}")
            time.sleep(300)

def main():
    if not TOKEN:
        print("CRITICAL ERROR: TELEGRAM_TOKEN not found!")
        return

    # تنظيف الـ Webhook القديم لضمان عدم حدوث Conflict
    try:
        requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
    except: pass
    
    time.sleep(2)

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("top10", top10_command))

    # تشغيل حلقة الفحص في Thread منفصل
    threading.Thread(target=scan_loop, daemon=True).start()

    print("Bot started successfully...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
