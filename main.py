import os
import time
import requests
import threading
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# جلب البيانات من إعدادات Railway
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "5523662724")

def send_telegram(message):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID, 
        "text": message, 
        "parse_mode": "HTML", 
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, data=payload, timeout=10)
        time.sleep(1) # لتجنب حظر تليجرام عند إرسال رسائل متتالية
    except Exception as e:
        print(f"Error sending to Telegram: {e}")

def get_bottom_coins():
    """وظيفة فحص العملات من OKX مع حماية كاملة"""
    # قائمة العملات (يمكنك زيادتها)
    symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "AVAX-USDT", "ADA-USDT"]
    results = []
    
    for symbol in symbols:
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=1H&limit=50"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200: continue
            
            res_json = response.json()
            # الحماية من KeyError: 'data'
            data = res_json.get('data', [])
            
            if not data: continue
            
            # السعر الحالي (الإغلاق)
            current_price = float(data[0][4])
            results.append(f"🟢 {symbol}: {current_price}")
            
        except Exception as e:
            print(f"🛑 خطأ في فحص {symbol}: {e}")
            continue
            
        time.sleep(0.3) # سرعة الفحص
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
            # رسالة التأكيد (ستصلك كل دقيقة)
            send_telegram("🔄 <b>بدء فحص السوق الآن...</b>")
            
            found_coins = get_bottom_coins()
            if found_coins:
                message = "🔍 <b>نتائج الفحص:</b>\n\n" + "\n".join(found_coins)
                send_telegram(message)
            else:
                send_telegram("⚠️ <b>فحص مكتمل:</b> لم يتم العثور على عملات مطابقة للشروط حالياً.")

            # تم تقليل الوقت لـ 60 ثانية للتجربة والتأكد
            time.sleep(60) 
            
        except Exception as e:
            print(f"Loop Error: {e}")
            time.sleep(30)

def main():
    if not TOKEN:
        print("CRITICAL ERROR: TELEGRAM_TOKEN not found!")
        return

    # تنظيف أي Webhook قديم لتجنب الـ Conflict
    try:
        requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
    except: pass
    
    time.sleep(2)

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("top10", top10_command))

    # تشغيل الفحص في Thread مستقل
    threading.Thread(target=scan_loop, daemon=True).start()

    print("Bot started successfully...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
