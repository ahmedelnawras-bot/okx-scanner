import os
import time
import pandas as pd
import numpy as np
import requests
import threading
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- الإعدادات (تأكد من كتابة الاسم صح في Railway) ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "5523662724")

def send_telegram(message):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        requests.post(url, data=payload, timeout=10)
        time.sleep(1)
    except: pass

# --- الأوامر التفاعلية ---
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "📖 <b>دليل Sniper الجديد:</b>\n• سبوت (4H) 🟢\n• فيوتشر (1H) 🔴\n\n/top10 - قائمة التميز\n/help - الدليل"
    await update.message.reply_html(text)

async def top10_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html("🏆 <b>Top 10:</b>\n<i>جاري جمع البيانات...</i>")

# --- منطق المسح الدوري (الشرط المزدوج) ---
def scan_loop():
    while True:
        try:
            # هنا الكود بيحدد العنوان بناءً على الحالة:
            # 1. المزدوجة: 🔥 🔵 Vol Spike & 🟡 BB Squeeze
            # 2. فوليوم فقط: 🔵 Volume Spike
            # 3. ضغط فقط: 🟡 BB Squeeze
            
            # مثال لإرسال تنبيه مزدوج منظم:
            # msg = "<b>🔥 🔵 Vol Spike & 🟡 BB Squeeze | فيوتشر 🔴</b>\n..."
            # send_telegram(msg)
            
            time.sleep(3600) # فحص كل ساعة
        except: time.sleep(300)

def main():
    if not TOKEN:
        print("CRITICAL ERROR: TELEGRAM_TOKEN not found in environment variables!")
        return

    # بناء البوت مع استقبال الأوامر
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("top10", top10_command))
    
    # تشغيل المسح في الخلفية
    threading.Thread(target=scan_loop, daemon=True).start()
    
    print("Bot is starting successfully...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
