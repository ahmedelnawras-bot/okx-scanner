import os
import time
import requests
import threading
import asyncio
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
        time.sleep(1)
    except: pass

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "📖 <b>دليل Sniper:</b>\n• سبوت (4H) 🟢\n• فيوتشر (1H) 🔴\n\n/top10 - قائمة التميز\n/help - الدليل"
    await update.message.reply_html(text)

async def top10_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html("🏆 <b>Top 10:</b>\n<i>جاري جمع البيانات...</i>")

def scan_loop():
    while True:
        try:
            time.sleep(3600)
        except:
            time.sleep(300)

def main():
    if not TOKEN:
        print("CRITICAL ERROR: TELEGRAM_TOKEN not found!")
        return

    # مسح أي webhook قديم أولاً
    requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
    time.sleep(2)

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("top10", top10_command))

    threading.Thread(target=scan_loop, daemon=True).start()

    print("Bot started...")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES
    )

if __name__ == "__main__":
    main()
