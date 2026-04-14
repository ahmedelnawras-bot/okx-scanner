import os
import time
import requests
import threading
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# الإعدادات الأساسية
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "5523662724")

def send_telegram(message):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, data=payload, timeout=10)
    except: pass

def get_bottom_coins():
    # قائمة مصغرة للتجربة السريعة
    symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
    results = []
    for symbol in symbols:
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=1H&limit=1"
        try:
            res = requests.get(url, timeout=10).json()
            data = res.get('data', [])
            if data:
                price = data[0][4]
                results.append(f"🟢 {symbol}: {price}")
        except: continue
    return results

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html("📖 دليل Sniper شغال!")

def scan_loop():
    while True:
        try:
            send_telegram("🔄 <b>بدء فحص السوق الآن...</b>")
            found = get_bottom_coins()
            if found:
                send_telegram("🔍 <b>النتائج:</b>\n" + "\n".join(found))
            time.sleep(60) # فحص كل دقيقة للتجربة
        except:
            time.sleep(10)

def main():
    if not TOKEN: return

    # --- الجزء الأهم لحل مشكلة Conflict ---
    print("Cleaning old connections...")
    try:
        # إجبار تليجرام على قطع أي اتصال قديم فوراً
        requests.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
        time.sleep(5) # انتظار التأكيد
    except: pass
    # ---------------------------------------

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("help", help_command))

    threading.Thread(target=scan_loop, daemon=True).start()

    print("Bot started successfully...")
    # إضافة ميزة إيقاف التحديثات المعلقة عند التشغيل
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
