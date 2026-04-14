import os
import asyncio
import requests
import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- الإعدادات ---
TOKEN = os.getenv("TELEGRAM_TOKEN", "8626651293:AAEcwKRT8kw_271P0pyLokigad7cYjrp-8g")
CHAT_ID = os.getenv("CHAT_ID", "5523662724")
STABLE_COINS = ['USDC', 'FDUSD', 'DAI', 'TUSD', 'EUR', 'GBP', 'BUSD']

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 **بوت القناص V8 شغال!**\n\nأنا ببحث دلوقتي عن فرص.. أول ما الاقي سكور عالي هبعتلك فوراً.", parse_mode="HTML")

def get_tv_link(symbol):
    clean_symbol = symbol.replace("-USDT", "USDT").replace("-SWAP", "")
    return f"https://www.tradingview.com/chart/index.html?symbol=OKX:{clean_symbol}"

async def run_scanner_logic(bot):
    """منطق الفحص الدوري"""
    while True:
        try:
            url_pairs = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
            pairs_data = requests.get(url_pairs, timeout=10).json().get('data', [])
            
            for p in pairs_data:
                symbol = p['instId']
                if "-USDT" not in symbol or any(s in symbol for s in STABLE_COINS): continue

                url_candles = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=1H&limit=30"
                res = requests.get(url_candles, timeout=10).json().get('data', [])
                if not res or len(res) < 20: continue
                
                df = pd.DataFrame(res, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'v2', 'v3', 'v4'])
                df['c'] = df['c'].astype(float)
                df = df.iloc[::-1]
                df['ma20'] = df['c'].rolling(window=20).mean()
                
                curr_price = df['c'].iloc[-1]
                ma20 = df['ma20'].iloc[-1]
                
                if abs(curr_price - ma20) / ma20 < 0.01:
                    direction = "LONG" if curr_price > ma20 else "SHORT"
                    msg = f"🟡 <b>فرصة جديدة | {direction}</b>\n<code>{symbol}</code>\n💰 السعر: {curr_price}\n🔗 <a href='{get_tv_link(symbol)}'>افتح الشارت</a>"
                    await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML", disable_web_page_preview=True)
                await asyncio.sleep(0.1)
            await asyncio.sleep(3600)
        except Exception as e:
            print(f"Scanner Error: {e}")
            await asyncio.sleep(60)

async def post_init(application: Application):
    """تشغيل الفحص في الخلفية فور تشغيل البوت"""
    asyncio.create_task(run_scanner_logic(application.bot))

def main():
    if not TOKEN: return
    # استخدام post_init يغنينا عن الـ JobQueue لو فيها مشكلة في المكتبات
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler("help", help_command))
    
    print("Sniper Bot V8 is starting...")
    # حل مشكلة الـ Conflict نهائياً
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
