import requests
import os


BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")


def send_telegram_message(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ Telegram config missing")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }

    try:
        response = requests.post(url, json=payload, timeout=15)

        if response.status_code != 200:
            print("❌ Telegram HTTP Error:", response.text)
            return False

        data = response.json()

        if not data.get("ok"):
            print("❌ Telegram API Error:", data)
            return False

        return True

    except Exception as e:
        print("❌ Telegram Exception:", e)
        return False
