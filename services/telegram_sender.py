import requests
from config import BOT_TOKEN, CHAT_ID


def send_telegram_message(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram config missing")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        response = requests.post(url, json=payload, timeout=15)
        data = response.json()

        if not data.get("ok"):
            print("Telegram send failed:", data)
            return False

        return True

    except Exception as e:
        print("Telegram error:", e)
        return False
