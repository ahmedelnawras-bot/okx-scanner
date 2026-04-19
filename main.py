import os
import sys
import time
import html
import json
import logging
import threading
import requests
import pandas as pd
import redis

from analysis.scoring import calculate_long_score, is_breakout
from analysis.backtest import build_deep_report
from tracking.performance import (
    register_trade,
    update_open_trades,
    get_winrate_summary,
    format_winrate_summary,
    get_period_summary,
    get_trade_summary,
    format_period_summary,
    calc_tp1,
    calc_tp2,
)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("okx-scanner")

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
REDIS_URL = os.getenv("REDIS_URL")

OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers"
OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
OKX_FUNDING_URL = "https://www.okx.com/api/v5/public/funding-rate"

SCAN_LIMIT = 200
TIMEFRAME = "15m"
HTF_TIMEFRAME = "1H"

FINAL_MIN_SCORE = 6.3
PRE_BREAKOUT_EXTRA_SCORE = 0.2
MAX_ALERTS_PER_RUN = 3

COOLDOWN_SECONDS = 3600
LOCAL_RECENT_SEND_SECONDS = 2700   # 45 دقيقة
GLOBAL_COOLDOWN_SECONDS = 300
COMMAND_POLL_INTERVAL = 3

MIN_24H_QUOTE_VOLUME = 1_000_000
NEW_LISTING_MAX_CANDLES = 50

TOP_MOMENTUM_PERCENT = 0.20
TOP_MOMENTUM_MIN_SCORE = 7.0
TOP_MOMENTUM_NEW_MIN_SCORE = 6.0

NEW_LISTING_MIN_VOL_RATIO = 1.8
NEW_LISTING_MIN_CANDLE_STRENGTH = 0.45
NEW_LISTING_MAX_PER_RUN = 1

PRE_BREAKOUT_LOOKBACK = 20
PRE_BREAKOUT_PROXIMITY_MIN = 0.965
PRE_BREAKOUT_VOLUME_SIGNIFICANCE = 1.20
PRE_BREAKOUT_RECENT_VOL_BARS = 3
PRE_BREAKOUT_BASELINE_VOL_BARS = 12

# Market state sampling
ALT_MARKET_SAMPLE_SIZE = 12
ALT_MARKET_MIN_VALID = 6
ALT_MARKET_TIMEFRAME = "1H"
ALT_MARKET_CANDLE_LIMIT = 60

SCAN_LOCK_KEY = "scan:running"
SCAN_LOCK_TTL = 300

TELEGRAM_OFFSET_KEY = "telegram:offset:long"
TELEGRAM_BOOTSTRAP_DONE_KEY = "telegram:bootstrap_done:long"
TELEGRAM_POLL_LOCK_KEY = "telegram:poll:lock:long"
TELEGRAM_POLL_LOCK_TTL = 10

# Economic news
NEWS_WINDOW_HOURS = 2
ECONOMIC_CALENDAR_URL = "https://www.tradingview.com/economic-calendar/"

# Admin / stats reset
ADMIN_CHAT_ID = str(CHAT_ID) if CHAT_ID else ""
STATS_RESET_TS_KEY = "stats:last_reset_ts:long"

# Candle cache
CANDLE_CACHE_TTL_15M = 25
CANDLE_CACHE_TTL_1H = 90
CANDLE_CACHE_TTL_DEFAULT = 20

# Alt snapshot cache
ALT_SNAPSHOT_CACHE_KEY = "cache:alt_snapshot"
ALT_SNAPSHOT_CACHE_TTL = 600  # 10 دقايق

# =========================
# REDIS
# =========================
r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        logger.info("✅ Redis connected")
    except Exception as e:
        logger.error(f"❌ Redis connection error: {e}")
        r = None
else:
    logger.warning("⚠️ REDIS_URL not found")

# =========================
# LOCAL CACHE
# =========================
sent_cache = {}
last_candle_cache = {}
last_global_send_ts = 0.0


def clean_symbol_for_message(symbol: str) -> str:
    return symbol.replace("-SWAP", "")


def get_same_candle_key(symbol: str, candle_time: int, signal_type: str = "long") -> str:
    return f"sent:{signal_type}:{symbol}:{candle_time}"


def get_symbol_cooldown_key(symbol: str, signal_type: str = "long") -> str:
    clean = clean_symbol_for_message(symbol)
    return f"cooldown:{signal_type}:{clean}"


def already_sent_same_candle(symbol: str, candle_time: int, signal_type: str = "long") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_same_candle_key(symbol, candle_time, signal_type)))
    except Exception as e:
        logger.error(f"Redis same candle exists error: {e}")
        return False


def is_symbol_on_cooldown(symbol: str, signal_type: str = "long") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_symbol_cooldown_key(symbol, signal_type)))
    except Exception as e:
        logger.error(f"Redis symbol cooldown exists error: {e}")
        return False


def reserve_signal_slot(symbol: str, candle_time: int, signal_type: str = "long") -> bool:
    if not r:
        return True

    same_candle_key = get_same_candle_key(symbol, candle_time, signal_type)
    cooldown_key = get_symbol_cooldown_key(symbol, signal_type)

    try:
        same_candle_ok = r.set(same_candle_key, "1", ex=7200, nx=True)
        if not same_candle_ok:
            return False

        cooldown_ok = r.set(cooldown_key, "1", ex=COOLDOWN_SECONDS, nx=True)
        if not cooldown_ok:
            try:
                r.delete(same_candle_key)
            except Exception:
                pass
            return False

        return True

    except Exception as e:
        logger.error(f"Redis reserve error: {e}")
        return False


def release_signal_slot(symbol: str, candle_time: int, signal_type: str = "long") -> None:
    if not r:
        return
    try:
        r.delete(get_same_candle_key(symbol, candle_time, signal_type))
        r.delete(get_symbol_cooldown_key(symbol, signal_type))
    except Exception as e:
        logger.error(f"Redis release error: {e}")


def acquire_scan_lock() -> bool:
    if not r:
        return True
    try:
        locked = r.set(SCAN_LOCK_KEY, "1", ex=SCAN_LOCK_TTL, nx=True)
        return bool(locked)
    except Exception as e:
        logger.error(f"Scan lock acquire error: {e}")
        return False


def release_scan_lock() -> None:
    if not r:
        return
    try:
        r.delete(SCAN_LOCK_KEY)
    except Exception as e:
        logger.error(f"Scan lock release error: {e}")


def is_global_cooldown_active() -> bool:
    if not r:
        return False
    try:
        return bool(r.exists("global_cooldown"))
    except Exception:
        return False


def set_global_cooldown() -> None:
    if not r:
        return
    try:
        r.set("global_cooldown", "1", ex=GLOBAL_COOLDOWN_SECONDS)
    except Exception:
        pass


# =========================
# ECONOMIC CALENDAR
# =========================
def get_upcoming_high_impact_events(window_hours: int = NEWS_WINDOW_HOURS) -> list:
    try:
        now = int(time.time())
        window_end = now + (window_hours * 3600)

        url = "https://economic-calendar.tradingview.com/events"
        params = {
            "from": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)),
            "to": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(window_end)),
            "countries": "US",
        }

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.tradingview.com/",
        }

        res = requests.get(url, params=params, headers=headers, timeout=10)

        if res.status_code != 200:
            logger.warning(f"Economic calendar HTTP {res.status_code}")
            return []

        data = res.json()
        events = data if isinstance(data, list) else data.get("result", [])

        high_impact = []
        for event in events:
            importance = str(event.get("importance", "")).lower()
            if importance in ("high", "3", "-1"):
                event_link = (
                    event.get("url")
                    or event.get("link")
                    or event.get("source_url")
                    or event.get("event_url")
                    or ECONOMIC_CALENDAR_URL
                )

                high_impact.append({
                    "title": event.get("title", "Unknown Event"),
                    "date": event.get("date", ""),
                    "country": event.get("country", ""),
                    "impact": "High",
                    "link": event_link,
                })

        logger.info(f"Economic calendar: {len(high_impact)} high-impact events in next {window_hours}h")
        return high_impact

    except Exception as e:
        logger.warning(f"Economic calendar error: {e}")
        return []


def format_news_warning(events: list) -> str:
    calendar_link = html.escape(ECONOMIC_CALENDAR_URL, quote=True)

    if not events:
        return f'📰 <b>الأخبار:</b> لا توجد أخبار High-Impact قريبة | <a href="{calendar_link}">Economic Calendar</a>'

    parts = []
    for event in events[:2]:
        title = html.escape(event.get("title", "Unknown Event"))
        link = html.escape(event.get("link", ECONOMIC_CALENDAR_URL), quote=True)
        parts.append(f'<a href="{link}">{title}</a>')

    events_text = " | ".join(parts)
    return f'📰 <b>الأخبار:</b> {events_text} | <a href="{calendar_link}">Economic Calendar</a>'


# =========================
# TELEGRAM OFFSET (Redis-persisted)
# =========================
def get_telegram_offset() -> int:
    if not r:
        return 0
    try:
        val = r.get(TELEGRAM_OFFSET_KEY)
        return int(val) if val else 0
    except Exception:
        return 0


def save_telegram_offset(offset: int) -> None:
    if not r:
        return
    try:
        r.set(TELEGRAM_OFFSET_KEY, str(offset))
    except Exception:
        pass


def is_telegram_bootstrap_done() -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(TELEGRAM_BOOTSTRAP_DONE_KEY))
    except Exception:
        return False


def mark_telegram_bootstrap_done() -> None:
    if not r:
        return
    try:
        r.set(TELEGRAM_BOOTSTRAP_DONE_KEY, "1")
    except Exception:
        pass


def acquire_telegram_poll_lock() -> bool:
    if not r:
        return True
    try:
        locked = r.set(TELEGRAM_POLL_LOCK_KEY, "1", ex=TELEGRAM_POLL_LOCK_TTL, nx=True)
        return bool(locked)
    except Exception as e:
        logger.error(f"Telegram poll lock acquire error: {e}")
        return False


def release_telegram_poll_lock() -> None:
    if not r:
        return
    try:
        r.delete(TELEGRAM_POLL_LOCK_KEY)
    except Exception as e:
        logger.error(f"Telegram poll lock release error: {e}")


# =========================
# TELEGRAM
# =========================
def clear_webhook() -> None:
    if not BOT_TOKEN:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
    params = {"drop_pending_updates": False}

    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            logger.info("Telegram webhook cleared")
        else:
            logger.error(f"Webhook clear HTTP error: {response.text}")
    except Exception as e:
        logger.error(f"Webhook clear error: {e}")


def send_telegram_message(message: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        logger.error("❌ Telegram config missing")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=15)

        if response.status_code != 200:
            logger.error(f"❌ Telegram HTTP Error: {response.text}")
            return False

        data = response.json()
        if not data.get("ok"):
            logger.error(f"❌ Telegram API Error: {data}")
            return False

        return True

    except Exception as e:
        logger.error(f"❌ Telegram Exception: {e}")
        return False


def send_telegram_reply(chat_id: str, message: str) -> bool:
    if not BOT_TOKEN or not chat_id:
        logger.error("❌ Telegram reply config missing")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=15)

        if response.status_code != 200:
            logger.error(f"❌ Telegram reply HTTP Error: {response.text}")
            return False

        data = response.json()
        if not data.get("ok"):
            logger.error(f"❌ Telegram reply API Error: {data}")
            return False

        return True

    except Exception as e:
        logger.error(f"❌ Telegram reply Exception: {e}")
        return False


def get_telegram_updates(offset: int = 0):
    if not BOT_TOKEN:
        return []

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {
        "timeout": 1,
        "offset": offset,
    }

    try:
        response = requests.get(url, params=params, timeout=5)

        if response.status_code != 200:
            logger.error(f"❌ getUpdates HTTP Error: {response.text}")
            return []

        data = response.json()
        if not data.get("ok"):
            logger.error(f"❌ getUpdates API Error: {data}")
            return []

        return data.get("result", [])

    except Exception as e:
        logger.error(f"❌ getUpdates Exception: {e}")
        return []


TELEGRAM_COMMANDS = {
    "/help": "عرض كل الأوامر المتاحة",
    "/how_it_work": "شرح طريقة عمل البوت",
    "/report_1h": "آخر ساعة",
    "/report_today": "اليوم",
    "/report_all": "كل الصفقات",
    "/report_deep": "تحليل متقدم للأداء",
    "/reset_stats": "تصفير نتائج البوت",
    "/stats_since_reset": "الأداء من بعد آخر تصفير",
}


def build_report_message(period: str) -> str:
    title_map = {
        "1h": "Report - Last 1H",
        "today": "Report - Today",
        "all": "Report - All Time",
    }

    summary = get_period_summary(
        redis_client=r,
        period=period,
        market_type="futures",
        side="long",
    )

    return format_period_summary(title_map.get(period, "Report"), summary)


def build_deep_report_message() -> str:
    try:
        return build_deep_report(r, market_type="futures", side="long")
    except Exception as e:
        logger.error(f"build_deep_report error: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"


def build_help_message() -> str:
    report_commands = []
    other_commands = []

    for command, description in TELEGRAM_COMMANDS.items():
        if command.startswith("/report"):
            report_commands.append(f"{command} - {description}")
        elif command != "/help":
            other_commands.append(f"{command} - {description}")

    lines = [
        "🤖 <b>OKX Scanner Bot</b>",
        "",
        "📊 <b>التقارير:</b>",
        *report_commands,
        "",
        "⚙️ <b>معلومات:</b>",
        "• البوت بيبعت إشارات Long Futures",
        "• مبني على Volume + Breakout + MTF",
    ]

    if other_commands:
        lines.extend([
            "",
            "📚 <b>أوامر إضافية:</b>",
            *other_commands,
        ])

    lines.extend([
        "",
        "🔥 <b>نصيحة:</b>",
        "استخدم /report_today لمتابعة الأداء",
    ])

    return "\n".join(lines)


def build_how_it_work_message() -> str:
    return """📘 <b>كيف يعمل البوت؟</b>

🤖 <b>فكرة البوت:</b>
البوت متخصص في البحث عن فرص <b>Long Futures</b> على منصة OKX،
ويعتمد على فلترة تدريجية لتقليل الإشارات الضعيفة ورفع جودة التنبيه.

🔍 <b>منطق العمل:</b>
1. اختيار العملات الأعلى سيولة وحجم تداول
2. تحليل الشموع على فريم <b>15 دقيقة</b>
3. تقييم عدة عوامل:
• الزخم
• الفوليوم
• موقع السعر بالنسبة للمتوسط
• الاختراق أو الزخم قبل الاختراق
• تأكيد فريم الساعة (1H)
• حالة السوق العامة
• قوة سوق الألت نفسها من السوق الفعلي، وليس من BTC فقط
4. إعطاء كل فرصة <b>Score من 10</b>
5. إرسال فقط الفرص التي تتجاوز الشروط النهائية

📊 <b>متى تعتبر الإشارة قوية؟</b>
• فوليوم أعلى من الطبيعي
• شمعة صاعدة نظيفة
• RSI في منطقة صحية
• اختراق أو زخم واضح
• توافق مع السوق العام

⚠️ <b>متى تكون الإشارة فيها مخاطرة؟</b>
• الحركة متأخرة نسبيًا
• RSI عالي جدًا (تشبع شراء)
• السوق ضعيف أو غير واضح
• فوليوم غير كافٍ
• وجود أخبار اقتصادية قريبة

🧠 <b>شرح رسالة البوت:</b>

🚀 <b>العملة</b>
الزوج الذي تم رصد الإشارة عليه

💰 <b>السعر</b>
سعر الدخول التقريبي

⭐ <b>السكور</b>
تقييم من 10
كلما زاد → جودة أعلى، لكن ليس ضمان ربح

🎯 <b>TP1 / TP2</b>
أهداف الربح
TP1 = هدف أقرب
TP2 = هدف أبعد

🛑 <b>SL</b>
مستوى وقف الخسارة

🧠 <b>نوع الفرصة</b>
يشرح شكل الفرصة مثل:
• Breakout مبكر
• استمرار
• Pullback

🌍 <b>السوق</b>
ملخص سريع يجمع:
• حالة الألت
• اتجاه BTC

📊 <b>أسباب الدخول</b>
أهم النقاط الإيجابية التي جعلت البوت يرسل الإشارة

⚠️ <b>ملاحظات</b>
أي تنبيهات أو نقاط تستحق الحذر

📰 <b>الأخبار</b>
تنويه مختصر لو في أخبار اقتصادية High-Impact قريبة

📍 <b>الدخول</b>
توقيت الدخول بالنسبة للحركة:
• 🟢 مبكر (بداية الحركة)
• 🟡 متوسط (نص الحركة)
• 🔴 متأخر (قرب النهاية)

⚖️ <b>المخاطرة</b>
تقييم عام للفرصة:
• منخفضة
• متوسطة
• عالية

🔗 <b>الشارت</b>
لينك مباشر على TradingView لمراجعة الشارت بسرعة

📌 <b>مهم جدًا:</b>
• البوت أداة مساعدة وليس قرار نهائي
• لا تعتمد عليه بدون مراجعة الشارت
• الجودة أهم من عدد الإشارات
• السوق دائمًا له الكلمة الأخيرة

✅ <b>أفضل استخدام:</b>
استخدم البوت كفلتر ذكي يوفر وقتك،
ثم خذ القرار بعد مراجعة سريعة للسوق والشارت."""
    

def reset_stats(chat_id: str):
    if ADMIN_CHAT_ID and str(chat_id) != ADMIN_CHAT_ID:
        send_telegram_reply(chat_id, "⛔ غير مسموح")
        return

    if not r:
        send_telegram_reply(chat_id, "❌ Redis غير متصل")
        return

    try:
        deleted = 0
        for key in r.scan_iter("trade:futures:long:*"):
            r.delete(key)
            deleted += 1

        try:
            r.delete("open_trades:futures:long")
            r.delete("stats:futures:long")
        except Exception:
            pass

        reset_ts = int(time.time())
        r.set(STATS_RESET_TS_KEY, str(reset_ts))

        send_telegram_reply(
            chat_id,
            f"🧹 تم تصفير بيانات اللونج بنجاح\n"
            f"📊 عدد المفاتيح المحذوفة: {deleted}\n"
            f"🕒 وقت التصفير: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(reset_ts))}"
        )

        logger.info(f"RESET LONG STATS → deleted keys: {deleted} | reset_ts={reset_ts}")

    except Exception as e:
        logger.error(f"Reset stats error: {e}")
        send_telegram_reply(chat_id, "❌ حصل خطأ أثناء التصفير")


def stats_since_reset(chat_id: str):
    if not r:
        send_telegram_reply(chat_id, "❌ Redis غير متصل")
        return

    try:
        reset_ts_raw = r.get(STATS_RESET_TS_KEY)
        if not reset_ts_raw:
            send_telegram_reply(chat_id, "ℹ️ لا يوجد Reset مسجل بعد")
            return

        reset_ts = int(reset_ts_raw)
        reset_time_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reset_ts))

        summary = get_trade_summary(
            redis_client=r,
            market_type="futures",
            side="long",
            since_ts=reset_ts,
        )

        header = f"📊 <b>Stats Since Reset</b>\n🕒 منذ: {html.escape(reset_time_text)}\n\n"
        body = format_period_summary("Since Reset", summary)

        send_telegram_reply(chat_id, header + body)
        logger.info(f"STATS SINCE RESET → reset_ts={reset_ts}")

    except Exception as e:
        logger.error(f"stats_since_reset error: {e}")
        send_telegram_reply(chat_id, "❌ حصل خطأ أثناء جلب التقرير")


COMMAND_HANDLERS = {
    "/help": lambda chat_id: send_telegram_reply(chat_id, build_help_message()),
    "/how_it_work": lambda chat_id: send_telegram_reply(chat_id, build_how_it_work_message()),
    "/report_1h": lambda chat_id: send_telegram_reply(chat_id, build_report_message("1h")),
    "/report_today": lambda chat_id: send_telegram_reply(chat_id, build_report_message("today")),
    "/report_all": lambda chat_id: send_telegram_reply(chat_id, build_report_message("all")),
    "/report_deep": lambda chat_id: send_telegram_reply(chat_id, build_deep_report_message()),
    "/reset_stats": lambda chat_id: reset_stats(chat_id),
    "/stats_since_reset": lambda chat_id: stats_since_reset(chat_id),
}


def bootstrap_telegram_offset_once():
    if is_telegram_bootstrap_done():
        return

    if not acquire_telegram_poll_lock():
        return

    try:
        updates = get_telegram_updates(offset=0)
        if updates:
            latest_offset = updates[-1]["update_id"] + 1
            save_telegram_offset(latest_offset)
            logger.info(f"Telegram bootstrap offset set to {latest_offset}")
        else:
            logger.info("Telegram bootstrap: no pending updates")

        mark_telegram_bootstrap_done()
    except Exception as e:
        logger.error(f"Telegram bootstrap error: {e}")
    finally:
        release_telegram_poll_lock()


def handle_telegram_commands():
    if not acquire_telegram_poll_lock():
        return

    try:
        offset = get_telegram_offset()
        updates = get_telegram_updates(offset=offset)

        latest_offset = offset

        for update in updates:
            try:
                latest_offset = update["update_id"] + 1

                message = update.get("message") or {}
                text = (message.get("text") or "").strip()
                chat = message.get("chat") or {}
                chat_id = str(chat.get("id", ""))

                if not text or not chat_id:
                    continue

                command = text.split()[0].split("@")[0]

                handler = COMMAND_HANDLERS.get(command)
                if handler:
                    handler(chat_id)

            except Exception as e:
                logger.error(f"handle_telegram_commands error: {e}")

        if latest_offset != offset:
            save_telegram_offset(latest_offset)

    finally:
        release_telegram_poll_lock()


# =========================
# OKX DATA
# =========================
def is_excluded_symbol(symbol: str) -> bool:
    excluded_prefixes = ("USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP", "USD0")
    if symbol.startswith(excluded_prefixes):
        return True

    base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "")
    if len(base) > 20:
        return True

    return False


def extract_24h_quote_volume(ticker: dict) -> float:
    fields = ["volCcy24h", "turnover24h", "quoteVolume", "vol24h"]
    for field in fields:
        value = ticker.get(field)
        if value is None:
            continue
        try:
            return float(value)
        except Exception:
            continue
    return 0.0


def extract_24h_change_percent(ticker: dict) -> float:
    try:
        last = float(ticker.get("last", 0) or 0)

        prev = ticker.get("prevPx")
        if prev is not None:
            prev = float(prev)
            if prev > 0:
                return round(((last - prev) / prev) * 100, 2)

        open24h = ticker.get("open24h")
        if open24h is not None:
            open24h = float(open24h)
            if open24h > 0:
                return round(((last - open24h) / open24h) * 100, 2)

        sod = ticker.get("sodUtc0") or ticker.get("sodUtc8")
        if sod is not None:
            sod = float(sod)
            if sod > 0:
                return round(((last - sod) / sod) * 100, 2)

        return 0.0
    except Exception:
        return 0.0


def get_ranked_pairs():
    try:
        res = requests.get(
            OKX_TICKERS_URL,
            params={"instType": "SWAP"},
            timeout=20,
        ).json()

        data = res.get("data", [])
        logger.info(f"Fetched {len(data)} futures pairs")

        filtered = []
        for item in data:
            symbol = item.get("instId", "")

            if "USDT" not in symbol:
                continue
            if not symbol.endswith("-SWAP"):
                continue
            if is_excluded_symbol(symbol):
                continue

            vol_24h = extract_24h_quote_volume(item)
            if vol_24h < MIN_24H_QUOTE_VOLUME:
                continue

            item["_rank_volume_24h"] = vol_24h
            filtered.append(item)

        filtered.sort(key=lambda x: x.get("_rank_volume_24h", 0), reverse=True)
        top = filtered[:SCAN_LIMIT]

        logger.info(f"After liquidity filter: {len(filtered)}")
        logger.info(f"Using top ranked pairs: {len(top)}")

        return top

    except Exception as e:
        logger.error(f"get_ranked_pairs error: {e}")
        return []


def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    avg_gain = gain.ewm(com=period - 1, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def compute_atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def to_dataframe(data):
    if not data:
        return None

    df = pd.DataFrame(data, columns=[
        "ts", "open", "high", "low", "close", "volume",
        "volCcy", "volCcyQuote", "confirm"
    ])

    numeric_cols = [
        "ts", "open", "high", "low", "close",
        "volume", "volCcy", "volCcyQuote", "confirm"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("ts").reset_index(drop=True)
    df["ma"] = df["close"].rolling(20).mean()
    df["rsi"] = compute_rsi(df["close"])
    df["atr"] = compute_atr(df)

    return df


def get_candle_cache_key(symbol: str, timeframe: str, limit: int) -> str:
    return f"candles:{symbol}:{timeframe}:{limit}"


def get_candle_cache_ttl(timeframe: str) -> int:
    tf = str(timeframe).strip().lower()
    if tf == "15m":
        return CANDLE_CACHE_TTL_15M
    if tf == "1h":
        return CANDLE_CACHE_TTL_1H
    return CANDLE_CACHE_TTL_DEFAULT


def get_candles(symbol, timeframe="15m", limit=100):
    cache_key = get_candle_cache_key(symbol, timeframe, limit)
    cache_ttl = get_candle_cache_ttl(timeframe)

    if r:
        try:
            cached = r.get(cache_key)
            if cached:
                data = json.loads(cached)
                if isinstance(data, list) and data:
                    logger.debug(f"{symbol} {timeframe} → candles cache hit")
                    return data
        except Exception as e:
            logger.warning(f"Candle cache read error on {symbol} {timeframe}: {e}")

    try:
        params = {"instId": symbol, "bar": timeframe, "limit": limit}
        res = requests.get(OKX_CANDLES_URL, params=params, timeout=20).json()
        data = res.get("data", [])

        if data and r:
            try:
                r.set(cache_key, json.dumps(data), ex=cache_ttl)
            except Exception as e:
                logger.warning(f"Candle cache write error on {symbol} {timeframe}: {e}")

        return data
    except Exception as e:
        logger.error(f"get_candles error on {symbol} {timeframe}: {e}")
        return []


def get_funding_rate(symbol):
    try:
        params = {"instId": symbol}
        res = requests.get(OKX_FUNDING_URL, params=params, timeout=10).json()
        data = res.get("data", [])
        if not data:
            return 0.0
        return float(data[0].get("fundingRate", 0))
    except Exception as e:
        logger.error(f"Funding rate error on {symbol}: {e}")
        return 0.0


def get_signal_row(df):
    try:
        if "confirm" not in df.columns or len(df) < 2:
            return df.iloc[-2]

        last = df.iloc[-1]
        if str(int(float(last["confirm"]))) == "1":
            return last

        return df.iloc[-2]
    except Exception:
        return df.iloc[-2]


def get_signal_candle_time(df):
    try:
        signal_row = get_signal_row(df)
        ts = int(signal_row["ts"])
        if ts > 10_000_000_000:
            return ts // 1000
        return ts
    except Exception:
        return int(time.time() // (15 * 60))


# =========================
# STRATEGY HELPERS
# =========================
def early_bullish_signal(df):
    try:
        if df is None or df.empty or len(df) < 25:
            return False

        signal_row = get_signal_row(df)
        signal_idx = signal_row.name

        if signal_idx is None or signal_idx < 1:
            return False

        last = df.iloc[signal_idx]
        prev = df.iloc[signal_idx - 1]

        score = 0
        if float(last["close"]) > float(last["open"]):
            score += 1
        if "rsi" in df.columns and float(last["rsi"]) > 50:
            score += 1
        if float(last["volume"]) > float(prev["volume"]):
            score += 1

        return score >= 2

    except Exception:
        return False


def is_higher_timeframe_confirmed(symbol):
    try:
        candles = get_candles(symbol, HTF_TIMEFRAME, 100)
        df = to_dataframe(candles)

        if df is None or df.empty or len(df) < 10:
            return False

        signal_row = get_signal_row(df)
        idx = signal_row.name

        if idx is None or idx < 3:
            return False

        checks = 0

        ma_value = signal_row.get("ma", None)
        if ma_value is not None and float(signal_row["close"]) > float(ma_value):
            checks += 1

        if float(signal_row.get("rsi", 0)) > 52:
            checks += 1

        last_3 = df.iloc[idx - 3:idx]
        green_candles = sum(
            1 for _, row in last_3.iterrows()
            if float(row["close"]) > float(row["open"])
        )
        if green_candles >= 2:
            checks += 1

        return checks >= 2

    except Exception as e:
        logger.error(f"MTF error on {symbol}: {e}")
        return False


def get_btc_mode():
    try:
        candles = get_candles("BTC-USDT-SWAP", "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return "🟡 محايد"

        signal_row = get_signal_row(df)
        ma_value = signal_row.get("ma", None)
        rsi_value = float(signal_row.get("rsi", 50))

        if ma_value is not None:
            if float(signal_row["close"]) > float(ma_value) and rsi_value >= 55:
                return "🟢 صاعد"
            if float(signal_row["close"]) < float(ma_value) and rsi_value <= 45:
                return "🔴 هابط"

        return "🟡 محايد"

    except Exception as e:
        logger.error(f"BTC mode error: {e}")
        return "🟡 محايد"


def get_btc_dominance_proxy(btc_mode: str) -> str:
    if "🔴 هابط" in btc_mode:
        return "🟢 داعم للألت"
    if "🟢 صاعد" in btc_mode:
        return "🔴 ضد الألت"
    return "🟡 محايد"


def get_alt_market_snapshot(ranked_pairs, sample_size=ALT_MARKET_SAMPLE_SIZE):
    try:
        if not ranked_pairs:
            return {
                "sample_size": 0,
                "valid_count": 0,
                "above_ma_ratio": 0.0,
                "rsi_support_ratio": 0.0,
                "positive_24h_ratio": 0.0,
                "alt_strength_score": 0.0,
                "alt_mode": "🟡 متماسك",
            }

        sampled = []
        for item in ranked_pairs:
            symbol = item.get("instId", "")
            if symbol == "BTC-USDT-SWAP":
                continue
            sampled.append(item)
            if len(sampled) >= sample_size:
                break

        valid = 0
        above_ma_count = 0
        rsi_support_count = 0
        positive_24h_count = 0

        for item in sampled:
            symbol = item.get("instId", "")
            change_24h = extract_24h_change_percent(item)

            candles = get_candles(symbol, ALT_MARKET_TIMEFRAME, ALT_MARKET_CANDLE_LIMIT)
            df = to_dataframe(candles)

            if df is None or df.empty or len(df) < 25:
                continue

            try:
                signal_row = get_signal_row(df)
                close = float(signal_row["close"])
                ma_value = float(signal_row.get("ma", 0) or 0)
                rsi_value = float(signal_row.get("rsi", 50) or 50)

                valid += 1

                if ma_value > 0 and close > ma_value:
                    above_ma_count += 1

                if rsi_value >= 52:
                    rsi_support_count += 1

                if change_24h > 0:
                    positive_24h_count += 1

            except Exception:
                continue

        if valid == 0:
            return {
                "sample_size": len(sampled),
                "valid_count": 0,
                "above_ma_ratio": 0.0,
                "rsi_support_ratio": 0.0,
                "positive_24h_ratio": 0.0,
                "alt_strength_score": 0.0,
                "alt_mode": "🟡 متماسك",
            }

        above_ma_ratio = round(above_ma_count / valid, 4)
        rsi_support_ratio = round(rsi_support_count / valid, 4)
        positive_24h_ratio = round(positive_24h_count / valid, 4)

        alt_strength_score = round(
            (above_ma_ratio * 0.45) +
            (rsi_support_ratio * 0.35) +
            (positive_24h_ratio * 0.20),
            4
        )

        if valid < ALT_MARKET_MIN_VALID:
            alt_mode = "🟡 متماسك"
        elif alt_strength_score >= 0.68 and above_ma_ratio >= 0.58 and rsi_support_ratio >= 0.50:
            alt_mode = "🟢 قوي"
        elif alt_strength_score >= 0.50:
            alt_mode = "🟡 متماسك"
        else:
            alt_mode = "🔴 ضعيف"

        snapshot = {
            "sample_size": len(sampled),
            "valid_count": valid,
            "above_ma_ratio": above_ma_ratio,
            "rsi_support_ratio": rsi_support_ratio,
            "positive_24h_ratio": positive_24h_ratio,
            "alt_strength_score": alt_strength_score,
            "alt_mode": alt_mode,
        }

        logger.info(
            f"ALT SNAPSHOT | valid={valid}/{len(sampled)} | "
            f"above_ma={above_ma_ratio:.2f} | rsi={rsi_support_ratio:.2f} | "
            f"pos24h={positive_24h_ratio:.2f} | strength={alt_strength_score:.2f} | "
            f"mode={alt_mode}"
        )

        return snapshot

    except Exception as e:
        logger.error(f"Alt market snapshot error: {e}")
        return {
            "sample_size": 0,
            "valid_count": 0,
            "above_ma_ratio": 0.0,
            "rsi_support_ratio": 0.0,
            "positive_24h_ratio": 0.0,
            "alt_strength_score": 0.0,
            "alt_mode": "🟡 متماسك",
        }


def get_market_state(btc_mode: str, alt_snapshot: dict):
    alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك")

    if "🔴 هابط" in btc_mode and "🔴 ضعيف" in alt_mode:
        return {
            "market_state": "risk_off",
            "market_state_label": "🔴 Risk-Off",
            "market_bias_label": "🔴 السوق ضعيف والسيولة دفاعية",
            "btc_dominance_proxy": "🔴 ضد الألت",
        }

    if "🟢 صاعد" in btc_mode and "🔴 ضعيف" in alt_mode:
        return {
            "market_state": "btc_leading",
            "market_state_label": "⚠️ BTC Leading",
            "market_bias_label": "🔴 BTC يقود والسيولة ليست في الألت",
            "btc_dominance_proxy": "🔴 ضد الألت",
        }

    if "🟢 صاعد" in btc_mode and "🟢 قوي" in alt_mode:
        return {
            "market_state": "bull_market",
            "market_state_label": "✅ Bull Market",
            "market_bias_label": "🟢 BTC والألت في توافق صاعد",
            "btc_dominance_proxy": "🟢 داعم للألت",
        }

    if ("🟡 محايد" in btc_mode or "🔴 هابط" in btc_mode) and "🟢 قوي" in alt_mode:
        return {
            "market_state": "alt_season",
            "market_state_label": "🔥 Alt Season",
            "market_bias_label": "🟢 الألت أقوى من BTC حالياً",
            "btc_dominance_proxy": "🟢 داعم للألت",
        }

    return {
        "market_state": "mixed",
        "market_state_label": "🟡 Mixed",
        "market_bias_label": "🟡 السوق مختلط والسيولة غير محسومة",
        "btc_dominance_proxy": "🟡 محايد",
    }


def get_dynamic_entry_threshold(
    market_state: str,
    score_result: dict,
    vol_ratio: float,
    mtf_confirmed: bool,
    is_new: bool,
) -> float:
    if market_state == "risk_off":
        threshold = 6.7
    elif market_state == "btc_leading":
        threshold = 6.8
    elif market_state == "mixed":
        threshold = 6.5
    elif market_state == "bull_market":
        threshold = 6.2
    elif market_state == "alt_season":
        threshold = 6.0
    else:
        threshold = 6.4

    if mtf_confirmed:
        threshold -= 0.1

    if vol_ratio >= 2.0:
        threshold -= 0.2
    elif vol_ratio >= 1.5:
        threshold -= 0.1

    if is_new:
        threshold += 0.2

    if score_result.get("fake_signal"):
        threshold += 0.2

    threshold = max(5.9, min(7.0, threshold))
    return round(threshold, 2)


def calculate_stop_loss(price, atr_value, signal_type="standard"):
    multipliers = {
        "breakout": 1.0,
        "pre_breakout": 1.5,
        "new_listing": 1.8,
        "standard": 1.2,
    }
    multiplier = multipliers.get(signal_type, 1.2)
    try:
        return round(float(price) - (float(atr_value) * multiplier), 6)
    except Exception:
        return round(float(price), 6)


def calculate_sl_percent(entry, sl):
    try:
        return round(((entry - sl) / entry) * 100, 2)
    except Exception:
        return 0.0


def is_new_listing_by_candles(candles) -> bool:
    try:
        return len(candles) < NEW_LISTING_MAX_CANDLES
    except Exception:
        return False


def build_tradingview_link(symbol):
    base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "").replace("-", "")
    tv_symbol = f"OKX:{base}USDT.P"
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"


def get_candle_strength_ratio(df) -> float:
    try:
        signal_row = get_signal_row(df)
        high = float(signal_row["high"])
        low = float(signal_row["low"])
        open_ = float(signal_row["open"])
        close = float(signal_row["close"])

        full = high - low
        if full <= 0:
            return 0.0

        body = abs(close - open_)
        return round(body / full, 4)
    except Exception:
        return 0.0


def get_volume_ratio(df) -> float:
    try:
        signal_row = get_signal_row(df)
        idx = signal_row.name
        if idx is None or idx < 1:
            return 1.0

        start_idx = max(0, idx - 20)
        avg_volume = float(df.iloc[start_idx:idx]["volume"].mean())
        last_volume = float(signal_row["volume"])

        if avg_volume <= 0:
            return 1.0

        return round(last_volume / avg_volume, 4)
    except Exception:
        return 1.0


def get_distance_from_ma_percent(df) -> float:
    try:
        signal_row = get_signal_row(df)
        close = float(signal_row["close"])
        ma_value = float(signal_row.get("ma", 0) or 0)
        if ma_value <= 0:
            return 0.0
        return round(((close - ma_value) / ma_value) * 100, 4)
    except Exception:
        return 0.0


def is_pre_breakout(df, lookback=PRE_BREAKOUT_LOOKBACK) -> bool:
    try:
        min_len = max(
            lookback + 6,
            PRE_BREAKOUT_BASELINE_VOL_BARS + PRE_BREAKOUT_RECENT_VOL_BARS + 2,
        )
        if df is None or df.empty or len(df) < min_len:
            return False

        signal_row = get_signal_row(df)
        idx = signal_row.name

        if idx is None or idx < max(lookback, PRE_BREAKOUT_BASELINE_VOL_BARS + PRE_BREAKOUT_RECENT_VOL_BARS):
            return False

        close = float(signal_row["close"])
        ma_value = float(signal_row.get("ma", close) or close)
        recent_high = float(df["high"].iloc[idx - lookback:idx].max())

        if recent_high <= 0 or close <= 0:
            return False

        proximity = close / recent_high
        if not (PRE_BREAKOUT_PROXIMITY_MIN <= proximity < 1.0):
            return False

        recent_vols = df["volume"].iloc[idx - PRE_BREAKOUT_RECENT_VOL_BARS:idx].astype(float).tolist()
        vol_increasing = (
            len(recent_vols) == PRE_BREAKOUT_RECENT_VOL_BARS
            and recent_vols[1] >= recent_vols[0]
            and recent_vols[2] >= recent_vols[1]
        )

        baseline_start = idx - (PRE_BREAKOUT_BASELINE_VOL_BARS + PRE_BREAKOUT_RECENT_VOL_BARS)
        baseline_end = idx - PRE_BREAKOUT_RECENT_VOL_BARS
        baseline_vols = df["volume"].iloc[baseline_start:baseline_end].astype(float)

        if baseline_vols.empty:
            return False

        recent_avg_vol = sum(recent_vols) / len(recent_vols)
        baseline_avg_vol = float(baseline_vols.mean())

        if baseline_avg_vol <= 0:
            return False

        volume_significant = recent_avg_vol >= baseline_avg_vol * PRE_BREAKOUT_VOLUME_SIGNIFICANCE

        recent_atr = float(signal_row.get("atr", 0) or 0)
        prev_atr = float(df["atr"].iloc[idx - 5:idx].mean() or 0)
        compressed = prev_atr > 0 and recent_atr > 0 and recent_atr < prev_atr * 0.90

        above_ma = close > ma_value

        return vol_increasing and volume_significant and compressed and above_ma

    except Exception:
        return False


def is_valid_candle_timing(df) -> bool:
    try:
        now = int(time.time())
        candle_seconds = 15 * 60

        last_completed_ts = (now // candle_seconds) * candle_seconds

        signal_row = get_signal_row(df)
        ts = int(signal_row["ts"])

        if ts > 10_000_000_000:
            ts = ts // 1000

        candle_age = last_completed_ts - ts
        return 0 <= candle_age <= (candle_seconds * 2)
    except Exception:
        return False


def passes_new_listing_filter(score: float, breakout: bool, vol_ratio: float, candle_strength: float) -> bool:
    checks = 0
    if breakout:
        checks += 1
    if vol_ratio >= NEW_LISTING_MIN_VOL_RATIO:
        checks += 1
    if candle_strength >= NEW_LISTING_MIN_CANDLE_STRENGTH:
        checks += 1
    if score >= TOP_MOMENTUM_NEW_MIN_SCORE:
        checks += 1
    return checks >= 3


def get_effective_min_score(is_new: bool) -> float:
    return TOP_MOMENTUM_NEW_MIN_SCORE if is_new else TOP_MOMENTUM_MIN_SCORE


def get_momentum_priority(score: float, breakout: bool, vol_ratio: float, is_new: bool, pre_breakout: bool = False) -> float:
    priority = float(score)

    if breakout:
        priority += 1.0
    elif pre_breakout:
        priority += 0.7

    if vol_ratio >= 2.0:
        priority += 1.0
    elif vol_ratio >= 1.5:
        priority += 0.5

    if is_new and vol_ratio >= NEW_LISTING_MIN_VOL_RATIO:
        priority += 0.5

    return round(priority, 2)


def get_candidate_bucket(candidate: dict) -> str:
    if candidate["is_new"] and candidate["breakout"]:
        return "new_breakout"
    if candidate.get("pre_breakout") and not candidate["breakout"]:
        return "pre_breakout"
    if candidate["breakout"]:
        return "breakout"
    if candidate["vol_ratio"] >= 2.0:
        return "volume"
    return "standard"


def apply_top_momentum_filter(candidates):
    if not candidates:
        return []

    strong_candidates = []
    for c in candidates:
        min_score = get_effective_min_score(c["is_new"])
        if c["score"] >= min_score:
            strong_candidates.append(c)

    if not strong_candidates:
        logger.info("Top momentum filter: no candidates above threshold")
        return []

    strong_candidates.sort(
        key=lambda x: (x["momentum_priority"], x["score"], x["rank_volume_24h"]),
        reverse=True,
    )

    top_n = max(3, int(len(strong_candidates) * TOP_MOMENTUM_PERCENT))
    filtered = strong_candidates[:top_n]

    final_candidates = []
    new_count = 0

    for c in filtered:
        if c["is_new"]:
            if new_count >= NEW_LISTING_MAX_PER_RUN:
                continue
            new_count += 1
        final_candidates.append(c)

    logger.info(
        f"Top momentum filter: kept {len(final_candidates)} of {len(strong_candidates)} "
        f"(from total {len(candidates)})"
    )

    return final_candidates


def diversify_candidates(candidates, max_alerts=3):
    if not candidates:
        return []

    buckets = {}
    for candidate in candidates:
        bucket = get_candidate_bucket(candidate)
        buckets.setdefault(bucket, []).append(candidate)

    for bucket in buckets:
        buckets[bucket].sort(
            key=lambda x: (x["momentum_priority"], x["score"], x["rank_volume_24h"]),
            reverse=True,
        )

    diversified = []
    used_patterns = set()

    for bucket_name in ["new_breakout", "pre_breakout", "breakout", "volume", "standard"]:
        if bucket_name not in buckets or not buckets[bucket_name]:
            continue

        candidate = buckets[bucket_name][0]
        pattern = (
            candidate["breakout"],
            candidate.get("pre_breakout", False),
            round(candidate["vol_ratio"], 1),
            candidate["is_new"],
        )

        if pattern not in used_patterns:
            diversified.append(candidate)
            used_patterns.add(pattern)

        if len(diversified) >= max_alerts:
            break

    if len(diversified) < max_alerts:
        remaining = []
        for items in buckets.values():
            remaining.extend(items)

        remaining.sort(
            key=lambda x: (x["momentum_priority"], x["score"], x["rank_volume_24h"]),
            reverse=True,
        )

        for candidate in remaining:
            if len(diversified) >= max_alerts:
                break

            pattern = (
                candidate["breakout"],
                candidate.get("pre_breakout", False),
                round(candidate["vol_ratio"], 1),
                candidate["is_new"],
            )

            if pattern in used_patterns:
                continue
            if candidate in diversified:
                continue

            diversified.append(candidate)
            used_patterns.add(pattern)

    logger.info(
        "Diversified selection: "
        + ", ".join(
            f"{c['symbol']}[{get_candidate_bucket(c)}|{c['momentum_priority']}]"
            for c in diversified
        )
    )

    return diversified[:max_alerts]


def normalize_reason(reason: str) -> str:
    mapping = {
        "RSI صحي": "RSI في منطقة صحية",
        "RSI جيد": "RSI جيد",
        "RSI صاعد بقوة": "RSI صاعد بقوة",
        "RSI مرتفع لكن بزخم": "RSI مرتفع بزخم",
        "RSI عالي": "RSI عالي (تشبع شراء)",
        "فوليوم داعم": "فوليوم داعم",
        "فوليوم قوي": "فوليوم قوي",
        "فوليوم انفجار": "فوليوم انفجاري",
        "فوق MA": "فوق المتوسط",
        "شمعة جيدة": "شمعة جيدة",
        "شمعة قوية": "شمعة قوية",
        "اختراق": "اختراق",
        "اختراق مبكر جداً": "اختراق مبكر",
        "اختراق متأخر": "اختراق متأخر",
        "اختراق قوي مؤكد": "اختراق قوي مؤكد",
        "تأكيد فريم الساعة": "تأكيد فريم الساعة",
        "BTC داعم": "BTC داعم",
        "BTC غير داعم": "BTC غير داعم",
        "هيمنة داعمة": "هيمنة داعمة للألت",
        "هيمنة ضد الألت": "هيمنة ضد الألت (ضغط على العملات)",
        "تمويل سلبي": "تمويل سلبي (داعم للشراء)",
        "تمويل إيجابي": "تمويل إيجابي (ضغط محتمل)",
        "عملة جديدة": "عملة جديدة",
        "بداية ترند مبكرة": "بداية ترند مبكرة",
        "زخم مبكر تحت المقاومة 🎯": "زخم مبكر تحت المقاومة 🎯",
        "بعيد عن MA (متأخر)": "بعيد عن المتوسط (دخول متأخر)",
        "ممتد زيادة": "ممتد زيادة",
        "أسفل المتوسط": "أسفل المتوسط",
        "رفض سعري علوي": "رفض سعري علوي",
        "أخبار اقتصادية مهمة قريبة": "أخبار اقتصادية مهمة قريبة",
    }
    return mapping.get(reason, reason)


def sort_reasons(reasons):
    priority = {
        "فوق المتوسط": 1,
        "بداية ترند مبكرة": 2,
        "زخم مبكر تحت المقاومة 🎯": 3,
        "اختراق": 4,
        "اختراق مبكر": 5,
        "اختراق قوي مؤكد": 6,
        "فوليوم داعم": 7,
        "فوليوم قوي": 8,
        "فوليوم انفجاري": 9,
        "شمعة جيدة": 10,
        "شمعة قوية": 11,
        "RSI في منطقة صحية": 12,
        "RSI جيد": 13,
        "RSI صاعد بقوة": 14,
        "RSI مرتفع بزخم": 15,
        "تأكيد فريم الساعة": 16,
        "BTC داعم": 17,
        "هيمنة داعمة للألت": 18,
        "تمويل سلبي (داعم للشراء)": 19,
        "عملة جديدة": 20,
        "RSI عالي (تشبع شراء)": 101,
        "أسفل المتوسط": 102,
        "بعيد عن المتوسط (دخول متأخر)": 103,
        "ممتد زيادة": 104,
        "اختراق متأخر": 105,
        "هيمنة ضد الألت (ضغط على العملات)": 106,
        "BTC غير داعم": 107,
        "تمويل إيجابي (ضغط محتمل)": 108,
        "رفض سعري علوي": 109,
        "أخبار اقتصادية مهمة قريبة": 110,
    }

    return sorted(reasons, key=lambda x: priority.get(x, 999))


def classify_reasons(reasons):
    warning_keywords = [
        "RSI عالي",
        "بعيد عن المتوسط",
        "بعيد عن MA",
        "ممتد",
        "اختراق متأخر",
        "هيمنة ضد",
        "أسفل المتوسط",
        "رفض سعري",
        "BTC غير داعم",
        "تمويل إيجابي",
        "أخبار اقتصادية",
    ]

    normalized = [normalize_reason(r) for r in reasons]

    bullish = []
    warnings = []

    for r in normalized:
        if any(k in r for k in warning_keywords):
            warnings.append(r)
        else:
            bullish.append(r)

    bullish = list(dict.fromkeys(bullish))
    warnings = list(dict.fromkeys(warnings))

    if "اختراق مبكر" in bullish and "اختراق" in bullish:
        bullish.remove("اختراق")

    if "اختراق قوي مؤكد" in bullish and "اختراق" in bullish:
        bullish.remove("اختراق")

    bullish = sort_reasons(bullish)
    warnings = sort_reasons(warnings)

    return bullish, warnings


def format_bullish_reasons(bullish):
    highlight_keywords = [
        "اختراق",
        "زخم مبكر",
        "فوليوم",
        "شمعة",
        "RSI",
    ]

    highlighted = []
    used = set()

    for kw in highlight_keywords:
        for r in bullish:
            if kw in r and r not in used:
                highlighted.append(r)
                used.add(r)
                break
        if len(highlighted) >= 2:
            break

    formatted = []
    for r in bullish:
        safe = html.escape(r)
        line = f"• {safe}"
        if r in highlighted:
            line = f"• <b>{safe}</b>"
        formatted.append(line)

    return "\n".join(formatted)


def classify_opportunity_type(breakout: bool, pre_breakout: bool, dist_ma: float, mtf_confirmed: bool) -> str:
    try:
        if pre_breakout and not breakout:
            return "Breakout مبكر"
        if breakout:
            return "Breakout"
        if dist_ma <= 1.2 and mtf_confirmed:
            return "Pullback"
        return "استمرار"
    except Exception:
        return "استمرار"


def classify_entry_timing(dist_ma: float, breakout: bool, pre_breakout: bool, vol_ratio: float) -> str:
    try:
        # breakout أو pre_breakout مبكر
        if (pre_breakout or breakout) and dist_ma <= 2.8 and vol_ratio >= 1.3:
            return "🟢 مبكر (بداية الحركة)"

        # breakout بس dist_ma أكبر شوية — لسه مقبول لو vol قوي
        if breakout and 2.8 < dist_ma <= 4.0 and vol_ratio >= 1.5:
            return "🟡 متوسط (نص الحركة)"

        # تحرك عادي في المنطقة المتوسطة
        if 2.8 < dist_ma <= 4.5 and vol_ratio >= 1.2:
            return "🟡 متوسط (نص الحركة)"

        return "🔴 متأخر (قرب النهاية)"
    except Exception:
        return "🟡 متوسط (نص الحركة)"


def get_base_risk_label(score_result: dict, warnings_count: int) -> str:
    risk_level = score_result.get("risk_level")
    if risk_level:
        return risk_level

    if warnings_count == 0:
        return "🟢 منخفضة"
    if warnings_count == 1:
        return "🟡 متوسطة"
    return "🔴 عالية"


def adjust_risk_with_entry_timing(base_risk: str, entry_timing: str) -> str:
    try:
        if "🔴 متأخر" in entry_timing:
            return "🔴 عالية"

        if "🟡 متوسط" in entry_timing and base_risk == "🟢 منخفضة":
            return "🟡 متوسطة"

        return base_risk
    except Exception:
        return base_risk


def build_market_summary(btc_mode: str, alt_mode: str) -> str:
    safe_alt = alt_mode if alt_mode else "🟡 متماسك"
    safe_btc = btc_mode if btc_mode else "🟡 محايد"
    return f"{safe_alt} | BTC: {safe_btc}"


def build_message(
    symbol,
    price,
    score_result,
    stop_loss,
    btc_mode,
    btc_dominance_proxy,
    tv_link,
    is_new,
    change_24h=0.0,
    market_state_label=None,
    market_bias_label=None,
    alt_mode=None,
    news_warning="",
    opportunity_type="استمرار",
    entry_timing="🟡 متوسط (نص الحركة)",
    display_risk="🟡 متوسطة",
):
    symbol_clean = clean_symbol_for_message(symbol)

    bullish, inferred_warnings = classify_reasons(score_result.get("reasons", []))

    explicit_warnings = [
        normalize_reason(w)
        for w in (score_result.get("warning_reasons") or [])
    ]

    warnings = explicit_warnings if explicit_warnings else inferred_warnings
    warnings = list(dict.fromkeys(warnings))
    warnings = sort_reasons(warnings)

    bullish_text = format_bullish_reasons(bullish) if bullish else "• زخم مبكر"
    warnings_text = "\n".join(f"• {html.escape(w)}" for w in warnings) if warnings else ""

    funding_text = score_result.get("funding_label", "🟡 محايد")
    signal_rating = score_result.get("signal_rating", "⚡ عادي")
    sl_pct = calculate_sl_percent(price, stop_loss)

    tp1 = calc_tp1(price, stop_loss)
    tp2 = calc_tp2(price, stop_loss)

    tp1_pct = round(((tp1 - price) / price) * 100, 2) if price else 0.0
    tp2_pct = round(((tp2 - price) / price) * 100, 2) if price else 0.0

    new_tag = "\n🆕 <b>عملة جديدة</b>" if is_new else ""

    safe_symbol = html.escape(symbol_clean)
    safe_market = html.escape(build_market_summary(btc_mode=btc_mode, alt_mode=alt_mode or "🟡 متماسك"))
    safe_funding = html.escape(funding_text)
    safe_rating = html.escape(signal_rating)
    safe_tv_link = html.escape(tv_link, quote=True)
    safe_opportunity_type = html.escape(opportunity_type)
    safe_entry_timing = html.escape(entry_timing)
    safe_display_risk = html.escape(display_risk)

    change_24h_text = f"{change_24h:+.2f}%"

    warnings_block = f"\n\n⚠️ <b>ملاحظات:</b>\n{warnings_text}" if warnings_text else ""
    news_block = f"\n\n{news_warning}" if news_warning else ""

    return f"""🚀 <b>لونج فيوتشر | {safe_symbol}</b>

💰 <b>السعر:</b> {price:.6f} | ⏱ <b>الفريم:</b> 15m
⭐ <b>السكور:</b> {score_result["score"]:.1f} / 10

🎯 <b>TP1:</b> {tp1:.6f} (+{tp1_pct}%)
🏁 <b>TP2:</b> {tp2:.6f} (+{tp2_pct}%)
🛑 <b>SL:</b> {stop_loss:.6f} (-{sl_pct}%)

🧠 <b>نوع الفرصة:</b> {safe_opportunity_type}

🌍 <b>السوق:</b> {safe_market}
💸 <b>التمويل:</b> {safe_funding}
📈 <b>تغير 24H:</b> {change_24h_text}{new_tag}

📊 <b>أسباب الدخول:</b>
{bullish_text}{warnings_block}{news_block}

📍 <b>الدخول:</b> {safe_entry_timing}
⚖️ <b>المخاطرة:</b> {safe_display_risk}

🏷 <b>التصنيف:</b> {safe_rating}
🔗 <a href="{safe_tv_link}">Open Chart (15m / 1H)</a>"""


def run_command_poller():
    bootstrap_telegram_offset_once()

    while True:
        try:
            handle_telegram_commands()
        except Exception as e:
            logger.error(f"Command poller error: {e}")

        time.sleep(COMMAND_POLL_INTERVAL)


def run_scanner_loop():
    global last_global_send_ts

    while True:
        scan_locked = False

        try:
            if is_global_cooldown_active():
                logger.info("GLOBAL COOLDOWN (Redis) — skipping scan")
                time.sleep(60)
                continue

            scan_locked = acquire_scan_lock()
            if not scan_locked:
                logger.info("Another scan is running — skipping")
                time.sleep(30)
                continue

            logger.info(f"RUN START | pid={os.getpid()} | ts={int(time.time())}")

            update_open_trades(r, market_type="futures", side="long", timeframe=TIMEFRAME)
            winrate_summary = get_winrate_summary(r, market_type="futures", side="long")
            logger.info(format_winrate_summary(winrate_summary))

            ranked_pairs = get_ranked_pairs()
            btc_mode = get_btc_mode()

            alt_snapshot = None
            if r:
                try:
                    cached_snapshot = r.get(ALT_SNAPSHOT_CACHE_KEY)
                    if cached_snapshot:
                        alt_snapshot = json.loads(cached_snapshot)
                        logger.info("ALT SNAPSHOT → loaded from cache")
                except Exception as e:
                    logger.warning(f"Alt snapshot cache read error: {e}")

            if alt_snapshot is None:
                alt_snapshot = get_alt_market_snapshot(ranked_pairs)
                if r:
                    try:
                        r.set(ALT_SNAPSHOT_CACHE_KEY, json.dumps(alt_snapshot), ex=ALT_SNAPSHOT_CACHE_TTL)
                    except Exception as e:
                        logger.warning(f"Alt snapshot cache write error: {e}")

            market_info = get_market_state(btc_mode, alt_snapshot)

            market_state = market_info["market_state"]
            market_state_label = market_info["market_state_label"]
            market_bias_label = market_info["market_bias_label"]
            btc_dominance_proxy = market_info["btc_dominance_proxy"]
            alt_mode = alt_snapshot.get("alt_mode", "🟡 متماسك")

            upcoming_events = get_upcoming_high_impact_events()
            has_high_impact_news = len(upcoming_events) > 0
            news_warning_text = format_news_warning(upcoming_events)

            if has_high_impact_news:
                logger.info(f"⚠️ High-impact events detected: {[e['title'] for e in upcoming_events]}")

            logger.info(
                f"MARKET STATE | btc={btc_mode} | alt={alt_mode} | "
                f"state={market_state_label} | flow={market_bias_label}"
            )

            tested = 0
            sent_count = 0
            sent_symbols_this_run = set()
            candidates = []
            candidates_symbols = set()

            for pair_data in ranked_pairs:
                tested += 1
                symbol = pair_data["instId"]
                change_24h = extract_24h_change_percent(pair_data)

                candles = get_candles(symbol, TIMEFRAME, 100)
                df = to_dataframe(candles)

                if df is None or df.empty:
                    continue

                if not is_valid_candle_timing(df):
                    logger.info(f"{symbol} → skipped (candle timing invalid)")
                    continue

                early_signal = early_bullish_signal(df)
                pre_breakout = is_pre_breakout(df)

                breakout = is_breakout(df)
                mtf_confirmed = is_higher_timeframe_confirmed(symbol)
                is_new = is_new_listing_by_candles(candles)
                funding = get_funding_rate(symbol)
                vol_ratio = get_volume_ratio(df)
                dist_ma = get_distance_from_ma_percent(df)

                try:
                    score_result = calculate_long_score(
                        df=df,
                        vol_ratio=vol_ratio,
                        mtf_confirmed=mtf_confirmed,
                        btc_mode=btc_mode,
                        breakout=breakout,
                        pre_breakout=pre_breakout,
                        is_new=is_new,
                        funding=funding,
                        btc_dominance_proxy=btc_dominance_proxy,
                        market_state=market_state,
                        alt_mode=alt_mode,
                        market_bias_label=market_bias_label,
                    )
                except Exception as score_err:
                    logger.error(f"{symbol} → calculate_long_score failed: {score_err}")
                    continue

                if not early_signal and not pre_breakout and vol_ratio < 1.3:
                    dynamic_threshold = get_dynamic_entry_threshold(
                        market_state=market_state,
                        score_result=score_result,
                        vol_ratio=vol_ratio,
                        mtf_confirmed=mtf_confirmed,
                        is_new=is_new,
                    )
                    if score_result["score"] < dynamic_threshold:
                        logger.info(
                            f"{symbol} → rejected (no early_signal / no pre_breakout / "
                            f"score<{dynamic_threshold} | market={market_state} | vol={vol_ratio})"
                        )
                        continue

                if has_high_impact_news:
                    if "warning_reasons" not in score_result:
                        score_result["warning_reasons"] = []

                    if "أخبار اقتصادية مهمة قريبة" not in score_result["warning_reasons"]:
                        score_result["warning_reasons"].append("أخبار اقتصادية مهمة قريبة")

                pre_breakout_only = pre_breakout and not early_signal
                required_min_score = FINAL_MIN_SCORE + PRE_BREAKOUT_EXTRA_SCORE if pre_breakout_only else FINAL_MIN_SCORE

                logger.info(
                    f"{symbol} → early_signal: {early_signal} | "
                    f"pre_breakout: {pre_breakout} | "
                    f"score: {score_result['score']} | "
                    f"min_required: {required_min_score} | "
                    f"score_signal: {score_result['signal']} | "
                    f"fake: {score_result['fake_signal']} | "
                    f"mtf: {mtf_confirmed} | "
                    f"new: {is_new} | "
                    f"market={market_state}"
                )

                if score_result["fake_signal"] and not pre_breakout:
                    logger.info(f"{symbol} → rejected by fake signal")
                    continue

                if score_result["score"] < required_min_score:
                    logger.info(f"{symbol} → rejected by final min score ({score_result['score']} < {required_min_score})")
                    continue

                if not breakout and not pre_breakout and dist_ma > 3.8:
                    logger.info(f"{symbol} → rejected (late move without breakout/pre-breakout)")
                    continue

                candle_time = get_signal_candle_time(df)
                now = time.time()

                if symbol in last_candle_cache and last_candle_cache[symbol] == candle_time:
                    logger.info(f"{symbol} → skipped (same candle in memory)")
                    continue

                if symbol in sent_cache and now - sent_cache[symbol] < LOCAL_RECENT_SEND_SECONDS:
                    logger.info(f"{symbol} → skipped (local cooldown active)")
                    continue

                if symbol in sent_symbols_this_run:
                    logger.info(f"{symbol} → skipped (already sent this run)")
                    continue

                if symbol in candidates_symbols:
                    logger.info(f"{symbol} → skipped (already queued this run)")
                    continue

                if already_sent_same_candle(symbol, candle_time, "long"):
                    logger.info(f"{symbol} → skipped (same candle in Redis)")
                    continue

                if is_symbol_on_cooldown(symbol, "long"):
                    logger.info(f"{symbol} → skipped (cooldown active)")
                    continue

                candle_strength = get_candle_strength_ratio(df)

                if is_new:
                    if not passes_new_listing_filter(
                        score=float(score_result["score"]),
                        breakout=breakout or pre_breakout,
                        vol_ratio=vol_ratio,
                        candle_strength=candle_strength,
                    ):
                        logger.info(f"{symbol} → rejected by balanced new listing filter")
                        continue

                signal_row = get_signal_row(df)
                price = float(signal_row["close"])
                atr_value = float(signal_row["atr"])

                if breakout:
                    sl_type = "breakout"
                elif pre_breakout:
                    sl_type = "pre_breakout"
                elif is_new:
                    sl_type = "new_listing"
                else:
                    sl_type = "standard"

                stop_loss = calculate_stop_loss(price, atr_value, signal_type=sl_type)
                tv_link = build_tradingview_link(symbol)

                opportunity_type = classify_opportunity_type(
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    dist_ma=dist_ma,
                    mtf_confirmed=mtf_confirmed,
                )
                entry_timing = classify_entry_timing(
                    dist_ma=dist_ma,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    vol_ratio=vol_ratio,
                )

                explicit_warnings = score_result.get("warning_reasons") or []
                _, inferred_warnings = classify_reasons(score_result.get("reasons", []))
                warnings_count = len(explicit_warnings) if explicit_warnings else len(inferred_warnings)
                base_risk = get_base_risk_label(score_result, warnings_count)
                display_risk = adjust_risk_with_entry_timing(base_risk, entry_timing)

                momentum_priority = get_momentum_priority(
                    score=float(score_result["score"]),
                    breakout=breakout,
                    vol_ratio=vol_ratio,
                    is_new=is_new,
                    pre_breakout=pre_breakout,
                )

                candidate = {
                    "symbol": symbol,
                    "score": float(score_result["score"]),
                    "momentum_priority": momentum_priority,
                    "breakout": breakout,
                    "pre_breakout": pre_breakout,
                    "vol_ratio": vol_ratio,
                    "candle_strength": candle_strength,
                    "is_new": is_new,
                    "rank_volume_24h": float(pair_data.get("_rank_volume_24h", 0)),
                    "message": build_message(
                        symbol=symbol,
                        price=price,
                        score_result=score_result,
                        stop_loss=stop_loss,
                        btc_mode=btc_mode,
                        btc_dominance_proxy=btc_dominance_proxy,
                        tv_link=tv_link,
                        is_new=is_new,
                        change_24h=change_24h,
                        market_state_label=market_state_label,
                        market_bias_label=market_bias_label,
                        alt_mode=alt_mode,
                        news_warning=news_warning_text,
                        opportunity_type=opportunity_type,
                        entry_timing=entry_timing,
                        display_risk=display_risk,
                    ),
                    "candle_time": candle_time,
                    "now": now,
                    "entry": price,
                    "sl": stop_loss,
                    "funding_label": score_result.get("funding_label", "🟡 محايد"),
                    "reasons": score_result.get("reasons", []),
                    "mtf_confirmed": mtf_confirmed,
                    "btc_dominance_proxy": btc_dominance_proxy,
                    "change_24h": change_24h,
                    "market_state": market_state,
                    "alt_mode": alt_mode,
                }
                candidate["bucket"] = get_candidate_bucket(candidate)

                candidates.append(candidate)
                candidates_symbols.add(symbol)

            logger.info(f"Candidates found before momentum filter: {len(candidates)}")

            candidates = apply_top_momentum_filter(candidates)
            logger.info(f"Candidates found after momentum filter: {len(candidates)}")

            top_candidates = diversify_candidates(candidates, MAX_ALERTS_PER_RUN)

            for candidate in top_candidates:
                symbol = candidate["symbol"]

                if symbol in sent_symbols_this_run:
                    logger.info(f"{symbol} → skipped (already sent final stage)")
                    continue

                logger.info(
                    f"FINAL CHECK | {symbol} | candle={candidate['candle_time']} | "
                    f"same_candle={already_sent_same_candle(symbol, candidate['candle_time'], 'long')} | "
                    f"cooldown={is_symbol_on_cooldown(symbol, 'long')}"
                )

                if already_sent_same_candle(symbol, candidate["candle_time"], "long"):
                    logger.info(f"{symbol} → skipped (already sent this candle Redis FINAL)")
                    continue

                if is_symbol_on_cooldown(symbol, "long"):
                    logger.info(f"{symbol} → skipped (cooldown active final)")
                    continue

                locked = reserve_signal_slot(
                    symbol=symbol,
                    candle_time=candidate["candle_time"],
                    signal_type="long",
                )

                if not locked:
                    logger.info(f"{symbol} → skipped (reserve failed / duplicate)")
                    continue

                sent_ok = send_telegram_message(candidate["message"])

                if sent_ok:
                    sent_symbols_this_run.add(symbol)
                    sent_count += 1
                    sent_cache[symbol] = time.time()
                    last_candle_cache[symbol] = candidate["candle_time"]
                    last_global_send_ts = time.time()

                    register_trade(
                        redis_client=r,
                        symbol=symbol,
                        market_type="futures",
                        side="long",
                        candle_time=candidate["candle_time"],
                        entry=candidate["entry"],
                        sl=candidate["sl"],
                        score=candidate["score"],
                        timeframe=TIMEFRAME,
                        btc_mode=btc_mode,
                        funding_label=candidate["funding_label"],
                        reasons=candidate["reasons"],
                        pre_breakout=candidate["pre_breakout"],
                        breakout=candidate["breakout"],
                        vol_ratio=candidate["vol_ratio"],
                        candle_strength=candidate["candle_strength"],
                        mtf_confirmed=candidate["mtf_confirmed"],
                        is_new=candidate["is_new"],
                        btc_dominance_proxy=candidate["btc_dominance_proxy"],
                        change_24h=candidate["change_24h"],
                    )

                    logger.info(
                        f"SENT → {symbol} | score: {candidate['score']} | "
                        f"momentum: {candidate['momentum_priority']} | "
                        f"bucket: {candidate['bucket']} | new={candidate['is_new']} | "
                        f"market={candidate['market_state']} | alt={candidate['alt_mode']}"
                    )
                else:
                    release_signal_slot(
                        symbol=symbol,
                        candle_time=candidate["candle_time"],
                        signal_type="long",
                    )
                    logger.error(f"FAILED SEND → {symbol}")

            if sent_count > 0:
                set_global_cooldown()
                logger.info(f"Global cooldown set for {GLOBAL_COOLDOWN_SECONDS}s after {sent_count} alert(s)")

            logger.info(f"Sent alerts this run: {sent_count}")
            logger.info(f"Tested {tested} pairs")
            logger.info("Sleeping 60 seconds...")

            time.sleep(60)

        except Exception as e:
            logger.error(f"Fatal error: {e}")
            time.sleep(10)

        finally:
            if scan_locked:
                release_scan_lock()


def run():
    logger.info(
        f"BOT STARTED | pid={os.getpid()} | replica={os.getenv('RAILWAY_REPLICA_ID', 'unknown')}"
    )

    clear_webhook()

    command_thread = threading.Thread(target=run_command_poller, daemon=True)
    command_thread.start()

    run_scanner_loop()


if __name__ == "__main__":
    run()
