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
from analysis.performance_diagnostics import (
    build_setups_report,
    build_scores_report,
    build_market_report,
    build_losses_report,
    build_full_diagnostics_report,
)
from tracking.performance import (
    register_trade,
    update_open_trades,
    get_winrate_summary,
    format_winrate_summary,
    get_period_summary,
    get_trade_summary,
    format_period_summary,
    get_setup_type_stats,
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
OKX_TICKER_SINGLE_URL = "https://www.okx.com/api/v5/market/ticker"

SCAN_LIMIT = 200
TIMEFRAME = "15m"
HTF_TIMEFRAME = "1H"

FINAL_MIN_SCORE = 6.2
PRE_BREAKOUT_EXTRA_SCORE = 0.2
MAX_ALERTS_PER_RUN = 5

COOLDOWN_SECONDS = 3600
LOCAL_RECENT_SEND_SECONDS = 2700
GLOBAL_COOLDOWN_SECONDS = 120
COMMAND_POLL_INTERVAL = 3

MIN_24H_QUOTE_VOLUME = 1_000_000
NEW_LISTING_MAX_CANDLES = 50

TOP_MOMENTUM_PERCENT = 0.30
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

# Late pump / bull-market protection
LATE_PUMP_DIST_MA = 4.2
LATE_PUMP_RSI = 67.0
LATE_PUMP_VOL_RATIO = 1.80
LATE_PUMP_CANDLE_STRENGTH = 0.62

BULL_CONTINUATION_MAX_DIST_MA = 3.2
BULL_CONTINUATION_MAX_RSI = 66.0
BULL_CONTINUATION_MAX_VOL_RATIO = 1.75

BULL_CONTINUATION_SCORE_PENALTY = 0.45
LATE_PUMP_SCORE_PENALTY = 0.65
EXTREME_LATE_PUMP_SCORE_PENALTY = 0.90

# Oversold reversal long
OVERSOLD_REVERSAL_ENABLED = True
OVERSOLD_REVERSAL_MIN_DIST_MA = 5.8
OVERSOLD_REVERSAL_MIN_24H_DROP = -12.0
OVERSOLD_REVERSAL_MAX_RSI = 32.0
OVERSOLD_REVERSAL_MIN_VOL_RATIO = 1.05
OVERSOLD_REVERSAL_SCORE_BONUS = 0.35
OVERSOLD_REVERSAL_MIN_SCORE = 6.2

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
STATS_RESET_TS_KEY = "stats:last_reset_ts:long"
EXTRA_ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

ADMIN_CHAT_IDS = set()
if CHAT_ID:
    ADMIN_CHAT_IDS.add(str(CHAT_ID))
if EXTRA_ADMIN_CHAT_ID:
    ADMIN_CHAT_IDS.add(str(EXTRA_ADMIN_CHAT_ID))

# Candle cache
CANDLE_CACHE_TTL_15M = 25
CANDLE_CACHE_TTL_1H = 90
CANDLE_CACHE_TTL_DEFAULT = 20

# Alt snapshot cache
ALT_SNAPSHOT_CACHE_KEY = "cache:alt_snapshot"
ALT_SNAPSHOT_CACHE_TTL = 600

# Alert tracking
ALERT_KEY_PREFIX = "alert:long"
ALERT_BY_MESSAGE_KEY_PREFIX = "alertmsg:long"
ALERT_TTL_SECONDS = 14 * 24 * 3600

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


def get_alert_key(alert_id: str) -> str:
    return f"{ALERT_KEY_PREFIX}:{alert_id}"


def get_alert_by_message_key(message_id: str) -> str:
    return f"{ALERT_BY_MESSAGE_KEY_PREFIX}:{message_id}"


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
        return bool(r.exists("global_cooldown:long"))
    except Exception:
        return False


def set_global_cooldown() -> None:
    if not r:
        return
    try:
        r.set("global_cooldown:long", "1", ex=GLOBAL_COOLDOWN_SECONDS)
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
# TELEGRAM OFFSET
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


def telegram_api_call(method: str, payload: dict) -> dict:
    if not BOT_TOKEN:
        return {"ok": False, "error": "missing_bot_token"}

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        response = requests.post(url, json=payload, timeout=20)
        if response.status_code != 200:
            logger.error(f"Telegram {method} HTTP Error: {response.text}")
            return {"ok": False, "error": response.text}

        data = response.json()
        if not data.get("ok"):
            logger.error(f"Telegram {method} API Error: {data}")
        return data
    except Exception as e:
        logger.error(f"Telegram {method} Exception: {e}")
        return {"ok": False, "error": str(e)}


def answer_callback_query(callback_query_id: str, text: str = "") -> None:
    if not callback_query_id:
        return
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    telegram_api_call("answerCallbackQuery", payload)


def send_telegram_message(message: str, reply_markup=None) -> dict:
    if not BOT_TOKEN or not CHAT_ID:
        logger.error("❌ Telegram config missing")
        return {"ok": False}

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    return telegram_api_call("sendMessage", payload)


def send_telegram_reply(chat_id: str, message: str) -> bool:
    if not BOT_TOKEN or not chat_id:
        logger.error("❌ Telegram reply config missing")
        return False

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    data = telegram_api_call("sendMessage", payload)
    return bool(data.get("ok"))


def get_telegram_updates(offset: int = 0):
    if not BOT_TOKEN:
        return []

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {"timeout": 1, "offset": offset}

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
    "/report_today": "تقرير اليوم",
    "/report_month": "تقرير آخر 30 يوم",
    "/report_all": "كل الصفقات",
    "/report_deep": "تحليل متقدم للأداء",
    "/report_setups": "تحليل أفضل وأسوأ أنواع الإشارات",
    "/report_scores": "تحليل الأداء حسب السكور",
    "/report_market": "تحليل الأداء حسب حالة السوق والدخول",
    "/report_losses": "تحليل أسباب الخسارة",
    "/report_diagnostics": "تقرير تشخيصي شامل",
    "/reset_stats": "تصفير نتائج البوت",
    "/stats_since_reset": "الأداء من بعد آخر تصفير",
}


def get_local_day_start_ts() -> int:
    try:
        now = time.localtime()
        return int(time.mktime((
            now.tm_year, now.tm_mon, now.tm_mday,
            0, 0, 0,
            now.tm_wday, now.tm_yday, now.tm_isdst
        )))
    except Exception:
        return int(time.time()) - 86400


def build_report_message(period: str) -> str:
    title_map = {
        "1h": "Long Report - Last 1H",
        "today": "Long Report - Today",
        "month": "Long Report - Last 30 Days",
        "30d": "Long Report - Last 30 Days",
        "all": "Long Report - All Time",
    }

    try:
        if period == "today":
            summary = get_trade_summary(
                redis_client=r,
                market_type="futures",
                side="long",
                since_ts=get_local_day_start_ts(),
            )
            return format_period_summary(title_map["today"], summary)

        # التقرير الشهري = آخر 30 يوم
        if period == "month":
            summary = get_period_summary(
                redis_client=r,
                period="30d",
                market_type="futures",
                side="long",
            )
            return format_period_summary(title_map["month"], summary)

        summary = get_period_summary(
            redis_client=r,
            period=period,
            market_type="futures",
            side="long",
        )
        return format_period_summary(title_map.get(period, "Long Report"), summary)

    except Exception as e:
        logger.error(f"build_report_message error on period={period}: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"


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
        "🤖 <b>OKX Scanner Bot - LONG</b>",
        "",
        "📊 <b>التقارير:</b>",
        *report_commands,
        "",
        "⚙️ <b>معلومات:</b>",
        "• البوت بيبعت إشارات Long Futures",
        "• مبني على Volume + Breakout + MTF",
        "• فيه زر 📌 Track لمتابعة نتيجة أي إشارة",
        "• Smart Early Priority للإشارات المبكرة",
        "• فيه تمييز خاص لفرص Oversold Reversal",
        "• التقارير تعرض الأداء المالي كنسبة فقط بدون إدخال قيمة لكل صفقة",
        "• الحساب مبني على حد استخدام 35% من المحفظة",
        "• إدارة المخاطرة تتحذر عند الاقتراب من خسارة 20% من إجمالي المحفظة",
        "• حساب الربح يراعي 50% عند TP1 و 50% تكمل إلى TP2 أو تعود Entry",
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
        "استخدم /report_today لمتابعة أداء اللونج",
    ])

    return "\n".join(lines)


def build_how_it_work_message() -> str:
    return """📘 <b>كيف يعمل بوت اللونج؟</b>

🤖 <b>فكرة البوت:</b>
البوت يبحث عن فرص <b>Long Futures</b> على OKX،
بفلترة متوازنة حتى لا يخنق الإشارات الجيدة.

🔍 <b>منطق العمل:</b>
1. اختيار العملات الأعلى سيولة وحجم تداول
2. تحليل فريم 15m
3. قياس قوة الزخم الصاعد
4. تقييم:
• الفوليوم
• RSI
• موقع السعر من المتوسط
• Breakout / Pre-Breakout
• تأكيد 1H
• حالة السوق العامة
5. Smart Early Priority للإشارات المبكرة
6. إعطاء Score من 10
7. إرسال فقط الفرص المقبولة نهائيًا

📌 <b>زر Track:</b>
يعرض لاحقًا:
• الحالة
• السعر الحالي
• أقصى صعود لصالح الصفقة
• أقصى هبوط ضد الصفقة
• مدة الصفقة

💰 <b>التقارير المالية:</b>
• البوت لا يحتاج إدخال قيمة لكل صفقة
• يحسب صافي حركة الصفقات كنسبة %
• يقدر تأثيرها على المحفظة بناءً على استخدام 35% من الرصيد
• TP1 محسوب كإغلاق 50% من الصفقة
• بعد TP1 يتم اعتبار النصف الثاني عند Entry لو رجع السعر
• حد المخاطرة المعروض مبني على خسارة 20% من إجمالي المحفظة

✅ <b>أفضل استخدام:</b>
راجع الشارت بسرعة وخد القرار بعد التأكد من السياق العام."""


# =========================
# RESET / STATS
# =========================
def reset_stats(chat_id: str):
    if ADMIN_CHAT_IDS and str(chat_id) not in ADMIN_CHAT_IDS:
        send_telegram_reply(chat_id, f"⛔ غير مسموح\nchat_id={chat_id}")
        logger.warning(f"RESET BLOCKED | chat_id={chat_id} | allowed={sorted(ADMIN_CHAT_IDS)}")
        return

    if not r:
        send_telegram_reply(chat_id, "❌ Redis غير متصل")
        return

    try:
        deleted = 0

        # يمسح trade keys العادية فقط — مش trade_history
        for key in r.scan_iter("trade:futures:long:*"):
            r.delete(key)
            deleted += 1

        extra_keys = [
            "open_trades:futures:long",
            "stats:futures:long",
        ]

        for key in extra_keys:
            try:
                r.delete(key)
            except Exception:
                pass

        reset_ts = int(time.time())
        r.set(STATS_RESET_TS_KEY, str(reset_ts))
        saved_reset = r.get(STATS_RESET_TS_KEY)

        send_telegram_reply(
            chat_id,
            f"🧹 تم تصفير إحصائيات اللونج بنجاح\n"
            f"📊 عدد مفاتيح الصفقات المحذوفة: {deleted}\n"
            f"🕒 وقت التصفير: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(reset_ts))}\n"
            f"📌 Trade History للـ Hybrid Label محفوظ ✅\n"
            f"✅ reset_key={saved_reset}"
        )

        logger.info(
            f"RESET LONG STATS ONLY → deleted={deleted} | reset_ts={reset_ts} | "
            f"saved={saved_reset} | trade_history=PRESERVED"
        )

    except Exception as e:
        logger.error(f"Reset stats error: {e}")
        send_telegram_reply(chat_id, f"❌ حصل خطأ أثناء التصفير\n{html.escape(str(e))}")


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

        body = format_period_summary("Since Reset", summary)

        send_telegram_reply(
            chat_id,
            f"📊 <b>Long Stats Since Reset</b>\n"
            f"🕒 منذ: {html.escape(reset_time_text)}\n\n"
            f"{body}"
        )
        logger.info(f"LONG STATS SINCE RESET → reset_ts={reset_ts}")

    except Exception as e:
        logger.error(f"stats_since_reset error: {e}")
        send_telegram_reply(chat_id, f"❌ حصل خطأ أثناء جلب التقرير\n{html.escape(str(e))}")


COMMAND_HANDLERS = {
    "/help": lambda chat_id: send_telegram_reply(chat_id, build_help_message()),
    "/how_it_work": lambda chat_id: send_telegram_reply(chat_id, build_how_it_work_message()),
    "/report_1h": lambda chat_id: send_telegram_reply(chat_id, build_report_message("1h")),
    "/report_today": lambda chat_id: send_telegram_reply(chat_id, build_report_message("today")),
    "/report_month": lambda chat_id: send_telegram_reply(chat_id, build_report_message("month")),
    "/report_all": lambda chat_id: send_telegram_reply(chat_id, build_report_message("all")),
    "/report_deep": lambda chat_id: send_telegram_reply(chat_id, build_deep_report_message()),
    "/report_setups": lambda chat_id: send_telegram_reply(
        chat_id,
        build_setups_report(r, market_type="futures", side="long", period="all")
    ),
    "/report_scores": lambda chat_id: send_telegram_reply(
        chat_id,
        build_scores_report(r, market_type="futures", side="long", period="all")
    ),
    "/report_market": lambda chat_id: send_telegram_reply(
        chat_id,
        build_market_report(r, market_type="futures", side="long", period="all")
    ),
    "/report_losses": lambda chat_id: send_telegram_reply(
        chat_id,
        build_losses_report(r, market_type="futures", side="long", period="all")
    ),
    "/report_diagnostics": lambda chat_id: send_telegram_reply(
        chat_id,
        build_full_diagnostics_report(r, market_type="futures", side="long", period="all")
    ),
    "/reset_stats": lambda chat_id: reset_stats(chat_id),
    "/stats_since_reset": lambda chat_id: stats_since_reset(chat_id),
}


# =========================
# ALERT TRACKING
# =========================
def build_alert_id(symbol: str, candle_time: int) -> str:
    return f"{clean_symbol_for_message(symbol)}:{int(candle_time)}"


def save_alert_snapshot(alert_data: dict, message_id=None) -> None:
    if not r or not alert_data:
        return

    try:
        alert_id = alert_data.get("alert_id")
        if not alert_id:
            return

        payload = dict(alert_data)
        if message_id is not None:
            payload["message_id"] = str(message_id)

        r.set(get_alert_key(alert_id), json.dumps(payload), ex=ALERT_TTL_SECONDS)
        if message_id:
            r.set(get_alert_by_message_key(str(message_id)), alert_id, ex=ALERT_TTL_SECONDS)
    except Exception as e:
        logger.error(f"save_alert_snapshot error: {e}")


def load_alert_snapshot(alert_id: str):
    if not r or not alert_id:
        return None
    try:
        raw = r.get(get_alert_key(alert_id))
        if not raw:
            return None
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.error(f"load_alert_snapshot error: {e}")
        return None


def get_last_price(symbol: str) -> float:
    try:
        res = requests.get(OKX_TICKER_SINGLE_URL, params={"instId": symbol}, timeout=10).json()
        data = res.get("data", [])
        if not data:
            return 0.0
        return _safe_float(data[0].get("last"), 0.0)
    except Exception as e:
        logger.error(f"get_last_price error on {symbol}: {e}")
        return 0.0


def get_max_move_since_alert(symbol: str, since_ts: int, entry: float, side: str = "long"):
    try:
        candles = get_candles(symbol, TIMEFRAME, 100)
        df = to_dataframe(candles)
        if df is None or df.empty:
            return 0.0, 0.0

        work = df[df["ts"] >= (since_ts * 1000 if since_ts < 10_000_000_000 else since_ts)].copy()
        if work.empty:
            work = df.tail(20).copy()

        lows = work["low"].astype(float)
        highs = work["high"].astype(float)

        if lows.empty or highs.empty or entry <= 0:
            return 0.0, 0.0

        if side == "long":
            favorable_pct = round(((float(highs.max()) - entry) / entry) * 100, 2)
            adverse_pct = round(((entry - float(lows.min())) / entry) * 100, 2)
            return favorable_pct, adverse_pct

        favorable_pct = round(((entry - float(lows.min())) / entry) * 100, 2)
        adverse_pct = round(((float(highs.max()) - entry) / entry) * 100, 2)
        return favorable_pct, adverse_pct

    except Exception as e:
        logger.error(f"get_max_move_since_alert error on {symbol}: {e}")
        return 0.0, 0.0


def get_alert_status(alert: dict) -> str:
    try:
        symbol = alert["symbol"]
        entry = _safe_float(alert.get("entry"), 0)
        sl = _safe_float(alert.get("sl"), 0)
        tp1 = _safe_float(alert.get("tp1"), 0)
        tp2 = _safe_float(alert.get("tp2"), 0)
        candle_time = int(_safe_float(alert.get("candle_time"), 0))

        favorable_pct, adverse_pct = get_max_move_since_alert(
            symbol=symbol,
            since_ts=candle_time,
            entry=entry,
            side="long",
        )

        if entry <= 0:
            return "غير معروف"

        sl_pct = round(((entry - sl) / entry) * 100, 4) if sl > 0 else 0
        tp1_pct = round(((tp1 - entry) / entry) * 100, 4) if tp1 > 0 else 0
        tp2_pct = round(((tp2 - entry) / entry) * 100, 4) if tp2 > 0 else 0

        if adverse_pct >= sl_pct > 0:
            return "SL Hit ❌"

        if favorable_pct >= tp2_pct > 0:
            return "TP2 Hit 🎯"

        if favorable_pct >= tp1_pct > 0:
            return "TP1 Hit ✅"

        return "Open ⏳"

    except Exception as e:
        logger.error(f"get_alert_status error: {e}")
        return "غير معروف"


def build_track_message(alert: dict) -> str:
    try:
        symbol = clean_symbol_for_message(alert.get("symbol", "Unknown"))
        entry = _safe_float(alert.get("entry"), 0.0)
        sl = _safe_float(alert.get("sl"), 0.0)
        tp1 = _safe_float(alert.get("tp1"), 0.0)
        tp2 = _safe_float(alert.get("tp2"), 0.0)
        candle_time = int(_safe_float(alert.get("candle_time"), 0))
        created_ts = int(_safe_float(alert.get("created_ts"), candle_time))
        current_price = get_last_price(alert.get("symbol", ""))

        favorable_pct, adverse_pct = get_max_move_since_alert(
            symbol=alert.get("symbol", ""),
            since_ts=candle_time,
            entry=entry,
            side="long",
        )

        status = get_alert_status(alert)
        duration_seconds = max(0, int(time.time()) - created_ts)
        duration_h = duration_seconds // 3600
        duration_m = (duration_seconds % 3600) // 60

        current_move = 0.0
        if entry > 0 and current_price > 0:
            current_move = round(((current_price - entry) / entry) * 100, 2)

        return (
            f"📌 <b>Alert Track</b>\n\n"
            f"العملة: {html.escape(symbol)}\n"
            f"النوع: Long\n"
            f"الفريم: {html.escape(str(alert.get('timeframe', TIMEFRAME)))}\n\n"
            f"Entry: {entry:.6f}\n"
            f"SL: {sl:.6f}\n"
            f"TP1: {tp1:.6f}\n"
            f"TP2: {tp2:.6f}\n\n"
            f"الحالة: {html.escape(status)}\n"
            f"السعر الحالي: {current_price:.6f}\n"
            f"الحركة الحالية: {current_move:+.2f}%\n"
            f"أقصى صعود: +{favorable_pct:.2f}%\n"
            f"أقصى هبوط ضدك: +{adverse_pct:.2f}%\n"
            f"المدة: {duration_h}h {duration_m}m"
        )
    except Exception as e:
        logger.error(f"build_track_message error: {e}")
        return "❌ حصل خطأ أثناء متابعة الإشارة"


def build_track_reply_markup(alert_id: str) -> dict:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "📌 Track",
                    "callback_data": f"track_long:{alert_id}"
                }
            ]
        ]
    }


def handle_callback_query(callback_query: dict):
    try:
        callback_id = callback_query.get("id", "")
        data = callback_query.get("data", "") or ""
        message = callback_query.get("message") or {}
        chat_id = str((message.get("chat") or {}).get("id", "") or "")

        if not data.startswith("track_long:"):
            answer_callback_query(callback_id, "زر غير مدعوم")
            return

        alert_id = data.split(":", 1)[1].strip()
        alert = load_alert_snapshot(alert_id)

        if not alert:
            answer_callback_query(callback_id, "الإشارة غير موجودة أو انتهت صلاحيتها")
            if chat_id:
                send_telegram_reply(chat_id, "ℹ️ لا يمكن العثور على بيانات هذه الإشارة")
            return

        answer_callback_query(callback_id, "جارِ جلب نتيجة الإشارة...")
        if chat_id:
            send_telegram_reply(chat_id, build_track_message(alert))

    except Exception as e:
        logger.error(f"handle_callback_query error: {e}")
        try:
            callback_id = callback_query.get("id", "")
            answer_callback_query(callback_id, "حصل خطأ")
        except Exception:
            pass


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

            change_24h = extract_24h_change_percent(item)
            item["_rank_volume_24h"] = vol_24h
            item["_rank_change_24h"] = change_24h
            filtered.append(item)

        # توزيع متوازن: volume + momentum + reversal + new listings
        by_volume   = sorted(filtered, key=lambda x: x.get("_rank_volume_24h", 0), reverse=True)
        by_momentum = sorted(filtered, key=lambda x: x.get("_rank_change_24h", 0), reverse=True)
        by_reversal = sorted(filtered, key=lambda x: x.get("_rank_change_24h", 0), reverse=False)

        n_vol      = int(SCAN_LIMIT * 0.35)
        n_momentum = int(SCAN_LIMIT * 0.25)
        n_reversal = int(SCAN_LIMIT * 0.25)
        n_new      = SCAN_LIMIT - n_vol - n_momentum - n_reversal

        seen = set()
        merged = []

        for item in by_volume[:n_vol]:
            sid = item.get("instId", "")
            if sid not in seen:
                seen.add(sid)
                merged.append(item)

        for item in by_momentum[:n_momentum * 2]:
            if len([x for x in merged if x.get("_rank_change_24h", 0) > 0]) >= n_momentum:
                break
            sid = item.get("instId", "")
            if sid not in seen and item.get("_rank_change_24h", 0) > 0:
                seen.add(sid)
                merged.append(item)

        for item in by_reversal[:n_reversal * 2]:
            if len([x for x in merged if x.get("_rank_change_24h", 0) < 0]) >= n_reversal:
                break
            sid = item.get("instId", "")
            if sid not in seen:
                seen.add(sid)
                merged.append(item)

        # fill remaining من top volume
        for item in by_volume:
            if len(merged) >= SCAN_LIMIT:
                break
            sid = item.get("instId", "")
            if sid not in seen:
                seen.add(sid)
                merged.append(item)

        merged = merged[:SCAN_LIMIT]

        logger.info(f"After liquidity filter: {len(filtered)}")
        logger.info(f"Using merged ranked pairs for long scan: {len(merged)}")

        return merged

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


def compute_bollinger_bands(series, period=20, std_mult=2):
    """
    حساب Bollinger Bands (الوسط، العلوي، السفلي)
    """
    ma = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = ma + (std * std_mult)
    lower = ma - (std * std_mult)
    return ma, upper, lower


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

    # ✅ إضافة Bollinger Bands
    df["bb_mid"], df["bb_upper"], df["bb_lower"] = compute_bollinger_bands(df["close"])

    return df


def get_candle_cache_key(symbol: str, timeframe: str, limit: int) -> str:
    return f"candles:long:{symbol}:{timeframe}:{limit}"


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
    """إرجاع صف الإشارة (آخر شمعة مكتملة) أو None في حالة الخطأ"""
    try:
        if df is None or df.empty:
            return None

        if len(df) == 1:
            return df.iloc[-1]

        if "confirm" not in df.columns:
            return df.iloc[-2]

        last = df.iloc[-1]
        if str(int(float(last["confirm"]))) == "1":
            return last

        return df.iloc[-2]
    except Exception:
        return None


def get_signal_candle_time(df):
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return int(time.time() // (15 * 60))
        ts = int(signal_row["ts"])
        if ts > 10_000_000_000:
            return ts // 1000
        return ts
    except Exception:
        return int(time.time() // (15 * 60))


# =========================
# HELPERS
# =========================
def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        if value != value:
            return default
        return float(value)
    except Exception:
        return default


def is_above_upper_bollinger(df) -> bool:
    """هل السعر فوق الـ Upper Bollinger Band؟"""
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        close = _safe_float(signal_row["close"])
        upper = _safe_float(signal_row.get("bb_upper"), 0)
        if upper <= 0:
            return False
        return close > upper
    except Exception:
        return False


def get_change_4h(df) -> float:
    """نسبة التغيير خلال آخر 4 ساعات (16 شمعة 15m)."""
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return 0.0
        idx = signal_row.name
        if idx is None or idx < 16:
            return 0.0
        current = _safe_float(df.iloc[idx]["close"])
        prev = _safe_float(df.iloc[idx - 16]["close"])
        if prev <= 0:
            return 0.0
        return round(((current - prev) / prev) * 100, 2)
    except Exception:
        return 0.0


def is_late_long_entry(dist_ma: float, breakout: bool, pre_breakout: bool) -> bool:
    try:
        if breakout or pre_breakout:
            return False
        return dist_ma > 6.2
    except Exception:
        return False


def is_exhausted_long_move(
    dist_ma: float,
    vol_ratio: float,
    candle_strength: float,
    breakout: bool,
    pre_breakout: bool,
) -> bool:
    try:
        if breakout or pre_breakout:
            return False

        if dist_ma > 5.5 and vol_ratio < 1.15:
            return True

        if dist_ma > 5.0 and candle_strength < 0.50:
            return True

        if dist_ma > 5.5 and vol_ratio < 1.45 and candle_strength < 0.55:
            return True

        return False
    except Exception:
        return False


def detect_late_pump_long(
    market_state: str,
    opportunity_type: str,
    dist_ma: float,
    rsi_now: float,
    vol_ratio: float,
    candle_strength: float,
    breakout: bool,
    pre_breakout: bool,
    breakout_quality: str,
    change_4h: float = 0.0,
) -> dict:
    """
    يكشف الدخول المتأخر بعد Pump.
    الهدف: منع الحالات التي كانت تظهر قوية لكنها تخسر:
    RSI عالي + فوليوم عالي + شمعة قوية + السعر بعيد عن MA.
    """
    try:
        reasons = []

        is_continuation = str(opportunity_type or "").strip() in ("استمرار", "continuation")

        over_ma = dist_ma >= LATE_PUMP_DIST_MA
        hot_rsi = rsi_now >= LATE_PUMP_RSI
        pump_volume = vol_ratio >= LATE_PUMP_VOL_RATIO
        big_candle = candle_strength >= LATE_PUMP_CANDLE_STRENGTH
        fast_4h_move = change_4h >= 3.0

        if over_ma:
            reasons.append("overextended_from_ma")
        if hot_rsi:
            reasons.append("rsi_overheated")
        if pump_volume:
            reasons.append("volume_pump")
        if big_candle:
            reasons.append("strong_candle_chase")
        if fast_4h_move:
            reasons.append("fast_4h_move")

        checks = sum([over_ma, hot_rsi, pump_volume, big_candle, fast_4h_move])

        late_pump_risk = checks >= 3

        extreme_late_pump = (
            dist_ma >= 5.2
            and rsi_now >= 70
            and vol_ratio >= 2.0
            and candle_strength >= 0.65
        )

        bull_continuation_risk = (
            market_state == "bull_market"
            and is_continuation
            and not pre_breakout
            and (
                dist_ma > BULL_CONTINUATION_MAX_DIST_MA
                or rsi_now > BULL_CONTINUATION_MAX_RSI
                or vol_ratio > BULL_CONTINUATION_MAX_VOL_RATIO
            )
        )

        weak_breakout_exception = (
            breakout
            and breakout_quality in ("none", "weak")
            and late_pump_risk
        )

        should_block = False

        # أقوى منع: استمرار في bull_market بعد امتداد واضح
        if bull_continuation_risk and late_pump_risk:
            should_block = True

        # منع Pump عنيف جدًا حتى لو فيه breakout ضعيف
        if extreme_late_pump and not pre_breakout:
            should_block = True

        # breakout ضعيف + late pump = غالبًا مطاردة
        if weak_breakout_exception:
            should_block = True

        return {
            "late_pump_risk": bool(late_pump_risk),
            "extreme_late_pump": bool(extreme_late_pump),
            "bull_continuation_risk": bool(bull_continuation_risk),
            "should_block": bool(should_block),
            "reasons": reasons,
            "checks": checks,
        }

    except Exception:
        return {
            "late_pump_risk": False,
            "extreme_late_pump": False,
            "bull_continuation_risk": False,
            "should_block": False,
            "reasons": [],
            "checks": 0,
        }


def append_late_pump_warnings(score_result: dict, late_guard: dict) -> dict:
    """
    يضيف أسباب التحذير داخل score_result بدون ما يكسر شكل الرسائل.
    """
    try:
        if "warning_reasons" not in score_result or score_result["warning_reasons"] is None:
            score_result["warning_reasons"] = []

        warning_map = {
            "overextended_from_ma": "بعيد عن المتوسط (دخول متأخر)",
            "rsi_overheated": "RSI عالي (تشبع شراء)",
            "volume_pump": "فوليوم انفجاري",
            "strong_candle_chase": "شمعة قوية لكن احتمال مطاردة",
            "fast_4h_move": "صعود سريع خلال 4 ساعات",
        }

        if late_guard.get("late_pump_risk"):
            score_result["warning_reasons"].append("خطر مطاردة Pump متأخر")
        if late_guard.get("bull_continuation_risk"):
            score_result["warning_reasons"].append("استمرار في Bull Market بعد امتداد خطر")

        for reason in late_guard.get("reasons", []):
            label = warning_map.get(reason)
            if label:
                score_result["warning_reasons"].append(label)

        score_result["warning_reasons"] = list(dict.fromkeys(score_result["warning_reasons"]))
        return score_result

    except Exception:
        return score_result


def is_oversold_reversal_long(
    df,
    dist_ma: float,
    change_24h: float,
    vol_ratio: float,
    funding: float = 0.0,
) -> bool:
    try:
        if not OVERSOLD_REVERSAL_ENABLED:
            return False

        if df is None or df.empty or len(df) < 25:
            return False

        if dist_ma >= 0:
            return False

        if change_24h > 5.0:
            return False

        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        idx = signal_row.name
        if idx is None or idx < 2:
            return False

        last = df.iloc[idx]
        prev = df.iloc[idx - 1]

        open_ = _safe_float(last["open"])
        close = _safe_float(last["close"])
        high = _safe_float(last["high"])
        low = _safe_float(last["low"])

        prev_close = _safe_float(prev["close"])
        prev_rsi = _safe_float(prev.get("rsi"), 50)
        rsi_now = _safe_float(last.get("rsi"), 50)

        candle_range = high - low
        body = abs(close - open_)
        body_ratio = (body / candle_range) if candle_range > 0 else 0.0
        upper_wick = high - max(open_, close)

        exhaustion_wick = (
            candle_range > 0
            and upper_wick > body * 2.0
            and upper_wick > candle_range * 0.4
        )
        if exhaustion_wick:
            return False

        bullish_close = close > open_
        gained_momentum = close >= prev_close
        rsi_turning = rsi_now <= OVERSOLD_REVERSAL_MAX_RSI and rsi_now >= prev_rsi
        strong_close_position = candle_range > 0 and ((close - low) / candle_range) >= 0.55
        decent_body = body_ratio >= 0.28
        negative_funding = funding < 0

        checks = 0

        if dist_ma <= -OVERSOLD_REVERSAL_MIN_DIST_MA:
            checks += 1
        if change_24h <= OVERSOLD_REVERSAL_MIN_24H_DROP:
            checks += 1
        if vol_ratio >= OVERSOLD_REVERSAL_MIN_VOL_RATIO:
            checks += 1
        if bullish_close:
            checks += 1
        if gained_momentum:
            checks += 1
        if rsi_turning:
            checks += 1
        if strong_close_position:
            checks += 1
        if decent_body:
            checks += 1
        if negative_funding:
            checks += 1

        return checks >= 5

    except Exception:
        return False


def get_reverse_banner_long(is_reverse: bool) -> str:
    if is_reverse:
        return "♻️ <b>OVERSOLD REVERSAL</b>"
    return ""


def get_reverse_style_note_long(is_reverse: bool) -> str:
    if is_reverse:
        return "⚠️ <b>تنبيه خاص:</b> الفرصة من نوع ارتداد عكسي بعد هبوط/امتداد مبالغ فيه"
    return ""


def get_effective_min_score_with_reverse(
    base_min_score: float,
    is_reverse: bool,
) -> float:
    try:
        if is_reverse:
            return round(min(base_min_score, OVERSOLD_REVERSAL_MIN_SCORE), 2)
        return round(base_min_score, 2)
    except Exception:
        return round(base_min_score, 2)


# =========================
# STRATEGY HELPERS
# =========================
def early_bullish_signal(df):
    try:
        if df is None or df.empty or len(df) < 25:
            return False

        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        idx = signal_row.name

        if idx is None or idx < 2:
            return False

        last = df.iloc[idx]
        prev = df.iloc[idx - 1]

        open_ = _safe_float(last["open"])
        close = _safe_float(last["close"])
        high = _safe_float(last["high"])
        low = _safe_float(last["low"])
        rsi_now = _safe_float(last.get("rsi"), 50)
        rsi_prev = _safe_float(prev.get("rsi"), 50)

        candle_range = high - low
        body = abs(close - open_)
        body_ratio = (body / candle_range) if candle_range > 0 else 0.0

        avg_vol = _safe_float(df.iloc[max(0, idx - 10):idx]["volume"].mean(), 0)
        vol_ok = avg_vol > 0 and _safe_float(last["volume"]) >= avg_vol * 1.08

        bullish_close = close > open_
        strong_close_position = candle_range > 0 and ((close - low) / candle_range) >= 0.55
        rsi_strengthening = rsi_now > 48 and rsi_now >= rsi_prev
        real_body = body_ratio >= 0.32

        checks = sum([bullish_close, strong_close_position, rsi_strengthening, vol_ok, real_body])
        return checks >= 3

    except Exception:
        return False


def is_higher_timeframe_confirmed(symbol):
    try:
        candles = get_candles(symbol, HTF_TIMEFRAME, 100)
        df = to_dataframe(candles)

        if df is None or df.empty or len(df) < 10:
            return False

        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        idx = signal_row.name

        if idx is None or idx < 3:
            return False

        checks = 0

        ma_value = signal_row.get("ma", None)
        if ma_value is not None and _safe_float(signal_row["close"]) > _safe_float(ma_value):
            checks += 1

        high_rsi = _safe_float(signal_row.get("rsi"), 50) >= 50
        if high_rsi:
            checks += 1

        last_3 = df.iloc[idx - 3:idx]
        green_candles = sum(
            1 for _, row in last_3.iterrows()
            if _safe_float(row["close"]) > _safe_float(row["open"])
        )
        if green_candles >= 2:
            checks += 1

        return checks >= 2

    except Exception as e:
        logger.error(f"MTF error on {symbol}: {e}")
        return False


def is_4h_oversold_confirmed(symbol: str) -> dict:
    """
    تأكيد 4H خاص للـ Oversold Reversal.
    """
    try:
        candles = get_candles(symbol, "4H", 60)
        df = to_dataframe(candles)

        if df is None or df.empty or len(df) < 20:
            return {
                "confirmed": False,
                "checks": 0,
                "details": f"• فريم {rtl_fix('4H')}: البيانات غير كافية"
            }

        signal_row = get_signal_row(df)
        if signal_row is None:
            return {
                "confirmed": False,
                "checks": 0,
                "details": f"• فريم {rtl_fix('4H')}: تعذر الحصول على صف الإشارة"
            }
        idx = signal_row.name
        if idx is None or idx < 3:
            return {
                "confirmed": False,
                "checks": 0,
                "details": f"• فريم {rtl_fix('4H')}: المؤشر الزمني غير كافٍ"
            }

        close  = _safe_float(signal_row["close"])
        open_  = _safe_float(signal_row["open"])
        high   = _safe_float(signal_row["high"])
        low    = _safe_float(signal_row["low"])
        rsi_4h = _safe_float(signal_row.get("rsi"), 50)
        ma_4h  = _safe_float(signal_row.get("ma"), close)

        candle_range = high - low
        body         = abs(close - open_)
        lower_wick   = min(open_, close) - low
        upper_wick   = high - max(open_, close)

        checks = 0

        rsi_ok = rsi_4h <= 35
        if rsi_ok:
            checks += 1

        below_ma = close < ma_4h
        if below_ma:
            checks += 1

        hammer = (
            candle_range > 0
            and lower_wick >= body * 1.8
            and upper_wick <= body * 0.6
            and close > open_
        )
        bullish_engulf = (
            close > open_
            and candle_range > 0
            and body >= candle_range * 0.60
        )
        reversal_candle = hammer or bullish_engulf
        if reversal_candle:
            checks += 1

        confirmed = checks >= 2

        details_lines = [
            f"• فريم {rtl_fix('4H')} RSI: {fmt_num(rsi_4h, 0)} {'✅' if rsi_ok else '⚠️'}",
            f"• فريم {rtl_fix('4H')}: السعر {'أسفل' if below_ma else 'ليس أسفل'} {rtl_fix('MA20')} {'✅' if below_ma else '⚠️'}",
            f"• فريم {rtl_fix('4H')}: {'توجد شمعة ارتداد واضحة' if reversal_candle else 'لا توجد شمعة ارتداد واضحة'} {'✅' if reversal_candle else '⚠️'}",
        ]
        details = "\n".join(details_lines)

        logger.info(
            f"4H OVERSOLD CHECK | {symbol} | "
            f"rsi={rsi_4h:.1f} | below_ma={below_ma} | "
            f"reversal_candle={reversal_candle} | checks={checks}/3 | confirmed={confirmed}"
        )

        return {"confirmed": confirmed, "checks": checks, "details": details}

    except Exception as e:
        logger.error(f"is_4h_oversold_confirmed error on {symbol}: {e}")
        return {
            "confirmed": False,
            "checks": 0,
            "details": f"• فريم {rtl_fix('4H')}: حدث خطأ أثناء التحقق"
        }


def get_btc_mode():
    try:
        candles = get_candles("BTC-USDT-SWAP", "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return "🟡 محايد"

        signal_row = get_signal_row(df)
        if signal_row is None:
            return "🟡 محايد"
        ma_value = signal_row.get("ma", None)
        rsi_value = _safe_float(signal_row.get("rsi"), 50)

        if ma_value is not None:
            if _safe_float(signal_row["close"]) > _safe_float(ma_value) and rsi_value >= 55:
                return "🟢 صاعد"
            if _safe_float(signal_row["close"]) < _safe_float(ma_value) and rsi_value <= 45:
                return "🔴 هابط"

        return "🟡 محايد"

    except Exception as e:
        logger.error(f"BTC mode error: {e}")
        return "🟡 محايد"


def is_gaining_intraday_strength(df) -> bool:
    try:
        if df is None or df.empty or len(df) < 5:
            return False

        signal_row = get_signal_row(df)
        if signal_row is None:
            return False
        idx = signal_row.name
        if idx is None or idx < 2:
            return False

        last = df.iloc[idx]
        prev = df.iloc[idx - 1]

        higher_close = _safe_float(last["close"]) >= _safe_float(prev["close"])
        stronger_rsi = _safe_float(last.get("rsi"), 50) >= _safe_float(prev.get("rsi"), 50)
        near_high = _safe_float(last["close"]) > (_safe_float(last["high"]) * 0.994)

        checks = sum([higher_close, stronger_rsi, near_high])
        return checks >= 2
    except Exception:
        return False


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
                if signal_row is None:
                    continue
                close = _safe_float(signal_row["close"])
                ma_value = _safe_float(signal_row.get("ma"), 0)
                rsi_value = _safe_float(signal_row.get("rsi"), 50)

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
    gaining_strength: bool,
) -> float:
    if market_state == "risk_off":
        threshold = 6.7
    elif market_state == "btc_leading":
        threshold = 6.8
    elif market_state == "mixed":
        threshold = 6.5
    elif market_state == "bull_market":
        threshold = 6.35
    elif market_state == "alt_season":
        threshold = 6.0
    else:
        threshold = 6.4

    if mtf_confirmed:
        threshold -= 0.10

    # الفوليوم العالي ليس دائمًا إيجابيًا في اللونج، خصوصًا بعد الحركة
    if market_state == "bull_market":
        if vol_ratio >= 2.0:
            threshold += 0.10
        elif vol_ratio >= 1.5:
            threshold += 0.05
    else:
        if vol_ratio >= 2.0:
            threshold -= 0.10
        elif vol_ratio >= 1.5:
            threshold -= 0.05

    if is_new:
        threshold += 0.10

    if not gaining_strength:
        threshold += 0.10

    if score_result.get("fake_signal"):
        threshold += 0.15

    threshold = max(5.6, min(6.8, threshold))
    return round(threshold, 2)


def calculate_stop_loss(price, atr_value, signal_type="standard"):
    multipliers = {
        "breakout":     2.5,
        "pre_breakout": 3.0,
        "new_listing":  3.2,
        "standard":     2.8,
    }
    multiplier = multipliers.get(signal_type, 2.8)
    try:
        sl = round(float(price) - (float(atr_value) * multiplier), 6)
        min_sl = round(float(price) * 0.990, 6)
        max_sl = round(float(price) * 0.960, 6)
        return max(max_sl, min(min_sl, sl))
    except Exception:
        return round(float(price) * 0.975, 6)


def calculate_sl_percent(entry, sl):
    try:
        return round(((float(entry) - float(sl)) / float(entry)) * 100, 2)
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
        if signal_row is None:
            return 0.0
        high = _safe_float(signal_row["high"])
        low = _safe_float(signal_row["low"])
        open_ = _safe_float(signal_row["open"])
        close = _safe_float(signal_row["close"])

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
        if signal_row is None:
            return 1.0
        idx = signal_row.name
        if idx is None or idx < 1:
            return 1.0

        start_idx = max(0, idx - 20)
        avg_volume = _safe_float(df.iloc[start_idx:idx]["volume"].mean(), 0)
        last_volume = _safe_float(signal_row["volume"], 0)

        if avg_volume <= 0:
            return 1.0

        return round(last_volume / avg_volume, 4)
    except Exception:
        return 1.0


def get_distance_from_ma_percent(df) -> float:
    try:
        signal_row = get_signal_row(df)
        if signal_row is None:
            return 0.0
        close = _safe_float(signal_row["close"], 0)
        ma_value = _safe_float(signal_row.get("ma"), 0)
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
        if signal_row is None:
            return False
        idx = signal_row.name

        if idx is None or idx < max(lookback, PRE_BREAKOUT_BASELINE_VOL_BARS + PRE_BREAKOUT_RECENT_VOL_BARS):
            return False

        close = _safe_float(signal_row["close"])
        ma_value = _safe_float(signal_row.get("ma"), close)
        recent_high = _safe_float(df["high"].iloc[idx - lookback:idx].max())

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

        recent_atr = _safe_float(signal_row.get("atr"), 0)
        prev_atr = _safe_float(df["atr"].iloc[idx - 5:idx].mean(), 0)
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
        if signal_row is None:
            return False
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


def classify_early_priority_long(
    early_signal: bool,
    breakout: bool,
    pre_breakout: bool,
    dist_ma: float,
    vol_ratio: float,
    candle_strength: float,
    mtf_confirmed: bool,
    gaining_strength: bool,
    market_state: str,
) -> str:
    try:
        if not early_signal or breakout or pre_breakout:
            return "none"

        score = 0

        if dist_ma <= 2.8:
            score += 2
        elif dist_ma <= 3.5:
            score += 1

        if vol_ratio >= 1.35:
            score += 2
        elif vol_ratio >= 1.15:
            score += 1

        if candle_strength >= 0.55:
            score += 2
        elif candle_strength >= 0.42:
            score += 1

        if mtf_confirmed:
            score += 2

        if gaining_strength:
            score += 1

        if market_state in ("bull_market", "alt_season"):
            score += 1
        elif market_state in ("risk_off", "btc_leading"):
            score -= 1

        if score >= 6:
            return "strong"
        if score >= 4:
            return "medium"
        return "weak"
    except Exception:
        return "none"


def get_early_priority_score_bonus(priority: str) -> float:
    if priority == "strong":
        return 0.25
    if priority == "medium":
        return 0.10
    if priority == "weak":
        return -0.10
    return 0.0


def get_early_priority_threshold_adjustment(priority: str) -> float:
    if priority == "strong":
        return -0.25
    if priority == "medium":
        return -0.10
    if priority == "weak":
        return 0.10
    return 0.0


def get_early_priority_min_score_adjustment(priority: str) -> float:
    if priority == "strong":
        return -0.20
    if priority == "medium":
        return -0.10
    return 0.0


def get_early_priority_momentum_bonus(priority: str) -> float:
    if priority == "strong":
        return 0.35
    if priority == "medium":
        return 0.15
    if priority == "weak":
        return -0.10
    return 0.0


# =========================
# SETUP TYPE SYSTEM
# =========================
def get_setup_family(candidate: dict) -> str:
    if candidate.get("is_reverse"):
        return "reverse"
    if candidate.get("breakout"):
        return "breakout"
    if candidate.get("pre_breakout"):
        return "pre_breakout"
    return "continuation"


def get_setup_volume_band(vol_ratio: float) -> str:
    try:
        v = float(vol_ratio or 0)
        if v >= 1.80:
            return "vol_high"
        if v >= 1.25:
            return "vol_mid"
        return "vol_low"
    except Exception:
        return "vol_low"


def get_setup_market_regime(market_state: str) -> str:
    allowed = {"bull_market", "alt_season", "mixed", "btc_leading", "risk_off"}
    value = str(market_state or "").strip()
    return value if value in allowed else "mixed"


def build_setup_type(candidate: dict) -> str:
    try:
        family = get_setup_family(candidate)
        mtf = "mtf_yes" if candidate.get("mtf_confirmed") else "mtf_no"
        vol_band = get_setup_volume_band(candidate.get("vol_ratio", 1.0))
        market_regime = get_setup_market_regime(candidate.get("market_state"))
        return f"{family}|{mtf}|{vol_band}|{market_regime}"
    except Exception:
        return "unknown"


def get_hybrid_label_from_stats(setup_stats: dict) -> str:
    try:
        closed = int(setup_stats.get("closed", 0) or 0)
        winrate = float(setup_stats.get("winrate", 0) or 0)

        if closed < 8:
            return f"⚪ No Data ({closed} trades)"

        if winrate >= 70 and closed >= 15:
            return f"🔥 ELITE ({winrate:.0f}% | {closed} trades)"

        if winrate >= 55 and closed >= 8:
            return f"🟢 GOOD ({winrate:.0f}% | {closed} trades)"

        return f"⚠️ WEAK ({winrate:.0f}% | {closed} trades)"
    except Exception:
        return "⚪ No Data (0 trades)"


def get_momentum_priority(
    score: float,
    breakout: bool,
    vol_ratio: float,
    is_new: bool,
    pre_breakout: bool = False,
    dist_ma: float = 0.0,
    gaining_strength: bool = False,
    early_priority: str = "none",
    is_reverse: bool = False,
) -> float:
    priority = float(score)

    if breakout:
        priority += 0.9
    elif pre_breakout:
        priority += 0.6

    if vol_ratio >= 1.8:
        priority += 0.8
    elif vol_ratio >= 1.35:
        priority += 0.4

    if is_new and vol_ratio >= NEW_LISTING_MIN_VOL_RATIO:
        priority += 0.4

    if not is_reverse:
        if dist_ma < -5.2:
            priority -= 0.7
        elif dist_ma < -4.2:
            priority -= 0.25
    else:
        if dist_ma <= -OVERSOLD_REVERSAL_MIN_DIST_MA:
            priority += 0.35

    if gaining_strength:
        priority += 0.20

    priority += get_early_priority_momentum_bonus(early_priority)

    return round(priority, 2)


def get_candidate_bucket(candidate: dict) -> str:
    if candidate.get("is_reverse"):
        return "reverse"
    if candidate["is_new"] and candidate["breakout"]:
        return "new_breakout"
    if candidate.get("pre_breakout") and not candidate["breakout"]:
        return "pre_breakout"
    if candidate["breakout"]:
        return "breakout"
    if candidate.get("early_priority") == "strong":
        return "early_strong"
    if candidate["vol_ratio"] >= 1.8:
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
        key=lambda x: (
            x["momentum_priority"],
            x["score"],
            x["change_24h"],
            x["rank_volume_24h"],
        ),
        reverse=True,
    )

    top_n = max(4, int(len(strong_candidates) * TOP_MOMENTUM_PERCENT))
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
            key=lambda x: (
                x["momentum_priority"],
                x["score"],
                x["change_24h"],
                x["rank_volume_24h"],
            ),
            reverse=True,
        )

    diversified = []
    used_patterns = set()

    for bucket_name in ["reverse", "new_breakout", "pre_breakout", "breakout", "early_strong", "volume", "standard"]:
        if bucket_name not in buckets or not buckets[bucket_name]:
            continue

        candidate = buckets[bucket_name][0]
        pattern = (
            candidate["breakout"],
            candidate.get("pre_breakout", False),
            round(candidate["vol_ratio"], 1),
            candidate["is_new"],
            candidate.get("early_priority", "none"),
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
            key=lambda x: (
                x["momentum_priority"],
                x["score"],
                x["change_24h"],
                x["rank_volume_24h"],
            ),
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
                candidate.get("early_priority", "none"),
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
        "Late Pump Risk": "خطر مطاردة Pump متأخر",
        "Bull Market Continuation Risk": "استمرار في Bull Market بعد امتداد خطر",
        "شمعة قوية لكن احتمال مطاردة": "شمعة قوية لكن احتمال مطاردة",
        "صعود سريع خلال 4 ساعات": "صعود سريع خلال 4 ساعات",
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
        "خطر مطاردة Pump متأخر": 111,
        "استمرار في Bull Market بعد امتداد خطر": 112,
        "شمعة قوية لكن احتمال مطاردة": 113,
        "صعود سريع خلال 4 ساعات": 114,
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

    for rr in normalized:
        if any(k in rr for k in warning_keywords):
            warnings.append(rr)
        else:
            bullish.append(rr)

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
        for rr in bullish:
            if kw in rr and rr not in used:
                highlighted.append(rr)
                used.add(rr)
                break
        if len(highlighted) >= 2:
            break

    formatted = []
    for rr in bullish:
        safe = html.escape(rr)
        line = f"• {safe}"
        if rr in highlighted:
            line = f"• <b>{safe}</b>"
        formatted.append(line)

    return "\n".join(formatted)


def classify_opportunity_type_long(
    breakout: bool,
    pre_breakout: bool,
    dist_ma: float,
    mtf_confirmed: bool,
    is_reverse: bool = False,
) -> str:
    try:
        if is_reverse:
            return "Oversold Reversal"
        if pre_breakout and not breakout:
            return "Breakout مبكر"
        if breakout:
            return "Breakout"
        if dist_ma <= 1.2 and mtf_confirmed:
            return "Pullback"
        return "استمرار"
    except Exception:
        return "استمرار"


def classify_entry_timing_long(
    dist_ma: float,
    breakout: bool,
    pre_breakout: bool,
    vol_ratio: float,
    rsi_now: float = 50.0,
    candle_strength: float = 0.0,
    late_pump_risk: bool = False,
) -> str:
    try:
        # أي Pump واضح لا يصح يتسمى مبكر
        if late_pump_risk:
            return "🔴 متأخر (مطاردة حركة)"

        # امتداد واضح من المتوسط
        if dist_ma > 5.0:
            return "🔴 متأخر (قرب النهاية)"

        # RSI عالي + بعيد نسبيًا = دخول متوسط/متأخر وليس مبكر
        if rsi_now >= 68 and dist_ma > 3.2:
            return "🔴 متأخر (RSI مرتفع)"

        # فوليوم انفجاري + شمعة قوية + السعر بعيد = مطاردة
        if vol_ratio >= 1.9 and candle_strength >= 0.62 and dist_ma > 3.5:
            return "🔴 متأخر (Pump محتمل)"

        # مبكر حقيقي: قريب من MA، RSI غير متضخم، وفوليوم داعم مش انفجاري
        if (pre_breakout or breakout) and dist_ma <= 2.6 and 1.10 <= vol_ratio <= 1.85 and rsi_now <= 66:
            return "🟢 مبكر (بداية الحركة)"

        # Breakout مقبول لكن مش مبكر
        if breakout and 2.6 < dist_ma <= 4.2 and vol_ratio >= 1.20 and rsi_now <= 68:
            return "🟡 متوسط (نص الحركة)"

        # استمرار قريب من المتوسط
        if dist_ma <= 3.2 and vol_ratio >= 1.05 and rsi_now <= 66:
            return "🟡 متوسط (نص الحركة)"

        if 3.2 < dist_ma <= 5.0:
            return "🟡 متوسط (نص الحركة)"

        return "🟡 متوسط (نص الحركة)"
    except Exception:
        return "🟡 متوسط (نص الحركة)"


def get_entry_timing_penalty(entry_timing: str) -> float:
    try:
        if "🔴 متأخر" in entry_timing:
            return 0.25
        if "🟡 متوسط" in entry_timing:
            return 0.10
        return 0.0
    except Exception:
        return 0.0


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


# =========================
# RTL / FORMAT HELPERS
# =========================
def rtl_fix(text: str) -> str:
    try:
        if text is None:
            return ""
        return f"\u200F{text}"
    except Exception:
        return str(text)


def fmt_num(value, decimals=2) -> str:
    try:
        return rtl_fix(f"{float(value):.{int(decimals)}f}")
    except Exception:
        return rtl_fix(str(value))


def fmt_pct(value, decimals=2) -> str:
    try:
        return rtl_fix(f"{float(value):+.{int(decimals)}f}%")
    except Exception:
        return rtl_fix(str(value))


def get_breakout_quality(df, vol_ratio: float) -> str:
    try:
        if df is None or df.empty or len(df) < 5:
            return "none"

        signal_row = get_signal_row(df)
        if signal_row is None:
            return "none"
        idx = signal_row.name
        if idx is None or idx < 3:
            return "none"

        close  = _safe_float(signal_row["close"])
        open_  = _safe_float(signal_row["open"])
        high   = _safe_float(signal_row["high"])
        low    = _safe_float(signal_row["low"])

        candle_range = high - low
        if candle_range <= 0:
            return "none"

        body = abs(close - open_)
        upper_wick = high - max(open_, close)
        close_position = (close - low) / candle_range

        lookback_start = max(0, idx - 20)
        recent_high = float(df["high"].iloc[lookback_start:idx].max())

        bullish_close = close > open_
        broke_above = close > recent_high
        strong_close = close_position >= 0.65
        ok_close = close_position >= 0.50
        small_wick = upper_wick <= body * 0.6
        vol_ok = vol_ratio >= 1.3

        if not bullish_close or not broke_above:
            return "none"

        score = 0
        if strong_close:
            score += 2
        elif ok_close:
            score += 1
        if small_wick:
            score += 1
        if vol_ok:
            score += 1

        if score >= 4:
            return "strong"
        if score >= 2:
            return "ok"
        return "weak"

    except Exception:
        return "none"


# =========================
# SL / TP LOGIC (تم تقريب TP بتقليل نسب RR)
# =========================
def get_rr_targets_long(signal_type="standard", entry_timing=""):
    # قمنا بتقليل RR لجميع الأنواع لجعل TP أقرب
    if signal_type == "breakout":
        return 2.2, 3.5
    if signal_type == "pre_breakout":
        return 2.3, 3.8
    if signal_type == "new_listing":
        return 2.5, 4.0
    if "🔴 متأخر" in entry_timing:
        return 2.0, 3.2
    # افتراضي
    return 2.0, 3.2


def calc_tp_long(entry: float, sl: float, rr: float) -> float:
    risk = float(entry) - float(sl)
    return round(float(entry) + (risk * rr), 6)


# =========================
# BUILD MESSAGE (UPDATED with cleaner pullback)
# =========================
def build_message(
    symbol,
    price,
    score_result,
    stop_loss,
    tp1,
    tp2,
    rr1,
    rr2,
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
    setup_stats=None,
    is_reverse=False,
    reversal_4h_confirmed=False,
    reversal_4h_details="",
    breakout_quality="none",
    pullback_low=None,
    pullback_high=None,
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

    if is_reverse:
        reverse_reason = "Oversold reversal بعد هبوط/امتداد مبالغ فيه"
        if reverse_reason not in bullish:
            bullish = [reverse_reason] + bullish

    bullish_text = format_bullish_reasons(bullish) if bullish else "• زخم مبكر"
    warnings_text = "\n".join(f"• {html.escape(w)}" for w in warnings) if warnings else ""

    funding_text = score_result.get("funding_label", "🟡 محايد")
    signal_rating = score_result.get("signal_rating", "⚡ عادي")
    sl_pct = calculate_sl_percent(price, stop_loss)
    tp1_pct = round(((tp1 - price) / price) * 100, 2) if price else 0.0
    tp2_pct = round(((tp2 - price) / price) * 100, 2) if price else 0.0

    new_tag = "\n🆕 <b>عملة جديدة</b>" if is_new else ""
    reverse_banner = get_reverse_banner_long(is_reverse)
    reverse_note = get_reverse_style_note_long(is_reverse)

    # RTL tokens
    safe_4h  = rtl_fix("4H")
    safe_15m = rtl_fix("15m")
    safe_1h  = rtl_fix("1H")
    safe_24h = rtl_fix("24H")

    # Pullback zone (cleaner formatting)
    pullback_text = ""
    if pullback_low is not None and pullback_high is not None:
        pullback_text = (
            f"📥 <b>منطقة دخول البول باك:</b> "
            f"من {fmt_num(pullback_low, 6)} إلى {fmt_num(pullback_high, 6)}\n"
        )

    # بلوك 4H للـ Oversold Reversal
    if is_reverse:
        if reversal_4h_confirmed:
            reversal_4h_block = (
                f"\n✅ <b>تأكيد فريم {safe_4h}:</b>\n"
                f"{reversal_4h_details}"
            )
        else:
            reversal_4h_block = (
                f"\n🔴 <b>تحذير فريم {safe_4h}:</b> غير مؤكد\n"
                f"{reversal_4h_details}\n"
                f"• مخاطرة أعلى — راجع شارت {safe_4h} قبل الدخول"
            )
    else:
        reversal_4h_block = ""

    # بلوك جودة الـ breakout
    bq_map = {
        "strong": f"🟢 كسر قوي",
        "ok":     f"🟡 كسر مقبول",
        "weak":   f"🔴 كسر ضعيف — تحقق قبل الدخول",
    }
    bq_label = bq_map.get(breakout_quality, "")
    breakout_quality_block = f"\n🧩 <b>جودة الكسر:</b> {bq_label}" if bq_label else ""

    safe_symbol           = html.escape(symbol_clean)
    safe_market           = html.escape(build_market_summary(btc_mode=btc_mode, alt_mode=alt_mode or "🟡 متماسك"))
    safe_funding          = html.escape(funding_text)
    safe_rating           = html.escape(signal_rating)
    safe_tv_link          = html.escape(tv_link, quote=True)
    safe_opportunity_type = html.escape(opportunity_type)
    safe_entry_timing     = html.escape(entry_timing)
    safe_display_risk     = html.escape(display_risk)

    warnings_block = f"\n\n⚠️ <b>ملاحظات:</b>\n{warnings_text}" if warnings_text else ""
    news_block     = f"\n\n{news_warning}" if news_warning else ""
    reverse_block  = f"\n{reverse_note}" if reverse_note else ""

    hybrid_label = html.escape(
        get_hybrid_label_from_stats(setup_stats or {})
    )

    header_block = f"{hybrid_label}\n\n" if hybrid_label else ""
    if reverse_banner:
        header_block += f"{reverse_banner}\n\n"

    return f'''{header_block}🚀 <b>لونج فيوتشر | {safe_symbol}</b>

💰 <b>السعر:</b> {fmt_num(price, 6)} | ⏱ <b>الفريم:</b> {safe_15m}
⭐ <b>السكور:</b> {rtl_fix(f"{float(score_result['score']):.1f} / 10")}
🏷 <b>التصنيف:</b> {safe_rating}
{pullback_text}
🎯 <b>TP1:</b> {fmt_num(tp1, 6)} ({fmt_pct(tp1_pct)} | {rtl_fix(f"{rr1}R")})
🏁 <b>TP2:</b> {fmt_num(tp2, 6)} ({fmt_pct(tp2_pct)} | {rtl_fix(f"{rr2}R")})
🛑 <b>SL:</b> {fmt_num(stop_loss, 6)} ({rtl_fix(f"-{abs(float(sl_pct)):.2f}%")})

🧠 <b>نوع الفرصة:</b> {safe_opportunity_type}{reverse_block}{reversal_4h_block}{breakout_quality_block}

🌍 <b>السوق:</b> {safe_market}
💸 <b>التمويل:</b> {safe_funding}
📈 <b>تغير {safe_24h}:</b> {fmt_pct(change_24h)}{new_tag}

📊 <b>أسباب الدخول:</b>
{bullish_text}{warnings_block}{news_block}

📍 <b>الدخول:</b> {safe_entry_timing}
⚖️ <b>المخاطرة:</b> {safe_display_risk}

🔗 <a href="{safe_tv_link}">Open Chart ({safe_15m} / {safe_1h})</a>'''


# =========================
# TELEGRAM LOOP
# =========================
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

                if update.get("callback_query"):
                    handle_callback_query(update["callback_query"])
                    continue

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
# MAIN LOOP
# =========================
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
                logger.info("GLOBAL COOLDOWN (Redis) — skipping long scan")
                time.sleep(60)
                continue

            scan_locked = acquire_scan_lock()
            if not scan_locked:
                logger.info("Another long scan is running — skipping")
                time.sleep(30)
                continue

            logger.info(f"LONG RUN START | pid={os.getpid()}")

            update_open_trades(r, market_type="futures", side="long", timeframe=TIMEFRAME)
            winrate_summary = get_winrate_summary(r, market_type="futures", side="long")
            logger.info(format_winrate_summary(winrate_summary))

            stats_reset_ts = None
            if r:
                try:
                    raw_reset = r.get(STATS_RESET_TS_KEY)
                    if raw_reset:
                        stats_reset_ts = int(raw_reset)
                except Exception:
                    pass

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

            logger.info(
                f"LONG MARKET STATE | btc={btc_mode} | alt={alt_mode} | "
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
                candle_strength = get_candle_strength_ratio(df)
                gaining_strength = is_gaining_intraday_strength(df)
                breakout_quality = get_breakout_quality(df, vol_ratio)

                # --- حساب is_reverse أولاً (لأن RSI تحتاجها) ---
                is_reverse = is_oversold_reversal_long(
                    df=df,
                    dist_ma=dist_ma,
                    change_24h=change_24h,
                    vol_ratio=vol_ratio,
                    funding=funding,
                )

                # --- الآن RSI filters (بعد is_reverse) ---
                signal_row = get_signal_row(df)
                if signal_row is None:
                    logger.info(f"{symbol} → skipped (no valid signal row)")
                    continue

                rsi_now = _safe_float(signal_row.get("rsi"), 50)
                if rsi_now > 75 and not is_reverse:
                    logger.info(f"{symbol} → skipped (RSI > 75 extreme peak)")
                    continue
                if rsi_now > 70 and dist_ma > 4:
                    logger.info(f"{symbol} → skipped (RSI > 70 and dist_ma > 4)")
                    continue
                if dist_ma > 3.5:
                    early_signal = False

                # --- PHASE 3 LITE: حساب BB و 4H change ---
                above_upper_bb = is_above_upper_bollinger(df)
                change_4h = get_change_4h(df)

                # === Late Pump / Bull Market Guard ===
                temp_opportunity_type = classify_opportunity_type_long(
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    dist_ma=dist_ma,
                    mtf_confirmed=mtf_confirmed,
                    is_reverse=is_reverse,
                )

                late_guard = detect_late_pump_long(
                    market_state=market_state,
                    opportunity_type=temp_opportunity_type,
                    dist_ma=dist_ma,
                    rsi_now=rsi_now,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    breakout_quality=breakout_quality,
                    change_4h=change_4h,
                )

                if late_guard.get("should_block") and not is_reverse:
                    logger.info(
                        f"{symbol} → skipped (late/bull continuation guard | "
                        f"market={market_state} | opp={temp_opportunity_type} | "
                        f"dist_ma={dist_ma:.2f} | rsi={rsi_now:.1f} | "
                        f"vol={vol_ratio:.2f} | candle={candle_strength:.2f} | "
                        f"bq={breakout_quality} | reasons={late_guard.get('reasons')})"
                    )
                    continue

                # === FILTER 1: Bollinger (SAFE) ===
                if above_upper_bb and not pre_breakout and not is_reverse:
                    if not breakout or breakout_quality == "weak":
                        logger.info(
                            f"{symbol} → skipped (upper BB | breakout={breakout} | "
                            f"pre_breakout={pre_breakout} | breakout_quality={breakout_quality} | "
                            f"reverse={is_reverse})"
                        )
                        continue

                # --- Pullback zone calculation (using signal_row index) ---
                signal_idx = signal_row.name
                lookback_start = max(0, signal_idx - 20)
                recent_high = _safe_float(df["high"].iloc[lookback_start:signal_idx].max(), 0)
                atr_value = _safe_float(signal_row.get("atr"), 0)
                pullback_low = None
                pullback_high = None
                if atr_value > 0 and recent_high > 0:
                    pullback_low = recent_high - (atr_value * 0.15)
                    pullback_high = recent_high + (atr_value * 0.35)
                # Show pullback zone only for breakout or pre_breakout
                if not breakout and not pre_breakout:
                    pullback_low = None
                    pullback_high = None

                # --- باقي الفلترة والتقييم ---
                reversal_4h_result = {"confirmed": False, "checks": 0, "details": ""}
                if is_reverse:
                    reversal_4h_result = is_4h_oversold_confirmed(symbol)
                    if not reversal_4h_result["confirmed"]:
                        logger.info(
                            f"{symbol} → REVERSAL 4H NOT CONFIRMED | "
                            f"{reversal_4h_result.get('details', '')} | "
                            f"سيُرسل بتحذير مرتفع"
                        )

                if vol_ratio < 1.02 and not breakout and not pre_breakout and not early_signal:
                    logger.info(f"{symbol} → skipped (hard floor vol_ratio too low: {vol_ratio:.2f})")
                    continue

                if is_late_long_entry(
                    dist_ma=dist_ma,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                ) and not is_reverse:
                    logger.info(f"{symbol} → skipped (late long entry | dist_ma={dist_ma:.2f})")
                    continue

                if is_exhausted_long_move(
                    dist_ma=dist_ma,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                ) and not is_reverse:
                    logger.info(
                        f"{symbol} → skipped (exhausted move | dist_ma={dist_ma:.2f} | "
                        f"vol_ratio={vol_ratio:.2f} | candle_strength={candle_strength:.2f})"
                    )
                    continue

                early_priority = classify_early_priority_long(
                    early_signal=early_signal,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    dist_ma=dist_ma,
                    vol_ratio=vol_ratio,
                    candle_strength=candle_strength,
                    mtf_confirmed=mtf_confirmed,
                    gaining_strength=gaining_strength,
                    market_state=market_state,
                )

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
                        is_reverse=is_reverse,
                    )
                except Exception as score_err:
                    logger.error(f"{symbol} → calculate_long_score failed: {score_err}")
                    continue

                raw_score = float(score_result.get("score", 0))
                effective_score = raw_score

                # أضف تحذيرات late pump داخل الرسالة والتسجيل
                score_result = append_late_pump_warnings(score_result, late_guard)

                # عقوبة للـ late pump بدل مكافأة الحركة المتأخرة
                if late_guard.get("extreme_late_pump") and not is_reverse:
                    effective_score -= EXTREME_LATE_PUMP_SCORE_PENALTY
                elif late_guard.get("late_pump_risk") and not is_reverse:
                    effective_score -= LATE_PUMP_SCORE_PENALTY

                if late_guard.get("bull_continuation_risk") and not is_reverse:
                    effective_score -= BULL_CONTINUATION_SCORE_PENALTY

                if score_result.get("fake_signal"):
                    if breakout or pre_breakout:
                        effective_score -= 0.20
                    elif early_signal:
                        effective_score -= 0.15
                    else:
                        effective_score -= 0.30

                if not gaining_strength and not breakout and not pre_breakout:
                    effective_score -= 0.15

                effective_score += get_early_priority_score_bonus(early_priority)

                # لا نكافئ الفوليوم العالي لو فيه late pump أو bull continuation risk
                if breakout and not late_guard.get("late_pump_risk") and not late_guard.get("bull_continuation_risk"):
                    if vol_ratio >= 1.5:
                        effective_score += 0.30
                    elif vol_ratio >= 1.3:
                        effective_score += 0.15

                if is_reverse:
                    effective_score += OVERSOLD_REVERSAL_SCORE_BONUS

                score_result["score"] = round(effective_score, 2)

                dynamic_threshold = get_dynamic_entry_threshold(
                    market_state=market_state,
                    score_result=score_result,
                    vol_ratio=vol_ratio,
                    mtf_confirmed=mtf_confirmed,
                    is_new=is_new,
                    gaining_strength=gaining_strength,
                )

                if dist_ma < -4.4 and not breakout and not pre_breakout and not is_reverse:
                    dynamic_threshold += 0.15

                if candle_strength < 0.45 and dist_ma < -4.0 and not breakout and not pre_breakout and not is_reverse:
                    dynamic_threshold += 0.10

                dynamic_threshold += get_early_priority_threshold_adjustment(early_priority)
                dynamic_threshold = round(dynamic_threshold, 2)

                # تم إزالة strong_early_override لعدم استخدامه
                if score_result["score"] < dynamic_threshold:
                    if early_priority != "strong":
                        logger.info(
                            f"{symbol} → rejected (score<{dynamic_threshold} | early_priority={early_priority})"
                        )
                        continue

                if has_high_impact_news:
                    if "warning_reasons" not in score_result:
                        score_result["warning_reasons"] = []

                    if "أخبار اقتصادية مهمة قريبة" not in score_result["warning_reasons"]:
                        score_result["warning_reasons"].append("أخبار اقتصادية مهمة قريبة")

                pre_breakout_only = pre_breakout and not early_signal
                required_min_score = FINAL_MIN_SCORE + (0.1 if pre_breakout_only else 0)

                opportunity_type = classify_opportunity_type_long(
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    dist_ma=dist_ma,
                    mtf_confirmed=mtf_confirmed,
                    is_reverse=is_reverse,
                )
                entry_timing = classify_entry_timing_long(
                    dist_ma=dist_ma,
                    breakout=breakout,
                    pre_breakout=pre_breakout,
                    vol_ratio=vol_ratio,
                    rsi_now=rsi_now,
                    candle_strength=candle_strength,
                    late_pump_risk=late_guard.get("late_pump_risk", False),
                )
                timing_penalty = get_entry_timing_penalty(entry_timing)
                effective_required_min_score = required_min_score + timing_penalty
                effective_required_min_score += get_early_priority_min_score_adjustment(early_priority)
                effective_required_min_score = get_effective_min_score_with_reverse(
                    effective_required_min_score,
                    is_reverse=is_reverse,
                )

                # === FILTER 2: MTF Late Trap ===
                if mtf_confirmed and "🔴 متأخر" in entry_timing and not is_reverse:
                    logger.info(f"{symbol} → skipped (MTF late)")
                    continue

                # === FILTER 3: MTF Chasing Move ===
                if mtf_confirmed and change_4h > 3 and not breakout and not pre_breakout and not is_reverse:
                    logger.info(f"{symbol} → skipped (chasing 4h move)")
                    continue

                logger.info(
                    f"{symbol} → early_signal: {early_signal} | "
                    f"early_priority={early_priority} | "
                    f"pre_b={pre_breakout} | "
                    f"score={score_result['score']} | "
                    f"min_required={effective_required_min_score} | "
                    f"dyn={dynamic_threshold} | "
                    f"fake={score_result.get('fake_signal')} | "
                    f"mtf={mtf_confirmed} | "
                    f"reverse={is_reverse} | "
                    f"market={market_state} | dist_ma={dist_ma:.2f} | vol={vol_ratio:.2f}"
                )

                if score_result["score"] < effective_required_min_score:
                    logger.info(
                        f"{symbol} → rejected by effective min score "
                        f"({score_result['score']} < {effective_required_min_score})"
                    )
                    continue

                if not breakout and not pre_breakout and dist_ma > 6.2 and not is_reverse:
                    logger.info(f"{symbol} → rejected (late move without breakout/pre-breakout | dist_ma={dist_ma:.2f})")
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

                if is_new:
                    if not passes_new_listing_filter(
                        score=float(score_result["score"]),
                        breakout=breakout or pre_breakout,
                        vol_ratio=vol_ratio,
                        candle_strength=candle_strength,
                    ):
                        logger.info(f"{symbol} → rejected by balanced new listing filter")
                        continue

                price = _safe_float(signal_row["close"], 0)

                if breakout:
                    sl_type = "breakout"
                elif pre_breakout:
                    sl_type = "pre_breakout"
                elif is_new:
                    sl_type = "new_listing"
                else:
                    sl_type = "standard"

                stop_loss = calculate_stop_loss(price, atr_value, signal_type=sl_type)
                rr1, rr2 = get_rr_targets_long(signal_type=sl_type, entry_timing=entry_timing)
                tp1 = calc_tp_long(price, stop_loss, rr=rr1)
                tp2 = calc_tp_long(price, stop_loss, rr=rr2)
                tv_link = build_tradingview_link(symbol)

                explicit_warnings = score_result.get("warning_reasons") or []
                _, inferred_warnings = classify_reasons(score_result.get("reasons", []))
                warnings_count = len(explicit_warnings) if explicit_warnings else len(inferred_warnings)
                base_risk = get_base_risk_label(score_result, warnings_count)
                display_risk = adjust_risk_with_entry_timing(base_risk, entry_timing)

                if "🔴 متأخر" in entry_timing and "🔴 عالية" in display_risk and not breakout and vol_ratio < 1.2:
                    logger.info(
                        f"{symbol} → rejected (late + high risk + no breakout + low vol | "
                        f"dist_ma={dist_ma:.2f} | vol={vol_ratio:.2f})"
                    )
                    continue

                momentum_priority = get_momentum_priority(
                    score=float(score_result["score"]),
                    breakout=breakout,
                    vol_ratio=vol_ratio,
                    is_new=is_new,
                    pre_breakout=pre_breakout,
                    dist_ma=dist_ma,
                    gaining_strength=gaining_strength,
                    early_priority=early_priority,
                    is_reverse=is_reverse,
                )

                # عقوبة momentum للـ late pump حتى لا تتفوق في الترتيب
                if late_guard.get("extreme_late_pump") and not is_reverse:
                    momentum_priority -= 0.90
                elif late_guard.get("late_pump_risk") and not is_reverse:
                    momentum_priority -= 0.60

                if late_guard.get("bull_continuation_risk") and not is_reverse:
                    momentum_priority -= 0.40

                momentum_priority = round(momentum_priority, 2)

                alert_id = build_alert_id(symbol, candle_time)

                setup_type_candidate = {
                    "is_reverse": is_reverse,
                    "breakout": breakout,
                    "pre_breakout": pre_breakout,
                    "mtf_confirmed": mtf_confirmed,
                    "vol_ratio": vol_ratio,
                    "market_state": market_state,
                }
                setup_type = build_setup_type(setup_type_candidate)
                setup_stats = get_setup_type_stats(
                    redis_client=r,
                    market_type="futures",
                    side="long",
                    setup_type=setup_type,
                    since_ts=stats_reset_ts,
                )
                logger.info(
                    f"{symbol} → setup_type={setup_type} | "
                    f"closed={setup_stats.get('closed', 0)} | "
                    f"winrate={setup_stats.get('winrate', 0)}%"
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
                    "early_priority": early_priority,
                    "is_reverse": is_reverse,
                    "setup_type": setup_type,
                    "raw_score": raw_score,
                    "dynamic_threshold": dynamic_threshold,
                    "required_min_score": effective_required_min_score,
                    "dist_ma": dist_ma,
                    "entry_timing": entry_timing,
                    "opportunity_type": opportunity_type,
                    "market_state": market_state,
                    "market_state_label": market_state_label,
                    "market_bias_label": market_bias_label,
                    "breakout_quality": breakout_quality,
                    "fake_signal": bool(score_result.get("fake_signal", False)),
                    "reversal_4h_confirmed": reversal_4h_result.get("confirmed", False),
                    "has_high_impact_news": has_high_impact_news,
                    "warning_reasons": score_result.get("warning_reasons", []),
                    "news_titles": [e.get("title", "") for e in upcoming_events[:3]],
                    "pullback_low": pullback_low,
                    "pullback_high": pullback_high,
                    "above_upper_bb": above_upper_bb,
                    "change_4h": change_4h,
                    "late_pump_risk": late_guard.get("late_pump_risk", False),
                    "extreme_late_pump": late_guard.get("extreme_late_pump", False),
                    "bull_continuation_risk": late_guard.get("bull_continuation_risk", False),
                    "late_guard_reasons": late_guard.get("reasons", []),
                    "rsi_now": rsi_now,
                    "message": build_message(
                        symbol=symbol,
                        price=price,
                        score_result=score_result,
                        stop_loss=stop_loss,
                        tp1=tp1,
                        tp2=tp2,
                        rr1=rr1,
                        rr2=rr2,
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
                        setup_stats=setup_stats,
                        is_reverse=is_reverse,
                        reversal_4h_confirmed=reversal_4h_result.get("confirmed", False),
                        reversal_4h_details=reversal_4h_result.get("details", ""),
                        breakout_quality=breakout_quality,
                        pullback_low=pullback_low,
                        pullback_high=pullback_high,
                    ),
                    "reply_markup": build_track_reply_markup(alert_id),
                    "alert_id": alert_id,
                    "alert_snapshot": {
                        "alert_id": alert_id,
                        "symbol": symbol,
                        "timeframe": TIMEFRAME,
                        "entry": price,
                        "sl": stop_loss,
                        "tp1": tp1,
                        "tp2": tp2,
                        "score": float(score_result["score"]),
                        "candle_time": candle_time,
                        "created_ts": int(time.time()),
                        "market_state": market_state,
                        "alt_mode": alt_mode,
                        "btc_mode": btc_mode,
                        "entry_timing": entry_timing,
                        "opportunity_type": opportunity_type,
                        "early_priority": early_priority,
                        "is_reverse": is_reverse,
                        "setup_type": setup_type,
                        "above_upper_bb": above_upper_bb,
                        "change_4h": change_4h,
                        "late_pump_risk": late_guard.get("late_pump_risk", False),
                        "bull_continuation_risk": late_guard.get("bull_continuation_risk", False),
                        "rsi_now": rsi_now,
                        "dist_ma": dist_ma,
                        "vol_ratio": vol_ratio,
                        "pullback_low": pullback_low,
                        "pullback_high": pullback_high,
                    },
                    "candle_time": candle_time,
                    "now": now,
                    "entry": price,
                    "sl": stop_loss,
                    "tp1": tp1,
                    "tp2": tp2,
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

            logger.info(f"Long candidates found before momentum filter: {len(candidates)}")

            candidates = apply_top_momentum_filter(candidates)
            logger.info(f"Long candidates found after momentum filter: {len(candidates)}")

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

                sent_data = send_telegram_message(
                    candidate["message"],
                    reply_markup=candidate.get("reply_markup"),
                )

                if sent_data.get("ok"):
                    sent_symbols_this_run.add(symbol)
                    sent_count += 1
                    sent_cache[symbol] = time.time()
                    last_candle_cache[symbol] = candidate["candle_time"]
                    last_global_send_ts = time.time()

                    message_id = str(((sent_data.get("result") or {}).get("message_id")) or "")
                    save_alert_snapshot(candidate.get("alert_snapshot", {}), message_id=message_id)

                    trade_reasons = list(candidate["reasons"] or [])
                    if candidate.get("is_reverse"):
                        if "OVERSOLD_REVERSAL" not in trade_reasons:
                            trade_reasons.append("OVERSOLD_REVERSAL")

                    register_trade(
                        redis_client=r,
                        symbol=symbol,
                        market_type="futures",
                        side="long",
                        candle_time=candidate["candle_time"],
                        entry=candidate["entry"],
                        sl=candidate["sl"],
                        tp1=candidate["tp1"],
                        tp2=candidate["tp2"],
                        score=candidate["score"],
                        timeframe=TIMEFRAME,
                        btc_mode=btc_mode,
                        funding_label=candidate["funding_label"],
                        reasons=trade_reasons,
                        pre_breakout=candidate["pre_breakout"],
                        breakout=candidate["breakout"],
                        vol_ratio=candidate["vol_ratio"],
                        candle_strength=candidate["candle_strength"],
                        mtf_confirmed=candidate["mtf_confirmed"],
                        is_new=candidate["is_new"],
                        btc_dominance_proxy=candidate["btc_dominance_proxy"],
                        change_24h=candidate["change_24h"],
                        setup_type=candidate.get("setup_type", "unknown"),
                        raw_score=candidate.get("raw_score", candidate["score"]),
                        effective_score=candidate["score"],
                        dynamic_threshold=candidate.get("dynamic_threshold", 0.0),
                        required_min_score=candidate.get("required_min_score", 0.0),
                        dist_ma=candidate.get("dist_ma", 0.0),
                        entry_timing=candidate.get("entry_timing", ""),
                        opportunity_type=candidate.get("opportunity_type", ""),
                        market_state=candidate.get("market_state", ""),
                        market_state_label=candidate.get("market_state_label", ""),
                        market_bias_label=candidate.get("market_bias_label", ""),
                        alt_mode=candidate.get("alt_mode", ""),
                        early_priority=candidate.get("early_priority", ""),
                        breakout_quality=candidate.get("breakout_quality", ""),
                        fake_signal=candidate.get("fake_signal", False),
                        is_reverse_signal=candidate.get("is_reverse", False),
                        reversal_4h_confirmed=candidate.get("reversal_4h_confirmed", False),
                        rank_volume_24h=candidate.get("rank_volume_24h", 0.0),
                        alert_id=candidate.get("alert_id", ""),
                        has_high_impact_news=candidate.get("has_high_impact_news", False),
                        news_titles=candidate.get("news_titles", []),
                        warning_reasons=candidate.get("warning_reasons", []),
                        pullback_low=candidate.get("pullback_low"),
                        pullback_high=candidate.get("pullback_high"),
                        pullback_entry=candidate.get("pullback_low"),
                    )

                    logger.info(
                        f"SENT LONG → {symbol} | score: {candidate['score']} | "
                        f"momentum: {candidate['momentum_priority']} | "
                        f"bucket: {candidate['bucket']} | reverse={candidate.get('is_reverse', False)} | "
                        f"setup_type={candidate.get('setup_type', 'unknown')} | "
                        f"new={candidate['is_new']} | "
                        f"early_priority={candidate.get('early_priority', 'none')} | "
                        f"market={candidate['market_state']} | alt={candidate['alt_mode']} | "
                        f"alert_id={candidate['alert_id']}"
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
                logger.info(f"Global long cooldown set for {GLOBAL_COOLDOWN_SECONDS}s after {sent_count} alert(s)")

            logger.info(f"Sent long alerts this run: {sent_count}")
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
        f"LONG BOT STARTED | pid={os.getpid()} | replica={os.getenv('RAILWAY_REPLICA_ID', 'unknown')}"
    )

    clear_webhook()

    command_thread = threading.Thread(target=run_command_poller, daemon=True)
    command_thread.start()

    run_scanner_loop()


if __name__ == "__main__":
    run()
