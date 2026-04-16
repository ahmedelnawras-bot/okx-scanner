import os
import time
import html
import logging
import requests
import pandas as pd
import redis

from analysis.scoring import calculate_long_score, is_breakout

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
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

SCAN_LIMIT = 200
TIMEFRAME = "15m"
HTF_TIMEFRAME = "1H"

MIN_SCORE = 5.0
MAX_ALERTS_PER_RUN = 3
COOLDOWN_SECONDS = 3600
MIN_24H_QUOTE_VOLUME = 1_000_000
NEW_LISTING_MAX_CANDLES = 50

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


def clean_symbol_for_message(symbol: str) -> str:
    return symbol.replace("-SWAP", "")


def get_same_candle_key(symbol: str, candle_time: int, signal_type: str = "long") -> str:
    return f"sent:{signal_type}:{symbol}:{candle_time}"


def get_cooldown_key(symbol: str, signal_type: str = "long") -> str:
    clean = clean_symbol_for_message(symbol)
    return f"cooldown:{signal_type}:{clean}"


def already_sent_same_candle(symbol: str, candle_time: int, signal_type: str = "long") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_same_candle_key(symbol, candle_time, signal_type)))
    except Exception as e:
        logger.error(f"Redis exists error (same candle): {e}")
        return False


def in_cooldown(symbol: str, signal_type: str = "long") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_cooldown_key(symbol, signal_type)))
    except Exception as e:
        logger.error(f"Redis exists error (cooldown): {e}")
        return False


def reserve_signal_slot(symbol: str, candle_time: int, signal_type: str = "long") -> bool:
    """
    حجز Atomic قبل الإرسال:
    - نفس الشمعة
    - نفس الزوج لنفس النوع أثناء الكولداون
    """
    if not r:
        return True

    same_candle_key = get_same_candle_key(symbol, candle_time, signal_type)
    cooldown_key = get_cooldown_key(symbol, signal_type)

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
        r.delete(get_cooldown_key(symbol, signal_type))
    except Exception as e:
        logger.error(f"Redis release error: {e}")


# =========================
# TELEGRAM
# =========================
def send_telegram_message(message: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        logger.error("❌ Telegram config missing")
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


# =========================
# OKX DATA
# =========================
def is_excluded_symbol(symbol: str) -> bool:
    excluded_prefixes = (
        "USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP", "USD0"
    )
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


def get_ranked_pairs():
    try:
        res = requests.get(OKX_TICKERS_URL, params={"instType": "SWAP"}, timeout=20).json()
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
    gain = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()

    rs = gain / loss
    return 100 - (100 / (1 + rs))


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

    numeric_cols = ["ts", "open", "high", "low", "close", "volume", "volCcy", "volCcyQuote"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("ts").reset_index(drop=True)

    df["ma"] = df["close"].rolling(20).mean()
    df["rsi"] = compute_rsi(df["close"])
    df["atr"] = compute_atr(df)

    return df


def get_candles(symbol, timeframe="15m", limit=100):
    try:
        params = {
            "instId": symbol,
            "bar": timeframe,
            "limit": limit
        }
        res = requests.get(OKX_CANDLES_URL, params=params, timeout=20).json()
        return res.get("data", [])
    except Exception as e:
        logger.error(f"get_candles error on {symbol} {timeframe}: {e}")
        return []


def get_last_candle_time(df):
    try:
        ts = int(df["ts"].iloc[-1])
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

        last = df.iloc[-1]
        prev = df.iloc[-2]

        score = 0

        if float(last["close"]) > float(last["open"]):
            score += 1

        if "rsi" in df.columns and float(last["rsi"]) > 50:
            score += 1

        if float(last["volume"]) > float(prev["volume"]):
            score += 1

        return score >= 1

    except Exception:
        return False


def is_higher_timeframe_confirmed(symbol):
    try:
        candles = get_candles(symbol, HTF_TIMEFRAME, 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return False

        last = df.iloc[-1]
        ma_value = last.get("ma", None)

        score = 0
        if ma_value is not None and float(last["close"]) > float(ma_value):
            score += 1
        if float(last.get("rsi", 0)) > 50:
            score += 1

        return score >= 1

    except Exception as e:
        logger.error(f"MTF error on {symbol}: {e}")
        return False


def get_btc_mode():
    try:
        candles = get_candles("BTC-USDT-SWAP", "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return "🟡 محايد"

        last = df.iloc[-1]
        ma_value = last.get("ma", None)
        rsi_value = float(last.get("rsi", 50))

        if ma_value is not None:
            if float(last["close"]) > float(ma_value) and rsi_value >= 55:
                return "🟢 صاعد (داعم)"
            if float(last["close"]) < float(ma_value) and rsi_value <= 45:
                return "🔴 هابط (ضاغط)"

        return "🟡 محايد"

    except Exception as e:
        logger.error(f"BTC mode error: {e}")
        return "🟡 محايد"


def calculate_stop_loss(price, atr_value):
    try:
        return round(float(price) - (float(atr_value) * 1.2), 6)
    except Exception:
        return round(float(price), 6)


def is_new_listing_by_candles(candles) -> bool:
    try:
        return len(candles) < NEW_LISTING_MAX_CANDLES
    except Exception:
        return False


def build_tradingview_link(symbol):
    # مثال: MINA-USDT-SWAP -> OKX:MINAUSDT.P
    base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "").replace("-", "")
    tv_symbol = f"OKX:{base}USDT.P"
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"


def build_message(symbol, price, score_result, stop_loss, btc_mode, tv_link, is_new):
    symbol_clean = clean_symbol_for_message(symbol)
    reasons = " + ".join(score_result["reasons"]) if score_result["reasons"] else "زخم مبكر"
    flags = " | ".join(score_result["flags"]) if score_result["flags"] else "Setup"

    new_tag = "\n🆕 <b>عملة جديدة</b>" if is_new else ""

    safe_symbol = html.escape(symbol_clean)
    safe_btc = html.escape(btc_mode)
    safe_reasons = html.escape(reasons)
    safe_flags = html.escape(flags)
    safe_tv_link = html.escape(tv_link, quote=True)

    return f"""🚀 <b>لونج فيوتشر | {safe_symbol}</b>

💰 {price:.6f} | ⏱ 15m
⭐ {score_result["score"]:.1f} / 10 | 🛑 {stop_loss}

🪙 BTC: {safe_btc}{new_tag}

📊 {safe_reasons}

🔥 {safe_flags}

🔗 <a href="{safe_tv_link}">Open Chart</a>
"""


# =========================
# MAIN LOOP
# =========================
def run():
    while True:
        try:
            logger.info("🚀 Bot Started...")

            ranked_pairs = get_ranked_pairs()
            btc_mode = get_btc_mode()

            tested = 0
            sent_count = 0
            sent_symbols_this_run = set()
            candidates = []

            for pair_data in ranked_pairs:
                tested += 1
                symbol = pair_data["instId"]

                candles = get_candles(symbol, TIMEFRAME, 100)
                df = to_dataframe(candles)

                if df is None or df.empty:
                    continue

                signal = early_bullish_signal(df)
                if not signal:
                    logger.info(f"{symbol} → signal: False")
                    continue

                breakout = is_breakout(df)
                mtf_confirmed = is_higher_timeframe_confirmed(symbol)
                is_new = is_new_listing_by_candles(candles)

                score_result = calculate_long_score(
                    df=df,
                    mtf_confirmed=mtf_confirmed,
                    btc_mode=btc_mode,
                    breakout=breakout,
                    is_new=is_new
                )

                logger.info(
                    f"{symbol} → signal: True | "
                    f"score: {score_result['score']} | "
                    f"fake: {score_result['fake_signal']} | "
                    f"breakout: {breakout} | "
                    f"mtf: {mtf_confirmed} | "
                    f"new: {is_new}"
                )

                if score_result["fake_signal"]:
                    logger.info(f"{symbol} → rejected by fake signal")
                    continue

                if score_result["score"] < MIN_SCORE:
                    logger.info(f"{symbol} → rejected by score ({score_result['score']})")
                    continue

                candle_time = get_last_candle_time(df)

                if symbol in sent_symbols_this_run:
                    logger.info(f"{symbol} → skipped (already sent this run)")
                    continue

                if already_sent_same_candle(symbol, candle_time, "long"):
                    logger.info(f"{symbol} → skipped (same candle in Redis)")
                    continue

                if in_cooldown(symbol, "long"):
                    logger.info(f"{symbol} → skipped (cooldown in Redis)")
                    continue

                price = float(df["close"].iloc[-1])
                atr_value = float(df["atr"].iloc[-1])
                stop_loss = calculate_stop_loss(price, atr_value)
                tv_link = build_tradingview_link(symbol)

                candidates.append({
                    "symbol": symbol,
                    "score": float(score_result["score"]),
                    "rank_volume_24h": float(pair_data.get("_rank_volume_24h", 0)),
                    "message": build_message(
                        symbol=symbol,
                        price=price,
                        score_result=score_result,
                        stop_loss=stop_loss,
                        btc_mode=btc_mode,
                        tv_link=tv_link,
                        is_new=is_new,
                    ),
                    "candle_time": candle_time,
                })

            candidates.sort(
                key=lambda x: (x["score"], x["rank_volume_24h"]),
                reverse=True
            )

            top_candidates = candidates[:MAX_ALERTS_PER_RUN]

            for candidate in top_candidates:
                symbol = candidate["symbol"]

                if symbol in sent_symbols_this_run:
                    logger.info(f"{symbol} → skipped (already sent final stage)")
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
                    logger.info(f"SENT → {symbol} | score: {candidate['score']}")
                else:
                    release_signal_slot(
                        symbol=symbol,
                        candle_time=candidate["candle_time"],
                        signal_type="long",
                    )
                    logger.error(f"FAILED SEND → {symbol}")

            logger.info(f"Candidates found: {len(candidates)}")
            logger.info(f"Sent alerts this run: {sent_count}")
            logger.info(f"Tested {tested} pairs")
            logger.info("Sleeping 60 seconds...")

            time.sleep(60)

        except Exception as e:
            logger.error(f"Fatal error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    run()
