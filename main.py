import sys
import os
import time
import redis
import html

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.okx_client import get_tickers, get_candles
from services.telegram_sender import send_telegram_message
from analysis.indicators import to_dataframe, add_ma, add_rsi, add_atr
from analysis.long_strategy import early_bullish_signal
from analysis.scoring import calculate_long_score, is_breakout

# =========================
# SETTINGS
# =========================
COOLDOWN_SECONDS = 3600
MAX_ALERTS_PER_RUN = 2
SCAN_LIMIT = 200
MIN_24H_QUOTE_VOLUME = 1_000_000
NEW_LISTING_MAX_CANDLES = 50

REDIS_URL = os.environ.get("REDIS_URL")

r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        print("✅ Redis connected")
    except Exception as e:
        print(f"❌ Redis connection error: {e}")
        r = None
else:
    print("⚠️ REDIS_URL not found")


# =========================
# REDIS
# =========================
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
        print(f"Redis exists error (same candle): {e}")
        return False


def in_cooldown(symbol: str, signal_type: str = "long") -> bool:
    if not r:
        return False
    try:
        return bool(r.exists(get_cooldown_key(symbol, signal_type)))
    except Exception as e:
        print(f"Redis exists error (cooldown): {e}")
        return False


def reserve_signal_slot(symbol: str, candle_time: int, signal_type: str = "long") -> bool:
    """
    حجز Atomic قبل الإرسال:
    1) نفس الشمعة
    2) نفس الزوج لمدة ساعة
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
        print(f"Redis reserve error: {e}")
        return False


def release_signal_slot(symbol: str, candle_time: int, signal_type: str = "long") -> None:
    if not r:
        return
    try:
        r.delete(get_same_candle_key(symbol, candle_time, signal_type))
        r.delete(get_cooldown_key(symbol, signal_type))
    except Exception as e:
        print(f"Redis release error: {e}")


# =========================
# MARKET FILTERING
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
    candidate_fields = [
        "volCcy24h",
        "turnover24h",
        "quoteVolume",
        "vol24h",
    ]
    for field in candidate_fields:
        value = ticker.get(field)
        if value is None:
            continue
        try:
            return float(value)
        except Exception:
            continue
    return 0.0


def get_ranked_pairs():
    futures = get_tickers("SWAP")
    print(f"Fetched {len(futures)} futures pairs")

    filtered = []
    for p in futures:
        symbol = p.get("instId", "")

        if "USDT" not in symbol:
            continue
        if not symbol.endswith("-SWAP"):
            continue
        if is_excluded_symbol(symbol):
            continue

        vol_24h = extract_24h_quote_volume(p)
        if vol_24h < MIN_24H_QUOTE_VOLUME:
            continue

        p["_rank_volume_24h"] = vol_24h
        filtered.append(p)

    filtered.sort(key=lambda x: x.get("_rank_volume_24h", 0), reverse=True)
    top_pairs = filtered[:SCAN_LIMIT]

    print(f"After liquidity filter: {len(filtered)}")
    print(f"Using top ranked pairs: {len(top_pairs)}")
    return top_pairs


# =========================
# HELPERS
# =========================
def get_last_candle_time(df):
    try:
        ts = int(df["ts"].iloc[-1])
        if ts > 10_000_000_000:
            return ts // 1000
        return ts
    except Exception:
        return int(time.time() // (15 * 60))


def get_btc_mode():
    try:
        candles = get_candles("BTC-USDT-SWAP", "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return "🟡 محايد"

        df = add_ma(df)
        df = add_rsi(df)

        last = df.iloc[-1]
        ma_value = last.get("ma", None)
        rsi_value = float(last.get("rsi", 50))

        if ma_value is not None:
            if last["close"] > ma_value and rsi_value >= 55:
                return "🟢 صاعد (داعم)"
            if last["close"] < ma_value and rsi_value <= 45:
                return "🔴 هابط (ضاغط)"
        return "🟡 محايد"

    except Exception as e:
        print(f"BTC mode error: {e}")
        return "🟡 محايد"


def is_higher_timeframe_confirmed(symbol):
    try:
        candles = get_candles(symbol, "1H", 100)
        df = to_dataframe(candles)

        if df is None or df.empty:
            return False

        df = add_ma(df)
        df = add_rsi(df)

        last = df.iloc[-1]
        ma_value = last.get("ma", None)

        score = 0
        if ma_value is not None and last["close"] > ma_value:
            score += 1
        if float(last.get("rsi", 0)) > 50:
            score += 1

        return score >= 1

    except Exception as e:
        print(f"MTF error on {symbol}: {e}")
        return False


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
    base = symbol.replace("-USDT-SWAP", "").replace("-SWAP", "").replace("-", "")
    tv_symbol = f"OKX:{base}USDT.P"
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"


def build_message(symbol, price, score_result, stop_loss, btc_mode, tv_link, is_new):
    symbol_clean = clean_symbol_for_message(symbol)

    reason_text = " + ".join(score_result["reasons"]) if score_result["reasons"] else "زخم مبكر"
    flags_text = " | ".join(score_result["flags"]) if score_result["flags"] else "Setup"

    new_tag = "\n🆕 <b>عملة جديدة</b>" if is_new else ""

    safe_symbol = html.escape(symbol_clean)
    safe_btc = html.escape(btc_mode)
    safe_reason = html.escape(reason_text)
    safe_flags = html.escape(flags_text)
    safe_tv_link = html.escape(tv_link, quote=True)

    return f"""🚀 <b>لونج فيوتشر | {safe_symbol}</b>

💰 {price:.6f} | ⏱ 15m
⭐ {score_result["score"]:.1f} / 10 | 🛑 {stop_loss}

🪙 BTC: {safe_btc}{new_tag}

📊 {safe_reason}

🔥 {safe_flags}

🔗 <a href="{safe_tv_link}">Open Chart</a>
"""


# =========================
# MAIN
# =========================
def run():
    print("🚀 Bot Started...")

    btc_mode = get_btc_mode()
    print(f"BTC mode: {btc_mode}")

    ranked_pairs = get_ranked_pairs()

    tested = 0
    sent_symbols_this_run = set()
    candidates = []

    for pair_data in ranked_pairs:
        tested += 1
        symbol = pair_data["instId"]

        try:
            candles = get_candles(symbol, "15m", 100)
            df = to_dataframe(candles)

            if df is None or df.empty:
                print(f"{symbol} → empty dataframe")
                continue

            df = add_ma(df)
            df = add_rsi(df)
            df = add_atr(df)

            breakout = is_breakout(df, lookback=20)
            mtf_confirmed = is_higher_timeframe_confirmed(symbol)
            is_new = is_new_listing_by_candles(candles)

            # فلتر مبكر
            signal = early_bullish_signal(df)
            if not signal:
                print(f"{symbol} → signal: False")
                continue

            score_result = calculate_long_score(
                df=df,
                mtf_confirmed=mtf_confirmed,
                btc_mode=btc_mode,
                breakout=breakout,
                is_new=is_new,
            )

            print(
                f"{symbol} → signal: True | "
                f"score: {score_result['score']} | "
                f"fake: {score_result['fake_signal']} | "
                f"breakout: {breakout} | "
                f"mtf: {mtf_confirmed} | "
                f"new: {is_new}"
            )

            if score_result["fake_signal"]:
                continue

            if score_result["score"] < 7.5:
                continue

            candle_time = get_last_candle_time(df)

            if symbol in sent_symbols_this_run:
                print(f"{symbol} → skipped (already sent this run)")
                continue

            if already_sent_same_candle(symbol, candle_time, "long"):
                print(f"{symbol} → skipped (same candle in Redis)")
                continue

            if in_cooldown(symbol, "long"):
                print(f"{symbol} → skipped (cooldown in Redis)")
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

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    candidates.sort(
        key=lambda x: (x["score"], x["rank_volume_24h"]),
        reverse=True
    )

    top_candidates = candidates[:MAX_ALERTS_PER_RUN]

    sent_count = 0

    for candidate in top_candidates:
        symbol = candidate["symbol"]

        if symbol in sent_symbols_this_run:
            print(f"{symbol} → skipped (already sent final stage)")
            continue

        locked = reserve_signal_slot(
            symbol=symbol,
            candle_time=candidate["candle_time"],
            signal_type="long",
        )
        if not locked:
            print(f"{symbol} → skipped (reserve failed / duplicate)")
            continue

        sent_ok = send_telegram_message(candidate["message"])

        if sent_ok:
            sent_symbols_this_run.add(symbol)
            sent_count += 1
            print(f'SENT → {symbol} | score: {candidate["score"]}')
        else:
            release_signal_slot(
                symbol=symbol,
                candle_time=candidate["candle_time"],
                signal_type="long",
            )
            print(f'FAILED SEND → {symbol}')

    print(f"Candidates found: {len(candidates)}")
    print(f"Sent alerts this run: {sent_count}")
    print(f"Tested {tested} pairs")


if __name__ == "__main__":
    while True:
        try:
            run()
        except Exception as e:
            print(f"Fatal error: {e}")

        print("Sleeping 60 seconds...")
        time.sleep(60)
