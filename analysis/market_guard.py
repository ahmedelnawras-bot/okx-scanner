"""Candle-based Market Guard snapshot builder with BTC Dominance helper.
"""
from __future__ import annotations

from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from datetime import datetime, timezone

from analysis.market_modes import MarketSnapshot


MARKET_GUARD_SAMPLE_SIZE = 50
MARKET_GUARD_MIN_VALID = 20
MARKET_GUARD_TIMEFRAME = "15m"
STRONG_15M_THRESHOLD = 0.40
HOURLY_MA_GUARD_SYMBOL = "BTC-USDT-SWAP"
HOURLY_MA_GUARD_BAR = "30m"
HOURLY_MA5_PRESSURE_GAP_PCT = -0.25

# CoinGecko endpoint for global data (no API key required, rate limited)
COINGECKO_GLOBAL_URL = "https://api.coingecko.com/api/v3/global"
DOMINANCE_CACHE: dict = {
    "current": None,
    "current_ts": 0.0,
    "history": [],  # [(timestamp, dominance)]
}


@dataclass
class GuardChange:
    symbol: str
    change_pct: float
    turnover_usdt: float = 0.0


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _symbol_of(pair) -> str:
    return str(getattr(pair, "symbol", "") or getattr(pair, "instId", "") or "")


def _turnover_of(pair) -> float:
    return _safe_float(getattr(pair, "turnover_usdt", 0.0), 0.0)


def select_market_guard_sample(ranked_pairs, limit: int = MARKET_GUARD_SAMPLE_SIZE) -> list:
    pairs = [p for p in list(ranked_pairs or []) if _symbol_of(p).endswith("-USDT-SWAP")]
    pairs = sorted(pairs, key=_turnover_of, reverse=True)
    sample: list = []
    seen: set[str] = set()
    btc = next((p for p in pairs if _symbol_of(p).startswith("BTC-")), None)
    if btc is not None:
        sample.append(btc)
        seen.add(_symbol_of(btc))
    for pair in pairs:
        if len(sample) >= max(1, int(limit)):
            break
        symbol = _symbol_of(pair)
        if symbol in seen:
            continue
        seen.add(symbol)
        sample.append(pair)
    return sample


def fetch_okx_candles(base_url: str, symbol: str, bar: str = MARKET_GUARD_TIMEFRAME, limit: int = 3, timeout: int = 15) -> list[list]:
    url = f"{base_url}/api/v5/market/candles"
    params = {"instId": symbol, "bar": bar, "limit": str(limit)}
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", []) if isinstance(payload, dict) else []
        return data if isinstance(data, list) else []
    except Exception:
        return []


def get_last_closed_candle_change_pct(candles: list[list]) -> float | None:
    if not candles:
        return None
    row = candles[1] if len(candles) > 1 else candles[0]
    if not isinstance(row, (list, tuple)) or len(row) < 5:
        return None
    open_ = _safe_float(row[1])
    close = _safe_float(row[4])
    if open_ <= 0 or close <= 0:
        return None
    return ((close - open_) / open_) * 100.0


def fetch_btc_dominance_change_1h() -> float:
    """Return BTC dominance change over last ~1 hour (percentage points).

    يجيب BTC.D من CoinGecko ويحسب الفرق عن ساعة سابقة.
    - Cache كل 10 دقايق لتجنب rate limits
    - بيحتفظ بـ history لآخر ساعتين
    - أول ساعة تشغيل → يرجع 0.0 لحد ما يتجمع بيانات كافية
    """
    global DOMINANCE_CACHE
    try:
        import time as _time
        now = _time.time()

        # لو الـ cache حديث (أقل من 10 دقايق) → مش نجيب من API
        if now - DOMINANCE_CACHE["current_ts"] >= 600 or DOMINANCE_CACHE["current"] is None:
            resp = requests.get(COINGECKO_GLOBAL_URL, timeout=10)
            if resp.status_code != 200:
                return 0.0
            data = resp.json()
            if not isinstance(data, dict):
                return 0.0
            market_cap_percentage = data.get("data", {}).get("market_cap_percentage", {})
            btc_dom = _safe_float(market_cap_percentage.get("btc", 0.0), 0.0)
            if btc_dom <= 0:
                return 0.0

            DOMINANCE_CACHE["current"] = btc_dom
            DOMINANCE_CACHE["current_ts"] = now
            DOMINANCE_CACHE["history"].append((now, btc_dom))

            # احتفظ بآخر ساعتين فقط
            cutoff = now - 7200
            DOMINANCE_CACHE["history"] = [
                (ts, d) for ts, d in DOMINANCE_CACHE["history"] if ts >= cutoff
            ]

        current = DOMINANCE_CACHE["current"]
        if current is None:
            return 0.0

        # ابحث عن قيمة قبل ~1 ساعة (±30 دقيقة)
        target_ts = now - 3600
        candidates = [
            (ts, d) for ts, d in DOMINANCE_CACHE["history"]
            if abs(ts - target_ts) <= 1800
        ]
        if not candidates:
            return 0.0

        closest = min(candidates, key=lambda x: abs(x[0] - target_ts))
        return round(current - closest[1], 3)

    except Exception:
        return 0.0


def _calc_hourly_ma5_guard(candles: list[list]) -> dict:
    closed = [row for row in (candles or [])[1:] if isinstance(row, (list, tuple)) and len(row) >= 5]
    closes = [_safe_float(row[4]) for row in closed]
    closes = [x for x in closes if x > 0]
    if len(closes) < 5:
        return {
            "hourly_ma5_pressure": False,
            "btc_1h_close": 0.0,
            "btc_1h_ma5": 0.0,
            "btc_1h_ma5_gap_pct": 0.0,
            "hourly_ma_guard_source": "unavailable",
        }
    close = closes[0]
    ma5 = sum(closes[1:6]) / 5.0 if len(closes) >= 6 else sum(closes[:5]) / 5.0
    gap_pct = ((close - ma5) / ma5) * 100.0 if ma5 > 0 else 0.0
    previous_below_ma5 = False
    if len(closes) >= 6:
        prev_close = closes[1]
        prev_ma5 = sum(closes[2:7]) / 5.0 if len(closes) >= 7 else sum(closes[1:6]) / 5.0  # ✅ FIX: previous window
        previous_below_ma5 = prev_ma5 > 0 and prev_close < prev_ma5
    ma5_below_ma10 = False
    if len(closes) >= 10:
        ma10 = sum(closes[:10]) / 10.0
        ma5_below_ma10 = ma5 < ma10
    pressure = bool(gap_pct <= HOURLY_MA5_PRESSURE_GAP_PCT and (previous_below_ma5 or ma5_below_ma10))
    return {
        "hourly_ma5_pressure": pressure,
        "btc_1h_close": close,
        "btc_1h_ma5": ma5,
        "btc_1h_ma5_gap_pct": gap_pct,
        "hourly_ma_guard_source": "btc_1h_ma5",
    }


def attach_hourly_ma5_guard(snapshot: MarketSnapshot, base_url: str, timeout: int = 15) -> MarketSnapshot:
    candles = fetch_okx_candles(base_url, HOURLY_MA_GUARD_SYMBOL, bar=HOURLY_MA_GUARD_BAR, limit=15, timeout=timeout)
    guard = _calc_hourly_ma5_guard(candles)
    for key, value in guard.items():
        setattr(snapshot, key, value)
    try:
        snapshot.hourly_ma5_pressure = bool(guard.get("hourly_ma5_pressure"))
        snapshot.btc_1h_close = float(guard.get("btc_1h_close") or 0.0)
        snapshot.btc_1h_ma5 = float(guard.get("btc_1h_ma5") or 0.0)
        snapshot.btc_1h_ma5_gap_pct = float(guard.get("btc_1h_ma5_gap_pct") or 0.0)
    except Exception:
        pass
    return snapshot


def _fallback_from_pair_change(ranked_pairs) -> MarketSnapshot:
    sample = select_market_guard_sample(ranked_pairs, limit=30)
    if not sample:
        snap = MarketSnapshot()
        setattr(snap, "market_guard_source", "fallback_empty")
        return snap
    changes = [_safe_float(getattr(p, "change_pct", 0.0), 0.0) for p in sample]
    red_count = sum(1 for x in changes if x < 0)
    avg_change = sum(changes) / max(1, len(changes))
    strong_count = sum(1 for x in changes if x >= 0.40)
    btc_change = next((_safe_float(getattr(p, "change_pct", 0.0), avg_change) for p in sample if _symbol_of(p).startswith("BTC-")), avg_change)
    snap = MarketSnapshot(
        btc_change_15m=btc_change,
        red_ratio_15m=red_count / max(1, len(changes)),
        avg_change_15m=avg_change,
        strong_coins_count=strong_count,
        fast_rebound=bool(avg_change > 0.20 and strong_count >= 6 and (red_count / max(1, len(changes))) <= 0.58),
        btc_reclaim=bool(btc_change > -0.15),
        breadth_improving=bool((red_count / max(1, len(changes))) <= 0.62 and avg_change > -0.55),
    )
    setattr(snap, "market_guard_source", "fallback_pair_change")
    setattr(snap, "market_guard_valid_count", len(changes))
    setattr(snap, "market_guard_red_count", red_count)
    # Add dominance field with fallback
    snap.btc_dominance_change_1h = 0.0
    return snap


def build_market_guard_snapshot(
    ranked_pairs,
    base_url: str,
    timeout: int = 15,
    sample_size: int = MARKET_GUARD_SAMPLE_SIZE,
    min_valid: int = MARKET_GUARD_MIN_VALID,
    timeframe: str = MARKET_GUARD_TIMEFRAME,
    debug: bool = True,
    verbose: bool = False,
) -> MarketSnapshot:
    """Build a MarketSnapshot from real candle changes."""
    sample = select_market_guard_sample(ranked_pairs, limit=sample_size)
    changes: list[GuardChange] = []

    def _fetch_pair_change(pair):
        symbol = _symbol_of(pair)
        candles = fetch_okx_candles(base_url, symbol, bar=timeframe, limit=3, timeout=timeout)
        change = get_last_closed_candle_change_pct(candles)
        if change is None:
            return None
        return GuardChange(symbol=symbol, change_pct=change, turnover_usdt=_turnover_of(pair))

    max_workers = min(12, max(4, len(sample)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_pair_change, pair) for pair in sample]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    changes.append(result)
            except Exception:
                continue

    if len(changes) < max(1, int(min_valid)):
        snap = _fallback_from_pair_change(ranked_pairs)
        if debug:
            print(
                "📊 MODE SNAPSHOT DEBUG | "
                f"Source={getattr(snap, 'market_guard_source', 'fallback')} | "
                f"Sample={len(sample)} | Valid candles={len(changes)} < {min_valid} | "
                f"Fallback avg={snap.avg_change_15m:.2f}% | Red={snap.red_ratio_15m*100:.0f}%",
                flush=True,
            )
        return snap

    red_count = sum(1 for item in changes if item.change_pct < 0)
    avg_change = sum(item.change_pct for item in changes) / max(1, len(changes))
    red_ratio = red_count / max(1, len(changes))
    strong_count = sum(1 for item in changes if item.change_pct >= STRONG_15M_THRESHOLD)
    btc_change = next((item.change_pct for item in changes if item.symbol.startswith("BTC-")), avg_change)

    fast_rebound = bool(avg_change > 0.20 and strong_count >= 6 and red_ratio <= 0.58 and btc_change > -0.40)
    btc_reclaim = bool(btc_change > -0.15)
    breadth_improving = bool(red_ratio <= 0.62 and avg_change > -0.55)

    # Fetch BTC dominance change over last hour
    dominance_change = fetch_btc_dominance_change_1h()

    snap = MarketSnapshot(
        btc_change_15m=btc_change,
        red_ratio_15m=red_ratio,
        avg_change_15m=avg_change,
        strong_coins_count=strong_count,
        fast_rebound=fast_rebound,
        btc_reclaim=btc_reclaim,
        breadth_improving=breadth_improving,
        btc_dominance_change_1h=dominance_change,
    )
    setattr(snap, "market_guard_source", f"candles_{timeframe}")
    setattr(snap, "market_guard_sample_size", len(sample))
    setattr(snap, "market_guard_valid_count", len(changes))
    setattr(snap, "market_guard_red_count", red_count)
    attach_hourly_ma5_guard(snap, base_url=base_url, timeout=timeout)

    if debug:
        gainers = sorted(changes, key=lambda x: x.change_pct, reverse=True)[:5]
        losers = sorted(changes, key=lambda x: x.change_pct)[:5]
        gainers_txt = ", ".join(f"{x.symbol.replace('-USDT-SWAP','')} {x.change_pct:+.2f}%" for x in gainers)
        losers_txt = ", ".join(f"{x.symbol.replace('-USDT-SWAP','')} {x.change_pct:+.2f}%" for x in losers)
        print(
            "📊 MODE SNAPSHOT DEBUG | "
            f"Source=candles_{timeframe} | Sample={len(sample)} | Valid={len(changes)} | "
            f"Red={red_count}/{len(changes)} ({red_ratio*100:.0f}%) | "
            f"Avg15m={avg_change:+.2f}% | BTC15m={btc_change:+.2f}% | Strong={strong_count} | "
            f"DomChange1h={dominance_change:+.2f}% | "
            f"FastRebound={fast_rebound} | BTCReclaim={btc_reclaim} | BreadthImproving={breadth_improving} | "
            f"1hMA5Pressure={getattr(snap, 'hourly_ma5_pressure', False)} (gap={getattr(snap, 'btc_1h_ma5_gap_pct', 0.0):+.2f}%)",
            flush=True,
        )
        if verbose:
            print(f"📈 Guard top gainers: {gainers_txt}", flush=True)
            print(f"📉 Guard top losers: {losers_txt}", flush=True)

    return snap


# ========== إضافة alias للتوافق مع main.py ==========
build_market_guard = build_market_guard_snapshot
