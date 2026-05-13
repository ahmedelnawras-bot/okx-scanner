"""Pair selection close to the old bot philosophy.

Key preserved ideas from the reference files:
- liquidity filter first
- merged ranked pairs, not top-volume only
- balanced mix of volume, positive momentum, and negative reversal names
- avoid over-choking the universe before candle analysis
"""
from __future__ import annotations

from .models import PairCandidate
from utils.safe_helpers import safe_float


EXCLUDED_SYMBOL_HINTS = ("USDC", "USDE", "FDUSD", "TUSD", "BUSD", "DAI")
MAJOR_SYMBOLS = ("BTC-", "ETH-", "SOL-", "XRP-", "DOGE-", "BNB-", "AVAX-", "LINK-")


def _infer_relative_strength(symbol: str, change_pct: float, turnover: float) -> bool:
    return bool(
        change_pct >= 1.8
        or (change_pct >= 0.8 and symbol.startswith(("BTC-", "ETH-", "SOL-", "LINK-", "AVAX-")))
        or (change_pct >= 1.0 and turnover >= 20_000_000)
    )



def _infer_near_resistance(change_pct: float, turnover: float) -> bool:
    return change_pct >= 4.2 and turnover < 30_000_000



def _base_candidate(raw: dict) -> PairCandidate | None:
    symbol = str(raw.get("instId") or raw.get("symbol") or "")
    if not symbol.endswith("-USDT-SWAP") or any(h in symbol for h in EXCLUDED_SYMBOL_HINTS):
        return None

    last_price = safe_float(raw.get("last") or raw.get("lastPrice"))
    turnover = safe_float(raw.get("volCcy24h") or raw.get("turnover_usdt") or raw.get("quoteVolume"))
    change_pct = safe_float(raw.get("change_pct") or raw.get("changePercent") or raw.get("_rank_change_24h") or raw.get("sodUtc8"))
    if abs(change_pct) > 100:
        change_pct = change_pct / 100.0

    score_hint = min(turnover / 2_500_000.0, 10.0)
    score_hint += max(change_pct, 0.0) * 0.60
    rebound_hint = max(-change_pct, 0.0) * 0.55

    tags: list[str] = []
    if turnover >= 3_000_000:
        tags.append("liquid")
    if any(symbol.startswith(prefix) for prefix in MAJOR_SYMBOLS):
        tags.append("major")
    if change_pct >= 1.25:
        tags.append("momentum")
    if change_pct >= 3.2:
        tags.append("breakout")
    if -5.5 <= change_pct <= -0.9:
        tags.append("rebound")
    if abs(change_pct) <= 0.7:
        tags.append("compression")
    if 0.75 <= change_pct <= 2.8:
        tags.append("continuation")
    if _infer_relative_strength(symbol, change_pct, turnover):
        tags.append("rs_btc")
    if _infer_near_resistance(change_pct, turnover):
        tags.append("near_resistance")

    return PairCandidate(
        symbol=symbol,
        last_price=last_price,
        change_pct=change_pct,
        turnover_usdt=turnover,
        score_hint=round(score_hint, 3),
        rebound_hint=round(rebound_hint, 3),
        tags=tags,
    )



def select_ranked_pairs(raw_tickers: list[dict], scan_limit: int = 80) -> list[PairCandidate]:
    """Use merged ranked pairs like the old bot.

    Reference behavior copied conceptually from the older files:
    - 35% volume leaders
    - 25% positive momentum
    - 25% negative reversal names
    - then fill from liquid names
    We keep a few RS/continuation names alive so strong mode does not choke formation too early.
    """
    base_candidates = [_base_candidate(t) for t in raw_tickers]
    candidates = [c for c in base_candidates if c and c.last_price > 0 and c.turnover_usdt >= 500_000]

    by_volume = sorted(candidates, key=lambda c: c.turnover_usdt, reverse=True)
    by_momentum = sorted(candidates, key=lambda c: c.change_pct, reverse=True)
    by_reversal = sorted(candidates, key=lambda c: c.change_pct)
    by_rs = sorted([c for c in candidates if "rs_btc" in c.tags], key=lambda c: (c.score_hint, c.turnover_usdt), reverse=True)
    by_continuation = sorted([c for c in candidates if "continuation" in c.tags], key=lambda c: (c.score_hint, c.turnover_usdt), reverse=True)

    n_vol = max(10, int(scan_limit * 0.35))
    n_momentum = max(8, int(scan_limit * 0.25))
    n_reversal = max(8, int(scan_limit * 0.25))

    merged: list[PairCandidate] = []
    seen: set[str] = set()

    def add(item: PairCandidate) -> bool:
        if item.symbol in seen:
            return False
        seen.add(item.symbol)
        merged.append(item)
        return True

    for item in by_volume[:n_vol]:
        add(item)

    positive_momentum_count = 0
    for item in by_momentum[: n_momentum * 2]:
        if positive_momentum_count >= n_momentum:
            break
        if item.change_pct > 0 and add(item):
            positive_momentum_count += 1

    negative_reversal_count = 0
    for item in by_reversal[: n_reversal * 2]:
        if negative_reversal_count >= n_reversal:
            break
        if item.change_pct < 0 and add(item):
            negative_reversal_count += 1

    # Keep some strong relative-strength/continuation names alive even if volume buckets filled first.
    for bucket in (by_rs[: max(6, scan_limit // 8)], by_continuation[: max(6, scan_limit // 8)]):
        for item in bucket:
            if len(merged) >= scan_limit:
                break
            add(item)

    for item in by_volume:
        if len(merged) >= scan_limit:
            break
        add(item)

    merged = merged[:scan_limit]
    return sorted(
        merged,
        key=lambda c: (
            c.score_hint
            + c.rebound_hint
            + (0.45 if "liquid" in c.tags else 0.0)
            + (0.35 if "rs_btc" in c.tags else 0.0)
            + (0.20 if "major" in c.tags else 0.0)
            - (0.15 if "near_resistance" in c.tags else 0.0),
            c.turnover_usdt,
        ),
        reverse=True,
    )
