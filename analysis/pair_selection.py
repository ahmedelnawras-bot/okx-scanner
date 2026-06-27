"""Pair selection close to the old bot philosophy.

Key preserved ideas from the reference files:
- liquidity filter first
- merged ranked pairs, not top-volume only
- balanced mix of volume, positive momentum, and negative reversal names
- avoid over-choking the universe before candle analysis

Phase update:
- reduce over-concentration on the most traded OKX names
- keep diversity across turnover tiers without touching scoring architecture
- cap hyper-liquid dominance in the selection/final ranking layer only
"""
from __future__ import annotations

from .models import PairCandidate
from utils.safe_helpers import safe_float


EXCLUDED_SYMBOL_HINTS = ("USDC", "USDE", "FDUSD", "TUSD", "BUSD", "DAI")
MAJOR_SYMBOLS = ("BTC-", "ETH-", "SOL-", "XRP-", "DOGE-", "BNB-", "AVAX-", "LINK-")

MEGA_LIQUID_TURNOVER = 60_000_000
HIGH_LIQUID_TURNOVER = 20_000_000
MID_LIQUID_TURNOVER = 5_000_000
FINAL_TURNOVER_TIE_CAP = 20_000_000

# ── Relative Strength (مقابل BTC فعلاً) ─────────────────────────────────────────
# العملة لازم تتفوّق على BTC بهذا الهامش عشان تُعتبر قوة نسبية حقيقية.
RS_OUTPERFORM_MARGIN_PCT = 1.0
# لو BTC نازل أكثر من كده (24h) → الترند هابط → نلغي RS (مصيدة "أقوى فاقد").
RS_DOWNTREND_BTC_PCT = -2.0


def _compute_change_pct(raw: dict) -> float:
    """حساب نسبة التغيّر من بيانات الـ ticker (نفس منطق _base_candidate).

    مستقلة عشان نقدر نحسب تغيّر BTC قبل بناء المرشحين بدون تكرار.
    """
    last_price = safe_float(raw.get("last") or raw.get("lastPrice"))
    explicit_change = raw.get("change_pct") or raw.get("changePercent") or raw.get("_rank_change_24h")
    if explicit_change is not None and explicit_change != "":
        change_pct = safe_float(explicit_change)
        if -1.0 <= change_pct <= 1.0 and any(k in raw for k in ("changePercent", "chgPct")):
            change_pct *= 100.0
        elif abs(change_pct) > 100:
            change_pct = change_pct / 100.0
        return change_pct
    ref_price = safe_float(raw.get("open24h") or raw.get("sodUtc8") or raw.get("sodUtc0") or raw.get("open"))
    return ((last_price - ref_price) / ref_price * 100.0) if ref_price > 0 and last_price > 0 else 0.0


def _extract_btc_change(raw_tickers: list[dict]) -> float:
    """تغيّر BTC من نفس قائمة الـ tickers (بدون API call إضافي ولا مشكلة ترتيب)."""
    for raw in raw_tickers or []:
        symbol = str(raw.get("instId") or raw.get("symbol") or "")
        if symbol.upper().startswith("BTC-USDT"):
            return _compute_change_pct(raw)
    return 0.0


def _infer_relative_strength(
    symbol: str,
    change_pct: float,
    turnover: float,
    btc_change: float = 0.0,
) -> bool:
    """قوة نسبية حقيقية مقابل BTC (مش مجرد "العملة طالعة").

    القديم كان بيعتمد على change_pct لوحده، فأي عملة طالعة لحظياً في سوق هابط
    بتاخد tag rs_btc → wave_3 → دخول → SL. ده فخ السوق الهابط.

    الجديد:
    - العملة لازم تتفوّق على BTC بهامش حقيقي (outperformance فعلي).
    - في الترند الهابط (BTC نازل بقوة): "أقوى فاقد" مش سبب شراء → نلغي RS
      أو نطلب تفوّق أكبر بكتير. ده متوافق مع فلسفة فلتر الاتجاه.
    """
    # ترند هابط واضح على BTC (24h) → RS مش ميزة، دي مصيدة.
    if btc_change <= RS_DOWNTREND_BTC_PCT:
        return False

    outperforms_btc = change_pct >= (btc_change + RS_OUTPERFORM_MARGIN_PCT)
    if not outperforms_btc:
        return False

    # لازم العملة نفسها موجبة فعلاً (مش مجرد أقل سلبية من BTC).
    return bool(
        change_pct >= 1.0
        and (
            change_pct >= 1.8
            or (change_pct >= 0.8 and symbol.startswith(("BTC-", "ETH-", "SOL-", "LINK-", "AVAX-")))
            or (change_pct >= 1.0 and turnover >= 20_000_000)
        )
    )


def _infer_near_resistance(change_pct: float, turnover: float) -> bool:
    return change_pct >= 4.2 and turnover < 30_000_000


def _turnover_tier(turnover: float) -> str:
    if turnover >= MEGA_LIQUID_TURNOVER:
        return "mega"
    if turnover >= HIGH_LIQUID_TURNOVER:
        return "high"
    if turnover >= MID_LIQUID_TURNOVER:
        return "mid"
    return "base"


def _tier_caps(scan_limit: int) -> dict[str, int]:
    """Dynamic diversity caps.

    These caps are selection-only safeguards so the scan universe does not get
    over-dominated by the hyper-liquid OKX names every cycle.
    """
    return {
        "mega": max(4, scan_limit // 12),
        "high": max(8, scan_limit // 8),
        "mid": max(16, scan_limit // 5),
        "base": scan_limit,
    }


def _selection_key(item: PairCandidate) -> tuple[float, float, float]:
    diversity_bonus = 0.15 if MID_LIQUID_TURNOVER <= item.turnover_usdt <= FINAL_TURNOVER_TIE_CAP and "major" not in item.tags else 0.0
    turnover_tie = min(float(item.turnover_usdt or 0.0), float(FINAL_TURNOVER_TIE_CAP))
    return (
        item.score_hint
        + item.rebound_hint
        + (0.45 if "liquid" in item.tags else 0.0)
        + (0.35 if "rs_btc" in item.tags else 0.0)
        + (0.20 if "major" in item.tags else 0.0)
        - (0.15 if "near_resistance" in item.tags else 0.0)
        + diversity_bonus,
        abs(float(item.change_pct or 0.0)),
        turnover_tie,
    )


def _base_candidate(raw: dict, btc_change: float = 0.0) -> PairCandidate | None:
    symbol = str(raw.get("instId") or raw.get("symbol") or "")
    if not symbol.endswith("-USDT-SWAP") or any(h in symbol for h in EXCLUDED_SYMBOL_HINTS):
        return None

    last_price = safe_float(raw.get("last") or raw.get("lastPrice"))
    turnover = safe_float(raw.get("volCcy24h") or raw.get("turnover_usdt") or raw.get("quoteVolume"))

    change_pct = _compute_change_pct(raw)

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
    if _infer_relative_strength(symbol, change_pct, turnover, btc_change):
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
    """Use merged ranked pairs like the old bot, with better diversity.

    Preserved behavior:
    - 35% volume leaders
    - 25% positive momentum
    - 25% negative reversal names
    - then fill from liquid names

    Diversity protection added:
    - hyper-liquid symbols cannot dominate the whole scan universe
    - major names are softly capped in the selection stage
    - final tie-breaking caps turnover influence instead of letting the most
      traded pairs always sit at the top
    """
    btc_change = _extract_btc_change(raw_tickers)
    base_candidates = [_base_candidate(t, btc_change) for t in raw_tickers]
    candidates = [c for c in base_candidates if c and c.last_price > 0 and c.turnover_usdt >= 500_000]

    by_volume = sorted(candidates, key=lambda c: c.turnover_usdt, reverse=True)
    by_momentum = sorted(candidates, key=lambda c: c.change_pct, reverse=True)
    by_reversal = sorted(candidates, key=lambda c: c.change_pct)
    by_rs = sorted([c for c in candidates if "rs_btc" in c.tags], key=_selection_key, reverse=True)
    by_continuation = sorted([c for c in candidates if "continuation" in c.tags], key=_selection_key, reverse=True)
    by_mid_liquidity = sorted(
        [c for c in candidates if MID_LIQUID_TURNOVER <= c.turnover_usdt < MEGA_LIQUID_TURNOVER],
        key=_selection_key,
        reverse=True,
    )

    n_vol = max(10, int(scan_limit * 0.35))
    n_momentum = max(8, int(scan_limit * 0.25))
    n_reversal = max(8, int(scan_limit * 0.25))

    merged: list[PairCandidate] = []
    seen: set[str] = set()
    tier_counts = {"mega": 0, "high": 0, "mid": 0, "base": 0}
    caps = _tier_caps(scan_limit)
    major_count = 0
    max_major = max(8, scan_limit // 10)

    def add(item: PairCandidate, *, force: bool = False) -> bool:
        nonlocal major_count

        if item.symbol in seen:
            return False

        tier = _turnover_tier(item.turnover_usdt)
        is_major = "major" in item.tags

        if not force:
            if tier_counts[tier] >= caps[tier]:
                return False
            if is_major and major_count >= max_major:
                return False

        seen.add(item.symbol)
        merged.append(item)
        tier_counts[tier] += 1
        if is_major:
            major_count += 1
        return True

    def add_bucket(bucket: list[PairCandidate], target_count: int, *, predicate=None, force: bool = False) -> int:
        added = 0
        for item in bucket:
            if len(merged) >= scan_limit or added >= target_count:
                break
            if predicate is not None and not predicate(item):
                continue
            if add(item, force=force):
                added += 1
        return added

    add_bucket(by_volume, n_vol)
    add_bucket(by_momentum[: n_momentum * 2], n_momentum, predicate=lambda item: item.change_pct > 0)
    add_bucket(by_reversal[: n_reversal * 2], n_reversal, predicate=lambda item: item.change_pct < 0)

    support_target = max(6, scan_limit // 8)
    add_bucket(by_rs[: support_target * 2], support_target)
    add_bucket(by_continuation[: support_target * 2], support_target)

    # Prefer some mid/high liquidity diversity before falling back to the full
    # volume list again. This reduces repetitive concentration on the most
    # traded OKX names without choking the universe.
    add_bucket(by_mid_liquidity, max(0, scan_limit - len(merged)))

    for item in by_volume:
        if len(merged) >= scan_limit:
            break
        add(item)

    # Final fallback: never return a half-empty list just because diversity caps
    # were too strict for this market state.
    if len(merged) < scan_limit:
        for item in by_volume:
            if len(merged) >= scan_limit:
                break
            add(item, force=True)

    merged = merged[:scan_limit]
    return sorted(merged, key=_selection_key, reverse=True)
