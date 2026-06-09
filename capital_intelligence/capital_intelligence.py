"""
Capital Intelligence Layer v1 - Shadow Mode

Purpose:
- Separate trade validity from capital priority.
- Calculate a Capital Bid for each already-valid SignalCandidate-like object.
- Rank candidates and show what the future auction would prefer.

Safety:
- Does NOT place orders.
- Does NOT reject trades in v1.
- Does NOT change score, TP, SL, OKX, Recovery, BLOCK, or main.py.
- Designed to be imported later by main.py with a tiny integration call.
"""
from __future__ import annotations

from typing import Any, Iterable

try:
    from .capital_config import CapitalIntelligenceConfig, DEFAULT_CONFIG
    from .capital_models import CapitalAuctionResult, CapitalBid, CapitalComponent
except Exception:  # allows direct script import during local testing
    from capital_config import CapitalIntelligenceConfig, DEFAULT_CONFIG
    from capital_models import CapitalAuctionResult, CapitalBid, CapitalComponent


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(float(low), min(float(high), float(value)))


def _get_meta(candidate: Any) -> dict[str, Any]:
    meta = getattr(candidate, "meta", {})
    return dict(meta or {}) if isinstance(meta, dict) else {}


def _as_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple) or isinstance(value, set):
        return list(value)
    return [value]


def _setup_names(candidate: Any) -> list[str]:
    meta = _get_meta(candidate)
    setup_type = str(getattr(candidate, "setup_type", "") or meta.get("setup_type") or "").strip()
    tags = _as_list(getattr(candidate, "execution_setup_tags", []) or meta.get("execution_setup_tags") or [])
    analytics = _as_list(meta.get("analytics_tags") or []) + _as_list(meta.get("derived_setups") or [])
    primary = str(meta.get("analytics_setup_primary") or "").strip()
    names: list[str] = []
    for item in [primary, setup_type, *analytics, *tags]:
        text = str(item or "").strip()
        if text and text not in names:
            names.append(text)
    return names


def classify_bid(score: float, config: CapitalIntelligenceConfig = DEFAULT_CONFIG) -> str:
    score = _safe_float(score, 0.0)
    if score >= config.class_a_plus_min:
        return "A+"
    if score >= config.class_a_min:
        return "A"
    if score >= config.class_b_min:
        return "B"
    return "C"


def calculate_setup_component(candidate: Any, config: CapitalIntelligenceConfig = DEFAULT_CONFIG) -> CapitalComponent:
    names = _setup_names(candidate)
    best_name = "unknown"
    best_points = 0.0
    for name in names:
        points = _safe_float(config.setup_weights.get(name), 0.0)
        if points > best_points:
            best_name = name
            best_points = points

    tag_points = 0.0
    applied_tags: list[str] = []
    for name in names:
        if name in config.tag_bonus:
            tag_points += _safe_float(config.tag_bonus.get(name), 0.0)
            applied_tags.append(f"+{name}")
        if name in config.tag_penalty:
            tag_points += _safe_float(config.tag_penalty.get(name), 0.0)
            applied_tags.append(f"{name}")

    points = _clamp(best_points + tag_points, 0.0, config.setup_max_points)
    return CapitalComponent(
        name="setup_quality",
        points=round(points, 2),
        max_points=config.setup_max_points,
        reason=f"best={best_name}",
        details={"setup_candidates": names, "best_setup": best_name, "tag_adjustments": applied_tags},
    )


def calculate_mtf_component(candidate: Any, config: CapitalIntelligenceConfig = DEFAULT_CONFIG) -> CapitalComponent:
    meta = _get_meta(candidate)
    setup_type = str(getattr(candidate, "setup_type", "") or "")
    htf = str(meta.get("htf_confirmation") or "").strip().lower()
    mtf_confirmed = bool(meta.get("mtf_confirmed"))
    tags = set(str(x) for x in _as_list(getattr(candidate, "execution_setup_tags", []) or [])) | set(str(x) for x in _as_list(meta.get("pair_tags") or []))

    points = 0.0
    reason = "weak_or_unknown"
    if mtf_confirmed or "rs_btc" in tags or "relative_strength_vs_btc" in tags:
        points = 11.0
        reason = "mtf_confirmed_or_rs"
    if htf in {"bullish", "bullish bias", "reclaim bias"}:
        points = max(points, 12.0 if htf == "bullish" else 9.0)
        reason = htf.replace(" ", "_")
    if setup_type in {"wave_3", "retest_breakout_confirmed", "higher_low_continuation"} and points < 8.0:
        points = 8.0
        reason = "structural_setup_proxy"

    points = _clamp(points, 0.0, config.mtf_max_points)
    return CapitalComponent("mtf_strength", round(points, 2), config.mtf_max_points, reason, {"htf_confirmation": htf, "mtf_confirmed": mtf_confirmed})


def calculate_pa_component(candidate: Any, config: CapitalIntelligenceConfig = DEFAULT_CONFIG) -> CapitalComponent:
    meta = _get_meta(candidate)
    pa_score = _safe_float(meta.get("pa_score"), 0.0)
    flags = dict(meta.get("pa_score_flags") or {}) if isinstance(meta.get("pa_score_flags"), dict) else {}
    reason = str(meta.get("pa_score_reason") or "neutral")

    # pa_score in scoring.py is small bounded scale roughly -0.65..0.55.
    normalized = (pa_score + 0.65) / 1.20
    points = _clamp(normalized * config.pa_max_points, 0.0, config.pa_max_points)

    # Smart flags add a little interpretability without exceeding cap.
    if flags.get("weak_breakout"):
        points = min(points, 5.0)
    if flags.get("acceptance") and flags.get("expansion"):
        points = max(points, 12.0)
    if flags.get("sweep") and flags.get("acceptance"):
        points = max(points, 10.0)

    return CapitalComponent("price_action", round(points, 2), config.pa_max_points, reason, {"pa_score": pa_score, "flags": flags})


def calculate_nour_component(candidate: Any, config: CapitalIntelligenceConfig = DEFAULT_CONFIG) -> CapitalComponent:
    meta = _get_meta(candidate)
    stability = _safe_float(meta.get("execution_stability"), 0.0)
    passed = bool(meta.get("nour_filter_passed"))
    reason = str(meta.get("nour_filter_reason") or "unknown")

    if stability <= 0:
        points = 4.0 if passed else 2.0
    elif stability >= 1.55:
        points = 10.0
    elif stability >= 1.30:
        points = 8.0
    elif stability >= 1.05:
        points = 6.0
    elif passed:
        points = 5.0
    else:
        points = 2.0

    if bool(meta.get("exhausted_move")):
        points = min(points, 3.0)
        reason = "exhausted_move"

    return CapitalComponent("nour_stability", round(_clamp(points, 0.0, config.nour_max_points), 2), config.nour_max_points, reason, {"execution_stability": stability, "passed": passed})


def calculate_resistance_component(candidate: Any, config: CapitalIntelligenceConfig = DEFAULT_CONFIG) -> CapitalComponent:
    meta = _get_meta(candidate)
    ctx = meta.get("resistance_4h") if isinstance(meta.get("resistance_4h"), dict) else {}
    status = str(meta.get("resistance_4h_status") or ctx.get("status") or "unknown").strip().lower()
    distance = _safe_float(meta.get("resistance_4h_distance_pct") or ctx.get("distance_pct"), 0.0)

    if status in {"cleared", "clear"} or distance >= config.resistance_clear_min_pct:
        points = config.resistance_max_points
        reason = "clear_or_cleared"
    elif distance >= config.resistance_watch_max_pct:
        points = 8.0
        reason = "enough_distance"
    elif distance >= config.resistance_near_max_pct:
        points = 5.0
        reason = "watch_distance"
    elif distance > 0:
        points = 1.0 if distance <= config.resistance_very_near_max_pct else 3.0
        reason = "near_resistance"
    else:
        points = 5.0
        reason = "unknown_neutral"

    return CapitalComponent("resistance_distance", round(_clamp(points, 0.0, config.resistance_max_points), 2), config.resistance_max_points, reason, {"status": status, "distance_pct": distance})


def calculate_context_component(candidate: Any, config: CapitalIntelligenceConfig = DEFAULT_CONFIG) -> CapitalComponent:
    meta = _get_meta(candidate)
    turnover = _safe_float(meta.get("turnover_usdt"), 0.0)
    change = abs(_safe_float(meta.get("change_pct"), 0.0))
    score = _safe_float(meta.get("boost_score") or getattr(candidate, "score", 0.0), 0.0)
    btc_status = str(meta.get("btc_control_status") or "").lower()
    warnings = _as_list(getattr(candidate, "warnings", []) or [])

    points = 4.0
    reasons: list[str] = ["base_context"]
    if turnover >= 20_000_000:
        points += 2.0
        reasons.append("high_liquidity")
    elif turnover >= 5_000_000:
        points += 1.0
        reasons.append("good_liquidity")
    elif 0 < turnover < 800_000:
        points -= 1.5
        reasons.append("thin_liquidity")

    if score >= 9.0:
        points += 2.0
        reasons.append("high_entry_score")
    elif score >= 8.0:
        points += 1.0
        reasons.append("good_entry_score")

    if change >= 4.0:
        points -= 2.0
        reasons.append("hot_move_penalty")
    elif change <= 2.2:
        points += 1.0
        reasons.append("not_overextended")

    if btc_status == "risk":
        points -= 1.5
        reasons.append("btc_control_risk")
    elif btc_status == "calm":
        points += 0.75
        reasons.append("btc_calm")

    if warnings:
        points -= min(2.0, len(warnings) * 0.75)
        reasons.append("warnings_present")

    return CapitalComponent("context_quality", round(_clamp(points, 0.0, config.context_max_points), 2), config.context_max_points, ",".join(reasons), {"turnover_usdt": turnover, "change_abs_pct": change, "btc_status": btc_status, "warnings_count": len(warnings)})


def calculate_capital_bid(candidate: Any, config: CapitalIntelligenceConfig = DEFAULT_CONFIG) -> CapitalBid:
    components = [
        calculate_setup_component(candidate, config),
        calculate_mtf_component(candidate, config),
        calculate_pa_component(candidate, config),
        calculate_nour_component(candidate, config),
        calculate_resistance_component(candidate, config),
        calculate_context_component(candidate, config),
    ]
    total = round(_clamp(sum(c.points for c in components), 0.0, 100.0), 2)
    trade_class = classify_bid(total, config)
    meta = _get_meta(candidate)
    symbol = str(getattr(candidate, "symbol", "") or meta.get("symbol") or "-")
    setup_type = str(getattr(candidate, "setup_type", "") or meta.get("setup_type") or "-")

    reasons = [f"{c.name}:{c.points:.2f}/{c.max_points:.0f}:{c.reason}" for c in components]
    warnings = [str(w) for w in _as_list(getattr(candidate, "warnings", []) or []) if str(w).strip()]

    return CapitalBid(
        symbol=symbol,
        setup_type=setup_type,
        bid_score=total,
        trade_class=trade_class,
        components=components,
        reasons=reasons,
        warnings=warnings,
        meta={
            "entry_score": _safe_float(getattr(candidate, "score", 0.0), 0.0),
            "entry": _safe_float(getattr(candidate, "entry", 0.0), 0.0),
            "execution_setup_tags": _as_list(getattr(candidate, "execution_setup_tags", []) or []),
            "analytics_tags": _as_list(meta.get("analytics_tags") or []),
            "derived_setups": _as_list(meta.get("derived_setups") or []),
            "resistance_4h_status": meta.get("resistance_4h_status"),
            "resistance_4h_distance_pct": meta.get("resistance_4h_distance_pct"),
        },
        model=config.model_name,
    )


def rank_candidates(candidates: Iterable[Any], available_slots: int = 0, config: CapitalIntelligenceConfig = DEFAULT_CONFIG, mode: str = "shadow") -> CapitalAuctionResult:
    candidate_list = list(candidates or [])
    bids = [calculate_capital_bid(candidate, config) for candidate in candidate_list]
    bids.sort(key=lambda item: (item.bid_score, item.trade_class, item.symbol), reverse=True)

    slots = max(0, int(available_slots or 0))
    selected_symbols: list[str] = []
    rejected_symbols: list[str] = []
    for index, bid in enumerate(bids, start=1):
        bid.rank = index
        would_select = bool(slots <= 0 or index <= slots)
        bid.advisory_selected = would_select
        bid.advisory_reason = "shadow_selected_by_rank" if would_select else "shadow_would_wait_for_slot"
        if would_select:
            selected_symbols.append(bid.symbol)
        else:
            rejected_symbols.append(bid.symbol)

    return CapitalAuctionResult(
        mode=str(mode or "shadow"),
        available_slots=slots,
        total_candidates=len(candidate_list),
        selected_count=len(selected_symbols),
        bids=bids,
        selected_symbols=selected_symbols,
        rejected_symbols=rejected_symbols,
        model=config.model_name,
        shadow_mode=bool(config.shadow_mode),
    )


def annotate_candidates_shadow(candidates: Iterable[Any], available_slots: int = 0, config: CapitalIntelligenceConfig = DEFAULT_CONFIG) -> CapitalAuctionResult:
    """Return auction result and attach capital_bid_shadow to candidate.meta when possible."""
    candidate_list = list(candidates or [])
    result = rank_candidates(candidate_list, available_slots=available_slots, config=config, mode="shadow")
    bid_by_symbol = {bid.symbol: bid for bid in result.bids}
    for candidate in candidate_list:
        try:
            symbol = str(getattr(candidate, "symbol", "") or "-")
            bid = bid_by_symbol.get(symbol)
            if bid is None:
                continue
            meta = getattr(candidate, "meta", None)
            if isinstance(meta, dict):
                meta["capital_bid_shadow"] = bid.to_dict()
                meta["capital_bid_score"] = bid.bid_score
                meta["capital_trade_class"] = bid.trade_class
                meta["capital_rank"] = bid.rank
        except Exception:
            continue
    return result


__all__ = [
    "calculate_capital_bid",
    "rank_candidates",
    "annotate_candidates_shadow",
    "classify_bid",
    "CapitalIntelligenceConfig",
    "CapitalBid",
    "CapitalAuctionResult",
]
