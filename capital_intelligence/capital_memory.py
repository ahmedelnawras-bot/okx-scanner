"""
Capital Intelligence Layer - Symbol Memory Engine v1.5 (Shadow Mode)

Purpose:
- Build a lightweight memory profile per symbol and per setup.
- Convert historical outcomes into an advisory capital bonus/penalty.
- Support future Capital Intelligence ranking without touching execution.

Safety:
- Does NOT place orders.
- Does NOT reject trades.
- Does NOT change score, TP, SL, OKX, Recovery, BLOCK, or main.py.
- Pure analytics/shadow module.

Typical future flow:
    profile = build_symbol_memory_profile("SOL-USDT-SWAP", history)
    adjustment = calculate_symbol_memory_adjustment(candidate, profile)
    # capital_intelligence.py may later add adjustment.points to Capital Bid.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Iterable


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(float(low), min(float(high), float(value)))


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, (tuple, set)):
        return list(value)
    return [value]


def _get_meta(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        meta = obj.get("meta")
        return dict(meta or {}) if isinstance(meta, dict) else {}
    meta = getattr(obj, "meta", {})
    return dict(meta or {}) if isinstance(meta, dict) else {}


def _get_field(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        if key in obj:
            return obj.get(key, default)
        meta = obj.get("meta")
        if isinstance(meta, dict):
            return meta.get(key, default)
        return default
    if hasattr(obj, key):
        return getattr(obj, key)
    meta = getattr(obj, "meta", {})
    if isinstance(meta, dict):
        return meta.get(key, default)
    return default


def _normalize_outcome(record: Any) -> str:
    """Normalize many possible trade/rejection outcomes into stable buckets."""
    raw = str(
        _get_field(record, "outcome")
        or _get_field(record, "result")
        or _get_field(record, "exit_reason")
        or _get_field(record, "status")
        or ""
    ).strip().lower()

    if raw in {"tp2", "take_profit_2", "runner", "runner_exit", "trailing", "trailing_exit"}:
        return "tp2"
    if raw in {"tp1", "take_profit_1", "partial_tp", "partial"}:
        return "tp1"
    if raw in {"sl", "stop", "stop_loss", "direct_sl", "loss"}:
        return "sl"
    if raw in {"breakeven", "be", "break_even", "scratch"}:
        return "breakeven"
    if raw in {"open", "running", "active"}:
        return "open"

    # Fallback from numeric PnL if available.
    pnl = _safe_float(
        _get_field(record, "realized_pnl_pct", None)
        or _get_field(record, "pnl_pct", None)
        or _get_field(record, "profit_pct", None),
        0.0,
    )
    if pnl >= 8.0:
        return "tp2"
    if pnl >= 2.0:
        return "tp1"
    if pnl <= -2.0:
        return "sl"
    if pnl != 0:
        return "breakeven"
    return "unknown"


def _setup_names(record: Any) -> list[str]:
    meta = _get_meta(record)
    names: list[str] = []
    sources = [
        _get_field(record, "setup_type"),
        meta.get("analytics_setup_primary"),
        meta.get("setup_type"),
        _get_field(record, "execution_setup_tags"),
        meta.get("execution_setup_tags"),
        meta.get("analytics_tags"),
        meta.get("derived_setups"),
        meta.get("pair_tags"),
    ]
    for src in sources:
        for item in _as_list(src):
            text = str(item or "").strip()
            if text and text not in names:
                names.append(text)
    return names


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SetupMemoryStats:
    setup_name: str
    trades: int = 0
    tp1_count: int = 0
    tp2_count: int = 0
    sl_count: int = 0
    breakeven_count: int = 0
    unknown_count: int = 0
    total_realized_pnl_pct: float = 0.0

    @property
    def win_count(self) -> int:
        return self.tp1_count + self.tp2_count

    @property
    def win_rate(self) -> float:
        return round((self.win_count / self.trades) * 100.0, 2) if self.trades else 0.0

    @property
    def tp2_rate(self) -> float:
        return round((self.tp2_count / self.trades) * 100.0, 2) if self.trades else 0.0

    @property
    def sl_rate(self) -> float:
        return round((self.sl_count / self.trades) * 100.0, 2) if self.trades else 0.0

    @property
    def avg_pnl_pct(self) -> float:
        return round(self.total_realized_pnl_pct / self.trades, 4) if self.trades else 0.0

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data.update({
            "win_count": self.win_count,
            "win_rate": self.win_rate,
            "tp2_rate": self.tp2_rate,
            "sl_rate": self.sl_rate,
            "avg_pnl_pct": self.avg_pnl_pct,
        })
        return data


@dataclass
class SymbolMemoryProfile:
    symbol: str
    trades: int = 0
    tp1_count: int = 0
    tp2_count: int = 0
    sl_count: int = 0
    breakeven_count: int = 0
    unknown_count: int = 0
    total_realized_pnl_pct: float = 0.0
    fake_breakout_count: int = 0
    clean_runner_count: int = 0
    setups: dict[str, SetupMemoryStats] = field(default_factory=dict)
    model: str = "symbol_memory_v1_shadow"
    updated_at: str = field(default_factory=_utc_now_iso)

    @property
    def win_count(self) -> int:
        return self.tp1_count + self.tp2_count

    @property
    def win_rate(self) -> float:
        return round((self.win_count / self.trades) * 100.0, 2) if self.trades else 0.0

    @property
    def tp2_rate(self) -> float:
        return round((self.tp2_count / self.trades) * 100.0, 2) if self.trades else 0.0

    @property
    def sl_rate(self) -> float:
        return round((self.sl_count / self.trades) * 100.0, 2) if self.trades else 0.0

    @property
    def avg_pnl_pct(self) -> float:
        return round(self.total_realized_pnl_pct / self.trades, 4) if self.trades else 0.0

    @property
    def fake_breakout_rate(self) -> float:
        return round((self.fake_breakout_count / self.trades) * 100.0, 2) if self.trades else 0.0

    @property
    def runner_rate(self) -> float:
        return round((self.clean_runner_count / self.trades) * 100.0, 2) if self.trades else 0.0

    def confidence(self) -> float:
        """0..1 confidence based on sample size. Full confidence around 30 trades."""
        return round(_clamp(self.trades / 30.0, 0.0, 1.0), 4)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "trades": self.trades,
            "tp1_count": self.tp1_count,
            "tp2_count": self.tp2_count,
            "sl_count": self.sl_count,
            "breakeven_count": self.breakeven_count,
            "unknown_count": self.unknown_count,
            "win_count": self.win_count,
            "win_rate": self.win_rate,
            "tp2_rate": self.tp2_rate,
            "sl_rate": self.sl_rate,
            "avg_pnl_pct": self.avg_pnl_pct,
            "fake_breakout_count": self.fake_breakout_count,
            "fake_breakout_rate": self.fake_breakout_rate,
            "clean_runner_count": self.clean_runner_count,
            "runner_rate": self.runner_rate,
            "confidence": self.confidence(),
            "setups": {name: stats.to_dict() for name, stats in sorted(self.setups.items())},
            "model": self.model,
            "updated_at": self.updated_at,
        }


@dataclass
class SymbolMemoryAdjustment:
    symbol: str
    points: float
    max_abs_points: float
    confidence: float
    reason: str
    details: dict[str, Any] = field(default_factory=dict)
    model: str = "symbol_memory_adjustment_v1_shadow"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ─────────────────────────────────────────────────────────────────────────────
# Profile Builder
# ─────────────────────────────────────────────────────────────────────────────

def build_symbol_memory_profile(symbol: str, history: Iterable[Any]) -> SymbolMemoryProfile:
    """Build memory profile from historical trade-like records for one symbol."""
    profile = SymbolMemoryProfile(symbol=str(symbol or "-").strip() or "-")

    for record in list(history or []):
        rec_symbol = str(_get_field(record, "symbol", profile.symbol) or profile.symbol)
        if profile.symbol != "-" and rec_symbol and rec_symbol != profile.symbol:
            continue

        outcome = _normalize_outcome(record)
        pnl = _safe_float(
            _get_field(record, "realized_pnl_pct", None)
            or _get_field(record, "pnl_pct", None)
            or _get_field(record, "profit_pct", None),
            0.0,
        )
        profile.trades += 1
        profile.total_realized_pnl_pct += pnl

        if outcome == "tp2":
            profile.tp2_count += 1
        elif outcome == "tp1":
            profile.tp1_count += 1
        elif outcome == "sl":
            profile.sl_count += 1
        elif outcome == "breakeven":
            profile.breakeven_count += 1
        else:
            profile.unknown_count += 1

        meta = _get_meta(record)
        if bool(meta.get("failed_breakout_risk")) or "failed_breakout" in _setup_names(record):
            profile.fake_breakout_count += 1
        if outcome == "tp2" or "protected_runner" in _setup_names(record) or "runner" in str(_get_field(record, "status", "")).lower():
            profile.clean_runner_count += 1

        for setup in _setup_names(record):
            stats = profile.setups.setdefault(setup, SetupMemoryStats(setup_name=setup))
            stats.trades += 1
            stats.total_realized_pnl_pct += pnl
            if outcome == "tp2":
                stats.tp2_count += 1
            elif outcome == "tp1":
                stats.tp1_count += 1
            elif outcome == "sl":
                stats.sl_count += 1
            elif outcome == "breakeven":
                stats.breakeven_count += 1
            else:
                stats.unknown_count += 1

    profile.updated_at = _utc_now_iso()
    return profile


def build_symbol_memory_book(history: Iterable[Any]) -> dict[str, SymbolMemoryProfile]:
    """Build profiles for all symbols found in history."""
    grouped: dict[str, list[Any]] = {}
    for record in list(history or []):
        symbol = str(_get_field(record, "symbol", "-") or "-").strip() or "-"
        grouped.setdefault(symbol, []).append(record)
    return {symbol: build_symbol_memory_profile(symbol, records) for symbol, records in sorted(grouped.items())}


# ─────────────────────────────────────────────────────────────────────────────
# Adjustment Engine
# ─────────────────────────────────────────────────────────────────────────────

def calculate_symbol_memory_adjustment(
    candidate: Any,
    profile: SymbolMemoryProfile | dict[str, Any] | None,
    max_abs_points: float = 6.0,
    min_trades_for_full_effect: int = 12,
) -> SymbolMemoryAdjustment:
    """Return advisory capital bonus/penalty from symbol memory.

    This is intentionally bounded and confidence-weighted.
    It should never dominate setup/PA/MTF; it only nudges Capital Bid.
    """
    symbol = str(_get_field(candidate, "symbol", "-") or "-")

    if profile is None:
        return SymbolMemoryAdjustment(symbol, 0.0, max_abs_points, 0.0, "no_profile")

    if isinstance(profile, dict):
        trades = _safe_int(profile.get("trades"), 0)
        win_rate = _safe_float(profile.get("win_rate"), 0.0)
        tp2_rate = _safe_float(profile.get("tp2_rate"), 0.0)
        sl_rate = _safe_float(profile.get("sl_rate"), 0.0)
        avg_pnl = _safe_float(profile.get("avg_pnl_pct"), 0.0)
        fake_rate = _safe_float(profile.get("fake_breakout_rate"), 0.0)
        runner_rate = _safe_float(profile.get("runner_rate"), 0.0)
        setups = profile.get("setups") if isinstance(profile.get("setups"), dict) else {}
    else:
        trades = profile.trades
        win_rate = profile.win_rate
        tp2_rate = profile.tp2_rate
        sl_rate = profile.sl_rate
        avg_pnl = profile.avg_pnl_pct
        fake_rate = profile.fake_breakout_rate
        runner_rate = profile.runner_rate
        setups = {name: stats.to_dict() for name, stats in profile.setups.items()}

    if trades <= 0:
        return SymbolMemoryAdjustment(symbol, 0.0, max_abs_points, 0.0, "empty_profile")

    confidence = _clamp(trades / max(1, int(min_trades_for_full_effect)), 0.0, 1.0)
    raw = 0.0
    reasons: list[str] = []

    if win_rate >= 65.0:
        raw += 2.0
        reasons.append("symbol_high_win_rate")
    elif win_rate >= 55.0:
        raw += 1.0
        reasons.append("symbol_good_win_rate")
    elif win_rate <= 35.0 and trades >= 5:
        raw -= 2.0
        reasons.append("symbol_weak_win_rate")

    if tp2_rate >= 30.0:
        raw += 1.5
        reasons.append("symbol_good_tp2_rate")
    elif tp2_rate <= 8.0 and trades >= 5:
        raw -= 1.0
        reasons.append("symbol_low_tp2_rate")

    if sl_rate >= 50.0 and trades >= 5:
        raw -= 2.0
        reasons.append("symbol_high_sl_rate")
    elif sl_rate <= 25.0 and trades >= 5:
        raw += 1.0
        reasons.append("symbol_low_sl_rate")

    if avg_pnl >= 3.0:
        raw += 1.0
        reasons.append("positive_avg_pnl")
    elif avg_pnl <= -2.0:
        raw -= 1.0
        reasons.append("negative_avg_pnl")

    if fake_rate >= 35.0 and trades >= 5:
        raw -= 1.5
        reasons.append("fake_breakout_prone")

    if runner_rate >= 25.0:
        raw += 1.0
        reasons.append("runner_friendly")

    # Setup-specific memory: if this exact candidate setup has history on symbol.
    candidate_setups = _setup_names(candidate)
    best_setup_bonus = 0.0
    best_setup_name = ""
    for setup in candidate_setups:
        item = setups.get(setup)
        if not isinstance(item, dict):
            continue
        setup_trades = _safe_int(item.get("trades"), 0)
        if setup_trades < 3:
            continue
        setup_win_rate = _safe_float(item.get("win_rate"), 0.0)
        setup_sl_rate = _safe_float(item.get("sl_rate"), 0.0)
        setup_tp2_rate = _safe_float(item.get("tp2_rate"), 0.0)
        local = 0.0
        if setup_win_rate >= 65.0:
            local += 1.5
        elif setup_win_rate <= 35.0:
            local -= 1.5
        if setup_tp2_rate >= 30.0:
            local += 1.0
        if setup_sl_rate >= 50.0:
            local -= 1.0
        if abs(local) > abs(best_setup_bonus):
            best_setup_bonus = local
            best_setup_name = setup

    if best_setup_bonus:
        raw += best_setup_bonus
        reasons.append(f"setup_memory:{best_setup_name}")

    weighted = raw * confidence
    points = round(_clamp(weighted, -abs(max_abs_points), abs(max_abs_points)), 2)

    return SymbolMemoryAdjustment(
        symbol=symbol,
        points=points,
        max_abs_points=abs(max_abs_points),
        confidence=round(confidence, 4),
        reason=",".join(reasons) if reasons else "neutral_memory",
        details={
            "trades": trades,
            "win_rate": win_rate,
            "tp2_rate": tp2_rate,
            "sl_rate": sl_rate,
            "avg_pnl_pct": avg_pnl,
            "fake_breakout_rate": fake_rate,
            "runner_rate": runner_rate,
            "candidate_setups": candidate_setups,
            "raw_points_before_confidence": round(raw, 3),
        },
    )


def attach_symbol_memory_shadow(
    candidate: Any,
    profile: SymbolMemoryProfile | dict[str, Any] | None,
    max_abs_points: float = 6.0,
) -> SymbolMemoryAdjustment:
    """Attach symbol memory adjustment to candidate.meta when possible."""
    adjustment = calculate_symbol_memory_adjustment(candidate, profile, max_abs_points=max_abs_points)
    try:
        meta = getattr(candidate, "meta", None)
        if isinstance(meta, dict):
            meta["symbol_memory_shadow"] = adjustment.to_dict()
            meta["symbol_memory_points"] = adjustment.points
            meta["symbol_memory_confidence"] = adjustment.confidence
            meta["symbol_memory_reason"] = adjustment.reason
    except Exception:
        pass
    return adjustment


def summarize_memory_book(memory_book: dict[str, SymbolMemoryProfile | dict[str, Any]], limit: int = 10) -> dict[str, Any]:
    """Compact summary for reports."""
    rows: list[dict[str, Any]] = []
    for symbol, profile in (memory_book or {}).items():
        if isinstance(profile, SymbolMemoryProfile):
            data = profile.to_dict()
        elif isinstance(profile, dict):
            data = dict(profile)
            data.setdefault("symbol", symbol)
        else:
            continue
        rows.append(data)

    rows.sort(key=lambda x: (_safe_float(x.get("avg_pnl_pct")), _safe_float(x.get("tp2_rate")), _safe_float(x.get("win_rate"))), reverse=True)
    top = rows[: max(0, int(limit or 10))]
    weak = sorted(rows, key=lambda x: (_safe_float(x.get("sl_rate")), -_safe_float(x.get("avg_pnl_pct"))), reverse=True)[: max(0, int(limit or 10))]

    return {
        "model": "symbol_memory_summary_v1_shadow",
        "symbols": len(rows),
        "top_symbols": top,
        "weak_symbols": weak,
        "generated_at": _utc_now_iso(),
    }


__all__ = [
    "SetupMemoryStats",
    "SymbolMemoryProfile",
    "SymbolMemoryAdjustment",
    "build_symbol_memory_profile",
    "build_symbol_memory_book",
    "calculate_symbol_memory_adjustment",
    "attach_symbol_memory_shadow",
    "summarize_memory_book",
]
