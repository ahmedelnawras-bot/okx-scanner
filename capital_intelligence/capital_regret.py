"""
Capital Intelligence Layer - Regret Engine v1

Shadow-only learning layer for Project #1.

Purpose:
- Evaluate whether a capital auction advisory decision was good or bad after outcome data appears.
- Detect false rejections: high-bid candidates that were not selected but later would have hit TP1/TP2.
- Detect false acceptances: selected high-bid candidates that later hit SL / closed badly.
- Produce regret metrics that can later feed Symbol Memory and Capital Reports.

Safety:
- Does NOT place orders.
- Does NOT reject trades.
- Does NOT change score, TP, SL, OKX, Recovery, BLOCK, or main.py.
- Designed to consume CapitalAuctionResult / CapitalBid dictionaries and trade or rejection outcome rows.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Iterable

try:
    from .capital_models import CapitalAuctionResult, CapitalBid
except Exception:  # allows direct import during mobile/local testing
    from capital_models import CapitalAuctionResult, CapitalBid


REGRET_MODEL_NAME = "capital_regret_shadow_v1"


# Conservative defaults. These values are scoring the capital decision, not PnL.
FALSE_REJECTION_TP2_REGRET = 100.0
FALSE_REJECTION_TP1_REGRET = 65.0
FALSE_ACCEPTANCE_SL_REGRET = 85.0
FALSE_ACCEPTANCE_FAST_SL_REGRET = 95.0
LOW_BID_SELECTED_SL_REGRET = 45.0
CORRECT_REJECTION_CREDIT = 25.0
CORRECT_ACCEPTANCE_CREDIT = 25.0


@dataclass
class CapitalRegretDecision:
    symbol: str
    setup_type: str = ""
    bid_score: float = 0.0
    trade_class: str = "C"
    rank: int = 0
    advisory_selected: bool = False
    advisory_reason: str = "shadow_only"
    market_mode: str = ""
    entry_score: float = 0.0
    entry: float = 0.0
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    raw_bid: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CapitalOutcome:
    symbol: str
    outcome: str = "unknown"  # tp2, tp1, sl, win, loss, flat, unknown
    pnl_pct: float = 0.0
    reached_tp1: bool = False
    reached_tp2: bool = False
    hit_sl: bool = False
    max_pump_after_rejection_pct: float = 0.0
    max_dump_after_rejection_pct: float = 0.0
    source: str = "unknown"
    observed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CapitalRegretVerdict:
    symbol: str
    setup_type: str
    bid_score: float
    trade_class: str
    rank: int
    advisory_selected: bool
    outcome: str
    verdict: str
    regret_score: float = 0.0
    credit_score: float = 0.0
    reason: str = ""
    decision: dict[str, Any] = field(default_factory=dict)
    outcome_row: dict[str, Any] = field(default_factory=dict)
    model: str = REGRET_MODEL_NAME
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CapitalRegretReport:
    total: int = 0
    evaluated: int = 0
    unknown: int = 0
    correct: int = 0
    wrong: int = 0
    false_rejections: int = 0
    false_acceptances: int = 0
    total_regret_score: float = 0.0
    avg_regret_score: float = 0.0
    total_credit_score: float = 0.0
    avg_credit_score: float = 0.0
    worst_false_rejections: list[dict[str, Any]] = field(default_factory=list)
    worst_false_acceptances: list[dict[str, Any]] = field(default_factory=list)
    verdicts: list[CapitalRegretVerdict] = field(default_factory=list)
    model: str = REGRET_MODEL_NAME
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["verdicts"] = [v.to_dict() for v in self.verdicts]
        return data


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on", "hit", "reached"}


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "to_dict"):
        try:
            data = value.to_dict()
            if isinstance(data, dict):
                return dict(data)
        except Exception:
            pass
    try:
        return dict(value.__dict__)
    except Exception:
        return {}


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _bid_rows(auction: CapitalAuctionResult | dict | Iterable[Any] | None) -> list[dict[str, Any]]:
    if auction is None:
        return []
    if isinstance(auction, CapitalAuctionResult):
        return [bid.to_dict() for bid in auction.bids]
    if isinstance(auction, dict):
        rows = auction.get("bids") or []
        return [row if isinstance(row, dict) else _as_dict(row) for row in rows]
    return [_as_dict(row) for row in list(auction or [])]


def build_regret_decisions(auction: CapitalAuctionResult | dict | Iterable[Any] | None) -> list[CapitalRegretDecision]:
    """Convert CapitalAuctionResult/CapitalBid rows into stable decision rows."""
    decisions: list[CapitalRegretDecision] = []
    for row in _bid_rows(auction):
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        decision = CapitalRegretDecision(
            symbol=_normalize_symbol(row.get("symbol") or meta.get("symbol") or "-"),
            setup_type=str(row.get("setup_type") or meta.get("setup_type") or ""),
            bid_score=_safe_float(row.get("bid_score"), 0.0),
            trade_class=str(row.get("trade_class") or "C"),
            rank=int(_safe_float(row.get("rank"), 0.0)),
            advisory_selected=bool(row.get("advisory_selected")),
            advisory_reason=str(row.get("advisory_reason") or "shadow_only"),
            market_mode=str(meta.get("market_mode") or row.get("market_mode") or ""),
            entry_score=_safe_float(meta.get("entry_score"), 0.0),
            entry=_safe_float(meta.get("entry"), 0.0),
            created_at=str(row.get("created_at") or datetime.now(timezone.utc).isoformat()),
            raw_bid=dict(row),
        )
        decisions.append(decision)
    return decisions


def infer_outcome_from_trade(trade: Any) -> CapitalOutcome:
    """Infer outcome from a tracked trade-like object or dict."""
    row = _as_dict(trade)
    symbol = _normalize_symbol(row.get("symbol") or getattr(trade, "symbol", ""))
    status = str(row.get("status") or getattr(trade, "status", "") or "").strip().lower()
    reached_tp2 = _safe_bool(row.get("tp2_hit") or getattr(trade, "tp2_hit", False))
    reached_tp1 = _safe_bool(row.get("tp1_hit") or getattr(trade, "tp1_hit", False)) or reached_tp2
    hit_sl = bool(status in {"closed_loss", "stopped", "sl", "stop_loss", "closed_by_sl"})

    pnl = 0.0
    for key in ("realized_pnl_pct", "pnl_pct", "floating_pnl_pct", "effective_pnl_pct"):
        value = _safe_float(row.get(key) if key in row else getattr(trade, key, 0.0), 0.0)
        if abs(value) > 1e-12:
            pnl = value
            break

    if reached_tp2:
        outcome = "tp2"
    elif reached_tp1:
        outcome = "tp1"
    elif hit_sl or pnl < 0:
        outcome = "sl" if hit_sl else "loss"
    elif pnl > 0:
        outcome = "win"
    elif status in {"closed", "breakeven_after_tp1"}:
        outcome = "flat"
    else:
        outcome = "unknown"

    return CapitalOutcome(
        symbol=symbol,
        outcome=outcome,
        pnl_pct=round(float(pnl), 4),
        reached_tp1=reached_tp1,
        reached_tp2=reached_tp2,
        hit_sl=hit_sl,
        source="trade",
        raw=row,
    )


def infer_outcome_from_rejection_row(row: dict[str, Any]) -> CapitalOutcome:
    """Infer outcome from post_rejection_tracking rows."""
    source = dict(row or {})
    symbol = _normalize_symbol(source.get("symbol"))
    tp2 = _safe_bool(source.get("would_hit_tp2"))
    tp1 = _safe_bool(source.get("would_hit_tp1")) or tp2
    sl = _safe_bool(source.get("would_hit_sl"))
    one_hour = _safe_float(source.get("price_after_1h_pct"), 0.0)
    max_pump = _safe_float(source.get("max_pump_after_rejection_pct"), 0.0)
    max_dump = _safe_float(source.get("max_dump_after_rejection_pct"), 0.0)

    if tp2:
        outcome = "tp2"
    elif tp1:
        outcome = "tp1"
    elif sl:
        outcome = "sl"
    elif "price_after_1h_pct" in source:
        outcome = "win" if one_hour > 0 else "loss" if one_hour < 0 else "flat"
    else:
        outcome = "unknown"

    return CapitalOutcome(
        symbol=symbol,
        outcome=outcome,
        pnl_pct=round(one_hour, 4),
        reached_tp1=tp1,
        reached_tp2=tp2,
        hit_sl=sl,
        max_pump_after_rejection_pct=round(max_pump, 4),
        max_dump_after_rejection_pct=round(max_dump, 4),
        source="post_rejection_tracking",
        raw=source,
    )


def build_outcome_map(
    trades: Iterable[Any] | None = None,
    rejection_rows: Iterable[dict[str, Any]] | None = None,
) -> dict[str, CapitalOutcome]:
    """Build symbol -> outcome map. Trade outcomes override rejection rows for same symbol."""
    out: dict[str, CapitalOutcome] = {}
    for row in list(rejection_rows or []):
        outcome = infer_outcome_from_rejection_row(row)
        if outcome.symbol:
            out[outcome.symbol] = outcome
    for trade in list(trades or []):
        outcome = infer_outcome_from_trade(trade)
        if outcome.symbol:
            out[outcome.symbol] = outcome
    return out


def evaluate_capital_decision(decision: CapitalRegretDecision | dict[str, Any], outcome: CapitalOutcome | dict[str, Any] | None) -> CapitalRegretVerdict:
    """Compare one capital advisory decision with its later outcome."""
    dec = decision if isinstance(decision, CapitalRegretDecision) else CapitalRegretDecision(**{k: v for k, v in dict(decision or {}).items() if k in CapitalRegretDecision.__dataclass_fields__})
    out = outcome if isinstance(outcome, CapitalOutcome) else (CapitalOutcome(**{k: v for k, v in dict(outcome or {}).items() if k in CapitalOutcome.__dataclass_fields__}) if outcome else None)

    if out is None or out.outcome == "unknown":
        return CapitalRegretVerdict(
            symbol=dec.symbol,
            setup_type=dec.setup_type,
            bid_score=dec.bid_score,
            trade_class=dec.trade_class,
            rank=dec.rank,
            advisory_selected=dec.advisory_selected,
            outcome="unknown",
            verdict="unknown_pending_outcome",
            reason="no_matured_outcome_yet",
            decision=dec.to_dict(),
            outcome_row=out.to_dict() if out else {},
        )

    selected = bool(dec.advisory_selected)
    outcome_name = str(out.outcome or "unknown")
    regret = 0.0
    credit = 0.0
    verdict = "neutral"
    reason = "neutral_outcome"

    if selected:
        if outcome_name in {"sl", "loss"}:
            verdict = "wrong_false_acceptance"
            regret = FALSE_ACCEPTANCE_SL_REGRET
            reason = "selected_candidate_later_lost_or_hit_sl"
            if dec.bid_score < 70:
                regret = LOW_BID_SELECTED_SL_REGRET
                reason = "low_bid_selected_lost"
        elif outcome_name in {"tp2", "tp1", "win"}:
            verdict = "correct_acceptance"
            credit = CORRECT_ACCEPTANCE_CREDIT + min(25.0, max(0.0, dec.bid_score - 70.0) * 0.5)
            reason = "selected_candidate_produced_positive_outcome"
        else:
            verdict = "neutral_selected_flat"
            reason = "selected_candidate_flat_or_unresolved"
    else:
        if outcome_name == "tp2":
            verdict = "wrong_false_rejection"
            regret = FALSE_REJECTION_TP2_REGRET
            reason = "missed_tp2_after_capital_rejection"
        elif outcome_name in {"tp1", "win"}:
            verdict = "wrong_false_rejection"
            regret = FALSE_REJECTION_TP1_REGRET
            reason = "missed_tp1_or_positive_move_after_capital_rejection"
        elif outcome_name in {"sl", "loss"}:
            verdict = "correct_rejection"
            credit = CORRECT_REJECTION_CREDIT
            reason = "rejected_candidate_later_failed"
        else:
            verdict = "neutral_rejection_flat"
            reason = "rejected_candidate_flat_or_unresolved"

    # Weight regret by bid confidence. Missing an A+ or choosing a high-bid loser is worse.
    if regret > 0:
        confidence_multiplier = 1.0 + max(0.0, dec.bid_score - 80.0) / 100.0
        regret = round(min(125.0, regret * confidence_multiplier), 2)
    if credit > 0:
        credit = round(min(60.0, credit), 2)

    return CapitalRegretVerdict(
        symbol=dec.symbol,
        setup_type=dec.setup_type,
        bid_score=round(dec.bid_score, 2),
        trade_class=dec.trade_class,
        rank=dec.rank,
        advisory_selected=selected,
        outcome=outcome_name,
        verdict=verdict,
        regret_score=regret,
        credit_score=credit,
        reason=reason,
        decision=dec.to_dict(),
        outcome_row=out.to_dict(),
    )


def build_capital_regret_report(
    auction: CapitalAuctionResult | dict | Iterable[Any] | None,
    trades: Iterable[Any] | None = None,
    rejection_rows: Iterable[dict[str, Any]] | None = None,
    outcomes_by_symbol: dict[str, CapitalOutcome | dict[str, Any]] | None = None,
    top_n: int = 8,
) -> CapitalRegretReport:
    decisions = build_regret_decisions(auction)
    outcome_map = build_outcome_map(trades=trades, rejection_rows=rejection_rows)
    for symbol, outcome in dict(outcomes_by_symbol or {}).items():
        norm = _normalize_symbol(symbol)
        if isinstance(outcome, CapitalOutcome):
            outcome_map[norm] = outcome
        elif isinstance(outcome, dict):
            outcome_map[norm] = CapitalOutcome(**{k: v for k, v in outcome.items() if k in CapitalOutcome.__dataclass_fields__})

    verdicts = [evaluate_capital_decision(decision, outcome_map.get(decision.symbol)) for decision in decisions]
    evaluated = [v for v in verdicts if v.verdict != "unknown_pending_outcome"]
    wrong = [v for v in evaluated if v.verdict.startswith("wrong_")]
    correct = [v for v in evaluated if v.verdict.startswith("correct_")]
    false_rejections = [v for v in wrong if v.verdict == "wrong_false_rejection"]
    false_acceptances = [v for v in wrong if v.verdict == "wrong_false_acceptance"]

    total_regret = round(sum(v.regret_score for v in wrong), 2)
    total_credit = round(sum(v.credit_score for v in correct), 2)
    n_wrong = max(1, len(wrong))
    n_correct = max(1, len(correct))

    false_rejections_sorted = sorted(false_rejections, key=lambda v: (v.regret_score, v.bid_score), reverse=True)
    false_acceptances_sorted = sorted(false_acceptances, key=lambda v: (v.regret_score, v.bid_score), reverse=True)

    return CapitalRegretReport(
        total=len(verdicts),
        evaluated=len(evaluated),
        unknown=len(verdicts) - len(evaluated),
        correct=len(correct),
        wrong=len(wrong),
        false_rejections=len(false_rejections),
        false_acceptances=len(false_acceptances),
        total_regret_score=total_regret,
        avg_regret_score=round(total_regret / n_wrong, 2) if wrong else 0.0,
        total_credit_score=total_credit,
        avg_credit_score=round(total_credit / n_correct, 2) if correct else 0.0,
        worst_false_rejections=[v.to_dict() for v in false_rejections_sorted[:max(1, int(top_n or 8))]],
        worst_false_acceptances=[v.to_dict() for v in false_acceptances_sorted[:max(1, int(top_n or 8))]],
        verdicts=verdicts,
    )


def build_capital_regret_report_text(report: CapitalRegretReport | dict | None, top_n: int = 5) -> str:
    if report is None:
        return "🧠 Capital Regret — Shadow Report\nNo regret data available."
    data = report.to_dict() if isinstance(report, CapitalRegretReport) else dict(report or {})

    lines = [
        "🧠 Capital Regret — Shadow Report",
        "━━━━━━━━━━━━",
        f"Decisions: {int(data.get('total', 0) or 0)}",
        f"Evaluated: {int(data.get('evaluated', 0) or 0)} | Unknown: {int(data.get('unknown', 0) or 0)}",
        f"Correct: {int(data.get('correct', 0) or 0)} | Wrong: {int(data.get('wrong', 0) or 0)}",
        f"False Rejections: {int(data.get('false_rejections', 0) or 0)}",
        f"False Acceptances: {int(data.get('false_acceptances', 0) or 0)}",
        f"Total Regret: {float(data.get('total_regret_score', 0.0) or 0.0):.2f}",
        f"Avg Regret: {float(data.get('avg_regret_score', 0.0) or 0.0):.2f}",
    ]

    missed = list(data.get("worst_false_rejections") or [])
    if missed:
        lines.extend(["", "❌ Worst Missed Opportunities"])
        for row in missed[:max(1, int(top_n or 5))]:
            lines.append(
                f"• {row.get('symbol', '-')} | Bid {float(row.get('bid_score', 0.0) or 0.0):.2f} | "
                f"{row.get('trade_class', 'C')} | outcome={row.get('outcome', '-')} | regret={float(row.get('regret_score', 0.0) or 0.0):.2f}"
            )

    bad_accepts = list(data.get("worst_false_acceptances") or [])
    if bad_accepts:
        lines.extend(["", "⚠️ Worst Accepted Losers"])
        for row in bad_accepts[:max(1, int(top_n or 5))]:
            lines.append(
                f"• {row.get('symbol', '-')} | Bid {float(row.get('bid_score', 0.0) or 0.0):.2f} | "
                f"{row.get('trade_class', 'C')} | outcome={row.get('outcome', '-')} | regret={float(row.get('regret_score', 0.0) or 0.0):.2f}"
            )

    lines.extend(["", "Mode: Shadow only — no execution impact."])
    return "\n".join(lines)


__all__ = [
    "CapitalRegretDecision",
    "CapitalOutcome",
    "CapitalRegretVerdict",
    "CapitalRegretReport",
    "build_regret_decisions",
    "infer_outcome_from_trade",
    "infer_outcome_from_rejection_row",
    "build_outcome_map",
    "evaluate_capital_decision",
    "build_capital_regret_report",
    "build_capital_regret_report_text",
]
