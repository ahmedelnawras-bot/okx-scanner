from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PairCandidate:
    symbol: str
    last_price: float
    change_pct: float
    turnover_usdt: float
    score_hint: float = 0.0
    rebound_hint: float = 0.0
    tags: list[str] = field(default_factory=list)


@dataclass
class SignalCandidate:
    symbol: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    score: float
    setup_type: str
    entry_timing: str
    market_mode: str
    execution_setup_tags: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
