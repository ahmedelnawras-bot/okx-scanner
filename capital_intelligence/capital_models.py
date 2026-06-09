"""
Capital Intelligence Layer - data models v1

Pure dataclasses; no trading side effects.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any


@dataclass
class CapitalComponent:
    name: str
    points: float
    max_points: float
    reason: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CapitalBid:
    symbol: str
    setup_type: str
    bid_score: float
    trade_class: str
    rank: int = 0
    advisory_selected: bool = False
    advisory_reason: str = "shadow_only"
    components: list[CapitalComponent] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    model: str = "capital_intelligence_shadow_v1"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["components"] = [component.to_dict() for component in self.components]
        return data


@dataclass
class CapitalAuctionResult:
    mode: str
    available_slots: int
    total_candidates: int
    selected_count: int
    bids: list[CapitalBid] = field(default_factory=list)
    selected_symbols: list[str] = field(default_factory=list)
    rejected_symbols: list[str] = field(default_factory=list)
    model: str = "capital_intelligence_shadow_v1"
    shadow_mode: bool = True
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "available_slots": self.available_slots,
            "total_candidates": self.total_candidates,
            "selected_count": self.selected_count,
            "selected_symbols": list(self.selected_symbols),
            "rejected_symbols": list(self.rejected_symbols),
            "model": self.model,
            "shadow_mode": self.shadow_mode,
            "created_at": self.created_at,
            "bids": [bid.to_dict() for bid in self.bids],
        }
