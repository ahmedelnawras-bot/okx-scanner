from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TrackedTrade:
    symbol: str
    status: str = "open"
    same_symbol_block_exempt: bool = False
