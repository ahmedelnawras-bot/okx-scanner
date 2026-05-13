from __future__ import annotations

from analysis.models import SignalCandidate
from .models import TrackedTrade



def register_trade(signal: SignalCandidate) -> TrackedTrade:
    return TrackedTrade(
        symbol=signal.symbol,
        entry=signal.entry,
        sl=signal.sl,
        tp1=signal.tp1,
        tp2=signal.tp2,
        setup_type=signal.setup_type,
        market_mode=signal.market_mode,
        score=signal.score,
        execution_setup_tags=list(signal.execution_setup_tags),
        warnings=list(signal.warnings),
        current_price=signal.entry,
    )
