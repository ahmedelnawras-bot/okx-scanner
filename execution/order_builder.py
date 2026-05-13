from __future__ import annotations

from analysis.models import SignalCandidate



def build_preview_order(signal: SignalCandidate) -> dict:
    return {
        "symbol": signal.symbol,
        "entry": signal.entry,
        "sl": signal.sl,
        "tp1": signal.tp1,
        "tp2": signal.tp2,
        "entry_mode": "pullback_pending" if signal.entry_timing == "pullback" else "market",
        "status": "preview_only",
    }
