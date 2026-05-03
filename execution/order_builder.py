from execution.config import (
    DEFAULT_LEVERAGE,
    TP1_CLOSE_PCT,
    TP2_CLOSE_PCT,
    TRAILING_POSITION_PCT,
    TRAILING_PCT,
    MOVE_SL_TO_ENTRY_AFTER_TP1,
)


def build_order_preview(symbol: str, candidate: dict) -> dict:
    """
    يبني Preview للأمر بدون إرساله إلى OKX.
    """

    entry = float(candidate.get("entry", candidate.get("market_entry", 0.0)) or 0.0)
    sl = float(candidate.get("sl", 0.0) or 0.0)
    tp1 = float(candidate.get("tp1", 0.0) or 0.0)
    tp2 = float(candidate.get("tp2", 0.0) or 0.0)
    score = float(candidate.get("score", candidate.get("effective_score", 0.0)) or 0.0)

    return {
        "symbol": symbol,
        "side": "long",
        "instType": "SWAP",
        "tdMode": "isolated",
        "ordType": "market",
        "leverage": DEFAULT_LEVERAGE,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "score": score,
        "tp_plan": {
            "tp1_close_pct": TP1_CLOSE_PCT,
            "tp2_close_pct": TP2_CLOSE_PCT,
            "trailing_position_pct": TRAILING_POSITION_PCT,
            "trailing_pct": TRAILING_PCT,
            "move_sl_to_entry_after_tp1": MOVE_SL_TO_ENTRY_AFTER_TP1,
        },
        "status": "preview_only",
        "note": "No real order is sent from order_builder.py",
    }
