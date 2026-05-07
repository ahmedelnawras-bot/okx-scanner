from execution.config import (
    DEFAULT_LEVERAGE,
    TP1_CLOSE_PCT,
    TP2_CLOSE_PCT,
    TRAILING_POSITION_PCT,
    TRAILING_PCT,
    MOVE_SL_TO_ENTRY_AFTER_TP1,
)


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def has_pullback_execution_plan(candidate: dict) -> bool:
    """
    يحدد هل التنفيذ نفسه Pullback Pending.

    مهم: وجود pullback_low/high أو has_pullback_plan قد يكون للعرض فقط داخل
    رسالة Telegram. لا نحول الصفقة إلى pending execution إلا إذا entry_mode
    صريح أنه pullback_pending.
    """
    if not isinstance(candidate, dict):
        return False

    entry_mode = str(candidate.get("entry_mode", "") or "").strip().lower()
    status = str(candidate.get("status", "") or "").strip().lower()
    execution_status = str(candidate.get("execution_status", "") or "").strip().lower()

    return (
        entry_mode in ("pullback_pending", "pending_pullback")
        or status == "pending_pullback"
        or execution_status == "pending_pullback_preview"
    )


def get_pullback_mid_entry(candidate: dict) -> float:
    """
    يحسب دخول التنفيذ من منتصف منطقة البول باك.
    لو pullback_low/high غير متاحين يستخدم pullback_entry كاحتياطي.
    """
    pullback_low = _safe_float(candidate.get("pullback_low"), 0.0)
    pullback_high = _safe_float(candidate.get("pullback_high"), 0.0)
    pullback_entry = _safe_float(candidate.get("pullback_entry"), 0.0)

    if pullback_low > 0 and pullback_high > 0:
        return (pullback_low + pullback_high) / 2.0

    if pullback_entry > 0:
        return pullback_entry

    return 0.0


def build_order_preview(symbol: str, candidate: dict) -> dict:
    """
    يبني Preview للأمر بدون إرساله إلى OKX.

    مهم:
    - لو الإشارة Pullback، لا نستخدم Market Entry.
    - Entry يصبح منتصف منطقة البول باك.
    - TP/SL تظل كما أتت من candidate حاليًا، حتى لا نغيّر منطق الاستراتيجية.
    """

    is_pullback = has_pullback_execution_plan(candidate)

    market_entry = _safe_float(
        candidate.get("entry", candidate.get("market_entry", 0.0)),
        0.0,
    )

    pullback_mid_entry = get_pullback_mid_entry(candidate)

    entry = pullback_mid_entry if is_pullback and pullback_mid_entry > 0 else market_entry

    sl = _safe_float(candidate.get("execution_sl", candidate.get("sl", 0.0)) if is_pullback else candidate.get("sl", 0.0), 0.0)
    tp1 = _safe_float(candidate.get("execution_tp1", candidate.get("tp1", 0.0)) if is_pullback else candidate.get("tp1", 0.0), 0.0)
    tp2 = _safe_float(candidate.get("execution_tp2", candidate.get("tp2", 0.0)) if is_pullback else candidate.get("tp2", 0.0), 0.0)
    score = _safe_float(candidate.get("score", candidate.get("effective_score", 0.0)), 0.0)

    return {
        "symbol": symbol,
        "side": "long",
        "instType": "SWAP",
        "tdMode": "isolated",
        "ordType": "limit" if is_pullback else "market",
        "leverage": DEFAULT_LEVERAGE,

        "entry": entry,
        "market_entry": market_entry,
        "execution_entry": entry,
        "pullback_mid_entry": pullback_mid_entry if is_pullback else None,
        "pullback_low": candidate.get("pullback_low"),
        "pullback_high": candidate.get("pullback_high"),
        "pullback_entry": candidate.get("pullback_entry"),
        "entry_mode": "pullback_pending" if is_pullback else "market",
        "has_pullback_plan": is_pullback,

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

        "status": "pending_pullback_preview" if is_pullback else "preview_only",
        "note": (
            "Pullback execution preview only - no market order is sent"
            if is_pullback
            else "No real order is sent from order_builder.py"
        ),
    }
