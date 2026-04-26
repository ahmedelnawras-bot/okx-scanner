from collections import Counter, defaultdict


# =========================
# SAFE HELPERS
# =========================
def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_bool(value):
    try:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return value != 0
        value = str(value).strip().lower()
        return value in ("1", "true", "yes", "y", "on")
    except Exception:
        return False


def _normalize_reason(reason: str) -> str:
    mapping = {
        "RSI صحي": "RSI في منطقة صحية",
        "RSI جيد": "RSI جيد",
        "RSI صاعد بقوة": "RSI صاعد بقوة",
        "RSI مرتفع لكن بزخم": "RSI مرتفع بزخم",
        "RSI عالي": "RSI عالي (تشبع شراء)",

        "فوليوم داعم": "فوليوم داعم",
        "فوليوم قوي": "فوليوم قوي",
        "فوليوم انفجار": "فوليوم انفجاري",

        "فوق MA": "فوق المتوسط",
        "فوق المتوسط": "فوق المتوسط",

        "شمعة جيدة": "شمعة جيدة",
        "شمعة قوية": "شمعة قوية",

        "اختراق": "اختراق",
        "اختراق مبكر جداً": "اختراق مبكر",
        "اختراق مبكر": "اختراق مبكر",
        "اختراق متأخر": "اختراق متأخر",
        "اختراق قوي مؤكد": "اختراق قوي مؤكد",

        "تأكيد فريم الساعة": "تأكيد فريم الساعة",

        "BTC داعم": "BTC داعم",
        "BTC غير داعم": "BTC غير داعم",

        "هيمنة داعمة": "هيمنة داعمة للألت",
        "هيمنة ضد الألت": "هيمنة ضد الألت (ضغط على العملات)",

        "تمويل سلبي": "تمويل سلبي (داعم للشراء)",
        "تمويل إيجابي": "تمويل إيجابي (ضغط محتمل)",

        "عملة جديدة": "عملة جديدة",
        "بداية ترند مبكرة": "بداية ترند مبكرة",
        "زخم مبكر تحت المقاومة 🎯": "زخم مبكر تحت المقاومة 🎯",

        "بعيد عن MA (متأخر)": "بعيد عن المتوسط (دخول متأخر)",
        "ممتد زيادة": "ممتد زيادة",
        "أسفل المتوسط": "أسفل المتوسط",
        "رفض سعري علوي": "رفض سعري علوي",

        "أخبار اقتصادية مهمة قريبة": "أخبار اقتصادية مهمة قريبة",

        "Late Pump Risk": "خطر مطاردة Pump متأخر",
        "Bull Market Continuation Risk": "استمرار في Bull Market بعد امتداد خطر",
        "خطر مطاردة Pump متأخر": "خطر مطاردة Pump متأخر",
        "استمرار في Bull Market بعد امتداد خطر": "استمرار في Bull Market بعد امتداد خطر",
        "شمعة قوية لكن احتمال مطاردة": "شمعة قوية لكن احتمال مطاردة",
        "صعود سريع خلال 4 ساعات": "صعود سريع خلال 4 ساعات",

        "Momentum Exhaustion Trap": "خطر نهاية الزخم بعد Pump",
        "far_from_vwap": "بعيد عن VWAP",
        "rsi_slope_weak": "RSI بدأ يضعف",
        "macd_hist_falling": "زخم MACD يتراجع",
        "macd_hist_negative": "MACD سلبي",
        "Weak Historical Setup": "نوع إشارة ضعيف تاريخيًا",

        "OVERSOLD_REVERSAL": "Oversold Reversal",
        "RECOVERY_LONG": "Recovery Long",
        "POST_CRASH_REBOUND": "Post Crash Rebound",
    }
    return mapping.get(str(reason), str(reason))


# =========================
# REDIS LOAD
# =========================
def _load_trade(redis_client, trade_key: str):
    if redis_client is None:
        return None

    try:
        raw = redis_client.get(trade_key)
        if not raw:
            return None

        import json
        data = json.loads(raw)

        if isinstance(data, dict):
            return data

        return None

    except Exception:
        return None


def _get_all_trades(redis_client):
    if redis_client is None:
        return []

    trades = []

    # الأفضل: الاعتماد على trades:all
    try:
        keys = list(redis_client.smembers("trades:all"))
    except Exception:
        keys = []

    for key in keys:
        trade = _load_trade(redis_client, key)
        if trade:
            trades.append(trade)

    # fallback لو trades:all فاضي
    if not trades:
        try:
            for key in redis_client.scan_iter("trade:*"):
                trade = _load_trade(redis_client, key)
                if trade:
                    trades.append(trade)
        except Exception:
            pass

    return trades


# =========================
# RESULT DETECTION FIXED
# =========================
def _trade_result_text(trade: dict) -> str:
    try:
        return str(trade.get("result", "") or "").strip().lower()
    except Exception:
        return ""


def _trade_status_text(trade: dict) -> str:
    try:
        return str(trade.get("status", "") or "").strip().lower()
    except Exception:
        return ""


def _trade_is_tp2_win(trade: dict) -> bool:
    """
    Full Win = الصفقة وصلت TP2.
    يدعم الصيغ القديمة والجديدة.
    """
    try:
        result = _trade_result_text(trade)
        status = _trade_status_text(trade)

        return (
            result in (
                "tp2_win",
                "tp2",
                "full_win",
                "win_tp2",
                "win",
            )
            or status in (
                "tp2_win",
                "tp2",
                "full_win",
                "win_tp2",
                "win",
            )
            or _safe_bool(trade.get("tp2_hit"))
        )
    except Exception:
        return False


def _trade_is_tp1_hit(trade: dict) -> bool:
    """
    TP1 Hit بأي صيغة محفوظة.
    ملاحظة مهمة:
    TP2 يعتبر TP1+ أيضًا لأن السعر لازم يعدي TP1 قبل TP2.
    """
    try:
        result = _trade_result_text(trade)
        status = _trade_status_text(trade)

        return (
            _safe_bool(trade.get("tp1_hit"))
            or _trade_is_tp2_win(trade)
            or result in (
                "tp1_win",
                "tp1",
                "tp1_only",
                "partial",
                "partial_win",
                "win_tp1",
            )
            or status in (
                "tp1_win",
                "tp1",
                "tp1_only",
                "partial",
                "partial_win",
                "win_tp1",
            )
        )
    except Exception:
        return False


def _trade_is_win(trade: dict) -> bool:
    """
    Win = TP1 أو TP2.
    """
    return _trade_is_tp1_hit(trade) or _trade_is_tp2_win(trade)


def _trade_is_loss(trade: dict) -> bool:
    try:
        result = _trade_result_text(trade)
        status = _trade_status_text(trade)

        return result == "loss" or status == "loss"
    except Exception:
        return False


def _trade_is_expired(trade: dict) -> bool:
    try:
        result = _trade_result_text(trade)
        status = _trade_status_text(trade)

        return result == "expired" or status == "expired"
    except Exception:
        return False


def _trade_is_open(trade: dict) -> bool:
    """
    الصفقة Open فقط لو ليست Win / Loss / Expired.
    """
    try:
        if _trade_is_win(trade) or _trade_is_loss(trade) or _trade_is_expired(trade):
            return False

        result = _trade_result_text(trade)
        status = _trade_status_text(trade)

        if status in ("open", "partial"):
            return True

        if result in ("", "open", "partial"):
            return True

        return False
    except Exception:
        return False


# =========================
# SUMMARY
# =========================
def _summarize_group(trades):
    total = len(trades)

    full_wins = sum(1 for t in trades if _trade_is_tp2_win(t))

    tp1_only = sum(
        1 for t in trades
        if _trade_is_tp1_hit(t) and not _trade_is_tp2_win(t)
    )

    wins = full_wins + tp1_only

    # لأن Wins هنا معناها TP1+
    tp1_hits = wins

    losses = sum(1 for t in trades if _trade_is_loss(t))
    expired = sum(1 for t in trades if _trade_is_expired(t))
    open_count = sum(1 for t in trades if _trade_is_open(t))

    closed = wins + losses + expired
    decided = wins + losses

    winrate = round((wins / decided) * 100, 2) if decided > 0 else 0.0
    tp1_rate = round((tp1_hits / total) * 100, 2) if total > 0 else 0.0

    return {
        "total": total,
        "closed": closed,
        "wins": wins,
        "full_wins": full_wins,
        "tp1_only": tp1_only,
        "losses": losses,
        "expired": expired,
        "open": open_count,
        "tp1_hits": tp1_hits,
        "winrate": winrate,
        "tp1_rate": tp1_rate,
    }


def _score_bucket(score):
    score = _safe_float(score, 0.0)

    if score < 6.3:
        return "<6.3"

    if score < 7.0:
        return "6.3-6.9"

    if score < 8.0:
        return "7.0-7.9"

    return "8.0+"


def _top_reasons(trades, result_filter: str, limit=5):
    counter = Counter()

    for trade in trades:
        if result_filter == "win":
            if not _trade_is_win(trade):
                continue
        elif result_filter == "loss":
            if not _trade_is_loss(trade):
                continue
        elif result_filter == "expired":
            if not _trade_is_expired(trade):
                continue
        else:
            if _trade_result_text(trade) != result_filter:
                continue

        reasons = trade.get("reasons") or []
        normalized = list(dict.fromkeys(_normalize_reason(r) for r in reasons))

        for reason in normalized:
            counter[reason] += 1

    return counter.most_common(limit)


def _format_group_line(name: str, trades: list) -> str:
    s = _summarize_group(trades)
    decided = s["wins"] + s["losses"]

    return (
        f"• {name}: {s['winrate']}% "
        f"({s['wins']}/{decided})"
    )


# =========================
# DEEP REPORT
# =========================
def build_deep_report(redis_client, market_type: str = None, side: str = None) -> str:
    trades = _get_all_trades(redis_client)

    if market_type:
        trades = [
            t for t in trades
            if t.get("market_type", "futures") == market_type
        ]

    if side:
        trades = [
            t for t in trades
            if t.get("side", "long") == side
        ]

    report_label = "Deep Report"
    if side == "short":
        report_label = "Deep Report — Short"
    elif side == "long":
        report_label = "Deep Report — Long"

    if not trades:
        return f"📊 <b>{report_label}</b>\n\nلا توجد صفقات مسجلة بعد."

    overall = _summarize_group(trades)

    # =========================
    # Score buckets
    # =========================
    bucket_groups = defaultdict(list)

    for trade in trades:
        bucket = _score_bucket(trade.get("score"))
        bucket_groups[bucket].append(trade)

    bucket_lines = []

    for bucket in ["<6.3", "6.3-6.9", "7.0-7.9", "8.0+"]:
        group = bucket_groups.get(bucket, [])
        if not group:
            continue

        bucket_lines.append(_format_group_line(bucket, group))

    # =========================
    # Setup types
    # =========================
    pre_only = []
    breakout_only = []
    both = []
    standard = []

    for trade in trades:
        pre = _safe_bool(trade.get("pre_breakout"))
        br = _safe_bool(trade.get("breakout"))

        if pre and br:
            both.append(trade)
        elif pre:
            pre_only.append(trade)
        elif br:
            breakout_only.append(trade)
        else:
            standard.append(trade)

    setup_map = [
        ("Pre-breakout فقط", pre_only),
        ("Breakout فقط", breakout_only),
        ("الاتنين معًا", both),
        ("Standard", standard),
    ]

    setup_lines = []

    for name, group in setup_map:
        if not group:
            continue

        setup_lines.append(_format_group_line(name, group))

    top_wins = _top_reasons(trades, "win", limit=5)
    top_losses = _top_reasons(trades, "loss", limit=5)

    win_reason_lines = [
        f"• {reason} ({count})"
        for reason, count in top_wins
    ] or ["• لا يوجد"]

    loss_reason_lines = [
        f"• {reason} ({count})"
        for reason, count in top_losses
    ] or ["• لا يوجد"]

    return (
        f"📊 <b>{report_label}</b>\n\n"
        f"📌 <b>الملخص العام:</b>\n"
        f"• Signals: {overall['total']}\n"
        f"• Closed: {overall['closed']}\n"
        f"• Wins (TP1+): {overall['wins']}\n"
        f"  - Full Wins (TP2): {overall['full_wins']}\n"
        f"  - TP1 Only: {overall['tp1_only']}\n"
        f"• Losses: {overall['losses']}\n"
        f"• Expired: {overall['expired']}\n"
        f"• Open: {overall['open']}\n"
        f"• TP1 Hits: {overall['tp1_hits']}\n"
        f"• Win Rate: {overall['winrate']}%\n"
        f"• TP1 Rate: {overall['tp1_rate']}%\n\n"
        f"🎯 <b>حسب السكور:</b>\n"
        f"{chr(10).join(bucket_lines) if bucket_lines else '• لا يوجد'}\n\n"
        f"🧠 <b>حسب نوع الإشارة:</b>\n"
        f"{chr(10).join(setup_lines) if setup_lines else '• لا يوجد'}\n\n"
        f"✅ <b>أكثر أسباب الفوز:</b>\n"
        f"{chr(10).join(win_reason_lines)}\n\n"
        f"⚠️ <b>أكثر أسباب الخسارة:</b>\n"
        f"{chr(10).join(loss_reason_lines)}"
    )
