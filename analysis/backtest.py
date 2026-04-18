from collections import Counter, defaultdict


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _safe_bool(value):
    return bool(value)


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
        "شمعة جيدة": "شمعة جيدة",
        "شمعة قوية": "شمعة قوية",
        "اختراق": "اختراق",
        "اختراق مبكر جداً": "اختراق مبكر",
        "اختراق متأخر": "اختراق متأخر",
        "اختراق قوي مؤكد": "اختراق قوي مؤكد",
        "تأكيد فريم الساعة": "تأكيد فريم الساعة",
        "BTC داعم": "BTC داعم",
        "هيمنة داعمة": "هيمنة داعمة للألت",
        "هيمنة ضد الألت": "هيمنة ضد الألت (ضغط على العملات)",
        "تمويل سلبي": "تمويل سلبي (داعم للشراء)",
        "عملة جديدة": "عملة جديدة",
        "بداية ترند مبكرة": "بداية ترند مبكرة",
        "زخم مبكر تحت المقاومة 🎯": "زخم مبكر تحت المقاومة 🎯",
        "بعيد عن MA (متأخر)": "بعيد عن المتوسط (دخول متأخر)",
        "ممتد زيادة": "ممتد زيادة",
    }
    return mapping.get(reason, reason)


def _load_trade(redis_client, trade_key: str):
    if redis_client is None:
        return None
    try:
        raw = redis_client.get(trade_key)
        if not raw:
            return None
        import json
        return json.loads(raw)
    except Exception:
        return None


def _get_all_trades(redis_client):
    if redis_client is None:
        return []

    try:
        keys = list(redis_client.smembers("trades:all"))
    except Exception:
        return []

    trades = []
    for key in keys:
        trade = _load_trade(redis_client, key)
        if trade:
            trades.append(trade)
    return trades


def _summarize_group(trades):
    total = len(trades)
    wins = sum(1 for t in trades if t.get("result") == "win")
    losses = sum(1 for t in trades if t.get("result") == "loss")
    expired = sum(1 for t in trades if t.get("result") == "expired")
    open_count = sum(1 for t in trades if t.get("status") in ("open", "partial"))
    tp1_hits = sum(1 for t in trades if t.get("tp1_hit"))

    decided = wins + losses
    winrate = round((wins / decided) * 100, 2) if decided > 0 else 0.0
    tp1_rate = round((tp1_hits / total) * 100, 2) if total > 0 else 0.0

    return {
        "total": total,
        "wins": wins,
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
        if trade.get("result") != result_filter:
            continue

        reasons = trade.get("reasons") or []
        normalized = list(dict.fromkeys(_normalize_reason(r) for r in reasons))
        for reason in normalized:
            counter[reason] += 1

    return counter.most_common(limit)


def build_deep_report(redis_client, market_type: str = None, side: str = None) -> str:
    trades = _get_all_trades(redis_client)

    if market_type:
        trades = [t for t in trades if t.get("market_type", "futures") == market_type]
    if side:
        trades = [t for t in trades if t.get("side", "long") == side]

    report_label = "Deep Report"
    if side == "short":
        report_label = "Deep Report — Short"
    elif side == "long":
        report_label = "Deep Report — Long"

    if not trades:
        return f"📊 <b>{report_label}</b>\n\nلا توجد صفقات مسجلة بعد."

    overall = _summarize_group(trades)

    # Score buckets
    bucket_groups = defaultdict(list)
    for trade in trades:
        bucket_groups[_score_bucket(trade.get("score"))].append(trade)

    bucket_lines = []
    for bucket in ["<6.3", "6.3-6.9", "7.0-7.9", "8.0+"]:
        group = bucket_groups.get(bucket, [])
        if not group:
            continue
        s = _summarize_group(group)
        bucket_lines.append(f"• {bucket}: {s['winrate']}% ({s['wins']}/{s['wins'] + s['losses']})")

    # Setup types
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
        s = _summarize_group(group)
        setup_lines.append(f"• {name}: {s['winrate']}% ({s['wins']}/{s['wins'] + s['losses']})")

    top_wins = _top_reasons(trades, "win", limit=5)
    top_losses = _top_reasons(trades, "loss", limit=5)

    win_reason_lines = [f"• {reason} ({count})" for reason, count in top_wins] or ["• لا يوجد"]
    loss_reason_lines = [f"• {reason} ({count})" for reason, count in top_losses] or ["• لا يوجد"]

    return (
        f"📊 <b>{report_label}</b>\n\n"
        f"📌 <b>الملخص العام:</b>\n"
        f"• Signals: {overall['total']}\n"
        f"• Wins: {overall['wins']}\n"
        f"• Losses: {overall['losses']}\n"
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
