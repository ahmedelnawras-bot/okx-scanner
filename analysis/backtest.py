import os
import json
import redis
from collections import Counter, defaultdict


REDIS_URL = os.getenv("REDIS_URL")


def get_redis():
    if not REDIS_URL:
        raise ValueError("REDIS_URL not found")
    client = redis.from_url(REDIS_URL, decode_responses=True)
    client.ping()
    return client


def load_trade(redis_client, trade_key: str):
    try:
        raw = redis_client.get(trade_key)
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


def get_all_trades(redis_client):
    try:
        trade_keys = list(redis_client.smembers("trades:all"))
    except Exception:
        return []

    trades = []
    for key in trade_keys:
        trade = load_trade(redis_client, key)
        if trade:
            trades.append(trade)

    return trades


def is_decided_trade(trade: dict) -> bool:
    return trade.get("result") in ("win", "loss")


def summarize_group(trades):
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


def get_score_bucket(score: float) -> str:
    try:
        score = float(score)
    except Exception:
        return "unknown"

    if score < 6.3:
        return "< 6.3"
    if score < 7.0:
        return "6.3 - 6.9"
    if score < 8.0:
        return "7.0 - 7.9"
    return "8.0+"


def summarize_by_score_bucket(trades):
    buckets = defaultdict(list)
    for trade in trades:
        bucket = get_score_bucket(trade.get("score", 0))
        buckets[bucket].append(trade)

    ordered = ["< 6.3", "6.3 - 6.9", "7.0 - 7.9", "8.0+", "unknown"]
    rows = []
    for bucket in ordered:
        group = buckets.get(bucket, [])
        if not group:
            continue
        summary = summarize_group(group)
        rows.append((bucket, summary))
    return rows


def summarize_setup_types(trades):
    groups = {
        "pre_breakout_only": [],
        "breakout_only": [],
        "breakout_and_pre_breakout": [],
        "standard": [],
        "new_listing": [],
    }

    for trade in trades:
        pre_breakout = bool(trade.get("pre_breakout", False))
        breakout = bool(trade.get("breakout", False))
        is_new = bool(trade.get("is_new", False))

        if is_new:
            groups["new_listing"].append(trade)

        if pre_breakout and breakout:
            groups["breakout_and_pre_breakout"].append(trade)
        elif pre_breakout:
            groups["pre_breakout_only"].append(trade)
        elif breakout:
            groups["breakout_only"].append(trade)
        else:
            groups["standard"].append(trade)

    rows = []
    for name, group in groups.items():
        if not group:
            continue
        rows.append((name, summarize_group(group)))
    return rows


def normalize_reason(reason: str) -> str:
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


def top_reasons(trades, result_filter: str, limit: int = 10):
    counter = Counter()

    for trade in trades:
        if trade.get("result") != result_filter:
            continue

        reasons = trade.get("reasons", []) or []
        normalized = list(dict.fromkeys(normalize_reason(r) for r in reasons))
        for reason in normalized:
            counter[reason] += 1

    return counter.most_common(limit)


def avg_metrics(trades):
    if not trades:
        return {
            "avg_score": 0.0,
            "avg_vol_ratio": 0.0,
            "avg_candle_strength": 0.0,
        }

    def _avg(field):
        vals = []
        for t in trades:
            try:
                vals.append(float(t.get(field, 0)))
            except Exception:
                pass
        return round(sum(vals) / len(vals), 4) if vals else 0.0

    return {
        "avg_score": _avg("score"),
        "avg_vol_ratio": _avg("vol_ratio"),
        "avg_candle_strength": _avg("candle_strength"),
    }


def print_summary(title: str, summary: dict):
    print(f"\n=== {title} ===")
    print(f"Total     : {summary['total']}")
    print(f"Wins      : {summary['wins']}")
    print(f"Losses    : {summary['losses']}")
    print(f"Expired   : {summary['expired']}")
    print(f"Open      : {summary['open']}")
    print(f"TP1 Hits  : {summary['tp1_hits']}")
    print(f"Win Rate  : {summary['winrate']}%")
    print(f"TP1 Rate  : {summary['tp1_rate']}%")


def main():
    redis_client = get_redis()
    trades = get_all_trades(redis_client)

    if not trades:
        print("No trades found.")
        return

    print_summary("OVERALL", summarize_group(trades))

    decided_trades = [t for t in trades if is_decided_trade(t)]
    print_summary("DECIDED ONLY", summarize_group(decided_trades))

    print("\n=== SCORE BUCKETS ===")
    for bucket, summary in summarize_by_score_bucket(trades):
        metrics = avg_metrics([
            t for t in trades
            if get_score_bucket(t.get("score", 0)) == bucket
        ])
        print(
            f"{bucket:12} | total={summary['total']:3} | "
            f"winrate={summary['winrate']:6}% | "
            f"avg_score={metrics['avg_score']}"
        )

    print("\n=== SETUP TYPES ===")
    for name, summary in summarize_setup_types(trades):
        metrics = avg_metrics([
            t for t in trades
            if (
                (name == "pre_breakout_only" and t.get("pre_breakout") and not t.get("breakout")) or
                (name == "breakout_only" and t.get("breakout") and not t.get("pre_breakout")) or
                (name == "breakout_and_pre_breakout" and t.get("breakout") and t.get("pre_breakout")) or
                (name == "standard" and not t.get("breakout") and not t.get("pre_breakout")) or
                (name == "new_listing" and t.get("is_new"))
            )
        ])
        print(
            f"{name:24} | total={summary['total']:3} | "
            f"winrate={summary['winrate']:6}% | "
            f"avg_score={metrics['avg_score']} | "
            f"avg_vol_ratio={metrics['avg_vol_ratio']}"
        )

    print("\n=== TOP WINNING REASONS ===")
    for reason, count in top_reasons(trades, "win", limit=12):
        print(f"{reason} -> {count}")

    print("\n=== TOP LOSING REASONS ===")
    for reason, count in top_reasons(trades, "loss", limit=12):
        print(f"{reason} -> {count}")


if __name__ == "__main__":
    main()
