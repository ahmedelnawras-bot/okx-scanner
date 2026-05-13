from __future__ import annotations

from datetime import datetime, timedelta, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def minutes_between(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 60)


def iso_now() -> str:
    return utc_now().isoformat()


def cutoff_for_period(period: str) -> datetime | None:
    now = utc_now()
    if period == "last_1h":
        return now - timedelta(hours=1)
    if period == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "last_7d":
        return now - timedelta(days=7)
    return None
