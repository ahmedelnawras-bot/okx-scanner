from __future__ import annotations

from utils.constants import REPORT_PERIOD_ORDER
from utils.time_utils import cutoff_for_period


def get_period_cutoff(period: str):
    return cutoff_for_period(period)


def all_periods() -> tuple[str, ...]:
    return REPORT_PERIOD_ORDER
