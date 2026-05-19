"""Mode Transition Guard — Phase 3.

يحمي الـ mode transitions الممنوعة صراحة في فلسفة المشروع.

القواعد المحمية:
1. STRONG → RECOVERY: ممنوع (Recovery من BLOCK فقط)
2. NORMAL → RECOVERY: ممنوع (Recovery من BLOCK فقط)
3. RECOVERY → STRONG مباشرة: مسموح (طريق خروج طبيعي)
4. BLOCK → NORMAL مباشرة: ممنوع (لازم يعدي على STRONG أو RECOVERY)
"""
from __future__ import annotations

from utils.constants import (
    MODE_NORMAL_LONG,
    MODE_STRONG_LONG_ONLY,
    MODE_BLOCK_LONGS,
    MODE_RECOVERY_LONG,
)

# الـ transitions الممنوعة صراحة
_FORBIDDEN_TRANSITIONS: set[tuple[str, str]] = {
    # RECOVERY من NORMAL ممنوع — Recovery طريق ما بعد BLOCK فقط
    (MODE_NORMAL_LONG, MODE_RECOVERY_LONG),
    # RECOVERY من STRONG ممنوع مباشرة
    (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG),
    # NORMAL مباشرة من BLOCK ممنوع — لازم STRONG أو RECOVERY في النص
    (MODE_BLOCK_LONGS, MODE_NORMAL_LONG),
}

# الـ transitions المسموحة الكاملة
_ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    MODE_NORMAL_LONG: {MODE_STRONG_LONG_ONLY, MODE_BLOCK_LONGS, MODE_NORMAL_LONG},
    MODE_STRONG_LONG_ONLY: {MODE_NORMAL_LONG, MODE_BLOCK_LONGS, MODE_STRONG_LONG_ONLY},
    MODE_BLOCK_LONGS: {MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG, MODE_BLOCK_LONGS},
    MODE_RECOVERY_LONG: {MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY, MODE_BLOCK_LONGS, MODE_RECOVERY_LONG},
}

# الـ fallback لو transition ممنوع
_FORBIDDEN_FALLBACK: dict[tuple[str, str], str] = {
    (MODE_NORMAL_LONG, MODE_RECOVERY_LONG):   MODE_STRONG_LONG_ONLY,
    (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG): MODE_STRONG_LONG_ONLY,
    (MODE_BLOCK_LONGS, MODE_NORMAL_LONG):     MODE_STRONG_LONG_ONLY,
}


def is_transition_allowed(from_mode: str, to_mode: str) -> bool:
    """بيتحقق إن الـ transition مسموح."""
    if from_mode == to_mode:
        return True
    if (from_mode, to_mode) in _FORBIDDEN_TRANSITIONS:
        return False
    allowed = _ALLOWED_TRANSITIONS.get(from_mode, set())
    return to_mode in allowed


def safe_transition(from_mode: str, candidate_mode: str) -> tuple[str, bool, str]:
    """بيرجع الـ mode الآمن مع سبب لو اتبدل.

    Returns:
        (final_mode, was_overridden, reason)
    """
    if is_transition_allowed(from_mode, candidate_mode):
        return candidate_mode, False, ""

    fallback = _FORBIDDEN_FALLBACK.get((from_mode, candidate_mode), from_mode)
    reason = (
        f"forbidden_transition:{from_mode}→{candidate_mode}"
        f"_redirected_to:{fallback}"
    )
    return fallback, True, reason


def validate_mode(mode: str) -> str:
    """بيتحقق إن الـ mode موجود وصح."""
    valid = {MODE_NORMAL_LONG, MODE_STRONG_LONG_ONLY, MODE_BLOCK_LONGS, MODE_RECOVERY_LONG}
    return mode if mode in valid else MODE_NORMAL_LONG
