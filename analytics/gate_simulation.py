"""Passive multi-recipe gate simulation reports for AI/replay research.

This module is read-only:
- it does not change live scoring, filters, TP/SL, market modes, or execution,
- it compares proposed mode gates against Historical Replay and Live Snapshot data,
- it uses one shared Gate Quality Score formula, then compares multiple recipes
  per mode by changing only acceptance thresholds and technical requirements.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
from statistics import median
from typing import Any, Iterable

from analytics.technical_dataset import load_snapshot_records
from historical_replay.dataset_writer import iter_records as iter_replay_records


MODES = {
    "normal": "NORMAL_LONG",
    "recovery": "RECOVERY_LONG",
    "strong": "STRONG_LONG_ONLY",
    "block": "BLOCK_LONGS",
}

GOOD_CONTINUATION_SETUPS = {"higher_low_continuation", "support_bounce_confirmed", "vwap_reclaim"}
RECOVERY_CORE_SETUPS = {"support_bounce_confirmed", "vwap_reclaim", "higher_low_continuation"}
STRONG_CORE_SETUPS = {"higher_low_continuation", "vwap_reclaim", "wave_3", "support_bounce_confirmed"}
BLOCK_EXCEPTION_SETUPS = {"relative_strength_vs_btc", "retest_breakout_confirmed", "vwap_reclaim", "wave_3"}
HIGH_RISK_SYMBOLS = {
    "RAVE-USDT-SWAP",
    "BSB-USDT-SWAP",
    "LAB-USDT-SWAP",
    "BASED-USDT-SWAP",
    "RLS-USDT-SWAP",
    "SIGN-USDT-SWAP",
}

GATE_SIM_EXPORT_DIR = Path("data/gate_simulations")

# One shared quality score formula for all modes/recipes.
QUALITY_SCORE_WEIGHTS = {
    "setup_quality": 35,
    "market_context": 20,
    "volume_momentum": 20,
    "risk_location": 15,
    "bot_score_zone": 10,
}


@dataclass(frozen=True)
class GateRecipe:
    name: str
    mode_key: str
    title: str
    description: str
    min_quality_score: float
    allowed_setups: tuple[str, ...]
    clean_setups_only: bool = False
    allow_relative_strength: bool = True
    relative_strength_min_vol: float = 1.15
    wave3_min_vol: float = 1.20
    min_vol_ratio: float = 0.0
    reject_near_resistance_without_breakout: bool = True
    reject_high_risk_without_edge: bool = True
    reject_extreme_score_without_confirmation: bool = True
    require_recovery_bounce: bool = False
    require_strong_confirmation: bool = False
    require_rs_structure_hybrid: bool = False
    require_ema_slope_close_filter: bool = False
    reject_normal_overextension: bool = False

    def rules(self) -> list[str]:
        rows = [
            f"Recipe: {self.name} — {self.title}",
            f"المود المطلوب: {MODES.get(self.mode_key, self.mode_key)}.",
            f"Gate Quality Score المطلوب: >= {self.min_quality_score:.1f} / 100.",
            f"السيت أب المسموح: {', '.join(self.allowed_setups) if self.allowed_setups else 'أي setup بشرط تحقيق الجودة' }.",
        ]
        if self.clean_setups_only:
            rows.append("يسمح فقط بالسيت أب النظيف المحدد؛ relative strength وحدها لا تكفي.")
        if self.allow_relative_strength:
            rows.append(f"relative_strength_vs_btc مسموحة إذا vol_ratio >= {self.relative_strength_min_vol:.2f}.")
        else:
            rows.append("relative_strength_vs_btc وحدها لا تقبل في هذه الخلطة.")
        if "wave_3" in self.allowed_setups:
            rows.append(f"wave_3 تحتاج vol_ratio >= {self.wave3_min_vol:.2f} أو MTF/breakout/relative strength.")
        if self.min_vol_ratio > 0:
            rows.append(f"الحد الأدنى للفوليوم: vol_ratio >= {self.min_vol_ratio:.2f} عند توفره.")
        if self.reject_near_resistance_without_breakout:
            rows.append("رفض near_resistance إلا مع breakout مؤكد.")
        if self.reject_high_risk_without_edge:
            rows.append("رموز high-risk تحتاج MTF/breakout أو جودة عالية جدًا.")
        if self.reject_extreme_score_without_confirmation:
            rows.append("منطقة السكور extreme لا تُرفض وحدها، لكنها تحتاج confirmation إذا معها امتداد/مقاومة.")
        if self.require_recovery_bounce:
            rows.append("Recovery يحتاج bounce/reclaim/support واضح؛ relative strength وحدها لا تكفي.")
        if self.require_strong_confirmation:
            rows.append("Strong/Block يحتاج relative strength أو volume/momentum confirmation.")
        if self.require_rs_structure_hybrid:
            rows.append("RS Hybrid: القوة النسبية لا تكفي وحدها؛ تحتاج structure/reclaim/breakout/MTF معها.")
        if self.require_ema_slope_close_filter:
            rows.append("EMA Slope + Close Position: يرفض Long إذا كان الإغلاق تحت EMA مع ميل EMA سلبي واضح.")
        if self.reject_normal_overextension:
            rows.append("Normal Overextension: يرفض الإشارات التي صعدت بشكل مبالغ فيه قبل الدخول أو أصبحت بعيدة جدًا فوق EMA.")
        if self.mode_key == "block":
            rows.append("BLOCK_LONGS: هذه ليست صفقات عادية؛ هي استثناءات فقط أثناء منع اللونج.")
        return rows


RECIPES: dict[str, tuple[GateRecipe, ...]] = {
    "normal": (
        # Recipe #1 is the protected Normal baseline from the last analysis. Keep unchanged.
        GateRecipe(
            name="normal_conservative",
            mode_key="normal",
            title="فلترة صارمة لاستمرار نظيف — baseline ثابت",
            description="يحافظ على أفضل setups فقط ويقلل ضغط المرشحين بقوة. هذه الخلطة رقم 1 ولا نعدل شروطها عند التجارب.",
            min_quality_score=72,
            allowed_setups=tuple(sorted(GOOD_CONTINUATION_SETUPS)),
            clean_setups_only=True,
            allow_relative_strength=False,
            min_vol_ratio=1.10,
        ),
        # Recipe #2 is the current best Normal challenger. Keep it as reference.
        GateRecipe(
            name="normal_structure_only",
            mode_key="normal",
            title="Structure فقط بدون مومنتوم عشوائي",
            description="أفضل منافس حاليًا: Normal كاستمرار نظيف قائم على structure/reclaim فقط، بدون relative strength وحدها.",
            min_quality_score=74,
            allowed_setups=tuple(sorted({"higher_low_continuation", "support_bounce_confirmed", "vwap_reclaim"})),
            clean_setups_only=True,
            allow_relative_strength=False,
            min_vol_ratio=1.15,
            reject_high_risk_without_edge=True,
        ),
        # Recipe #3 is intentionally different: very compact VWAP/HL reclaim with stricter noise control.
        GateRecipe(
            name="normal_compact_reclaim",
            mode_key="normal",
            title="Reclaim مضغوط بمخاطرة أنظف",
            description="خلطة جديدة مختلفة: تقبل Normal فقط عند وجود reclaim/HL واضح، فوليوم جيد، ورفض قوي للضوضاء والامتداد الضعيف.",
            min_quality_score=76,
            allowed_setups=tuple(sorted({"vwap_reclaim", "higher_low_continuation"})),
            clean_setups_only=True,
            allow_relative_strength=False,
            min_vol_ratio=1.18,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
            reject_extreme_score_without_confirmation=True,
        ),
        # Normal filter variants: same recipes, plus overextension protection. Originals remain unchanged.
        GateRecipe(
            name="normal_conservative_no_overextension",
            mode_key="normal",
            title="فلترة صارمة + منع المطاردة",
            description="Variant للمقارنة فقط: normal_conservative مع فلتر يمنع الإشارات التي صعدت بشكل مبالغ فيه قبل الدخول.",
            min_quality_score=72,
            allowed_setups=tuple(sorted(GOOD_CONTINUATION_SETUPS)),
            clean_setups_only=True,
            allow_relative_strength=False,
            min_vol_ratio=1.10,
            reject_normal_overextension=True,
        ),
        GateRecipe(
            name="normal_structure_only_no_overextension",
            mode_key="normal",
            title="Structure فقط + منع المطاردة",
            description="Variant للمقارنة فقط: normal_structure_only مع فلتر يمنع الدخول بعد امتداد سعري مبالغ فيه.",
            min_quality_score=74,
            allowed_setups=tuple(sorted({"higher_low_continuation", "support_bounce_confirmed", "vwap_reclaim"})),
            clean_setups_only=True,
            allow_relative_strength=False,
            min_vol_ratio=1.15,
            reject_high_risk_without_edge=True,
            reject_normal_overextension=True,
        ),
        GateRecipe(
            name="normal_compact_reclaim_no_overextension",
            mode_key="normal",
            title="Reclaim مضغوط + منع المطاردة",
            description="Variant للمقارنة فقط: normal_compact_reclaim مع فلتر يمنع الدخول إذا السعر بعيد جدًا فوق EMA أو الحركة السابقة ممتدة.",
            min_quality_score=76,
            allowed_setups=tuple(sorted({"vwap_reclaim", "higher_low_continuation"})),
            clean_setups_only=True,
            allow_relative_strength=False,
            min_vol_ratio=1.18,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
            reject_extreme_score_without_confirmation=True,
            reject_normal_overextension=True,
        ),
    ),
    "recovery": (
        GateRecipe(
            name="recovery_bounce_confirmed",
            mode_key="recovery",
            title="ارتداد دعم مؤكد",
            description="خلطة Recovery رقم 1: لا تشتري السقوط؛ تقبل فقط support bounce / higher low / reclaim واضح.",
            min_quality_score=72,
            allowed_setups=tuple(sorted({"support_bounce_confirmed", "higher_low_continuation", "vwap_reclaim"})),
            clean_setups_only=True,
            allow_relative_strength=False,
            min_vol_ratio=1.08,
            require_recovery_bounce=True,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
        ),
        GateRecipe(
            name="recovery_vwap_reclaim",
            mode_key="recovery",
            title="استرداد VWAP بعد هبوط",
            description="خلطة Recovery رقم 2: تركز على VWAP reclaim أو retest clean بعد ضغط، مع منع relative strength وحدها.",
            min_quality_score=68,
            allowed_setups=tuple(sorted({"vwap_reclaim", "support_bounce_confirmed", "retest_breakout_confirmed"})),
            clean_setups_only=True,
            allow_relative_strength=False,
            min_vol_ratio=1.12,
            require_recovery_bounce=True,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
        ),
        GateRecipe(
            name="recovery_fast_reversal_safe",
            mode_key="recovery",
            title="ارتداد سريع مشروط",
            description="خلطة Recovery رقم 3: تسمح بارتداد أسرع لكن فقط مع volume/RS واضح وبدون recovery-fail context.",
            min_quality_score=64,
            allowed_setups=tuple(sorted({"support_bounce_confirmed", "vwap_reclaim", "higher_low_continuation", "relative_strength_vs_btc"})),
            clean_setups_only=False,
            allow_relative_strength=True,
            relative_strength_min_vol=1.25,
            min_vol_ratio=1.05,
            require_recovery_bounce=True,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
        ),
    ),
    "strong": (
        # Recipe #1 is the protected baseline. Do not change it when testing new Strong ideas.
        GateRecipe(
            name="strong_strict",
            mode_key="strong",
            title="أقوى العملات فقط — baseline ثابت",
            description="الخلطة الناجحة الحالية؛ تُستخدم كخط أساس ولا نعدل شروطها عند إضافة مؤشرات جديدة.",
            min_quality_score=70,
            allowed_setups=tuple(sorted(STRONG_CORE_SETUPS)),
            relative_strength_min_vol=1.20,
            wave3_min_vol=1.25,
            min_vol_ratio=1.15,
            require_strong_confirmation=True,
        ),
        # Recipe #2 is the current best Strong challenger. Keep it as reference.
        GateRecipe(
            name="strong_runner_hunter",
            mode_key="strong",
            title="اصطياد امتداد TP2/Runner",
            description="أفضل منافس حاليًا: يركز على wave_3 و vwap_reclaim و relative strength بفوليوم واضح لصيد امتداد runner.",
            min_quality_score=72,
            allowed_setups=tuple(sorted({"wave_3", "vwap_reclaim", "relative_strength_vs_btc", "retest_breakout_confirmed"})),
            relative_strength_min_vol=1.25,
            wave3_min_vol=1.30,
            min_vol_ratio=1.15,
            reject_high_risk_without_edge=True,
            require_strong_confirmation=True,
        ),
        # Recipe #3 is intentionally different: cleaner pullback/reclaim, not pure momentum chasing.
        GateRecipe(
            name="strong_pullback_reclaim",
            mode_key="strong",
            title="Pullback ثم Reclaim نظيف",
            description="خلطة جديدة مختلفة: تبحث عن pullback/reclaim أو higher-low داخل مود Strong بدل مطاردة wave_3 فقط.",
            min_quality_score=74,
            allowed_setups=tuple(sorted({"vwap_reclaim", "higher_low_continuation", "support_bounce_confirmed", "retest_breakout_confirmed"})),
            clean_setups_only=True,
            allow_relative_strength=False,
            wave3_min_vol=1.30,
            min_vol_ratio=1.12,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
            reject_extreme_score_without_confirmation=True,
            require_strong_confirmation=True,
        ),
        # Recipe #4: RS + structure hybrid. Adds a new test without changing existing recipes.
        GateRecipe(
            name="strong_rs_hybrid",
            mode_key="strong",
            title="RS + Structure Hybrid",
            description="خلطة Strong جديدة: لا تقبل القوة النسبية وحدها؛ تحتاج معها structure/reclaim/breakout أو MTF لتقليل الارتدادات الضعيفة.",
            min_quality_score=71,
            allowed_setups=tuple(sorted({"relative_strength_vs_btc", "vwap_reclaim", "higher_low_continuation", "support_bounce_confirmed", "retest_breakout_confirmed", "wave_3"})),
            clean_setups_only=False,
            allow_relative_strength=True,
            relative_strength_min_vol=1.25,
            wave3_min_vol=1.28,
            min_vol_ratio=1.10,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
            reject_extreme_score_without_confirmation=True,
            require_strong_confirmation=True,
            require_rs_structure_hybrid=True,
        ),
        # Recipe #5: volume edge. Volume can act as the strong confirmation, while risk filters remain unchanged.
        GateRecipe(
            name="strong_volume_edge",
            mode_key="strong",
            title="Volume Edge Confirmation",
            description="خلطة Strong جديدة: فوليوم واضح جدًا يسمح بالقبول حتى لو التأكيدات الأخرى أقل، مع استمرار رفض المقاومة/high-risk/امتداد بلا تأكيد.",
            min_quality_score=69,
            allowed_setups=tuple(sorted({"wave_3", "vwap_reclaim", "retest_breakout_confirmed", "higher_low_continuation"})),
            clean_setups_only=False,
            allow_relative_strength=False,
            relative_strength_min_vol=1.55,
            wave3_min_vol=1.45,
            min_vol_ratio=1.55,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
            reject_extreme_score_without_confirmation=True,
            require_strong_confirmation=True,
        ),
        # Strong filter variants: best three recipes + EMA slope / close-position protection. Originals remain unchanged.
        GateRecipe(
            name="strong_strict_ema_slope_close",
            mode_key="strong",
            title="Baseline + EMA slope/close",
            description="Variant للمقارنة فقط: strong_strict مع فلتر يرفض Long إذا الإغلاق تحت EMA وميل EMA سلبي بوضوح.",
            min_quality_score=70,
            allowed_setups=tuple(sorted(STRONG_CORE_SETUPS)),
            relative_strength_min_vol=1.20,
            wave3_min_vol=1.25,
            min_vol_ratio=1.15,
            require_strong_confirmation=True,
            require_ema_slope_close_filter=True,
        ),
        GateRecipe(
            name="strong_runner_hunter_ema_slope_close",
            mode_key="strong",
            title="Runner Hunter + EMA slope/close",
            description="Variant للمقارنة فقط: strong_runner_hunter مع فلتر يمنع مطاردة Long تحت EMA عندما يكون الميل هابطًا.",
            min_quality_score=72,
            allowed_setups=tuple(sorted({"wave_3", "vwap_reclaim", "relative_strength_vs_btc", "retest_breakout_confirmed"})),
            relative_strength_min_vol=1.25,
            wave3_min_vol=1.30,
            min_vol_ratio=1.15,
            reject_high_risk_without_edge=True,
            require_strong_confirmation=True,
            require_ema_slope_close_filter=True,
        ),
        GateRecipe(
            name="strong_rs_hybrid_ema_slope_close",
            mode_key="strong",
            title="RS Hybrid + EMA slope/close",
            description="Variant للمقارنة فقط: strong_rs_hybrid مع نفس شرط RS+Structure وفوقه فلتر EMA slope/close.",
            min_quality_score=71,
            allowed_setups=tuple(sorted({"relative_strength_vs_btc", "vwap_reclaim", "higher_low_continuation", "support_bounce_confirmed", "retest_breakout_confirmed", "wave_3"})),
            clean_setups_only=False,
            allow_relative_strength=True,
            relative_strength_min_vol=1.25,
            wave3_min_vol=1.28,
            min_vol_ratio=1.10,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
            reject_extreme_score_without_confirmation=True,
            require_strong_confirmation=True,
            require_rs_structure_hybrid=True,
            require_ema_slope_close_filter=True,
        ),
    ),
    "block": (
        GateRecipe(
            name="block_exception_ultra_strict",
            mode_key="block",
            title="استثناء بلوك نادر جدًا",
            description="خلطة Block رقم 1: لا تسمح إلا بإشارة استثنائية عالية الجودة جدًا وقت منع اللونج.",
            min_quality_score=80,
            allowed_setups=tuple(sorted({"relative_strength_vs_btc", "retest_breakout_confirmed", "vwap_reclaim"})),
            clean_setups_only=True,
            allow_relative_strength=True,
            relative_strength_min_vol=1.45,
            min_vol_ratio=1.30,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
            reject_extreme_score_without_confirmation=True,
            require_strong_confirmation=True,
        ),
        GateRecipe(
            name="block_exception_breakout_retest",
            mode_key="block",
            title="استثناء اختراق/ريتست مؤكد",
            description="خلطة Block رقم 2: تركز فقط على retest_breakout_confirmed أو VWAP reclaim نظيف جدًا أثناء BLOCK_LONGS.",
            min_quality_score=76,
            allowed_setups=("retest_breakout_confirmed", "vwap_reclaim"),
            clean_setups_only=True,
            allow_relative_strength=False,
            min_vol_ratio=1.25,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
            reject_extreme_score_without_confirmation=True,
            require_strong_confirmation=True,
        ),
        GateRecipe(
            name="block_exception_rs_volume",
            mode_key="block",
            title="استثناء قوة نسبية بفوليوم",
            description="خلطة Block رقم 3: تقبل relative strength فقط إذا كان الفوليوم واضحًا والسياق ليس انهيارًا مباشرًا.",
            min_quality_score=78,
            allowed_setups=("relative_strength_vs_btc", "wave_3"),
            clean_setups_only=False,
            allow_relative_strength=True,
            relative_strength_min_vol=1.55,
            wave3_min_vol=1.45,
            min_vol_ratio=1.35,
            reject_near_resistance_without_breakout=True,
            reject_high_risk_without_edge=True,
            reject_extreme_score_without_confirmation=True,
            require_strong_confirmation=True,
        ),
    ),
}


def _pct(part: int | float, total: int | float) -> str:
    total = float(total or 0)
    if total <= 0:
        return "0.0%"
    return f"{(float(part or 0) / total) * 100.0:.1f}%"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _features(record: dict[str, Any]) -> dict[str, Any]:
    value = record.get("features")
    return value if isinstance(value, dict) else {}


def _first_existing(features: dict[str, Any], names: tuple[str, ...], default: Any = None) -> Any:
    for name in names:
        if name in features and features.get(name) is not None:
            return features.get(name)
    return default


def _setup_tags(record: dict[str, Any]) -> set[str]:
    features = _features(record)
    tags = features.get("setup_tags") or []
    if not isinstance(tags, list):
        tags = []
    setup_type = str(features.get("setup_type") or "")
    result = {str(x) for x in tags if x}
    if setup_type:
        result.add(setup_type)
    return result


def _pair_tags(record: dict[str, Any]) -> set[str]:
    tags = _features(record).get("pair_tags") or []
    if not isinstance(tags, list):
        tags = []
    return {str(x) for x in tags if x}


def _warnings_text(record: dict[str, Any]) -> str:
    features = _features(record)
    warnings = features.get("warnings") or []
    if not isinstance(warnings, list):
        warnings = [warnings]
    return " ".join([str(features.get("resistance_warning") or "")] + [str(x) for x in warnings])


def _near_resistance(record: dict[str, Any]) -> bool:
    text = _warnings_text(record).lower()
    tags = _setup_tags(record) | _pair_tags(record)
    return bool("near_resistance" in tags or "resistance" in text or "مقاوم" in text)


def _breakout_confirmed(record: dict[str, Any]) -> bool:
    features = _features(record)
    quality = str(features.get("breakout_quality") or "").lower()
    return bool(features.get("breakout") or "breakout" in _setup_tags(record) or quality in {"good", "strong"})


def _mtf_confirmed(record: dict[str, Any]) -> bool:
    features = _features(record)
    return bool(features.get("mtf_confirmed") or features.get("mtf") == "confirmed")


def _score(record: dict[str, Any]) -> float:
    features = _features(record)
    return _num(features.get("effective_score"), _num(features.get("score"), _num(features.get("raw_score"), _num(record.get("score")))))


def _vol_ratio(record: dict[str, Any]) -> float:
    features = _features(record)
    return _num(_first_existing(features, ("vol_ratio", "volume_ratio", "volume_spike_ratio"), 1.0), 1.0)


def _ema_slope_pct(record: dict[str, Any]) -> float | None:
    """Best-effort EMA slope reader. Missing data returns None, not bearish."""
    features = _features(record)
    value = _first_existing(
        features,
        (
            "ema_slope_pct",
            "ema20_slope_pct",
            "ema_20_slope_pct",
            "ema10_slope_pct",
            "ema_10_slope_pct",
            "ema5_slope_pct",
            "ema_5_slope_pct",
            "ema_slope",
            "ma_slope_pct",
            "ma5_slope_pct",
            "ma_5_slope_pct",
            "btc_1h_ma5_slope_pct",
        ),
        None,
    )
    if value is None:
        return None
    return _num(value, 0.0)


def _close_ema_gap_pct(record: dict[str, Any]) -> float | None:
    """Percent distance of close from EMA/MA. Positive means above EMA, negative below.

    Live snapshots currently expose this mainly as ``dist_ma``. Historical replay
    may not have a direct MA distance, so this function still falls back to
    close/EMA values when they are present.
    """
    features = _features(record)
    direct = _first_existing(
        features,
        (
            "close_ema_gap_pct",
            "close_vs_ema_pct",
            "close_to_ema_pct",
            "price_to_ema_pct",
            "ema_distance_pct",
            "distance_from_ema_pct",
            "close_ma_gap_pct",
            "close_vs_ma_pct",
            "close_to_ma_pct",
            "ma_distance_pct",
            "dist_ma",
            "distance_ma",
            "distance_from_ma",
            "ma_gap_pct",
            "price_ma_gap_pct",
        ),
        None,
    )
    if direct is not None:
        return _num(direct, 0.0)

    close = _first_existing(features, ("close", "last", "price", "entry", "entry_price"), None)
    ema = _first_existing(features, ("ema", "ema20", "ema_20", "ema10", "ema_10", "ema5", "ema_5", "ma5", "ma_5"), None)
    c = _num(close, 0.0)
    e = _num(ema, 0.0)
    if c > 0 and e > 0:
        return ((c - e) / e) * 100.0
    return None


def _change_pct(record: dict[str, Any], names: tuple[str, ...]) -> float:
    features = _features(record)
    return _num(_first_existing(features, names, 0.0), 0.0)


def _recent_symbol_change_pct(record: dict[str, Any]) -> float:
    """Best available recent move proxy for replay/live records."""
    return _change_pct(
        record,
        (
            "change_pct",
            "symbol_change_pct",
            "symbol_bounce_pct",
            "change_15m",
            "pct_change_15m",
            "change_pct_15m",
            "change_30m",
            "pct_change_30m",
            "change_pct_30m",
            "change_1h",
            "pct_change_1h",
            "change_pct_1h",
        ),
    )


def _ema_slope_close_bearish(record: dict[str, Any]) -> bool:
    """Balanced Strong filter.

    Preferred path: reject only when EMA/MA slope and close position are both bad.
    Fallback path: when slope is missing, use close-vs-MA (``dist_ma``) and the
    recent symbol move so the filter is not dead on the current replay/live data.
    """
    slope = _ema_slope_pct(record)
    gap = _close_ema_gap_pct(record)
    recent = _recent_symbol_change_pct(record)

    if slope is not None and gap is not None:
        return bool((slope <= -0.05 and gap <= -0.15) or (slope <= -0.12 and gap <= 0.05))

    # Current live snapshots provide dist_ma but not EMA slope. Treat clearly weak
    # close position as bearish, and treat borderline close position as bearish
    # only if the recent move is also negative.
    if gap is not None:
        return bool(gap <= -0.35 or (gap <= 0.10 and recent <= -2.0) or (gap <= 0.35 and recent <= -4.0))

    # Historical replay may only have change_pct. Keep this conservative to avoid
    # over-filtering Strong momentum setups.
    return bool(recent <= -5.0)


def _normal_overextended_before_entry(record: dict[str, Any]) -> bool:
    """Normal filter: reject late long entries after exaggerated move far above EMA/MA.

    Uses ``dist_ma`` when available and ``change_pct`` as the replay-compatible
    recent-move proxy.
    """
    gap = _close_ema_gap_pct(record)
    recent = _recent_symbol_change_pct(record)
    ch15 = _change_pct(record, ("change_15m", "pct_change_15m", "change_pct_15m"))
    ch30 = _change_pct(record, ("change_30m", "pct_change_30m", "change_pct_30m"))
    ch1h = _change_pct(record, ("change_1h", "pct_change_1h", "change_pct_1h"))
    extension = _num(
        _first_existing(
            _features(record),
            ("overextension_pct", "price_extension_pct", "extension_pct", "distance_from_vwap_pct"),
            0.0,
        ),
        0.0,
    )
    effective_gap = max(gap if gap is not None else 0.0, extension)

    # Far above MA: likely chase entry.
    if effective_gap >= 3.5:
        return True
    # Moderately above MA plus a strong recent move.
    if effective_gap >= 2.0 and max(recent, ch15, ch30, ch1h) >= 3.5:
        return True
    # Replay-compatible protection when MA distance is missing.
    if recent >= 5.0 or ch15 >= 5.0 or ch30 >= 7.5 or ch1h >= 10.0:
        return True
    return False


def _setup_type(record: dict[str, Any]) -> str:
    return str(_features(record).get("setup_type") or "")


def _is_relative_strength(record: dict[str, Any]) -> bool:
    tags = _setup_tags(record) | _pair_tags(record)
    setup = _setup_type(record)
    return bool("relative_strength_vs_btc" in tags or "rs_btc" in tags or setup == "relative_strength_vs_btc")


def _is_high_risk_symbol(record: dict[str, Any]) -> bool:
    return str(record.get("symbol") or "") in HIGH_RISK_SYMBOLS


def _is_block_exception(record: dict[str, Any]) -> bool:
    text = " ".join(str(x).lower() for x in [
        record.get("execution_path"),
        record.get("legacy_gate_reason"),
        record.get("block_reason"),
        _features(record).get("legacy_gate_reason"),
        _features(record).get("market_reason"),
    ])
    tags = _setup_tags(record) | _pair_tags(record)
    return bool("block_exception" in text or "block_exception" in tags)


def _record_time(record: dict[str, Any]) -> datetime | None:
    value = record.get("time") or record.get("timestamp") or record.get("created_at") or record.get("signal_time")
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _outcome(record: dict[str, Any]) -> dict[str, Any]:
    value = record.get("outcome")
    return value if isinstance(value, dict) else {}


def _tp2_lock_minutes(record: dict[str, Any]) -> int:
    """How long a same-symbol trade should block new entries in no-stacking simulation.

    If TP2 was hit, the lock ends at time_to_tp2_min. If TP2 was not hit, we
    conservatively lock until the replay horizon ends, because the user rule is:
    do not open another trade on the same symbol before TP2.
    """
    outcome = _outcome(record)
    if outcome.get("hit_tp2") and outcome.get("time_to_tp2_min") is not None:
        return max(15, int(_num(outcome.get("time_to_tp2_min"), 15)))
    bars = int(_num(outcome.get("horizon_bars"), 96) or 96)
    return max(15, bars * 15)


def _has_recovery_fail_risk(record: dict[str, Any]) -> bool:
    features = _features(record)
    text = " ".join(str(x).lower() for x in [
        features.get("market_reason"),
        features.get("legacy_gate_reason"),
        record.get("legacy_gate_reason"),
        record.get("block_reason"),
    ])
    return bool("recovery_hard_fail" in text or "recovery_soft_fail" in text or "fail" in text and "recovery" in text)


def _calibration(scores: list[float]) -> dict[str, float]:
    values = sorted(float(x) for x in scores if x is not None)
    if not values:
        return {"count": 0, "p20": 0.0, "p50": 0.0, "p85": 0.0, "p95": 0.0}

    def pick(p: float) -> float:
        if len(values) == 1:
            return values[0]
        idx = int(round((len(values) - 1) * p))
        idx = max(0, min(len(values) - 1, idx))
        return values[idx]

    return {"count": len(values), "p20": pick(0.20), "p50": median(values), "p85": pick(0.85), "p95": pick(0.95)}


def _score_zone(score: float, calibration: dict[str, float]) -> str:
    if not calibration or int(calibration.get("count") or 0) <= 5:
        return "unknown"
    if score <= float(calibration.get("p20") or 0):
        return "low"
    if score <= float(calibration.get("p85") or 0):
        return "healthy"
    if score <= float(calibration.get("p95") or 0):
        return "high"
    return "extreme"


def _component_setup(record: dict[str, Any]) -> float:
    tags = _setup_tags(record)
    setup = _setup_type(record)
    scores = [8.0]
    if "support_bounce_confirmed" in tags or setup == "support_bounce_confirmed":
        scores.append(35.0)
    if "vwap_reclaim" in tags or setup == "vwap_reclaim":
        scores.append(33.0)
    if "higher_low_continuation" in tags or setup == "higher_low_continuation":
        scores.append(31.0)
    if "retest_breakout_confirmed" in tags or setup == "retest_breakout_confirmed":
        scores.append(29.0)
    if "wave_3" in tags or setup == "wave_3":
        scores.append(25.0)
    if _is_relative_strength(record):
        scores.append(21.0)
    return min(35.0, max(scores))


def _component_market(record: dict[str, Any]) -> float:
    features = _features(record)
    points = 10.0  # neutral baseline when context is missing.
    btc_15m = _num(_first_existing(features, ("btc_change_15m", "btc_15m_change", "btc_15m"), None), 0.0)
    btc_1h = _num(_first_existing(features, ("btc_change_1h", "btc_1h_change", "btc_1h"), None), 0.0)
    red_ratio = _num(_first_existing(features, ("red_ratio", "market_red_ratio"), None), -1.0)
    strong_coins = _num(_first_existing(features, ("strong_coins", "strong_coins_count"), None), -1.0)

    if btc_15m >= 0.15:
        points += 2.0
    elif btc_15m <= -0.45:
        points -= 3.0
    if btc_1h >= 0.25:
        points += 2.0
    elif btc_1h <= -0.80:
        points -= 4.0
    if red_ratio >= 0:
        if red_ratio <= 50:
            points += 2.0
        elif red_ratio >= 65:
            points -= 3.0
    if strong_coins >= 0:
        if strong_coins >= 6:
            points += 2.0
        elif strong_coins <= 3:
            points -= 2.0
    if _has_recovery_fail_risk(record):
        points -= 5.0
    return max(0.0, min(20.0, points))


def _component_volume_momentum(record: dict[str, Any]) -> float:
    features = _features(record)
    vol = _vol_ratio(record)
    points = 5.0
    if vol >= 2.0:
        points += 10.0
    elif vol >= 1.5:
        points += 8.0
    elif vol >= 1.25:
        points += 5.0
    elif vol >= 1.10:
        points += 3.0
    if _is_relative_strength(record):
        points += 3.0
    if _setup_type(record) == "wave_3":
        points += 2.0
    change_15m = _num(_first_existing(features, ("change_15m", "pct_change_15m", "change_pct_15m"), None), 0.0)
    if 0.10 <= change_15m <= 2.8:
        points += 2.0
    elif change_15m > 4.5:
        points -= 2.0
    if bool(features.get("close_near_high") or features.get("impulse_candle")):
        points += 2.0
    return max(0.0, min(20.0, points))


def _component_risk(record: dict[str, Any]) -> float:
    points = 10.0
    near_res = _near_resistance(record)
    breakout_ok = _breakout_confirmed(record)
    if near_res and not breakout_ok:
        points -= 9.0
    elif near_res and breakout_ok:
        points -= 2.0
    else:
        points += 3.0
    setup = _setup_type(record)
    if setup in {"support_bounce_confirmed", "vwap_reclaim", "higher_low_continuation"}:
        points += 2.0
    if _is_high_risk_symbol(record):
        points -= 3.0
    features = _features(record)
    sl_pct = _num(_first_existing(features, ("sl_pct", "sl_distance_pct", "risk_pct"), None), 0.0)
    if sl_pct:
        if 1.2 <= sl_pct <= 2.8:
            points += 1.0
        elif sl_pct > 4.0:
            points -= 2.0
    return max(0.0, min(15.0, points))


def _component_bot_score(record: dict[str, Any], calibration: dict[str, float]) -> tuple[float, str]:
    score = _score(record)
    zone = _score_zone(score, calibration)
    if zone == "healthy":
        return 10.0, zone
    if zone == "high":
        return 7.0, zone
    if zone == "low":
        return 4.0, zone
    if zone == "extreme":
        return 4.0, zone
    return 6.0, zone


def calculate_gate_quality(record: dict[str, Any], calibration: dict[str, float]) -> dict[str, Any]:
    """Single shared 0-100 score used by all modes and all recipes.

    Recipes are only allowed to change acceptance thresholds and technical
    conditions; they do not use a different score formula.
    """
    setup = _component_setup(record)
    market = _component_market(record)
    volume = _component_volume_momentum(record)
    risk = _component_risk(record)
    bot_score, zone = _component_bot_score(record, calibration)
    total = setup + market + volume + risk + bot_score
    features = _features(record)
    return {
        "gate_quality_score": round(max(0.0, min(100.0, total)), 4),
        "components": {
            "setup_quality": round(setup, 4),
            "market_context": round(market, 4),
            "volume_momentum": round(volume, 4),
            "risk_location": round(risk, 4),
            "bot_score_zone": round(bot_score, 4),
        },
        "weights": QUALITY_SCORE_WEIGHTS,
        "score_raw": round(_score(record), 4),
        "score_zone": zone,
        "indicators": {
            "setup_type": _setup_type(record),
            "setup_tags": sorted(_setup_tags(record)),
            "near_resistance": _near_resistance(record),
            "breakout_confirmed": _breakout_confirmed(record),
            "mtf_confirmed": _mtf_confirmed(record),
            "relative_strength_vs_btc": _is_relative_strength(record),
            "vol_ratio": _vol_ratio(record),
            "ema_slope_pct": _ema_slope_pct(record),
            "close_ema_gap_pct": _close_ema_gap_pct(record),
            "ema_slope_close_bearish": _ema_slope_close_bearish(record),
            "normal_overextended_before_entry": _normal_overextended_before_entry(record),
            "high_risk_symbol": _is_high_risk_symbol(record),
            "btc_change_15m": _first_existing(features, ("btc_change_15m", "btc_15m_change", "btc_15m"), None),
            "btc_change_1h": _first_existing(features, ("btc_change_1h", "btc_1h_change", "btc_1h"), None),
        },
    }


def evaluate_recipe(record: dict[str, Any], recipe: GateRecipe, calibration: dict[str, float]) -> tuple[bool, str, dict[str, Any]]:
    quality = calculate_gate_quality(record, calibration)
    q = float(quality.get("gate_quality_score") or 0.0)
    mode = MODES.get(recipe.mode_key, recipe.mode_key)
    if str(record.get("mode") or "") != mode:
        return False, "wrong_mode", quality

    setup = _setup_type(record)
    tags = _setup_tags(record)
    allowed = set(recipe.allowed_setups or ())
    near_res = _near_resistance(record)
    breakout = _breakout_confirmed(record)
    mtf = _mtf_confirmed(record)
    vol = _vol_ratio(record)
    rel = _is_relative_strength(record)

    if q < recipe.min_quality_score:
        return False, "quality_score_below_recipe_threshold", quality
    if recipe.reject_near_resistance_without_breakout and near_res and not breakout:
        return False, "near_resistance_without_breakout", quality
    if recipe.reject_high_risk_without_edge and _is_high_risk_symbol(record) and not (mtf or breakout or q >= 78):
        return False, "high_risk_symbol_without_edge", quality
    if recipe.reject_extreme_score_without_confirmation and quality.get("score_zone") == "extreme" and (near_res or not (mtf or breakout)):
        return False, "extreme_score_without_confirmation", quality
    if recipe.min_vol_ratio > 0 and vol < recipe.min_vol_ratio:
        return False, "volume_below_recipe_min", quality
    if recipe.require_ema_slope_close_filter and _ema_slope_close_bearish(record):
        return False, "ema_slope_close_bearish", quality
    if recipe.reject_normal_overextension and _normal_overextended_before_entry(record):
        return False, "normal_overextended_before_entry", quality
    if recipe.require_recovery_bounce and setup not in RECOVERY_CORE_SETUPS:
        if not (bool(_features(record).get("recovery_relative_bounce")) and vol >= recipe.relative_strength_min_vol):
            return False, "missing_recovery_bounce_or_reclaim", quality
    if recipe.require_strong_confirmation and not (rel or mtf or breakout or vol >= recipe.relative_strength_min_vol):
        return False, "missing_strong_confirmation", quality
    if recipe.require_rs_structure_hybrid and rel:
        structure_setups = {"higher_low_continuation", "support_bounce_confirmed", "vwap_reclaim", "retest_breakout_confirmed"}
        has_structure = bool((tags | {setup}) & structure_setups) or breakout or mtf
        if not has_structure:
            return False, "relative_strength_without_structure", quality

    if allowed and setup not in allowed and not (rel and recipe.allow_relative_strength):
        return False, "setup_not_allowed_for_recipe", quality
    if recipe.clean_setups_only and setup not in allowed:
        return False, "clean_setup_required", quality
    if setup == "wave_3" and not (rel or mtf or breakout or vol >= recipe.wave3_min_vol):
        return False, "wave3_without_confirmation", quality
    if rel and setup not in allowed and recipe.allow_relative_strength and vol < recipe.relative_strength_min_vol:
        return False, "relative_strength_without_volume", quality
    if rel and not recipe.allow_relative_strength and setup not in allowed:
        return False, "relative_strength_not_allowed", quality

    return True, "recipe_pass", quality


def _new_stats() -> dict[str, Any]:
    return {
        "total": 0,
        "quality": 0,
        "execution": 0,
        "blocked": 0,
        "outcomes": 0,
        "first_tp1": 0,
        "first_sl": 0,
        "tp1": 0,
        "tp2": 0,
        "sl": 0,
        "max_gain_sum": 0.0,
        "drawdown_sum": 0.0,
        "weighted_sum": 0.0,
        "weighted_count": 0,
        "quality_score_sum": 0.0,
        "quality_score_count": 0,
        "runner_active": 0,
        "runner_closed": 0,
        "runner_positive": 0,
        "runner_max_gain_sum": 0.0,
        "runner_exit_sum": 0.0,
        "runner_contribution_sum": 0.0,
        "weighted_without_runner_sum": 0.0,
        "weighted_without_runner_count": 0,
    }


def _is_quality_candidate(record: dict[str, Any]) -> bool:
    return bool(record.get("quality_candidate") or record.get("execution_candidate") or record.get("blocked_by_limit"))


def _update_stats(stats: dict[str, Any], record: dict[str, Any], quality_score: float | None = None) -> None:
    stats["total"] += 1
    if _is_quality_candidate(record):
        stats["quality"] += 1
    if record.get("execution_candidate"):
        stats["execution"] += 1
    if record.get("blocked_by_limit"):
        stats["blocked"] += 1
    if quality_score is not None:
        stats["quality_score_sum"] += float(quality_score)
        stats["quality_score_count"] += 1
    outcome = record.get("outcome")
    if isinstance(outcome, dict) and outcome:
        stats["outcomes"] += 1
        if outcome.get("hit_tp1"):
            stats["tp1"] += 1
        if outcome.get("hit_tp2"):
            stats["tp2"] += 1
        if outcome.get("hit_sl"):
            stats["sl"] += 1
        if outcome.get("first_event") == "tp1":
            stats["first_tp1"] += 1
        if outcome.get("first_event") == "sl":
            stats["first_sl"] += 1
        stats["max_gain_sum"] += _num(outcome.get("max_gain_24h"))
        stats["drawdown_sum"] += _num(outcome.get("max_drawdown_24h"))
        weighted = outcome.get("weighted_trade_result_pct")
        if weighted is None:
            weighted = outcome.get("weighted_result_pct")
        if weighted is not None:
            weighted_value = _num(weighted)
            stats["weighted_sum"] += weighted_value
            stats["weighted_count"] += 1
            runner_pct = _num(outcome.get("runner_pct"), 0.0)
            runner_exit_pct = _num(outcome.get("runner_exit_pct"), 0.0)
            runner_contribution = runner_exit_pct * (runner_pct / 100.0) if outcome.get("runner_active") else 0.0
            stats["runner_contribution_sum"] += runner_contribution
            stats["weighted_without_runner_sum"] += weighted_value - runner_contribution
            stats["weighted_without_runner_count"] += 1
        if outcome.get("runner_active"):
            stats["runner_active"] += 1
        if outcome.get("runner_closed"):
            stats["runner_closed"] += 1
        runner_max_gain = _num(outcome.get("runner_max_gain_pct"), 0.0)
        runner_exit_pct = _num(outcome.get("runner_exit_pct"), 0.0)
        if outcome.get("runner_active") and (runner_max_gain > 0 or runner_exit_pct > 0):
            stats["runner_positive"] += 1
        stats["runner_max_gain_sum"] += runner_max_gain
        stats["runner_exit_sum"] += runner_exit_pct


def _plain_stats(stats: dict[str, Any]) -> dict[str, Any]:
    out = {
        "total": int(stats.get("total") or 0),
        "quality": int(stats.get("quality") or 0),
        "execution": int(stats.get("execution") or 0),
        "blocked": int(stats.get("blocked") or 0),
        "outcomes": int(stats.get("outcomes") or 0),
        "first_tp1": int(stats.get("first_tp1") or 0),
        "first_sl": int(stats.get("first_sl") or 0),
        "tp1": int(stats.get("tp1") or 0),
        "tp2": int(stats.get("tp2") or 0),
        "sl": int(stats.get("sl") or 0),
        "max_gain_sum": float(stats.get("max_gain_sum") or 0.0),
        "drawdown_sum": float(stats.get("drawdown_sum") or 0.0),
        "weighted_sum": float(stats.get("weighted_sum") or 0.0),
        "weighted_count": int(stats.get("weighted_count") or 0),
        "quality_score_sum": float(stats.get("quality_score_sum") or 0.0),
        "quality_score_count": int(stats.get("quality_score_count") or 0),
        "runner_active": int(stats.get("runner_active") or 0),
        "runner_closed": int(stats.get("runner_closed") or 0),
        "runner_positive": int(stats.get("runner_positive") or 0),
        "runner_max_gain_sum": float(stats.get("runner_max_gain_sum") or 0.0),
        "runner_exit_sum": float(stats.get("runner_exit_sum") or 0.0),
        "runner_contribution_sum": float(stats.get("runner_contribution_sum") or 0.0),
        "weighted_without_runner_sum": float(stats.get("weighted_without_runner_sum") or 0.0),
        "weighted_without_runner_count": int(stats.get("weighted_without_runner_count") or 0),
    }
    outcomes = max(1, int(out["outcomes"] or 0))
    out["first_tp1_rate_pct"] = round((out["first_tp1"] / outcomes) * 100.0, 4) if out["outcomes"] else 0.0
    out["first_sl_rate_pct"] = round((out["first_sl"] / outcomes) * 100.0, 4) if out["outcomes"] else 0.0
    out["avg_weighted_result_pct"] = round(out["weighted_sum"] / max(1, out["weighted_count"]), 4) if out["weighted_count"] else 0.0
    out["avg_weighted_without_runner_pct"] = round(out["weighted_without_runner_sum"] / max(1, out["weighted_without_runner_count"]), 4) if out["weighted_without_runner_count"] else 0.0
    out["avg_runner_contribution_pct"] = round(out["runner_contribution_sum"] / max(1, out["weighted_count"]), 4) if out["weighted_count"] else 0.0
    out["avg_runner_max_gain_pct"] = round(out["runner_max_gain_sum"] / max(1, out["runner_active"]), 4) if out["runner_active"] else 0.0
    out["avg_runner_exit_pct"] = round(out["runner_exit_sum"] / max(1, out["runner_active"]), 4) if out["runner_active"] else 0.0
    out["runner_positive_rate_pct"] = round((out["runner_positive"] / max(1, out["runner_active"])) * 100.0, 4) if out["runner_active"] else 0.0
    out["avg_gate_quality_score"] = round(out["quality_score_sum"] / max(1, out["quality_score_count"]), 4) if out["quality_score_count"] else 0.0
    return out


def _format_stats(label: str, stats: dict[str, Any]) -> list[str]:
    total = int(stats.get("total") or 0)
    outcomes = int(stats.get("outcomes") or 0)
    avg_gain = (float(stats.get("max_gain_sum") or 0.0) / outcomes) if outcomes else 0.0
    avg_dd = (float(stats.get("drawdown_sum") or 0.0) / outcomes) if outcomes else 0.0
    avg_q = (float(stats.get("quality_score_sum") or 0.0) / max(1, int(stats.get("quality_score_count") or 0))) if stats.get("quality_score_count") else 0.0
    rows = [
        f"{label}: {total}",
        f"- quality/exe/blocked: {stats.get('quality', 0)} / {stats.get('execution', 0)} / {stats.get('blocked', 0)}",
    ]
    if avg_q:
        rows.append(f"- avg gate quality score: {avg_q:.1f}/100")
    if outcomes:
        rows.extend([
            f"- first TP1/SL: {_pct(stats.get('first_tp1', 0), outcomes)} / {_pct(stats.get('first_sl', 0), outcomes)}",
            f"- TP1/TP2/SL touch: {_pct(stats.get('tp1', 0), outcomes)} / {_pct(stats.get('tp2', 0), outcomes)} / {_pct(stats.get('sl', 0), outcomes)}",
            f"- avg max/DD: {avg_gain:.2f}% / {avg_dd:.2f}%",
        ])
        if stats.get("weighted_count"):
            avg_weighted = float(stats.get("weighted_sum") or 0.0) / max(1, int(stats.get("weighted_count") or 0))
            rows.append(f"- avg weighted result: {avg_weighted:.2f}%")
        if stats.get("runner_active"):
            rows.append(
                f"- runner active/closed/positive: {stats.get('runner_active',0)} / {stats.get('runner_closed',0)} / {_pct(stats.get('runner_positive',0), stats.get('runner_active',0))}"
            )
            rows.append(
                f"- avg runner max/exit/contribution: {stats.get('avg_runner_max_gain_pct',0):.2f}% / {stats.get('avg_runner_exit_pct',0):.2f}% / {stats.get('avg_runner_contribution_pct',0):.3f}%"
            )
    return rows


def _build_calibration(records: list[dict[str, Any]], mode: str) -> dict[str, float]:
    scores = [_score(r) for r in records if str(r.get("mode") or "") == mode]
    return _calibration(scores)


def _setup_counters(records: list[dict[str, Any]], mode: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    for record in records:
        if str(record.get("mode") or "") == mode:
            counter[_setup_type(record) or "unknown"] += 1
    return counter


def _analyze_recipe(records: list[dict[str, Any]], recipe: GateRecipe, calibration: dict[str, float]) -> dict[str, Any]:
    mode = MODES[recipe.mode_key]
    before = _new_stats()
    after = _new_stats()
    rejected = _new_stats()
    reasons: Counter[str] = Counter()
    pass_setup_counter: Counter[str] = Counter()
    all_setup_counter: Counter[str] = Counter()
    score_zones_before: Counter[str] = Counter()
    score_zones_after: Counter[str] = Counter()
    passed_records: list[tuple[dict[str, Any], float]] = []

    for record in records:
        if str(record.get("mode") or "") != mode:
            continue
        quality = calculate_gate_quality(record, calibration)
        q = float(quality.get("gate_quality_score") or 0.0)
        zone = str(quality.get("score_zone") or "unknown")
        _update_stats(before, record, q)
        all_setup_counter[_setup_type(record) or "unknown"] += 1
        score_zones_before[zone] += 1
        passed, reason, quality = evaluate_recipe(record, recipe, calibration)
        if passed:
            q_pass = float(quality.get("gate_quality_score") or 0.0)
            _update_stats(after, record, q_pass)
            passed_records.append((record, q_pass))
            pass_setup_counter[_setup_type(record) or "unknown"] += 1
            score_zones_after[str(quality.get("score_zone") or "unknown")] += 1
        else:
            _update_stats(rejected, record, float(quality.get("gate_quality_score") or 0.0))
            reasons[reason] += 1

    return {
        "recipe": asdict(recipe),
        "rules": recipe.rules(),
        "before": before,
        "after": after,
        "rejected": rejected,
        "reasons": reasons,
        "setups": all_setup_counter,
        "pass_setups": pass_setup_counter,
        "score_zones_before": score_zones_before,
        "score_zones_after": score_zones_after,
        "no_stacking_before_tp2": _simulate_no_stacking_before_tp2(passed_records),
    }


def _simulate_no_stacking_before_tp2(passed_records: list[tuple[dict[str, Any], float]]) -> dict[str, Any]:
    """Simulate the user rule: do not open another trade on same symbol before TP2.

    This is analysis-only. It estimates realistic trade count after same-symbol
    de-duplication. It does not emulate global max positions/exposure.
    """
    accepted = _new_stats()
    blocked = _new_stats()
    lock_until: dict[str, datetime] = {}
    accepted_count = 0
    blocked_count = 0
    for record, quality_score in sorted(passed_records, key=lambda item: (_record_time(item[0]) or datetime.min.replace(tzinfo=timezone.utc), str(item[0].get("symbol") or ""))):
        symbol = str(record.get("symbol") or "")
        t = _record_time(record)
        if not symbol or t is None or not isinstance(record.get("outcome"), dict):
            # Live snapshots or records without outcomes cannot be lifecycle de-stacked safely.
            continue
        until = lock_until.get(symbol)
        if until and t < until:
            _update_stats(blocked, record, quality_score)
            blocked_count += 1
            continue
        _update_stats(accepted, record, quality_score)
        accepted_count += 1
        lock_until[symbol] = t + timedelta(minutes=_tp2_lock_minutes(record))
    raw = len([1 for r, _ in passed_records if isinstance(r.get("outcome"), dict) and _record_time(r) is not None])
    return {
        "rule": "no_new_trade_same_symbol_before_tp2",
        "raw_passed_with_outcomes": raw,
        "accepted_trades": _plain_stats(accepted),
        "blocked_existing_trade_before_tp2": _plain_stats(blocked),
        "accepted_count": accepted_count,
        "blocked_count": blocked_count,
        "reduction_pct": round((blocked_count / max(1, raw)) * 100.0, 4) if raw else 0.0,
    }


def _analysis_payload(analysis: dict[str, Any]) -> dict[str, Any]:
    before = _plain_stats(analysis.get("before") or {})
    after = _plain_stats(analysis.get("after") or {})
    rejected = _plain_stats(analysis.get("rejected") or {})
    total = before.get("total", 0)
    passed = after.get("total", 0)
    return {
        "recipe": analysis.get("recipe") or {},
        "rules": analysis.get("rules") or [],
        "before": before,
        "after_gate": after,
        "rejected_to_normal": rejected,
        "reduction_pct": round(((max(total - passed, 0) / total) * 100.0), 4) if total else 0.0,
        "reject_reasons": dict((analysis.get("reasons") or Counter()).most_common()),
        "all_setups": dict((analysis.get("setups") or Counter()).most_common()),
        "pass_setups": dict((analysis.get("pass_setups") or Counter()).most_common()),
        "score_zones_before": dict((analysis.get("score_zones_before") or Counter()).most_common()),
        "score_zones_after": dict((analysis.get("score_zones_after") or Counter()).most_common()),
        "no_stacking_before_tp2": analysis.get("no_stacking_before_tp2") or {},
    }


def _recipe_score_for_recommendation(replay_payload: dict[str, Any], live_payload: dict[str, Any]) -> float:
    """Rank recipes by outcome quality + enough opportunity + live pressure reduction."""
    after = replay_payload.get("after_gate") or {}
    before = replay_payload.get("before") or {}
    live_after = live_payload.get("after_gate") or {}
    live_before = live_payload.get("before") or {}
    outcomes = float(after.get("outcomes") or 0)
    if outcomes <= 0:
        return 0.0
    no_stack = replay_payload.get("no_stacking_before_tp2") or {}
    accepted = no_stack.get("accepted_trades") or {}
    first_tp1 = float((accepted.get("first_tp1_rate_pct") if accepted.get("outcomes") else after.get("first_tp1_rate_pct")) or 0.0)
    avg_weighted = float((accepted.get("avg_weighted_result_pct") if accepted.get("weighted_count") else after.get("avg_weighted_result_pct")) or 0.0)
    kept_ratio = float(after.get("total") or 0) / max(1.0, float(before.get("total") or 0))
    live_reduction = 100.0 - ((float(live_after.get("total") or 0) / max(1.0, float(live_before.get("total") or 0))) * 100.0)
    # Avoid selecting a recipe that keeps almost nothing even if the rate is high.
    opportunity_bonus = min(20.0, kept_ratio * 35.0)
    return (first_tp1 * 0.55) + (avg_weighted * 4.0) + opportunity_bonus + (live_reduction * 0.10)


def _write_gate_json(payload: dict[str, Any], name: str) -> dict[str, Any]:
    GATE_SIM_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in name)
    path = GATE_SIM_EXPORT_DIR / f"{safe_name}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {"ok": True, "path": str(path), "size_bytes": path.stat().st_size if path.exists() else 0}


def _gate_payload_base(kind: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "analytics.gate_simulation",
        "note": "Passive multi-recipe gate simulation only. Does not change live scoring, execution, TP/SL, or market modes.",
        "shared_gate_quality_score": {
            "scale": "0-100",
            "formula": "single shared formula for all modes and recipes",
            "weights": QUALITY_SCORE_WEIGHTS,
            "important": "Recipes change acceptance thresholds and technical conditions only; they do not use different score formulas.",
        },
    }


def _load_inputs(settings: Any | None, redis_client: Any | None, live_limit: int = 50000) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    replay_records = list(iter_replay_records(redis_client=redis_client))
    live_records = load_snapshot_records(settings=settings, limit=live_limit, redis_client=redis_client)
    return replay_records, live_records


def build_gate_sim_payload(gate: str, settings: Any | None = None, redis_client: Any | None = None, live_limit: int = 50000) -> dict[str, Any]:
    gate = str(gate or "").lower().strip()
    if gate not in MODES:
        return {**_gate_payload_base("single_gate_multi_recipe"), "ok": False, "error": "unknown_gate", "gate": gate}

    replay_records, live_records = _load_inputs(settings, redis_client, live_limit=live_limit)
    mode = MODES[gate]
    replay_cal = _build_calibration(replay_records, mode)
    live_cal = _build_calibration(live_records, mode)
    recipes_payload: list[dict[str, Any]] = []

    best_name = ""
    best_score = -10**9
    for recipe in RECIPES[gate]:
        replay_analysis = _analysis_payload(_analyze_recipe(replay_records, recipe, replay_cal))
        live_analysis = _analysis_payload(_analyze_recipe(live_records, recipe, live_cal))
        rank_score = _recipe_score_for_recommendation(replay_analysis, live_analysis)
        if rank_score > best_score:
            best_score = rank_score
            best_name = recipe.name
        recipes_payload.append({
            "name": recipe.name,
            "title": recipe.title,
            "description": recipe.description,
            "recommendation_score": round(rank_score, 4),
            "rules": recipe.rules(),
            "recipe_config": asdict(recipe),
            "replay": replay_analysis,
            "live_snapshot": live_analysis,
        })

    return {
        **_gate_payload_base("single_gate_multi_recipe"),
        "ok": True,
        "gate": gate,
        "mode": mode,
        "score_calibration": {
            "replay": replay_cal,
            "live_snapshot": live_cal,
        },
        "recipes": recipes_payload,
        "recommended_recipe": best_name,
    }


def _format_recipe_short(recipe_payload: dict[str, Any]) -> list[str]:
    replay = recipe_payload.get("replay") or {}
    live = recipe_payload.get("live_snapshot") or {}
    rb = replay.get("before") or {}
    ra = replay.get("after_gate") or {}
    lb = live.get("before") or {}
    la = live.get("after_gate") or {}
    rows = [
        f"🧪 {recipe_payload.get('name')} — {recipe_payload.get('title')}",
        f"- قبول: gate_quality_score >= {((recipe_payload.get('recipe_config') or {}).get('min_quality_score') or 0):.1f}",
        f"- Replay pass: {ra.get('total', 0)}/{rb.get('total', 0)} | reduction {replay.get('reduction_pct', 0):.1f}%",
        f"- Replay first TP1/SL: {_pct(ra.get('first_tp1', 0), ra.get('outcomes', 0))} / {_pct(ra.get('first_sl', 0), ra.get('outcomes', 0))}",
    ]
    if ra.get("weighted_count"):
        rows.append(f"- Replay avg weighted: {ra.get('avg_weighted_result_pct', 0):.2f}%")
        if ra.get("runner_active"):
            rows.append(
                f"- Runner active/positive/contrib: {ra.get('runner_active',0)} / {ra.get('runner_positive_rate_pct',0):.1f}% / {ra.get('avg_runner_contribution_pct',0):.3f}%"
            )
    no_stack = replay.get("no_stacking_before_tp2") or {}
    if no_stack.get("raw_passed_with_outcomes"):
        accepted = no_stack.get("accepted_trades") or {}
        rows.append(
            f"- No-stack before TP2: {no_stack.get('accepted_count',0)}/{no_stack.get('raw_passed_with_outcomes',0)} trades | "
            f"TP1/SL {_pct(accepted.get('first_tp1',0), accepted.get('outcomes',0))}/{_pct(accepted.get('first_sl',0), accepted.get('outcomes',0))} | "
            f"avg {accepted.get('avg_weighted_result_pct',0):.2f}%"
        )
    rows.extend([
        f"- Live pass: {la.get('total', 0)}/{lb.get('total', 0)} | pressure reduction {live.get('reduction_pct', 0):.1f}%",
    ])
    reasons = (replay.get("reject_reasons") or {})
    if reasons:
        rows.append("- Top replay rejects: " + ", ".join([f"{k}:{v}" for k, v in list(reasons.items())[:3]]))
    setups = (replay.get("pass_setups") or {})
    if setups:
        rows.append("- Top pass setups: " + ", ".join([f"{k}:{v}" for k, v in list(setups.items())[:3]]))
    return rows


def _format_report_from_payload(payload: dict[str, Any]) -> str:
    if not payload.get("ok"):
        return "⚠️ Gate simulation غير معروف. استخدم normal / recovery / strong."
    rows = [
        f"🧪 Gate Simulation — {payload.get('mode')} — Multi Recipes",
        "┄┄┄┄┄┄┄┄",
        "تحليل فقط: لا يغير execution أو scoring أو TP/SL.",
        "طريقة حساب Gate Quality Score واحدة لكل المودات والخلطات؛ الاختلاف فقط في رقم القبول وشروط المؤشرات.",
        "📎 تم تجهيز ملف JSON بنفس نتائج التقرير.",
        "",
        "⚙️ Gate Quality Score",
        "- Setup Quality: 35%",
        "- Market Context: 20%",
        "- Volume / Momentum: 20%",
        "- Risk Location: 15%",
        "- Bot Score Zone: 10%",
    ]
    cal = payload.get("score_calibration") or {}
    replay_cal = cal.get("replay") or {}
    live_cal = cal.get("live_snapshot") or {}
    rows.extend([
        "",
        "📏 Score Calibration",
        f"- Replay p20/p50/p85/p95: {replay_cal.get('p20', 0):.2f} / {replay_cal.get('p50', 0):.2f} / {replay_cal.get('p85', 0):.2f} / {replay_cal.get('p95', 0):.2f}",
        f"- Live p20/p50/p85/p95: {live_cal.get('p20', 0):.2f} / {live_cal.get('p50', 0):.2f} / {live_cal.get('p85', 0):.2f} / {live_cal.get('p95', 0):.2f}",
        "",
    ])
    for recipe_payload in payload.get("recipes") or []:
        rows.extend(_format_recipe_short(recipe_payload))
        rows.append("")
    if payload.get("recommended_recipe"):
        rows.append(f"✅ Recommended recipe مبدئيًا: {payload.get('recommended_recipe')}")
    rows.append("📌 القرار النهائي بعد مقارنة JSON والتأكد من replay الجديد 45d.")
    return "\n".join(rows)


def build_gate_sim_report(gate: str, settings: Any | None = None, redis_client: Any | None = None, live_limit: int = 50000) -> str:
    return _format_report_from_payload(build_gate_sim_payload(gate, settings=settings, redis_client=redis_client, live_limit=live_limit))


def build_gate_sim_artifact(gate: str, settings: Any | None = None, redis_client: Any | None = None, live_limit: int = 50000) -> dict[str, Any]:
    payload = build_gate_sim_payload(gate, settings=settings, redis_client=redis_client, live_limit=live_limit)
    text = _format_report_from_payload(payload)
    if not payload.get("ok"):
        return {"ok": False, "text": text, "message": payload.get("error") or "unknown_gate"}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_result = _write_gate_json(payload, f"gate_sim_{payload.get('gate')}_multi_recipe_{stamp}")
    return {
        "ok": True,
        "text": text,
        "path": file_result.get("path"),
        "size_bytes": file_result.get("size_bytes"),
        "caption": f"Gate Simulation JSON — {payload.get('mode')} — Multi Recipes",
        "payload": payload,
    }


def build_gate_sim_all_payload(settings: Any | None = None, redis_client: Any | None = None) -> dict[str, Any]:
    replay_records, live_records = _load_inputs(settings, redis_client, live_limit=50000)
    modes_payload: dict[str, Any] = {}
    for gate, mode in MODES.items():
        replay_cal = _build_calibration(replay_records, mode)
        live_cal = _build_calibration(live_records, mode)
        recipes_payload: list[dict[str, Any]] = []
        best_name = ""
        best_score = -10**9
        for recipe in RECIPES[gate]:
            replay_analysis = _analysis_payload(_analyze_recipe(replay_records, recipe, replay_cal))
            live_analysis = _analysis_payload(_analyze_recipe(live_records, recipe, live_cal))
            rank_score = _recipe_score_for_recommendation(replay_analysis, live_analysis)
            if rank_score > best_score:
                best_score = rank_score
                best_name = recipe.name
            recipes_payload.append({
                "name": recipe.name,
                "title": recipe.title,
                "description": recipe.description,
                "recommendation_score": round(rank_score, 4),
                "rules": recipe.rules(),
                "recipe_config": asdict(recipe),
                "replay": replay_analysis,
                "live_snapshot": live_analysis,
            })
        modes_payload[gate] = {
            "mode": mode,
            "score_calibration": {"replay": replay_cal, "live_snapshot": live_cal},
            "recipes": recipes_payload,
            "recommended_recipe": best_name,
        }
    return {
        **_gate_payload_base("all_gates_multi_recipe"),
        "ok": True,
        "modes": modes_payload,
    }


def _format_all_report_from_payload(payload: dict[str, Any]) -> str:
    rows = [
        "🧪 Gate Simulation — All Modes — Multi Recipes",
        "┄┄┄┄┄┄┄┄",
        "تحليل مختصر فقط؛ استخدم أوامر كل مود للتفاصيل.",
        "كل المودات تستخدم نفس Gate Quality Score، والاختلاف في recipe thresholds والمؤشرات.",
        "📎 تم تجهيز ملف JSON شامل لكل البوابات والخلطات.",
        "",
    ]
    for gate, data in (payload.get("modes") or {}).items():
        mode = data.get("mode") or MODES.get(gate, gate)
        rows.append(f"{mode}")
        for recipe_payload in data.get("recipes") or []:
            replay = recipe_payload.get("replay") or {}
            live = recipe_payload.get("live_snapshot") or {}
            ra = replay.get("after_gate") or {}
            rb = replay.get("before") or {}
            la = live.get("after_gate") or {}
            lb = live.get("before") or {}
            rows.append(
                f"- {recipe_payload.get('name')}: Replay {ra.get('total',0)}/{rb.get('total',0)} "
                f"TP1/SL {_pct(ra.get('first_tp1',0), ra.get('outcomes',0))}/{_pct(ra.get('first_sl',0), ra.get('outcomes',0))} | "
                f"Live {la.get('total',0)}/{lb.get('total',0)}"
            )
        if data.get("recommended_recipe"):
            rows.append(f"  ✅ recommended: {data.get('recommended_recipe')}")
        rows.append("")
    rows.append("📌 لا يتم تطبيق أي Gate على البوت الحقيقي من هذه الأوامر.")
    return "\n".join(rows)


def build_gate_sim_all_report(settings: Any | None = None, redis_client: Any | None = None) -> str:
    return _format_all_report_from_payload(build_gate_sim_all_payload(settings=settings, redis_client=redis_client))


def build_gate_sim_all_artifact(settings: Any | None = None, redis_client: Any | None = None) -> dict[str, Any]:
    payload = build_gate_sim_all_payload(settings=settings, redis_client=redis_client)
    text = _format_all_report_from_payload(payload)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_result = _write_gate_json(payload, f"gate_sim_all_multi_recipe_{stamp}")
    return {
        "ok": True,
        "text": text,
        "path": file_result.get("path"),
        "size_bytes": file_result.get("size_bytes"),
        "caption": "Gate Simulation JSON — All Modes — Multi Recipes",
        "payload": payload,
    }


def _mode_count_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(records)
    counts: Counter[str] = Counter(str(r.get("mode") or "unknown") for r in records)
    return {
        "total": total,
        "by_mode": {mode: {"count": count, "pct": round((count / max(1, total)) * 100.0, 4)} for mode, count in counts.most_common()},
    }


def _mode_transition_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    buckets: dict[datetime, Counter[str]] = {}
    for record in records:
        t = _record_time(record)
        mode = str(record.get("mode") or "unknown")
        if t is None:
            continue
        buckets.setdefault(t, Counter())[mode] += 1
    series: list[tuple[datetime, str]] = []
    for t in sorted(buckets):
        mode, _count = buckets[t].most_common(1)[0]
        series.append((t, mode))
    transitions: Counter[str] = Counter()
    previous: str | None = None
    changes = 0
    for _t, mode in series:
        if previous is not None and mode != previous:
            transitions[f"{previous} -> {mode}"] += 1
            changes += 1
        previous = mode
    return {
        "scan_points": len(series),
        "mode_changes": changes,
        "transitions": dict(transitions.most_common()),
    }


def _mode_reason_payload(records: list[dict[str, Any]], limit: int = 10) -> dict[str, Any]:
    by_mode: dict[str, Counter[str]] = {}
    for record in records:
        mode = str(record.get("mode") or "unknown")
        features = _features(record)
        reason = (
            record.get("mode_reason")
            or record.get("market_reason")
            or record.get("trigger")
            or features.get("mode_reason")
            or features.get("market_reason")
            or features.get("trigger")
            or "unknown"
        )
        by_mode.setdefault(mode, Counter())[str(reason)] += 1
    return {mode: dict(counter.most_common(limit)) for mode, counter in by_mode.items()}


def build_mode_coverage_payload(settings: Any | None = None, redis_client: Any | None = None, live_limit: int = 50000) -> dict[str, Any]:
    replay_records, live_records = _load_inputs(settings, redis_client, live_limit=live_limit)
    payload = {
        **_gate_payload_base("mode_coverage_diagnostic"),
        "ok": True,
        "important": "Diagnostic only. Does not change market-mode thresholds or execution.",
        "replay": {
            "distribution": _mode_count_payload(replay_records),
            "transitions": _mode_transition_payload(replay_records),
            "top_reasons_by_mode": _mode_reason_payload(replay_records),
        },
        "live_snapshot": {
            "distribution": _mode_count_payload(live_records),
            "transitions": _mode_transition_payload(live_records),
            "top_reasons_by_mode": _mode_reason_payload(live_records),
        },
    }
    replay_modes = (payload["replay"]["distribution"].get("by_mode") or {})
    missing = [mode for mode in MODES.values() if mode not in replay_modes or int((replay_modes.get(mode) or {}).get("count") or 0) <= 0]
    payload["replay"]["missing_modes"] = missing
    return payload


def build_mode_coverage_report(settings: Any | None = None, redis_client: Any | None = None, live_limit: int = 50000) -> str:
    payload = build_mode_coverage_payload(settings=settings, redis_client=redis_client, live_limit=live_limit)

    def render_source(title: str, data: dict[str, Any]) -> list[str]:
        dist = data.get("distribution") or {}
        rows = [title, f"- Total records: {dist.get('total', 0)}"]
        for mode, info in (dist.get("by_mode") or {}).items():
            rows.append(f"- {mode}: {info.get('count', 0)} ({info.get('pct', 0):.2f}%)")
        transitions = data.get("transitions") or {}
        rows.append(f"- Scan points: {transitions.get('scan_points', 0)} | Mode changes: {transitions.get('mode_changes', 0)}")
        top_transitions = transitions.get("transitions") or {}
        if top_transitions:
            rows.append("- Top transitions: " + ", ".join([f"{k}: {v}" for k, v in list(top_transitions.items())[:5]]))
        missing = data.get("missing_modes") or []
        if missing:
            rows.append("⚠️ Missing modes in replay: " + ", ".join(missing))
        return rows

    rows = [
        "🧭 Mode Coverage Diagnostic",
        "┄┄┄┄┄┄┄┄",
        "تحليل فقط: لا يغير المودات أو التنفيذ أو thresholds.",
        "",
    ]
    rows.extend(render_source("Replay", payload.get("replay") or {}))
    rows.append("")
    rows.extend(render_source("Live Snapshot", payload.get("live_snapshot") or {}))
    rows.extend([
        "",
        "📌 الاستخدام العملي:",
        "- لو BLOCK/RECOVERY = 0 في replay، لا نحكم على recipes الخاصة بهم.",
        "- لو الانتقالات قليلة جدًا، نراجع تسجيل/توزيع المودات قبل أي optimization.",
    ])
    return "\n".join(rows)


def build_score_calibration_payload(settings: Any | None = None, redis_client: Any | None = None, live_limit: int = 50000) -> dict[str, Any]:
    replay_records, live_records = _load_inputs(settings, redis_client, live_limit=live_limit)
    modes: dict[str, Any] = {}
    for gate, mode in MODES.items():
        replay_cal = _build_calibration(replay_records, mode)
        live_cal = _build_calibration(live_records, mode)
        modes[gate] = {
            "mode": mode,
            "replay": replay_cal,
            "live_snapshot": live_cal,
            "gap": {
                "p50_live_minus_replay": round(float(live_cal.get("p50") or 0.0) - float(replay_cal.get("p50") or 0.0), 4),
                "p85_live_minus_replay": round(float(live_cal.get("p85") or 0.0) - float(replay_cal.get("p85") or 0.0), 4),
                "p95_live_minus_replay": round(float(live_cal.get("p95") or 0.0) - float(replay_cal.get("p95") or 0.0), 4),
            },
        }
    return {
        **_gate_payload_base("score_calibration_diagnostic"),
        "ok": True,
        "important": "Diagnostic only. Does not change scoring, recipes, or execution.",
        "modes": modes,
    }


def build_score_calibration_report(settings: Any | None = None, redis_client: Any | None = None, live_limit: int = 50000) -> str:
    payload = build_score_calibration_payload(settings=settings, redis_client=redis_client, live_limit=live_limit)
    rows = [
        "📏 Score Calibration Diagnostic",
        "┄┄┄┄┄┄┄┄",
        "تحليل فقط: يقارن توزيع score بين replay و live snapshots ولا يغير أي scoring.",
        "",
        "Mode | Replay count p50/p85/p95 | Live count p50/p85/p95 | Gap live-replay",
    ]
    for data in (payload.get("modes") or {}).values():
        r = data.get("replay") or {}
        l = data.get("live_snapshot") or {}
        g = data.get("gap") or {}
        rows.append(
            f"- {data.get('mode')}: "
            f"R {int(r.get('count') or 0)} {r.get('p50',0):.2f}/{r.get('p85',0):.2f}/{r.get('p95',0):.2f} | "
            f"L {int(l.get('count') or 0)} {l.get('p50',0):.2f}/{l.get('p85',0):.2f}/{l.get('p95',0):.2f} | "
            f"Δ {g.get('p50_live_minus_replay',0):+.2f}/{g.get('p85_live_minus_replay',0):+.2f}/{g.get('p95_live_minus_replay',0):+.2f}"
        )
    rows.extend([
        "",
        "📌 القراءة:",
        "- لو Live أعلى كثيرًا من Replay، thresholds الجديدة قد تكون مضللة.",
        "- لو أحد المودات count=0، لا تستخدم calibration الخاص به في قرار recipe.",
    ])
    return "\n".join(rows)
