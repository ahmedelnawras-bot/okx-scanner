from __future__ import annotations

from analytics.technical_dataset import (
    build_gate_suggestions_report,
    build_technical_dataset_export,
    build_technical_dataset_status,
)


def build_technical_dataset_help() -> str:
    return "\n".join([
        "🧠 Technical Dataset / AI Data",
        "┄┄┄┄┄┄┄┄",
        "/tech_snapshot_status — حالة تسجيل الداتا الفنية",
        "/tech_snapshot_on — تشغيل تسجيل الإشارات الخام",
        "/tech_snapshot_off — إيقاف تسجيل الإشارات الخام",
        "/tech_snapshot_export — مكان ملف JSONL للـ AI",
        "",
        "📊 Reports",
        "/gate_suggestions — قراءة مبدئية للبوابات بدون تطبيق",
        "/historical_report — تقرير الداتا التاريخية عند توفرها",
        "",
        "🔒 التسجيل لا يغير السكور أو التنفيذ أو TP/SL.",
    ])


def build_historical_report(settings=None) -> str:
    # Placeholder report reuses current AI snapshot until historical builder output is added.
    return "\n".join([
        "📜 Historical Dataset Report",
        "┄┄┄┄┄┄┄┄",
        "الهيكل جاهز. ملف الداتا التاريخية سيتم توليده من Historical Builder في خطوة مستقلة.",
        "حاليًا استخدم /tech_snapshot_status للداتا الحية المسجلة.",
    ])


__all__ = [
    "build_technical_dataset_help",
    "build_technical_dataset_status",
    "build_technical_dataset_export",
    "build_gate_suggestions_report",
    "build_historical_report",
]
