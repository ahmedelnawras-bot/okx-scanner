from __future__ import annotations

from analytics.technical_dataset import (
    build_gate_suggestions_report,
    build_technical_dataset_export,
    build_technical_dataset_export_file,
    build_technical_dataset_status,
)


def build_technical_dataset_help() -> str:
    return "\n".join([
        "🧠 Technical Dataset / Live AI Data",
        "┄┄┄┄┄┄┄┄",
        "/tech_snapshot_status",
        "↳ يعرض حالة التسجيل وعدد السجلات حسب النوع والمود.",
        "/tech_snapshot_on",
        "↳ يشغل تسجيل الإشارات الخام من البوت الحي.",
        "/tech_snapshot_off",
        "↳ يوقف التسجيل فقط ويحافظ على الداتا الموجودة.",
        "/tech_snapshot_export",
        "↳ يعرض مكان/حالة مخزن JSONL/Redis ومعاينة بسيطة.",
        "/tech_snapshot_export_file",
        "↳ يرسل ملف live snapshots مضغوط ZIP للتحميل.",
        "/tech_snapshot_clear",
        "↳ يمسح داتا snapshots فقط عند الحاجة.",
        "",
        "📊 Reports",
        "/gate_suggestions",
        "↳ قراءة مبدئية للبوابات بدون تطبيق أي فلتر.",
        "/historical_report",
        "↳ تقرير الداتا التاريخية عند توفرها.",
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
    "build_technical_dataset_export_file",
    "build_gate_suggestions_report",
    "build_historical_report",
]
