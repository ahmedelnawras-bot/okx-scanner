"""
Scanner Handler — تكامل الماسح مع البوت الرئيسي
- جدولة الفحص (كل 15 دقيقة)
- التنبيهات (Telegram)
- التقارير اليومية (24 ساعة)
- الأوامر اليدوية (/scan_1h, /scan_24h, /scan_week)
"""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
import asyncio
import json
from pathlib import Path

from analysis.scanner_engine import ScannerEngine, ConsolidationSignal


class ScannerHandler:
    """معالج الماسح — جدولة + تنبيهات + تقارير"""
    
    def __init__(self, telegram_sender=None, redis_client=None, settings=None):
        self.engine = ScannerEngine()
        self.telegram_sender = telegram_sender
        self.redis_client = redis_client
        self.settings = settings
        
        # المسار للحفظ (على السيرفر، فولدر مؤقت بجانب البوت)
        self.data_dir = Path("data/scanner_reports")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # تاريخ آخر فحص (لتجنب التكرار)
        self.last_scan_time = None
        self.last_alert_time = None
        self.last_report_time = None
        self.current_signals = []
        
    async def get_all_tradeable_symbols(self) -> list[str]:
        """جلب قائمة بكل العملات القابلة للتداول من OKX"""
        try:
            import requests
            
            url = "https://www.okx.com/api/v5/market/tickers"
            params = {"instType": "SWAP"}
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("code") == "0":
                    symbols = []
                    for ticker in data.get("data", []):
                        inst_id = ticker.get("instId", "")
                        # فلتر: BTC-USDT-SWAP فقط، لا stablecoins
                        if inst_id.endswith("-USDT-SWAP") and not any(
                            x in inst_id.upper() for x in ["USDC", "USDE", "FDUSD", "TUSD", "BUSD", "DAI"]
                        ):
                            symbol = inst_id.split("-")[0]
                            if symbol != "BTC":  # لا نفحص BTC نفسه
                                symbols.append(symbol)
                    
                    return list(set(symbols))[:300]  # أفضل 300 عملة
        except Exception as e:
            print(f"❌ خطأ جلب قائمة العملات: {e}")
        
        return []
    
    async def run_scan(self, btc_dominance_change: float = 0.0) -> list[ConsolidationSignal]:
        """تشغيل فحص واحد
        
        Return: قائمة الإشارات (الفرص المكتشفة)
        """
        
        # جلب BTC 4h
        btc_candles = await self.engine.fetch_btc_4h_candles()
        if not btc_candles:
            return []
        
        # جلب العملات
        symbols = await self.get_all_tradeable_symbols()
        if not symbols:
            return []
        
        # فحص الكل
        signals = await self.engine.scan_all(symbols, btc_candles, btc_dominance_change)
        
        self.current_signals = signals
        self.last_scan_time = datetime.now(timezone.utc)
        
        return signals
    
    async def send_alert_telegram(self, signals: list[ConsolidationSignal]) -> bool:
        """إرسال تنبيه تلقائي كل 15 دقيقة"""
        
        if not self.telegram_sender or not signals:
            return False
        
        try:
            # تنسيق بسيط للتنبيه السريع
            alert_text = "🔔 *فرص جديدة — آخر 15 دقيقة*\n━━━━━━━━━━━━━\n"
            
            for sig in signals[:5]:  # أفضل 5 فرص
                alert_text += (
                    f"\n🟢 {sig.symbol}\n"
                    f"`الدعم: ${sig.support_level:.8f}`\n"
                    f"`الحالي: ${sig.current_price:.8f}`\n"
                    f"`Score: {sig.accumulation_score:.0f}`\n"
                )
            
            self.telegram_sender.send_message(alert_text, parse_mode="Markdown")
            self.last_alert_time = datetime.now(timezone.utc)
            return True
            
        except Exception as e:
            print(f"⚠️ خطأ إرسال التنبيه: {e}")
            return False
    
    async def send_daily_report(self, signals: list[ConsolidationSignal], period: str = "24h") -> bool:
        """إرسال تقرير يومي"""
        
        if not self.telegram_sender:
            return False
        
        try:
            report = self.engine.format_report_ar(
                signals,
                title=f"📊 تقرير الماسح — آخر {period}"
            )
            
            # حفظ الملف
            report_file = self.data_dir / f"scanner_report_{period}_{datetime.now(timezone.utc).isoformat()}.txt"
            report_file.write_text(report, encoding="utf-8")
            
            # إرسال
            self.telegram_sender.send_message(
                f"📊 *تقرير يومي*\n\n{report}",
                parse_mode="Markdown"
            )
            
            self.last_report_time = datetime.now(timezone.utc)
            return True
            
        except Exception as e:
            print(f"⚠️ خطأ إرسال التقرير: {e}")
            return False
    
    async def schedule_scanner(self, alert_interval_minutes: int = 15, report_interval_hours: int = 24):
        """جدولة الفحص الدوري
        
        - كل 15 دقيقة: فحص + تنبيه
        - كل 24 ساعة: تقرير شامل
        """
        
        print("🚀 بدء جدولة الماسح")
        
        while True:
            try:
                now = datetime.now(timezone.utc)
                
                # الفحص كل 15 دقيقة
                if (self.last_scan_time is None or 
                    (now - self.last_scan_time).total_seconds() >= alert_interval_minutes * 60):
                    
                    print(f"📡 فحص... ({now.isoformat()})")
                    signals = await self.run_scan(btc_dominance_change=0.0)
                    
                    if signals:
                        print(f"✅ وجدت {len(signals)} فرصة")
                        await self.send_alert_telegram(signals)
                    else:
                        print("⚪ لا توجد فرص حالياً")
                
                # التقرير كل 24 ساعة
                if (self.last_report_time is None or 
                    (now - self.last_report_time).total_seconds() >= report_interval_hours * 3600):
                    
                    print("📊 إرسال التقرير اليومي")
                    await self.send_daily_report(self.current_signals, period="24h")
                
                # الانتظار قبل الفحص التالي
                await asyncio.sleep(60)  # فحص كل دقيقة في الواقع، بس الفحص الفعلي كل 15
                
            except Exception as e:
                print(f"❌ خطأ في الجدولة: {e}")
                await asyncio.sleep(60)
    
    async def handle_scan_command(self, period: str = "1h") -> str:
        """معالج الأمر `/scan_1h` أو `/scan_24h` أو `/scan_week`"""
        
        print(f"📡 أمر يدوي: /scan_{period}")
        signals = await self.run_scan()
        
        if not signals:
            return "❌ لا توجد فرص حالياً"
        
        report = self.engine.format_report_ar(
            signals,
            title=f"🔍 الماسح — آخر {period}"
        )
        
        return report


# دالة التكامل مع main.py
async def setup_scanner_in_main(main_instance, telegram_sender, redis_client=None):
    """إضافة الماسح للبوت الرئيسي
    
    استدعي هذه من main.py في دالة البدء
    """
    
    scanner = ScannerHandler(
        telegram_sender=telegram_sender,
        redis_client=redis_client,
        settings=main_instance.settings if hasattr(main_instance, 'settings') else None
    )
    
    # حفظ للبوت
    main_instance.scanner = scanner
    
    # تشغيل الجدولة في الخلفية
    asyncio.create_task(scanner.schedule_scanner(alert_interval_minutes=15, report_interval_hours=24))
    
    print("✅ الماسح متصل وجاهز للعمل")
    
    return scanner


# اختبار
if __name__ == "__main__":
    import asyncio
    
    async def test():
        handler = ScannerHandler()
        
        # فحص واحد
        signals = await handler.run_scan()
        print(f"✅ وجدت {len(signals)} فرصة")
        
        # التقرير
        print(handler.engine.format_report_ar(signals))
    
    asyncio.run(test())
