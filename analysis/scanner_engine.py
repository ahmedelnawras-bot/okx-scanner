"""
Scanner Engine — كاشف الفرص (Consolidation + Support + Supply/Demand)
يكتشف العملات المضغوطة في مرحلة تجميع استعداداً للانفجار

Strategy:
├─ BTC 4h: عند دعم (support)
├─ Alt: في consolidation (نطاق ضيق + volume يزيد)
├─ Supply/Demand: في صالح الشراء
└─ DCA: 3 دخلات (10 + 20 + 20) عند الارتداد 15m
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import asyncio
import requests


@dataclass
class ConsolidationSignal:
    """إشارة تجميع في عملة"""
    symbol: str
    current_price: float
    support_level: float
    resistance_level: float
    consolidation_range_pct: float
    volume_trend: float  # نسبة الـ volume الحالي / المتوسط
    btc_distance_to_support_pct: float
    btc_dominance_change: float
    accumulation_score: float  # 0-100
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> dict:
        return {
            'symbol': self.symbol,
            'current_price': round(self.current_price, 8),
            'support': round(self.support_level, 8),
            'resistance': round(self.resistance_level, 8),
            'range_%': round(self.consolidation_range_pct, 2),
            'volume_trend': round(self.volume_trend, 2),
            'btc_distance_%': round(self.btc_distance_to_support_pct, 2),
            'btc_dominance_change_%': round(self.btc_dominance_change, 2),
            'accumulation_score': round(self.accumulation_score, 1),
            'timestamp': self.timestamp.isoformat(),
        }


class ScannerEngine:
    """محرك الماسح الأساسي"""
    
    def __init__(self):
        self.btc_4h_support = None
        self.btc_4h_resistance = None
        self.btc_dominance = 0.0
        self.btc_dominance_change = 0.0
        
    async def fetch_btc_4h_candles(self, base_url: str = "https://www.okx.com", limit: int = 20) -> list[list]:
        """جلب شموع BTC 4h (آخر 20 شمعة)
        
        Return: [[timestamp, open, high, low, close, volume, ...], ...]
        """
        try:
            return await asyncio.to_thread(self._fetch_candles_sync, base_url, "BTC-USDT", "4h", limit)
        except Exception as e:
            print(f"❌ خطأ جلب BTC 4h: {e}", flush=True)
            return []

    def _fetch_candles_sync(self, base_url: str, inst_id: str, bar: str, limit: int) -> list[list]:
        """جلب الشموع (استدعاء متزامن — يُشغَّل داخل thread عبر asyncio.to_thread
        عشان الفحوصات المتعددة تشتغل بالتوازي فعلاً، مش الواحدة ورا التانية).
        """
        url = f"{base_url}/api/v5/market/candles"
        params = {"instId": inst_id, "bar": bar, "limit": limit}

        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ OKX HTTP {response.status_code} | {inst_id}", flush=True)
            return []

        data = response.json()
        if data.get("code") != "0" or not data.get("data"):
            print(f"⚠️ OKX API error | {inst_id} | code={data.get('code')} msg={data.get('msg')}", flush=True)
            return []

        # OKX يرجّع [ts, o, h, l, c, vol, volCcy, ...] — نستخدم index 5 (vol) الآمن دايماً.
        candles = [list(map(float, c[:5])) + [float(c[5])] for c in data["data"]]
        # ⚠️ مهم جداً: OKX يرجّع الشموع بترتيب الأحدث أولاً (newest-first).
        # كل منطق الماسح بيفترض ترتيب زمني تصاعدي عشان [-1] = الشمعة الحالية فعلاً.
        candles.reverse()
        return candles
    
    async def fetch_alt_candles(self, symbol: str, timeframe: str = "4h", limit: int = 20, base_url: str = "https://www.okx.com") -> list[list]:
        """جلب شموع عملة (4h أو 15m) — في thread منفصل لتوازي حقيقي"""
        try:
            return await asyncio.to_thread(
                self._fetch_candles_sync, base_url, f"{symbol}-USDT", timeframe, limit
            )
        except Exception as e:
            print(f"❌ خطأ جلب {symbol}: {e}", flush=True)
            return []
    
    def detect_btc_support_resistance(self, candles: list[list]) -> tuple[float, float]:
        """كشف دعم/مقاومة BTC من آخر 20 شمعة 4h
        
        Support = أقل قاع (swing low)
        Resistance = أعلى قمة (swing high)
        """
        if not candles or len(candles) < 2:
            return None, None
        
        lows = [c[3] for c in candles]  # low
        highs = [c[2] for c in candles]  # high
        
        support = min(lows[-10:])  # آخر 10 شموة
        resistance = max(highs[-10:])
        
        return support, resistance
    
    def detect_consolidation(self, candles: list[list], min_candles: int = 10) -> dict:
        """كشف مرحلة تجميع (consolidation pattern)
        
        Signs:
        ├─ السعر في نطاق ضيق (آخر 10-15 شمعة)
        ├─ الـ volume بيزيد تدريجياً
        └─ السعر لسه ما انفجر (waiting)
        """
        if len(candles) < min_candles:
            return None
        
        last_n = candles[-min_candles:]
        
        # النطاق السعري (range)
        highs = [c[2] for c in last_n]
        lows = [c[3] for c in last_n]
        closes = [c[4] for c in last_n]
        volumes = [c[5] for c in last_n]
        
        high = max(highs)
        low = min(lows)
        current = closes[-1]
        range_pct = ((high - low) / low) * 100.0 if low > 0 else 0.0
        
        # الـ volume trend
        vol_avg_past = sum(volumes[:-3]) / max(len(volumes) - 3, 1) if len(volumes) > 3 else 0
        vol_current = volumes[-1]
        volume_trend = (vol_current / vol_avg_past) if vol_avg_past > 0 else 1.0
        
        # هل في consolidation؟
        is_consolidating = range_pct < 3.0  # نطاق ضيق (أقل من 3%)
        volume_increasing = volume_trend > 1.1  # volume يزيد 10%+
        
        support = low
        resistance = high
        
        return {
            "is_consolidating": is_consolidating,
            "range_pct": range_pct,
            "volume_trend": volume_trend,
            "support": support,
            "resistance": resistance,
            "current_price": current,
            "position_in_range": ((current - low) / (high - low) * 100.0) if (high - low) > 0 else 50,
        }
    
    def calculate_accumulation_score(self, consolidation: dict, volume_trend: float) -> float:
        """حساب score التجميع (0-100)
        
        عوامل:
        ├─ تضيق النطاق (range صغير = تجميع حقيقي)
        ├─ volume يزيد (pressure من المشترين)
        ├─ السعر في النص/الأسفل (تراكم، مش قمة)
        └─ النطاق المقفل (الآخر 5 شموة مثلاً)
        """
        score = 0.0
        
        # العامل 1: تضيق النطاق (أقل من 2% = 30 نقطة)
        range_pct = consolidation.get("range_pct", 10)
        if range_pct < 1.0:
            score += 30
        elif range_pct < 2.0:
            score += 25
        elif range_pct < 3.0:
            score += 15
        
        # العامل 2: volume يزيد (>1.2 = 25 نقطة)
        if volume_trend > 1.3:
            score += 25
        elif volume_trend > 1.1:
            score += 15
        
        # العامل 3: السعر في النص/الأسفل (تراكم)
        pos = consolidation.get("position_in_range", 50)
        if pos < 40:
            score += 20  # في الأسفل = تراكم
        elif pos > 60:
            score -= 10  # في القمة = خطر
        
        # العامل 4: الـ volume locked (آخر شموة)
        # (سيُضاف لاحقاً عند تحليل شموة 15m)
        
        return min(max(score, 0), 100)
    
    async def scan_symbol(self, symbol: str, btc_support: float, btc_resistance: float, 
                         btc_dominance_change: float, base_url: str = "https://www.okx.com") -> tuple[Optional[ConsolidationSignal], str]:
        """فحص عملة واحدة للفرص
        
        Return: (الإشارة أو None, سبب الرفض للتشخيص)
        """
        
        try:
            # جلب الشموع 4h
            candles_4h = await self.fetch_alt_candles(symbol, timeframe="4h", limit=20, base_url=base_url)
            if not candles_4h or len(candles_4h) < 10:
                return None, "no_data"
            
            # كشف التجميع
            consolidation = self.detect_consolidation(candles_4h)
            if not consolidation or not consolidation.get("is_consolidating"):
                return None, "not_consolidating"
            
            # حساب volume trend
            last_n = candles_4h[-10:]
            volumes = [c[5] for c in last_n]
            vol_avg = sum(volumes[:-3]) / max(len(volumes) - 3, 1)
            vol_current = volumes[-1]
            volume_trend = (vol_current / vol_avg) if vol_avg > 0 else 1.0
            
            # حساب accumulation score
            score = self.calculate_accumulation_score(consolidation, volume_trend)
            if score < 50:  # threshold 50
                return None, "low_score"
            
            # فاصلة من دعم BTC
            current_price = consolidation["current_price"]
            btc_distance = ((current_price - btc_support) / btc_support * 100) if btc_support > 0 else 0
            
            signal = ConsolidationSignal(
                symbol=symbol,
                current_price=current_price,
                support_level=consolidation["support"],
                resistance_level=consolidation["resistance"],
                consolidation_range_pct=consolidation["range_pct"],
                volume_trend=volume_trend,
                btc_distance_to_support_pct=btc_distance,
                btc_dominance_change=btc_dominance_change,
                accumulation_score=score,
            )
            
            return signal, "ok"
            
        except Exception as e:
            print(f"⚠️ خطأ فحص {symbol}: {e}", flush=True)
            return None, "exception"
    
    async def scan_all(self, symbols: list[str], btc_candles: list[list], 
                      btc_dominance_change: float = 0.0, max_workers: int = 10,
                      base_url: str = "https://www.okx.com") -> dict:
        """فحص كل العملات (بالتوازي الحقيقي عبر threads)
        
        Return: {
            "signals": [...],
            "diagnostics": {"total": N, "no_data": N, "not_consolidating": N, "low_score": N, "exception": N, "ok": N}
        }
        """
        
        diagnostics = {"total": len(symbols), "no_data": 0, "not_consolidating": 0, "low_score": 0, "exception": 0, "ok": 0}
        
        # كشف BTC support/resistance
        btc_support, btc_resistance = self.detect_btc_support_resistance(btc_candles)
        if not btc_support or not btc_resistance:
            print("❌ لا يمكن كشف دعم BTC — شموع BTC غير كافية أو فارغة", flush=True)
            diagnostics["btc_fetch_failed"] = True
            return {"signals": [], "diagnostics": diagnostics}
        
        # فحص بالتوازي (asyncio.to_thread داخل scan_symbol → توازي فعلي)
        tasks = [
            self.scan_symbol(symbol, btc_support, btc_resistance, btc_dominance_change, base_url=base_url)
            for symbol in symbols
        ]
        
        results = await asyncio.gather(*tasks)
        
        signals = []
        for sig, reason in results:
            diagnostics[reason] = diagnostics.get(reason, 0) + 1
            if sig is not None:
                signals.append(sig)
        
        # ترتيب بـ score تنازلي
        signals.sort(key=lambda s: s.accumulation_score, reverse=True)
        
        print(
            f"📊 Scan diagnostics | total={diagnostics['total']} "
            f"no_data={diagnostics['no_data']} not_consolidating={diagnostics['not_consolidating']} "
            f"low_score={diagnostics['low_score']} exception={diagnostics['exception']} ok={diagnostics['ok']}",
            flush=True,
        )
        
        return {"signals": signals, "diagnostics": diagnostics}
    
    def format_report_ar(self, signals: list[ConsolidationSignal], title: str = "🔍 تقرير الماسح", diagnostics: dict | None = None) -> str:
        """تنسيق التقرير بالعربي (مع تشخيص واضح عند عدم وجود نتائج)"""
        
        if not signals:
            if diagnostics:
                if diagnostics.get("btc_fetch_failed"):
                    return f"{title}\n━━━━━━━━━━━━\n⚠️ فشل جلب بيانات BTC من OKX. حاول مرة أخرى بعد قليل."
                total = diagnostics.get("total", 0)
                if total == 0:
                    return f"{title}\n━━━━━━━━━━━━\n⚠️ فشل جلب قائمة العملات من OKX (0 عملة). تحقق من الاتصال."
                no_data = diagnostics.get("no_data", 0)
                not_cons = diagnostics.get("not_consolidating", 0)
                low_score = diagnostics.get("low_score", 0)
                exc = diagnostics.get("exception", 0)
                return (
                    f"{title}\n━━━━━━━━━━━━\n"
                    f"❌ لا توجد فرص حالياً\n\n"
                    f"📊 تفاصيل الفحص ({total} عملة):\n"
                    f"• بدون بيانات: {no_data}\n"
                    f"• غير مجمّعة (خارج النطاق): {not_cons}\n"
                    f"• أقل من الحد الأدنى (score<50): {low_score}\n"
                    f"• أخطاء اتصال: {exc}\n"
                )
            return f"{title}\n━━━━━━━━━━━━\n❌ لا توجد فرص حالياً"
        
        lines = [
            title,
            "━━━━━━━━━━━━",
        ]
        
        for i, sig in enumerate(signals[:10], 1):  # أفضل 10
            lines.append(
                f"\n🟢 {i}. {sig.symbol}\n"
                f"   السعر: ${sig.current_price:.8f}\n"
                f"   الدعم: ${sig.support_level:.8f}\n"
                f"   المقاومة: ${sig.resistance_level:.8f}\n"
                f"   النطاق: {sig.consolidation_range_pct:.2f}%\n"
                f"   الحجم: {sig.volume_trend:.2f}x\n"
                f"   Score: {sig.accumulation_score:.0f}/100"
            )
        
        return "\n".join(lines)


# اختبار سريع
if __name__ == "__main__":
    import asyncio
    
    async def test():
        engine = ScannerEngine()
        
        # جلب BTC 4h
        btc_candles = await engine.fetch_btc_4h_candles()
        if not btc_candles:
            print("❌ لا يمكن جلب بيانات BTC")
            return
        
        # فحص عينة من العملات
        test_symbols = ["ALGO", "ARB", "NEAR", "DYDX", "JTO", "MEME", "PEPE"]
        signals = await engine.scan_all(test_symbols, btc_candles, btc_dominance_change=-0.5)
        
        print(engine.format_report_ar(signals))
        
        # بيانات JSON
        for sig in signals[:3]:
            print(sig.to_dict())
    
    asyncio.run(test())
