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
            url = f"{base_url}/api/v5/market/candles"
            params = {
                "instId": "BTC-USDT",
                "bar": "4h",
                "limit": limit,
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("code") == "0" and data.get("data"):
                    # OKX يرجّع [ts, o, h, l, c, vol, volCcy, ...] لكل شمعة.
                    # نستخدم index 5 (vol) بدل 7 (volCcyQuote) — دايماً موجود، وأأمن.
                    candles = [list(map(float, c[:5])) + [float(c[5])] for c in data["data"]]
                    # ⚠️ مهم جداً: OKX يرجّع الشموع بترتيب الأحدث أولاً (newest-first).
                    # كل الكود هنا بيفترض ترتيب زمني تصاعدي (الأقدم أولاً، الأحدث آخراً)
                    # عشان index [-1] يمثّل فعلاً "الشمعة/السعر الحالي". لو مانعكسناش
                    # الترتيب هنا، الماسح بيحلل بيانات قديمة على إنها حالية = نتائج خاطئة بالكامل.
                    candles.reverse()
                    return candles
        except Exception as e:
            print(f"❌ خطأ جلب BTC 4h: {e}")
        
        return []
    
    async def fetch_alt_candles(self, symbol: str, timeframe: str = "4h", limit: int = 20) -> list[list]:
        """جلب شموع عملة (4h أو 15m)"""
        try:
            url = "https://www.okx.com/api/v5/market/candles"
            params = {
                "instId": f"{symbol}-USDT",
                "bar": timeframe,
                "limit": limit,
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("code") == "0" and data.get("data"):
                    candles = [list(map(float, c[:5])) + [float(c[5])] for c in data["data"]]
                    # ⚠️ نفس ملاحظة BTC — لازم نعكس الترتيب عشان [-1] = الأحدث فعلاً.
                    candles.reverse()
                    return candles
        except Exception as e:
            print(f"❌ خطأ جلب {symbol}: {e}")
        
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
                         btc_dominance_change: float) -> Optional[ConsolidationSignal]:
        """فحص عملة واحدة للفرص"""
        
        try:
            # جلب الشموع 4h
            candles_4h = await self.fetch_alt_candles(symbol, timeframe="4h", limit=20)
            if not candles_4h or len(candles_4h) < 10:
                return None
            
            # كشف التجميع
            consolidation = self.detect_consolidation(candles_4h)
            if not consolidation or not consolidation.get("is_consolidating"):
                return None
            
            # حساب volume trend
            last_n = candles_4h[-10:]
            volumes = [c[5] for c in last_n]
            vol_avg = sum(volumes[:-3]) / max(len(volumes) - 3, 1)
            vol_current = volumes[-1]
            volume_trend = (vol_current / vol_avg) if vol_avg > 0 else 1.0
            
            # حساب accumulation score
            score = self.calculate_accumulation_score(consolidation, volume_trend)
            if score < 50:  # threshold 50
                return None
            
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
            
            return signal
            
        except Exception as e:
            print(f"⚠️ خطأ فحص {symbol}: {e}")
            return None
    
    async def scan_all(self, symbols: list[str], btc_candles: list[list], 
                      btc_dominance_change: float = 0.0, max_workers: int = 10) -> list[ConsolidationSignal]:
        """فحص كل العملات
        
        يستخدم ThreadPoolExecutor للسرعة
        """
        
        # كشف BTC support/resistance
        btc_support, btc_resistance = self.detect_btc_support_resistance(btc_candles)
        if not btc_support or not btc_resistance:
            print("❌ لا يمكن كشف دعم BTC")
            return []
        
        signals = []
        
        # فحص بالتوازي
        tasks = [
            self.scan_symbol(symbol, btc_support, btc_resistance, btc_dominance_change)
            for symbol in symbols
        ]
        
        results = await asyncio.gather(*tasks)
        signals = [s for s in results if s is not None]
        
        # ترتيب بـ score تنازلي
        signals.sort(key=lambda s: s.accumulation_score, reverse=True)
        
        return signals
    
    def format_report_ar(self, signals: list[ConsolidationSignal], title: str = "🔍 تقرير الماسح") -> str:
        """تنسيق التقرير بالعربي"""
        
        if not signals:
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
