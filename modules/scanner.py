import ccxt.async_support as ccxt
import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional

# --- KURUMSAL PRICE ACTION (SMC) SINIFLARI VE FONKSİYONLARI ---
class Trend(Enum):
    UP = "UP"
    DOWN = "DOWN"
    RANGE = "RANGE"

class SwingType(Enum):
    HIGH = "HIGH"
    LOW = "LOW"

@dataclass
class SwingPoint:
    index: int
    price: float
    type: SwingType

@dataclass
class StructureBreak:
    kind: str
    direction: Trend
    broken_level: float
    break_close: float

def find_swing_points(candles, lookback: int = 2) -> List[SwingPoint]:
    points = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback : i + lookback + 1]
        c = candles[i]
        highs = [w[2] for w in window] # High index = 2
        lows = [w[3] for w in window]  # Low index = 3
        if c[2] == max(highs):
            points.append(SwingPoint(index=i, price=c[2], type=SwingType.HIGH))
        elif c[3] == min(lows):
            points.append(SwingPoint(index=i, price=c[3], type=SwingType.LOW))
    return points

def determine_trend(swings: List[SwingPoint]) -> Trend:
    highs = [s for s in swings if s.type == SwingType.HIGH][-3:]
    lows = [s for s in swings if s.type == SwingType.LOW][-3:]
    if len(highs) >= 2 and len(lows) >= 2:
        higher_highs = highs[-1].price > highs[-2].price
        higher_lows = lows[-1].price > lows[-2].price
        lower_highs = highs[-1].price < highs[-2].price
        lower_lows = lows[-1].price < lows[-2].price
        if higher_highs and higher_lows: return Trend.UP
        if lower_highs and lower_lows: return Trend.DOWN
    return Trend.RANGE

def detect_structure_break(candles, swings: List[SwingPoint], prevailing_trend: Trend) -> Optional[StructureBreak]:
    if not candles or not swings: return None
    last_close = candles[-1][4] # Close index = 4
    last_high = next((s for s in reversed(swings) if s.type == SwingType.HIGH), None)
    last_low = next((s for s in reversed(swings) if s.type == SwingType.LOW), None)

    if prevailing_trend == Trend.UP and last_high and last_close > last_high.price:
        return StructureBreak("BOS", Trend.UP, last_high.price, last_close)
    if prevailing_trend == Trend.DOWN and last_low and last_close < last_low.price:
        return StructureBreak("BOS", Trend.DOWN, last_low.price, last_close)
    return None

def volume_anomaly_ratio(candles, baseline_window: int = 20) -> float:
    if len(candles) < baseline_window + 1: return 1.0
    baseline = candles[-(baseline_window + 1) : -1]
    avg_vol = sum(c[5] for c in baseline) / len(baseline) # Volume index = 5
    if avg_vol == 0: return 1.0
    return candles[-1][5] / avg_vol

def momentum_roc(candles, periods: int = 5) -> float:
    if len(candles) < periods + 1: return 0.0
    past = candles[-(periods + 1)][4]
    now = candles[-1][4]
    if past == 0: return 0.0
    return (now - past) / past * 100

# --- ANA TARAYICI SINIFI ---
class MarketScanner:
    def __init__(self, config):
        self.config = config
        self.exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
        self.previous_ranks = {}
        self.NEUTRAL_RANK = 10000

    async def close(self):
        await self.exchange.close()

    async def _get_hot_candidates(self) -> List[str]:
        """Rank Velocity ve Composite Score mantığı ile en sıcak coinleri bulur."""
        try:
            tickers = await self.exchange.fetch_tickers()
            usable = [t for t in tickers.values() if t['symbol'].endswith('USDT')]
            
            by_vol = sorted(usable, key=lambda t: t.get('quoteVolume', 0), reverse=True)
            volume_rank = {t['symbol']: i + 1 for i, t in enumerate(by_vol[:100])}
            
            scored_candidates = []
            current_ranks = {}
            
            for symbol, t in tickers.items():
                if not symbol.endswith('USDT'): continue
                
                best_rank = volume_rank.get(symbol, self.NEUTRAL_RANK)
                current_ranks[symbol] = best_rank
                prev_rank = self.previous_ranks.get(symbol)
                velocity = (prev_rank - best_rank) if prev_rank else 0
                is_new = prev_rank is None and best_rank <= 40
                
                # Sadece ivmeli, yeni giren veya zaten çok hacimli olanları seç
                if velocity >= 5 or is_new or best_rank <= 20:
                    scored_candidates.append(symbol)
                    
            self.previous_ranks = current_ranks
            return scored_candidates[:25] # GÜNCELLEME: En sıcak 25 coini analiz için gönder
        except Exception:
            return []

    async def scan_market(self):
        """Piyasayı tarar, SMC kurallarına göre analiz eder ve işlem sinyali üretir."""
        opportunities = []
        hot_symbols = await self._get_hot_candidates()
        
        for symbol in hot_symbols:
            try:
                # 4H, 1H, 15M, 5M verilerini paralel çek (Hız için)
                tasks = [
                    self.exchange.fetch_ohlcv(symbol, timeframe='4h', limit=50),
                    self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50),
                    self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=30),
                    self.exchange.fetch_ohlcv(symbol, timeframe='5m', limit=30)
                ]
                c_4h, c_1h, c_15m, c_5m = await asyncio.gather(*tasks)
                
                # 1. Aşama: 4H Makro Yön
                swings_4h = find_swing_points(c_4h)
                macro_trend = determine_trend(swings_4h)
                if macro_trend == Trend.RANGE: continue # Yatay piyasayı çöpe at
                
                # 2. Aşama: 1H Uyum ve BOS (Yapı Kırılımı)
                swings_1h = find_swing_points(c_1h)
                trend_1h = determine_trend(swings_1h)
                if trend_1h != macro_trend: continue # Uyumsuz trend
                
                structure_break = detect_structure_break(c_1h, swings_1h, macro_trend)
                if not structure_break or structure_break.kind != "BOS": continue # Kırılım yoksa girme
                
                # 3. Aşama: 15M ve 5M Hacim/Momentum İvmesi
                vol_ratio = volume_anomaly_ratio(c_5m)
                roc = momentum_roc(c_5m)
                
                trend_str = 'long' if macro_trend == Trend.UP else 'short'
                momentum_aligned = (roc > 0.1 and trend_str == 'long') or (roc < -0.1 and trend_str == 'short')
                
                # GÜNCELLEME: Hacim anomalisini 1.30'a çektik
                if vol_ratio >= 1.30 and momentum_aligned:
                    # SL Hesaplama (Son Swing Low / High)
                    if trend_str == 'long':
                        sl_price = min(c[3] for c in c_15m[-5:]) * 0.995 # Son 5 mumun en düşüğü
                    else:
                        sl_price = max(c[2] for c in c_15m[-5:]) * 1.005
                        
                    reason = f"4H/1H makro yön uyumlu. 1H grafikte BOS (Yapı Kırılımı) onaylandı. 5M'de x{vol_ratio:.2f} hacim anomalisi ve {roc:+.2f}% ivme var!"
                    
                    opportunities.append({
                        "symbol": symbol,
                        "trend": trend_str,
                        "sl_price": sl_price,
                        "reason": reason
                    })
                    break # Bulduğumuz ilk kaliteli sinyalde döngüyü kes, işleme git.
                    
            except Exception as e:
                continue
                
        return opportunities

    async def check_momentum_reversal(self, symbol: str, trend: str) -> bool:
        """İçerideyken trendin terse dönüp dönmediğini (CHoCH) kontrol eder."""
        try:
            candles = await self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=20)
            swings = find_swing_points(candles)
            prevailing = Trend.UP if trend == 'long' else Trend.DOWN
            
            # Trendin tersi yönde bir kırılım (CHoCH) var mı?
            last_close = candles[-1][4]
            last_high = next((s for s in reversed(swings) if s.type == SwingType.HIGH), None)
            last_low = next((s for s in reversed(swings) if s.type == SwingType.LOW), None)

            if prevailing == Trend.UP and last_low and last_close < last_low.price:
                return True # Long'daydık, aşağı kırdı!
            if prevailing == Trend.DOWN and last_high and last_close > last_high.price:
                return True # Short'taydık, yukarı kırdı!
                
            return False
        except:
            return False
