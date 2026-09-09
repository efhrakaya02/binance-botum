import ccxt.async_support as ccxt
import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional

# --- KURUMSAL PRICE ACTION & TIMSAH (CROCODILE) SINIFLARI ---
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

def find_swing_points(candles, lookback: int = 2) -> List[SwingPoint]:
    points = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback : i + lookback + 1]
        c = candles[i]
        highs = [w[2] for w in window] 
        lows = [w[3] for w in window]  
        if c[2] == max(highs):
            points.append(SwingPoint(index=i, price=c[2], type=SwingType.HIGH))
        elif c[3] == min(lows):
            points.append(SwingPoint(index=i, price=c[3], type=SwingType.LOW))
    return points

def determine_trend(swings: List[SwingPoint]) -> Trend:
    highs = [s for s in swings if s.type == SwingType.HIGH][-3:]
    lows = [s for s in swings if s.type == SwingType.LOW][-3:]
    if len(highs) >= 2 and len(lows) >= 2:
        if highs[-1].price > highs[-2].price and lows[-1].price > lows[-2].price: 
            return Trend.UP
        if highs[-1].price < highs[-2].price and lows[-1].price < lows[-2].price: 
            return Trend.DOWN
    return Trend.RANGE

def check_exhaustion(candles) -> Optional[str]:
    """Mumların anatomisine bakarak tepeden veya dipten red yeme (Fitil) durumunu tespit eder."""
    for c in candles[-3:]:
        open_p, high_p, low_p, close_p = c[1], c[2], c[3], c[4]
        body = max(abs(close_p - open_p), close_p * 0.0001) 
        
        upper_wick = high_p - max(open_p, close_p)
        lower_wick = min(open_p, close_p) - low_p
        
        if upper_wick > body * 2.5 and upper_wick > lower_wick * 2:
            return 'TOP_REJECTION'
        if lower_wick > body * 2.5 and lower_wick > upper_wick * 2:
            return 'BOTTOM_REJECTION'
    return None

def calculate_atr(candles, period: int = 14) -> float:
    if len(candles) < period + 1: return 0.0
    tr_list = []
    for i in range(1, len(candles)):
        high_p, low_p, prev_close = candles[i][2], candles[i][3], candles[i-1][4]
        tr = max(high_p - low_p, abs(high_p - prev_close), abs(low_p - prev_close))
        tr_list.append(tr)
    return sum(tr_list[-period:]) / period

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
        try:
            tickers = await self.exchange.fetch_tickers()
            usable = [t for t in tickers.values() if t['symbol'].endswith('USDT')]
            
            by_vol = sorted(usable, key=lambda t: t.get('quoteVolume', 0) or 0, reverse=True)
            by_gain = sorted(usable, key=lambda t: t.get('percentage', 0) or 0, reverse=True)
            by_loss = sorted(usable, key=lambda t: t.get('percentage', 0) or 0)
            
            volume_rank = {t['symbol']: i + 1 for i, t in enumerate(by_vol[:50])}
            gainer_rank = {t['symbol']: i + 1 for i, t in enumerate(by_gain[:50])}
            loser_rank = {t['symbol']: i + 1 for i, t in enumerate(by_loss[:50])}
            
            candidate_symbols = set(volume_rank.keys()) | set(gainer_rank.keys()) | set(loser_rank.keys())
            
            scored_candidates = []
            for symbol in candidate_symbols:
                best_rank = min(volume_rank.get(symbol, self.NEUTRAL_RANK), 
                                gainer_rank.get(symbol, self.NEUTRAL_RANK), 
                                loser_rank.get(symbol, self.NEUTRAL_RANK))
                if best_rank <= 25:
                    scored_candidates.append((symbol, best_rank))
                    
            scored_candidates.sort(key=lambda x: x[1])
            return [x[0] for x in scored_candidates[:20]]
        except Exception:
            return []

    async def scan_market(self):
        opportunities = []
        hot_symbols = await self._get_hot_candidates()
        
        for symbol in hot_symbols:
            try:
                tasks = [
                    self.exchange.fetch_ohlcv(symbol, timeframe='4h', limit=50),
                    self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=20),
                    self.exchange.fetch_ohlcv(symbol, timeframe='5m', limit=20)
                ]
                c_4h, c_15m, c_5m = await asyncio.gather(*tasks)
                
                c_4h_closed = c_4h[:-1]
                c_15m_closed = c_15m[:-1]
                c_5m_closed = c_5m[:-1]
                
                # 1. MAKRO TREND TAYİNİ
                swings_4h = find_swing_points(c_4h_closed)
                macro_trend = determine_trend(swings_4h)
                if macro_trend == Trend.RANGE:
                    continue # Yatay piyasada timsah ava çıkmaz.
                
                # 2. KAPASİTE (ATR) KONTROLÜ - 4H Mumda yenecek et kaldı mı?
                atr_4h = calculate_atr(c_4h_closed, period=14)
                active_4h_candle = c_4h[-1]
                current_range = active_4h_candle[2] - active_4h_candle[3] # Anlık Yüksek - Anlık Düşük
                
                if current_range >= atr_4h * 0.75:
                    continue # Mum zaten potansiyelinin %75'ini doldurmuş, riskli.
                
                # 3. MİKRO DÜZELTME (PULLBACK) VE ONAY AVI
                exh_15m = check_exhaustion(c_15m_closed)
                exh_5m = check_exhaustion(c_5m_closed)
                
                target_trend = None
                reason = ""
                
                if macro_trend == Trend.UP:
                    # Makro yükseliş. Fiyatın destekten sekmesini (Alt Fitil) bekliyoruz.
                    if exh_15m == 'BOTTOM_REJECTION' or exh_5m == 'BOTTOM_REJECTION':
                        target_trend = 'long'
                        reason = f"Makro (4H) Yükseliş trendinde. 15M/5M'de destekten sekme (Alt Fitil) yakalandı. Kapasite uygun."
                        
                elif macro_trend == Trend.DOWN:
                    # Makro düşüş. Fiyatın dirençten reddedilmesini (Üst Fitil) bekliyoruz.
                    if exh_15m == 'TOP_REJECTION' or exh_5m == 'TOP_REJECTION':
                        target_trend = 'short'
                        reason = f"Makro (4H) Düşüş trendinde. 15M/5M'de dirençten red (Üst Fitil) yakalandı. Kapasite uygun."

                if not target_trend:
                    continue

                # 4. TİMSAH RİSK YÖNETİMİ (Geniş SL, Mantıklı TP)
                current_price = c_15m[-1][4]
                
                if target_trend == 'long':
                    final_sl = current_price - (atr_4h * 1.5) # 4H ATR'nin 1.5 katı genişliğinde güvenli stop
                    target_tp = current_price + (atr_4h - current_range) # Mumun kalan potansiyeli kâr hedefi
                else:
                    final_sl = current_price + (atr_4h * 1.5)
                    target_tp = current_price - (atr_4h - current_range)

                opportunities.append({
                    "symbol": symbol,
                    "trend": target_trend,
                    "sl_price": final_sl,
                    "tp_price": target_tp,   # 🚀 TP NOKTASI EKLENDİ
                    "reason": reason
                })
                break 

            except Exception:
                continue
                
        return opportunities
