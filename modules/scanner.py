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
    kind: str          # "BOS" (Devam) veya "CHoCH" (Tersine Dönüş)
    direction: Trend   # Kırılım sonrası işaret ettiği yeni yön
    broken_level: float
    break_close: float
    is_strong: bool    # Mum gövdesi güçlü mü?

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
        higher_highs = highs[-1].price > highs[-2].price
        higher_lows = lows[-1].price > lows[-2].price
        lower_highs = highs[-1].price < highs[-2].price
        lower_lows = lows[-1].price < lows[-2].price
        if higher_highs and higher_lows: return Trend.UP
        if lower_highs and lower_lows: return Trend.DOWN
    return Trend.RANGE

def detect_structure_break(candles, swings: List[SwingPoint], prevailing_trend: Trend) -> Optional[StructureBreak]:
    if not candles or not swings: return None
    last_candle = candles[-1]
    open_p, high_p, low_p, close_p = last_candle[1], last_candle[2], last_candle[3], last_candle[4]
    
    last_high = next((s for s in reversed(swings) if s.type == SwingType.HIGH), None)
    last_low = next((s for s in reversed(swings) if s.type == SwingType.LOW), None)

    # Mum gövdesi güçlü mü analizi (Kapanış mumun zirvesine/dibine yakın mı?)
    body = abs(close_p - open_p)
    total_range = high_p - low_p
    total_range = max(total_range, close_p * 0.0001)
    is_strong_body = (body / total_range) > 0.60 # Mumun %60'ı gövde ise güçlüdür

    # BOS (Trend Devamı)
    if prevailing_trend == Trend.UP and last_high and close_p > last_high.price:
        return StructureBreak("BOS", Trend.UP, last_high.price, close_p, is_strong_body)
    if prevailing_trend == Trend.DOWN and last_low and close_p < last_low.price:
        return StructureBreak("BOS", Trend.DOWN, last_low.price, close_p, is_strong_body)
        
    # CHoCH (Trend Dönüşü - Erken Sinyal)
    if prevailing_trend == Trend.UP and last_low and close_p < last_low.price:
        return StructureBreak("CHoCH", Trend.DOWN, last_low.price, close_p, is_strong_body)
    if prevailing_trend == Trend.DOWN and last_high and close_p > last_high.price:
        return StructureBreak("CHoCH", Trend.UP, last_high.price, close_p, is_strong_body)
        
    return None

def check_exhaustion(candles) -> Optional[str]:
    for c in candles[-3:]:
        open_p, high_p, low_p, close_p = c[1], c[2], c[3], c[4]
        body = abs(close_p - open_p)
        body = max(body, close_p * 0.0001) 
        
        upper_wick = high_p - max(open_p, close_p)
        lower_wick = min(open_p, close_p) - low_p
        
        if upper_wick > body * 2.5 and upper_wick > lower_wick * 2:
            return 'TOP_REJECTION'
        if lower_wick > body * 2.5 and lower_wick > upper_wick * 2:
            return 'BOTTOM_REJECTION'
    return None

def volume_anomaly_ratio(candles, baseline_window: int = 20) -> float:
    if len(candles) < baseline_window + 1: return 1.0
    baseline = candles[-(baseline_window + 1) : -1]
    avg_vol = sum(c[5] for c in baseline) / len(baseline) 
    if avg_vol == 0: return 1.0
    return candles[-1][5] / avg_vol

def momentum_roc(candles, periods: int = 5) -> float:
    if len(candles) < periods + 1: return 0.0
    past = candles[-(periods + 1)][4]
    now = candles[-1][4]
    if past == 0: return 0.0
    return (now - past) / past * 100

# 🚀 YENİ EKLENEN: ATR Hesaplayıcı (Dinamik Stop İçin)
def calculate_atr(candles, period: int = 14) -> float:
    if len(candles) < period + 1: return 0.0
    tr_list = []
    for i in range(1, len(candles)):
        high_p = candles[i][2]
        low_p = candles[i][3]
        prev_close = candles[i-1][4]
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
            current_ranks = {}
            
            for symbol in candidate_symbols:
                rv = volume_rank.get(symbol, self.NEUTRAL_RANK)
                rg = gainer_rank.get(symbol, self.NEUTRAL_RANK)
                rl = loser_rank.get(symbol, self.NEUTRAL_RANK)
                best_rank = min(rv, rg, rl)
                
                current_ranks[symbol] = best_rank
                prev_rank = self.previous_ranks.get(symbol)
                velocity = (prev_rank - best_rank) if prev_rank else 0
                is_new = prev_rank is None and best_rank <= 40
                
                if velocity >= 5 or is_new or best_rank <= 20:
                    score = (self.NEUTRAL_RANK - best_rank) + (velocity * 2) + (10 if is_new else 0)
                    scored_candidates.append((symbol, score))
                    
            self.previous_ranks = current_ranks
            scored_candidates.sort(key=lambda x: x[1], reverse=True)
            return [x[0] for x in scored_candidates[:25]]
            
        except Exception as e:
            return []

    async def scan_market(self):
        opportunities = []
        hot_symbols = await self._get_hot_candidates()
        
        for symbol in hot_symbols:
            try:
                tasks = [
                    self.exchange.fetch_ohlcv(symbol, timeframe='4h', limit=50),
                    self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50),
                    self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=50)
                ]
                c_4h, c_1h, c_15m = await asyncio.gather(*tasks)
                
                swings_4h = find_swing_points(c_4h)
                macro_trend = determine_trend(swings_4h)
                
                vol_ratio = volume_anomaly_ratio(c_15m)
                roc = momentum_roc(c_15m)
                if vol_ratio < 1.30: continue 
                
                swings_15m = find_swing_points(c_15m)
                swings_1h = find_swing_points(c_1h)
                trend_1h = determine_trend(swings_1h)
                
                struct_break = detect_structure_break(c_15m, swings_15m, trend_1h)
                exhaustion = check_exhaustion(c_15m)
                
                # 🚀 YENİ: Dinamik ATR Stop ve %1.5 Kesin Sınır Hesaplaması
                current_price = c_15m[-1][4]
                atr = calculate_atr(c_15m)
                
                # LONG için: 2x ATR uzağa koy, ama %1.5'ten daha aşağı inmesine İZİN VERME!
                long_atr_sl = current_price - (atr * 2)
                long_max_sl = current_price * 0.985
                final_long_sl = max(long_atr_sl, long_max_sl) # Hangisi daha güvenliyse onu al (Yüksek olanı)

                # SHORT için: 2x ATR uzağa koy, ama %1.5'ten daha yukarı çıkmasına İZİN VERME!
                short_atr_sl = current_price + (atr * 2)
                short_max_sl = current_price * 1.015
                final_short_sl = min(short_atr_sl, short_max_sl) # Hangisi daha güvenliyse onu al (Düşük olanı)

                target_trend = None
                reason = ""
                sl_price = 0.0

                # 1. SENARYO: ERKEN DÖNÜŞ YAKALAMA (Mikro Makroyu Büküyor)
                if struct_break and struct_break.kind == "CHoCH" and struct_break.is_strong:
                    if struct_break.direction == Trend.UP and roc > 0.1:
                        target_trend = 'long'
                        sl_price = final_long_sl
                        reason = f"Makro yapı ({macro_trend.name}) bağımsız, 15M'de Hacimli (x{vol_ratio:.2f}) ve Güçlü Gövdeli bir Yukarı Dönüş (CHoCH) yakalandı. Erken LONG!"
                    
                    elif struct_break.direction == Trend.DOWN and roc < -0.1:
                        target_trend = 'short'
                        sl_price = final_short_sl
                        reason = f"Makro yapı ({macro_trend.name}) bağımsız, 15M'de Hacimli (x{vol_ratio:.2f}) ve Güçlü Gövdeli bir Aşağı Dönüş (CHoCH) yakalandı. Erken SHORT!"

                # 2. SENARYO: TÜKENİŞ / TEPEDEN-DİPTEN RED YAKALAMA
                elif exhaustion:
                    if exhaustion == 'TOP_REJECTION' and roc < -0.1:
                        target_trend = 'short'
                        sl_price = final_short_sl
                        reason = f"15M grafikte devasa üst fitil (Tepeden Red) oluştu. Alıcılar tükendi, balinalar boşaltıyor. SHORT giriyoruz!"
                    
                    elif exhaustion == 'BOTTOM_REJECTION' and roc > 0.1:
                        target_trend = 'long'
                        sl_price = final_long_sl
                        reason = f"15M grafikte devasa alt fitil (Dipten Red) oluştu. Satıcılar tükendi, balinalar topluyor. LONG giriyoruz!"

                # 3. SENARYO: TREND DEVAMI (BOS)
                elif struct_break and struct_break.kind == "BOS" and struct_break.is_strong:
                    if struct_break.direction == Trend.UP and roc > 0.1:
                        target_trend = 'long'
                        sl_price = final_long_sl
                        reason = f"15M grafikte Güçlü Gövdeli kırılım (BOS) ile trend devam ediyor. Hacim: x{vol_ratio:.2f}. LONG giriyoruz."
                    
                    elif struct_break.direction == Trend.DOWN and roc < -0.1:
                        target_trend = 'short'
                        sl_price = final_short_sl
                        reason = f"15M grafikte Güçlü Gövdeli kırılım (BOS) ile trend devam ediyor. Hacim: x{vol_ratio:.2f}. SHORT giriyoruz."

                if target_trend:
                    opportunities.append({
                        "symbol": symbol,
                        "trend": target_trend,
                        "sl_price": sl_price,
                        "reason": reason
                    })
                    break 

            except Exception as e:
                continue
                
        return opportunities

    async def check_momentum_reversal(self, symbol: str, trend: str) -> bool:
        try:
            candles = await self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=20)
            swings = find_swing_points(candles)
            prevailing = Trend.UP if trend == 'long' else Trend.DOWN
            
            last_close = candles[-1][4]
            last_high = next((s for s in reversed(swings) if s.type == SwingType.HIGH), None)
            last_low = next((s for s in reversed(swings) if s.type == SwingType.LOW), None)

            if prevailing == Trend.UP and last_low and last_close < last_low.price:
                return True 
            if prevailing == Trend.DOWN and last_high and last_close > last_high.price:
                return True 
                
            return False
        except:
            return False
