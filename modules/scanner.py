import ccxt.async_support as ccxt
import asyncio
import pandas as pd

class MarketScanner:
    def __init__(self, config):
        self.config = config
        self.exchange = ccxt.binance({
            'apiKey': self.config.BINANCE_API_KEY,
            'secret': self.config.BINANCE_API_SECRET,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })

    def _analyze_candle_anatomy(self, open_p, high_p, low_p, close_p):
        """Mumun anatomik yapısını (Price Action formasyonunu) belirler."""
        body = abs(close_p - open_p)
        candle_range = high_p - low_p
        
        if candle_range == 0:
            return 'doji'
            
        upper_wick = high_p - max(open_p, close_p)
        lower_wick = min(open_p, close_p) - low_p
        
        body_ratio = body / candle_range

        if body_ratio < 0.1:
            if lower_wick > 2 * upper_wick: return 'hammer' # Çekiç / Pinbar (Boğa)
            if upper_wick > 2 * lower_wick: return 'gravestone' # Mezar Taşı (Ayı)
            return 'doji' # Kararsızlık
            
        if body_ratio > 0.65:
            return 'strong_bullish' if close_p > open_p else 'strong_bearish' # Marubozu / Yutan
            
        if lower_wick > 2 * body and upper_wick < body:
            return 'hammer'
        if upper_wick > 2 * body and lower_wick < body:
            return 'shooting_star' # Kayan Yıldız
            
        return 'neutral'

    async def _get_btc_context(self):
        """0. Aşama: Piyasaya yön veren BTC'nin 4H trendini okur."""
        try:
            ohlcv = await self.exchange.fetch_ohlcv('BTC/USDT', timeframe='4h', limit=10)
            if not ohlcv or len(ohlcv) < 3: return 'neutral'
            
            c_close = ohlcv[-2][4]
            c_open = ohlcv[-2][1]
            return 'bullish' if c_close > c_open else 'bearish'
        except:
            return 'neutral'

    async def get_top_coins(self):
        """Hacimli ve hareketli adayları havuzda toplar."""
        try:
            tickers = await self.exchange.fetch_tickers()
            usdt_pairs = {k: v for k, v in tickers.items() if ':USDT' in k}
            
            data_list = []
            for ticker_info in usdt_pairs.values():
                data_list.append({
                    'symbol': ticker_info.get('symbol', ''),
                    'percentage': ticker_info.get('percentage', 0.0),
                    'quoteVolume': ticker_info.get('quoteVolume', 0.0)
                })
                
            df = pd.DataFrame(data_list)
            df['percentage'] = df['percentage'].fillna(0)
            df['quoteVolume'] = df['quoteVolume'].fillna(0)

            gainers = df.sort_values(by='percentage', ascending=False).head(40)['symbol'].tolist()
            losers = df.sort_values(by='percentage', ascending=True).head(40)['symbol'].tolist()
            volume_leaders = df.sort_values(by='quoteVolume', ascending=False).head(40)['symbol'].tolist()
            return list(set(gainers + losers + volume_leaders))
        except:
            return []

    async def check_volume_breakout(self, symbol):
        """Filtreyi geçen elit coinler için 50 mumluk Hacim Patlaması analizi."""
        try:
            ohlcv_15m = await self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=50)
            if not ohlcv_15m or len(ohlcv_15m) < 50: return False
            
            volumes = [c[5] for c in ohlcv_15m[:-2]]
            avg_volume_50 = sum(volumes) / len(volumes)
            current_volume = ohlcv_15m[-2][5]
            
            # Son mumun hacmi, son 50 mumun ortalamasının en az 2 katı olmalı (Gerçek Kırılım)
            return current_volume > (avg_volume_50 * 2.0)
        except:
            return False

    async def analyze_trend(self, symbol, btc_trend):
        """Çok Katmanlı (4H -> 1H -> 15M -> 5M -> 1M) Anatomi ve PA Analizi"""
        try:
            # --- 1. KATMAN (Makro Trend & API Koruyucu) ---
            ohlcv_4h = await self.exchange.fetch_ohlcv(symbol, timeframe='4h', limit=10)
            ohlcv_1h = await self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=20)
            
            if not ohlcv_4h or not ohlcv_1h: return None
            
            prev_4h_anatomy = self._analyze_candle_anatomy(ohlcv_4h[-2][1], ohlcv_4h[-2][2], ohlcv_4h[-2][3], ohlcv_4h[-2][4])
            curr_4h_is_green = ohlcv_4h[-1][4] > ohlcv_4h[-1][1]
            prev_4h_is_green = ohlcv_4h[-2][4] > ohlcv_4h[-2][1]
            
            prev_1h_anatomy = self._analyze_candle_anatomy(ohlcv_1h[-2][1], ohlcv_1h[-2][2], ohlcv_1h[-2][3], ohlcv_1h[-2][4])
            curr_1h_is_green = ohlcv_1h[-1][4] > ohlcv_1h[-1][1]

            # Makro Uyum Kontrolü (Long Adayı mı, Short Adayı mı?)
            potential_trend = None
            if prev_4h_is_green and curr_4h_is_green and prev_4h_anatomy not in ['gravestone', 'shooting_star']:
                if curr_1h_is_green and prev_1h_anatomy not in ['gravestone', 'shooting_star']:
                    potential_trend = 'long'
                    
            elif not prev_4h_is_green and not curr_4h_is_green and prev_4h_anatomy not in ['hammer']:
                if not curr_1h_is_green and prev_1h_anatomy not in ['hammer']:
                    potential_trend = 'short'
            
            if not potential_trend: return None # Makro trend yoksa hemen çık, API yorma.

            # Korelasyon Kontrolü (BTC düşerken Long aranıyorsa ekstra temkin)
            is_contrarian = (potential_trend == 'long' and btc_trend == 'bearish') or (potential_trend == 'short' and btc_trend == 'bullish')

            # --- 2. KATMAN (Momentum: 15M ve 5M) ---
            ohlcv_15m = await self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=16)
            ohlcv_5m = await self.exchange.fetch_ohlcv(symbol, timeframe='5m', limit=20)
            
            prev_15m_anatomy = self._analyze_candle_anatomy(ohlcv_15m[-2][1], ohlcv_15m[-2][2], ohlcv_15m[-2][3], ohlcv_15m[-2][4])
            prev_5m_anatomy = self._analyze_candle_anatomy(ohlcv_5m[-2][1], ohlcv_5m[-2][2], ohlcv_5m[-2][3], ohlcv_5m[-2][4])
            
            # Momentum Evresi: 5M'de son 3 mum peş peşe doji ise trend yorulmuştur (Exhaustion)
            last_3_5m_anatomies = [self._analyze_candle_anatomy(c[1], c[2], c[3], c[4]) for c in ohlcv_5m[-4:-1]]
            if last_3_5m_anatomies.count('doji') >= 2: return None 

            # --- 3. KATMAN (Hacim Patlaması & 1M Kesin Giriş) ---
            if is_contrarian:
                # BTC'ye ters gidiyorsa hacim kırılımı ZORUNLUDUR!
                has_breakout = await self.check_volume_breakout(symbol)
                if not has_breakout: return None

            ohlcv_1m = await self.exchange.fetch_ohlcv(symbol, timeframe='1m', limit=15)
            curr_1m_is_green = ohlcv_1m[-1][4] > ohlcv_1m[-1][1]
            
            # Swing SL (Dinamik Stop) için 15M yapıları
            df_15 = pd.DataFrame(ohlcv_15m[:-1], columns=['t', 'o', 'h', 'l', 'c', 'v'])
            recent_low = df_15['l'].min()
            recent_high = df_15['h'].max()

            # NİHAİ KARAR MEKANİZMASI
            if potential_trend == 'long':
                if prev_15m_anatomy in ['strong_bullish', 'hammer'] and prev_5m_anatomy not in ['shooting_star', 'gravestone']:
                    if curr_1m_is_green: # Bıçak düşmüyor, yön yukarı döndü
                        return {"symbol": symbol, "trend": "long", "sl_price": recent_low * 0.995}
                        
            elif potential_trend == 'short':
                if prev_15m_anatomy in ['strong_bearish', 'shooting_star'] and prev_5m_anatomy not in ['hammer']:
                    if not curr_1m_is_green: # Fiyat anlık olarak yukarı fırlamıyor
                        return {"symbol": symbol, "trend": "short", "sl_price": recent_high * 1.005}

            return None
        except Exception as e:
            return None

    async def scan_market(self):
        btc_trend = await self._get_btc_context()
        top_coins = await self.get_top_coins()
        
        radar_list = []
        if not top_coins: return radar_list
        
        batch_size = 5
        for i in range(0, len(top_coins), batch_size):
            batch = top_coins[i:i+batch_size]
            tasks = [self.analyze_trend(coin, btc_trend) for coin in batch]
            results = await asyncio.gather(*tasks)
            for res in results:
                if res:
                    radar_list.append(res)
            await asyncio.sleep(0.3) # API Rate Limit koruması
            
        return radar_list

    async def check_momentum_reversal(self, symbol, trade_type):
        """Açık işlemler için 15M'de ani geri dönüş (Çekiç/Kayan Yıldız) kontrolü."""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=3)
            anatomy = self._analyze_candle_anatomy(ohlcv[-2][1], ohlcv[-2][2], ohlcv[-2][3], ohlcv[-2][4])
            
            if trade_type == 'long' and anatomy in ['shooting_star', 'gravestone', 'strong_bearish']: return True
            if trade_type == 'short' and anatomy in ['hammer', 'strong_bullish']: return True
            
            return False
        except:
            return False

    async def close(self):
        await self.exchange.close()
