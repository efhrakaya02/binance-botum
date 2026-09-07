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

    def _calculate_average_range(self, ohlcv, period=14):
        if len(ohlcv) < period:
            return 0
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['range_pct'] = ((df['high'] - df['low']) / df['low']) * 100
        return df['range_pct'].tail(period).mean()

    def _get_buying_selling_pressure(self, ohlcv, period=10):
        if len(ohlcv) < period:
            return 0, 0
        df = pd.DataFrame(ohlcv[-period:], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        bullish_bodies = (df[df['close'] > df['open']]['close'] - df[df['close'] > df['open']]['open']).sum()
        bearish_bodies = (df[df['open'] > df['close']]['open'] - df[df['open'] > df['close']]['close']).sum()
        return bullish_bodies, bearish_bodies

    def _get_market_structure(self, ohlcv, lookback=10):
        if len(ohlcv) < lookback + 1:
            return 0, 0
        df = pd.DataFrame(ohlcv[-(lookback+1):-1], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        recent_high = df['high'].max()
        recent_low = df['low'].min()
        return recent_high, recent_low

    # 🚀 YENİ İLAVE 1: Bitcoin (BTC) Yön Teyidi
    async def _get_btc_trend(self):
        try:
            ohlcv = await self.exchange.fetch_ohlcv('BTC/USDT', timeframe='15m', limit=5)
            c_close = ohlcv[-2][4]
            c_open = ohlcv[-2][1]
            return 'bullish' if c_close > c_open else 'bearish'
        except:
            return 'neutral'

    async def check_momentum_reversal(self, symbol, trade_type):
        try:
            ohlcv_5m = await self.exchange.fetch_ohlcv(symbol, timeframe='5m', limit=5)
            if not ohlcv_5m or len(ohlcv_5m) < 3:
                return False
            c1_open, c1_high, c1_low, c1_close = ohlcv_5m[-2][1], ohlcv_5m[-2][2], ohlcv_5m[-2][3], ohlcv_5m[-2][4]
            c2_open, c2_high, c2_low, c2_close = ohlcv_5m[-3][1], ohlcv_5m[-3][2], ohlcv_5m[-3][3], ohlcv_5m[-3][4]

            if trade_type == 'long':
                if (c1_close < c1_open) and (c1_close < c2_low): return True
            elif trade_type == 'short':
                if (c1_close > c1_open) and (c1_close > c2_high): return True
            return False
        except:
            return False

    async def get_top_coins(self):
        try:
            tickers = await self.exchange.fetch_tickers()
            usdt_pairs = {k: v for k, v in tickers.items() if ':USDT' in k}
            if not usdt_pairs: return []
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
            gainers = df.sort_values(by='percentage', ascending=False).head(50)['symbol'].tolist()
            losers = df.sort_values(by='percentage', ascending=True).head(50)['symbol'].tolist()
            volume_leaders = df.sort_values(by='quoteVolume', ascending=False).head(50)['symbol'].tolist()
            return list(set(gainers + losers + volume_leaders))
        except:
            return []

    async def analyze_trend(self, symbol, btc_trend):
        try:
            tasks = [
                self.exchange.fetch_ohlcv(symbol, timeframe='4h', limit=5),
                self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=20),
                self.exchange.fetch_ohlcv(symbol, timeframe='5m', limit=15),
                self.exchange.fetch_ohlcv(symbol, timeframe='1m', limit=5)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception) or not res or len(res) < 5: return None
                    
            ohlcv_4h, ohlcv_15m, ohlcv_5m, ohlcv_1m = results
            
            close_4h_prev = ohlcv_4h[-2][4]
            open_4h_prev = ohlcv_4h[-2][1]
            is_4h_bullish = close_4h_prev > open_4h_prev 
            is_4h_bearish = close_4h_prev < open_4h_prev 
            
            avg_range = self._calculate_average_range(ohlcv_15m[:-1], 10)
            if avg_range < 0.35: return None
                
            recent_high, recent_low = self._get_market_structure(ohlcv_15m, 10)
            momentum_ivme_pct = ((recent_high - recent_low) / recent_low) * 100
            
            if momentum_ivme_pct < 1.2: return None
                
            curr_price_15m = ohlcv_15m[-2][4] 
            bull_pressure, bear_pressure = self._get_buying_selling_pressure(ohlcv_15m[:-1], 7)

            open_5m = ohlcv_5m[-2][1]
            high_5m = ohlcv_5m[-2][2]
            low_5m = ohlcv_5m[-2][3]
            close_5m = ohlcv_5m[-2][4]
            current_volume = ohlcv_5m[-2][5] 
            
            volumes = [candle[5] for candle in ohlcv_5m[-12:-2]]
            avg_volume = sum(volumes) / len(volumes) if volumes else 0
            
            candle_size = high_5m - low_5m
            if candle_size == 0: return None
            body_size = abs(close_5m - open_5m)
            body_ratio = body_size / candle_size 
            
            open_1m = ohlcv_1m[-2][1]
            close_1m = ohlcv_1m[-2][4]

            # 🚀 LONG SENARYOSU (BTC Teyidi Eklendi)
            if btc_trend in ['bullish', 'neutral']:
                if is_4h_bullish: 
                    if curr_price_15m >= (recent_high * 0.990) and bull_pressure > (bear_pressure * 1.2): 
                        if close_5m > open_5m and body_ratio > 0.50 and current_volume > (avg_volume * 1.2): 
                            if close_1m > open_1m: 
                                # 🚀 YENİ İLAVE 2: Dinamik Stop Hesaplaması (Dibin %0.5 altı)
                                sl_price = recent_low * 0.995 
                                return {"symbol": symbol, "trend": "long", "sl_price": sl_price}
                            
            # 🚀 SHORT SENARYOSU (BTC Teyidi Eklendi)
            if btc_trend in ['bearish', 'neutral']:
                if is_4h_bearish: 
                    if curr_price_15m <= (recent_low * 1.010) and bear_pressure > (bull_pressure * 1.2): 
                        if close_5m < open_5m and body_ratio > 0.50 and current_volume > (avg_volume * 1.2): 
                            if close_1m < open_1m: 
                                # 🚀 YENİ İLAVE 2: Dinamik Stop Hesaplaması (Zirvenin %0.5 üstü)
                                sl_price = recent_high * 1.005 
                                return {"symbol": symbol, "trend": "short", "sl_price": sl_price}
            
            return None
        except Exception:
            return None

    async def scan_market(self):
        # İşlem taramadan önce BTC'nin anlık yönünü alır
        btc_trend = await self._get_btc_trend()
        
        top_coins = await self.get_top_coins()
        radar_list = []
        if not top_coins:
            return radar_list
        batch_size = 5
        for i in range(0, len(top_coins), batch_size):
            batch = top_coins[i:i+batch_size]
            tasks = [self.analyze_trend(coin, btc_trend) for coin in batch]
            results = await asyncio.gather(*tasks)
            for res in results:
                if res:
                    radar_list.append(res)
            await asyncio.sleep(0.5) 
        return radar_list

    async def close(self):
        await self.exchange.close()
