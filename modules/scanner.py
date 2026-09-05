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

    def _calculate_rsi(self, prices, period=14):
        if len(prices) < period:
            return 50 
        delta = pd.Series(prices).diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
        avg_loss = loss.ewm(com=period-1, min_periods=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]

    def _calculate_adx(self, ohlcv, period=14):
        if len(ohlcv) < period * 2:
            return 0
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['h-l'] = df['high'] - df['low']
        df['h-pc'] = (df['high'] - df['close'].shift(1)).abs()
        df['l-pc'] = (df['low'] - df['close'].shift(1)).abs()
        df['tr'] = df[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        
        df['up_move'] = df['high'] - df['high'].shift(1)
        df['down_move'] = df['low'].shift(1) - df['low']
        
        df['+dm'] = 0.0
        df.loc[(df['up_move'] > df['down_move']) & (df['up_move'] > 0), '+dm'] = df['up_move']
        df['-dm'] = 0.0
        df.loc[(df['down_move'] > df['up_move']) & (df['down_move'] > 0), '-dm'] = df['down_move']
        
        tr_sma = df['tr'].rolling(window=period).mean().replace(0, pd.NA)
        pdm_sma = df['+dm'].rolling(window=period).mean()
        mdm_sma = df['-dm'].rolling(window=period).mean()
        
        pdi = 100 * (pdm_sma / tr_sma)
        mdi = 100 * (mdm_sma / tr_sma)
        dx = 100 * (abs(pdi - mdi) / (pdi + mdi))
        adx = dx.rolling(window=period).mean()
        
        val = adx.iloc[-1]
        return val if not pd.isna(val) else 0

    # 🚀 YENİ: ATR Yüzdesi Hesaplayıcı (Coin ne kadar agresif hareket ediyor?)
    def _calculate_atr_pct(self, ohlcv, period=14):
        if len(ohlcv) < period + 1:
            return 0
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['h-l'] = df['high'] - df['low']
        df['h-pc'] = (df['high'] - df['close'].shift(1)).abs()
        df['l-pc'] = (df['low'] - df['close'].shift(1)).abs()
        df['tr'] = df[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        
        atr = df['tr'].rolling(window=period).mean().iloc[-1]
        current_close = df['close'].iloc[-1]
        
        # ATR'yi fiyata oranlayıp yüzdeye çeviriyoruz
        atr_pct = (atr / current_close) * 100
        return atr_pct if not pd.isna(atr_pct) else 0

    async def get_top_coins(self):
        try:
            tickers = await self.exchange.fetch_tickers()
            usdt_pairs = {k: v for k, v in tickers.items() if ':USDT' in k}
            if not usdt_pairs:
                return []
            
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
        except Exception:
            return []

    async def analyze_trend(self, symbol):
        try:
            tasks = [
                self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=5),
                self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=40),
                self.exchange.fetch_ohlcv(symbol, timeframe='5m', limit=15)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception) or not res:
                    return None
                    
            ohlcv_1h, ohlcv_15m, ohlcv_5m = results

            close_1h_current = ohlcv_1h[-1][4]
            close_1h_prev = ohlcv_1h[-2][4]
            
            open_5m = ohlcv_5m[-2][1]
            high_5m = ohlcv_5m[-2][2]
            low_5m = ohlcv_5m[-2][3]
            close_5m = ohlcv_5m[-2][4]
            current_volume = ohlcv_5m[-2][5] 
            
            volumes = [candle[5] for candle in ohlcv_5m[-12:-2]]
            avg_volume = sum(volumes) / len(volumes) if volumes else 0
            if current_volume < (avg_volume * 2.0):
                return None 

            body_size = abs(close_5m - open_5m)
            upper_wick = high_5m - max(open_5m, close_5m)
            lower_wick = min(open_5m, close_5m) - low_5m
            
            closes_15m = [candle[4] for candle in ohlcv_15m[:-1]] 
            rsi_15m = self._calculate_rsi(closes_15m, 14)
            adx_15m = self._calculate_adx(ohlcv_15m[:-1], 14)
            atr_pct_15m = self._calculate_atr_pct(ohlcv_15m[:-1], 14)

            # 🚀 FİLTRE: Trend Gücü (ADX) 25'in altındaysa pas geç
            if adx_15m < 25:
                return None
                
            # 🚀 FİLTRE: Oynaklık (ATR) %0.8'in altındaysa (Coin hantal/yavaşsa) pas geç!
            # (Bu sayede %1 kâr potansiyeli olmayan uyuşuk coinlere bulaşmayacağız)
            if atr_pct_15m < 0.8:
                return None

            if close_1h_current > close_1h_prev: # MACRO LONG TREND
                if close_5m <= open_5m: 
                    return None
                if rsi_15m > 70:
                    return None
                if upper_wick > (body_size * 1.5):
                    return None
                return {"symbol": symbol, "trend": "long"}
                
            elif close_1h_current < close_1h_prev: # MACRO SHORT TREND
                if close_5m >= open_5m: 
                    return None
                if rsi_15m < 30:
                    return None
                if lower_wick > (body_size * 1.5):
                    return None
                return {"symbol": symbol, "trend": "short"}
            
            return None
        except Exception:
            return None

    async def scan_market(self):
        top_coins = await self.get_top_coins()
        radar_list = []
        if not top_coins:
            return radar_list
        batch_size = 5
        for i in range(0, len(top_coins), batch_size):
            batch = top_coins[i:i+batch_size]
            tasks = [self.analyze_trend(coin) for coin in batch]
            results = await asyncio.gather(*tasks)
            for res in results:
                if res:
                    radar_list.append(res)
            await asyncio.sleep(0.5) 
        return radar_list

    async def close(self):
        await self.exchange.close()
