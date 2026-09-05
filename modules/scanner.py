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
        # RSI hesaplama fonksiyonu (Pandas ile üstel hareketli ortalama kullanarak)
        if len(prices) < period:
            return 50 # Yeterli veri yoksa nötr dön
        delta = pd.Series(prices).diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
        avg_loss = loss.ewm(com=period-1, min_periods=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]

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
            # 🚀 HIZ OPTİMİZASYONU: 4 farklı veriyi aynı anda (paralel) çekiyoruz
            tasks = [
                self.exchange.fetch_ohlcv(symbol, timeframe='4h', limit=5),
                self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=10),
                self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=20),
                self.exchange.fetch_ohlcv(symbol, timeframe='5m', limit=15)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for res in results:
                if isinstance(res, Exception) or not res:
                    return None
                    
            ohlcv_4h, ohlcv_1h, ohlcv_15m, ohlcv_5m = results

            close_4h_current = ohlcv_4h[-1][4]
            close_4h_prev = ohlcv_4h[-2][4]
            
            # 5 Dakikalık Mum Verileri (-2 son kapanan mumdur)
            open_5m = ohlcv_5m[-2][1]
            high_5m = ohlcv_5m[-2][2]
            low_5m = ohlcv_5m[-2][3]
            close_5m = ohlcv_5m[-2][4]
            current_volume = ohlcv_5m[-2][5] 
            
            # ESKİ KURAL 1: Hacim Teyidi
            volumes = [candle[5] for candle in ohlcv_5m[-12:-2]]
            avg_volume = sum(volumes) / len(volumes) if volumes else 0

            if current_volume < (avg_volume * 2.0):
                return None 

            # YENİ KURAL HAZIRLIKLARI: Fitil ve RSI
            body_size = abs(close_5m - open_5m)
            upper_wick = high_5m - max(open_5m, close_5m)
            lower_wick = min(open_5m, close_5m) - low_5m
            
            closes_15m = [candle[4] for candle in ohlcv_15m[:-1]] # Son kapanan muma kadar
            rsi_15m = self._calculate_rsi(closes_15m, 14)

            if close_4h_current > close_4h_prev: # MACRO LONG TREND
                # ESKİ KURAL 2: Mum kırmızıysa girme
                if close_5m <= open_5m: 
                    return None
                    
                # 🚀 YENİ KURAL: RSI Çok Şişmişse (Aşırı Alım) girme
                if rsi_15m > 70:
                    return None
                    
                # 🚀 YENİ KURAL: Üst fitil, gövdenin 1.5 katından büyükse (Satış baskısı yemişse) girme
                if upper_wick > (body_size * 1.5):
                    return None
                    
                return {"symbol": symbol, "trend": "long"}
                
            elif close_4h_current < close_4h_prev: # MACRO SHORT TREND
                # ESKİ KURAL 2: Mum yeşilse girme
                if close_5m >= open_5m: 
                    return None
                    
                # 🚀 YENİ KURAL: RSI Çok Düşmüşse (Aşırı Satım) girme
                if rsi_15m < 30:
                    return None
                    
                # 🚀 YENİ KURAL: Alt fitil, gövdenin 1.5 katından büyükse (Alıcı baskısı yemişse) girme
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
