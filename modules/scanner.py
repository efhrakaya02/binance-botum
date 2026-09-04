import ccxt.async_support as ccxt
import asyncio
import pandas as pd

class MarketScanner:
    def __init__(self, config):
        self.config = config
        self.exchange = ccxt.binance({
            'apiKey': self.config.BINANCE_API_KEY,
            'secret': self.config.BINANCE_SECRET,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })

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
            ohlcv_4h = await self.exchange.fetch_ohlcv(symbol, timeframe='4h', limit=5)
            ohlcv_1h = await self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=10)
            
            if not ohlcv_4h or not ohlcv_1h:
                return None

            close_4h_current = ohlcv_4h[-1][4]
            close_4h_prev = ohlcv_4h[-2][4]
            
            if close_4h_current > close_4h_prev:
                return {"symbol": symbol, "trend": "long"}
            elif close_4h_current < close_4h_prev:
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
