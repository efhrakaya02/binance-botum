import ccxt.async_support as ccxt
import asyncio
import pandas as pd

class MarketScanner:
    def __init__(self, config):
        self.config = config
        # Binance Futures API bağlantısı
        self.exchange = ccxt.binance({
            'apiKey': self.config.BINANCE_API_KEY,
            'secret': self.config.BINANCE_API_SECRET,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })

    async def get_top_coins(self):
        """Gainer, Loser ve 24h Volume listelerinin ilk 50'sini çeker."""
        try:
            tickers = await self.exchange.fetch_tickers()
            
            # Sadece vadeli işlemlerdeki USDT paritelerini al
            usdt_pairs = {k: v for k, v in tickers.items() if k.endswith('/USDT')}
            
            df = pd.DataFrame(usdt_pairs.values())
            df = df[['symbol', 'percentage', 'quoteVolume']].dropna()

            # İlk 50 Gainer, Loser ve Volume
            gainers = df.sort_values(by='percentage', ascending=False).head(50)['symbol'].tolist()
            losers = df.sort_values(by='percentage', ascending=True).head(50)['symbol'].tolist()
            volume_leaders = df.sort_values(by='quoteVolume', ascending=False).head(50)['symbol'].tolist()

            # Aynı coinler birden fazla listede olabileceği için benzersiz (unique) bir havuz oluşturuyoruz
            combined_list = list(set(gainers + losers + volume_leaders))
            return combined_list
        except Exception as e:
            print(f"Veri çekme hatası: {e}")
            return []

    async def analyze_trend(self, symbol):
        """
        4h ve 1h mumlarını çekerek fiyat hareketinin (PA) ilk safhasında olup olmadığını kontrol eder.
        """
        try:
            # 4h ve 1h verilerini kısıtlı çekiyoruz (API limitine takılmamak için)
            ohlcv_4h = await self.exchange.fetch_ohlcv(symbol, timeframe='4h', limit=5)
            ohlcv_1h = await self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=10)
            
            if not ohlcv_4h or not ohlcv_1h:
                return None

            close_4h_current = ohlcv_4h[-1][4]
            close_4h_prev = ohlcv_4h[-2][4]
            
            # Burada makro yön belirlenir (İleride PA ve hacim filtreleri buraya eklenecek)
            # Şimdilik hareketin yeni başladığını tespit etmek için basit bir ivme kontrolü
            if close_4h_current > close_4h_prev:
                return {"symbol": symbol, "trend": "long"}
            elif close_4h_current < close_4h_prev:
                return {"symbol": symbol, "trend": "short"}
            
            return None
        except Exception:
            return None

    async def scan_market(self):
        """Tüm havuzu tarar ve mikro analiz (orderbook) için 'Radar' listesini döner."""
        print("🔍 Piyasa taranıyor... (Gainer, Loser, Vol Top 50)")
        top_coins = await self.get_top_coins()
        
        radar_list = []
        # Rate-limit (API Ban) yememek için 5'li paketler halinde asenkron sorgu atıyoruz
        batch_size = 5
        for i in range(0, len(top_coins), batch_size):
            batch = top_coins[i:i+batch_size]
            tasks = [self.analyze_trend(coin) for coin in batch]
            results = await asyncio.gather(*tasks)
            
            for res in results:
                if res:
                    radar_list.append(res)
            
            await asyncio.sleep(0.5) # Binance'i yormamak için mikro bekleme

        print(f"✅ Tarama bitti. Harekete hazırlanan coin sayısı: {len(radar_list)}")
        return radar_list

    async def close(self):
        # İşlem bitince bağlantıyı güvenli kapat
        await self.exchange.close()
