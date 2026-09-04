import asyncio
import json
import websockets

class OrderbookAnalyzer:
    def __init__(self, symbol):
        # Binance Futures WebSocket için sembolü küçük harfe çevirip formatlıyoruz (örn: btcusdt)
        self.symbol = symbol.replace('/', '').lower()
        self.ws_url = f"wss://fstream.binance.com/ws/{self.symbol}@depth20@100ms"
        
    async def check_for_walls_and_sweeps(self, current_price, is_long=True):
        """
        Canlı emir defterine bağlanıp likidasyon bloklarını (duvarları) analiz eder.
        Eğer duvar çok yakınsa False (İşleme Girme), mesafe varsa True döner.
        """
        try:
            async with websockets.connect(self.ws_url) as ws:
                # Sadece ilk gelen canlı veriyi alıp analizi yapıp çıkıyoruz (hızlı karar için)
                response = await ws.recv()
                data = json.loads(response)
                
                bids = data.get('b', []) # Alıcılar
                asks = data.get('a', []) # Satıcılar
                
                if not bids or not asks:
                    return False
                
                if is_long:
                    # Long işlem için satıcı duvarlarına (Asks) bakıyoruz
                    # İlk 20 kademedeki en büyük duvarı bul
                    max_ask_vol = 0
                    wall_price = 0
                    for ask in asks:
                        price = float(ask[0])
                        volume = float(ask[1])
                        if volume > max_ask_vol:
                            max_ask_vol = volume
                            wall_price = price
                            
                    # Duvarın fiyata olan uzaklığını hesapla (%)
                    distance_pct = ((wall_price - current_price) / current_price) * 100
                    
                    # Eğer %0.5'ten daha yakın bir mesafede devasa bir duvar varsa (Likidasyon bloğu)
                    # Buradan dönebilir, işleme girme!
                    if distance_pct < 0.5:
                        print(f"⚠️ {self.symbol} için {wall_price} seviyesinde yakın duvar tespit edildi. Sweep riski! İşlem iptal.")
                        return False
                    else:
                        print(f"✅ {self.symbol} önü açık. İlk direnç (duvar) %{distance_pct:.2f} uzakta.")
                        return True
                        
                else:
                    # Short işlem için alıcı duvarlarına (Bids) bakıyoruz
                    max_bid_vol = 0
                    wall_price = 0
                    for bid in bids:
                        price = float(bid[0])
                        volume = float(bid[1])
                        if volume > max_bid_vol:
                            max_bid_vol = volume
                            wall_price = price
                            
                    distance_pct = ((current_price - wall_price) / current_price) * 100
                    
                    if distance_pct < 0.5:
                        print(f"⚠️ {self.symbol} için {wall_price} seviyesinde yakın alıcı duvarı var. İşlem iptal.")
                        return False
                    else:
                        return True

        except Exception as e:
            print(f"WebSocket Orderbook Hatası ({self.symbol}): {e}")
            # Hata durumunda güvenliği seçip işlemi reddediyoruz
            return False
