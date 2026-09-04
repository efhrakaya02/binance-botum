import asyncio
import json
import websockets

class OrderbookAnalyzer:
    def __init__(self, symbol):
        # 'SOXS/USDT:USDT' -> 'SOXS/USDT' -> 'soxsusdt' formatına dönüştürüyoruz
        base_symbol = symbol.split(':')[0]
        self.symbol = base_symbol.replace('/', '').lower()
        self.ws_url = f"wss://fstream.binance.com/ws/{self.symbol}@depth20@100ms"
        
    async def check_for_walls_and_sweeps(self, current_price, is_long=True):
        """
        Canlı emir defterine bağlanıp likidasyon bloklarını (duvarları) analiz eder.
        Eğer duvar çok yakınsa False (İşleme Girme), mesafe varsa True döner.
        """
        try:
            # open_timeout: Bağlantı kurulamazsa 5 saniyede pes et
            async with websockets.connect(self.ws_url, open_timeout=5.0) as ws:
                
                # Verinin gelmesi için maksimum 5 saniye bekle, gelmezse TimeoutError ver
                response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(response)
                
                bids = data.get('b', []) # Alıcılar
                asks = data.get('a', []) # Satıcılar
                
                if not bids or not asks:
                    return False
                
                if is_long:
                    # Long işlem için satıcı duvarlarına (Asks) bakıyoruz
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
                    
                    if distance_pct < 0.5:
                        print(f"⚠️ {self.symbol.upper()} için {wall_price} seviyesinde yakın duvar tespit edildi. Sweep riski! İşlem iptal.")
                        return False
                    else:
                        print(f"✅ {self.symbol.upper()} önü açık. İlk direnç (duvar) %{distance_pct:.2f} uzakta.")
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
                        print(f"⚠️ {self.symbol.upper()} için {wall_price} seviyesinde yakın alıcı duvarı var. İşlem iptal.")
                        return False
                    else:
                        print(f"✅ {self.symbol.upper()} önü açık. İlk destek (duvar) %{distance_pct:.2f} uzakta.")
                        return True

        except asyncio.TimeoutError:
            print(f"⏱️ WebSocket Zaman Aşımı ({self.symbol.upper()}): Veri 5 sn içinde gelmedi, atlanıyor.")
            return False
        except Exception as e:
            print(f"❌ WebSocket Orderbook Hatası ({self.symbol.upper()}): {e}")
            return False
