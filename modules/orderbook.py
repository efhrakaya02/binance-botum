import asyncio
import json
import websockets

class OrderbookAnalyzer:
    def __init__(self, symbol):
        base_symbol = symbol.split(':')[0]
        self.symbol = base_symbol.replace('/', '').lower()
        self.ws_url = f"wss://fstream.binance.com/ws/{self.symbol}@depth20@100ms"
        
    async def check_for_walls_and_sweeps(self, current_price, is_long=True):
        try:
            async with websockets.connect(self.ws_url, open_timeout=5.0) as ws:
                response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(response)
                
                bids = data.get('b', []) 
                asks = data.get('a', []) 
                
                if not bids or not asks:
                    return False
                
                if is_long:
                    max_ask_vol = 0
                    wall_price = 0
                    for ask in asks:
                        price = float(ask[0])
                        volume = float(ask[1])
                        if volume > max_ask_vol:
                            max_ask_vol = volume
                            wall_price = price
                            
                    distance_pct = ((wall_price - current_price) / current_price) * 100
                    if distance_pct < 0.5:
                        return False
                    else:
                        return True
                else:
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
                        return False
                    else:
                        return True
        except Exception:
            return False
