import ccxt.async_support as ccxt
import asyncio

class OrderbookAnalyzer:
    def __init__(self, symbol):
        self.symbol = symbol
        self.exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})

    async def _get_taker_volume(self):
        try:
            trades = await self.exchange.fetch_trades(self.symbol, limit=100)
            taker_buy_vol = sum(t['amount'] * t['price'] for t in trades if t['side'] == 'buy')
            taker_sell_vol = sum(t['amount'] * t['price'] for t in trades if t['side'] == 'sell')
            return taker_buy_vol, taker_sell_vol
        except:
            return 0, 0

    async def check_for_walls_and_sweeps(self, current_price, is_long=True):
        """Tahtayı inceler ve kararla birlikte gerekçesini (metin olarak) döner."""
        try:
            ob_task = self.exchange.fetch_order_book(self.symbol, limit=50)
            taker_task = self._get_taker_volume()
            orderbook, (taker_buy, taker_sell) = await asyncio.gather(ob_task, taker_task)
            
            bids, asks = orderbook['bids'], orderbook['asks']
            threshold = 0.015 
            
            if is_long:
                ask_vol = sum([ask[0] * ask[1] for ask in asks if ask[0] <= current_price * (1 + threshold)])
                bid_vol = sum([bid[0] * bid[1] for bid in bids if bid[0] >= current_price * (1 - threshold)])
                
                if ask_vol > (bid_vol * 3):
                    if taker_buy > (taker_sell * 1.5):
                        return True, "Önümüzde devasa bir satış duvarı (Spoofing) vardı ama Taker alıcılar tahtayı sildiği için duvarı umursamadım."
                    return False, "Tahtada sağlam bir satış duvarı var ve alıcılar zayıf. Ezilmemek için girmedim."
                return True, "Tahta temiz, önümüzde bizi ezecek bir balina satış duvarı yok."

            else:
                bid_vol = sum([bid[0] * bid[1] for bid in bids if bid[0] >= current_price * (1 - threshold)])
                ask_vol = sum([ask[0] * ask[1] for ask in asks if ask[0] <= current_price * (1 + threshold)])
                
                if bid_vol > (ask_vol * 3):
                    if taker_sell > (taker_buy * 1.5):
                        return True, "Alış duvarı var ama Taker satıcılar panikle tahtaya vuruyor, duvarı yıkacaklar."
                    return False, "Aşağıda sağlam bir alış duvarı destek veriyor, short girmek riskli."
                return True, "Aşağı yönlü tahta derinliği temiz, destek duvarı yok."
        except Exception:
            return False, "Tahta verisi çekilirken hata oluştu, güvenli kalmak için pas geçtim."
        finally:
            await self.exchange.close()
