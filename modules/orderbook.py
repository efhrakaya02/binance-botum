import ccxt.async_support as ccxt
import asyncio

class OrderbookAnalyzer:
    def __init__(self, symbol):
        self.symbol = symbol
        # Sadece public (herkese açık) verileri çekeceğimiz için API key'e gerek yok
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })

    async def _get_taker_volume(self):
        """Son 100 işlemi çekerek 'Agresif Alıcı' (Taker Buy) ve 'Agresif Satıcı' (Taker Sell) hacmini ölçer."""
        try:
            trades = await self.exchange.fetch_trades(self.symbol, limit=100)
            taker_buy_vol = 0
            taker_sell_vol = 0
            
            for trade in trades:
                # 'side' parametresi işlemi marketten vuran (Taker) tarafı gösterir
                if trade['side'] == 'buy':
                    taker_buy_vol += trade['amount'] * trade['price']
                elif trade['side'] == 'sell':
                    taker_sell_vol += trade['amount'] * trade['price']
                    
            return taker_buy_vol, taker_sell_vol
        except Exception:
            return 0, 0

    async def check_for_walls_and_sweeps(self, current_price, is_long=True):
        """Emir defteri duvarlarını ve Spoofing (Sahte Duvar) manipülasyonunu analiz eder."""
        try:
            # Zaman kaybetmemek için Emir Defterini ve Taker İşlemlerini aynı anda (async) çekiyoruz
            ob_task = self.exchange.fetch_order_book(self.symbol, limit=50)
            taker_task = self._get_taker_volume()
            
            orderbook, (taker_buy, taker_sell) = await asyncio.gather(ob_task, taker_task)
            
            bids = orderbook['bids'] # Alış emirleri
            asks = orderbook['asks'] # Satış emirleri
            
            # Hedefimiz %3 olduğu için, fiyatın %1.5 uzağına kadar olan derinliğe bakıyoruz
            threshold = 0.015 
            
            if is_long:
                relevant_asks = [ask for ask in asks if ask[0] <= current_price * (1 + threshold)]
                ask_vol = sum([ask[0] * ask[1] for ask in relevant_asks])
                
                relevant_bids = [bid for bid in bids if bid[0] >= current_price * (1 - threshold)]
                bid_vol = sum([bid[0] * bid[1] for bid in relevant_bids])
                
                # Önümüzde bizi engelleyecek devasa bir satış duvarı var mı? (Alışların 3 katından fazlaysa)
                is_wall_blocking = ask_vol > (bid_vol * 3)
                
                if is_wall_blocking:
                    # 🚀 SPOOFING FİLTRESİ: Duvar var ama agresif alıcılar satıcıların 1.5 katıysa, o duvarı yıkarlar!
                    if taker_buy > (taker_sell * 1.5):
                        return True # Duvar manipülasyon amaçlı veya alıcılar çok güçlü, YOL AÇIK!
                    return False # Duvar gerçek ve alıcılar zayıf, GERİ ÇEKİL.
                return True # Duvar yok, YOL AÇIK.

            else: # SHORT Senaryosu
                relevant_bids = [bid for bid in bids if bid[0] >= current_price * (1 - threshold)]
                bid_vol = sum([bid[0] * bid[1] for bid in relevant_bids])
                
                relevant_asks = [ask for ask in asks if ask[0] <= current_price * (1 + threshold)]
                ask_vol = sum([ask[0] * ask[1] for ask in relevant_asks])
                
                # Önümüzde fiyatı destekleyecek devasa bir alış duvarı var mı?
                is_wall_blocking = bid_vol > (ask_vol * 3)
                
                if is_wall_blocking:
                    # 🚀 SPOOFING FİLTRESİ: Duvar var ama satıcılar (Taker Sell) panikle çok sert vuruyorsa!
                    if taker_sell > (taker_buy * 1.5):
                        return True # Duvarı kıracaklar, YOL AÇIK!
                    return False # Duvar sağlam, GERİ ÇEKİL.
                return True # Duvar yok, YOL AÇIK.

        except Exception as e:
            # Herhangi bir API hatasında sermayeyi korumak için "Güvensiz" yanıtı dön
            return False
        finally:
            await self.exchange.close()
