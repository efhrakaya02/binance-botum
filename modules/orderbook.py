import ccxt.async_support as ccxt
import asyncio

class OrderbookAnalyzer:
    def __init__(self, symbol):
        self.symbol = symbol
        self.exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
        self.MIN_SAFE_DISTANCE_PCT = 0.5 # Duvar veya likidasyon bize %0.5'ten yakınsa GİRME

    async def _get_funding_and_oi(self):
        try:
            # Fonlama oranı ve Açık Pozisyon (OI) verilerini al
            funding = await self.exchange.fetch_funding_rate(self.symbol)
            return funding.get('fundingRate', 0)
        except:
            return 0

    async def check_for_walls_and_sweeps(self, current_price, is_long=True):
        """İşleme girmeden önce Duvar mesafesini ve Likidasyon riskini hesaplar."""
        try:
            # Tahta derinliğini ve Fonlama oranını aynı anda çek
            ob_task = self.exchange.fetch_order_book(self.symbol, limit=100)
            fund_task = self._get_funding_and_oi()
            orderbook, funding_rate = await asyncio.gather(ob_task, fund_task)
            
            # --- 1. LİKİDASYON MIKNATISI KONTROLÜ ---
            # Eğer funding çok yüksek/pozitifse, içerisi Long doludur. Balina aşağı basıp onları patlatmak ister.
            if is_long and funding_rate > 0.005: 
                return False, f"Fonlama oranı çok yüksek (+%{funding_rate*100:.2f}). İçerisi Long dolu, balinaların aşağı yönlü Sweep (Süpürme) operasyonu yapma riski yüksek. Masadan uzak duruyorum."
            elif not is_long and funding_rate < -0.005:
                return False, f"Fonlama oranı çok negatif (-%{abs(funding_rate)*100:.2f}). İçerisi Short dolu, yukarı yönlü sert bir Stop-Hunt gelebilir. Pas geçiyorum."

            # --- 2. DUVAR MESAFE KONTROLÜ ---
            bids, asks = orderbook['bids'], orderbook['asks']
            
            if is_long:
                # Yukarıdaki (Ask) duvarlara bakıyoruz. Bizi aşağı ezecek likidite var mı?
                if not asks: return True, "Tahta temiz."
                avg_qty = sum(ask[1] for ask in asks) / len(asks)
                
                for ask in asks:
                    price, qty = ask[0], ask[1]
                    if qty >= avg_qty * 4: # Ortalamanın 4 katı bir duvar bulundu
                        distance_pct = abs(price - current_price) / current_price * 100
                        if distance_pct < self.MIN_SAFE_DISTANCE_PCT:
                            return False, f"Çok yakında devasa bir satış duvarı var (Mesafe: %{distance_pct:.2f}). Kafamıza balyoz yememek için işlemi iptal ettim."
                        else:
                            break # Duvar var ama uzakta, sorun yok.
                return True, "Yukarı yönlü tahta temiz, bizi ezecek yakın bir balina duvarı yok."

            else:
                # Short için aşağıdaki (Bid) destek duvarlarına bakıyoruz.
                if not bids: return True, "Tahta temiz."
                avg_qty = sum(bid[1] for bid in bids) / len(bids)
                
                for bid in bids:
                    price, qty = bid[0], bid[1]
                    if qty >= avg_qty * 4: # Ortalamanın 4 katı bir duvar bulundu
                        distance_pct = abs(price - current_price) / current_price * 100
                        if distance_pct < self.MIN_SAFE_DISTANCE_PCT:
                            return False, f"Hemen altımızda beton gibi bir alış duvarı var (Mesafe: %{distance_pct:.2f}). Fiyat buraya çarpıp yukarı sekebilir, işlemi iptal ettim."
                        else:
                            break
                return True, "Aşağı yönlü tahta derinliği temiz, fiyatın düşmesini engelleyecek yakın bir duvar yok."

        except Exception as e:
            return False, "Tahta verisi çekilirken hata oluştu, kör uçuş yapmamak için pas geçtim."
        finally:
            await self.exchange.close()
