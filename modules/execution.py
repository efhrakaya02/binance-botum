import ccxt.async_support as ccxt
import math

class ExecutionEngine:
    def __init__(self, config):
        self.config = config
        self.exchange = ccxt.binance({
            'apiKey': self.config.BINANCE_API_KEY,
            'secret': self.config.BINANCE_SECRET,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })

    async def setup_margin_and_leverage(self, symbol):
        """İzole marjin ve kaldıracı ayarlar."""
        try:
            # Marjin modunu Isolated (İzole) yap
            await self.exchange.set_margin_mode('isolated', symbol)
        except Exception:
            # Zaten isolated modundaysa API hata dönebilir, sorun değil yoksay
            pass
            
        try:
            # Kaldıracı ayarla (Örn: 5x)
            await self.exchange.set_leverage(self.config.LEVERAGE, symbol)
        except Exception as e:
            print(f"⚠️ Kaldıraç ayarlama uyarısı ({symbol}): {e}")

    async def calculate_amount(self, symbol, current_price):
        """10 USDT ve 5x kaldıraça göre alınacak coin miktarını (size) hesaplar."""
        # Toplam pozisyon büyüklüğü = 10 USDT * 5 = 50 USDT
        position_size_usdt = self.config.MARGIN_PER_TRADE_USDT * self.config.LEVERAGE
        
        # Coin miktarı = 50 / Fiyat
        amount = position_size_usdt / current_price
        
        # Binance miktar ondalık hassasiyeti (Şimdilik 3 hane, API'den dinamik de çekilebilir)
        amount = round(amount, 3) 
        return amount

    async def open_position(self, symbol, side, current_price):
        """Piyasa fiyatından pozisyon açar (Market Order)."""
        await self.setup_margin_and_leverage(symbol)
        amount = await self.calculate_amount(symbol, current_price)
        
        print(f"🚀 İşlem Açılıyor: {symbol} | Yön: {side.upper()} | Miktar: {amount}")
        try:
            # Piyasadan emri gönder
            order = await self.exchange.create_market_order(symbol, side, amount)
            # Emrin gerçekleştiği ortalama fiyatı al (Slippage dahil gerçek maliyet)
            entry_price = order['average'] if 'average' in order and order['average'] else current_price
            return {
                "status": "success",
                "entry_price": entry_price,
                "amount": amount
            }
        except Exception as e:
            print(f"❌ İşlem açılamadı ({symbol}): {e}")
            return {"status": "error"}

    async def close_position(self, symbol, side, amount):
        """Mevcut pozisyonu piyasa fiyatından kapatır (TP/SL)."""
        # Eğer Long (buy) açıksa, Short (sell) order göndererek kapatılır
        close_side = 'sell' if side == 'buy' else 'buy'
        print(f"🛑 İşlem Kapatılıyor: {symbol} | Kâr/Zarar Gerçekleşti.")
        try:
            # Sadece mevcut pozisyonu kapatmak için reduceOnly=True kullanıyoruz
            await self.exchange.create_market_order(
                symbol, 
                close_side, 
                amount, 
                params={"reduceOnly": True}
            )
            return True
        except Exception as e:
            print(f"❌ Kapatma hatası ({symbol}): {e}")
            return False

    async def close(self):
        """Bağlantıyı temizler."""
        await self.exchange.close()
