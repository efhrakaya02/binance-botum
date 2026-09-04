import ccxt.async_support as ccxt

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
        if hasattr(self.config, 'PAPER_TRADING') and self.config.PAPER_TRADING:
            return
        try:
            await self.exchange.set_margin_mode('isolated', symbol)
        except Exception:
            pass
            
        try:
            await self.exchange.set_leverage(self.config.LEVERAGE, symbol)
        except Exception:
            pass

    async def calculate_amount(self, symbol, current_price):
        position_size_usdt = self.config.MARGIN_PER_TRADE_USDT * self.config.LEVERAGE
        amount = position_size_usdt / current_price
        return round(amount, 3) 

    async def open_position(self, symbol, side, current_price):
        amount = await self.calculate_amount(symbol, current_price)
        
        if hasattr(self.config, 'PAPER_TRADING') and self.config.PAPER_TRADING:
            print(f"🛠️ [TEST MODU] Sanal İşlem Açıldı: {symbol} | Yön: {side.upper()} | Miktar: {amount}")
            return {
                "status": "success",
                "entry_price": current_price,
                "amount": amount
            }

        await self.setup_margin_and_leverage(symbol)
        try:
            order = await self.exchange.create_market_order(symbol, side, amount)
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
        if hasattr(self.config, 'PAPER_TRADING') and self.config.PAPER_TRADING:
            print(f"🛠️ [TEST MODU] Sanal İşlem Kapatıldı: {symbol}")
            return True
            
        close_side = 'sell' if side == 'buy' else 'buy'
        try:
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
        await self.exchange.close()
