import asyncio
import datetime
from config import Config
from modules.scanner import MarketScanner
from modules.execution import ExecutionEngine
from modules.risk_manager import RiskManager

class TradingBot:
    def __init__(self):
        self.config = Config()
        self.scanner = MarketScanner(self.config)
        self.execution = ExecutionEngine(self.config)
        self.risk_manager = RiskManager(self.config)
        self.active_positions = {}

    def is_4h_candle_closing(self) -> bool:
        """
        Binance 4H mum kapanışlarından (UTC: 00, 04, 08, 12, 16, 20)
        1 dakika önce (XX:59) tüm işlemleri kapatmak için uyarı verir.
        """
        now = datetime.datetime.now(datetime.UTC)
        if now.hour % 4 == 3 and now.minute >= 59:
            return True
        return False

    async def monitor_positions(self):
        while True:
            # 1. TIME-EXIT (ZAMAN ÇIKIŞI) KONTROLÜ
            if self.is_4h_candle_closing() and len(self.active_positions) > 0:
                print("⏳ [TIME-EXIT] 4H Mum Kapanıyor! Tüm pozisyonlar belirsizlik riskine karşı kapatılıyor...")
                for symbol in list(self.active_positions.keys()):
                    pos = self.active_positions[symbol]
                    await self.execution.close_position(symbol, pos['side'], pos['amount'])
                    print(f"🔒 {symbol} işlemi mum kapanışı nedeniyle süreden sonlandırıldı.")
                    del self.active_positions[symbol]
                
                # Yeni mumun açılmasını bekle (Çift işlem açmamak için)
                await asyncio.sleep(65)
                continue

            # 2. STANDART FİYAT VE HEDEF (SL/TP) KONTROLÜ
            for symbol in list(self.active_positions.keys()):
                pos = self.active_positions[symbol]
                current_price = await self.execution.get_current_price(symbol)
                
                if current_price:
                    exit_check = self.risk_manager.check_exit_conditions(
                        current_price=current_price,
                        entry_price=pos['entry_price'],
                        side=pos['side'],
                        sl_price=pos['sl_price'],
                        tp_price=pos['tp_price']
                    )
                    
                    if exit_check['exit']:
                        print(f"\n{exit_check['reason']}")
                        await self.execution.close_position(symbol, pos['side'], pos['amount'])
                        del self.active_positions[symbol]
                        
            await asyncio.sleep(self.config.MONITOR_INTERVAL)

    async def run(self):
        print("🐊 Timsah Modu Aktif. Soğukkanlı 4H analizleri başlıyor...\n")
        
        # Takip döngüsünü arka planda başlat
        asyncio.create_task(self.monitor_positions())
        
        while True:
            if len(self.active_positions) < self.config.MAX_OPEN_POSITIONS:
                opportunities = await self.scanner.scan_market()
                
                for opp in opportunities:
                    if opp['symbol'] not in self.active_positions and len(self.active_positions) < self.config.MAX_OPEN_POSITIONS:
                        sym = opp['symbol']
                        side = 'buy' if opp['trend'] == 'long' else 'sell'
                        
                        print(f"🎯 Hedef Bulundu: {sym} | Yön: {side.upper()}")
                        print(f"📝 Sebep: {opp['reason']}")
                        
                        order = await self.execution.open_position(sym, side, await self.execution.get_current_price(sym))
                        
                        if order['status'] == 'success':
                            self.active_positions[sym] = {
                                'side': side,
                                'entry_price': order['entry_price'],
                                'amount': order['amount'],
                                'sl_price': opp['sl_price'],
                                'tp_price': opp.get('tp_price', 0) # Scanner'dan gelen TP noktası
                            }
                            print(f"✅ {sym} İşleme Alındı. SL: {opp['sl_price']:.4f} | TP: {opp.get('tp_price', 0):.4f}\n")
                            
            await asyncio.sleep(self.config.SCAN_INTERVAL)

if __name__ == "__main__":
    bot = TradingBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\nBot manuel olarak durduruldu.")
