import asyncio
import time
from config import Config
from modules.scanner import MarketScanner
from modules.orderbook import OrderbookAnalyzer
from modules.risk_manager import RiskManager
from modules.execution import ExecutionEngine

class TradingBot:
    def __init__(self):
        self.config = Config()
        self.scanner = MarketScanner(self.config)
        self.risk_manager = RiskManager(self.config)
        self.execution = ExecutionEngine(self.config)
        self.active_trades = {}  # Aktif işlemleri takip edeceğimiz sözlük
        self.max_active_trades = getattr(Config, 'MAX_OPEN_POSITIONS', 3)

    async def manage_active_trades(self):
        """Aktif işlemleri yönetir: SL/TP, Dinamik Kâr Kilidi, Zaman Aşımı ve Geri Dönüşleri kontrol eder."""
        current_time = time.time()
        
        for symbol, trade in list(self.active_trades.items()):
            try:
                current_price = await self.execution.get_current_price(symbol)
                if not current_price:
                    continue
                    
                is_long = trade['trend'] == 'long'
                
                # Zirve/Dip fiyatı güncelle
                if is_long and current_price > trade['max_reached_price']:
                    trade['max_reached_price'] = current_price
                elif not is_long and current_price < trade['max_reached_price']:
                    trade['max_reached_price'] = current_price

                # 1. 60 DAKİKA ZAMAN AŞIMI (Time Stop) Kontrolü
                trade_duration = (current_time - trade['start_time']) / 60
                if trade_duration >= 60:
                    profit_pct = ((current_price - trade['entry_price']) / trade['entry_price']) * 100 if is_long else ((trade['entry_price'] - current_price) / trade['entry_price']) * 100
                    if profit_pct < 1.0: # 60 dakika geçmiş ve %1 kâr bile yapamamışsa kes!
                        print(f"[{symbol}] Zaman Aşımı (60dk). İşlem hacimsiz, kapatılıyor. PnL: %{profit_pct:.2f}")
                        await self.execution.close_position(symbol, trade['side'], trade['amount'])
                        del self.active_trades[symbol]
                        continue

                # 2. 15M ANİ GERİ DÖNÜŞ (Reversal) Kontrolü
                is_reversing = await self.scanner.check_momentum_reversal(symbol, trade['trend'])
                if is_reversing:
                    print(f"[{symbol}] 15M Grafikte Sert Ters Mum (Çekiç/Mezar Taşı) tespit edildi. Güvenlik çıkışı yapılıyor!")
                    await self.execution.close_position(symbol, trade['side'], trade['amount'])
                    del self.active_trades[symbol]
                    continue

                # 3. DİNAMİK STOP LOSS ve KÂR KİLİDİ KONTROLÜ
                current_sl = self.risk_manager.calculate_stop_loss(
                    entry_price=trade['entry_price'],
                    max_reached_price=trade['max_reached_price'],
                    is_long=is_long,
                    initial_sl=trade['initial_sl']
                )

                # TP (Take Profit - Hard %3) veya SL/Kilit Tetiklenmesi
                if is_long:
                    if current_price >= trade['entry_price'] * 1.03: # %3 Hedef
                        print(f"[{symbol}] HEDEF VURULDU (+%3.00). Kâr alındı!")
                        await self.execution.close_position(symbol, trade['side'], trade['amount'])
                        del self.active_trades[symbol]
                    elif current_price <= current_sl:
                        print(f"[{symbol}] Stop-Loss / Kâr Kilidi tetiklendi. Çıkış yapıldı.")
                        await self.execution.close_position(symbol, trade['side'], trade['amount'])
                        del self.active_trades[symbol]
                else: # Short
                    if current_price <= trade['entry_price'] * 0.97: # %3 Hedef
                        print(f"[{symbol}] HEDEF VURULDU (+%3.00). Kâr alındı!")
                        await self.execution.close_position(symbol, trade['side'], trade['amount'])
                        del self.active_trades[symbol]
                    elif current_price >= current_sl:
                        print(f"[{symbol}] Stop-Loss / Kâr Kilidi tetiklendi. Çıkış yapıldı.")
                        await self.execution.close_position(symbol, trade['side'], trade['amount'])
                        del self.active_trades[symbol]

            except Exception as e:
                print(f"Aktif işlem yönetilirken hata ({symbol}): {e}")

    async def run(self):
        print("🚀 Kurumsal Price Action Botu Başlatıldı...")
        
        while True:
            try:
                # 1. Aşama: Mevcut işlemleri kontrol et
                await self.manage_active_trades()

                # 2. Aşama: Kapasite varsa yeni fırsat tara
                if len(self.active_trades) < self.max_active_trades:
                    print(f"🔍 Yeni fırsatlar taranıyor... (Aktif: {len(self.active_trades)}/{self.max_active_trades})")
                    
                    # Scanner (4H -> 1H -> 15M -> 5M -> Hacim Kırılımı)
                    opportunities = await self.scanner.scan_market()
                    
                    for opp in opportunities:
                        symbol = opp['symbol']
                        trend = opp['trend']
                        initial_sl = opp['sl_price']
                        is_long = trend == 'long'
                        
                        if symbol in self.active_trades:
                            continue # Zaten içerideyiz
                            
                        # 3. Aşama: Emir Defteri ve Taker Hacim (Spoofing) Kontrolü
                        current_price = await self.execution.get_current_price(symbol)
                        if not current_price:
                            continue
                            
                        ob_analyzer = OrderbookAnalyzer(symbol)
                        is_path_clear = await ob_analyzer.check_for_walls_and_sweeps(current_price, is_long)
                        
                        if is_path_clear:
                            print(f"✅ [{symbol}] Tüm PA filtreleri ve Tahta analizi geçildi. {trend.upper()} giriliyor!")
                            
                            side = 'buy' if is_long else 'sell'
                            execution_result = await self.execution.open_position(symbol, side, current_price)
                            
                            if execution_result and execution_result.get("status") == "success":
                                # İşlemi takibe al
                                self.active_trades[symbol] = {
                                    'trend': trend,
                                    'side': side,
                                    'amount': execution_result['amount'],
                                    'entry_price': execution_result['entry_price'],
                                    'max_reached_price': execution_result['entry_price'],
                                    'initial_sl': initial_sl,
                                    'start_time': time.time()
                                }
                                
                                if len(self.active_trades) >= self.max_active_trades:
                                    break # Kapasite doldu, taramayı durdur
                        else:
                            print(f"⚠️ [{symbol}] Kurumsal onaylardan geçti fakat Tahtada manipülasyon (Spoofing) veya Balina Duvarı tespit edildi. Pas geçiliyor.")

                await asyncio.sleep(10) # Döngüyü çok hızlı çalıştırıp sistemi yormamak için
                
            except Exception as e:
                print(f"Ana döngüde hata: {e}")
                await asyncio.sleep(10)

    async def shutdown(self):
        await self.scanner.close()
        await self.execution.close()

if __name__ == "__main__":
    bot = TradingBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\nBot manuel olarak durduruldu.")
        asyncio.run(bot.shutdown())
