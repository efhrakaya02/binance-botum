import asyncio
import time
from config import Config
from modules.scanner import MarketScanner
from modules.orderbook import OrderbookAnalyzer
from modules.risk_manager import RiskManager
from modules.execution import ExecutionEngine
from modules.state_manager import StateManager

class TradingBot:
    def __init__(self):
        self.config = Config()
        self.scanner = MarketScanner(self.config)
        self.risk_manager = RiskManager(self.config)
        self.execution = ExecutionEngine(self.config)
        self.state_manager = StateManager()
        self.active_trades = {}
        self.max_active_trades = getattr(Config, 'MAX_OPEN_POSITIONS', 3)

    async def fast_price_monitor(self):
        """1 SANİYELİK DÖNGÜ: Anlık fiyat takibi ve Stop-Loss/Take-Profit tetikleyicisi."""
        while True:
            for symbol, trade in list(self.active_trades.items()):
                try:
                    current_price = await self.execution.get_current_price(symbol)
                    if not current_price: continue
                        
                    is_long = trade['trend'] == 'long'
                    
                    if is_long and current_price > trade['max_reached_price']:
                        trade['max_reached_price'] = current_price
                    elif not is_long and current_price < trade['max_reached_price']:
                        trade['max_reached_price'] = current_price

                    current_sl = self.risk_manager.calculate_stop_loss(
                        entry_price=trade['entry_price'], max_reached_price=trade['max_reached_price'],
                        is_long=is_long, initial_sl=trade['initial_sl']
                    )

                    profit_pct = ((current_price - trade['entry_price']) / trade['entry_price']) * 100 if is_long else ((trade['entry_price'] - current_price) / trade['entry_price']) * 100
                    trade['current_sl'] = current_sl # Rapor için kaydet

                    closed = False
                    if is_long:
                        if profit_pct >= self.config.HARD_TP_PCT:
                            print(f"\n✅ [KÂR ALINDI] Patron, {symbol} hedefimize ulaştı! %{profit_pct:.2f} kârı kasaya koydum, masadan kalkıyoruz.")
                            await self.execution.close_position(symbol, trade['side'], trade['amount'])
                            self.state_manager.record_closed_trade(symbol, profit_pct, "Take Profit")
                            closed = True
                        elif current_price <= current_sl:
                            print(f"\n🚨 [STOP-LOSS] Bana kızma patron ama {symbol} işleminde piyasa aniden tersine döndü. Kırmızı çizgimizi delmesine izin vermeden zararı %{profit_pct:.2f} seviyesinde acımasızca kestim. Sermayeyi koruduk.")
                            await self.execution.close_position(symbol, trade['side'], trade['amount'])
                            self.state_manager.record_closed_trade(symbol, profit_pct, "Stop Loss")
                            closed = True
                    else: # Short için
                        if profit_pct >= self.config.HARD_TP_PCT:
                            print(f"\n✅ [KÂR ALINDI] Patron, {symbol} hedefimize ulaştı! %{profit_pct:.2f} kârı kasaya koydum.")
                            await self.execution.close_position(symbol, trade['side'], trade['amount'])
                            self.state_manager.record_closed_trade(symbol, profit_pct, "Take Profit")
                            closed = True
                        elif current_price >= current_sl:
                            print(f"\n🚨 [STOP-LOSS] Patron, {symbol} beklediğimiz gibi gitmedi. Anaparayı korumak için zararı %{profit_pct:.2f} seviyesinde kestim.")
                            await self.execution.close_position(symbol, trade['side'], trade['amount'])
                            self.state_manager.record_closed_trade(symbol, profit_pct, "Stop Loss")
                            closed = True

                    if closed:
                        del self.active_trades[symbol]

                except Exception as e:
                    pass
            await asyncio.sleep(self.config.MONITOR_INTERVAL) 

    async def slow_trade_manager(self):
        """1 DAKİKALIK DÖNGÜ: Detaylı takip raporu ve API gerektiren 15M/Zaman çıkışları."""
        while True:
            current_time = time.time()
            for symbol, trade in list(self.active_trades.items()):
                if symbol not in self.active_trades: continue
                try:
                    current_price = await self.execution.get_current_price(symbol)
                    if not current_price: continue
                    
                    is_long = trade['trend'] == 'long'
                    profit_pct = ((current_price - trade['entry_price']) / trade['entry_price']) * 100 if is_long else ((trade['entry_price'] - current_price) / trade['entry_price']) * 100
                    max_profit_pct = ((trade['max_reached_price'] - trade['entry_price']) / trade['entry_price']) * 100 if is_long else ((trade['entry_price'] - trade['max_reached_price']) / trade['entry_price']) * 100
                    trade_duration_mins = (current_time - trade['start_time']) / 60
                    target_tp = trade['entry_price'] * 1.03 if is_long else trade['entry_price'] * 0.97
                    current_sl = trade.get('current_sl', trade['initial_sl'])

                    # DAKİKALIK TAKİP RAPORU
                    if current_time - trade.get('last_log_time', 0) >= 60:
                        print(f"📊 [TAKİP] {symbol} ({trade['trend'].upper()}) | Süre: {trade_duration_mins:.1f}dk | Giriş: {trade['entry_price']:.4f} | Anlık: {current_price:.4f} | Hedef: {target_tp:.4f} | SL: {current_sl:.4f}")
                        print(f"   => Anlık PnL: %{profit_pct:.2f} (Görülen Zirve PnL: %{max_profit_pct:.2f})")
                        trade['last_log_time'] = current_time

                    # 60 Dk Zaman Aşımı
                    if trade_duration_mins >= 60 and profit_pct < 1.0:
                        print(f"\n⏳ [ZAMAN AŞIMI] Patron, {symbol} tam 60 dakikadır yataya bağladı. Paramızı içeride esir edemem, %{profit_pct:.2f} ile masadan kalktım.")
                        await self.execution.close_position(symbol, trade['side'], trade['amount'])
                        self.state_manager.record_closed_trade(symbol, profit_pct, "Zaman Aşımı")
                        del self.active_trades[symbol]
                        continue

                    # 15M Ters Mum
                    is_reversing = await self.scanner.check_momentum_reversal(symbol, trade['trend'])
                    if is_reversing:
                        print(f"\n⚠️ [TEHLİKE SEZİLDİ] {symbol} 15M grafiğinde ters yönlü sert bir mum sezdim. Güvenlik protokolünü devreye sokup işlemi %{profit_pct:.2f} PnL ile kapattım.")
                        await self.execution.close_position(symbol, trade['side'], trade['amount'])
                        self.state_manager.record_closed_trade(symbol, profit_pct, "15M Ters Mum")
                        del self.active_trades[symbol]
                        
                except Exception as e:
                    pass
            await asyncio.sleep(60) 

    async def market_scanner(self):
        """5 DAKİKALIK DÖNGÜ: Piyasayı tarar ve fırsat kollar."""
        while True:
            try:
                if len(self.active_trades) < self.max_active_trades:
                    print(f"🔍 Yeni fırsatlar taranıyor... (Kapasite: {len(self.active_trades)}/{self.max_active_trades})")
                    opportunities = await self.scanner.scan_market()
                    
                    for opp in opportunities:
                        symbol = opp['symbol']
                        trend = opp['trend']
                        initial_sl = opp['sl_price']
                        pa_reason = opp.get('reason', 'Güçlü Price Action formasyonu.')
                        is_long = trend == 'long'
                        
                        if symbol in self.active_trades: continue
                            
                        current_price = await self.execution.get_current_price(symbol)
                        if not current_price: continue
                            
                        ob_analyzer = OrderbookAnalyzer(symbol)
                        is_path_clear, ob_reason = await ob_analyzer.check_for_walls_and_sweeps(current_price, is_long)
                        
                        if is_path_clear:
                            print(f"\n🚀 [İŞLEM AÇILIYOR] Patron, {symbol} radarıma takıldı!")
                            print(f"   => Seçim Nedenim: {pa_reason}")
                            print(f"   => Tahta Analizim: {ob_reason}")
                            print(f"   => {trend.upper()} işlemine giriyorum!")
                            
                            side = 'buy' if is_long else 'sell'
                            execution_result = await self.execution.open_position(symbol, side, current_price)
                            
                            if execution_result and execution_result.get("status") == "success":
                                self.active_trades[symbol] = {
                                    'trend': trend, 'side': side,
                                    'amount': execution_result['amount'],
                                    'entry_price': execution_result['entry_price'],
                                    'max_reached_price': execution_result['entry_price'],
                                    'initial_sl': initial_sl, 'current_sl': initial_sl,
                                    'start_time': time.time(), 'last_log_time': time.time()
                                }
                                if len(self.active_trades) >= self.max_active_trades: break
            except Exception as e:
                pass
            await asyncio.sleep(self.config.SCAN_INTERVAL) # 5 DAKİKA BEKLE

    async def hourly_reporter(self):
        """60 DAKİKALIK DÖNGÜ: Saatlik kapanan işlemleri raporlar."""
        while True:
            await asyncio.sleep(3600) # 1 saat bekle
            report = self.state_manager.generate_hourly_report()
            print(f"\n{report}")

    async def run(self):
        print("🤖 Kurumsal Price Action Asistanın Uyandı ve Taramaya Başlıyor...")
        await asyncio.gather(
            self.fast_price_monitor(),
            self.slow_trade_manager(),
            self.market_scanner(),
            self.hourly_reporter()
        )

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
