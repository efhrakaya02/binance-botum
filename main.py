import asyncio
import time
import config
from modules.scanner import MarketScanner
from modules.orderbook import OrderbookAnalyzer
from modules.risk_manager import RiskManager
from modules.state_manager import StateManager
from modules.execution import ExecutionEngine

async def main_loop():
    print("🤖 PA Bot Başlatılıyor... (Sadeleştirilmiş Log Modu Aktif)")
    
    state_mgr = StateManager()
    risk_mgr = RiskManager(config)
    scanner = MarketScanner(config)
    executor = ExecutionEngine(config)
    
    # İşlem takibi loglarının sıklığını yönetmek için zamanlayıcı (Her 60 saniyede 1 rapor)
    last_status_print = {} 
    
    while True:
        try:
            now = time.time()
            active_trades = state_mgr.state["active_trades"].copy()
            
            # ---------------------------------------------------------
            # 1. AŞAMA: AKTİF İŞLEMLERİ YÖNET
            # ---------------------------------------------------------
            for symbol, trade_data in active_trades.items():
                try:
                    ticker = await executor.exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    
                    is_long = trade_data["type"] == "long"
                    entry_price = trade_data["entry"]
                    max_price = trade_data["max_price"]
                    
                    if is_long and current_price > max_price:
                        trade_data["max_price"] = current_price
                    elif not is_long and current_price < max_price:
                        trade_data["max_price"] = current_price
                        
                    dynamic_sl = risk_mgr.calculate_stop_loss(entry_price, trade_data["max_price"], is_long)
                    close_condition = (is_long and current_price <= dynamic_sl) or (not is_long and current_price >= dynamic_sl)
                    
                    # Anlık Kar/Zarar yüzdesini hesapla
                    if is_long:
                        pnl_pct = ((current_price - entry_price) / entry_price) * 100
                    else:
                        pnl_pct = ((entry_price - current_price) / entry_price) * 100
                    
                    if close_condition:
                        print(f"\n🔔 İŞLEM KAPATILDI (STOP/TP): {symbol} | Kapanış: {current_price} | Net PnL: %{pnl_pct:.2f}")
                        success = await executor.close_position(symbol, 'buy' if is_long else 'sell', trade_data["amount"])
                        if success:
                            del state_mgr.state["active_trades"][symbol]
                            state_mgr.save_state()
                    else:
                        state_mgr.state["active_trades"][symbol] = trade_data
                        state_mgr.save_state()
                        
                        # Ekrana 60 saniyede bir işlem durumu bas (Spam engelleme)
                        if now - last_status_print.get(symbol, 0) > 60:
                            print(f"📊 TAKİP [{symbol}] | Yön: {trade_data['type'].upper()} | Giriş: {entry_price:.5f} | Anlık: {current_price:.5f} | Dinamik Stop: {dynamic_sl:.5f} | PnL: %{pnl_pct:.2f}")
                            last_status_print[symbol] = now
                            
                except Exception:
                    pass # Anlık API kopmalarını sessizce geç
            
            # ---------------------------------------------------------
            # 2. AŞAMA: YENİ FIRSAT TARAMASI
            # ---------------------------------------------------------
            if state_mgr.get_used_slots() < config.MAX_OPEN_POSITIONS:
                # Ekranda sürekli "Taranıyor" yazısını göstermemek için sessizce tarar
                radar_list = await scanner.scan_market()
                
                for opportunity in radar_list:
                    if state_mgr.get_used_slots() >= config.MAX_OPEN_POSITIONS:
                        break 
                        
                    symbol = opportunity["symbol"]
                    trend = opportunity["trend"]
                    
                    if symbol in state_mgr.state["active_trades"]:
                        continue
                        
                    ob_analyzer = OrderbookAnalyzer(symbol)
                    try:
                        ticker = await executor.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # Burada sadece is_safe True ise log basılacak (orderbook içinden)
                        is_safe = await ob_analyzer.check_for_walls_and_sweeps(current_price, is_long=(trend=="long"))
                        
                        if is_safe:
                            trade_result = await executor.open_position(symbol, 'buy' if trend == "long" else 'sell', current_price)
                            
                            if trade_result["status"] == "success":
                                state_mgr.state["active_trades"][symbol] = {
                                    "entry": trade_result["entry_price"],
                                    "max_price": trade_result["entry_price"],
                                    "type": trend,
                                    "amount": trade_result["amount"]
                                }
                                state_mgr.save_state()
                                last_status_print[symbol] = now # Hemen ardından takip logu atmasın diye zamanı başlat
                                print(f"🚀 BAŞARILI: {symbol} işlemi açıldı. (Miktar: {trade_result['amount']})\n")
                    except Exception:
                        continue 
            
            await asyncio.sleep(5)
            
        except Exception:
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass
