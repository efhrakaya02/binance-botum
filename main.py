import asyncio
import time
import config
from modules.scanner import MarketScanner
from modules.orderbook import OrderbookAnalyzer
from modules.risk_manager import RiskManager
from modules.state_manager import StateManager
from modules.execution import ExecutionEngine

async def main_loop():
    print("🤖 PA Bot Başlatılıyor... (Filtreler, Zaman Aşımı ve 30Dk Raporlama Aktif)")
    
    state_mgr = StateManager()
    risk_mgr = RiskManager(config)
    scanner = MarketScanner(config)
    executor = ExecutionEngine(config)
    
    last_status_print = {} 
    
    # 🚀 YENİ: Raporlama için değişkenler
    last_report_time = time.time()
    closed_trades_history = [] 
    
    while True:
        try:
            now = time.time()
            active_trades = state_mgr.state["active_trades"].copy()
            
            # --- 1. AŞAMA: AKTİF İŞLEMLERİ KONTROL ET ---
            for symbol, trade_data in active_trades.items():
                try:
                    ticker = await executor.exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    
                    is_long = trade_data["type"] == "long"
                    entry_price = trade_data["entry"]
                    max_price = trade_data["max_price"]
                    entry_time = trade_data.get("entry_time", now) 
                    
                    if is_long and current_price > max_price:
                        trade_data["max_price"] = current_price
                    elif not is_long and current_price < max_price:
                        trade_data["max_price"] = current_price
                        
                    dynamic_sl = risk_mgr.calculate_stop_loss(entry_price, trade_data["max_price"], is_long)
                    
                    if is_long:
                        pnl_pct = ((current_price - entry_price) / entry_price) * 100
                    else:
                        pnl_pct = ((entry_price - current_price) / entry_price) * 100
                    
                    # Zaman Aşımı Kontrolü (60 Dakika / 3600 Saniye)
                    is_timeout = (now - entry_time) >= 3600
                    timeout_close = is_timeout and pnl_pct < config.TRAILING_ACTIVATION_PCT
                    
                    close_condition = (is_long and current_price <= dynamic_sl) or (not is_long and current_price >= dynamic_sl)
                    
                    if close_condition or timeout_close:
                        if timeout_close:
                            print(f"\n⏳ İŞLEM ZAMAN AŞIMI (60dk): {symbol} yataya bağladı. Kapanış: {current_price} | Net PnL: %{pnl_pct:.2f}")
                        else:
                            print(f"\n🔔 İŞLEM KAPATILDI (STOP/TP): {symbol} | Kapanış: {current_price} | Net PnL: %{pnl_pct:.2f}")
                            
                        success = await executor.close_position(symbol, 'buy' if is_long else 'sell', trade_data["amount"])
                        if success:
                            # 🚀 YENİ: Rapor geçmişine ekle
                            closed_trades_history.append({"symbol": symbol, "pnl": pnl_pct})
                            
                            del state_mgr.state["active_trades"][symbol]
                            state_mgr.set_cooldown(symbol, config.COOLDOWN_MINUTES)
                            print(f"❄️ {symbol} için {config.COOLDOWN_MINUTES} dakikalık soğuma süresi başlatıldı.")
                            state_mgr.save_state()
                    else:
                        state_mgr.state["active_trades"][symbol] = trade_data
                        state_mgr.save_state()
                        
                        if now - last_status_print.get(symbol, 0) > 60:
                            print(f"📊 TAKİP [{symbol}] | Yön: {trade_data['type'].upper()} | Giriş: {entry_price:.5f} | Anlık: {current_price:.5f} | Dinamik Stop: {dynamic_sl:.5f} | PnL: %{pnl_pct:.2f} | Süre: {int((now - entry_time)/60)} dk")
                            last_status_print[symbol] = now
                            
                except Exception:
                    pass 
            
            # --- 2. AŞAMA: 30 DAKİKALIK RAPOR OLUŞTURMA ---
            if now - last_report_time >= 1800: # 1800 saniye = 30 dakika
                print("\n" + "="*45)
                print("🕒 30 DAKİKALIK PERFORMANS ÖZETİ")
                print("="*45)
                if not closed_trades_history:
                    print("ℹ️ Son 30 dakikada kapanan işlem bulunmuyor.")
                else:
                    total_pnl = 0
                    wins = 0
                    losses = 0
                    for t in closed_trades_history:
                        total_pnl += t["pnl"]
                        if t["pnl"] > 0:
                            wins += 1
                        else:
                            losses += 1
                        print(f"🔸 {t['symbol']:<15} | Net PnL: %{t['pnl']:.2f}")
                    
                    print("-" * 45)
                    print(f"✅ Başarılı İşlem: {wins} | ❌ Stop/Zarar: {losses}")
                    print(f"💰 Toplam PnL Değişimi: %{total_pnl:.2f}")
                print("="*45 + "\n")
                
                # Rapor verildi, geçmişi temizle ve süreyi sıfırla
                closed_trades_history.clear()
                last_report_time = now

            # --- 3. AŞAMA: YENİ FIRSAT TARAMASI ---
            if state_mgr.get_used_slots() < config.MAX_OPEN_POSITIONS:
                radar_list = await scanner.scan_market()
                
                for opportunity in radar_list:
                    if state_mgr.get_used_slots() >= config.MAX_OPEN_POSITIONS:
                        break 
                        
                    symbol = opportunity["symbol"]
                    trend = opportunity["trend"]
                    
                    if symbol in state_mgr.state["active_trades"]:
                        continue
                        
                    if state_mgr.is_in_cooldown(symbol):
                        continue
                        
                    ob_analyzer = OrderbookAnalyzer(symbol)
                    try:
                        ticker = await executor.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        is_safe = await ob_analyzer.check_for_walls_and_sweeps(current_price, is_long=(trend=="long"))
                        
                        if is_safe:
                            trade_result = await executor.open_position(symbol, 'buy' if trend == "long" else 'sell', current_price)
                            
                            if trade_result["status"] == "success":
                                state_mgr.state["active_trades"][symbol] = {
                                    "entry": trade_result["entry_price"],
                                    "max_price": trade_result["entry_price"],
                                    "type": trend,
                                    "amount": trade_result["amount"],
                                    "entry_time": time.time()
                                }
                                state_mgr.save_state()
                                last_status_print[symbol] = now 
                                print(f"🚀 BAŞARILI: {symbol} sanal işlem açıldı.\n")
                    except Exception:
                        continue 
            
            await asyncio.sleep(5)
            
        except Exception as e:
            import traceback
            print(f"❌ Ana Döngü Hatası (10 sn sonra tekrar denenecek): {e}")
            traceback.print_exc()
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass
