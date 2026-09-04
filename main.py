import asyncio
import time
import config
from modules.scanner import MarketScanner
from modules.orderbook import OrderbookAnalyzer
from modules.risk_manager import RiskManager
from modules.state_manager import StateManager
from modules.execution import ExecutionEngine

async def main_loop():
    print("🤖 PA Bot Başlatılıyor...")
    
    # Modülleri ayağa kaldır
    state_mgr = StateManager()
    risk_mgr = RiskManager(config)
    scanner = MarketScanner(config)
    executor = ExecutionEngine(config)
    
    while True:
        try:
            # ---------------------------------------------------------
            # 1. AŞAMA: AKTİF İŞLEMLERİ YÖNET (Risk ve Kar Kilitleme)
            # ---------------------------------------------------------
            active_trades = state_mgr.state["active_trades"].copy()
            for symbol, trade_data in active_trades.items():
                
                # Canlı fiyatı çek
                ticker = await executor.exchange.fetch_ticker(symbol)
                current_price = ticker['last']
                
                is_long = trade_data["type"] == "long"
                entry_price = trade_data["entry"]
                max_price = trade_data["max_price"]
                
                # Fiyat yeni bir zirve/dip yaptıysa max ulaşılan fiyatı güncelle
                if is_long and current_price > max_price:
                    trade_data["max_price"] = current_price
                elif not is_long and current_price < max_price:
                    trade_data["max_price"] = current_price
                    
                # Dinamik Trailing Stop / Breakeven seviyesini Risk Manager'dan hesapla
                dynamic_sl = risk_mgr.calculate_stop_loss(entry_price, trade_data["max_price"], is_long)
                
                # Fiyat stop seviyesine değdi mi? (Kâr al (TP) veya Zarar Kes (SL))
                close_condition = (is_long and current_price <= dynamic_sl) or (not is_long and current_price >= dynamic_sl)
                
                if close_condition:
                    print(f"🔔 STOP/TP TETİKLENDİ: {symbol} - Kapatılıyor... (Fiyat: {current_price})")
                    # İşlemi borsada kapat
                    success = await executor.close_position(symbol, 'buy' if is_long else 'sell', trade_data["amount"])
                    if success:
                        # Hafızadan sil ve 1 slot boşa çıkar
                        del state_mgr.state["active_trades"][symbol]
                        state_mgr.save_state()
                        print(f"✅ {symbol} başarıyla kapatıldı ve slot boşaldı.")
                else:
                    # Kapanmadıysa güncel fiyatı (max_price) kaydet
                    state_mgr.state["active_trades"][symbol] = trade_data
                    state_mgr.save_state()

            # ---------------------------------------------------------
            # 2. AŞAMA: YENİ FIRSAT TARAMASI (Eğer boş slot varsa)
            # ---------------------------------------------------------
            if state_mgr.get_used_slots() < config.MAX_OPEN_POSITIONS:
                print(f"🔎 Boş slot var ({config.MAX_OPEN_POSITIONS - state_mgr.get_used_slots()}). Fırsatlar taranıyor...")
                radar_list = await scanner.scan_market()
                
                for opportunity in radar_list:
                    if state_mgr.get_used_slots() >= config.MAX_OPEN_POSITIONS:
                        break # Slotlar dolduysa aramayı kes
                        
                    symbol = opportunity["symbol"]
                    trend = opportunity["trend"]
                    
                    # Eğer bu coin zaten açıksa atla
                    if symbol in state_mgr.state["active_trades"]:
                        continue
                        
                    print(f"🎯 Potansiyel Bulundu: {symbol} Yön: {trend.upper()}")
                    
                    # 3. AŞAMA: ORDERBOOK & LİKİDASYON SAVUNMASI (Sweep Kontrolü)
                    ob_analyzer = OrderbookAnalyzer(symbol)
                    ticker = await executor.exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    
                    print(f"🛡️ {symbol} için Orderbook derinliği ve duvarlar kontrol ediliyor...")
                    is_safe = await ob_analyzer.check_for_walls_and_sweeps(current_price, is_long=(trend=="long"))
                    
                    if is_safe:
                        # 4. AŞAMA: GÜVENLİYSE İŞLEME GİR (Execution)
                        trade_result = await executor.open_position(symbol, 'buy' if trend == "long" else 'sell', current_price)
                        
                        if trade_result["status"] == "success":
                            # İşlemi hafızaya yaz ve slotu doldur
                            state_mgr.state["active_trades"][symbol] = {
                                "entry": trade_result["entry_price"],
                                "max_price": trade_result["entry_price"],
                                "type": trend,
                                "amount": trade_result["amount"]
                            }
                            state_mgr.save_state()
                            print(f"🔥 İşlem başarıyla eklendi ve hafızaya alındı: {symbol}")
            
            # API'yi yormamak ve Binance'ten ban yememek için ana döngü beklemesi
            print("⏳ Döngü tamamlandı. Piyasa izleniyor...")
            await asyncio.sleep(5)
            
        except Exception as e:
            print(f"⚠️ Ana döngüde beklenmeyen hata: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    # Windows/Linux Asyncio uyumluluğu
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("Bot manuel olarak durduruldu.")
