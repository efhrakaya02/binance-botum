import os
import ccxt
import pandas as pd
import numpy as np
import time
import threading
import gc
import logging
from flask import Flask, jsonify

# ============================================================
# RAILWAY & BINANCE OPTİMİZE HİBRİT BOT (SCALP & FIRSAT)
# ============================================================

app = Flask(__name__)

API_KEY = os.getenv("BINANCE_API_KEY")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    print("UYARI: BINANCE_API_KEY veya BINANCE_SECRET_KEY eksik!", flush=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ============================================================
# ÇALIŞMA MODLARI VE KİLİTLİ LİMİTLER
# ============================================================
TRADING_ENABLED = True
POSITION_MONITOR_ENABLED = True

# Finansal ve Risk Kısıtları (Kesin Kurallar)
SCALP_MARGIN = 10.0          # Kesin kural: Scalp için tam 10 USDT
OPPORTUNITY_MARGIN = 15.0    # Kesin kural: Fırsat için tam 15 USDT
MAX_SCALP_POSITIONS = 1      # Maksimum 1 Scalp pozisyonu (Kesin kural)
MAX_OPPORTUNITY_POSITIONS = 1# Maksimum 1 Fırsat pozisyonu (Kesin kural)
MAX_TOTAL_POSITIONS = 2      # Toplamda maksimum 2 pozisyon (1 Scalp + 1 Fırsat)
LEVERAGE = 5                 # KALDIRAÇ KESİN OLARAK 5X (Değiştirilemez)

MIN_SCORE_THRESHOLD = 85     # Minimum skor sınırı
SCALP_TARGET_USDT = 0.30     # Scalp modunda minimum net kar hedefi

# Runtime State
pozisyon_en_yuksek_kar = {}
pozisyon_tipleri = {}
pozisyon_yonleri = {}
pozisyon_giris_fiyatlari = {}
onceki_aktif_pozisyonlar = set()

islem_acma_lock = threading.Lock()
pozisyon_monitor_lock = threading.Lock()
monitor_basladi = False

# ============================================================
# BINANCE BAĞLANTI
# ============================================================
def get_exchange():
    return ccxt.binance({
        "apiKey": API_KEY,
        "secret": SECRET_KEY,
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "adjustForTimeDifference": True,
            "warnOnFetchOpenOrdersWithoutSymbol": False
        }
    })

def sembol_duzelt(symbol):
    if symbol == "BCC/USDT":
        return "BCH/USDT"
    return symbol

def gecerli_kripto_mu(symbol):
    yasakli = ["UP/", "DOWN/", "BEAR/", "BULL/", "_", "BID", "ASK"]
    if not symbol.endswith("/USDT") and not "/USDT:" in symbol:
        return False
    if "BTC" in symbol or "XAU" in symbol:
        return False
    for yasak in yasakli:
        if yasak in symbol:
            return False
    return True

# ============================================================
# VERİ ÇEKME VE İNDİKATÖRLER
# ============================================================
def ohlcv_getir(exchange, symbol, timeframe, limit=50):
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not data or len(data) < 25:
            return None
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
        df["ema9"] = df["close"].ewm(span=9, adjust=False).mean()
        df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()
        
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df["rsi"] = 100 - (100 / (1 + rs))
        
        high_low = df["high"] - df["low"]
        high_close = abs(df["high"] - df["close"].shift())
        low_close = abs(df["low"] - df["close"].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr"] = tr.ewm(alpha=1/14, adjust=False).mean()
        
        return df
    except Exception:
        return None

# ============================================================
# GELİŞMİŞ REGRESYON ANALİZİ
# ============================================================
def gelismis_regresyon_teyidi(df, direction, periyot=15):
    if df is None or len(df) < periyot:
        return False, 0.0, 0.0, "Yetersiz veri."

    closes = df["close"].iloc[-periyot:].values
    x = np.arange(periyot)
    slope, intercept = np.polyfit(x, closes, 1)
    y_pred = intercept + slope * x
    corr_matrix = np.corrcoef(closes, y_pred)
    r_squared = corr_matrix[0, 1] ** 2 if not np.isnan(corr_matrix[0, 1]) else 0.0
    
    anlik_fiyat = closes[-1]
    regresyon_orta = intercept + slope * (periyot - 1)
    analiz_ozeti = f"Eğim: {slope:.4f} | R²: {r_squared:.2f} | Fiyat: {anlik_fiyat:.4f}"
    min_r_squared = 0.30

    if direction == "buy":
        if slope > 0 and r_squared >= min_r_squared and anlik_fiyat >= (regresyon_orta * 0.995):
            return True, slope, r_squared, f"ONAYLANDI (BUY) -> {analiz_ozeti}"
    elif direction == "sell":
        if slope < 0 and r_squared >= min_r_squared and anlik_fiyat <= (regresyon_orta * 1.005):
            return True, slope, r_squared, f"ONAYLANDI (SELL) -> {analiz_ozeti}"

    return False, slope, r_squared, f"REDDEDİLDİ -> {analiz_ozeti}"

def check_pullback_and_confirmation(df, direction):
    if df is None or len(df) < 3: return False
    last_close = df["close"].iloc[-1]
    prev_close = df["close"].iloc[-2]
    prev_low = df["low"].iloc[-2]
    prev_high = df["high"].iloc[-2]
    ema50 = df["ema50"].iloc[-1]
    ema9 = df["ema9"].iloc[-1]
    ema21 = df["ema21"].iloc[-1]

    if direction == "buy":
        return last_close > ema50 and ema9 > ema21 and (prev_low <= df["ema50"].iloc[-2] or prev_close < df["open"].iloc[-2] or last_close > prev_close)
    elif direction == "sell":
        return last_close < ema50 and ema9 < ema21 and (prev_high >= df["ema50"].iloc[-2] or prev_close > df["open"].iloc[-2] or last_close < prev_close)
    return False

# ============================================================
# TARAMA FONKSİYONLARI
# ============================================================
def scan_scalp_market(exchange):
    try:
        tickers = exchange.fetch_tickers()
        sorted_tickers = sorted(
            [t for t in tickers.values() if gecerli_kripto_mu(t['symbol'])], 
            key=lambda x: float(x.get('quoteVolume', 0) or 0), 
            reverse=True
        )
        top_symbols = [t['symbol'] for t in sorted_tickers[:25]]
        
        candidates = []
        for symbol in top_symbols:
            df = ohlcv_getir(exchange, symbol, timeframe='30m', limit=50)
            if df is None: continue
            last_row = df.iloc[-1]
            rsi, close, ema9, ema21 = float(last_row['rsi']), float(last_row['close']), float(last_row['ema9']), float(last_row['ema21'])
            
            score = 75
            direction = None
            if rsi > 52 and close > ema9 and ema9 > ema21:
                direction = "buy"
                score += 15
            elif rsi < 48 and close < ema9 and ema9 < ema21:
                direction = "sell"
                score += 15
                
            if direction and score >= MIN_SCORE_THRESHOLD:
                candidates.append({"symbol": symbol, "score": score, "direction": direction, "mode": "scalp", "df": df})
            else:
                del df
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:5]
    except Exception as e:
        logging.error(f"Scalp tarama hatası: {e}")
        return []

def scan_opportunity_market(exchange):
    try:
        tickers = exchange.fetch_tickers()
        usdt_tickers = [t for t in tickers.values() if gecerli_kripto_mu(t['symbol']) and t.get('percentage') is not None]
        gainers = sorted(usdt_tickers, key=lambda x: float(x['percentage']), reverse=True)[:20]
        losers = sorted(usdt_tickers, key=lambda x: float(x['percentage']), reverse=False)[:10]
        target_pool = list(set([t['symbol'] for t in gainers + losers]))
        
        candidates = []
        for symbol in target_pool:
            df = ohlcv_getir(exchange, symbol, timeframe='1h', limit=70)
            if df is None: continue
            last_row = df.iloc[-1]
            vol_mean = df['volume'].rolling(20).mean().iloc[-1]
            volume_spike = float(last_row['volume']) > (vol_mean * 2.0) if vol_mean > 0 else False
            rsi = float(last_row['rsi'])
            
            score = 75
            direction = "buy" if float(last_row['close']) > float(last_row['ema50']) else "sell"
            if volume_spike: score += 18
            if 35 < rsi < 70: score += 12
            
            if score >= MIN_SCORE_THRESHOLD:
                candidates.append({"symbol": symbol, "score": score, "direction": direction, "mode": "opportunity", "df": df})
            else:
                del df
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:5]
    except Exception as e:
        logging.error(f"Fırsat tarama hatası: {e}")
        return []

# ============================================================
# İŞLEM AÇMA (KESİN 5X KALDIRAÇ VE 1 SCALP + 1 FIRSAT KURALI)
# ============================================================
def pozisyon_ac(exchange, symbol, direction, score, p_type):
    if not TRADING_ENABLED: return False
    
    with islem_acma_lock:
        try:
            positions = exchange.fetch_positions()
            active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
            
            if len(active_positions) >= MAX_TOTAL_POSITIONS: 
                logging.warning(f"[SINIR AŞILDI] Maksimum toplam pozisyon sınırına ({MAX_TOTAL_POSITIONS}) ulaşıldı.")
                return False
                
            for p in active_positions:
                if sembol_duzelt(p.get("symbol")) == symbol: 
                    return False

            aktif_scalp_var = False
            aktif_firsat_var = False

            for p in active_positions:
                p_sym = sembol_duzelt(p.get("symbol"))
                turu = pozisyon_tipleri.get(p_sym, "opportunity")
                if turu == "scalp":
                    aktif_scalp_var = True
                else:
                    aktif_firsat_var = True

            if p_type == "scalp" and aktif_scalp_var:
                logging.info(f"[LİMİT KORUMA] Binance'te zaten aktif 1 Scalp pozisyonu var. Yeni Scalp AÇILMAYACAK.")
                return False
                
            if p_type == "opportunity" and aktif_firsat_var:
                logging.info(f"[LİMİT KORUMA] Binance'te zaten aktif 1 Fırsat pozisyonu var. Yeni Fırsat AÇILMAYACAK.")
                return False

            # KESİN 5X KALDIRAÇ UYGULAMASI
            leverage = LEVERAGE
            try:
                exchange.set_margin_mode("isolated", symbol)
                exchange.set_leverage(leverage, symbol)
            except Exception as lev_err:
                logging.warning(f"Kaldıraç ayarlama uyarısı ({symbol}): {lev_err}")

            ticker = exchange.fetch_ticker(symbol)
            price = float(ticker["last"])
            
            margin = OPPORTUNITY_MARGIN if p_type == "opportunity" else SCALP_MARGIN
            notional = margin * leverage
            raw_amount = notional / price

            market = exchange.market(symbol)
            min_amount = market['limits']['amount']['min']
            
            if raw_amount < min_amount:
                logging.warning(f"[LİMİT YETERSİZ] {symbol} minimum miktar sınırına takıldı.")
                return False

            amount = float(exchange.amount_to_precision(symbol, raw_amount))
            gercek_notional = amount * price
            gercek_margin = gercek_notional / leverage

            side = "buy" if direction == "buy" else "sell"
            order = exchange.create_order(symbol, "market", side, amount, None, {"leverage": leverage})
            
            if order:
                pozisyon_tipleri[symbol] = p_type
                pozisyon_yonleri[symbol] = direction
                pozisyon_giris_fiyatlari[symbol] = price
                pozisyon_en_yuksek_kar[symbol] = 0.0
                logging.info(f"[ İŞLEM AÇILDI ] {p_type.upper()} | {symbol} {side.upper()} | Giriş: {price} | Skor: {score} | Marj: ~{gercek_margin:.2f} USDT | Kaldıraç: {leverage}x")
                
                time.sleep(1.5)
                try:
                    close_side = "sell" if side == "buy" else "buy"
                    df_temp = ohlcv_getir(exchange, symbol, "30m", 20)
                    atr = float(df_temp.iloc[-1]["atr"]) if df_temp is not None else (price * 0.01)
                    del df_temp
                    
                    if p_type == "scalp":
                        fiyat_farki = SCALP_TARGET_USDT / amount
                        tp_price = (price + fiyat_farki) if side == "buy" else (price - fiyat_farki)
                        sl_price = (price - (atr * 2.0)) if side == "buy" else (price + (atr * 2.0))
                        
                        exchange.create_order(symbol, 'take_profit_market', close_side, amount, None, {'stopPrice': float(exchange.price_to_precision(symbol, tp_price)), 'reduceOnly': True, 'workingType': 'MARK_PRICE'})
                        exchange.create_order(symbol, 'stop_market', close_side, amount, None, {'stopPrice': float(exchange.price_to_precision(symbol, sl_price)), 'reduceOnly': True, 'workingType': 'MARK_PRICE'})
                    else:
                        sl_price = (price - (atr * 2.5)) if side == "buy" else (price + (atr * 2.5))
                        exchange.create_order(symbol, 'stop_market', close_side, amount, None, {'stopPrice': float(exchange.price_to_precision(symbol, sl_price)), 'reduceOnly': True, 'workingType': 'MARK_PRICE'})
                except Exception as e:
                    logging.error(f"SL/TP emir hatası: {e}")
                return True
        except Exception as e:
            logging.error(f"İşlem açma hata {symbol}: {e}")
        return False

# ============================================================
# POZİSYON MONİTÖRÜ VE KADEMELİ TRAILING STOP
# ============================================================
def pozisyonlari_yonet(exchange, positions):
    global onceki_aktif_pozisyonlar
    aktif_semboller = {sembol_duzelt(p.get("symbol")) for p in positions if float(p.get("contracts") or 0) > 0}
    
    kapananlar = onceki_aktif_pozisyonlar - aktif_semboller
    for sym in kapananlar:
        if sym in pozisyon_tipleri: del pozisyon_tipleri[sym]
        if sym in pozisyon_en_yuksek_kar: del pozisyon_en_yuksek_kar[sym]
        if sym in pozisyon_yonleri: del pozisyon_yonleri[sym]
        if sym in pozisyon_giris_fiyatlari: del pozisyon_giris_fiyatlari[sym]
        try:
            exchange.cancel_all_orders(sym)
        except Exception:
            pass

    onceki_aktif_pozisyonlar = aktif_semboller.copy()

    for p in positions:
        symbol = sembol_duzelt(p.get("symbol"))
        try:
            contracts = float(p.get("contracts") or 0)
            if contracts <= 0: continue
            side = p.get("side") 
            entry_price = float(p.get("entryPrice") or 0)
            mark_price = float(p.get("markPrice") or 0)
            leverage = float(p.get("leverage") or LEVERAGE)
            if entry_price == 0 or mark_price == 0: continue

            p_type = pozisyon_tipleri.get(symbol, "opportunity")
            
            api_percentage = p.get("percentage")
            roi = float(api_percentage) if api_percentage is not None else 0.0
            
            current_max = pozisyon_en_yuksek_kar.get(symbol, 0.0)
            if roi > current_max:
                pozisyon_en_yuksek_kar[symbol] = roi
                current_max = roi

            if p_type == "opportunity":
                yeni_sl = None
                log_aciklama = ""
                
                if current_max >= 15.0:
                    hedef_roi_koruma = current_max - 3.0
                    fiyat_degisim_orani = (hedef_roi_koruma / 100.0) / leverage
                    yeni_sl = entry_price * (1 + fiyat_degisim_orani) if side == "long" else entry_price * (1 - fiyat_degisim_orani)
                    log_aciklama = f"KRİTİK ZİRVE (%15+) | Hedef ROI: %{hedef_roi_koruma:.2f}"

                elif current_max >= 5.0:
                    oran = (current_max - 5.0) / 10.0
                    if side == "long":
                        yeni_sl = entry_price + ((mark_price - entry_price) * oran * 0.5)
                        if yeni_sl < entry_price: yeni_sl = entry_price
                    else:
                        yeni_sl = entry_price - ((entry_price - mark_price) * oran * 0.5)
                        if yeni_sl > entry_price: yeni_sl = entry_price
                    log_aciklama = f"KADEMELİ (%5-%15) | Zirve: %{current_max:.2f}"

                if yeni_sl is not None:
                    try:
                        if side == "long" and yeni_sl >= mark_price: yeni_sl = mark_price * 0.998 
                        elif side == "short" and yeni_sl <= mark_price: yeni_sl = mark_price * 1.002

                        exchange.cancel_all_orders(symbol)
                        time.sleep(0.5)
                        close_side = "sell" if side == "long" else "buy"
                        exchange.create_order(symbol, 'stop_market', close_side, contracts, None, {'stopPrice': float(exchange.price_to_precision(symbol, yeni_sl)), 'reduceOnly': True, 'workingType': 'MARK_PRICE'})
                    except Exception as ex:
                        logging.error(f"Trailing Stop Hata {symbol}: {ex}")
        except Exception as e:
            logging.error(f"Pozisyon yönetimi hata {symbol}: {e}")

def pozisyon_monitor_loop():
    global monitor_basladi
    if not POSITION_MONITOR_ENABLED or monitor_basladi: return
    monitor_basladi = True
    exchange = None
    while True:
        try:
            if exchange is None:
                exchange = get_exchange()
                exchange.load_markets()
            if pozisyon_monitor_lock.acquire(blocking=False):
                try:
                    positions = exchange.fetch_positions()
                    active_pos = [p for p in positions if float(p.get("contracts") or 0) > 0]
                    pozisyonlari_yonet(exchange, active_pos)
                finally:
                    pozisyon_monitor_lock.release()
        except Exception:
            exchange = None
        time.sleep(5.0)

def monitor_baslat():
    if POSITION_MONITOR_ENABLED:
        threading.Thread(target=pozisyon_monitor_loop, daemon=True, name="PositionMonitor").start()

# ============================================================
# ANA HİBRİT ÇALIŞMA DÖNGÜSÜ
# ============================================================
def ana_tarama_dongusu():
    monitor_baslat()
    while True:
        exchange = None
        try:
            exchange = get_exchange()
            exchange.load_markets()
            
            logging.info("Hibrit Piyasa Taraması Başlatılıyor...")
            
            positions = exchange.fetch_positions()
            active_pos = [p for p in positions if float(p.get("contracts") or 0) > 0]
            
            aktif_scalp_var = False
            aktif_firsat_var = False
            for p in active_pos:
                sym_p = sembol_duzelt(p.get("symbol"))
                turu = pozisyon_tipleri.get(sym_p, "opportunity")
                if turu == "scalp":
                    aktif_scalp_var = True
                else:
                    aktif_firsat_var = True

            # 1. FIRSAT KONTROLÜ
            if not aktif_firsat_var:
                logging.info("Fırsat pozisyonu eksik, Fırsat pazarı taranıyor...")
                firsat_listesi = scan_opportunity_market(exchange)
                if firsat_listesi:
                    for candidate in firsat_listesi:
                        sym = candidate['symbol']
                        dir_val = candidate['direction']
                        df_check = candidate.get('df')
                        
                        if df_check is not None:
                            pullback_ok = check_pullback_and_confirmation(df_check, dir_val)
                            reg_ok, slope_val, r2_val, reg_mesaj = gelismis_regresyon_teyidi(df_check, dir_val, periyot=20)

                            if pullback_ok and reg_ok:
                                logging.info(f"[FIRSAT ONAYLANDI] {sym} açılıyor...")
                                basarili = pozisyon_ac(exchange, sym, dir_val, candidate['score'], "opportunity")
                                if basarili:
                                    break
                for item in firsat_listesi:
                    if 'df' in item and item['df'] is not None: del item['df']
            else:
                logging.info("[KORUMA] Binance'te zaten aktif 1 Fırsat pozisyonu var. Yeni Fırsat taranmıyor.")

            # 2. SCALP KONTROLÜ
            if not aktif_scalp_var:
                logging.info("Scalp pozisyonu eksik, Scalp pazarı taranıyor...")
                scalp_listesi = scan_scalp_market(exchange)
                if scalp_listesi:
                    for candidate in scalp_listesi:
                        sym = candidate['symbol']
                        dir_val = candidate['direction']
                        df_check = candidate.get('df')
                        
                        if df_check is not None:
                            pullback_ok = check_pullback_and_confirmation(df_check, dir_val)
                            reg_ok, slope_val, r2_val, reg_mesaj = gelismis_regresyon_teyidi(df_check, dir_val, periyot=12)

                            if pullback_ok and reg_ok:
                                logging.info(f"[SCALP ONAYLANDI] {sym} açılıyor...")
                                basarili = pozisyon_ac(exchange, sym, dir_val, candidate['score'], "scalp")
                                if basarili:
                                    break
                for item in scalp_listesi:
                    if 'df' in item and item['df'] is not None: del item['df']
            else:
                logging.info("[KORUMA] Binance'te zaten aktif 1 Scalp pozisyonu var. Yeni Scalp taranmıyor.")

            active_pos.clear()

        except Exception as e:
            logging.error(f"Ana döngü hatası: {e}")
        finally:
            gc.collect()
            
        time.sleep(120)

# ============================================================
# FLASK WEB ENDPOINTLERİ
# ============================================================
@app.route("/")
def index():
    return jsonify({"status": "Bot Aktif", "acik_pozisyonlar": list(pozisyon_tipleri.keys())})

@app.route("/durum")
def durum():
    try:
        exchange = get_exchange()
        exchange.load_markets()
        positions = exchange.fetch_positions()
        active_pos = [p for p in positions if float(p.get("contracts") or 0) > 0]
        
        detaylar = []
        for p in active_pos:
            sym = sembol_duzelt(p.get("symbol"))
            p_type = pozisyon_tipleri.get(sym, "Opportunity")
            entry = float(p.get("entryPrice", 0))
            mark = float(p.get("markPrice", 0))
            lev = float(p.get("leverage", 1))
            side = p.get("side")
            roi = float(p.get("percentage") or 0.0)
            
            detaylar.append({
                "symbol": sym,
                "mod": p_type.upper(),
                "yon": side.upper(),
                "giris_fiyati": entry,
                "anlik_fiyat": mark,
                "kaldirac": lev,
                "binance_net_roi_yuzde": round(roi, 2),
                "max_gorulen_kar": round(pozisyon_en_yuksek_kar.get(sym, 0.0), 2)
            })
        return jsonify({"success": True, "aktif_islem_sayisi": len(detaylar), "islemler": detaylar})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/tetikle")
def tetikle():
    threading.Thread(target=ana_tarama_dongusu, daemon=True).start()
    return jsonify({"success": True, "message": "Manuel tetikleme başarılı."})

if __name__ == "__main__":
    t = threading.Thread(target=ana_tarama_dongusu, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
