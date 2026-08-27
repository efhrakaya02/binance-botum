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

# Finansal Kısıtlar
SCALP_MARGIN = 10.0          # Kesin kural: Scalp için tam 10 USDT
OPPORTUNITY_MARGIN = 15.0    # Kesin kural: Fırsat için tam 15 USDT
MAX_TOTAL_POSITIONS = 2      # Toplamda maksimum 2 pozisyon (1 Scalp + 1 Fırsat)

MIN_SCORE_THRESHOLD = 85     # %80+ isabet için minimum skor sınırı
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
# BINANCE BAĞLANTI (Rate Limit & Bellek Optimizasyonlu)
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
    for yasak in yasakli:
        if yasak in symbol:
            return False
    return True

# ============================================================
# VERİ ÇEKME VE BELLEK DOSTU İNDİKATÖRLER
# ============================================================
def ohlcv_getir(exchange, symbol, timeframe, limit=60):
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
# PULLBACK VE FAKEOUT KORUMA TEYİDİ
# ============================================================
def check_pullback_and_confirmation(df, direction):
    if df is None or len(df) < 3:
        return False
        
    last_close = df["close"].iloc[-1]
    prev_close = df["close"].iloc[-2]
    prev_low = df["low"].iloc[-2]
    prev_high = df["high"].iloc[-2]
    ema50 = df["ema50"].iloc[-1]

    if direction == "buy":
        is_above_ema = last_close > ema50
        has_pulled_back = prev_low <= df["ema50"].iloc[-2] or prev_close < df["open"].iloc[-2]
        is_recovering = last_close > prev_close
        return is_above_ema and (has_pulled_back or is_recovering)
    
    elif direction == "sell":
        is_below_ema = last_close < ema50
        has_pulled_back = prev_high >= df["ema50"].iloc[-2] or prev_close > df["open"].iloc[-2]
        is_recovering = last_close < prev_close
        return is_below_ema and (has_pulled_back or is_recovering)

    return False

# ============================================================
# SCALP MODU TARAMASI
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
            rsi = float(last_row['rsi'])
            close = float(last_row['close'])
            ema50 = float(last_row['ema50'])
            
            score = 75
            direction = None
            if rsi > 50 and close > ema50:
                direction = "buy"
                score += 12
            elif rsi < 50 and close < ema50:
                direction = "sell"
                score += 12
                
            if direction and score >= MIN_SCORE_THRESHOLD:
                if check_pullback_and_confirmation(df, direction):
                    candidates.append({"symbol": symbol, "score": score, "direction": direction, "mode": "scalp"})
            
            del df
        
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:5]
    except Exception as e:
        logging.error(f"Scalp tarama hatası: {e}")
        return []

# ============================================================
# FIRSAT MODU TARAMASI
# ============================================================
def scan_opportunity_market(exchange):
    try:
        tickers = exchange.fetch_tickers()
        usdt_tickers = [t for t in tickers.values() if gecerli_kripto_mu(t['symbol']) and t.get('percentage') is not None]
        
        gainers = sorted(usdt_tickers, key=lambda x: float(x['percentage']), reverse=True)[:15]
        losers = sorted(usdt_tickers, key=lambda x: float(x['percentage']), reverse=False)[:15]
        target_pool = list(set([t['symbol'] for t in gainers + losers]))
        
        candidates = []
        for symbol in target_pool:
            df = ohlcv_getir(exchange, symbol, timeframe='1h', limit=70)
            if df is None: continue
            
            last_row = df.iloc[-1]
            vol_mean = df['volume'].rolling(20).mean().iloc[-1]
            volume_spike = float(last_row['volume']) > (vol_mean * 1.6) if vol_mean > 0 else False
            rsi = float(last_row['rsi'])
            
            score = 75
            direction = "buy" if float(last_row['close']) > float(last_row['ema50']) else "sell"
            
            if volume_spike: score += 15
            if 40 < rsi < 65: score += 10
            
            if score >= MIN_SCORE_THRESHOLD:
                candidates.append({
                    "symbol": symbol, 
                    "score": score, 
                    "direction": direction, 
                    "mode": "opportunity",
                    "df": df
                })
            else:
                del df
            
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:5]
    except Exception as e:
        logging.error(f"Fırsat tarama hatası: {e}")
        return []

# ============================================================
# İŞLEM YÖNETİMİ VE EMİR GÖNDERİMİ
# ============================================================
def pozisyon_ac(exchange, symbol, direction, score, p_type):
    if not TRADING_ENABLED: return False
    
    with islem_acma_lock:
        try:
            positions = exchange.fetch_positions()
            active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
            
            if len(active_positions) >= MAX_TOTAL_POSITIONS: return False
            for p in active_positions:
                if sembol_duzelt(p.get("symbol")) == symbol: return False

            leverage = 4
            try:
                exchange.set_margin_mode("isolated", symbol)
                exchange.set_leverage(leverage, symbol)
            except Exception:
                pass

            ticker = exchange.fetch_ticker(symbol)
            price = float(ticker["last"])
            
            margin = OPPORTUNITY_MARGIN if p_type == "opportunity" else SCALP_MARGIN
            notional = margin * leverage
            amount = notional / price
            amount = float(exchange.amount_to_precision(symbol, amount))

            side = "buy" if direction == "buy" else "sell"
            order = exchange.create_order(symbol, "market", side, amount, None, {"leverage": leverage})
            
            if order:
                pozisyon_tipleri[symbol] = p_type
                pozisyon_yonleri[symbol] = direction
                pozisyon_giris_fiyatlari[symbol] = price
                pozisyon_en_yuksek_kar[symbol] = 0.0
                logging.info(f"[İŞLEM AÇILDI] {p_type.upper()} | {symbol} {side.upper()} | Giriş: {price} | Puan: {score} | Marj: {margin} USDT")
                
                time.sleep(1)
                try:
                    close_side = "sell" if side == "buy" else "buy"
                    df_temp = ohlcv_getir(exchange, symbol, "30m", 20)
                    atr = float(df_temp.iloc[-1]["atr"]) if df_temp is not None else (price * 0.01)
                    
                    if p_type == "scalp":
                        fiyat_farki = SCALP_TARGET_USDT / amount
                        tp_price = (price + fiyat_farki) if side == "buy" else (price - fiyat_farki)
                        sl_price = (price - (atr * 2.0)) if side == "buy" else (price + (atr * 2.0))
                        
                        exchange.create_order(symbol, 'take_profit_market', close_side, amount, None, {'stopPrice': float(exchange.price_to_precision(symbol, tp_price)), 'reduceOnly': True})
                        exchange.create_order(symbol, 'stop_market', close_side, amount, None, {'stopPrice': float(exchange.price_to_precision(symbol, sl_price)), 'reduceOnly': True})
                    else:
                        sl_price = (price - (atr * 2.5)) if side == "buy" else (price + (atr * 2.5))
                        exchange.create_order(symbol, 'stop_market', close_side, amount, None, {'stopPrice': float(exchange.price_to_precision(symbol, sl_price)), 'reduceOnly': True})
                except Exception:
                    pass
                return True
        except Exception as e:
            logging.error(f"İşlem açma hata {symbol}: {e}")
        return False

# ============================================================
# POZİSYON MONİTÖRÜ VE DETAYLI TAKİP LİSTESİ
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
            leverage = float(p.get("leverage") or 1)
            if entry_price == 0 or mark_price == 0: continue

            p_type = pozisyon_tipleri.get(symbol, "bilinmiyor")
            roi = ((mark_price - entry_price) / entry_price) * 100 * leverage if side == "long" else ((entry_price - mark_price) / entry_price) * 100 * leverage
            
            current_max = pozisyon_en_yuksek_kar.get(symbol, 0.0)
            if roi > current_max:
                pozisyon_en_yuksek_kar[symbol] = roi
                current_max = roi

            logging.info(f"[TAKİP] {p_type.upper()} | {symbol} | Yön: {side.upper()} | Giriş: {entry_price} | Anlık: {mark_price} | ROI: %{roi:.2f} | Max Kar: %{current_max:.2f}")

            if p_type == "opportunity":
                yeni_sl = None
                if current_max >= 10.0:
                    yeni_sl = mark_price * (1 - 0.02 / leverage) if side == "long" else mark_price * (1 + 0.02 / leverage)
                elif current_max >= 5.0:
                    yeni_sl = entry_price if side == "long" else entry_price

                if yeni_sl is not None:
                    try:
                        exchange.cancel_all_orders(symbol)
                        close_side = "sell" if side == "long" else "buy"
                        exchange.create_order(symbol, 'stop_market', close_side, contracts, None, {'stopPrice': float(exchange.price_to_precision(symbol, yeni_sl)), 'reduceOnly': True})
                        logging.info(f"[TRAILING STOP GÜNCELLENDİ] {symbol} yeni SL seviyesi: {yeni_sl}")
                    except Exception:
                        pass
        except Exception:
            pass

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
            
            scalp_listesi = scan_scalp_market(exchange)
            firsat_listesi = scan_opportunity_market(exchange)
            
            # Puanlama Listelerini Loglara Detaylı Yazdır
            logging.info("--- SCALP MODU ADAY PUANLAMA LİSTESİ ---")
            if scalp_listesi:
                for idx, c in enumerate(scalp_listesi, 1):
                    logging.info(f"{idx}. {c['symbol']} | Yön: {c['direction'].upper()} | Skor: {c['score']}")
            else:
                logging.info("Scalp kriterlerine uyan aday bulunamadı.")

            logging.info("--- FIRSAT MODU TAKİP/PUANLAMA LİSTESİ ---")
            if firsat_listesi:
                for idx, c in enumerate(firsat_listesi, 1):
                    logging.info(f"{idx}. {c['symbol']} | Yön: {c['direction'].upper()} | Skor: {c['score']} (Teyit bekleniyor)")
            else:
                logging.info("Fırsat kriterlerine uyan aday bulunamadı.")
            
            for item in scalp_listesi:
                if item['score'] >= 92:
                    item['mode'] = 'opportunity'
                    if not any(f['symbol'] == item['symbol'] for f in firsat_listesi):
                        firsat_listesi.append(item)
                        logging.info(f"ÇAPRAZ KÖPRÜ: {item['symbol']} Scalp modundan Fırsat takip listesine aktarıldı!")

            positions = exchange.fetch_positions()
            active_pos = [p for p in positions if float(p.get("contracts") or 0) > 0]
            
            firsat_var = any(pozisyon_tipleri.get(sembol_duzelt(p.get("symbol"))) == "opportunity" for p in active_pos)
            scalp_var = any(pozisyon_tipleri.get(sembol_duzelt(p.get("symbol"))) == "scalp" for p in active_pos)
            
            # Fırsat Modu: Takip listesindekiler teyit ve pullback şartını geçerse işleme girilir
            if not firsat_var and firsat_listesi:
                for candidate in firsat_listesi:
                    sym = candidate['symbol']
                    dir_val = candidate['direction']
                    df_check = candidate.get('df')
                    if df_check is None:
                        df_check = ohlcv_getir(exchange, sym, '1h', 70)
                    
                    if df_check is not None and check_pullback_and_confirmation(df_check, dir_val):
                        logging.info(f"[FIRSAT TEYİT GEÇTİ] {sym} için pullback onayı alındı, işleme giriliyor...")
                        pozisyon_ac(exchange, sym, dir_val, candidate['score'], "opportunity")
                        if df_check is not None: del df_check
                        break
                    else:
                        logging.info(f"[FIRSAT BEKLİYOR] {sym} takipte, henüz pullback teyidi alınmadı.")
                        if df_check is not None: del df_check

            # Scalp Modu: En iyi scalp adayı ile işlem açılır
            if not scalp_var and scalp_listesi:
                best_s = scalp_listesi[0]
                pozisyon_ac(exchange, best_s['symbol'], best_s['direction'], best_s['score'], "scalp")

        except Exception as e:
            logging.error(f"Ana döngü hatası: {e}")
        finally:
            gc.collect()
            
        time.sleep(120)

# ============================================================
# FLASK WEB ENDPOINTLERİ & AKTİF İŞLEM LİSTELEME
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
            p_type = pozisyon_tipleri.get(sym, "Bilinmiyor")
            entry = float(p.get("entryPrice", 0))
            mark = float(p.get("markPrice", 0))
            lev = float(p.get("leverage", 1))
            side = p.get("side")
            roi = ((mark - entry) / entry) * 100 * lev if side == "long" else ((entry - mark) / entry) * 100 * lev
            max_kar = pozisyon_en_yuksek_kar.get(sym, 0.0)
            
            detaylar.append({
                "symbol": sym,
                "mod": p_type.upper(),
                "yon": side.upper(),
                "giris_fiyati": entry,
                "anlik_fiyat": mark,
                "kaldirac": lev,
                "roi_yuzde": round(roi, 2),
                "max_gorulen_kar": round(max_kar, 2)
            })
            
        return jsonify({"success": True, "aktif_islem_sayisi": len(detaylar), "islemler": detaylar})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/tetikle")
def tetikle():
    threading.Thread(target=ana_tarama_dongusu, daemon=True).start()
    return jsonify({"success": True, "message": "Manuel tetikleme başarılı."})

@app.route("/otomatik-analiz")
def otomatik_analiz():
    threading.Thread(target=ana_tarama_dongusu, daemon=True).start()
    return jsonify({"success": True, "message": "Otomatik analiz cron tetiklemesi başarılı."})

if __name__ == "__main__":
    t = threading.Thread(target=ana_tarama_dongusu, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
