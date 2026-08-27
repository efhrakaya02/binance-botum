import os
import ccxt
import pandas as pd
import numpy as np
import time
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify

# ============================================================
# BINANCE FUTURES BOT - KATI LİMİT VE YETİM EMİR TEMİZLİKLİ SÜRÜM
# ============================================================

app = Flask(__name__)

API_KEY = os.getenv("BINANCE_API_KEY")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    print("UYARI: BINANCE_API_KEY veya BINANCE_SECRET_KEY eksik!", flush=True)

# ============================================================
# GÜVENLİK VE ÇALIŞMA MODLARI
# ============================================================
TRADING_ENABLED = True
POSITION_MONITOR_ENABLED = True

# ============================================================
# AYARLAR VE STRATEJİ PARAMETRELERİ
# ============================================================
SCALP_ENABLED = True
SCALP_MARGIN = 10.0      # Scalp işlemleri için 10 USDT teminat
MAX_SCALP_POSITIONS = 1
SCALP_TP_ROI = 3.0       # %3 ROI hedeflenir

OPPORTUNITY_ENABLED = True
OPPORTUNITY_MARGIN = 12.0  # Fırsat işlemleri için 12 USDT teminat
MAX_OPPORTUNITY_POSITIONS = 1

MAX_TOTAL_POSITIONS = 2 

OPPORTUNITY_MIN_SCORE = 68
SCALP_MIN_SCORE = 75

COOLDOWN_HOURS = 4
cooldown_ms = COOLDOWN_HOURS * 60 * 60 * 1000
son_kapanis_zamanlari = {}

POSITION_MONITOR_INTERVAL = 2.0

# ============================================================
# RUNTIME STATE
# ============================================================
pozisyon_en_yuksek_kar = {}
pozisyon_tipleri = {}
pozisyon_kapatma_lock = threading.Lock()
pozisyon_monitor_lock = threading.Lock()
islem_acma_lock = threading.Lock()
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
            "adjustForTimeDifference": True
        }
    })

def islem_izni_var_mi():
    if not TRADING_ENABLED:
        return False
    return True

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

def cooldown_aktif_mi(symbol):
    son_zaman = son_kapanis_zamanlari.get(symbol, 0)
    simdiki_zaman = int(time.time() * 1000)
    if simdiki_zaman - son_zaman < cooldown_ms:
        return True
    return False

# ============================================================
# İNDİKATÖRLER
# ============================================================
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def hesapla_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def hesapla_atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = abs(df["high"] - df["close"].shift())
    low_close = abs(df["low"] - df["close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()

def hesapla_adx(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/period, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/period, adjust=False).mean() / atr
    denominator = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * abs(plus_di - minus_di) / denominator
    adx = dx.ewm(alpha=1 / period, adjust=False).mean()
    return adx, plus_di, minus_di

def hesapla_macd(df):
    ema12 = ema(df["close"], 12)
    ema26 = ema(df["close"], 26)
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    histogram = macd - signal
    return macd, signal, histogram

def hesapla_obv(df):
    direction = np.sign(df["close"].diff()).fillna(0)
    return (direction * df["volume"]).cumsum()

def teknik_indikatorleri_hesapla(df):
    df = df.copy()
    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["ema50"] = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["rsi"] = hesapla_rsi(df["close"], 14)
    df["atr"] = hesapla_atr(df, 14)
    df["adx"], df["plus_di"], df["minus_di"] = hesapla_adx(df, 14)
    df["macd"], df["macd_signal"], df["macd_hist"] = hesapla_macd(df)
    df["obv"] = hesapla_obv(df)
    df["obv_ma"] = df["obv"].rolling(20).mean()
    df["bb_mid"] = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    df["bb_upper"] = df["bb_mid"] + 2 * std
    df["bb_lower"] = df["bb_mid"] - 2 * std
    df["volume_ma20"] = df["volume"].rolling(20).mean()
    df["volume_ratio"] = df["volume"] / df["volume_ma20"]
    df["recent_high"] = df["high"].rolling(20).max().shift(1)
    df["recent_low"] = df["low"].rolling(20).min().shift(1)
    return df

def ohlcv_getir(exchange, symbol, timeframe, limit=250):
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not data or len(data) < 50:
            return None
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        return teknik_indikatorleri_hesapla(df)
    except Exception as e:
        return None

# ============================================================
# SIKI SKORLAMA VE ÇOK KATMANLI TEYİT MEKANİZMASI
# ============================================================
def skorla_coin(exchange, symbol):
    result = {
        "symbol": symbol, "long_score": 0, "short_score": 0,
        "direction": None, "score": 0, "atr": None, "price": None, "funding": 0
    }
    try:
        try:
            funding_data = exchange.fetch_funding_rate(symbol)
            funding = float(funding_data.get("fundingRate", 0) or 0)
            result["funding"] = funding
            if abs(funding) >= 0.0015:
                return None
        except Exception:
            funding = 0

        df15 = ohlcv_getir(exchange, symbol, "15m", 150)
        df1 = ohlcv_getir(exchange, symbol, "1h", 250)
        df4 = ohlcv_getir(exchange, symbol, "4h", 250)

        if df15 is None or df1 is None or df4 is None:
            return None

        d15, d1, d4 = df15.iloc[-2], df1.iloc[-2], df4.iloc[-2]
        price, atr = float(d15["close"]), float(d15["atr"])
        result["price"], result["atr"] = price, atr

        if not np.isfinite(price) or not np.isfinite(atr) or (atr / price * 100) > 10:
            return None

        if float(d15["volume_ratio"]) < 0.70:
            return None

        trend4_long = (d4["close"] > d4["ema50"]) and (d4["ema50"] > d4["ema200"])
        trend4_short = (d4["close"] < d4["ema50"]) and (d4["ema50"] < d4["ema200"])
        trend1_long = (d1["close"] > d1["ema50"]) and (d1["ema9"] > d1["ema21"])
        trend1_short = (d1["close"] < d1["ema50"]) and (d1["ema9"] < d1["ema21"])
        
        adx = float(d1["adx"])
        if adx < 18:
            return None

        rsi15 = float(d15["rsi"])
        long_score, short_score = 0, 0

        can_long = rsi15 < 72
        can_short = rsi15 > 28

        if can_long:
            if trend4_long: long_score += 18
            if trend1_long: long_score += 16
            if d15["macd"] > d15["macd_signal"]: long_score += 10
            if d15["plus_di"] > d15["minus_di"]: long_score += 8
            if adx >= 25: long_score += 8
            if 48 <= rsi15 <= 65: long_score += 12
            if d15["obv"] > d15["obv_ma"]: long_score += 8
            if price > d15["recent_high"]: long_score += 10

        if can_short:
            if trend4_short: short_score += 18
            if trend1_short: short_score += 16
            if d15["macd"] < d15["macd_signal"]: short_score += 10
            if d15["minus_di"] > d15["plus_di"]: short_score += 8
            if adx >= 25: short_score += 8
            if 35 <= rsi15 <= 52: short_score += 12
            if d15["obv"] < d15["obv_ma"]: short_score += 8
            if price < d15["recent_low"]: short_score += 10

        if long_score >= short_score:
            result["direction"], result["score"] = "buy", long_score
        else:
            result["direction"], result["score"] = "sell", short_score

        if abs(long_score - short_score) < 8:
            return None

        return result
    except Exception as e:
        return None

# ============================================================
# KALDIRAÇ VE İŞLEM YÖNETİMİ
# ============================================================
def kaldirac_belirle(score):
    if score >= 90:
        return 5
    elif score >= 80:
        return 3
    return 3

def miktar_hesapla(exchange, symbol, margin, leverage, price):
    market = exchange.market(symbol)
    min_amount = market.get("limits", {}).get("amount", {}).get("min", 0.001)
    notional = margin * leverage
    raw_amount = notional / price
    amount = max(raw_amount, float(min_amount))
    try:
        return float(exchange.amount_to_precision(symbol, amount))
    except Exception:
        return float(amount)

def isolated_ve_kaldirac_ayarla(exchange, symbol, leverage):
    if not islem_izni_var_mi():
        return False
    try:
        exchange.set_margin_mode("isolated", symbol)
    except Exception:
        pass
    try:
        exchange.set_leverage(leverage, symbol)
        return True
    except Exception as e:
        return False

def pozisyon_ac(exchange, symbol, direction, score, p_type):
    if not islem_izni_var_mi():
        return False
    
    with islem_acma_lock:
        try:
            positions = exchange.fetch_positions()
            active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
            
            if len(active_positions) >= MAX_TOTAL_POSITIONS:
                print(f"[ENGELLENDİ] Borsa üzerinde zaten {len(active_positions)} aktif pozisyon var. Yeni işlem açılmadı.", flush=True)
                return False

            for p in active_positions:
                if sembol_duzelt(p.get("symbol")) == symbol:
                    print(f"[ENGELLENDİ] {symbol} üzerinde zaten açık pozisyon mevcut.", flush=True)
                    return False

            leverage = kaldirac_belirle(score)
            if not isolated_ve_kaldirac_ayarla(exchange, symbol, leverage):
                return False

            ticker = exchange.fetch_ticker(symbol)
            price = float(ticker["last"])
            
            margin = SCALP_MARGIN if p_type == "scalp" else OPPORTUNITY_MARGIN
            amount = miktar_hesapla(exchange, symbol, margin, leverage, price)

            side = "buy" if direction == "buy" else "sell"
            order = exchange.create_order(symbol, "market", side, amount, None, {"leverage": leverage})
            
            if order:
                pozisyon_tipleri[symbol] = p_type
                pozisyon_en_yuksek_kar[symbol] = 0.0
                print(f"[İŞLEM AÇILDI] {p_type.upper()} | {symbol} {side.upper()} | Puan: {score} | Teminat: {margin} USDT | Kaldıraç: {leverage}x", flush=True)
                
                time.sleep(1)
                try:
                    close_side = "sell" if side == "buy" else "buy"
                    
                    if side == "buy":
                        tp_price = price * (1 + (SCALP_TP_ROI / 100) / leverage)
                        sl_price = price * (1 - 2.5 / 100 / leverage)
                    else:
                        tp_price = price * (1 - (SCALP_TP_ROI / 100) / leverage)
                        sl_price = price * (1 + 2.5 / 100 / leverage)
                    
                    tp_price = float(exchange.price_to_precision(symbol, tp_price))
                    sl_price = float(exchange.price_to_precision(symbol, sl_price))

                    exchange.create_order(symbol, 'take_profit_market', close_side, amount, None, {
                        'stopPrice': tp_price,
                        'reduceOnly': True
                    })
                    
                    exchange.create_order(symbol, 'stop_market', close_side, amount, None, {
                        'stopPrice': sl_price,
                        'reduceOnly': True
                    })
                    print(f"[TP/SL AYARLANDI] {symbol} | TP: {tp_price} | SL: {sl_price}", flush=True)
                except Exception as tp_err:
                    print(f"[TP/SL HATA] {symbol}: {tp_err}", flush=True)

                return True
        except Exception as e:
            print(f"[İŞLEM AÇMA HATA] {symbol}: {e}", flush=True)
        return False

def market_pozisyon_kapat(exchange, symbol, side, amount, sebep):
    if not islem_izni_var_mi():
        return False
    with pozisyon_kapatma_lock:
        try:
            # İşlem kapatılırken veya kapanmadan önce o coine ait tüm bekleyen ek emirleri (TP/SL) temizle
            try:
                exchange.cancel_all_orders(symbol)
            except Exception:
                pass

            close_side = "sell" if side == "buy" else "buy"
            exchange.create_order(symbol, "market", close_side, abs(amount), None, {'reduceOnly': True})
            son_kapanis_zamanlari[symbol] = int(time.time() * 1000)
            if symbol in pozisyon_en_yuksek_kar:
                del pozisyon_en_yuksek_kar[symbol]
            if symbol in pozisyon_tipleri:
                del pozisyon_tipleri[symbol]
            print(f"[POZİSYON KAPATILDI] {symbol} | Sebep: {sebep} (Bekleyen ek emirler temizlendi)", flush=True)
            return True
        except Exception as e:
            print(f"[KAPATMA HATA] {symbol}: {e}", flush=True)
            return False

def pozisyonlari_yonet(exchange, positions):
    # Aktif pozisyon sembollerini takip etmek için küme oluşturuyoruz
    aktif_semboller = {sembol_duzelt(p.get("symbol")) for p in positions if float(p.get("contracts") or 0) > 0}

    # Eğer daha önceden botun hafızasında olan ancak borsada artık kontratı kalmamış (TP/SL ile kapanmış) coin varsa, ek emirlerini temizle
    for sym in list(pozisyon_tipleri.keys()):
        if sym not in aktif_semboller:
            try:
                exchange.cancel_all_orders(sym)
                print(f"[YETİM EMİR TEMİZLİĞİ] {sym} pozisyonu kapanmış, arkada kalan ek emirler iptal edildi.", flush=True)
            except Exception:
                pass
            if sym in pozisyon_tipleri:
                del pozisyon_tipleri[sym]
            if sym in pozisyon_en_yuksek_kar:
                del pozisyon_en_yuksek_kar[sym]

    for p in positions:
        symbol = sembol_duzelt(p.get("symbol"))
        try:
            contracts = float(p.get("contracts") or 0)
            if contracts <= 0:
                continue
            
            side = p.get("side") 
            entry_price = float(p.get("entryPrice") or 0)
            mark_price = float(p.get("markPrice") or 0)
            leverage = float(p.get("leverage") or 1)
            
            if entry_price == 0 or mark_price == 0:
                continue

            if side == "long":
                roi = ((mark_price - entry_price) / entry_price) * 100 * leverage
            else:
                roi = ((entry_price - mark_price) / entry_price) * 100 * leverage

            p_type = pozisyon_tipleri.get(symbol, "opportunity")
            current_max = pozisyon_en_yuksek_kar.get(symbol, 0.0)
            if roi > current_max:
                pozisyon_en_yuksek_kar[symbol] = roi
                current_max = roi

            if p_type == "opportunity" and current_max >= 3.0 and roi <= (current_max * 0.65):
                market_pozisyon_kapat(exchange, symbol, "buy" if side == "long" else "sell", contracts, f"Fırsat Kar Kilitleme (Max: %{current_max:.2f})")

        except Exception as e:
            pass

# ============================================================
# POZİSYON MONİTÖRÜ
# ============================================================
def pozisyon_monitor_loop():
    global monitor_basladi
    if not POSITION_MONITOR_ENABLED or monitor_basladi:
        return
    monitor_basladi = True
    print("[MONITOR] Pozisyon takip ve kar koruma sistemi aktifleşti.", flush=True)
    
    exchange = None
    while True:
        try:
            if exchange is None:
                exchange = get_exchange()
                exchange.load_markets()

            if pozisyon_monitor_lock.acquire(blocking=False):
                try:
                    positions = exchange.fetch_positions()
                    # Tüm pozisyonları göndererek hem kar yönetimi hem de kapanan pozisyonların emir temizliğini yap
                    pozisyonlari_yonet(exchange, positions)
                finally:
                    pozisyon_monitor_lock.release()
        except Exception as e:
            exchange = None
        time.sleep(POSITION_MONITOR_INTERVAL)

def monitor_baslat():
    if POSITION_MONITOR_ENABLED:
        thread = threading.Thread(target=pozisyon_monitor_loop, daemon=True, name="PositionMonitor")
        thread.start()

# ============================================================
# ANA TARAMA DÖNGÜSÜ, PUANLAMA VE ANLIK İŞLEM RAPORU
# ============================================================
def piyasa_tara_ve_islem_yap():
    exchange = get_exchange()
    try:
        exchange.load_markets()
        tickers = exchange.fetch_tickers()
        coin_listesi = []
        
        for symbol, ticker in tickers.items():
            if gecerli_kripto_mu(symbol):
                yuzde_degisim = ticker.get("percentage", 0) or 0
                coin_listesi.append({
                    "symbol": symbol, 
                    "change": float(yuzde_degisim)
                })
                
        coin_listesi.sort(key=lambda x: x["change"], reverse=True)
        
        gainers = [item["symbol"] for item in coin_listesi[:25]]
        losers = [item["symbol"] for item in coin_listesi[-25:]]
        hedef_coini_listesi = list(set(gainers + losers))
        
        print(f"[TARAMA] Toplam {len(hedef_coini_listesi)} adet hareketli coin katı süzgeçten geçiriliyor...", flush=True)
        
    except Exception as e:
        print(f"[PİYASA LİSTE HATA]: {e}", flush=True)
        return

    try:
        positions = exchange.fetch_positions()
        active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
        aktif_sayisi = len(active_positions)
        
        scalp_var = False
        firsat_var = False
        
        for p in active_positions:
            p_sym = sembol_duzelt(p.get("symbol"))
            p_contracts = float(p.get("contracts") or 0)
            p_entry = float(p.get("entryPrice") or 0)
            p_lev = float(p.get("leverage") or 1)
            approx_margin = (p_contracts * p_entry) / p_lev if p_lev > 0 else 0
            
            if approx_margin >= 11.0:
                firsat_var = True
                pozisyon_tipleri[p_sym] = "opportunity"
            else:
                scalp_var = True
                pozisyon_tipleri[p_sym] = "scalp"
                
    except Exception as e:
        print(f"[POZİSYON SORGULAMA HATA]: {e}", flush=True)
        active_positions = []
        aktif_sayisi = 0
        scalp_var, firsat_var = False, False

    adaylar = []
    for symbol in hedef_coini_listesi:
        if cooldown_aktif_mi(symbol):
            continue
        res = skorla_coin(exchange, symbol)
        if res and res["score"] >= OPPORTUNITY_MIN_SCORE:
            adaylar.append(res)

    if adaylar:
        adaylar.sort(key=lambda x: x["score"], reverse=True)

        print("\n" + "="*50, flush=True)
        print("📊 TEYİT EDİLMİŞ PİYASA ANALİZ SONUÇLARI VE PUAN TABLOSU", flush=True)
        print("="*50, flush=True)
        
        firsat_adaylari = adaylar[:5]
        print("🏆 ANA FIRSAT İÇİN EN İYİ İLK 5 ADAY (12 USDT Margin):", flush=True)
        for i, c in enumerate(firsat_adaylari, 1):
            print(f"  {i}. {c['symbol']} | Yön: {c['direction'].upper()} | Puan: {c['score']} | Fiyat: {c['price']}", flush=True)

        scalp_adaylari = [c for c in adaylar if c['score'] >= SCALP_MIN_SCORE][:5]
        if scalp_adaylari:
            print("\n⚡ SCALP İÇİN EN İYİ İLK 5 ADAY (10 USDT Margin):", flush=True)
            for i, c in enumerate(scalp_adaylari, 1):
                print(f"  {i}. {c['symbol']} | Yön: {c['direction'].upper()} | Puan: {c['score']} | Fiyat: {c['price']}", flush=True)
        else:
            print("\n⚡ SCALP İÇİN EN İYİ İLK 5 ADAY: (75+ Teyitli scalp adayı bulunamadı)", flush=True)
        print("="*50 + "\n", flush=True)
    else:
        print("[ANALİZ] Kriterleri ve süzgeçleri sağlayan uygun coin bulunamadı.", flush=True)

    print("="*50, flush=True)
    print(f"📌 ANLIK AÇIK İŞLEM DURUMU (Aktif İşlem Sayısı: {aktif_sayisi}/{MAX_TOTAL_POSITIONS})", flush=True)
    print("="*50, flush=True)
    if active_positions:
        for p in active_positions:
            p_sym = sembol_duzelt(p.get("symbol"))
            p_side = str(p.get("side")).upper()
            p_entry = float(p.get("entryPrice") or 0)
            p_mark = float(p.get("markPrice") or 0)
            p_lev = float(p.get("leverage") or 1)
            p_type_val = pozisyon_tipleri.get(p_sym, "BİLİNMİYOR").upper()
            
            if p_side == "LONG":
                p_roi = ((p_mark - p_entry) / p_entry) * 100 * p_lev
            else:
                p_roi = ((p_entry - p_mark) / p_entry) * 100 * p_lev
                
            print(f"  • Tip: {p_type_val} | Coin: {p_sym} | Yön: {p_side} | Giriş: {p_entry} | Mark: {p_mark} | Kaldıraç: {p_lev}x | Anlık Kar (ROI): %{p_roi:.2f}", flush=True)
    else:
        print("  • Şu an borsada açık aktif bir işlem bulunmuyor.", flush=True)
    print("="*50 + "\n", flush=True)

    if aktif_sayisi >= MAX_TOTAL_POSITIONS:
        return

    if not adaylar:
        return

    for aday in adaylar:
        try:
            current_check_pos = exchange.fetch_positions()
            active_check_count = len([p for p in current_check_pos if float(p.get("contracts") or 0) > 0])
            if active_check_count >= MAX_TOTAL_POSITIONS:
                break
        except Exception:
            pass

        s = aday["symbol"]
        score = aday["score"]
        
        if score >= SCALP_MIN_SCORE and not scalp_var and not (s in pozisyon_tipleri):
            if pozisyon_ac(exchange, s, aday["direction"], score, "scalp"):
                scalp_var = True
                aktif_sayisi += 1
                break 
        elif not firsat_var and not (s in pozisyon_tipleri):
            if pozisyon_ac(exchange, s, aday["direction"], score, "opportunity"):
                firsat_var = True
                aktif_sayisi += 1
                break

# ============================================================
# FLASK ENDPOINTLERİ
# ============================================================
@app.route("/")
def index():
    return jsonify({
        "status": "Bot Kesintisiz Çalışıyor (Yetim Emir Temizliği Aktif)", 
        "trading_enabled": TRADING_ENABLED, 
        "monitor_active": POSITION_MONITOR_ENABLED
    })

@app.route("/otomatik-analiz")
def otomatik_analiz_tetikle():
    try:
        threading.Thread(target=piyasa_tara_ve_islem_yap, daemon=True).start()
        return jsonify({"success": True, "message": "Emir temizlikli tarama tetiklendi."}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

def bot_ana_dongu():
    monitor_baslat()
    while True:
        try:
            piyasa_tara_ve_islem_yap()
        except Exception as e:
            print(f"[ANA DÖNGÜ HATA]: {e}", flush=True)
        time.sleep(60)

if __name__ == "__main__":
    t = threading.Thread(target=bot_ana_dongu, daemon=True)
    t.start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
