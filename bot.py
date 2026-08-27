import os
import ccxt
import pandas as pd
import numpy as np
import time
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify

# ============================================================
# BINANCE FUTURES BOT - 30M, ATR STOP, %4 ROI & FORMASYON AVCISI
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
SCALP_MARGIN = 15.0      
MAX_SCALP_POSITIONS = 1
SCALP_TP_ROI = 4.0       

OPPORTUNITY_ENABLED = True
OPPORTUNITY_MARGIN = 20.0  
MAX_OPPORTUNITY_POSITIONS = 1

MAX_TOTAL_POSITIONS = 2 

MINIMUM_PROCESS_SCORE = 75  # Formasyon avcılığı için taban puan esnetildi
SCALP_MIN_SCORE = 80

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
# İNDİKATÖRLER VE FORMASYON TESPİT MOTORU
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

# --- FORMASYON TESPİT FONKSİYONLARI ---
def formasyonlari_tespit_et(df):
    """
    OBO, TOBO, Bayrak (Bull Flag) ve Ters Bayrak (Bear Flag) tespiti yapar.
    Dönüş değerleri: 'tobo', 'obo', 'bull_flag', 'bear_flag', None
    """
    if len(df) < 40:
        return None, None

    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values
    volumes = df["volume"].values

    # Son 30 mumluk dar alan
    recent_lows = lows[-30:]
    recent_highs = highs[-30:]
    recent_closes = closes[-30:]

    # 1. TOBO Tespiti (Ters Omuz Baş Omuz - Dip Dönüşü)
    # Ortadaki dip (baş) en düşük nokta olmalı, sol ve sağ omuzlar ona yakın yükseklikte olmalı
    min_idx = np.argmin(recent_lows)
    if 8 <= min_idx <= 22: # Baş ortada yer almalı
        head = recent_lows[min_idx]
        left_shoulder = np.min(recent_lows[:min_idx])
        right_shoulder = np.min(recent_lows[min_idx+1:])
        
        # Baş, omuzlardan belirgin şekilde aşağıda olmalı ve omuzlar birbirine yakın olmalı
        if head < left_shoulder and head < right_shoulder and abs(left_shoulder - right_shoulder) / left_shoulder < 0.03:
            if recent_closes[-1] > np.mean([left_shoulder, right_shoulder]): # Boyun çizgisi yukarı kırılmışsa
                return "tobo", "buy"

    # 2. OBO Tespiti (Omuz Baş Omuz - Tepe Dönüşü)
    max_idx = np.argmax(recent_highs)
    if 8 <= max_idx <= 22:
        head_h = recent_highs[max_idx]
        left_h = np.max(recent_highs[:max_idx])
        right_h = np.max(recent_highs[max_idx+1:])
        
        if head_h > left_h and head_h > right_h and abs(left_h - right_h) / left_h < 0.03:
            if recent_closes[-1] < np.mean([left_h, right_h]): # Boyun çizgisi aşağı kırılmışsa
                return "obo", "sell"

    # 3. Boğa Bayrağı (Bull Flag - Sert Yükseliş Sonrası Yatay Sıkışma & Devam)
    # Önceki 15 mum sert yükselmiş, son 15 mum hafif aşağı dar bir kanalda dinleniyor
    price_change_prev = (closes[-20] - closes[-40]) / closes[-40]
    if price_change_prev > 0.05: # Son 20 mumda %5+ ralli
        flag_volatility = np.std(recent_closes[-10:]) / np.mean(recent_closes[-10:])
        if flag_volatility < 0.015 and recent_closes[-1] > recent_closes[-5]: # Sıkışma bitip tekrar yukarı kırıyor
            return "bull_flag", "buy"

    # 4. Ters Bayrak (Bear Flag - Sert Düşüş Sonrası Yatay Tepki & Düşüşün Devamı)
    price_drop_prev = (closes[-40] - closes[-20]) / closes[-40]
    if price_drop_prev > 0.05: # Son 20 mumda %5+ sert düşüş
        flag_volatility_bear = np.std(recent_closes[-10:]) / np.mean(recent_closes[-10:])
        if flag_volatility_bear < 0.015 and recent_closes[-1] < recent_closes[-5]: # Tepki bitti ve aşağı kırıyor
            return "bear_flag", "sell"

    return None, None

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
    df["volume_ma20"] = df["volume"].rolling(20).mean()
    df["volume_ratio"] = df["volume"] / df["volume_ma20"]
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
# BTC KORELASYON KONTROLÜ
# ============================================================
def btc_egilimini_getir(exchange):
    try:
        df_btc = ohlcv_getir(exchange, "BTC/USDT", "1h", 50)
        if df_btc is None or len(df_btc) < 10:
            return "neutral"
        b_last = df_btc.iloc[-2]
        if b_last["close"] > b_last["ema50"] and b_last["ema9"] > b_last["ema21"]:
            return "bullish"
        elif b_last["close"] < b_last["ema50"] and b_last["ema9"] < b_last["ema21"]:
            return "bearish"
        return "neutral"
    except Exception:
        return "neutral"

# ============================================================
# FORMASYON DESTEKLİ 30M PULLBACK VE SKORLAMA
# ============================================================
def skorla_coin(exchange, symbol, btc_trend):
    result = {
        "symbol": symbol, "long_score": 0, "short_score": 0,
        "direction": None, "score": 0, "atr": None, "price": None, "formasyon": None
    }
    try:
        try:
            funding_data = exchange.fetch_funding_rate(symbol)
            funding = float(funding_data.get("fundingRate", 0) or 0)
            if abs(funding) >= 0.0012:
                return None
        except Exception:
            funding = 0

        df30 = ohlcv_getir(exchange, symbol, "30m", 150)
        df1 = ohlcv_getir(exchange, symbol, "1h", 250)
        df4 = ohlcv_getir(exchange, symbol, "4h", 250)

        if df30 is None or df1 is None or df4 is None:
            return None

        d30, d1, d4 = df30.iloc[-2], df1.iloc[-2], df4.iloc[-2]
        price, atr = float(d30["close"]), float(d30["atr"])
        result["price"], result["atr"] = price, atr

        if not np.isfinite(price) or not np.isfinite(atr) or (atr / price * 100) > 10:
            return None

        # Formasyon Tespiti Çağrısı
        formasyon_adi, formasyon_yonu = formasyonlari_tespit_et(df30)
        result["formasyon"] = formasyon_adi

        vol_ratio = float(d30["volume_ratio"])
        if vol_ratio < 1.3 and not formasyon_adi: # Formasyon varsa hacim esnetilir
            return None

        trend4_long = (d4["close"] > d4["ema50"]) and (d4["ema50"] > d4["ema200"])
        trend4_short = (d4["close"] < d4["ema50"]) and (d4["ema50"] < d4["ema200"])
        trend1_long = (d1["close"] > d1["ema50"]) and (d1["ema9"] > d1["ema21"])
        trend1_short = (d1["close"] < d1["ema50"]) and (d1["ema9"] < d1["ema21"])
        
        adx = float(d1["adx"])
        if adx < 16 and not formasyon_adi:
            return None

        rsi30 = float(d30["rsi"])
        long_score, short_score = 0, 0

        can_long = rsi30 < 72
        can_short = rsi30 > 28

        is_pullback_long = (price <= d30["ema9"]) and (price >= d30["ema21"])
        is_pullback_short = (price >= d30["ema9"]) and (price <= d30["ema21"])

        if can_long:
            if btc_trend == "bullish": long_score += 10
            if trend4_long: long_score += 15
            if trend1_long: long_score += 15
            if d30["macd"] > d30["macd_signal"]: long_score += 10
            if is_pullback_long: long_score += 15
            
            # Formasyon Avcısı Puanı (TOBO veya Boğa Bayrağı yakalandıysa +25 Puan)
            if formasyon_adi in ["tobo", "bull_flag"] and formasyon_yonu == "buy":
                long_score += 28
                print(f"[AVLANAN FORMASYON] {symbol} -> {formasyon_adi.upper()} Yakalandı! (+28 Puan)", flush=True)

        if can_short:
            if btc_trend == "bearish": short_score += 10
            if trend4_short: short_score += 15
            if trend1_short: short_score += 15
            if d30["macd"] < d30["macd_signal"]: short_score += 10
            if is_pullback_short: short_score += 15
            
            # Formasyon Avcısı Puanı (OBO veya Ters Bayrak yakalandıysa +25 Puan)
            if formasyon_adi in ["obo", "bear_flag"] and formasyon_yonu == "sell":
                short_score += 28
                print(f"[AVLANAN FORMASYON] {symbol} -> {formasyon_adi.upper()} Yakalandı! (+28 Puan)", flush=True)

        if long_score >= short_score:
            result["direction"], result["score"] = "buy", long_score
        else:
            result["direction"], result["score"] = "sell", short_score

        if abs(long_score - short_score) < 8 and not formasyon_adi:
            return None

        return result
    except Exception as e:
        return None

# ============================================================
# KALDIRAÇ VE İŞLEM YÖNETİMİ
# ============================================================
def kaldirac_belirle(score):
    if score >= 90: return 5
    elif score >= 80: return 3
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
    if not islem_izni_var_mi(): return False
    try:
        exchange.set_margin_mode("isolated", symbol)
    except Exception:
        pass
    try:
        exchange.set_leverage(leverage, symbol)
        return True
    except Exception:
        return False

def pozisyon_ac(exchange, symbol, direction, score, p_type):
    if not islem_izni_var_mi(): return False
    
    with islem_acma_lock:
        try:
            positions = exchange.fetch_positions()
            active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
            
            if len(active_positions) >= MAX_TOTAL_POSITIONS:
                return False

            for p in active_positions:
                if sembol_duzelt(p.get("symbol")) == symbol:
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
                    df_temp = ohlcv_getir(exchange, symbol, "30m", 30)
                    current_atr = float(df_temp.iloc[-1]["atr"]) if df_temp is not None else (price * 0.01)
                    
                    guvenli_tp_roi = SCALP_TP_ROI * 1.15 
                    
                    if side == "buy":
                        tp_price = price * (1 + (guvenli_tp_roi / 100) / leverage)
                        sl_price = price - (current_atr * 1.8)
                    else:
                        tp_price = price * (1 - (guvenli_tp_roi / 100) / leverage)
                        sl_price = price + (current_atr * 1.8)
                    
                    tp_price = float(exchange.price_to_precision(symbol, tp_price))
                    sl_price = float(exchange.price_to_precision(symbol, sl_price))

                    exchange.create_order(symbol, 'take_profit_market', close_side, amount, None, {
                        'stopPrice': tp_price, 'reduceOnly': True
                    })
                    exchange.create_order(symbol, 'stop_market', close_side, amount, None, {
                        'stopPrice': sl_price, 'reduceOnly': True
                    })
                    print(f"[TP/SL AYARLANDI (%{SCALP_TP_ROI} ROI)] {symbol} | TP: {tp_price} | SL: {sl_price}", flush=True)
                except Exception as tp_err:
                    print(f"[TP/SL HATA] {symbol}: {tp_err}", flush=True)

                return True
        except Exception as e:
            print(f"[İŞLEM AÇMA HATA] {symbol}: {e}", flush=True)
        return False

def market_pozisyon_kapat(exchange, symbol, side, amount, sebep):
    if not islem_izni_var_mi(): return False
    with pozisyon_kapatma_lock:
        try:
            try:
                exchange.cancel_all_orders(symbol)
            except Exception:
                pass

            close_side = "sell" if side == "buy" else "buy"
            exchange.create_order(symbol, "market", close_side, abs(amount), None, {'reduceOnly': True})
            son_kapanis_zamanlari[symbol] = int(time.time() * 1000)
            if symbol in pozisyon_en_yuksek_kar: del pozisyon_en_yuksek_kar[symbol]
            if symbol in pozisyon_tipleri: del pozisyon_tipleri[symbol]
            print(f"[POZİSYON KAPATILDI] {symbol} | Sebep: {sebep}", flush=True)
            return True
        except Exception as e:
            return False

def pozisyonlari_yonet(exchange, positions):
    aktif_semboller = {sembol_duzelt(p.get("symbol")) for p in positions if float(p.get("contracts") or 0) > 0}
    for sym in list(pozisyon_tipleri.keys()):
        if sym not in aktif_semboller:
            try:
                exchange.cancel_all_orders(sym)
            except Exception:
                pass
            if sym in pozisyon_tipleri: del pozisyon_tipleri[sym]
            if sym in pozisyon_en_yuksek_kar: del pozisyon_en_yuksek_kar[sym]

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

            roi = ((mark_price - entry_price) / entry_price) * 100 * leverage if side == "long" else ((entry_price - mark_price) / entry_price) * 100 * leverage
            p_type = pozisyon_tipleri.get(symbol, "opportunity")
            current_max = pozisyon_en_yuksek_kar.get(symbol, 0.0)
            if roi > current_max: pozisyon_en_yuksek_kar[symbol] = roi
        except Exception:
            pass

# ============================================================
# POZİSYON MONİTÖRÜ
# ============================================================
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
                    pozisyonlari_yonet(exchange, positions)
                finally:
                    pozisyon_monitor_lock.release()
        except Exception:
            exchange = None
        time.sleep(POSITION_MONITOR_INTERVAL)

def monitor_baslat():
    if POSITION_MONITOR_ENABLED:
        threading.Thread(target=pozisyon_monitor_loop, daemon=True, name="PositionMonitor").start()

# ============================================================
# ANA TARAMA DÖNGÜSÜ
# ============================================================
def piyasa_tara_ve_islem_yap():
    exchange = get_exchange()
    try:
        exchange.load_markets()
        btc_trend = btc_egilimini_getir(exchange)
        tickers = exchange.fetch_tickers()
        coin_listesi = []
        
        for symbol, ticker in tickers.items():
            if gecerli_kripto_mu(symbol):
                coin_listesi.append({"symbol": symbol, "change": float(ticker.get("percentage", 0) or 0)})
                
        coin_listesi.sort(key=lambda x: x["change"], reverse=True)
        hedef_coini_listesi = list(set([i["symbol"] for i in coin_listesi[:25]] + [i["symbol"] for i in coin_listesi[-25:]]))
        print(f"[TARAMA] Toplam {len(hedef_coini_listesi)} coin Formasyon Avcısı motoruyla taranıyor...", flush=True)
    except Exception as e:
        return

    try:
        positions = exchange.fetch_positions()
        active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
        aktif_sayisi = len(active_positions)
        scalp_var = any(float(p.get("contracts") or 0) * float(p.get("entryPrice") or 0) / float(p.get("leverage") or 1) < 18.0 for p in active_positions)
    except Exception:
        active_positions = []
        aktif_sayisi = 0
        scalp_var = False

    adaylar = []
    for symbol in hedef_coini_listesi:
        if cooldown_aktif_mi(symbol): continue
        res = skorla_coin(exchange, symbol, btc_trend)
        if res and res["score"] >= MINIMUM_PROCESS_SCORE:
            adaylar.append(res)

    if adaylar:
        adaylar.sort(key=lambda x: x["score"], reverse=True)
        print("\n" + "="*50, flush=True)
        print("🎯 FORMASYON AVCISI TARAMA SONUÇLARI", flush=True)
        print("="*50, flush=True)
        for i, c in enumerate(adaylar[:5], 1):
            form_str = f" [Formasyon: {c['formasyon'].upper()}]" if c['formasyon'] else ""
            print(f"  {i}. {c['symbol']} | Yön: {c['direction'].upper()} | Puan: {c['score']}{form_str}", flush=True)
        print("="*50 + "\n", flush=True)

    if aktif_sayisi >= MAX_TOTAL_POSITIONS or not adaylar: return

    for aday in adaylar:
        s = aday["symbol"]
        score = aday["score"]
        if score >= SCALP_MIN_SCORE and not scalp_var and not (s in pozisyon_tipleri):
            if pozisyon_ac(exchange, s, aday["direction"], score, "scalp"):
                break

# ============================================================
# FLASK ENDPOINTLERİ
# ============================================================
@app.route("/")
def index():
    return jsonify({"status": "Bot Çalışıyor (Formasyon Avcısı & 30M Sürüm)"})

@app.route("/otomatik-analiz")
def otomatik_analiz_tetikle():
    try:
        threading.Thread(target=piyasa_tara_ve_islem_yap, daemon=True).start()
        return jsonify({"success": True, "message": "Formasyon taraması tetiklendi."}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

def bot_ana_dongu():
    monitor_baslat()
    while True:
        try:
            piyasa_tara_ve_islem_yap()
        except Exception:
            pass
        time.sleep(60)

if __name__ == "__main__":
    t = threading.Thread(target=bot_ana_dongu, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
