import os
import ccxt
import pandas as pd
import numpy as np
import time
import threading
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify

# ============================================================
# BINANCE FUTURES BOT - 5M & 15M TEYİT, TOP 3 VE KORELASYON KORUMASI
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
# AYARLAR VE KESİN İŞLEM LİMİTLERİ (85 PUAN KİLİTLİ)
# ============================================================
SCALP_ENABLED = True
SCALP_MARGIN = 10.0            # Kesin kural: Scalp için tam 10 USDT
MAX_SCALP_POSITIONS = 1        # En fazla 1 adet Scalp

# --- SCALP 0.30 USDT NET KAR HEDEFİ ---
SCALP_TARGET_PROFIT_USDT = 0.30

OPPORTUNITY_ENABLED = True
OPPORTUNITY_MARGIN = 12.0      # Kesin kural: Fırsat için tam 12 USDT
MAX_OPPORTUNITY_POSITIONS = 1  # En fazla 1 adet Fırsat

MAX_TOTAL_POSITIONS = 2        # Toplamda kesinlikle aynı anda maksimum 2 işlem

MINIMUM_PROCESS_SCORE = 85  
SCALP_MIN_SCORE = 85

# --- COOLDOWN / 4 SAATLİK MUM KISITLAMASI ---
cooldown_4h_tracker = {}       # { "symbol_direction": son_giris_4h_timestamp }

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

# ============================================================
# ORGANİK BAĞLI / KORELE COİN KONTROLÜ (Örn: BTC & BCH)
# ============================================================
def organik_bag_kontrolu(exchange, symbol, direction):
    try:
        positions = exchange.fetch_positions()
        active_pos = [p for p in positions if float(p.get("contracts") or 0) > 0]
        
        bagli_gruplar = [
            ["BTC/USDT", "BCH/USDT", "BTCUSDT", "BCHUSDT"]
        ]
        
        target_group = None
        for grup in bagli_gruplar:
            if any(sembol_duzelt(symbol) in g for g in grup):
                target_group = grup
                break
                
        if target_group:
            for p in active_pos:
                p_sym = sembol_duzelt(p.get("symbol"))
                p_side = str(p.get("side")).lower()
                
                if any(p_sym in g for g in target_group):
                    mapped_direction = "buy" if p_side == "long" else "sell"
                    if mapped_direction == direction:
                        print(f"[ORGANİK BAĞ FİLTRESİ] {symbol} ({direction.upper()}), gruptaki {p_sym} ile aynı yönde. İşlem engellendi.", flush=True)
                        return False
    except Exception:
        pass
    return True

# ============================================================
# 4 SAATLİK MUM TABANLI COOLDOWN KONTROLÜ
# ============================================================
def can_open_position_4h_cooldown(exchange, symbol, direction):
    key = f"{symbol}_{direction}"
    if key not in cooldown_4h_tracker:
        return True
    
    last_trade_4h_ts = cooldown_4h_tracker[key]
    try:
        df_4h = ohlcv_getir(exchange, symbol, "4h", 5)
        if df_4h is not None and not df_4h.empty:
            current_last_4h_ts = int(df_4h.iloc[-1]["timestamp"])
            if current_last_4h_ts <= last_trade_4h_ts:
                print(f"[4H COOLDOWN AKTİF] {symbol} ({direction.upper()}) | Henüz yeni bir 4H mum kapanmadı. İşlem engellendi.", flush=True)
                return False
    except Exception:
        pass
    return True

def record_trade_4h_cooldown(exchange, symbol, direction):
    key = f"{symbol}_{direction}"
    try:
        df_4h = ohlcv_getir(exchange, symbol, "4h", 5)
        if df_4h is not None and not df_4h.empty:
            cooldown_4h_tracker[key] = int(df_4h.iloc[-1]["timestamp"])
            return
    except Exception:
        pass
    cooldown_4h_tracker[key] = int(time.time() * 1000)

# ============================================================
# İNDİKATÖRLER VE FORMASYON / PUMP-DUMP TESPİT MOTORU
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

def formasyon_ve_volatilite_avcisi(df):
    if len(df) < 40:
        return None, None

    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values

    recent_lows = lows[-30:]
    recent_highs = highs[-30:]
    recent_closes = closes[-30:]

    pump_check = (closes[-5] - closes[-20]) / closes[-20]
    if pump_check > 0.12 and recent_highs[-1] > recent_closes[-1] * 1.03: 
        return "sert_dump_reversal", "sell"

    dump_check = (closes[-20] - closes[-5]) / closes[-20]
    if dump_check > 0.12 and recent_lows[-1] < recent_closes[-1] * 0.97: 
        return "sert_pump_reversal", "buy"

    min_idx = np.argmin(recent_lows)
    if 8 <= min_idx <= 22:
        head = recent_lows[min_idx]
        left_shoulder = np.min(recent_lows[:min_idx])
        right_shoulder = np.min(recent_lows[min_idx+1:])
        if head < left_shoulder and head < right_shoulder and abs(left_shoulder - right_shoulder) / left_shoulder < 0.04:
            if recent_closes[-1] > np.mean([left_shoulder, right_shoulder]):
                return "tobo", "buy"

    max_idx = np.argmax(recent_highs)
    if 8 <= max_idx <= 22:
        head_h = recent_highs[max_idx]
        left_h = np.max(recent_highs[:max_idx])
        right_h = np.max(recent_highs[max_idx+1:])
        if head_h > left_h and head_h > right_h and abs(left_h - right_h) / left_h < 0.04:
            if recent_closes[-1] < np.mean([left_h, right_h]):
                return "obo", "sell"

    if (closes[-20] - closes[-40]) / closes[-40] > 0.05:
        if np.std(recent_closes[-10:]) / np.mean(recent_closes[-10:]) < 0.015 and recent_closes[-1] > recent_closes[-5]:
            return "bull_flag", "buy"

    if (closes[-40] - closes[-20]) / closes[-40] > 0.05:
        if np.std(recent_closes[-10:]) / np.mean(recent_closes[-10:]) < 0.015 and recent_closes[-1] < recent_closes[-5]:
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

def btc_egilimini_getir(exchange):
    try:
        df_btc = ohlcv_getir(exchange, "BTC/USDT", "1h", 50)
        if df_btc is None or len(df_btc) < 10:
            return "neutral", None
        b_last = df_btc.iloc[-2]
        trend = "neutral"
        if b_last["close"] > b_last["ema50"] and b_last["ema9"] > b_last["ema21"]:
            trend = "bullish"
        elif b_last["close"] < b_last["ema50"] and b_last["ema9"] < b_last["ema21"]:
            trend = "bearish"
        return trend, df_btc
    except Exception:
        return "neutral", None

# ============================================================
# GELİŞMİŞ TEYİT VE DOĞRULAMA MODÜLÜ
# ============================================================
def btc_korelasyon_kontrolu(df_alt, df_btc, yon):
    if df_alt is None or df_btc is None or len(df_alt) < 10 or len(df_btc) < 10:
        return True, 0  
    
    alt_returns = df_alt["close"].pct_change().tail(10)
    btc_returns = df_btc["close"].pct_change().tail(10)
    
    corr = alt_returns.corr(btc_returns)
    
    if yon == "buy":
        if corr < -0.65: 
            return False, -15
        elif corr > 0.3:
            return True, 10
    else: 
        if corr > 0.7 and btc_returns.iloc[-1] > 0: 
            return False, -20
        elif corr < 0 or btc_returns.iloc[-1] < 0:
            return True, 10
            
    return True, 0

def destek_direnc_konum_teyidi(df, price, atr, yon):
    recent_high = df["high"].iloc[-50:].max()
    recent_low = df["low"].iloc[-50:].min()
    
    puan_katkisi = 0
    if yon == "buy":
        ema50 = df["ema50"].iloc[-2]
        dist_to_support = abs(price - recent_low)
        if dist_to_support < (1.5 * atr) or abs(price - ema50) < (1.0 * atr):
            puan_katkisi += 15
    else:
        ema50 = df["ema50"].iloc[-2]
        dist_to_resistance = abs(recent_high - price)
        if dist_to_resistance < (1.5 * atr) or abs(price - ema50) < (1.0 * atr):
            puan_katkisi += 15
            
    return puan_katkisi, ""

# ============================================================
# SKORLAMA VE DETAYLI LOGLAMA MOTORU
# ============================================================
def skorla_coin(exchange, symbol, btc_trend, df_btc):
    result = {
        "symbol": symbol, "long_score": 0, "short_score": 0,
        "direction": None, "score": 0, "atr": None, "price": None, "formasyon": None, "is_firsat": False
    }
    try:
        try:
            funding_data = exchange.fetch_funding_rate(symbol)
            funding = float(funding_data.get("fundingRate", 0) or 0)
            if abs(funding) >= 0.0015:
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

        formasyon_adi, formasyon_yonu = formasyon_ve_volatilite_avcisi(df30)
        result["formasyon"] = formasyon_adi

        vol_ratio = float(d30["volume_ratio"])
        if vol_ratio < 1.3 and not formasyon_adi:
            return None

        rsi30 = float(d30["rsi"])
        long_score, short_score = 0, 0

        if formasyon_adi:
            result["is_firsat"] = True
            if formasyon_yonu == "buy": long_score += 35
            else: short_score += 35

        trend4_long = (d4["close"] > d4["ema50"])
        trend4_short = (d4["close"] < d4["ema50"])
        trend1_long = (d1["close"] > d1["ema50"]) and (d1["ema9"] > d1["ema21"])
        trend1_short = (d1["close"] < d1["ema50"]) and (d1["ema9"] < d1["ema21"])

        if rsi30 < 72:
            if trend4_long: long_score += 15
            if trend1_long: long_score += 15
            if d30["macd"] > d30["macd_signal"]: long_score += 10
            if (price <= d30["ema9"]) and (price >= d30["ema21"]): long_score += 15

        if rsi30 > 28:
            if trend4_short: short_score += 15
            if trend1_short: short_score += 15
            if d30["macd"] < d30["macd_signal"]: short_score += 10
            if (price >= d30["ema9"]) and (price <= d30["ema21"]): short_score += 15

        temp_dir = "buy" if long_score >= short_score else "sell"

        if temp_dir == "buy" and funding < 0: long_score += 10
        elif temp_dir == "sell" and funding > 0.0005: short_score += 10

        sr_puan, _ = destek_direnc_konum_teyidi(df30, price, atr, temp_dir)
        if temp_dir == "buy": long_score += sr_puan
        else: short_score += sr_puan

        kor_gecerli, kor_puan = btc_korelasyon_kontrolu(df30, df_btc, temp_dir)
        if not kor_gecerli: return None
        
        if temp_dir == "buy": long_score += kor_puan
        else: short_score += kor_puan

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
# ANLIK 5M VE 15M MUM GÖVDE VE TREND UYUM TEYİDİ
# ============================================================
def anlik_giris_zamanlama_teyidi(exchange, symbol, direction):
    try:
        # 5m ve 15m verilerini çek
        df_5m = ohlcv_getir(exchange, symbol, "5m", 15)
        df_15m = ohlcv_getir(exchange, symbol, "15m", 15)

        if df_5m is not None and len(df_5m) >= 3:
            last_5m = df_5m.iloc[-1]
            body_5m = last_5m['close'] - last_5m['open']
            atr_5m = last_5m.get('atr', 0)
            
            # 5m ters yönde büyük mum kontrolü
            if direction == "buy" and body_5m < 0 and abs(body_5m) > (atr_5m * 0.4):
                print(f"[5M TEYİDİ REDDİ] {symbol} (BUY) | 5m mum ters yönde güçlü.", flush=True)
                return False
            elif direction == "sell" and body_5m > 0 and body_5m > (atr_5m * 0.4):
                print(f"[5M TEYİDİ REDDİ] {symbol} (SELL) | 5m mum ters yönde güçlü.", flush=True)
                return False

        if df_15m is not None and len(df_15m) >= 3:
            last_15m = df_15m.iloc[-1]
            body_15m = last_15m['close'] - last_15m['open']
            atr_15m = last_15m.get('atr', 0)
            
            # 15m ters yönde büyük mum ve trend uyum kontrolü
            if direction == "buy" and body_15m < 0 and abs(body_15m) > (atr_15m * 0.4):
                print(f"[15M TEYİDİ REDDİ] {symbol} (BUY) | 15m mum ters yönde güçlü.", flush=True)
                return False
            elif direction == "sell" and body_15m > 0 and body_15m > (atr_15m * 0.4):
                print(f"[15M TEYİDİ REDDİ] {symbol} (SELL) | 15m mum ters yönde güçlü.", flush=True)
                return False

        return True
    except Exception:
        return True

# ============================================================
# KALDIRAÇ VE İŞLEM YÖNETİMİ
# ============================================================
def kaldirac_belirle(score):
    if score >= 92: return 5
    elif score >= 85: return 4
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
    
    # 4 Saatlik Mum Tabanlı Cooldown Koruması
    if not can_open_position_4h_cooldown(exchange, symbol, direction):
        return False

    # Organik Bağ / Korelasyon Kontrolü (Örn: BTC ve BCH aynı yönde olamaz)
    if not organik_bag_kontrolu(exchange, symbol, direction):
        return False

    # 5m ve 15m Doğru Yer / Zaman Teyidi
    if not anlik_giris_zamanlama_teyidi(exchange, symbol, direction):
        return False

    with islem_acma_lock:
        try:
            positions = exchange.fetch_positions()
            active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
            
            if len(active_positions) >= MAX_TOTAL_POSITIONS:
                return False

            for p in active_positions:
                if sembol_duzelt(p.get("symbol")) == symbol:
                    return False

            if p_type == "opportunity":
                mevcut_firsat = sum(1 for p in active_positions if sembol_duzelt(p.get("symbol")) in pozisyon_tipleri and pozisyon_tipleri[sembol_duzelt(p.get("symbol"))] == "opportunity")
                if mevcut_firsat >= MAX_OPPORTUNITY_POSITIONS:
                    return False
            elif p_type == "scalp":
                mevcut_scalp = sum(1 for p in active_positions if sembol_duzelt(p.get("symbol")) in pozisyon_tipleri and pozisyon_tipleri[sembol_duzelt(p.get("symbol"))] == "scalp")
                if mevcut_scalp >= MAX_SCALP_POSITIONS:
                    return False

            leverage = kaldirac_belirle(score)
            if not isolated_ve_kaldirac_ayarla(exchange, symbol, leverage):
                return False

            ticker = exchange.fetch_ticker(symbol)
            price = float(ticker["last"])
            
            margin = OPPORTUNITY_MARGIN if p_type == "opportunity" else SCALP_MARGIN
            amount = miktar_hesapla(exchange, symbol, margin, leverage, price)

            side = "buy" if direction == "buy" else "sell"
            order = exchange.create_order(symbol, "market", side, amount, None, {"leverage": leverage})
            
            if order:
                pozisyon_tipleri[symbol] = p_type
                pozisyon_en_yuksek_kar[symbol] = 0.0
                
                record_trade_4h_cooldown(exchange, symbol, direction)
                print(f"[İŞLEM AÇILDI] {p_type.upper()} | {symbol} {side.upper()} | Puan: {score} | Teminat: {margin} USDT", flush=True)
                
                time.sleep(1)
                try:
                    close_side = "sell" if side == "buy" else "buy"
                    df_temp = ohlcv_getir(exchange, symbol, "30m", 30)
                    current_atr = float(df_temp.iloc[-1]["atr"]) if df_temp is not None else (price * 0.01)
                    
                    if p_type == "scalp":
                        fiyat_farki = SCALP_TARGET_PROFIT_USDT / amount
                        if side == "buy":
                            tp_price = price + fiyat_farki
                            sl_price = price - (current_atr * 2.0)
                        else:
                            tp_price = price - fiyat_farki
                            sl_price = price + (current_atr * 2.0)
                        
                        tp_price = float(exchange.price_to_precision(symbol, tp_price))
                        sl_price = float(exchange.price_to_precision(symbol, sl_price))

                        exchange.create_order(symbol, 'take_profit_market', close_side, amount, None, {
                            'stopPrice': tp_price, 'reduceOnly': True
                        })
                        exchange.create_order(symbol, 'stop_market', close_side, amount, None, {
                            'stopPrice': sl_price, 'reduceOnly': True
                        })
                    else:
                        if side == "buy":
                            sl_price = price - (current_atr * 2.0)
                        else:
                            sl_price = price + (current_atr * 2.0)
                        
                        sl_price = float(exchange.price_to_precision(symbol, sl_price))
                        exchange.create_order(symbol, 'stop_market', close_side, amount, None, {
                            'stopPrice': sl_price, 'reduceOnly': True
                        })

                except Exception as tp_err:
                    pass

                return True
        except Exception as e:
            print(f"[İŞLEM AÇMA HATA] {symbol}: {e}", flush=True)
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

            approx_margin = (contracts * entry_price) / leverage if leverage > 0 else 0
            if symbol not in pozisyon_tipleri:
                if approx_margin >= 11.0: pozisyon_tipleri[symbol] = "opportunity"
                else: pozisyon_tipleri[symbol] = "scalp"

            if pozisyon_tipleri.get(symbol) == "opportunity":
                roi = ((mark_price - entry_price) / entry_price) * 100 * leverage if side == "long" else ((entry_price - mark_price) / entry_price) * 100 * leverage
                current_max = pozisyon_en_yuksek_kar.get(symbol, 0.0)
                
                if roi > current_max:
                    pozisyon_en_yuksek_kar[symbol] = roi
                    current_max = roi

                yeni_sl = None
                if current_max >= 8.0:
                    yeni_sl = mark_price * (1 - 0.03 / leverage) if side == "long" else mark_price * (1 + 0.03 / leverage)
                elif current_max >= 4.0:
                    yeni_sl = entry_price * (1 + 0.005 / leverage) if side == "long" else entry_price * (1 - 0.005 / leverage)

                if yeni_sl is not None:
                    try:
                        exchange.cancel_all_orders(symbol)
                        close_side = "sell" if side == "long" else "buy"
                        yeni_sl = float(exchange.price_to_precision(symbol, yeni_sl))
                        exchange.create_order(symbol, 'stop_market', close_side, contracts, None, {
                            'stopPrice': yeni_sl, 'reduceOnly': True
                        })
                    except Exception:
                        pass
        except Exception:
            pass

# ============================================================
# POZİSYON MONİTÖRÜ VE TAKİP PANELİ
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
                    active_pos = [p for p in positions if float(p.get("contracts") or 0) > 0]
                    
                    if active_pos:
                        print("\n========== [AKTİF İŞLEMLER TAKİP PANELİ] ==========", flush=True)
                        for p in active_pos:
                            sym = sembol_duzelt(p.get("symbol"))
                            p_type = pozisyon_tipleri.get(sym, "bilinmiyor").upper()
                            side = str(p.get("side")).upper()
                            entry = float(p.get("entryPrice") or 0)
                            mark = float(p.get("markPrice") or 0)
                            lev = float(p.get("leverage") or 1)
                            unrealized_pnl = float(p.get("unrealizedPnl") or 0)
                            roi = ((mark - entry) / entry) * 100 * lev if side == "LONG" else ((entry - mark) / entry) * 100 * lev
                            print(f" * [{p_type}] {sym} | Yön: {side} | Kaldıraç: {lev}x | Giriş: {entry} | Anlık: {mark} | ROI: %{roi:.2f} | PnL: {unrealized_pnl:.2f} USDT", flush=True)
                        print("==================================================\n", flush=True)

                    pozisyonlari_yonet(exchange, active_pos)
                finally:
                    pozisyon_monitor_lock.release()
        except Exception:
            exchange = None
        time.sleep(POSITION_MONITOR_INTERVAL)

def monitor_baslat():
    if POSITION_MONITOR_ENABLED:
        threading.Thread(target=pozisyon_monitor_loop, daemon=True, name="PositionMonitor").start()

# ============================================================
# ANA TARAMA DÖNGÜSÜ (TOP 3 VE 5M/15M TEYİTLİ SEÇİM)
# ============================================================
def piyasa_tara_ve_islem_yap():
    exchange = get_exchange()
    try:
        exchange.load_markets()
        btc_trend, df_btc = btc_egilimini_getir(exchange)
        tickers = exchange.fetch_tickers()
        coin_listesi = []
        
        for symbol, ticker in tickers.items():
            if gecerli_kripto_mu(symbol):
                coin_listesi.append({"symbol": symbol, "change": float(ticker.get("percentage", 0) or 0)})
                
        coin_listesi.sort(key=lambda x: x["change"], reverse=True)
        gainers_25 = [i["symbol"] for i in coin_listesi[:25]]
        losers_25 = [i["symbol"] for i in coin_listesi[-25:]]
        hedef_coini_listesi = list(set(gainers_25 + losers_25))
    except Exception as e:
        return

    try:
        positions = exchange.fetch_positions()
        active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
        aktif_sayisi = len(active_positions)
        
        scalp_var, firsat_var = False, False
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
    except Exception:
        aktif_sayisi = 0
        scalp_var, firsat_var = False, False

    scalp_adaylari = []
    firsat_adaylari = []

    for symbol in hedef_coini_listesi:
        res = skorla_coin(exchange, symbol, btc_trend, df_btc)
        if res and res["score"] >= MINIMUM_PROCESS_SCORE:
            if res["is_firsat"]:
                firsat_adaylari.append(res)
            else:
                scalp_adaylari.append(res)

    scalp_adaylari.sort(key=lambda x: x["score"], reverse=True)
    firsat_adaylari.sort(key=lambda x: x["score"], reverse=True)

    print("\n--- [EN İYİ 3 SCALP ANALİZ ADAYI] ---", flush=True)
    top_scalp = scalp_adaylari[:3]
    if top_scalp:
        for sa in top_scalp:
            print(f" -> {sa['symbol']} | Yön: {sa['direction'].upper()} | Puan: {sa['score']}", flush=True)
    else:
        print(" (Uygun Scalp adayı bulunamadı)", flush=True)

    print("--- [EN İYİ 3 FIRSAT ANALİZ ADAYI] ---", flush=True)
    top_firsat = firsat_adaylari[:3]
    if top_firsat:
        for fa in top_firsat:
            print(f" -> {fa['symbol']} | Formasyon: {fa['formasyon']} | Yön: {fa['direction'].upper()} | Puan: {fa['score']}", flush=True)
    else:
        print(" (Uygun Fırsat adayı bulunamadı)", flush=True)
    print("---------------------------------------\n", flush=True)

    if aktif_sayisi >= MAX_TOTAL_POSITIONS: 
        return

    if not firsat_var and top_firsat:
        for aday in top_firsat:
            s = aday["symbol"]
            score = aday["score"]
            if not (s in pozisyon_tipleri) and score >= SCALP_MIN_SCORE:
                if pozisyon_ac(exchange, s, aday["direction"], score, "opportunity"):
                    firsat_var = True
                    break
        
    if not scalp_var and top_scalp:
        for aday in top_scalp:
            s = aday["symbol"]
            score = aday["score"]
            if not (s in pozisyon_tipleri) and score >= SCALP_MIN_SCORE:
                if pozisyon_ac(exchange, s, aday["direction"], score, "scalp"):
                    scalp_var = True
                    break

# ============================================================
# FLASK ENDPOINTLERİ
# ============================================================
@app.route("/")
def index():
    return jsonify({"status": "Bot Çalışıyor (Top 3 Aday, 5m & 15m Zamanlama Teyidi Aktif)"})

@app.route("/tetikle")
@app.route("/otomatik-analiz")
def otomatik_analiz_tetikle():
    try:
        threading.Thread(target=piyasa_tara_ve_islem_yap, daemon=True).start()
        return jsonify({"success": True, "message": "Tarama, Top 3 analizi ve 5m/15m teyit tetiklendi."}), 200
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
