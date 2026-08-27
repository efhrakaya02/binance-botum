import os
import ccxt
import pandas as pd
import numpy as np
import time
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify

# ============================================================
# BINANCE FUTURES BOT - GÜNCELLENMİŞ KAR KORUMA SÜRÜMÜ
#
# Bu sürümde:
# - Maksimum 1 Fırsat + 1 Scalp olmak üzere TOPLAM 2 pozisyon sınırı.
# - Her işlem için SABİT 10 USDT Margin.
# - Kaldıraç aralığı: Min 3x, Max 5x.
# - Aktif pozisyon takip döngüsü (Position Monitor) AÇIK.
# - Scalp için net kar al (TP) ve Stop Loss (SL).
# - Fırsat için dinamik Stop yükseltme (Trailing / Karı Kilitleme).
# ============================================================

app = Flask(__name__)

API_KEY = os.getenv("BINANCE_API_KEY")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    print("UYARI: BINANCE_API_KEY veya BINANCE_SECRET_KEY eksik!", flush=True)

# ============================================================
# GÜVENLİK VE ÇALIŞMA MODLARI
# ============================================================
# Canlı işlem için True yapabilirsiniz. Test için False bırakın.
TRADING_ENABLED = True
POSITION_MONITOR_ENABLED = True

# ============================================================
# GÜNCELLENMİŞ YENİ AYARLAR
# ============================================================
SCALP_ENABLED = True
SCALP_MARGIN = 10.0
MAX_SCALP_POSITIONS = 1

SCALP_TP_ROI = 3.0  # %3 Kar Al
SCALP_SL_ROI = -1.5 # %1.5 Zarar Durdur

OPPORTUNITY_ENABLED = True
OPPORTUNITY_MARGIN = 10.0 # İstediğiniz gibi 10 USDT yapıldı
MAX_OPPORTUNITY_POSITIONS = 1

MAX_TOTAL_POSITIONS = 2 # Toplam en fazla 2 pozisyon (1 Scalp + 1 Fırsat)

OPPORTUNITY_MIN_SCORE = 68
SCALP_MIN_SCORE = 72

MIN_LEVERAGE = 3
MAX_LEVERAGE = 5 # İstediğiniz gibi max 5x ile sınırlandı

COOLDOWN_HOURS = 4
cooldown_ms = COOLDOWN_HOURS * 60 * 60 * 1000
son_kapanis_zamanlari = {}

POSITION_MONITOR_INTERVAL = 2.0

# ============================================================
# RUNTIME STATE
# ============================================================
pozisyon_en_yuksek_kar = {}
pozisyon_stoplari = {}
pozisyon_tipleri = {}
pozisyon_kapatma_lock = threading.Lock()
analiz_lock = threading.Lock()
pozisyon_monitor_lock = threading.Lock()
monitor_basladi = False

MACRO_BLOCK_WINDOWS_UTC = os.getenv("MACRO_BLOCK_WINDOWS_UTC", "").strip()

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
        print("[GÜVENLİK] TRADING_ENABLED=False -> Emir gönderimi engellendi.", flush=True)
        return False
    return True

def sembol_duzelt(symbol):
    if symbol == "BCC/USDT":
        return "BCH/USDT"
    return symbol

def gecerli_kripto_mu(symbol):
    yasakli = ["UP/", "DOWN/", "BEAR/", "BULL/", "_", "BID", "ASK"]
    if not symbol.endswith("/USDT"):
        return False
    for yasak in yasakli:
        if yasak in symbol:
            return False
    return True

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
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]
    df["bb_width_ma"] = df["bb_width"].rolling(50).mean()
    df["squeeze"] = df["bb_width"] < df["bb_width_ma"] * 0.85
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
        print(f"[OHLCV HATA] {symbol} {timeframe}: {e}", flush=True)
        return None

# ============================================================
# SKORLAMA VE ANALİZ
# ============================================================
def skorla_coin(exchange, symbol):
    result = {
        "symbol": symbol, "long_score": 0, "short_score": 0,
        "long_reasons": [], "short_reasons": [], "direction": None,
        "score": 0, "atr": None, "price": None, "funding": 0
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

        df5 = ohlcv_getir(exchange, symbol, "5m", 100)
        df15 = ohlcv_getir(exchange, symbol, "15m", 100)
        df1 = ohlcv_getir(exchange, symbol, "1h", 250)
        df4 = ohlcv_getir(exchange, symbol, "4h", 250)

        if df5 is None or df15 is None or df1 is None or df4 is None:
            return None

        d5, d15, d1, d4 = df5.iloc[-2], df15.iloc[-2], df1.iloc[-2], df4.iloc[-2]
        price, atr = float(d5["close"]), float(d1["atr"])
        result["price"], result["atr"] = price, atr

        if not np.isfinite(price) or not np.isfinite(atr) or (atr / price * 100) > 12:
            return None

        volume_ratio_5 = float(d5["volume_ratio"])
        if volume_ratio_5 < 0.65:
            return None

        trend4_long = (d4["close"] > d4["ema50"]) and (d4["ema50"] > d4["ema200"])
        trend4_short = (d4["close"] < d4["ema50"]) and (d4["ema50"] < d4["ema200"])
        trend1_long = (d1["close"] > d1["ema50"]) and (d1["ema9"] > d1["ema21"])
        trend1_short = (d1["close"] < d1["ema50"]) and (d1["ema9"] < d1["ema21"])
        adx = float(d1["adx"])
        if adx < 15:
            return None

        rsi1 = float(d1["rsi"])
        long_score, short_score = 0, 0
        long_reasons, short_reasons = [], []

        if trend4_long: long_score += 18
        if trend1_long: long_score += 16
        if d1["macd"] > d1["macd_signal"]: long_score += 10
        if d1["plus_di"] > d1["minus_di"]: long_score += 8
        if adx >= 25: long_score += 8
        if 48 <= rsi1 <= 65: long_score += 10
        if d1["obv"] > d1["obv_ma"]: long_score += 8
        if price > d1["recent_high"]: long_score += 10

        if trend4_short: short_score += 18
        if trend1_short: short_score += 16
        if d1["macd"] < d1["macd_signal"]: short_score += 10
        if d1["minus_di"] > d1["plus_di"]: short_score += 8
        if adx >= 25: short_score += 8
        if 35 <= rsi1 <= 52: short_score += 10
        if d1["obv"] < d1["obv_ma"]: short_score += 8
        if price < d1["recent_low"]: short_score += 10

        if long_score >= short_score:
            result["direction"], result["score"], result["reasons"] = "buy", long_score, long_reasons
        else:
            result["direction"], result["score"], result["reasons"] = "sell", short_score, short_reasons

        if abs(long_score - short_score) < 8:
            return None

        return result
    except Exception as e:
        print(f"[SKOR HATA] {symbol}: {e}", flush=True)
        return None

# ============================================================
# KALDIRAÇ VE MİKTAR
# ============================================================
def kaldirac_belirle(score):
    # En iyi puanlı işlem için maksimum 5x, en az 3x
    if score >= 85:
        return 5
    elif score >= 75:
        return 4
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
        print(f"[LEVERAGE HATA] {symbol}: {e}", flush=True)
        return False

# ============================================================
# İŞLEM AÇMA VE POZİSYON YÖNETİMİ (KAR KORUMA ODAKLI)
# ============================================================
def pozisyon_ac(exchange, symbol, direction, score, p_type):
    if not islem_izni_var_mi():
        return False
    try:
        leverage = kaldirac_belirle(score)
        if not isolated_ve_kaldirac_ayarla(exchange, symbol, leverage):
            return False

        ticker = exchange.fetch_ticker(symbol)
        price = float(ticker["last"])
        
        # Sabit 10 USDT Margin
        margin = SCALP_MARGIN if p_type == "scalp" else OPPORTUNITY_MARGIN
        amount = miktar_hesapla(exchange, symbol, margin, leverage, price)

        side = "buy" if direction == "buy" else "sell"
        order = exchange.create_order(symbol, "market", side, amount, None, {"leverage": leverage})
        
        if order:
            pozisyon_tipleri[symbol] = p_type
            pozisyon_en_yuksek_kar[symbol] = 0.0
            print(f"[İŞLEM AÇILDI] {p_type.upper()} | {symbol} {side.upper()} | Miktar: {amount} | Kaldıraç: {leverage}x", flush=True)
            return True
    except Exception as e:
        print(f"[İŞLEM AÇMA HATA] {symbol}: {e}", flush=True)
    return False

def market_pozisyon_kapat(exchange, symbol, side, amount, sebep):
    if not islem_izni_var_mi():
        return False
    with pozisyon_kapatma_lock:
        try:
            close_side = "sell" if side == "buy" else "buy"
            exchange.create_order(symbol, "market", close_side, abs(amount))
            son_kapanis_zamanlari[symbol] = int(time.time() * 1000)
            if symbol in pozisyon_en_yuksek_kar:
                del pozisyon_en_yuksek_kar[symbol]
            if symbol in pozisyon_tipleri:
                del pozisyon_tipleri[symbol]
            print(f"[POZİSYON KAPATILDI] {symbol} | Sebep: {sebep}", flush=True)
            return True
        except Exception as e:
            print(f"[KAPATMA HATA] {symbol}: {e}", flush=True)
            return False

def pozisyonlari_yonet(exchange, positions):
    for p in positions:
        symbol = sembol_duzelt(p.get("symbol"))
        try:
            contracts = float(p.get("contracts") or 0)
            if contracts <= 0:
                continue
            
            side = p.get("side") # 'long' veya 'short'
            entry_price = float(p.get("entryPrice") or 0)
            mark_price = float(p.get("markPrice") or 0)
            leverage = float(p.get("leverage") or 1)
            
            if entry_price == 0 or mark_price == 0:
                continue

            # ROI (Return on Investment) Hesaplama
            if side == "long":
                roi = ((mark_price - entry_price) / entry_price) * 100 * leverage
            else:
                roi = ((entry_price - mark_price) / entry_price) * 100 * leverage

            p_type = pozisyon_tipleri.get(symbol, "opportunity")

            # En yüksek karı hafızada tut
            current_max = pozisyon_en_yuksek_kar.get(symbol, 0.0)
            if roi > current_max:
                pozisyon_en_yuksek_kar[symbol] = roi
                current_max = roi

            print(f"[MONITOR] {symbol} ({p_type}) | ROI: %{roi:.2f} | Max Kar: %{current_max:.2f}", flush=True)

            # ================= SCALP MODU YÖNETİMİ =================
            if p_type == "scalp":
                if roi >= SCALP_TP_ROI:
                    market_pozisyon_kapat(exchange, symbol, "buy" if side == "long" else "sell", contracts, f"Scalp TP Hedefi (%{SCALP_TP_ROI})")
                elif roi <= SCALP_SL_ROI:
                    market_pozisyon_kapat(exchange, symbol, "buy" if side == "long" else "sell", contracts, f"Scalp Stop Loss (%{SCALP_SL_ROI})")

            # ================= FIRSAT MODU (STOP YÜKSELTME & KAR KİLİTLEME) =================
            elif p_type == "opportunity":
                # Eğer kar %2.5 üzerine çıktıysa ve sonradan düşmeye başladıysa karı korumak için kapat veya stop yükselt
                if current_max >= 2.5 and roi <= (current_max * 0.60):
                    market_pozisyon_kapat(exchange, symbol, "buy" if side == "long" else "sell", contracts, f"Fırsat Kar Kilitleme (Max: %{current_max:.2f} -> Güncel: %{roi:.2f})")
                elif roi <= -2.0: # Sabit genel güvenlik stopu
                    market_pozisyon_kapat(exchange, symbol, "buy" if side == "long" else "sell", contracts, "Fırsat Acil Stop Loss (%-2)")

        except Exception as e:
            print(f"[YÖNETİM HATA] {symbol}: {e}", flush=True)

# ============================================================
# POZİSYON MONİTÖR LOOP
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
                    active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
                    if active_positions:
                        pozisyonlari_yonet(exchange, active_positions)
                finally:
                    pozisyon_monitor_lock.release()
        except Exception as e:
            print(f"[MONITOR HATA] {e}", flush=True)
            exchange = None
        time.sleep(POSITION_MONITOR_INTERVAL)

def monitor_baslat():
    if POSITION_MONITOR_ENABLED:
        thread = threading.Thread(target=pozisyon_monitor_loop, daemon=True, name="PositionMonitor")
        thread.start()

# ============================================================
# TARAMA VE ANA DÖNGÜ (1 FIRSAT + 1 SCALP)
# ============================================================
def piyasa_tara_ve_islem_yap():
    exchange = get_exchange()
    try:
        exchange.load_markets()
    except Exception:
        return

    symbols = [s for s in exchange.symbols if gecerli_kripto_mu(s)]
    
    # Mevcut açık pozisyonları kontrol et
    try:
        positions = exchange.fetch_positions()
        active_positions = [p for p in positions if float(p.get("contracts") or 0) > 0]
        aktif_sayisi = len(active_positions)
        
        # Hangi tiplerde açık pozisyon var?
        acik_tipler = [pozisyon_tipleri.get(sembol_duzelt(p.get("symbol"))) for p in active_positions]
        scalp_var = "scalp" in acik_tipler
        firsat_var = "opportunity" in acik_tipler
    except Exception:
        aktif_sayisi = 0
        scalp_var, firsat_var = False, False

    if aktif_sayisi >= MAX_TOTAL_POSITIONS:
        return

    adaylar = []
    for symbol in symbols:
        if cooldown_aktif_mi(symbol):
            continue
        res = skorla_coin(exchange, symbol)
        if res and res["score"] >= OPPORTUNITY_MIN_SCORE:
            adaylar.append(res)

    if not adaylar:
        return

    # Puana göre en yüksekten düşüğe sırala
    adaylar.sort(key=lambda x: x["score"], reverse=True)

    for aday in adaylar:
        if aktif_sayisi >= MAX_TOTAL_POSITIONS:
            break

        s = aday["symbol"]
        score = aday["score"]
        
        # Tip belirleme
        if score >= SCALP_MIN_SCORE and not scalp_var and not (s in pozisyon_tipleri):
            if pozisyon_ac(exchange, s, aday["direction"], score, "scalp"):
                scalp_var = True
                aktif_sayisi += 1
        elif not firsat_var and not (s in pozisyon_tipleri):
            if pozisyon_ac(exchange, s, aday["direction"], score, "opportunity"):
                firsat_var = True
                aktif_sayisi += 1

@app.route("/")
def index():
    return jsonify({"status": "Bot Çalışıyor", "trading_enabled": TRADING_ENABLED, "monitor_active": POSITION_MONITOR_ENABLED})

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
    app.run(host="0.0.0.0", port=5000)
