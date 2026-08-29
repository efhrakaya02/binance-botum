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
# RAILWAY & BINANCE HİBRİT BOT
# SCALP + OPPORTUNITY
#
# GELİŞTİRİLMİŞ:
# - Momentum Engine
# - Acceleration Engine
# - Setup / Trigger / Confirmation / Entry
# - Multi-Timeframe Confirmation
# - Closed Candle Signal
# - Breakout Timing
# - Volume Acceleration
# - Structure
# - Pullback
# - Time Stop
# - Profit Protection
# - ATR Dynamic Management
# ============================================================

app = Flask(__name__)

API_KEY = os.getenv("BINANCE_API_KEY")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    print(
        "UYARI: BINANCE_API_KEY veya BINANCE_SECRET_KEY eksik!",
        flush=True
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================================
# ANA CONFIG
# ============================================================

TRADING_ENABLED = True
POSITION_MONITOR_ENABLED = True

# Finansal kurallar
SCALP_MARGIN = 10.0
OPPORTUNITY_MARGIN = 15.0

MAX_SCALP_POSITIONS = 1
MAX_OPPORTUNITY_POSITIONS = 1
MAX_TOTAL_POSITIONS = 2

LEVERAGE = 5

# ============================================================
# SCALP
# ============================================================

SCALP_TARGET_USDT = 0.35

# Yaklaşık komisyon + slip payı.
# Binance ücret yapısı hesaba göre değişebileceği için
# güvenlik marjı kullanıyoruz.
SCALP_FEE_BUFFER_USDT = 0.04

SCALP_MAX_HOLD_MINUTES = 35

SCALP_EARLY_PROFIT_PROTECTION_ENABLED = True
SCALP_EARLY_PROFIT_MIN_ROI = 1.8

# Kâr oluştuğunda geri vermeyi önlemek için
SCALP_PROFIT_LOCK_ROI = 2.0

# ============================================================
# OPPORTUNITY
# ============================================================

OPPORTUNITY_MAX_HOLD_HOURS = 24

OPPORTUNITY_MOMENTUM_EXIT_ENABLED = True

# ============================================================
# TARAMA
# ============================================================

GAINER_COUNT = 25
LOSER_COUNT = 25

SCALP_SCAN_TIMEFRAME = "15m"
SCALP_TRIGGER_TIMEFRAME = "5m"

OPPORTUNITY_SCAN_TIMEFRAME = "1h"
OPPORTUNITY_TRIGGER_TIMEFRAME = "15m"

HIGHER_TIMEFRAME = "4h"

# ============================================================
# SCORE
# ============================================================

MIN_SCORE_THRESHOLD = 75

SCALP_MIN_FINAL_SCORE = 78
OPPORTUNITY_MIN_FINAL_SCORE = 80

SCALP_MIN_ENTRY_SCORE = 78
OPPORTUNITY_MIN_ENTRY_SCORE = 80

SCALP_MIN_MOMENTUM = 70
OPPORTUNITY_MIN_MOMENTUM = 65

SCALP_MIN_ACCELERATION = 75
OPPORTUNITY_MIN_ACCELERATION = 70

SCALP_MAX_EXHAUSTION = 55
OPPORTUNITY_MAX_EXHAUSTION = 60

# ============================================================
# BREAKOUT
# ============================================================

# Fiyat breakout seviyesinin bu kadar ATR üzerinde ise
# giriş kovalanmış kabul edilir.
SCALP_MAX_BREAKOUT_ATR = 0.90
OPPORTUNITY_MAX_BREAKOUT_ATR = 1.50

# İdeal breakout bölgesi
IDEAL_BREAKOUT_ATR = 0.65

# ============================================================
# HACİM
# ============================================================

SCALP_MIN_VOLUME_RATIO = 1.30
OPPORTUNITY_MIN_VOLUME_RATIO = 1.50

# ============================================================
# MOMENTUM ENGINE
# ============================================================

MOMENTUM_ENGINE_ENABLED = True

# ============================================================
# ENTRY ENGINE
# ============================================================

ENTRY_TIMING_ENABLED = True

ENTRY_REQUIRE_TRIGGER = True
ENTRY_REQUIRE_CONFIRMATION = True

# Çok kısa sürede aşırı hareket etmiş coinleri kovalamama
ENTRY_MAX_CANDLE_ATR = 1.80

# ============================================================
# COOLDOWN
# ============================================================

COOLDOWN_HOURS = 4
cooldown_map = {}

# ============================================================
# RUNTIME STATE
# ============================================================

pozisyon_en_yuksek_kar = {}
pozisyon_tipleri = {}
pozisyon_yonleri = {}
pozisyon_giris_fiyatlari = {}

pozisyon_acilis_zamanlari = {}

# Son kullanılan trailing fiyatı
pozisyon_son_sl = {}

onceki_aktif_pozisyonlar = set()

son_detayli_analiz_raporu = {
    "zaman": "Henüz tarama yapılmadı",
    "scalp_takip_listesi": [],
    "firsat_takip_listesi": [],
    "aktif_pozisyonlar_roi_durumu": [],
    "yapilan_islemler": [],
    "aciklamalar": []
}

islem_acma_lock = threading.Lock()
pozisyon_monitor_lock = threading.Lock()

monitor_basladi = False


# ============================================================
# BINANCE
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

    if not symbol:
        return False

    yasakli = [
        "UP/",
        "DOWN/",
        "BEAR/",
        "BULL/",
        "_",
        "BID",
        "ASK"
    ]

    if not symbol.endswith("/USDT") and "/USDT:" not in symbol:
        return False

    if "BTC" in symbol or "XAU" in symbol:
        return False

    for yasak in yasakli:
        if yasak in symbol:
            return False

    return True


# ============================================================
# COOLDOWN
# ============================================================

def cooldown_aktif_mi(symbol):

    now = time.time()

    son_islem = cooldown_map.get(symbol)

    if son_islem is None:
        return False

    return (now - son_islem) < (COOLDOWN_HOURS * 3600)


def cooldown_baslat(symbol):

    cooldown_map[symbol] = time.time()


# ============================================================
# POSITION TYPE
# ============================================================

def pozisyon_tipini_cozumle(p):

    sym = sembol_duzelt(p.get("symbol"))

    if sym in pozisyon_tipleri:
        return pozisyon_tipleri[sym]

    try:

        contracts = float(p.get("contracts") or 0)
        entry_price = float(p.get("entryPrice") or 0)
        leverage = float(p.get("leverage") or LEVERAGE)

        if contracts > 0 and entry_price > 0:

            notional = contracts * entry_price
            margin = notional / leverage

            if margin < 12.5:
                pozisyon_tipleri[sym] = "scalp"
            else:
                pozisyon_tipleri[sym] = "opportunity"

            return pozisyon_tipleri[sym]

    except Exception:
        pass

    return "opportunity"


# ============================================================
# OHLCV + INDICATORS
# ============================================================

def ohlcv_getir(exchange, symbol, timeframe, limit=250):

    try:

        data = exchange.fetch_ohlcv(
            symbol,
            timeframe=timeframe,
            limit=limit
        )

        if not data or len(data) < 60:
            return None

        df = pd.DataFrame(
            data,
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]
        )

        # ----------------------------------------------------
        # EMA
        # ----------------------------------------------------

        df["ema9"] = df["close"].ewm(
            span=9,
            adjust=False
        ).mean()

        df["ema21"] = df["close"].ewm(
            span=21,
            adjust=False
        ).mean()

        df["ema50"] = df["close"].ewm(
            span=50,
            adjust=False
        ).mean()

        # Gerçek EMA200
        df["ema200"] = df["close"].ewm(
            span=200,
            adjust=False
        ).mean()

        # ----------------------------------------------------
        # MACD
        # ----------------------------------------------------

        exp12 = df["close"].ewm(
            span=12,
            adjust=False
        ).mean()

        exp26 = df["close"].ewm(
            span=26,
            adjust=False
        ).mean()

        df["macd"] = exp12 - exp26

        df["macd_signal"] = df["macd"].ewm(
            span=9,
            adjust=False
        ).mean()

        df["macd_hist"] = (
            df["macd"] -
            df["macd_signal"]
        )

        # ----------------------------------------------------
        # RSI
        # ----------------------------------------------------

        delta = df["close"].diff()

        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.ewm(
            alpha=1 / 14,
            adjust=False
        ).mean()

        avg_loss = loss.ewm(
            alpha=1 / 14,
            adjust=False
        ).mean()

        rs = avg_gain / avg_loss.replace(
            0,
            np.nan
        )

        df["rsi"] = (
            100 -
            (100 / (1 + rs))
        )

        # ----------------------------------------------------
        # ATR - DÜZELTİLDİ
        # ----------------------------------------------------

        high_low = (
            df["high"] -
            df["low"]
        )

        high_close = abs(
            df["high"] -
            df["close"].shift()
        )

        low_close = abs(
            df["low"] -
            df["close"].shift()
        )

        tr = pd.concat(
            [
                high_low,
                high_close,
                low_close
            ],
            axis=1
        ).max(axis=1)

        df["tr"] = tr

        df["atr"] = tr.ewm(
            alpha=1 / 14,
            adjust=False
        ).mean()

        # ----------------------------------------------------
        # ADX / DI
        # ----------------------------------------------------

        up_move = df["high"].diff()
        down_move = -df["low"].diff()

        plus_dm = np.where(
            (up_move > down_move) &
            (up_move > 0),
            up_move,
            0.0
        )

        minus_dm = np.where(
            (down_move > up_move) &
            (down_move > 0),
            down_move,
            0.0
        )

        plus_dm = pd.Series(
            plus_dm,
            index=df.index
        )

        minus_dm = pd.Series(
            minus_dm,
            index=df.index
        )

        atr_safe = df["atr"].replace(
            0,
            np.nan
        )

        df["plus_di"] = (
            100 *
            plus_dm.ewm(
                alpha=1 / 14,
                adjust=False
            ).mean() /
            atr_safe
        )

        df["minus_di"] = (
            100 *
            minus_dm.ewm(
                alpha=1 / 14,
                adjust=False
            ).mean() /
            atr_safe
        )

        di_sum = (
            df["plus_di"] +
            df["minus_di"]
        ).replace(0, np.nan)

        df["dx"] = (
            100 *
            abs(
                df["plus_di"] -
                df["minus_di"]
            ) /
            di_sum
        )

        df["adx"] = df["dx"].ewm(
            alpha=1 / 14,
            adjust=False
        ).mean()

        # ----------------------------------------------------
        # ROC
        # ----------------------------------------------------

        df["roc"] = (
            df["close"].pct_change(9) * 100
        )

        # ----------------------------------------------------
        # Bollinger
        # ----------------------------------------------------

        sma20 = df["close"].rolling(20).mean()
        std20 = df["close"].rolling(20).std()

        df["bb_middle"] = sma20

        df["bb_upper"] = (
            sma20 +
            (std20 * 2)
        )

        df["bb_lower"] = (
            sma20 -
            (std20 * 2)
        )

        df["bb_width"] = (
            (df["bb_upper"] - df["bb_lower"]) /
            sma20.replace(0, np.nan)
        )

        # ----------------------------------------------------
        # OBV
        # ----------------------------------------------------

        direction = np.sign(
            df["close"].diff()
        )

        df["obv"] = (
            direction *
            df["volume"]
        ).fillna(0).cumsum()

        # ----------------------------------------------------
        # VWAP
        # ----------------------------------------------------

        typical_price = (
            df["high"] +
            df["low"] +
            df["close"]
        ) / 3

        cumulative_volume = (
            df["volume"].cumsum()
        )

        cumulative_pv = (
            typical_price *
            df["volume"]
        ).cumsum()

        df["vwap"] = (
            cumulative_pv /
            cumulative_volume.replace(
                0,
                np.nan
            )
        )

        # ----------------------------------------------------
        # Candle structure
        # ----------------------------------------------------

        df["body"] = abs(
            df["close"] -
            df["open"]
        )

        df["range"] = (
            df["high"] -
            df["low"]
        )

        df["body_ratio"] = (
            df["body"] /
            df["range"].replace(
                0,
                np.nan
            )
        )

        df["upper_wick"] = (
            df["high"] -
            df[["open", "close"]].max(axis=1)
        )

        df["lower_wick"] = (
            df[["open", "close"]].min(axis=1) -
            df["low"]
        )

        # ----------------------------------------------------
        # Volume
        # ----------------------------------------------------

        df["volume_ma20"] = (
            df["volume"].rolling(20).mean()
        )

        df["volume_ma10"] = (
            df["volume"].rolling(10).mean()
        )

        df["volume_ratio"] = (
            df["volume"] /
            df["volume_ma20"].replace(
                0,
                np.nan
            )
        )

        # ----------------------------------------------------
        # Price distance from EMA
        # ----------------------------------------------------

        df["ema21_distance_atr"] = (
            (df["close"] - df["ema21"]) /
            df["atr"].replace(0, np.nan)
        )

        df["ema50_distance_atr"] = (
            (df["close"] - df["ema50"]) /
            df["atr"].replace(0, np.nan)
        )

        # ----------------------------------------------------
        # Cleanup
        # ----------------------------------------------------

        df.replace(
            [np.inf, -np.inf],
            np.nan,
            inplace=True
        )

        df.ffill(inplace=True)
        df.bfill(inplace=True)

        return df

    except Exception as e:

        logging.error(
            f"OHLCV hata {symbol} {timeframe}: {e}"
        )

        return None


# ============================================================
# CLOSED CANDLE
# ============================================================

def son_kapanmis_mum(df):

    if df is None or len(df) < 3:
        return None

    # Binance son mum çoğu durumda halen açık olabilir.
    # Sinyal için son kapanmış mumu kullanıyoruz.
    return df.iloc[-2]


# ============================================================
# REGRESSION
# ============================================================

def gelismis_regresyon_teyidi(
    df,
    direction,
    periyot=20
):

    if df is None or len(df) < periyot + 2:
        return False, 0.0, 0.0, "Yetersiz veri."

    # Son kapanmış mum dahil
    closes = (
        df["close"]
        .iloc[-periyot-1:-1]
        .values
    )

    x = np.arange(len(closes))

    slope, intercept = np.polyfit(
        x,
        closes,
        1
    )

    y_pred = (
        intercept +
        slope * x
    )

    if np.std(closes) == 0:
        r_squared = 0
    else:
        corr = np.corrcoef(
            closes,
            y_pred
        )[0, 1]

        r_squared = (
            corr ** 2
            if not np.isnan(corr)
            else 0
        )

    price = closes[-1]

    regression_mid = (
        intercept +
        slope * (len(closes) - 1)
    )

    atr = float(
        df["atr"].iloc[-2]
    )

    if atr <= 0:
        atr = price * 0.01

    slope_atr = (
        slope /
        atr
    )

    min_r2 = 0.55

    if direction == "buy":

        ok = (
            slope > 0 and
            r_squared >= min_r2 and
            price >= regression_mid - atr * 0.5
        )

    else:

        ok = (
            slope < 0 and
            r_squared >= min_r2 and
            price <= regression_mid + atr * 0.5
        )

    mesaj = (
        f"Eğim={slope:.6f} | "
        f"SlopeATR={slope_atr:.3f} | "
        f"R²={r_squared:.2f}"
    )

    return (
        ok,
        slope,
        r_squared,
        mesaj
    )


# ============================================================
# STRUCTURE ENGINE
# ============================================================

def structure_score(df, direction):

    if df is None or len(df) < 30:
        return 50

    d = df.iloc[:-1]

    last = d.iloc[-1]
    prev = d.iloc[-2]

    score = 50

    recent_high = d["high"].iloc[-20:-1].max()
    recent_low = d["low"].iloc[-20:-1].min()

    if direction == "buy":

        if last["close"] > last["ema21"]:
            score += 10

        if last["ema21"] > last["ema50"]:
            score += 10

        if last["close"] > prev["close"]:
            score += 5

        if last["low"] >= prev["low"]:
            score += 5

        if last["close"] >= recent_high:
            score += 15

    else:

        if last["close"] < last["ema21"]:
            score += 10

        if last["ema21"] < last["ema50"]:
            score += 10

        if last["close"] < prev["close"]:
            score += 5

        if last["high"] <= prev["high"]:
            score += 5

        if last["close"] <= recent_low:
            score += 15

    return min(max(score, 0), 100)


# ============================================================
# PULLBACK ENGINE
# ============================================================

def pullback_score(df, direction):

    if df is None or len(df) < 10:
        return 0

    d = df.iloc[:-1]

    last = d.iloc[-1]
    prev = d.iloc[-2]

    score = 50

    if direction == "buy":

        if prev["low"] <= prev["ema21"]:
            score += 20

        if last["close"] > last["ema21"]:
            score += 15

        if last["close"] > prev["close"]:
            score += 10

        if last["rsi"] >= 50:
            score += 5

    else:

        if prev["high"] >= prev["ema21"]:
            score += 20

        if last["close"] < last["ema21"]:
            score += 15

        if last["close"] < prev["close"]:
            score += 10

        if last["rsi"] <= 50:
            score += 5

    return min(max(score, 0), 100)


# ============================================================
# BREAKOUT ENGINE
# ============================================================

def breakout_analysis(df, direction):

    if df is None or len(df) < 25:
        return {
            "breakout": False,
            "distance_atr": 0,
            "fresh": False,
            "quality": 0
        }

    d = df.iloc[:-1]

    last = d.iloc[-1]

    atr = float(last["atr"])

    if atr <= 0:
        return {
            "breakout": False,
            "distance_atr": 0,
            "fresh": False,
            "quality": 0
        }

    previous_high = d["high"].iloc[-21:-1].max()
    previous_low = d["low"].iloc[-21:-1].min()

    if direction == "buy":

        distance = (
            last["close"] -
            previous_high
        ) / atr

        breakout = (
            last["close"] >
            previous_high
        )

    else:

        distance = (
            previous_low -
            last["close"]
        ) / atr

        breakout = (
            last["close"] <
            previous_low
        )

    distance = max(distance, 0)

    # Breakout'un yeni olması
    prior_breakout = False

    if direction == "buy":

        prior_breakout = (
            d["close"].iloc[-2] >
            d["high"].iloc[-21:-2].max()
        )

    else:

        prior_breakout = (
            d["close"].iloc[-2] <
            d["low"].iloc[-21:-2].min()
        )

    fresh = breakout and not prior_breakout

    quality = 50

    if breakout:
        quality += 20

    if fresh:
        quality += 15

    if 0 <= distance <= IDEAL_BREAKOUT_ATR:
        quality += 15

    if distance > 1.5:
        quality -= 30

    return {
        "breakout": bool(breakout),
        "distance_atr": round(float(distance), 3),
        "fresh": bool(fresh),
        "quality": min(max(quality, 0), 100)
    }


# ============================================================
# CANDLE QUALITY
# ============================================================

def candle_quality(df, direction):

    if df is None or len(df) < 5:
        return 50

    d = df.iloc[:-1]
    last = d.iloc[-1]

    rng = float(last["range"])

    if rng <= 0:
        return 50

    body_ratio = float(last["body_ratio"])

    score = 50

    if direction == "buy":

        if last["close"] > last["open"]:
            score += 20

        if body_ratio >= 0.60:
            score += 15

        if (
            last["upper_wick"] / rng
        ) < 0.25:
            score += 10

    else:

        if last["close"] < last["open"]:
            score += 20

        if body_ratio >= 0.60:
            score += 15

        if (
            last["lower_wick"] / rng
        ) < 0.25:
            score += 10

    return min(max(score, 0), 100)


# ============================================================
# MOMENTUM ENGINE
# ============================================================

def calculate_momentum_engine(
    df,
    direction
):

    if df is None or len(df) < 30:

        return {
            "momentum_score": 50,
            "acceleration_score": 50,
            "exhaustion_score": 0,
            "entry_score": 50,
            "state": "WAIT",
            "breakout_distance_atr": 0,
            "volume_ratio": 1,
            "structure_score": 50,
            "pullback_score": 50,
            "candle_quality": 50,
            "trend_score": 50,
            "trigger_score": 50
        }

    d = df.iloc[:-1]

    last = d.iloc[-1]

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    momentum = 50

    if direction == "buy":

        if last["ema9"] > last["ema21"]:
            momentum += 15

        if last["ema21"] > last["ema50"]:
            momentum += 15

        if last["macd_hist"] > 0:
            momentum += 10

        if last["rsi"] > 50:
            momentum += 10

    else:

        if last["ema9"] < last["ema21"]:
            momentum += 15

        if last["ema21"] < last["ema50"]:
            momentum += 15

        if last["macd_hist"] < 0:
            momentum += 10

        if last["rsi"] < 50:
            momentum += 10

    momentum = min(max(momentum, 0), 100)

    # --------------------------------------------------------
    # ACCELERATION
    # --------------------------------------------------------

    adx_now = float(last["adx"])
    adx_prev = float(d["adx"].iloc[-4])

    macd_now = float(last["macd_hist"])
    macd_prev = float(d["macd_hist"].iloc[-4])

    ema_spread_now = (
        (last["ema9"] - last["ema21"]) /
        last["ema21"] *
        100
    )

    ema_spread_prev = (
        (d["ema9"].iloc[-4] -
         d["ema21"].iloc[-4]) /
        d["ema21"].iloc[-4] *
        100
    )

    acceleration = 50

    if direction == "buy":

        if adx_now > adx_prev:
            acceleration += 15

        if macd_now > macd_prev:
            acceleration += 15

        if ema_spread_now > ema_spread_prev:
            acceleration += 15

        if last["roc"] > d["roc"].iloc[-4]:
            acceleration += 5

    else:

        if adx_now > adx_prev:
            acceleration += 15

        if macd_now < macd_prev:
            acceleration += 15

        if ema_spread_now < ema_spread_prev:
            acceleration += 15

        if last["roc"] < d["roc"].iloc[-4]:
            acceleration += 5

    acceleration = min(max(acceleration, 0), 100)

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    volume_ratio = float(
        last["volume_ratio"]
    )

    volume_score = min(
        100,
        50 + (volume_ratio - 1) * 35
    )

    # --------------------------------------------------------
    # BREAKOUT
    # --------------------------------------------------------

    breakout = breakout_analysis(
        df,
        direction
    )

    # --------------------------------------------------------
    # STRUCTURE
    # --------------------------------------------------------

    structure = structure_score(
        df,
        direction
    )

    # --------------------------------------------------------
    # PULLBACK
    # --------------------------------------------------------

    pullback = pullback_score(
        df,
        direction
    )

    # --------------------------------------------------------
    # CANDLE
    # --------------------------------------------------------

    candle = candle_quality(
        df,
        direction
    )

    # --------------------------------------------------------
    # TREND
    # --------------------------------------------------------

    trend = 50

    if direction == "buy":

        if last["close"] > last["ema50"]:
            trend += 15

        if last["ema50"] > last["ema200"]:
            trend += 20

        if last["ema21"] > last["ema50"]:
            trend += 15

    else:

        if last["close"] < last["ema50"]:
            trend += 15

        if last["ema50"] < last["ema200"]:
            trend += 20

        if last["ema21"] < last["ema50"]:
            trend += 15

    trend = min(max(trend, 0), 100)

    # --------------------------------------------------------
    # EXHAUSTION
    # --------------------------------------------------------

    exhaustion = 0

    if direction == "buy":

        if last["rsi"] > 72:
            exhaustion += 30

        if last["rsi"] > 78:
            exhaustion += 20

        if breakout["distance_atr"] > 1.0:
            exhaustion += 20

        if macd_now < macd_prev:
            exhaustion += 15

        if adx_now > 35 and adx_now < adx_prev:
            exhaustion += 15

        if last["close"] > last["ema21"] + last["atr"] * 1.5:
            exhaustion += 15

    else:

        if last["rsi"] < 28:
            exhaustion += 30

        if last["rsi"] < 22:
            exhaustion += 20

        if breakout["distance_atr"] > 1.0:
            exhaustion += 20

        if macd_now > macd_prev:
            exhaustion += 15

        if adx_now > 35 and adx_now < adx_prev:
            exhaustion += 15

        if last["close"] < last["ema21"] - last["atr"] * 1.5:
            exhaustion += 15

    exhaustion = min(
        max(exhaustion, 0),
        100
    )

    # --------------------------------------------------------
    # TRIGGER
    # --------------------------------------------------------

    trigger = 50

    if direction == "buy":

        if last["close"] > last["open"]:
            trigger += 10

        if last["close"] > last["ema9"]:
            trigger += 10

        if last["macd_hist"] > 0:
            trigger += 10

        if last["plus_di"] > last["minus_di"]:
            trigger += 10

        if acceleration >= 75:
            trigger += 10

    else:

        if last["close"] < last["open"]:
            trigger += 10

        if last["close"] < last["ema9"]:
            trigger += 10

        if last["macd_hist"] < 0:
            trigger += 10

        if last["minus_di"] > last["plus_di"]:
            trigger += 10

        if acceleration >= 75:
            trigger += 10

    trigger = min(
        max(trigger, 0),
        100
    )

    # --------------------------------------------------------
    # STATE
    # --------------------------------------------------------

    if exhaustion >= 70:

        state = "EXHAUSTING"

    elif (
        acceleration >= 80 and
        momentum >= 75
    ):

        state = "ACCELERATING"

    elif momentum >= 75:

        state = "STRONG"

    elif momentum >= 60:

        state = "BUILDING"

    else:

        state = "WEAK"

    # --------------------------------------------------------
    # ENTRY SCORE
    #
    # Kısa vadeli işlemlerde trendden daha çok:
    # momentum + acceleration + trigger + volume
    # --------------------------------------------------------

    entry_score = (
        momentum * 0.20 +
        acceleration * 0.25 +
        trigger * 0.20 +
        volume_score * 0.15 +
        structure * 0.10 +
        candle * 0.05 +
        pullback * 0.05
    )

    # Exhaustion penalty
    if exhaustion > 50:
        entry_score -= (
            exhaustion - 50
        ) * 0.35

    entry_score = min(
        max(entry_score, 0),
        100
    )

    return {
        "momentum_score": round(momentum, 2),
        "acceleration_score": round(acceleration, 2),
        "exhaustion_score": round(exhaustion, 2),
        "entry_score": round(entry_score, 2),
        "state": state,
        "breakout_distance_atr": round(
            breakout["distance_atr"],
            3
        ),
        "breakout_quality": breakout["quality"],
        "breakout_fresh": breakout["fresh"],
        "volume_ratio": round(
            volume_ratio,
            2
        ),
        "volume_score": round(
            volume_score,
            2
        ),
        "structure_score": round(
            structure,
            2
        ),
        "pullback_score": round(
            pullback,
            2
        ),
        "candle_quality": round(
            candle,
            2
        ),
        "trend_score": round(
            trend,
            2
        ),
        "trigger_score": round(
            trigger,
            2
        )
    }


# ============================================================
# MULTI TIMEFRAME TREND
# ============================================================

def higher_timeframe_confirmation(
    exchange,
    symbol,
    direction
):

    df4 = ohlcv_getir(
        exchange,
        symbol,
        HIGHER_TIMEFRAME,
        250
    )

    if df4 is None:
        return {
            "ok": False,
            "score": 50,
            "trend": "UNKNOWN"
        }

    d = df4.iloc[:-1]

    last = d.iloc[-1]

    if direction == "buy":

        score = 50

        if last["close"] > last["ema50"]:
            score += 20

        if last["ema50"] > last["ema200"]:
            score += 20

        if last["ema9"] > last["ema21"]:
            score += 10

        ok = score >= 65

        trend = "BULLISH" if score >= 65 else "NEUTRAL"

    else:

        score = 50

        if last["close"] < last["ema50"]:
            score += 20

        if last["ema50"] < last["ema200"]:
            score += 20

        if last["ema9"] < last["ema21"]:
            score += 10

        ok = score >= 65

        trend = "BEARISH" if score >= 65 else "NEUTRAL"

    del df4

    return {
        "ok": ok,
        "score": min(max(score, 0), 100),
        "trend": trend
    }


# ============================================================
# TRIGGER TIMEFRAME
# ============================================================

def trigger_confirmation(
    exchange,
    symbol,
    direction,
    timeframe
):

    df = ohlcv_getir(
        exchange,
        symbol,
        timeframe,
        100
    )

    if df is None:
        return {
            "ok": False,
            "score": 0,
            "state": "NO_DATA"
        }

    mom = calculate_momentum_engine(
        df,
        direction
    )

    d = df.iloc[:-1]

    last = d.iloc[-1]

    trigger_score = mom["trigger_score"]

    if direction == "buy":

        if last["close"] > last["ema9"]:
            trigger_score += 5

        if last["plus_di"] > last["minus_di"]:
            trigger_score += 5

    else:

        if last["close"] < last["ema9"]:
            trigger_score += 5

        if last["minus_di"] > last["plus_di"]:
            trigger_score += 5

    trigger_score = min(
        trigger_score,
        100
    )

    ok = (
        trigger_score >= 65 and
        mom["exhaustion_score"] <= 65
    )

    result = {
        "ok": ok,
        "score": round(trigger_score, 2),
        "state": mom["state"],
        "momentum": mom["momentum_score"],
        "acceleration": mom["acceleration_score"],
        "exhaustion": mom["exhaustion_score"]
    }

    del df

    return result


# ============================================================
# FINAL ENTRY ENGINE
# ============================================================

def evaluate_entry(
    exchange,
    symbol,
    direction,
    mode,
    df
):

    if df is None:
        return {
            "approved": False,
            "reason": "DATA"
        }

    mom = calculate_momentum_engine(
        df,
        direction
    )

    if mode == "scalp":

        min_entry = SCALP_MIN_ENTRY_SCORE
        min_momentum = SCALP_MIN_MOMENTUM
        min_acceleration = SCALP_MIN_ACCELERATION
        max_exhaustion = SCALP_MAX_EXHAUSTION
        max_breakout = SCALP_MAX_BREAKOUT_ATR

    else:

        min_entry = OPPORTUNITY_MIN_ENTRY_SCORE
        min_momentum = OPPORTUNITY_MIN_MOMENTUM
        min_acceleration = OPPORTUNITY_MIN_ACCELERATION
        max_exhaustion = OPPORTUNITY_MAX_EXHAUSTION
        max_breakout = OPPORTUNITY_MAX_BREAKOUT_ATR

    # --------------------------------------------------------
    # 1. ENTRY SCORE
    # --------------------------------------------------------

    if mom["entry_score"] < min_entry:

        return {
            "approved": False,
            "reason": "ENTRY_SCORE",
            "momentum": mom
        }

    # --------------------------------------------------------
    # 2. MOMENTUM
    # --------------------------------------------------------

    if mom["momentum_score"] < min_momentum:

        return {
            "approved": False,
            "reason": "MOMENTUM",
            "momentum": mom
        }

    # --------------------------------------------------------
    # 3. ACCELERATION
    # --------------------------------------------------------

    if mom["acceleration_score"] < min_acceleration:

        return {
            "approved": False,
            "reason": "ACCELERATION",
            "momentum": mom
        }

    # --------------------------------------------------------
    # 4. EXHAUSTION
    # --------------------------------------------------------

    if mom["exhaustion_score"] > max_exhaustion:

        return {
            "approved": False,
            "reason": "EXHAUSTION",
            "momentum": mom
        }

    # --------------------------------------------------------
    # 5. BREAKOUT DISTANCE
    # --------------------------------------------------------

    if (
        mom["breakout_distance_atr"] >
        max_breakout
    ):

        return {
            "approved": False,
            "reason": "BREAKOUT_TOO_FAR",
            "momentum": mom
        }

    # --------------------------------------------------------
    # 6. VOLUME
    # --------------------------------------------------------

    min_volume = (
        SCALP_MIN_VOLUME_RATIO
        if mode == "scalp"
        else OPPORTUNITY_MIN_VOLUME_RATIO
    )

    if mom["volume_ratio"] < min_volume:

        return {
            "approved": False,
            "reason": "VOLUME",
            "momentum": mom
        }

    # --------------------------------------------------------
    # 7. STATE
    # --------------------------------------------------------

    allowed_states = [
        "ACCELERATING",
        "STRONG",
        "BUILDING"
    ]

    if mom["state"] not in allowed_states:

        return {
            "approved": False,
            "reason": "STATE",
            "momentum": mom
        }

    # --------------------------------------------------------
    # 8. CANDLE
    # --------------------------------------------------------

    if mom["candle_quality"] < 55:

        return {
            "approved": False,
            "reason": "CANDLE",
            "momentum": mom
        }

    # --------------------------------------------------------
    # 9. TRIGGER TIMEFRAME
    # --------------------------------------------------------

    trigger_tf = (
        SCALP_TRIGGER_TIMEFRAME
        if mode == "scalp"
        else OPPORTUNITY_TRIGGER_TIMEFRAME
    )

    trigger = trigger_confirmation(
        exchange,
        symbol,
        direction,
        trigger_tf
    )

    if ENTRY_REQUIRE_TRIGGER and not trigger["ok"]:

        return {
            "approved": False,
            "reason": "TRIGGER",
            "momentum": mom,
            "trigger": trigger
        }

    # --------------------------------------------------------
    # 10. HIGHER TIMEFRAME
    # --------------------------------------------------------

    if mode == "opportunity":

        htf = higher_timeframe_confirmation(
            exchange,
            symbol,
            direction
        )

        if not htf["ok"]:

            return {
                "approved": False,
                "reason": "HTF",
                "momentum": mom,
                "trigger": trigger,
                "htf": htf
            }

    else:

        htf = {
            "ok": True,
            "score": 50,
            "trend": "SCALP"
        }

    # --------------------------------------------------------
    # 11. REGRESSION
    # --------------------------------------------------------

    period = (
        12
        if mode == "scalp"
        else 20
    )

    reg_ok, slope, r2, reg_msg = (
        gelismis_regresyon_teyidi(
            df,
            direction,
            period
        )
    )

    if not reg_ok:

        return {
            "approved": False,
            "reason": "REGRESSION",
            "momentum": mom,
            "trigger": trigger,
            "htf": htf,
            "r2": r2
        }

    # --------------------------------------------------------
    # 12. PULLBACK / STRUCTURE
    # --------------------------------------------------------

    pullback = mom["pullback_score"]
    structure = mom["structure_score"]

    if pullback < 55:

        return {
            "approved": False,
            "reason": "PULLBACK",
            "momentum": mom
        }

    if structure < 60:

        return {
            "approved": False,
            "reason": "STRUCTURE",
            "momentum": mom
        }

    # --------------------------------------------------------
    # FINAL TIMING SCORE
    # --------------------------------------------------------

    if mode == "scalp":

        final_score = (
            mom["momentum_score"] * 0.18 +
            mom["acceleration_score"] * 0.22 +
            mom["entry_score"] * 0.20 +
            mom["structure_score"] * 0.12 +
            mom["pullback_score"] * 0.08 +
            mom["trigger_score"] * 0.10 +
            mom["volume_score"] * 0.05 +
            mom["trend_score"] * 0.05
        )

    else:

        final_score = (
            mom["momentum_score"] * 0.16 +
            mom["acceleration_score"] * 0.18 +
            mom["entry_score"] * 0.16 +
            mom["structure_score"] * 0.12 +
            mom["pullback_score"] * 0.06 +
            mom["trigger_score"] * 0.08 +
            mom["volume_score"] * 0.08 +
            mom["trend_score"] * 0.16
        )

    # Trend düşük ama kısa vadeli momentum çok güçlü ise
    # Scalp'in gereksiz yere elenmesini önle.
    if mode == "scalp":

        if (
            mom["acceleration_score"] >= 90 and
            mom["entry_score"] >= 88 and
            mom["volume_ratio"] >= 2.0 and
            mom["breakout_distance_atr"] <= 0.7 and
            mom["exhaustion_score"] <= 45
        ):

            final_score += 5

    final_score = min(
        max(final_score, 0),
        100
    )

    min_final = (
        SCALP_MIN_FINAL_SCORE
        if mode == "scalp"
        else OPPORTUNITY_MIN_FINAL_SCORE
    )

    approved = (
        final_score >= min_final
    )

    return {
        "approved": approved,
        "reason": (
            "APPROVED"
            if approved
            else "FINAL_SCORE"
        ),
        "final_score": round(
            final_score,
            2
        ),
        "momentum": mom,
        "trigger": trigger,
        "htf": htf,
        "regression_r2": round(
            r2,
            2
        ),
        "regression_slope": slope,
        "regression_message": reg_msg
    }


# ============================================================
# SCALP MARKET SCAN
# ============================================================

def scan_scalp_market(exchange):

    try:

        tickers = exchange.fetch_tickers()

        usdt_tickers = [
            t
            for t in tickers.values()
            if gecerli_kripto_mu(t.get("symbol"))
            and t.get("percentage") is not None
        ]

        gainers = sorted(
            usdt_tickers,
            key=lambda x: float(x["percentage"]),
            reverse=True
        )[:GAINER_COUNT]

        losers = sorted(
            usdt_tickers,
            key=lambda x: float(x["percentage"])
        )[:LOSER_COUNT]

        target_pool = list(
            set(
                [
                    t["symbol"]
                    for t in gainers + losers
                ]
            )
        )

        candidates = []

        for symbol in target_pool:

            if cooldown_aktif_mi(symbol):
                continue

            df = ohlcv_getir(
                exchange,
                symbol,
                SCALP_SCAN_TIMEFRAME,
                250
            )

            if df is None:
                continue

            d = df.iloc[:-1]

            last = d.iloc[-1]

            # ----------------------------------------------
            # YÖN
            # ----------------------------------------------

            long_score = 0
            short_score = 0

            if last["close"] > last["ema50"]:
                long_score += 25

            if last["ema9"] > last["ema21"]:
                long_score += 25

            if last["macd_hist"] > 0:
                long_score += 20

            if last["plus_di"] > last["minus_di"]:
                long_score += 15

            if last["close"] > last["vwap"]:
                long_score += 15

            if last["close"] < last["ema50"]:
                short_score += 25

            if last["ema9"] < last["ema21"]:
                short_score += 25

            if last["macd_hist"] < 0:
                short_score += 20

            if last["minus_di"] > last["plus_di"]:
                short_score += 15

            if last["close"] < last["vwap"]:
                short_score += 15

            if long_score >= short_score:
                direction = "buy"
                base_score = long_score
            else:
                direction = "sell"
                base_score = short_score

            # ----------------------------------------------
            # MOMENTUM
            # ----------------------------------------------

            mom = calculate_momentum_engine(
                df,
                direction
            )

            # İlk aday filtresi
            if (
                mom["momentum_score"] < 60 or
                mom["volume_ratio"] < 1.1
            ):
                del df
                continue

            candidates.append({
                "symbol": symbol,
                "direction": direction,
                "base_score": base_score,
                "momentum": mom,
                "df": df
            })

        # Önce momentum kalitesi
        candidates.sort(
            key=lambda x: (
                x["momentum"]["entry_score"],
                x["momentum"]["acceleration_score"],
                x["momentum"]["volume_ratio"]
            ),
            reverse=True
        )

        # İlk 8'e düşür
        return candidates[:8]

    except Exception as e:

        logging.error(
            f"Scalp tarama hatası: {e}"
        )

        return []


# ============================================================
# OPPORTUNITY MARKET SCAN
# ============================================================

def scan_opportunity_market(exchange):

    try:

        tickers = exchange.fetch_tickers()

        usdt_tickers = [
            t
            for t in tickers.values()
            if gecerli_kripto_mu(t.get("symbol"))
            and t.get("percentage") is not None
        ]

        gainers = sorted(
            usdt_tickers,
            key=lambda x: float(x["percentage"]),
            reverse=True
        )[:GAINER_COUNT]

        losers = sorted(
            usdt_tickers,
            key=lambda x: float(x["percentage"])
        )[:LOSER_COUNT]

        target_pool = list(
            set(
                [
                    t["symbol"]
                    for t in gainers + losers
                ]
            )
        )

        candidates = []

        for symbol in target_pool:

            if cooldown_aktif_mi(symbol):
                continue

            df = ohlcv_getir(
                exchange,
                symbol,
                OPPORTUNITY_SCAN_TIMEFRAME,
                250
            )

            if df is None:
                continue

            d = df.iloc[:-1]
            last = d.iloc[-1]

            # ----------------------------------------------
            # YÖN ARTIK SADECE EMA50'E GÖRE BELİRLENMİYOR
            # ----------------------------------------------

            long_score = 0
            short_score = 0

            if last["close"] > last["ema50"]:
                long_score += 20

            if last["ema50"] > last["ema200"]:
                long_score += 20

            if last["ema9"] > last["ema21"]:
                long_score += 15

            if last["macd_hist"] > 0:
                long_score += 15

            if last["plus_di"] > last["minus_di"]:
                long_score += 15

            if last["close"] > last["vwap"]:
                long_score += 15

            if last["close"] < last["ema50"]:
                short_score += 20

            if last["ema50"] < last["ema200"]:
                short_score += 20

            if last["ema9"] < last["ema21"]:
                short_score += 15

            if last["macd_hist"] < 0:
                short_score += 15

            if last["minus_di"] > last["plus_di"]:
                short_score += 15

            if last["close"] < last["vwap"]:
                short_score += 15

            if long_score >= short_score:
                direction = "buy"
                base_score = long_score
            else:
                direction = "sell"
                base_score = short_score

            mom = calculate_momentum_engine(
                df,
                direction
            )

            if (
                mom["momentum_score"] < 55 or
                mom["volume_ratio"] < 1.2
            ):
                del df
                continue

            candidates.append({
                "symbol": symbol,
                "direction": direction,
                "base_score": base_score,
                "momentum": mom,
                "df": df
            })

        candidates.sort(
            key=lambda x: (
                x["momentum"]["entry_score"],
                x["momentum"]["momentum_score"],
                x["momentum"]["volume_ratio"]
            ),
            reverse=True
        )

        return candidates[:8]

    except Exception as e:

        logging.error(
            f"Opportunity tarama hatası: {e}"
        )

        return []


# ============================================================
# SCALP TP HESABI
# ============================================================

def scalp_tp_price(
    exchange,
    symbol,
    side,
    amount,
    entry_price
):

    # Hedef net kar
    target_net = (
        SCALP_TARGET_USDT +
        SCALP_FEE_BUFFER_USDT
    )

    price_difference = (
        target_net /
        amount
    )

    if side == "buy":

        tp = (
            entry_price +
            price_difference
        )

    else:

        tp = (
            entry_price -
            price_difference
        )

    return float(
        exchange.price_to_precision(
            symbol,
            tp
        )
    )


# ============================================================
# POSITION OPEN
# ============================================================

def pozisyon_ac(
    exchange,
    symbol,
    direction,
    score,
    p_type,
    analiz_detay=""
):

    if not TRADING_ENABLED:
        return False

    with islem_acma_lock:

        try:

            if cooldown_aktif_mi(symbol):
                return False

            positions = exchange.fetch_positions()

            active_positions = [
                p
                for p in positions
                if float(
                    p.get("contracts") or 0
                ) > 0
            ]

            if len(active_positions) >= MAX_TOTAL_POSITIONS:
                return False

            for p in active_positions:

                if (
                    sembol_duzelt(
                        p.get("symbol")
                    ) == symbol
                ):
                    return False

            active_scalp = sum(
                1
                for p in active_positions
                if pozisyon_tipini_cozumle(p)
                == "scalp"
            )

            active_opportunity = sum(
                1
                for p in active_positions
                if pozisyon_tipini_cozumle(p)
                == "opportunity"
            )

            if (
                p_type == "scalp"
                and active_scalp >= MAX_SCALP_POSITIONS
            ):
                return False

            if (
                p_type == "opportunity"
                and active_opportunity >= MAX_OPPORTUNITY_POSITIONS
            ):
                return False

            # ------------------------------------------------
            # ISOLATED + 5X
            # ------------------------------------------------

            try:

                exchange.set_margin_mode(
                    "isolated",
                    symbol
                )

            except Exception:
                pass

            try:

                exchange.set_leverage(
                    LEVERAGE,
                    symbol
                )

            except Exception:
                pass

            # ------------------------------------------------
            # PRICE
            # ------------------------------------------------

            ticker = exchange.fetch_ticker(
                symbol
            )

            price = float(
                ticker["last"]
            )

            target_margin = (
                OPPORTUNITY_MARGIN
                if p_type == "opportunity"
                else SCALP_MARGIN
            )

            notional = (
                target_margin *
                LEVERAGE
            )

            raw_amount = (
                notional /
                price
            )

            market = exchange.market(
                symbol
            )

            min_amount = (
                market["limits"]["amount"]["min"]
            )

            if raw_amount < min_amount:
                return False

            amount = float(
                exchange.amount_to_precision(
                    symbol,
                    raw_amount
                )
            )

            if amount <= 0:
                return False

            real_notional = (
                amount *
                price
            )

            real_margin = (
                real_notional /
                LEVERAGE
            )

            # ------------------------------------------------
            # ORDER
            # ------------------------------------------------

            side = (
                "buy"
                if direction == "buy"
                else "sell"
            )

            logging.info(
                "============================================================"
            )

            logging.info(
                f"[İŞLEM AÇILIYOR] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{side.upper()}"
            )

            logging.info(
                f"[SKOR] {score}"
            )

            logging.info(
                f"[DETAY] {analiz_detay}"
            )

            logging.info(
                f"[GİRİŞ] {price}"
            )

            logging.info(
                f"[MARJ] {real_margin:.2f} USDT"
            )

            logging.info(
                f"[LEVERAGE] {LEVERAGE}X"
            )

            logging.info(
                "============================================================"
            )

            order = exchange.create_order(
                symbol,
                "market",
                side,
                amount,
                None,
                {
                    "leverage": LEVERAGE
                }
            )

            if not order:
                return False

            # ------------------------------------------------
            # STATE
            # ------------------------------------------------

            pozisyon_tipleri[symbol] = p_type
            pozisyon_yonleri[symbol] = direction

            pozisyon_giris_fiyatlari[symbol] = price

            pozisyon_en_yuksek_kar[symbol] = 0.0

            pozisyon_acilis_zamanlari[symbol] = time.time()

            pozisyon_son_sl.pop(
                symbol,
                None
            )

            cooldown_baslat(symbol)

            # ------------------------------------------------
            # INITIAL SL / TP
            # ------------------------------------------------

            time.sleep(1.0)

            timeframe = (
                SCALP_SCAN_TIMEFRAME
                if p_type == "scalp"
                else OPPORTUNITY_SCAN_TIMEFRAME
            )

            df_temp = ohlcv_getir(
                exchange,
                symbol,
                timeframe,
                100
            )

            if df_temp is not None:

                last_closed = son_kapanmis_mum(
                    df_temp
                )

                atr = float(
                    last_closed["atr"]
                )

            else:

                atr = (
                    price *
                    0.01
                )

            if atr <= 0:
                atr = price * 0.01

            close_side = (
                "sell"
                if side == "buy"
                else "buy"
            )

            # ------------------------------------------------
            # SCALP
            # ------------------------------------------------

            if p_type == "scalp":

                tp_price = scalp_tp_price(
                    exchange,
                    symbol,
                    side,
                    amount,
                    price
                )

                sl_distance = (
                    atr *
                    1.8
                )

                sl_price = (
                    price - sl_distance
                    if side == "buy"
                    else
                    price + sl_distance
                )

                sl_price = float(
                    exchange.price_to_precision(
                        symbol,
                        sl_price
                    )
                )

                try:

                    exchange.create_order(
                        symbol,
                        "take_profit_market",
                        close_side,
                        amount,
                        None,
                        {
                            "stopPrice": tp_price,
                            "reduceOnly": True,
                            "workingType": "MARK_PRICE"
                        }
                    )

                    exchange.create_order(
                        symbol,
                        "stop_market",
                        close_side,
                        amount,
                        None,
                        {
                            "stopPrice": sl_price,
                            "reduceOnly": True,
                            "workingType": "MARK_PRICE"
                        }
                    )

                    logging.info(
                        f"[SCALP TP] {tp_price}"
                    )

                    logging.info(
                        f"[SCALP SL] {sl_price}"
                    )

                except Exception as e:

                    logging.error(
                        f"Scalp SL/TP hata: {e}"
                    )

            # ------------------------------------------------
            # OPPORTUNITY
            # ------------------------------------------------

            else:

                sl_distance = (
                    atr *
                    2.5
                )

                sl_price = (
                    price - sl_distance
                    if side == "buy"
                    else
                    price + sl_distance
                )

                sl_price = float(
                    exchange.price_to_precision(
                        symbol,
                        sl_price
                    )
                )

                try:

                    exchange.create_order(
                        symbol,
                        "stop_market",
                        close_side,
                        amount,
                        None,
                        {
                            "stopPrice": sl_price,
                            "reduceOnly": True,
                            "workingType": "MARK_PRICE"
                        }
                    )

                    pozisyon_son_sl[symbol] = sl_price

                    logging.info(
                        f"[OPPORTUNITY INITIAL SL] "
                        f"{sl_price}"
                    )

                except Exception as e:

                    logging.error(
                        f"Opportunity SL hata: {e}"
                    )

            return True

        except Exception as e:

            logging.error(
                f"İşlem açma hata {symbol}: {e}"
            )

            return False


# ============================================================
# POSITION CLOSE
# ============================================================

def pozisyon_kapat(
    exchange,
    symbol,
    side,
    contracts,
    reason
):

    try:

        close_side = (
            "sell"
            if side == "long"
            else "buy"
        )

        exchange.cancel_all_orders(
            symbol
        )

        time.sleep(0.2)

        exchange.create_order(
            symbol,
            "market",
            close_side,
            contracts,
            None,
            {
                "reduceOnly": True
            }
        )

        logging.warning(
            f"[POZİSYON KAPATILDI] "
            f"{symbol} | "
            f"{reason}"
        )

        return True

    except Exception as e:

        logging.error(
            f"Pozisyon kapatma hata "
            f"{symbol}: {e}"
        )

        return False


# ============================================================
# SCALP TIME STOP
# ============================================================

def scalp_time_stop(
    exchange,
    symbol,
    side,
    contracts,
    roi
):

    open_time = (
        pozisyon_acilis_zamanlari
        .get(symbol)
    )

    if open_time is None:
        return False

    elapsed_minutes = (
        time.time() -
        open_time
    ) / 60

    if elapsed_minutes < SCALP_MAX_HOLD_MINUTES:
        return False

    # 0.35 hedefe yaklaşmış ama henüz alamamışsa
    # biraz daha tolerans vermiyoruz.
    if roi < 0.5:

        logging.warning(
            f"[SCALP TIME STOP] "
            f"{symbol} | "
            f"{elapsed_minutes:.1f} dk | "
            f"ROI %{roi:.2f}"
        )

        return pozisyon_kapat(
            exchange,
            symbol,
            side,
            contracts,
            "TIME_STOP"
        )

    return False


# ============================================================
# SCALP PROFIT PROTECTION
# ============================================================

def scalp_profit_protection(
    exchange,
    symbol,
    side,
    contracts,
    roi
):

    if not SCALP_EARLY_PROFIT_PROTECTION_ENABLED:
        return False

    if roi < SCALP_EARLY_PROFIT_MIN_ROI:
        return False

    timeframe = SCALP_SCAN_TIMEFRAME

    df = ohlcv_getir(
        exchange,
        symbol,
        timeframe,
        80
    )

    if df is None:
        return False

    direction = (
        "buy"
        if side == "long"
        else "sell"
    )

    mom = calculate_momentum_engine(
        df,
        direction
    )

    # Kârdayken momentum tamamen bozulursa
    # geri dönüşü beklemiyoruz.
    if (
        mom["exhaustion_score"] >= 70
        or
        mom["state"] == "EXHAUSTING"
    ):

        logging.warning(
            f"[SCALP KAR KORUMA] "
            f"{symbol} | "
            f"ROI %{roi:.2f} | "
            f"Exhaustion {mom['exhaustion_score']}"
        )

        return pozisyon_kapat(
            exchange,
            symbol,
            side,
            contracts,
            "MOMENTUM_EXHAUSTION"
        )

    return False


# ============================================================
# OPPORTUNITY TRAILING
# ============================================================

def opportunity_trailing(
    exchange,
    symbol,
    side,
    contracts,
    entry_price,
    mark_price,
    leverage,
    current_max
):

    yeni_sl = None

    # --------------------------------------------------------
    # 5 ROI sonrası koruma başlar
    # --------------------------------------------------------

    if current_max >= 5:

        # 5 -> 15 ROI arası kademeli koruma
        progress = min(
            (current_max - 5) / 10,
            1
        )

        locked_roi = (
            1.0 +
            progress * 4.0
        )

        price_move = (
            locked_roi /
            100 /
            leverage
        )

        if side == "long":

            yeni_sl = (
                entry_price *
                (1 + price_move)
            )

        else:

            yeni_sl = (
                entry_price *
                (1 - price_move)
            )

    # --------------------------------------------------------
    # 15+ ROI sonrası daha sıkı trailing
    # --------------------------------------------------------

    if current_max >= 15:

        locked_roi = max(
            8,
            current_max - 3
        )

        price_move = (
            locked_roi /
            100 /
            leverage
        )

        if side == "long":

            yeni_sl = (
                entry_price *
                (1 + price_move)
            )

        else:

            yeni_sl = (
                entry_price *
                (1 - price_move)
            )

    if yeni_sl is None:
        return

    # --------------------------------------------------------
    # Stop mevcut fiyattan geride olmalı
    # --------------------------------------------------------

    if side == "long":

        if yeni_sl >= mark_price:
            yeni_sl = (
                mark_price *
                0.998
            )

    else:

        if yeni_sl <= mark_price:
            yeni_sl = (
                mark_price *
                1.002
            )

    yeni_sl = float(
        exchange.price_to_precision(
            symbol,
            yeni_sl
        )
    )

    eski_sl = pozisyon_son_sl.get(
        symbol
    )

    # Stop daha kötüye gitmeyecek.
    if eski_sl is not None:

        if side == "long" and yeni_sl <= eski_sl:
            return

        if side == "short" and yeni_sl >= eski_sl:
            return

    try:

        # Gereksiz cancel/create spam'i yok.
        exchange.cancel_all_orders(
            symbol
        )

        time.sleep(0.15)

        close_side = (
            "sell"
            if side == "long"
            else "buy"
        )

        exchange.create_order(
            symbol,
            "stop_market",
            close_side,
            contracts,
            None,
            {
                "stopPrice": yeni_sl,
                "reduceOnly": True,
                "workingType": "MARK_PRICE"
            }
        )

        pozisyon_son_sl[symbol] = yeni_sl

        logging.info(
            f"[TRAILING] "
            f"{symbol} | "
            f"MaxROI %{current_max:.2f} | "
            f"YeniSL {yeni_sl}"
        )

    except Exception as e:

        logging.error(
            f"Trailing hata {symbol}: {e}"
        )


# ============================================================
# OPPORTUNITY MOMENTUM EXIT
# ============================================================

def opportunity_momentum_exit(
    exchange,
    symbol,
    side,
    contracts,
    roi
):

    if not OPPORTUNITY_MOMENTUM_EXIT_ENABLED:
        return False

    # Zararda iken sadece exhaustion nedeniyle
    # agresif kapatma yapmıyoruz.
    if roi < 0:
        return False

    df = ohlcv_getir(
        exchange,
        symbol,
        OPPORTUNITY_SCAN_TIMEFRAME,
        100
    )

    if df is None:
        return False

    direction = (
        "buy"
        if side == "long"
        else "sell"
    )

    mom = calculate_momentum_engine(
        df,
        direction
    )

    # Büyük kâr + momentum dönüşü
    if (
        roi >= 12 and
        (
            mom["state"] == "EXHAUSTING"
            or
            (
                mom["acceleration_score"] < 40
                and
                mom["momentum_score"] < 55
            )
        )
    ):

        logging.warning(
            f"[OPPORTUNITY TREND EXIT] "
            f"{symbol} | "
            f"ROI %{roi:.2f} | "
            f"Momentum {mom['momentum_score']} | "
            f"Acceleration {mom['acceleration_score']}"
        )

        return pozisyon_kapat(
            exchange,
            symbol,
            side,
            contracts,
            "TREND_REVERSAL"
        )

    return False


# ============================================================
# POSITION MANAGER
# ============================================================

def pozisyonlari_yonet(
    exchange,
    positions
):

    global onceki_aktif_pozisyonlar

    aktif_semboller = {
        sembol_duzelt(
            p.get("symbol")
        )
        for p in positions
        if float(
            p.get("contracts") or 0
        ) > 0
    }

    # --------------------------------------------------------
    # KAPANANLAR
    # --------------------------------------------------------

    kapananlar = (
        onceki_aktif_pozisyonlar -
        aktif_semboller
    )

    for sym in kapananlar:

        pozisyon_tipleri.pop(
            sym,
            None
        )

        pozisyon_en_yuksek_kar.pop(
            sym,
            None
        )

        pozisyon_yonleri.pop(
            sym,
            None
        )

        pozisyon_giris_fiyatlari.pop(
            sym,
            None
        )

        pozisyon_acilis_zamanlari.pop(
            sym,
            None
        )

        pozisyon_son_sl.pop(
            sym,
            None
        )

        try:
            exchange.cancel_all_orders(
                sym
            )
        except Exception:
            pass

        logging.info(
            f"[POZİSYON KAPANDI] {sym}"
        )

    onceki_aktif_pozisyonlar = (
        aktif_semboller.copy()
    )

    # --------------------------------------------------------
    # AKTİF
    # --------------------------------------------------------

    for p in positions:

        symbol = sembol_duzelt(
            p.get("symbol")
        )

        try:

            contracts = float(
                p.get("contracts") or 0
            )

            if contracts <= 0:
                continue

            side = p.get("side")

            entry_price = float(
                p.get("entryPrice") or 0
            )

            mark_price = float(
                p.get("markPrice") or 0
            )

            leverage = float(
                p.get("leverage") or LEVERAGE
            )

            roi = float(
                p.get("percentage") or 0
            )

            if entry_price <= 0 or mark_price <= 0:
                continue

            p_type = pozisyon_tipini_cozumle(p)

            # ------------------------------------------------
            # MAX ROI
            # ------------------------------------------------

            current_max = (
                pozisyon_en_yuksek_kar
                .get(symbol, 0)
            )

            if roi > current_max:

                pozisyon_en_yuksek_kar[
                    symbol
                ] = roi

                current_max = roi

                logging.info(
                    f"[ZİRVE] "
                    f"{symbol} | "
                    f"ROI %{roi:.2f}"
                )

            logging.info(
                f"[POZİSYON] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{side.upper()} | "
                f"ROI %{roi:.2f} | "
                f"MAX %{current_max:.2f}"
            )

            # ------------------------------------------------
            # SCALP
            # ------------------------------------------------

            if p_type == "scalp":

                if scalp_time_stop(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    roi
                ):
                    continue

                if scalp_profit_protection(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    roi
                ):
                    continue

            # ------------------------------------------------
            # OPPORTUNITY
            # ------------------------------------------------

            else:

                if opportunity_momentum_exit(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    roi
                ):
                    continue

                opportunity_trailing(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    entry_price,
                    mark_price,
                    leverage,
                    current_max
                )

        except Exception as e:

            logging.error(
                f"Pozisyon yönetimi hata "
                f"{symbol}: {e}"
            )


# ============================================================
# POSITION MONITOR LOOP
# ============================================================

def pozisyon_monitor_loop():

    global monitor_basladi

    if (
        not POSITION_MONITOR_ENABLED
        or monitor_basladi
    ):
        return

    monitor_basladi = True

    exchange = None

    while True:

        try:

            if exchange is None:

                exchange = get_exchange()

                exchange.load_markets()

            if pozisyon_monitor_lock.acquire(
                blocking=False
            ):

                try:

                    positions = (
                        exchange.fetch_positions()
                    )

                    active_pos = [
                        p
                        for p in positions
                        if float(
                            p.get("contracts") or 0
                        ) > 0
                    ]

                    pozisyonlari_yonet(
                        exchange,
                        active_pos
                    )

                finally:

                    pozisyon_monitor_lock.release()

        except Exception as e:

            logging.error(
                f"Monitor bağlantı/hata: {e}"
            )

            exchange = None

        # Süre 15 saniyeye güncellendi
        time.sleep(15.0)


# ============================================================
# MONITOR START
# ============================================================

def monitor_baslat():

    if POSITION_MONITOR_ENABLED:

        threading.Thread(
            target=pozisyon_monitor_loop,
            daemon=True,
            name="PositionMonitor"
        ).start()


# ============================================================
# CANDIDATE LOG
# ============================================================

def candidate_log(
    mode,
    symbol,
    direction,
    evaluation
):

    mom = evaluation.get(
        "momentum",
        {}
    )

    logging.info(
        f"ANALİZ: "
        f"{symbol} | "
        f"Mode={mode.upper()} | "
        f"Dir={direction.upper()} | "
        f"Final={evaluation.get('final_score', 0):.2f} | "
        f"Trend={mom.get('trend_score', 0):.1f} | "
        f"Momentum={mom.get('momentum_score', 0):.1f} | "
        f"Acceleration={mom.get('acceleration_score', 0):.1f} | "
        f"Entry={mom.get('entry_score', 0):.1f} | "
        f"Exhaustion={mom.get('exhaustion_score', 0):.1f} | "
        f"Structure={mom.get('structure_score', 0):.1f} | "
        f"Pullback={mom.get('pullback_score', 0):.1f} | "
        f"State={mom.get('state', 'NA')} | "
        f"BreakoutATR={mom.get('breakout_distance_atr', 0):.2f} | "
        f"Volume={mom.get('volume_ratio', 0):.2f} | "
        f"Trigger={mom.get('trigger_score', 0):.1f} | "
        f"R²={evaluation.get('regression_r2', 0):.2f}"
    )


# ============================================================
# MAIN SCAN LOOP
# ============================================================

def ana_tarama_dongusu():

    global son_detayli_analiz_raporu

    monitor_baslat()

    while True:

        exchange = None

        try:

            exchange = get_exchange()

            exchange.load_markets()

            logging.info(
                "=============================================="
            )

            logging.info(
                ">>> YENİ HİBRİT ANALİZ BAŞLADI <<<"
            )

            logging.info(
                "=============================================="
            )

            anlik_islem_loglari = []
            scalp_takip = []
            firsat_takip = []
            aktif_pozisyonlar_roi_listesi = []
            aciklama_loglari = []

            # ------------------------------------------------
            # POSITIONS
            # ------------------------------------------------

            positions = (
                exchange.fetch_positions()
            )

            active_pos = [
                p
                for p in positions
                if float(
                    p.get("contracts") or 0
                ) > 0
            ]

            aktif_scalp_var = False
            aktif_firsat_var = False

            for p in active_pos:

                sym = sembol_duzelt(
                    p.get("symbol")
                )

                turu = pozisyon_tipini_cozumle(
                    p
                )

                roi = float(
                    p.get("percentage") or 0
                )

                max_roi = (
                    pozisyon_en_yuksek_kar
                    .get(sym, 0)
                )

                aktif_pozisyonlar_roi_listesi.append({
                    "symbol": sym,
                    "mod": turu.upper(),
                    "binance_gercek_roi_yuzde": round(
                        roi,
                        2
                    ),
                    "max_gorulen_zirve_kar_yuzde": round(
                        max_roi,
                        2
                    )
                })

                if turu == "scalp":
                    aktif_scalp_var = True
                else:
                    aktif_firsat_var = True

            # =================================================
            # OPPORTUNITY
            # =================================================

            if not aktif_firsat_var:

                logging.info(
                    "[FIRSAT] Pozisyon yok -> tarama başlıyor"
                )

                firsat_listesi = (
                    scan_opportunity_market(
                        exchange
                    )
                )

                logging.info(
                    f"[FIRSAT] "
                    f"{len(firsat_listesi)} aday"
                )

                for i, candidate in enumerate(
                    firsat_listesi,
                    1
                ):

                    sym = candidate["symbol"]
                    direction = candidate["direction"]

                    evaluation = evaluate_entry(
                        exchange,
                        sym,
                        direction,
                        "opportunity",
                        candidate["df"]
                    )

                    candidate_log(
                        "opportunity",
                        sym,
                        direction,
                        evaluation
                    )

                    mom = candidate["momentum"]

                    firsat_takip.append({
                        "symbol": sym,
                        "yon": direction,
                        "final_score": evaluation.get(
                            "final_score",
                            0
                        ),
                        "entry_score": mom.get(
                            "entry_score",
                            0
                        ),
                        "momentum": mom.get(
                            "momentum_score",
                            0
                        ),
                        "acceleration": mom.get(
                            "acceleration_score",
                            0
                        ),
                        "exhaustion": mom.get(
                            "exhaustion_score",
                            0
                        ),
                        "state": mom.get(
                            "state",
                            ""
                        ),
                        "volume": mom.get(
                            "volume_ratio",
                            0
                        ),
                        "breakout_atr": mom.get(
                            "breakout_distance_atr",
                            0
                        ),
                        "r2": evaluation.get(
                            "regression_r2",
                            0
                        ),
                        "approved": evaluation.get(
                            "approved",
                            False
                        )
                    })

                # En yüksek final skorunu seç
                evaluated = []

                for candidate in firsat_listesi:

                    evaluation = evaluate_entry(
                        exchange,
                        candidate["symbol"],
                        candidate["direction"],
                        "opportunity",
                        candidate["df"]
                    )

                    if evaluation.get(
                        "approved",
                        False
                    ):

                        evaluated.append(
                            (
                                evaluation.get(
                                    "final_score",
                                    0
                                ),
                                candidate,
                                evaluation
                            )
                        )

                evaluated.sort(
                    key=lambda x: x[0],
                    reverse=True
                )

                if evaluated:

                    final_score, candidate, evaluation = (
                        evaluated[0]
                    )

                    sym = candidate["symbol"]
                    direction = candidate["direction"]

                    mom = evaluation["momentum"]

                    analiz_detayi = (
                        f"Final={final_score:.2f} | "
                        f"Trend={mom['trend_score']:.1f} | "
                        f"Momentum={mom['momentum_score']:.1f} | "
                        f"Acceleration={mom['acceleration_score']:.1f} | "
                        f"Entry={mom['entry_score']:.1f} | "
                        f"Exhaustion={mom['exhaustion_score']:.1f} | "
                        f"Structure={mom['structure_score']:.1f} | "
                        f"Pullback={mom['pullback_score']:.1f} | "
                        f"State={mom['state']} | "
                        f"BreakoutATR={mom['breakout_distance_atr']:.2f} | "
                        f"Volume={mom['volume_ratio']:.2f} | "
                        f"R²={evaluation['regression_r2']:.2f}"
                    )

                    logging.info(
                        f"[FIRSAT ONAY] "
                        f"{sym} | "
                        f"{direction.upper()} | "
                        f"{analiz_detayi}"
                    )

                    basarili = pozisyon_ac(
                        exchange,
                        sym,
                        direction,
                        final_score,
                        "opportunity",
                        analiz_detayi
                    )

                    if basarili:

                        anlik_islem_loglari.append(
                            f"Fırsat: {sym} "
                            f"{direction.upper()} "
                            f"Final={final_score:.2f}"
                        )

                else:

                    logging.info(
                        "[FIRSAT] Uygun entry bulunamadı"
                    )

                # Data cleanup
                for item in firsat_listesi:

                    if (
                        item.get("df") is not None
                    ):

                        del item["df"]

            else:

                logging.info(
                    "[FIRSAT] Aktif pozisyon mevcut"
                )

            # =================================================
            # SCALP
            # =================================================

            if not aktif_scalp_var:

                logging.info(
                    "[SCALP] Pozisyon yok -> tarama başlıyor"
                )

                scalp_listesi = (
                    scan_scalp_market(
                        exchange
                    )
                )

                logging.info(
                    f"[SCALP] "
                    f"{len(scalp_listesi)} aday"
                )

                for candidate in scalp_listesi:

                    sym = candidate["symbol"]
                    direction = candidate["direction"]

                    evaluation = evaluate_entry(
                        exchange,
                        sym,
                        direction,
                        "scalp",
                        candidate["df"]
                    )

                    candidate_log(
                        "scalp",
                        sym,
                        direction,
                        evaluation
                    )

                    mom = candidate["momentum"]

                    scalp_takip.append({
                        "symbol": sym,
                        "yon": direction,
                        "final_score": evaluation.get(
                            "final_score",
                            0
                        ),
                        "entry_score": mom.get(
                            "entry_score",
                            0
                        ),
                        "momentum": mom.get(
                            "momentum_score",
                            0
                        ),
                        "acceleration": mom.get(
                            "acceleration_score",
                            0
                        ),
                        "exhaustion": mom.get(
                            "exhaustion_score",
                            0
                        ),
                        "state": mom.get(
                            "state",
                            ""
                        ),
                        "volume": mom.get(
                            "volume_ratio",
                            0
                        ),
                        "breakout_atr": mom.get(
                            "breakout_distance_atr",
                            0
                        ),
                        "r2": evaluation.get(
                            "regression_r2",
                            0
                        ),
                        "approved": evaluation.get(
                            "approved",
                            False
                        )
                    })

                # ------------------------------------------------
                # En iyi Scalping entry
                # ------------------------------------------------

                evaluated = []

                for candidate in scalp_listesi:

                    evaluation = evaluate_entry(
                        exchange,
                        candidate["symbol"],
                        candidate["direction"],
                        "scalp",
                        candidate["df"]
                    )

                    if evaluation.get(
                        "approved",
                        False
                    ):

                        evaluated.append(
                            (
                                evaluation.get(
                                    "final_score",
                                    0
                                ),
                                candidate,
                                evaluation
                            )
                        )

                evaluated.sort(
                    key=lambda x: x[0],
                    reverse=True
                )

                if evaluated:

                    final_score, candidate, evaluation = (
                        evaluated[0]
                    )

                    sym = candidate["symbol"]
                    direction = candidate["direction"]

                    mom = evaluation["momentum"]

                    analiz_detayi = (
                        f"Final={final_score:.2f} | "
                        f"Trend={mom['trend_score']:.1f} | "
                        f"Momentum={mom['momentum_score']:.1f} | "
                        f"Acceleration={mom['acceleration_score']:.1f} | "
                        f"Entry={mom['entry_score']:.1f} | "
                        f"Exhaustion={mom['exhaustion_score']:.1f} | "
                        f"Structure={mom['structure_score']:.1f} | "
                        f"Pullback={mom['pullback_score']:.1f} | "
                        f"State={mom['state']} | "
                        f"BreakoutATR={mom['breakout_distance_atr']:.2f} | "
                        f"Volume={mom['volume_ratio']:.2f} | "
                        f"Trigger={mom['trigger_score']:.1f} | "
                        f"R²={evaluation['regression_r2']:.2f}"
                    )

                    logging.info(
                        f"[SCALP ONAY] "
                        f"{sym} | "
                        f"{direction.upper()} | "
                        f"{analiz_detayi}"
                    )

                    basarili = pozisyon_ac(
                        exchange,
                        sym,
                        direction,
                        final_score,
                        "scalp",
                        analiz_detayi
                    )

                    if basarili:

                        anlik_islem_loglari.append(
                            f"Scalp: {sym} "
                            f"{direction.upper()} "
                            f"Final={final_score:.2f}"
                        )

                else:

                    logging.info(
                        "[SCALP] Uygun entry bulunamadı"
                    )

                for item in scalp_listesi:

                    if (
                        item.get("df") is not None
                    ):

                        del item["df"]

            else:

                logging.info(
                    "[SCALP] Aktif pozisyon mevcut"
                )

            # =================================================
            # RAPOR
            # =================================================

            son_detayli_analiz_raporu = {
                "zaman": pd.Timestamp.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "scalp_takip_listesi": scalp_takip,
                "firsat_takip_listesi": firsat_takip,
                "aktif_pozisyonlar_roi_durumu":
                    aktif_pozisyonlar_roi_listesi,
                "yapilan_islemler":
                    anlik_islem_loglari,
                "aciklamalar":
                    aciklama_loglari
            }

            active_pos.clear()

        except Exception as e:

            logging.error(
                f"Ana döngü hatası: {e}"
            )

            son_detayli_analiz_raporu[
                "hata"
            ] = str(e)

        finally:

            gc.collect()

        logging.info(
            ">>> ANALİZ TAMAMLANDI - "
            "300 SANİYE BEKLENİYOR <<<"
        )

        # Süre 300 saniyeye güncellendi
        time.sleep(300)


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def index():

    return jsonify({
        "status":
            "Bot aktif - Momentum + Entry Timing Engine",
        "trading_enabled":
            TRADING_ENABLED,
        "positions":
            list(
                pozisyon_tipleri.keys()
            )
    })


@app.route("/durum")
def durum():

    try:

        exchange = get_exchange()

        exchange.load_markets()

        positions = (
            exchange.fetch_positions()
        )

        active_pos = [
            p
            for p in positions
            if float(
                p.get("contracts") or 0
            ) > 0
        ]

        detaylar = []

        for p in active_pos:

            sym = sembol_duzelt(
                p.get("symbol")
            )

            p_type = (
                pozisyon_tipini_cozumle(p)
            )

            entry = float(
                p.get("entryPrice") or 0
            )

            mark = float(
                p.get("markPrice") or 0
            )

            lev = float(
                p.get("leverage") or LEVERAGE
            )

            side = p.get("side")

            roi = float(
                p.get("percentage") or 0
            )

            detaylar.append({
                "symbol": sym,
                "mod": p_type.upper(),
                "yon": (
                    side.upper()
                    if side
                    else ""
                ),
                "giris_fiyati": entry,
                "anlik_fiyat": mark,
                "kaldirac": lev,
                "binance_gercek_roi_yuzde":
                    round(roi, 2),
                "max_gorulen_zirve_kar_yuzde":
                    round(
                        pozisyon_en_yuksek_kar.get(
                            sym,
                            0
                        ),
                        2
                    ),
                "trailing_stop":
                    pozisyon_son_sl.get(
                        sym
                    )
            })

        return jsonify({
            "success": True,
            "aktif_islem_sayisi":
                len(detaylar),
            "islemler":
                detaylar
        })

    except Exception as e:

        return jsonify({
            "success": False,
            "error": str(e)
        })


@app.route("/otomatik-analiz")
def otomatik_analiz():

    return jsonify({
        "success":
            True,
        "mesaj":
            "Momentum & Entry Timing analiz raporu",
        "analiz_raporu":
            son_detayli_analiz_raporu
    })


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    t = threading.Thread(
        target=ana_tarama_dongusu,
        daemon=True,
        name="AnalysisLoop"
    )

    t.start()

    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
