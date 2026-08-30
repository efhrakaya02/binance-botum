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
# DİNAMİK TRADE PLAN + MOMENTUM CONTINUATION
# DİNAMİK TP + RİSK KORUMASI + TRAILING
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
SCALP_MIN_TARGET_USDT = 0.25
SCALP_MAX_TARGET_USDT = 0.55

SCALP_FEE_BUFFER_USDT = 0.04

SCALP_MIN_HOLD_MINUTES = 3
SCALP_MAX_HOLD_MINUTES = 25

SCALP_EARLY_PROFIT_PROTECTION_ENABLED = True
SCALP_EARLY_PROFIT_MIN_ROI = 1.5
SCALP_PROFIT_LOCK_ROI = 1.8

# ============================================================
# OPPORTUNITY
# ============================================================

OPPORTUNITY_TARGET_USDT = 0.75
OPPORTUNITY_MIN_TARGET_USDT = 0.55
OPPORTUNITY_MAX_TARGET_USDT = 1.50

OPPORTUNITY_MIN_HOLD_MINUTES = 10
OPPORTUNITY_MAX_HOLD_HOURS = 8

OPPORTUNITY_MOMENTUM_EXIT_ENABLED = True

# ============================================================
# RİSK YÖNETİMİ
# ============================================================

# KRİTİK:
# Maksimum planlı zarar = hedef kârın %75'i
MAX_LOSS_TO_TARGET_RATIO = 0.75

# Pozisyonun zarar oranı hedefe göre hesaplanır.
# Margin üzerinden ROI hesaplanır.

# ============================================================
# TRAILING STOP
# ============================================================

TRAILING_ENABLED = True

SCALP_TRAILING_START_RATIO = 0.55
SCALP_TRAILING_LOCK_RATIO = 0.35

OPPORTUNITY_TRAILING_START_RATIO = 0.50
OPPORTUNITY_TRAILING_LOCK_RATIO = 0.30

TRAILING_ATR_MULT_MIN = 0.45
TRAILING_ATR_MULT_MAX = 1.10

# ============================================================
# MOMENTUM CONTINUATION
# ============================================================

MOMENTUM_ENGINE_ENABLED = True
MOMENTUM_CONTINUATION_ENABLED = True

MOMENTUM_HEALTH_EXIT_SCORE = 35
MOMENTUM_WEAK_SCORE = 48
MOMENTUM_STRONG_SCORE = 70

# ============================================================
# ENTRY
# ============================================================

ENTRY_TIMING_ENABLED = True
ENTRY_REQUIRE_TRIGGER = True
ENTRY_REQUIRE_CONFIRMATION = True

ENTRY_MAX_CANDLE_ATR = 1.80

# ============================================================
# TARAMA
# ============================================================

GAINER_COUNT = 20
LOSER_COUNT = 20

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

SCALP_MIN_ACCELERATION = 70
OPPORTUNITY_MIN_ACCELERATION = 65

SCALP_MAX_EXHAUSTION = 55
OPPORTUNITY_MAX_EXHAUSTION = 60

# ============================================================
# BREAKOUT & HACİM
# ============================================================

SCALP_MAX_BREAKOUT_ATR = 0.90
OPPORTUNITY_MAX_BREAKOUT_ATR = 1.50

IDEAL_BREAKOUT_ATR = 0.65

SCALP_MIN_VOLUME_RATIO = 1.30
OPPORTUNITY_MIN_VOLUME_RATIO = 1.50

# ============================================================
# COOLDOWN
# ============================================================

COOLDOWN_HOURS = 4
cooldown_map = {}

# ============================================================
# RUNTIME STATE
# ============================================================

pozisyon_en_yuksek_kar = {}
pozisyon_en_yuksek_roi = {}

pozisyon_tipleri = {}
pozisyon_yonleri = {}

pozisyon_giris_fiyatlari = {}
pozisyon_acilis_zamanlari = {}

pozisyon_son_sl = {}

pozisyon_trade_plan = {}
pozisyon_saglik_loglari = {}

pozisyon_son_momentum = {}
pozisyon_son_analiz_zamani = {}

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


# ============================================================
# SYMBOL
# ============================================================

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

        # EMA
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

        df["ema200"] = df["close"].ewm(
            span=200,
            adjust=False
        ).mean()

        # MACD
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

        # RSI
        delta = df["close"].diff()

        gain = delta.where(
            delta > 0,
            0.0
        )

        loss = -delta.where(
            delta < 0,
            0.0
        )

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

        df["rsi"] = 100 - (
            100 / (1 + rs)
        )

        # ATR
        high_low = df["high"] - df["low"]

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

        # ADX
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

        # ROC
        df["roc"] = (
            df["close"].pct_change(9) * 100
        )

        # Bollinger
        sma20 = df["close"].rolling(20).mean()
        std20 = df["close"].rolling(20).std()

        df["bb_middle"] = sma20

        df["bb_upper"] = (
            sma20 +
            std20 * 2
        )

        df["bb_lower"] = (
            sma20 -
            std20 * 2
        )

        df["bb_width"] = (
            (
                df["bb_upper"] -
                df["bb_lower"]
            ) /
            sma20.replace(0, np.nan)
        )

        # OBV
        direction_sign = np.sign(
            df["close"].diff()
        )

        df["obv"] = (
            direction_sign *
            df["volume"]
        ).fillna(0).cumsum()

        # VWAP
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

        # Candle
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

        # Volume
        df["volume_ma20"] = (
            df["volume"].rolling(20).mean()
        )

        df["volume_ratio"] = (
            df["volume"] /
            df["volume_ma20"].replace(
                0,
                np.nan
            )
        )

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


def son_kapanmis_mum(df):
    if df is None or len(df) < 3:
        return None

    return df.iloc[-2]


# ============================================================
# TREND SCORE
# ============================================================

def trend_score_hesapla(df, direction):
    if df is None or len(df) < 30:
        return 50.0

    d = df.iloc[:-1]
    last = d.iloc[-1]

    score = 50.0

    if direction == "buy":

        if last["close"] > last["ema21"]:
            score += 10

        if last["ema9"] > last["ema21"]:
            score += 10

        if last["ema21"] > last["ema50"]:
            score += 10

        if last["ema50"] > last["ema200"]:
            score += 10

        if last["plus_di"] > last["minus_di"]:
            score += 5

        if last["adx"] > 20:
            score += 5

    else:

        if last["close"] < last["ema21"]:
            score += 10

        if last["ema9"] < last["ema21"]:
            score += 10

        if last["ema21"] < last["ema50"]:
            score += 10

        if last["ema50"] < last["ema200"]:
            score += 10

        if last["minus_di"] > last["plus_di"]:
            score += 5

        if last["adx"] > 20:
            score += 5

    return min(max(score, 0), 100)


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

    closes = df[
        "close"
    ].iloc[
        -periyot - 1:-1
    ].values

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
        slope *
        (len(closes) - 1)
    )

    atr = float(
        df["atr"].iloc[-2]
    )

    if atr <= 0:
        atr = price * 0.01

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
        f"R²={r_squared:.2f}"
    )

    return (
        ok,
        slope,
        r_squared,
        mesaj
    )


# ============================================================
# STRUCTURE
# ============================================================

def structure_score(df, direction):
    if df is None or len(df) < 30:
        return 50

    d = df.iloc[:-1]

    last = d.iloc[-1]
    prev = d.iloc[-2]

    score = 50

    recent_high = d[
        "high"
    ].iloc[-20:-1].max()

    recent_low = d[
        "low"
    ].iloc[-20:-1].min()

    if direction == "buy":

        if last["close"] > last["ema21"]:
            score += 10

        if last["ema21"] > last["ema50"]:
            score += 10

        if last["close"] > prev["close"]:
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

        if last["close"] <= recent_low:
            score += 15

    return min(max(score, 0), 100)


# ============================================================
# PULLBACK
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

        if last["rsi"] >= 50:
            score += 5

    else:

        if prev["high"] >= prev["ema21"]:
            score += 20

        if last["close"] < last["ema21"]:
            score += 15

        if last["rsi"] <= 50:
            score += 5

    return min(max(score, 0), 100)


# ============================================================
# BREAKOUT
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

    previous_high = d[
        "high"
    ].iloc[-21:-1].max()

    previous_low = d[
        "low"
    ].iloc[-21:-1].min()

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

    distance = max(
        distance,
        0
    )

    quality = (
        70
        if breakout
        else 30
    )

    return {
        "breakout": bool(breakout),
        "distance_atr": round(
            float(distance),
            3
        ),
        "fresh": True,
        "quality": quality
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

    score = 50

    if direction == "buy":

        if last["close"] > last["open"]:
            score += 25

        if last["body_ratio"] >= 0.55:
            score += 15

    else:

        if last["close"] < last["open"]:
            score += 25

        if last["body_ratio"] >= 0.55:
            score += 15

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
    prev = d.iloc[-2]

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    momentum = 50

    if direction == "buy":

        if last["ema9"] > last["ema21"]:
            momentum += 15

        if last["macd_hist"] > 0:
            momentum += 15

        if last["rsi"] > 50:
            momentum += 10

        if last["close"] > prev["close"]:
            momentum += 10

    else:

        if last["ema9"] < last["ema21"]:
            momentum += 15

        if last["macd_hist"] < 0:
            momentum += 15

        if last["rsi"] < 50:
            momentum += 10

        if last["close"] < prev["close"]:
            momentum += 10

    momentum = min(
        max(momentum, 0),
        100
    )

    # --------------------------------------------------------
    # ACCELERATION
    # --------------------------------------------------------

    adx_now = float(last["adx"])
    adx_prev = float(prev["adx"])

    acceleration = 50

    if adx_now > 20:
        acceleration += 10

    if adx_now > 25:
        acceleration += 10

    if adx_now > adx_prev:
        acceleration += 15

    if direction == "buy":
        if last["macd_hist"] > prev["macd_hist"]:
            acceleration += 15
    else:
        if last["macd_hist"] < prev["macd_hist"]:
            acceleration += 15

    acceleration = min(
        max(acceleration, 0),
        100
    )

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    volume_ratio = float(
        last["volume_ratio"]
    )

    volume_score = min(
        100,
        50 +
        (volume_ratio - 1) * 35
    )

    # --------------------------------------------------------
    # STRUCTURE
    # --------------------------------------------------------

    breakout = breakout_analysis(
        df,
        direction
    )

    structure = structure_score(
        df,
        direction
    )

    pullback = pullback_score(
        df,
        direction
    )

    candle = candle_quality(
        df,
        direction
    )

    trend = trend_score_hesapla(
        df,
        direction
    )

    # --------------------------------------------------------
    # EXHAUSTION
    # --------------------------------------------------------

    exhaustion = 0

    if direction == "buy":

        if last["rsi"] > 70:
            exhaustion += 20

        if last["rsi"] > 78:
            exhaustion += 25

        if breakout["distance_atr"] > 0.8:
            exhaustion += 15

    else:

        if last["rsi"] < 30:
            exhaustion += 20

        if last["rsi"] < 22:
            exhaustion += 25

        if breakout["distance_atr"] > 0.8:
            exhaustion += 15

    exhaustion = min(
        exhaustion,
        100
    )

    # --------------------------------------------------------
    # TRIGGER
    # --------------------------------------------------------

    trigger = 50

    if direction == "buy":

        if last["close"] > last["open"]:
            trigger += 15

        if last["close"] > prev["close"]:
            trigger += 15

        if last["macd_hist"] > prev["macd_hist"]:
            trigger += 20

    else:

        if last["close"] < last["open"]:
            trigger += 15

        if last["close"] < prev["close"]:
            trigger += 15

        if last["macd_hist"] < prev["macd_hist"]:
            trigger += 20

    trigger = min(
        max(trigger, 0),
        100
    )

    # --------------------------------------------------------
    # ENTRY SCORE
    # --------------------------------------------------------

    entry_score = (
        momentum * 0.25 +
        acceleration * 0.20 +
        volume_score * 0.15 +
        structure * 0.15 +
        pullback * 0.10 +
        trend * 0.10 +
        trigger * 0.05
    )

    entry_score = min(
        max(entry_score, 0),
        100
    )

    # --------------------------------------------------------
    # STATE
    # --------------------------------------------------------

    if (
        momentum >= 80 and
        acceleration >= 75
    ):
        state = "ACCELERATING"

    elif momentum >= 70:
        state = "BUILDING"

    elif momentum >= 55:
        state = "WEAKENING"

    else:
        state = "FADING"

    return {
        "momentum_score": round(
            momentum,
            2
        ),
        "acceleration_score": round(
            acceleration,
            2
        ),
        "exhaustion_score": round(
            exhaustion,
            2
        ),
        "entry_score": round(
            entry_score,
            2
        ),
        "state": state,
        "breakout_distance_atr": breakout[
            "distance_atr"
        ],
        "breakout_quality": breakout[
            "quality"
        ],
        "breakout_fresh": breakout[
            "fresh"
        ],
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
# HIGHER TIMEFRAME CONFIRMATION
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
        100
    )

    if df4 is None:
        return {
            "ok": False,
            "score": 0,
            "trend": "NO_DATA"
        }

    d = df4.iloc[:-1]

    last = d.iloc[-1]

    trend_score = trend_score_hesapla(
        df4,
        direction
    )

    if direction == "buy":

        ok = (
            last["close"] >
            last["ema50"] and
            last["ema21"] >
            last["ema50"]
        )

        trend = (
            "BULLISH"
            if ok
            else "BEARISH"
        )

    else:

        ok = (
            last["close"] <
            last["ema50"] and
            last["ema21"] <
            last["ema50"]
        )

        trend = (
            "BEARISH"
            if ok
            else "BULLISH"
        )

    return {
        "ok": bool(ok),
        "score": round(
            trend_score,
            2
        ),
        "trend": trend
    }


# ============================================================
# TRIGGER CONFIRMATION
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
        80
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

    score = 0

    if mom["momentum_score"] >= 65:
        score += 25

    if mom["acceleration_score"] >= 65:
        score += 25

    if mom["trigger_score"] >= 65:
        score += 25

    if mom["volume_ratio"] >= 1.15:
        score += 15

    if mom["exhaustion_score"] <= 50:
        score += 10

    score = min(
        score,
        100
    )

    ok = (
        score >= 70 and
        mom["momentum_score"] >= 60 and
        mom["acceleration_score"] >= 55
    )

    return {
        "ok": bool(ok),
        "score": round(score, 2),
        "state": mom["state"],
        "momentum": mom
    }


# ============================================================
# ENTRY EVALUATION
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

    # --------------------------------------------------------
    # MODE CONFIG
    # --------------------------------------------------------

    if mode == "scalp":

        min_final = SCALP_MIN_FINAL_SCORE
        min_entry = SCALP_MIN_ENTRY_SCORE
        min_momentum = SCALP_MIN_MOMENTUM
        min_acceleration = SCALP_MIN_ACCELERATION
        max_exhaustion = SCALP_MAX_EXHAUSTION
        max_breakout = SCALP_MAX_BREAKOUT_ATR
        min_volume = SCALP_MIN_VOLUME_RATIO
        trigger_tf = SCALP_TRIGGER_TIMEFRAME

    else:

        min_final = OPPORTUNITY_MIN_FINAL_SCORE
        min_entry = OPPORTUNITY_MIN_ENTRY_SCORE
        min_momentum = OPPORTUNITY_MIN_MOMENTUM
        min_acceleration = OPPORTUNITY_MIN_ACCELERATION
        max_exhaustion = OPPORTUNITY_MAX_EXHAUSTION
        max_breakout = OPPORTUNITY_MAX_BREAKOUT_ATR
        min_volume = OPPORTUNITY_MIN_VOLUME_RATIO
        trigger_tf = OPPORTUNITY_TRIGGER_TIMEFRAME

    # --------------------------------------------------------
    # BASIC FILTERS
    # --------------------------------------------------------

    if mom["momentum_score"] < min_momentum:
        return {
            "approved": False,
            "reason": "LOW_MOMENTUM",
            "momentum": mom
        }

    if mom["acceleration_score"] < min_acceleration:
        return {
            "approved": False,
            "reason": "LOW_ACCELERATION",
            "momentum": mom
        }

    if mom["exhaustion_score"] > max_exhaustion:
        return {
            "approved": False,
            "reason": "EXHAUSTION",
            "momentum": mom
        }

    if mom["volume_ratio"] < min_volume:
        return {
            "approved": False,
            "reason": "LOW_VOLUME",
            "momentum": mom
        }

    if mom["breakout_distance_atr"] > max_breakout:
        return {
            "approved": False,
            "reason": "LATE_ENTRY",
            "momentum": mom
        }

    # --------------------------------------------------------
    # REGRESSION
    # --------------------------------------------------------

    reg_ok, reg_slope, reg_r2, reg_msg = (
        gelismis_regresyon_teyidi(
            df,
            direction
        )
    )

    if not reg_ok:
        return {
            "approved": False,
            "reason": "REGRESSION_FAIL",
            "momentum": mom,
            "regression_r2": reg_r2
        }

    # --------------------------------------------------------
    # HIGHER TIMEFRAME
    # --------------------------------------------------------

    htf = higher_timeframe_confirmation(
        exchange,
        symbol,
        direction
    )

    if not htf["ok"]:
        return {
            "approved": False,
            "reason": "HTF_FAIL",
            "momentum": mom,
            "higher_timeframe": htf
        }

    # --------------------------------------------------------
    # TRIGGER
    # --------------------------------------------------------

    trigger = trigger_confirmation(
        exchange,
        symbol,
        direction,
        trigger_tf
    )

    if not trigger["ok"]:
        return {
            "approved": False,
            "reason": "TRIGGER_FAIL",
            "momentum": mom,
            "higher_timeframe": htf,
            "trigger": trigger
        }

    # --------------------------------------------------------
    # FINAL SCORE
    # --------------------------------------------------------

    final_score = (
        mom["entry_score"] * 0.45 +
        mom["momentum_score"] * 0.20 +
        mom["trend_score"] * 0.10 +
        trigger["score"] * 0.15 +
        htf["score"] * 0.10
    )

    final_score = min(
        max(final_score, 0),
        100
    )

    approved = (
        final_score >= min_final and
        mom["entry_score"] >= min_entry
    )

    return {
        "approved": bool(approved),
        "reason": (
            "APPROVED"
            if approved
            else "LOW_FINAL_SCORE"
        ),
        "final_score": round(
            final_score,
            2
        ),
        "momentum": mom,
        "higher_timeframe": htf,
        "trigger": trigger,
        "regression_r2": round(
            reg_r2,
            2
        ),
        "regression_slope": reg_slope,
        "regression_message": reg_msg
    }


# ============================================================
# DYNAMIC TARGET ENGINE
# ============================================================

def hesapla_dinamik_hedef(
    mode,
    margin,
    momentum_data,
    final_score
):

    momentum = momentum_data[
        "momentum_score"
    ]

    acceleration = momentum_data[
        "acceleration_score"
    ]

    volume = momentum_data[
        "volume_ratio"
    ]

    trend = momentum_data[
        "trend_score"
    ]

    exhaustion = momentum_data[
        "exhaustion_score"
    ]

    quality = (
        final_score * 0.35 +
        momentum * 0.20 +
        acceleration * 0.15 +
        trend * 0.15 +
        min(volume * 40, 100) * 0.10 +
        (100 - exhaustion) * 0.05
    )

    if mode == "scalp":

        base = SCALP_TARGET_USDT

        if quality >= 90:
            target = base * 1.35

        elif quality >= 85:
            target = base * 1.20

        elif quality >= 80:
            target = base * 1.05

        else:
            target = base * 0.90

        target = min(
            max(
                target,
                SCALP_MIN_TARGET_USDT
            ),
            SCALP_MAX_TARGET_USDT
        )

    else:

        base = OPPORTUNITY_TARGET_USDT

        if quality >= 92:
            target = base * 1.60

        elif quality >= 87:
            target = base * 1.35

        elif quality >= 82:
            target = base * 1.10

        else:
            target = base * 0.90

        target = min(
            max(
                target,
                OPPORTUNITY_MIN_TARGET_USDT
            ),
            OPPORTUNITY_MAX_TARGET_USDT
        )

    target_roi = (
        target /
        margin
    ) * 100

    max_loss_usdt = (
        target *
        MAX_LOSS_TO_TARGET_RATIO
    )

    max_loss_roi = (
        max_loss_usdt /
        margin
    ) * 100

    return {
        "target_usdt": round(
            target,
            4
        ),
        "target_roi": round(
            target_roi,
            3
        ),
        "max_loss_usdt": round(
            max_loss_usdt,
            4
        ),
        "max_loss_roi": round(
            max_loss_roi,
            3
        ),
        "quality_score": round(
            quality,
            2
        )
    }


# ============================================================
# DYNAMIC HOLD TIME
# ============================================================

def hesapla_dinamik_sure(
    mode,
    momentum_data,
    final_score
):

    momentum = momentum_data[
        "momentum_score"
    ]

    acceleration = momentum_data[
        "acceleration_score"
    ]

    volume = momentum_data[
        "volume_ratio"
    ]

    exhaustion = momentum_data[
        "exhaustion_score"
    ]

    quality = (
        final_score * 0.40 +
        momentum * 0.25 +
        acceleration * 0.20 +
        min(volume * 40, 100) * 0.10 +
        (100 - exhaustion) * 0.05
    )

    if mode == "scalp":

        if quality >= 90:
            minutes = 8

        elif quality >= 85:
            minutes = 12

        elif quality >= 80:
            minutes = 16

        else:
            minutes = 20

        minutes = min(
            max(
                minutes,
                SCALP_MIN_HOLD_MINUTES
            ),
            SCALP_MAX_HOLD_MINUTES
        )

    else:

        if quality >= 92:
            minutes = 45

        elif quality >= 87:
            minutes = 75

        elif quality >= 82:
            minutes = 120

        else:
            minutes = 180

        minutes = min(
            max(
                minutes,
                OPPORTUNITY_MIN_HOLD_MINUTES
            ),
            OPPORTUNITY_MAX_HOLD_HOURS * 60
        )

    return int(minutes)


# ============================================================
# PRICE FROM ROI
# ============================================================

def roi_to_price(
    entry_price,
    roi_percent,
    side,
    leverage
):

    price_change = (
        roi_percent /
        leverage /
        100
    )

    if side == "buy":
        return entry_price * (
            1 + price_change
        )

    return entry_price * (
        1 - price_change
    )


# ============================================================
# DYNAMIC TRADE PLAN
# ============================================================

def hesapla_dinamik_trade_plan(
    entry_price,
    side,
    atr,
    p_type,
    leverage,
    margin,
    momentum_data,
    final_score
):

    target = hesapla_dinamik_hedef(
        p_type,
        margin,
        momentum_data,
        final_score
    )

    hold_minutes = hesapla_dinamik_sure(
        p_type,
        momentum_data,
        final_score
    )

    target_roi = target[
        "target_roi"
    ]

    max_loss_roi = target[
        "max_loss_roi"
    ]

    # --------------------------------------------------------
    # TP PRICE
    # --------------------------------------------------------

    tp_price = roi_to_price(
        entry_price,
        target_roi,
        side,
        leverage
    )

    # --------------------------------------------------------
    # HARD RISK PRICE
    # --------------------------------------------------------

    hard_sl_price = roi_to_price(
        entry_price,
        -max_loss_roi,
        side,
        leverage
    )

    # --------------------------------------------------------
    # ATR INFORMATION
    # --------------------------------------------------------

    if atr <= 0:
        atr = entry_price * 0.01

    if p_type == "scalp":

        atr_sl_price = (
            entry_price -
            atr * 1.2
            if side == "buy"
            else
            entry_price +
            atr * 1.2
        )

    else:

        atr_sl_price = (
            entry_price -
            atr * 1.8
            if side == "buy"
            else
            entry_price +
            atr * 1.8
        )

    # HER ZAMAN daha korumacı olan stop kullanılır.
    if side == "buy":
        sl_price = max(
            hard_sl_price,
            atr_sl_price
        )
    else:
        sl_price = min(
            hard_sl_price,
            atr_sl_price
        )

    # Risk fiyatı hiçbir koşulda
    # hedefe göre izin verilen zararı aşamaz.

    return {
        "sl_price": float(sl_price),
        "tp_price": float(tp_price),

        "target_usdt": target[
            "target_usdt"
        ],

        "target_roi": target[
            "target_roi"
        ],

        "max_loss_usdt": target[
            "max_loss_usdt"
        ],

        "max_loss_roi": target[
            "max_loss_roi"
        ],

        "hold_minutes": hold_minutes,

        "atr_kullanilan": float(atr),

        "quality_score": target[
            "quality_score"
        ],

        "trailing_enabled": TRAILING_ENABLED,

        "trailing_started": False,

        "highest_roi": 0.0,

        "highest_pnl": 0.0
    }


# ============================================================
# MOMENTUM CONTINUATION
# ============================================================

def momentum_continuation_analizi(
    exchange,
    symbol,
    side,
    p_type
):

    timeframe = (
        SCALP_TRIGGER_TIMEFRAME
        if p_type == "scalp"
        else OPPORTUNITY_TRIGGER_TIMEFRAME
    )

    df = ohlcv_getir(
        exchange,
        symbol,
        timeframe,
        80
    )

    if df is None:
        return {
            "score": 0,
            "state": "NO_DATA",
            "continue": False
        }

    direction = (
        "buy"
        if side == "long"
        else "sell"
    )

    mom = calculate_momentum_engine(
        df,
        direction
    )

    score = 0

    # Momentum
    score += (
        mom["momentum_score"] *
        0.30
    )

    # Acceleration
    score += (
        mom["acceleration_score"] *
        0.25
    )

    # Trend
    score += (
        mom["trend_score"] *
        0.20
    )

    # Volume
    volume_component = min(
        mom["volume_ratio"] * 40,
        100
    )

    score += (
        volume_component *
        0.10
    )

    # Trigger
    score += (
        mom["trigger_score"] *
        0.10
    )

    # Exhaustion
    score += (
        (100 - mom["exhaustion_score"]) *
        0.05
    )

    score = min(
        max(score, 0),
        100
    )

    # --------------------------------------------------------
    # CONTINUATION
    # --------------------------------------------------------

    continuation = (
        score >= MOMENTUM_WEAK_SCORE and
        mom["exhaustion_score"] < 70
    )

    if score >= 75:
        state = "STRONG_CONTINUATION"

    elif score >= 60:
        state = "CONTINUING"

    elif score >= 48:
        state = "WEAKENING"

    else:
        state = "REVERSING"

    return {
        "score": round(
            score,
            2
        ),
        "state": state,
        "continue": bool(
            continuation
        ),
        "momentum": mom
    }


# ============================================================
# TRADE HEALTH
# ============================================================

def trade_health_analizi(
    df,
    side,
    current_roi,
    momentum_data
):

    score = 100

    last = df.iloc[-2]

    if side == "long":

        if last["close"] < last["ema21"]:
            score -= 25

        if last["ema9"] < last["ema21"]:
            score -= 15

        if last["macd_hist"] < 0:
            score -= 20

        if last["minus_di"] > last["plus_di"]:
            score -= 15

    else:

        if last["close"] > last["ema21"]:
            score -= 25

        if last["ema9"] > last["ema21"]:
            score -= 15

        if last["macd_hist"] > 0:
            score -= 20

        if last["plus_di"] > last["minus_di"]:
            score -= 15

    if momentum_data.get(
        "exhaustion_score",
        0
    ) > 60:
        score -= 15

    if current_roi < 0:
        score -= min(
            abs(current_roi) * 2,
            25
        )

    return max(
        0,
        min(
            100,
            score
        )
    )


# ============================================================
# TRAILING STOP
# ============================================================

def dinamik_trailing_hesapla(
    entry_price,
    mark_price,
    side,
    p_type,
    roi,
    highest_roi,
    atr,
    target_roi
):

    if not TRAILING_ENABLED:
        return None

    if p_type == "scalp":

        start_ratio = (
            SCALP_TRAILING_START_RATIO
        )

        lock_ratio = (
            SCALP_TRAILING_LOCK_RATIO
        )

    else:

        start_ratio = (
            OPPORTUNITY_TRAILING_START_RATIO
        )

        lock_ratio = (
            OPPORTUNITY_TRAILING_LOCK_RATIO
        )

    start_roi = (
        target_roi *
        start_ratio
    )

    if highest_roi < start_roi:
        return None

    profit_range = max(
        highest_roi -
        start_roi,
        0
    )

    locked_roi = (
        start_roi +
        profit_range *
        lock_ratio
    )

    # ATR mesafesi
    if p_type == "scalp":
        atr_mult = 0.60
    else:
        atr_mult = 0.90

    atr_distance = (
        atr *
        atr_mult
    )

    if side == "long":

        atr_trail = (
            mark_price -
            atr_distance
        )

        roi_trail = roi_to_price(
            entry_price,
            locked_roi,
            "buy",
            LEVERAGE
        )

        trailing_price = max(
            atr_trail,
            roi_trail
        )

    else:

        atr_trail = (
            mark_price +
            atr_distance
        )

        roi_trail = roi_to_price(
            entry_price,
            locked_roi,
            "sell",
            LEVERAGE
        )

        trailing_price = min(
            atr_trail,
            roi_trail
        )

    return float(
        trailing_price
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
    analiz_detay="",
    eval_res=None
):

    if not TRADING_ENABLED:
        return False

    with islem_acma_lock:

        try:

            if cooldown_aktif_mi(symbol):
                return False

            # ------------------------------------------------
            # POSITION LIMIT
            # ------------------------------------------------

            current_positions = 0

            try:
                existing = exchange.fetch_positions()

                current_positions = len([
                    p for p in existing
                    if float(
                        p.get("contracts") or 0
                    ) > 0
                ])

            except Exception:
                pass

            if current_positions >= MAX_TOTAL_POSITIONS:
                logging.info(
                    f"[POZİSYON LİMİT] "
                    f"{symbol} | Aktif={current_positions}"
                )
                return False

            # ------------------------------------------------
            # MARGIN / LEVERAGE
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

            side = (
                "buy"
                if direction == "buy"
                else "sell"
            )

            # ------------------------------------------------
            # CURRENT ATR
            # ------------------------------------------------

            df_temp = ohlcv_getir(
                exchange,
                symbol,
                (
                    SCALP_TRIGGER_TIMEFRAME
                    if p_type == "scalp"
                    else OPPORTUNITY_TRIGGER_TIMEFRAME
                ),
                80
            )

            if df_temp is not None:

                atr = float(
                    df_temp.iloc[-2]["atr"]
                )

            else:

                atr = (
                    price *
                    0.01
                )

            if eval_res is not None:

                momentum_data = eval_res[
                    "momentum"
                ]

                final_score = eval_res[
                    "final_score"
                ]

            else:

                momentum_data = calculate_momentum_engine(
                    df_temp,
                    direction
                )

                final_score = score

            # ------------------------------------------------
            # DYNAMIC TRADE PLAN
            # ------------------------------------------------

            trade_plan = hesapla_dinamik_trade_plan(
                price,
                side,
                atr,
                p_type,
                LEVERAGE,
                target_margin,
                momentum_data,
                final_score
            )

            # ------------------------------------------------
            # MARKET ORDER
            # ------------------------------------------------

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
            # RUNTIME STATE
            # ------------------------------------------------

            pozisyon_tipleri[
                symbol
            ] = p_type

            pozisyon_yonleri[
                symbol
            ] = direction

            pozisyon_giris_fiyatlari[
                symbol
            ] = price

            pozisyon_en_yuksek_kar[
                symbol
            ] = 0.0

            pozisyon_en_yuksek_roi[
                symbol
            ] = 0.0

            pozisyon_acilis_zamanlari[
                symbol
            ] = time.time()

            pozisyon_trade_plan[
                symbol
            ] = trade_plan

            pozisyon_saglik_loglari[
                symbol
            ] = 100

            pozisyon_son_momentum[
                symbol
            ] = momentum_data

            pozisyon_son_analiz_zamani[
                symbol
            ] = time.time()

            cooldown_baslat(
                symbol
            )

            # ------------------------------------------------
            # EXCHANGE STOP / TP
            # ------------------------------------------------

            close_side = (
                "sell"
                if side == "buy"
                else "buy"
            )

            try:

                exchange.create_order(
                    symbol,
                    "stop_market",
                    close_side,
                    amount,
                    None,
                    {
                        "stopPrice": exchange.price_to_precision(
                            symbol,
                            trade_plan["sl_price"]
                        ),
                        "reduceOnly": True,
                        "workingType": "MARK_PRICE"
                    }
                )

            except Exception as e:

                logging.error(
                    f"[SL EMİR HATA] "
                    f"{symbol}: {e}"
                )

            try:

                exchange.create_order(
                    symbol,
                    "take_profit_market",
                    close_side,
                    amount,
                    None,
                    {
                        "stopPrice": exchange.price_to_precision(
                            symbol,
                            trade_plan["tp_price"]
                        ),
                        "reduceOnly": True,
                        "workingType": "MARK_PRICE"
                    }
                )

            except Exception as e:

                logging.error(
                    f"[TP EMİR HATA] "
                    f"{symbol}: {e}"
                )

            # ------------------------------------------------
            # DETAILED LOG
            # ------------------------------------------------

            logging.info(
                f"[BAŞARILI İŞLEM] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{'LONG' if side == 'buy' else 'SHORT'} | "
                f"Entry={price:.8f} | "
                f"Score={final_score:.2f} | "
                f"Momentum={momentum_data['momentum_score']:.1f} | "
                f"Trend={momentum_data['trend_score']:.1f} | "
                f"Volume={momentum_data['volume_ratio']:.2f} | "
                f"Target=${trade_plan['target_usdt']:.4f} | "
                f"TargetROI={trade_plan['target_roi']:.2f}% | "
                f"MaxLoss=${trade_plan['max_loss_usdt']:.4f} | "
                f"MaxLossROI={trade_plan['max_loss_roi']:.2f}% | "
                f"TP={trade_plan['tp_price']:.8f} | "
                f"SL={trade_plan['sl_price']:.8f} | "
                f"Plan={trade_plan['hold_minutes']}dk | "
                f"Trailing={'ON' if TRAILING_ENABLED else 'OFF'}"
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
            if side in ["buy", "long"]
            else "buy"
        )

        try:
            exchange.cancel_all_orders(
                symbol
            )
        except Exception:
            pass

        time.sleep(0.15)

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
            f"Neden={reason}"
        )

        return True

    except Exception as e:

        logging.error(
            f"Pozisyon kapatma hata "
            f"{symbol}: {e}"
        )

        return False


# ============================================================
# POSITION MANAGEMENT
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

            roi = float(
                p.get("percentage") or 0
            )

            unrealized_pnl = float(
                p.get("unrealizedPnl") or 0
            )

            p_type = pozisyon_tipini_cozumle(
                p
            )

            plan = pozisyon_trade_plan.get(
                symbol
            )

            # =================================================
            # PLAN YOKSA OLUŞTUR
            # =================================================

            if plan is None:

                df = ohlcv_getir(
                    exchange,
                    symbol,
                    (
                        SCALP_TRIGGER_TIMEFRAME
                        if p_type == "scalp"
                        else OPPORTUNITY_TRIGGER_TIMEFRAME
                    ),
                    80
                )

                if df is not None:

                    atr = float(
                        df.iloc[-2]["atr"]
                    )

                    direction = (
                        "buy"
                        if side == "long"
                        else "sell"
                    )

                    momentum_data = calculate_momentum_engine(
                        df,
                        direction
                    )

                    margin = (
                        SCALP_MARGIN
                        if p_type == "scalp"
                        else OPPORTUNITY_MARGIN
                    )

                    plan = hesapla_dinamik_trade_plan(
                        entry_price,
                        direction,
                        atr,
                        p_type,
                        LEVERAGE,
                        margin,
                        momentum_data,
                        80
                    )

                    pozisyon_trade_plan[
                        symbol
                    ] = plan

            if plan is None:
                continue

            # =================================================
            # HIGH WATER MARK
            # =================================================

            if roi > pozisyon_en_yuksek_roi.get(
                symbol,
                0
            ):

                pozisyon_en_yuksek_roi[
                    symbol
                ] = roi

                pozisyon_en_yuksek_kar[
                    symbol
                ] = unrealized_pnl

            highest_roi = (
                pozisyon_en_yuksek_roi.get(
                    symbol,
                    roi
                )
            )

            # =================================================
            # MOMENTUM CONTINUATION
            # =================================================

            continuation = momentum_continuation_analizi(
                exchange,
                symbol,
                side,
                p_type
            )

            momentum_score = continuation[
                "score"
            ]

            pozisyon_son_momentum[
                symbol
            ] = continuation

            pozisyon_son_analiz_zamani[
                symbol
            ] = time.time()

            # =================================================
            # TRADE HEALTH
            # =================================================

            df_health = ohlcv_getir(
                exchange,
                symbol,
                (
                    SCALP_TRIGGER_TIMEFRAME
                    if p_type == "scalp"
                    else OPPORTUNITY_TRIGGER_TIMEFRAME
                ),
                60
            )

            if df_health is not None:

                direction = (
                    "buy"
                    if side == "long"
                    else "sell"
                )

                health_mom = calculate_momentum_engine(
                    df_health,
                    direction
                )

                health_score = trade_health_analizi(
                    df_health,
                    side,
                    roi,
                    health_mom
                )

                pozisyon_saglik_loglari[
                    symbol
                ] = health_score

            else:

                health_score = 50

            # =================================================
            # HARD MAX LOSS
            # =================================================

            max_loss_roi = -float(
                plan["max_loss_roi"]
            )

            if roi <= max_loss_roi:

                pozisyon_kapat(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    "TARGET_BASED_MAX_LOSS"
                )

                continue

            # =================================================
            # TIME STOP
            # =================================================

            open_time = pozisyon_acilis_zamanlari.get(
                symbol,
                time.time()
            )

            elapsed_minutes = (
                time.time() -
                open_time
            ) / 60

            planned_minutes = float(
                plan["hold_minutes"]
            )

            if elapsed_minutes >= planned_minutes:

                # Kârda ise momentum durumuna bak.
                if roi > 0:

                    if momentum_score < 55:

                        pozisyon_kapat(
                            exchange,
                            symbol,
                            side,
                            contracts,
                            "DYNAMIC_TIME_STOP_MOMENTUM_WEAK"
                        )

                        continue

                else:

                    # Zarardaki pozisyon süreyi doldurduysa
                    # momentum güçlü değilse artık taşımıyoruz.
                    if momentum_score < 60:

                        pozisyon_kapat(
                            exchange,
                            symbol,
                            side,
                            contracts,
                            "DYNAMIC_TIME_STOP"
                        )

                        continue

            # =================================================
            # MOMENTUM REVERSAL
            # =================================================

            if MOMENTUM_CONTINUATION_ENABLED:

                if momentum_score < MOMENTUM_HEALTH_EXIT_SCORE:

                    pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "MOMENTUM_REVERSAL"
                    )

                    continue

                # Zararda ve momentum ciddi biçimde
                # zayıflıyorsa stopu bekleme.
                if (
                    roi < 0 and
                    momentum_score < MOMENTUM_WEAK_SCORE
                ):

                    pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "LOSS_MOMENTUM_WEAK"
                    )

                    continue

            # =================================================
            # TRAILING
            # =================================================

            if TRAILING_ENABLED:

                atr = (
                    float(
                        df_health.iloc[-2]["atr"]
                    )
                    if df_health is not None
                    else entry_price * 0.01
                )

                trailing_price = dinamik_trailing_hesapla(
                    entry_price,
                    mark_price,
                    side,
                    p_type,
                    roi,
                    highest_roi,
                    atr,
                    float(
                        plan["target_roi"]
                    )
                )

                if trailing_price is not None:

                    plan[
                        "trailing_started"
                    ] = True

                    plan[
                        "trailing_price"
                    ] = trailing_price

                    plan[
                        "highest_roi"
                    ] = highest_roi

                    # LONG
                    if side == "long":

                        if mark_price <= trailing_price:

                            pozisyon_kapat(
                                exchange,
                                symbol,
                                side,
                                contracts,
                                "DYNAMIC_TRAILING"
                            )

                            continue

                    # SHORT
                    else:

                        if mark_price >= trailing_price:

                            pozisyon_kapat(
                                exchange,
                                symbol,
                                side,
                                contracts,
                                "DYNAMIC_TRAILING"
                            )

                            continue

            # =================================================
            # EARLY PROFIT PROTECTION
            # =================================================

            if p_type == "scalp" and \
               SCALP_EARLY_PROFIT_PROTECTION_ENABLED:

                if roi >= SCALP_EARLY_PROFIT_MIN_ROI:

                    if momentum_score < 55:

                        pozisyon_kapat(
                            exchange,
                            symbol,
                            side,
                            contracts,
                            "SCALP_PROFIT_MOMENTUM_FADE"
                        )

                        continue

            # =================================================
            # LOG
            # =================================================

            logging.info(
                f"[POZİSYON] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{side.upper()} | "
                f"ROI %{roi:.2f} | "
                f"MAX %{highest_roi:.2f} | "
                f"Momentum={momentum_score:.1f} | "
                f"Health={health_score:.1f}"
            )

        except Exception as e:

            logging.error(
                f"Pozisyon yönetimi hata "
                f"{symbol}: {e}"
            )

    onceki_aktif_pozisyonlar = (
        aktif_semboller.copy()
    )


# ============================================================
# SCAN MARKET
# ============================================================

def market_ticker_pool(exchange):

    try:

        tickers = exchange.fetch_tickers()

        usdt_tickers = [
            t
            for t in tickers.values()
            if gecerli_kripto_mu(
                t.get("symbol")
            )
            and
            t.get("percentage") is not None
        ]

        gainers = sorted(
            usdt_tickers,
            key=lambda x: float(
                x["percentage"]
            ),
            reverse=True
        )[:GAINER_COUNT]

        losers = sorted(
            usdt_tickers,
            key=lambda x: float(
                x["percentage"]
            )
        )[:LOSER_COUNT]

        return gainers, losers

    except Exception as e:

        logging.error(
            f"Ticker pool hata: {e}"
        )

        return [], []


# ============================================================
# SCALP MARKET
# ============================================================

def scan_scalp_market(exchange):

    try:

        gainers, losers = market_ticker_pool(
            exchange
        )

        candidates = []

        # -----------------------------
        # LONG
        # -----------------------------

        for ticker in gainers[:10]:

            symbol = ticker["symbol"]

            if cooldown_aktif_mi(symbol):
                continue

            df = ohlcv_getir(
                exchange,
                symbol,
                SCALP_SCAN_TIMEFRAME,
                100
            )

            if df is None:
                continue

            candidates.append({
                "symbol": symbol,
                "direction": "buy",
                "df": df
            })

        # -----------------------------
        # SHORT
        # -----------------------------

        for ticker in losers[:10]:

            symbol = ticker["symbol"]

            if cooldown_aktif_mi(symbol):
                continue

            df = ohlcv_getir(
                exchange,
                symbol,
                SCALP_SCAN_TIMEFRAME,
                100
            )

            if df is None:
                continue

            candidates.append({
                "symbol": symbol,
                "direction": "sell",
                "df": df
            })

        return candidates

    except Exception as e:

        logging.error(
            f"Scalp tarama hatası: {e}"
        )

        return []


# ============================================================
# OPPORTUNITY MARKET
# ============================================================

def scan_opportunity_market(exchange):

    try:

        gainers, losers = market_ticker_pool(
            exchange
        )

        candidates = []

        # LONG
        for ticker in gainers[:10]:

            symbol = ticker["symbol"]

            if cooldown_aktif_mi(symbol):
                continue

            df = ohlcv_getir(
                exchange,
                symbol,
                OPPORTUNITY_SCAN_TIMEFRAME,
                100
            )

            if df is None:
                continue

            candidates.append({
                "symbol": symbol,
                "direction": "buy",
                "df": df
            })

        # SHORT
        for ticker in losers[:10]:

            symbol = ticker["symbol"]

            if cooldown_aktif_mi(symbol):
                continue

            df = ohlcv_getir(
                exchange,
                symbol,
                OPPORTUNITY_SCAN_TIMEFRAME,
                100
            )

            if df is None:
                continue

            candidates.append({
                "symbol": symbol,
                "direction": "sell",
                "df": df
            })

        return candidates

    except Exception as e:

        logging.error(
            f"Opportunity tarama hatası: {e}"
        )

        return []


# ============================================================
# MONITOR LOOP
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
                            p.get(
                                "contracts"
                            ) or 0
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
                f"Monitor hata: {e}"
            )

            exchange = None

        # Binance rate-limit koruması
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
# MAIN ANALYSIS LOOP
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
                ">>> HİBRİT ANALİZ TARAMASI BAŞLADI <<<"
            )

            # =================================================
            # OPPORTUNITY
            # =================================================

            firsat_listesi = (
                scan_opportunity_market(
                    exchange
                )
            )

            firsat_listesi = sorted(
                firsat_listesi,
                key=lambda x: 0,
                reverse=True
            )

            for candidate in firsat_listesi:

                eval_res = evaluate_entry(
                    exchange,
                    candidate["symbol"],
                    candidate["direction"],
                    "opportunity",
                    candidate["df"]
                )

                if eval_res["approved"]:

                    pozisyon_ac(
                        exchange,
                        candidate["symbol"],
                        candidate["direction"],
                        eval_res["final_score"],
                        "opportunity",
                        eval_res=eval_res
                    )

                    break

            # =================================================
            # SCALP
            # =================================================

            scalp_listesi = (
                scan_scalp_market(
                    exchange
                )
            )

            for candidate in scalp_listesi:

                eval_res = evaluate_entry(
                    exchange,
                    candidate["symbol"],
                    candidate["direction"],
                    "scalp",
                    candidate["df"]
                )

                if eval_res["approved"]:

                    pozisyon_ac(
                        exchange,
                        candidate["symbol"],
                        candidate["direction"],
                        eval_res["final_score"],
                        "scalp",
                        eval_res=eval_res
                    )

                    break

        except Exception as e:

            logging.error(
                f"Ana döngü hatası: {e}"
            )

        finally:

            gc.collect()

        # Cron yerine mevcut yapıyı koruyoruz:
        # analiz 5 dakikada bir.
        # Pozisyon takibi ayrı thread'de 15 saniye.

        time.sleep(300)


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def index():

    return jsonify({
        "status":
            "Bot aktif - Dynamic Trade Plan + Momentum Engine",
        "trading_enabled":
            TRADING_ENABLED,
        "positions":
            list(
                pozisyon_tipleri.keys()
            )
    })


@app.route("/durum")
def durum():

    return jsonify({
        "success": True,
        "aktif_islem_sayisi":
            len(
                pozisyon_tipleri
            ),
        "saglik_durumlari":
            pozisyon_saglik_loglari,
        "trade_planlari":
            pozisyon_trade_plan,
        "momentum_durumlari":
            pozisyon_son_momentum
    })


@app.route("/otomatik-analiz")
def otomatik_analiz():

    return jsonify({
        "success": True,
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