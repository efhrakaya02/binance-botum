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
# RAILWAY & BINANCE HYBRID BOT V2
# SCALP + OPPORTUNITY
#
# MİMARİ KORUNDU:
# - Ana analiz döngüsü
# - Ayrı position monitor thread
# - Binance Futures
# - ISOLATED
# - 5X
# - Scalp = 10 USDT
# - Opportunity = 15 USDT
# - Max 1 Scalp + 1 Opportunity
# - Max toplam 2 pozisyon
#
# V2 ANALİZ:
# 30m / 15m / 5m MTF
# Momentum
# Acceleration
# Breakout quality
# Pullback / Re-acceleration
# Expected Move
# Support / Resistance distance
# Exhaustion
# Dynamic Signal Quality
# Smart Scalp Exit
# Opportunity reversal protection
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

# Maksimum kabul edilebilir zarar:
SCALP_MAX_ATR_STOP = 2.2

# Minimum signal quality
SCALP_FINAL_SCORE_MIN = 74

# Scalp hedefe ulaşabilirlik filtresi
SCALP_MIN_EXPECTED_MOVE_RATIO = 0.85

# Entry için maksimum breakout uzaması
SCALP_MAX_BREAKOUT_ATR = 0.75

# Erken kâr koruma
SCALP_EARLY_PROFIT_PROTECTION_ENABLED = True
SCALP_EARLY_PROFIT_USDT = 0.20

# Smart scalp exit
SCALP_SMART_EXIT_ENABLED = True
SCALP_MOMENTUM_EXIT_MIN_USDT = 0.15

# ============================================================
# OPPORTUNITY
# ============================================================

OPPORTUNITY_FINAL_SCORE_MIN = 72

OPPORTUNITY_INITIAL_ATR_STOP = 3.0

OPPORTUNITY_TRAILING_START_ROI = 5.0
OPPORTUNITY_STRONG_TRAILING_ROI = 15.0

OPPORTUNITY_REVERSAL_EXIT_ENABLED = True
OPPORTUNITY_REVERSAL_MIN_ROI = 2.0

# ============================================================
# MOMENTUM ENGINE
# ============================================================

MOMENTUM_ENGINE_ENABLED = True

MOMENTUM_MIN_SCORE = 60
ACCELERATION_MIN_SCORE = 65
ENTRY_MIN_SCORE = 70

EXHAUSTION_MAX_ENTRY = 55

# ============================================================
# MARKET STRUCTURE
# ============================================================

SR_LOOKBACK = 30

BREAKOUT_VOLUME_MIN = 1.30
STRONG_BREAKOUT_VOLUME = 1.80

ATR_EXPANSION_MIN = 1.05

# ============================================================
# MONITOR
# ============================================================

POSITION_MONITOR_INTERVAL = 3.0

# Candle bazlı analizlerin gereksiz API çağrısı yapmasını önler.
SCALP_ANALYSIS_REFRESH_SECONDS = 10
OPPORTUNITY_ANALYSIS_REFRESH_SECONDS = 20

# ============================================================
# RUNTIME STATE
# ============================================================

pozisyon_en_yuksek_kar = {}
pozisyon_tipleri = {}
pozisyon_yonleri = {}
pozisyon_giris_fiyatlari = {}

pozisyon_son_analiz = {}
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
# POZİSYON TÜRÜ
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
# TEKNİK VERİ
# ============================================================

def ohlcv_getir(exchange, symbol, timeframe, limit=150):

    try:

        data = exchange.fetch_ohlcv(
            symbol,
            timeframe=timeframe,
            limit=limit
        )

        if not data or len(data) < 40:
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

        df["ema200"] = df["close"].ewm(
            span=200,
            adjust=False
        ).mean() if len(df) >= 200 else df["ema50"]

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

        df["rsi"] = (
            100 -
            (100 / (1 + rs))
        )

        # ----------------------------------------------------
        # ATR
        # ----------------------------------------------------

        high_low = (
            df["high"] -
            df["low"]
        )

        high_close = (
            df["high"] -
            df["close"].shift()
        ).abs()

        low_close = (
            df["low"] -
            df["close"].shift()
        ).abs()

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
        # ATR REGIME
        # ----------------------------------------------------

        df["atr_mean20"] = df["atr"].rolling(
            20
        ).mean()

        df["atr_ratio"] = (
            df["atr"] /
            df["atr_mean20"].replace(
                0,
                np.nan
            )
        )

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

        plus_di = (
            100 *
            plus_dm.ewm(
                alpha=1 / 14,
                adjust=False
            ).mean()
            /
            df["atr"].replace(
                0,
                np.nan
            )
        )

        minus_di = (
            100 *
            minus_dm.ewm(
                alpha=1 / 14,
                adjust=False
            ).mean()
            /
            df["atr"].replace(
                0,
                np.nan
            )
        )

        df["plus_di"] = plus_di
        df["minus_di"] = minus_di

        dx = (
            100 *
            abs(plus_di - minus_di)
            /
            (plus_di + minus_di).replace(
                0,
                np.nan
            )
        )

        df["adx"] = dx.ewm(
            alpha=1 / 14,
            adjust=False
        ).mean()

        df["adx"] = df["adx"].fillna(20)

        # ----------------------------------------------------
        # ROC
        # ----------------------------------------------------

        df["roc1"] = (
            df["close"].pct_change(1) * 100
        )

        df["roc3"] = (
            df["close"].pct_change(3) * 100
        )

        df["roc5"] = (
            df["close"].pct_change(5) * 100
        )

        df["roc9"] = (
            df["close"].pct_change(9) * 100
        )

        # ----------------------------------------------------
        # Bollinger
        # ----------------------------------------------------

        sma20 = df["close"].rolling(20).mean()
        std20 = df["close"].rolling(20).std()

        df["bb_mid"] = sma20

        df["bb_upper"] = (
            sma20 +
            (2 * std20)
        )

        df["bb_lower"] = (
            sma20 -
            (2 * std20)
        )

        df["bb_width"] = (
            (df["bb_upper"] - df["bb_lower"])
            /
            sma20.replace(
                0,
                np.nan
            )
        )

        # ----------------------------------------------------
        # Volume
        # ----------------------------------------------------

        df["volume_ma10"] = df["volume"].rolling(
            10
        ).mean()

        df["volume_ma20"] = df["volume"].rolling(
            20
        ).mean()

        df["volume_ratio"] = (
            df["volume"] /
            df["volume_ma20"].replace(
                0,
                np.nan
            )
        )

        df["volume_ema3"] = df["volume"].ewm(
            span=3,
            adjust=False
        ).mean()

        df["volume_ema10"] = df["volume"].ewm(
            span=10,
            adjust=False
        ).mean()

        df["volume_acceleration"] = (
            df["volume_ema3"] /
            df["volume_ema10"].replace(
                0,
                np.nan
            )
        )

        # ----------------------------------------------------
        # OBV
        # ----------------------------------------------------

        df["obv"] = (
            np.sign(
                df["close"].diff()
            ) *
            df["volume"]
        ).fillna(0).cumsum()

        df["obv_ema"] = df["obv"].ewm(
            span=10,
            adjust=False
        ).mean()

        # ----------------------------------------------------
        # Candle structure
        # ----------------------------------------------------

        df["body"] = (
            df["close"] -
            df["open"]
        )

        df["range"] = (
            df["high"] -
            df["low"]
        )

        df["body_ratio"] = (
            abs(df["body"]) /
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
        # Price velocity / acceleration
        # ----------------------------------------------------

        df["price_velocity"] = (
            df["roc3"] -
            df["roc3"].shift(1)
        )

        df["price_acceleration"] = (
            df["price_velocity"] -
            df["price_velocity"].shift(1)
        )

        # ----------------------------------------------------
        # EMA spread
        # ----------------------------------------------------

        df["ema_spread"] = (
            (df["ema9"] - df["ema21"]) /
            df["ema21"].replace(
                0,
                np.nan
            ) *
            100
        )

        df["ema_spread_slope"] = (
            df["ema_spread"] -
            df["ema_spread"].shift(3)
        )

        # ----------------------------------------------------
        # MACD histogram acceleration
        # ----------------------------------------------------

        df["macd_hist_slope"] = (
            df["macd_hist"] -
            df["macd_hist"].shift(3)
        )

        df["macd_hist_acceleration"] = (
            df["macd_hist_slope"] -
            df["macd_hist_slope"].shift(2)
        )

        # ----------------------------------------------------
        # ADX slope
        # ----------------------------------------------------

        df["adx_slope"] = (
            df["adx"] -
            df["adx"].shift(3)
        )

        return df

    except Exception as e:

        logging.error(
            f"OHLCV hata {symbol} {timeframe}: {e}"
        )

        return None


# ============================================================
# TREND SCORE
# ============================================================

def trend_score(df, direction):

    if df is None or len(df) < 30:
        return 0

    last = df.iloc[-1]

    score = 0

    if direction == "buy":

        if last["close"] > last["ema21"]:
            score += 15

        if last["ema9"] > last["ema21"]:
            score += 15

        if last["ema21"] > last["ema50"]:
            score += 20

        if last["ema50"] > last["ema200"]:
            score += 15

        if last["macd_hist"] > 0:
            score += 10

        if last["plus_di"] > last["minus_di"]:
            score += 10

        if last["adx"] >= 20:
            score += 15

    else:

        if last["close"] < last["ema21"]:
            score += 15

        if last["ema9"] < last["ema21"]:
            score += 15

        if last["ema21"] < last["ema50"]:
            score += 20

        if last["ema50"] < last["ema200"]:
            score += 15

        if last["macd_hist"] < 0:
            score += 10

        if last["minus_di"] > last["plus_di"]:
            score += 10

        if last["adx"] >= 20:
            score += 15

    return min(score, 100)


# ============================================================
# MOMENTUM SCORE
# ============================================================

def momentum_score(df, direction):

    if df is None or len(df) < 20:
        return 0

    last = df.iloc[-1]

    score = 0

    if direction == "buy":

        if last["ema9"] > last["ema21"]:
            score += 20

        if last["macd_hist"] > 0:
            score += 15

        if last["roc3"] > 0:
            score += 15

        if 50 <= last["rsi"] <= 70:
            score += 15

        if last["plus_di"] > last["minus_di"]:
            score += 15

        if last["obv"] > last["obv_ema"]:
            score += 10

        if last["volume_ratio"] >= 1.0:
            score += 10

    else:

        if last["ema9"] < last["ema21"]:
            score += 20

        if last["macd_hist"] < 0:
            score += 15

        if last["roc3"] < 0:
            score += 15

        if 30 <= last["rsi"] <= 50:
            score += 15

        if last["minus_di"] > last["plus_di"]:
            score += 15

        if last["obv"] < last["obv_ema"]:
            score += 10

        if last["volume_ratio"] >= 1.0:
            score += 10

    return min(score, 100)


# ============================================================
# ACCELERATION SCORE
# ============================================================

def acceleration_score(df, direction):

    if df is None or len(df) < 15:
        return 0

    last = df.iloc[-1]

    score = 0

    # MACD acceleration
    if direction == "buy":

        if last["macd_hist_slope"] > 0:
            score += 20

        if last["macd_hist_acceleration"] > 0:
            score += 10

        if last["ema_spread_slope"] > 0:
            score += 20

        if last["price_velocity"] > 0:
            score += 15

        if last["price_acceleration"] > 0:
            score += 10

    else:

        if last["macd_hist_slope"] < 0:
            score += 20

        if last["macd_hist_acceleration"] < 0:
            score += 10

        if last["ema_spread_slope"] < 0:
            score += 20

        if last["price_velocity"] < 0:
            score += 15

        if last["price_acceleration"] < 0:
            score += 10

    # Volume acceleration
    if last["volume_acceleration"] >= 1.05:
        score += 15

    # ATR expansion
    if last["atr_ratio"] >= ATR_EXPANSION_MIN:
        score += 10

    return min(score, 100)


# ============================================================
# EXHAUSTION
# ============================================================

def exhaustion_score(df, direction):

    if df is None or len(df) < 20:
        return 100

    last = df.iloc[-1]

    score = 0

    if direction == "buy":

        if last["rsi"] > 72:
            score += 25

        if last["rsi"] > 78:
            score += 15

        if last["ema_spread"] > 2.0:
            score += 15

        if last["roc5"] > 4:
            score += 15

        if last["adx"] > 40 and last["adx_slope"] < 0:
            score += 15

        if last["macd_hist_slope"] < 0:
            score += 15

    else:

        if last["rsi"] < 28:
            score += 25

        if last["rsi"] < 22:
            score += 15

        if last["ema_spread"] < -2.0:
            score += 15

        if last["roc5"] < -4:
            score += 15

        if last["adx"] > 40 and last["adx_slope"] < 0:
            score += 15

        if last["macd_hist_slope"] > 0:
            score += 15

    return min(score, 100)


# ============================================================
# BREAKOUT / STRUCTURE
# ============================================================

def structure_analysis(df, direction):

    if df is None or len(df) < SR_LOOKBACK + 2:
        return {
            "breakout": False,
            "breakout_distance_atr": 0,
            "volume_ratio": 1,
            "structure_score": 0,
            "resistance_distance_pct": 999,
            "support_distance_pct": 999
        }

    last = df.iloc[-1]

    recent_high = df["high"].iloc[
        -SR_LOOKBACK:-1
    ].max()

    recent_low = df["low"].iloc[
        -SR_LOOKBACK:-1
    ].min()

    atr = float(last["atr"])

    if atr <= 0:
        atr = float(last["close"]) * 0.01

    close = float(last["close"])

    if direction == "buy":

        breakout_distance = (
            (close - recent_high) /
            atr
        )

        resistance_distance_pct = (
            (recent_high - close) /
            close *
            100
        )

        support_distance_pct = (
            (close - recent_low) /
            close *
            100
        )

        breakout = (
            close > recent_high and
            last["volume_ratio"] >= BREAKOUT_VOLUME_MIN
        )

        structure_score = 0

        if close >= recent_high * 0.997:
            structure_score += 25

        if breakout:
            structure_score += 30

        if last["volume_ratio"] >= BREAKOUT_VOLUME_MIN:
            structure_score += 20

        if last["body_ratio"] >= 0.55:
            structure_score += 15

        if last["close"] > last["open"]:
            structure_score += 10

    else:

        breakout_distance = (
            (recent_low - close) /
            atr
        )

        resistance_distance_pct = (
            (recent_high - close) /
            close *
            100
        )

        support_distance_pct = (
            (close - recent_low) /
            close *
            100
        )

        breakout = (
            close < recent_low and
            last["volume_ratio"] >= BREAKOUT_VOLUME_MIN
        )

        structure_score = 0

        if close <= recent_low * 1.003:
            structure_score += 25

        if breakout:
            structure_score += 30

        if last["volume_ratio"] >= BREAKOUT_VOLUME_MIN:
            structure_score += 20

        if last["body_ratio"] >= 0.55:
            structure_score += 15

        if last["close"] < last["open"]:
            structure_score += 10

    return {
        "breakout": breakout,
        "breakout_distance_atr": round(
            max(breakout_distance, 0),
            3
        ),
        "volume_ratio": round(
            float(last["volume_ratio"]),
            2
        ),
        "structure_score": min(
            structure_score,
            100
        ),
        "resistance_distance_pct": round(
            resistance_distance_pct,
            3
        ),
        "support_distance_pct": round(
            support_distance_pct,
            3
        )
    }


# ============================================================
# PULLBACK / RE-ACCELERATION
# ============================================================

def pullback_confirmation(df, direction):

    if df is None or len(df) < 10:
        return False, 0

    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]

    score = 0

    if direction == "buy":

        if last["close"] > last["ema21"]:
            score += 20

        if prev["low"] <= prev["ema21"] * 1.003:
            score += 25

        if last["close"] > prev["close"]:
            score += 20

        if last["macd_hist"] > prev["macd_hist"]:
            score += 15

        if last["volume_ratio"] > 1.0:
            score += 10

        if last["ema9"] >= prev["ema9"]:
            score += 10

    else:

        if last["close"] < last["ema21"]:
            score += 20

        if prev["high"] >= prev["ema21"] * 0.997:
            score += 25

        if last["close"] < prev["close"]:
            score += 20

        if last["macd_hist"] < prev["macd_hist"]:
            score += 15

        if last["volume_ratio"] > 1.0:
            score += 10

        if last["ema9"] <= prev["ema9"]:
            score += 10

    return score >= 60, min(score, 100)


# ============================================================
# REGRESYON
# ============================================================

def gelismis_regresyon_teyidi(
    df,
    direction,
    periyot=20
):

    if df is None or len(df) < periyot:
        return False, 0.0, 0.0, "Yetersiz veri."

    closes = df["close"].iloc[
        -periyot:
    ].values

    x = np.arange(periyot)

    slope, intercept = np.polyfit(
        x,
        closes,
        1
    )

    y_pred = (
        intercept +
        slope * x
    )

    correlation = np.corrcoef(
        closes,
        y_pred
    )[0, 1]

    r_squared = (
        correlation ** 2
        if not np.isnan(correlation)
        else 0.0
    )

    current_price = closes[-1]

    regression_mid = (
        intercept +
        slope * (periyot - 1)
    )

    min_r_squared = 0.55

    if direction == "buy":

        ok = (
            slope > 0 and
            r_squared >= min_r_squared and
            current_price >= regression_mid * 0.997
        )

    else:

        ok = (
            slope < 0 and
            r_squared >= min_r_squared and
            current_price <= regression_mid * 1.003
        )

    message = (
        f"Eğim={slope:.6f} | "
        f"R²={r_squared:.2f} | "
        f"Fiyat={current_price:.6f}"
    )

    return (
        ok,
        slope,
        r_squared,
        "ONAYLANDI -> " + message
        if ok
        else "REDDEDİLDİ -> " + message
    )


# ============================================================
# EXPECTED MOVE / TP FEASIBILITY
# ============================================================

def expected_move_analysis(
    df_5m,
    direction,
    target_usdt,
    amount
):

    if (
        df_5m is None or
        len(df_5m) < 20 or
        amount <= 0
    ):
        return {
            "target_distance_pct": 999,
            "atr_pct": 0,
            "expected_move_pct": 0,
            "target_vs_expected": 0,
            "feasible": False
        }

    last = df_5m.iloc[-1]

    close = float(last["close"])
    atr = float(last["atr"])

    if close <= 0:
        return {
            "target_distance_pct": 999,
            "atr_pct": 0,
            "expected_move_pct": 0,
            "target_vs_expected": 0,
            "feasible": False
        }

    target_price_distance = (
        target_usdt /
        amount
    )

    target_pct = (
        target_price_distance /
        close *
        100
    )

    atr_pct = (
        atr /
        close *
        100
    )

    # Kısa vadede makul beklenen hareket.
    # ATR'nin 1.5 katı baz alınır.
    expected_move_pct = atr_pct * 1.5

    ratio = (
        expected_move_pct /
        target_pct
        if target_pct > 0
        else 0
    )

    feasible = (
        ratio >= SCALP_MIN_EXPECTED_MOVE_RATIO
    )

    return {
        "target_distance_pct": round(
            target_pct,
            4
        ),
        "atr_pct": round(
            atr_pct,
            4
        ),
        "expected_move_pct": round(
            expected_move_pct,
            4
        ),
        "target_vs_expected": round(
            ratio,
            2
        ),
        "feasible": feasible
    }


# ============================================================
# ENTRY TIMING ENGINE
# ============================================================

def entry_timing_engine(
    df_5m,
    direction,
    target_usdt=None,
    amount=None
):

    if df_5m is None or len(df_5m) < 25:

        return {
            "entry_score": 0,
            "state": "WAIT",
            "momentum": 0,
            "acceleration": 0,
            "exhaustion": 100,
            "structure_score": 0,
            "pullback_score": 0,
            "expected_move": None
        }

    momentum = momentum_score(
        df_5m,
        direction
    )

    acceleration = acceleration_score(
        df_5m,
        direction
    )

    exhaustion = exhaustion_score(
        df_5m,
        direction
    )

    structure = structure_analysis(
        df_5m,
        direction
    )

    pullback_ok, pullback_score = (
        pullback_confirmation(
            df_5m,
            direction
        )
    )

    last = df_5m.iloc[-1]

    # --------------------------------------------------------
    # Entry timing
    # --------------------------------------------------------

    timing = 0

    if direction == "buy":

        if last["close"] > last["ema9"]:
            timing += 15

        if last["ema9"] > last["ema21"]:
            timing += 15

        if last["macd_hist_slope"] > 0:
            timing += 15

        if last["price_velocity"] > 0:
            timing += 10

        if last["volume_acceleration"] >= 1.05:
            timing += 15

        if pullback_ok:
            timing += 20

        if (
            structure["breakout_distance_atr"]
            <= SCALP_MAX_BREAKOUT_ATR
        ):
            timing += 10

    else:

        if last["close"] < last["ema9"]:
            timing += 15

        if last["ema9"] < last["ema21"]:
            timing += 15

        if last["macd_hist_slope"] < 0:
            timing += 15

        if last["price_velocity"] < 0:
            timing += 10

        if last["volume_acceleration"] >= 1.05:
            timing += 15

        if pullback_ok:
            timing += 20

        if (
            structure["breakout_distance_atr"]
            <= SCALP_MAX_BREAKOUT_ATR
        ):
            timing += 10

    # --------------------------------------------------------
    # Expected move
    # --------------------------------------------------------

    expected_move = None

    if (
        target_usdt is not None and
        amount is not None
    ):
        expected_move = expected_move_analysis(
            df_5m,
            direction,
            target_usdt,
            amount
        )

    # --------------------------------------------------------
    # Final entry score
    # --------------------------------------------------------

    entry_score = (
        momentum * 0.20 +
        acceleration * 0.25 +
        timing * 0.25 +
        structure["structure_score"] * 0.15 +
        pullback_score * 0.10 +
        (100 - exhaustion) * 0.05
    )

    # Expected move bonus / penalty
    if expected_move is not None:

        if expected_move["feasible"]:
            entry_score += 5
        else:
            entry_score -= 15

    entry_score = min(
        max(entry_score, 0),
        100
    )

    # --------------------------------------------------------
    # State
    # --------------------------------------------------------

    if exhaustion >= 70:
        state = "EXHAUSTING"

    elif (
        acceleration >= 75 and
        momentum >= 70
    ):
        state = "ACCELERATING"

    elif (
        momentum >= 70 and
        acceleration >= 55
    ):
        state = "STRONG"

    elif momentum >= 50:
        state = "BUILDING"

    else:
        state = "WEAK"

    return {
        "entry_score": round(
            entry_score,
            2
        ),
        "state": state,
        "momentum": round(
            momentum,
            2
        ),
        "acceleration": round(
            acceleration,
            2
        ),
        "exhaustion": round(
            exhaustion,
            2
        ),
        "structure_score": structure[
            "structure_score"
        ],
        "pullback_score": pullback_score,
        "breakout_distance_atr": structure[
            "breakout_distance_atr"
        ],
        "volume_ratio": structure[
            "volume_ratio"
        ],
        "expected_move": expected_move
    }


# ============================================================
# MULTI TIMEFRAME ENGINE
# ============================================================

def multi_timeframe_analysis(
    exchange,
    symbol,
    direction,
    mode
):

    if mode == "scalp":

        trend_tf = "30m"
        momentum_tf = "15m"

    else:

        trend_tf = "1h"
        momentum_tf = "15m"

    df_trend = ohlcv_getir(
        exchange,
        symbol,
        trend_tf,
        100
    )

    df_momentum = ohlcv_getir(
        exchange,
        symbol,
        momentum_tf,
        100
    )

    df_5m = ohlcv_getir(
        exchange,
        symbol,
        "5m",
        100
    )

    if (
        df_trend is None or
        df_momentum is None or
        df_5m is None
    ):
        return None

    trend = trend_score(
        df_trend,
        direction
    )

    momentum = momentum_score(
        df_momentum,
        direction
    )

    acceleration = acceleration_score(
        df_5m,
        direction
    )

    exhaustion = exhaustion_score(
        df_5m,
        direction
    )

    structure = structure_analysis(
        df_5m,
        direction
    )

    pullback_ok, pullback_score = (
        pullback_confirmation(
            df_5m,
            direction
        )
    )

    reg_ok, slope, r2, reg_message = (
        gelismis_regresyon_teyidi(
            df_momentum,
            direction,
            20
        )
    )

    return {
        "df_trend": df_trend,
        "df_momentum": df_momentum,
        "df_5m": df_5m,
        "trend_score": trend,
        "momentum_score": momentum,
        "acceleration_score": acceleration,
        "exhaustion_score": exhaustion,
        "structure": structure,
        "pullback_ok": pullback_ok,
        "pullback_score": pullback_score,
        "reg_ok": reg_ok,
        "reg_slope": slope,
        "reg_r2": r2,
        "reg_message": reg_message
    }


# ============================================================
# FINAL SIGNAL SCORE
# ============================================================

def final_signal_score(data, mode):

    if data is None:
        return 0

    trend = data["trend_score"]
    momentum = data["momentum_score"]
    acceleration = data["acceleration_score"]
    exhaustion = data["exhaustion_score"]

    structure = data["structure"]["structure_score"]
    pullback = data["pullback_score"]

    regression_bonus = (
        100
        if data["reg_ok"]
        else 0
    )

    score = (
        trend * 0.25 +
        momentum * 0.20 +
        acceleration * 0.20 +
        structure * 0.15 +
        pullback * 0.10 +
        regression_bonus * 0.10
    )

    # Exhaustion ağır ceza
    if exhaustion >= 70:
        score -= 20

    elif exhaustion >= 55:
        score -= 10

    # Çok uzak breakout kovalanmasın
    if (
        data["structure"]["breakout_distance_atr"]
        > 1.0
    ):
        score -= 10

    return round(
        min(max(score, 0), 100),
        2
    )


# ============================================================
# GAINER / LOSER POOL
# ============================================================

def get_candidate_pool(exchange):

    try:

        tickers = exchange.fetch_tickers()

        usdt_tickers = [
            t
            for t in tickers.values()
            if gecerli_kripto_mu(
                t.get("symbol", "")
            )
            and t.get("percentage") is not None
            and t.get("last") is not None
        ]

        gainers = sorted(
            usdt_tickers,
            key=lambda x: float(
                x["percentage"]
            ),
            reverse=True
        )[:25]

        losers = sorted(
            usdt_tickers,
            key=lambda x: float(
                x["percentage"]
            )
        )[:25]

        symbols = list(
            dict.fromkeys(
                [
                    t["symbol"]
                    for t in
                    gainers + losers
                ]
            )
        )

        return symbols

    except Exception as e:

        logging.error(
            f"Candidate pool hatası: {e}"
        )

        return []


# ============================================================
# SCALP MARKET SCAN
# ============================================================

def scan_scalp_market(exchange):

    candidates = []

    symbols = get_candidate_pool(
        exchange
    )

    logging.info(
        f"[SCALP] {len(symbols)} coin taranıyor..."
    )

    for symbol in symbols:

        try:

            data = multi_timeframe_analysis(
                exchange,
                symbol,
                "buy",
                "scalp"
            )

            buy_data = data

            sell_data = multi_timeframe_analysis(
                exchange,
                symbol,
                "sell",
                "scalp"
            )

            if buy_data is None and sell_data is None:
                continue

            # ------------------------------------------------
            # Long ve Short tarafını karşılaştır
            # ------------------------------------------------

            buy_score = (
                final_signal_score(
                    buy_data,
                    "scalp"
                )
                if buy_data
                else 0
            )

            sell_score = (
                final_signal_score(
                    sell_data,
                    "scalp"
                )
                if sell_data
                else 0
            )

            if buy_score >= sell_score:
                direction = "buy"
                data = buy_data
                base_score = buy_score
            else:
                direction = "sell"
                data = sell_data
                base_score = sell_score

            if data is None:
                continue

            # ------------------------------------------------
            # Scalp amount / expected move
            # ------------------------------------------------

            ticker = exchange.fetch_ticker(
                symbol
            )

            price = float(
                ticker["last"]
            )

            target_margin = SCALP_MARGIN

            notional = (
                target_margin *
                LEVERAGE
            )

            amount = (
                notional /
                price
            )

            timing = entry_timing_engine(
                data["df_5m"],
                direction,
                SCALP_TARGET_USDT,
                amount
            )

            # ------------------------------------------------
            # Zorunlu filtreler
            # ------------------------------------------------

            if base_score < SCALP_FINAL_SCORE_MIN:
                continue

            if timing["entry_score"] < ENTRY_MIN_SCORE:
                continue

            if timing["exhaustion"] > EXHAUSTION_MAX_ENTRY:
                continue

            if timing["state"] not in [
                "BUILDING",
                "ACCELERATING",
                "STRONG"
            ]:
                continue

            if not data["reg_ok"]:
                continue

            if (
                timing["expected_move"] is not None
                and
                not timing["expected_move"]["feasible"]
            ):
                continue

            candidates.append({

                "symbol": symbol,

                "score": base_score,

                "direction": direction,

                "mode": "scalp",

                "df": data["df_5m"],

                "trend_score": data[
                    "trend_score"
                ],

                "momentum_score": data[
                    "momentum_score"
                ],

                "acceleration_score": data[
                    "acceleration_score"
                ],

                "entry_score": timing[
                    "entry_score"
                ],

                "momentum_state": timing[
                    "state"
                ],

                "exhaustion_score": timing[
                    "exhaustion"
                ],

                "structure_score": timing[
                    "structure_score"
                ],

                "pullback_score": timing[
                    "pullback_score"
                ],

                "breakout_dist": timing[
                    "breakout_distance_atr"
                ],

                "volume_ratio": timing[
                    "volume_ratio"
                ],

                "expected_move": timing[
                    "expected_move"
                ],

                "reg_r2": data["reg_r2"]

            })

        except Exception as e:

            logging.error(
                f"[SCALP] {symbol} hata: {e}"
            )

    # En iyi entry kalitesi
    candidates.sort(
        key=lambda x: (
            x["entry_score"],
            x["acceleration_score"],
            x["score"]
        ),
        reverse=True
    )

    return candidates[:5]


# ============================================================
# OPPORTUNITY MARKET SCAN
# ============================================================

def scan_opportunity_market(exchange):

    candidates = []

    symbols = get_candidate_pool(
        exchange
    )

    logging.info(
        f"[FIRSAT] {len(symbols)} coin taranıyor..."
    )

    for symbol in symbols:

        try:

            buy_data = multi_timeframe_analysis(
                exchange,
                symbol,
                "buy",
                "opportunity"
            )

            sell_data = multi_timeframe_analysis(
                exchange,
                symbol,
                "sell",
                "opportunity"
            )

            buy_score = (
                final_signal_score(
                    buy_data,
                    "opportunity"
                )
                if buy_data
                else 0
            )

            sell_score = (
                final_signal_score(
                    sell_data,
                    "opportunity"
                )
                if sell_data
                else 0
            )

            if buy_score >= sell_score:
                direction = "buy"
                data = buy_data
                base_score = buy_score
            else:
                direction = "sell"
                data = sell_data
                base_score = sell_score

            if data is None:
                continue

            timing = entry_timing_engine(
                data["df_5m"],
                direction
            )

            if base_score < OPPORTUNITY_FINAL_SCORE_MIN:
                continue

            if timing["entry_score"] < ENTRY_MIN_SCORE:
                continue

            if timing["exhaustion"] > EXHAUSTION_MAX_ENTRY:
                continue

            if timing["state"] not in [
                "BUILDING",
                "ACCELERATING",
                "STRONG"
            ]:
                continue

            if not data["reg_ok"]:
                continue

            candidates.append({

                "symbol": symbol,

                "score": base_score,

                "direction": direction,

                "mode": "opportunity",

                "df": data["df_5m"],

                "trend_score": data[
                    "trend_score"
                ],

                "momentum_score": data[
                    "momentum_score"
                ],

                "acceleration_score": data[
                    "acceleration_score"
                ],

                "entry_score": timing[
                    "entry_score"
                ],

                "momentum_state": timing[
                    "state"
                ],

                "exhaustion_score": timing[
                    "exhaustion"
                ],

                "structure_score": timing[
                    "structure_score"
                ],

                "pullback_score": timing[
                    "pullback_score"
                ],

                "breakout_dist": timing[
                    "breakout_distance_atr"
                ],

                "volume_ratio": timing[
                    "volume_ratio"
                ],

                "reg_r2": data["reg_r2"]

            })

        except Exception as e:

            logging.error(
                f"[FIRSAT] {symbol} hata: {e}"
            )

    candidates.sort(
        key=lambda x: (
            x["entry_score"],
            x["acceleration_score"],
            x["score"]
        ),
        reverse=True
    )

    return candidates[:5]


# ============================================================
# POZİSYON AÇMA
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

            aktif_scalp = any(
                pozisyon_tipini_cozumle(p)
                == "scalp"
                for p in active_positions
            )

            aktif_firsat = any(
                pozisyon_tipini_cozumle(p)
                == "opportunity"
                for p in active_positions
            )

            if (
                p_type == "scalp"
                and
                aktif_scalp
            ):
                return False

            if (
                p_type == "opportunity"
                and
                aktif_firsat
            ):
                return False

            # ------------------------------------------------
            # Isolated + 5X
            # ------------------------------------------------

            try:

                exchange.set_margin_mode(
                    "isolated",
                    symbol
                )

            except Exception as e:

                logging.warning(
                    f"Margin mode uyarısı {symbol}: {e}"
                )

            try:

                exchange.set_leverage(
                    LEVERAGE,
                    symbol
                )

            except Exception as e:

                logging.warning(
                    f"Leverage uyarısı {symbol}: {e}"
                )

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
                or 0
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

            if real_margin > target_margin:

                amount = float(
                    exchange.amount_to_precision(
                        symbol,
                        raw_amount * 0.98
                    )
                )

                if amount <= 0:
                    return False

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
                f"Score={score} | "
                f"Margin={real_margin:.2f} | "
                f"Leverage={LEVERAGE}X"
            )

            logging.info(
                f"ANALİZ: {analiz_detay}"
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
            # Runtime state
            # ------------------------------------------------

            pozisyon_tipleri[symbol] = p_type

            pozisyon_yonleri[symbol] = direction

            pozisyon_giris_fiyatlari[symbol] = price

            pozisyon_en_yuksek_kar[symbol] = 0.0

            pozisyon_son_sl[symbol] = None

            pozisyon_son_analiz[symbol] = 0

            # ------------------------------------------------
            # İlk koruma emri
            # ------------------------------------------------

            time.sleep(0.8)

            try:

                df = ohlcv_getir(
                    exchange,
                    symbol,
                    "5m"
                    if p_type == "scalp"
                    else "15m",
                    50
                )

                if df is not None:

                    atr = float(
                        df.iloc[-1]["atr"]
                    )

                else:

                    atr = price * 0.01

                close_side = (
                    "sell"
                    if side == "buy"
                    else "buy"
                )

                if p_type == "scalp":

                    sl_price = (
                        price -
                        atr * SCALP_MAX_ATR_STOP
                        if side == "buy"
                        else
                        price +
                        atr * SCALP_MAX_ATR_STOP
                    )

                else:

                    sl_price = (
                        price -
                        atr * OPPORTUNITY_INITIAL_ATR_STOP
                        if side == "buy"
                        else
                        price +
                        atr * OPPORTUNITY_INITIAL_ATR_STOP
                    )

                sl_price = float(
                    exchange.price_to_precision(
                        symbol,
                        sl_price
                    )
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

                pozisyon_son_sl[symbol] = sl_price

            except Exception as e:

                logging.error(
                    f"İlk SL oluşturma hatası {symbol}: {e}"
                )

            logging.info(
                f"[POZİSYON AÇILDI] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{direction.upper()}"
            )

            return True

        except Exception as e:

            logging.error(
                f"Pozisyon açma hata {symbol}: {e}"
            )

            return False


# ============================================================
# MARKET POZİSYON KAPATMA
# ============================================================

def pozisyon_kapat(
    exchange,
    symbol,
    contracts,
    side,
    reason=""
):

    try:

        try:
            exchange.cancel_all_orders(
                symbol
            )
        except Exception:
            pass

        close_side = (
            "sell"
            if side == "long"
            else "buy"
        )

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
            f"[MARKET EXIT] "
            f"{symbol} | "
            f"{side.upper()} | "
            f"NEDEN: {reason}"
        )

        return True

    except Exception as e:

        logging.error(
            f"Market kapatma hata {symbol}: {e}"
        )

        return False


# ============================================================
# SMART SCALP EXIT
# ============================================================

def scalp_exit_engine(
    exchange,
    symbol,
    side,
    contracts,
    roi,
    entry_price,
    mark_price
):

    if not SCALP_SMART_EXIT_ENABLED:
        return False

    try:

        now = time.time()

        last_analysis = pozisyon_son_analiz.get(
            symbol,
            0
        )

        if (
            now - last_analysis
            <
            SCALP_ANALYSIS_REFRESH_SECONDS
        ):
            return False

        pozisyon_son_analiz[symbol] = now

        direction = (
            "buy"
            if side == "long"
            else "sell"
        )

        df = ohlcv_getir(
            exchange,
            symbol,
            "5m",
            40
        )

        if df is None:
            return False

        timing = entry_timing_engine(
            df,
            direction
        )

        # ----------------------------------------------------
        # Kâr var ama momentum tamamen çöktü
        # ----------------------------------------------------

        if (
            roi > 0 and
            timing["state"] == "EXHAUSTING" and
            roi >= 1.0
        ):

            return pozisyon_kapat(
                exchange,
                symbol,
                contracts,
                side,
                "Scalp momentum exhaustion"
            )

        # ----------------------------------------------------
        # Kâr pozitifken momentum tersine dönüyorsa
        # ----------------------------------------------------

        if (
            roi > 0 and
            timing["momentum"] < 45 and
            timing["acceleration"] < 40
        ):

            return pozisyon_kapat(
                exchange,
                symbol,
                contracts,
                side,
                "Scalp momentum reversal"
            )

        return False

    except Exception as e:

        logging.error(
            f"Scalp exit engine {symbol}: {e}"
        )

        return False


# ============================================================
# OPPORTUNITY REVERSAL ENGINE
# ============================================================

def opportunity_reversal_engine(
    exchange,
    symbol,
    side,
    roi,
    contracts
):

    if not OPPORTUNITY_REVERSAL_EXIT_ENABLED:
        return False

    if roi < OPPORTUNITY_REVERSAL_MIN_ROI:
        return False

    try:

        now = time.time()

        last_analysis = pozisyon_son_analiz.get(
            symbol,
            0
        )

        if (
            now - last_analysis
            <
            OPPORTUNITY_ANALYSIS_REFRESH_SECONDS
        ):
            return False

        pozisyon_son_analiz[symbol] = now

        direction = (
            "buy"
            if side == "long"
            else "sell"
        )

        df_15m = ohlcv_getir(
            exchange,
            symbol,
            "15m",
            50
        )

        df_5m = ohlcv_getir(
            exchange,
            symbol,
            "5m",
            50
        )

        if (
            df_15m is None or
            df_5m is None
        ):
            return False

        m15 = momentum_score(
            df_15m,
            direction
        )

        a5 = acceleration_score(
            df_5m,
            direction
        )

        e5 = exhaustion_score(
            df_5m,
            direction
        )

        # ----------------------------------------------------
        # Trendin tersine döndüğünü daha erken yakala
        # ----------------------------------------------------

        if direction == "buy":

            reversal = (
                m15 < 42 and
                a5 < 35 and
                e5 >= 50
            )

        else:

            reversal = (
                m15 < 42 and
                a5 < 35 and
                e5 >= 50
            )

        if reversal:

            return pozisyon_kapat(
                exchange,
                symbol,
                contracts,
                side,
                "Opportunity 15m/5m trend reversal"
            )

        return False

    except Exception as e:

        logging.error(
            f"Opportunity reversal {symbol}: {e}"
        )

        return False


# ============================================================
# TRAILING STOP
# ============================================================

def update_trailing_stop(
    exchange,
    symbol,
    contracts,
    side,
    entry_price,
    mark_price,
    current_max_roi
):

    try:

        leverage = LEVERAGE

        new_sl = None

        # ----------------------------------------------------
        # ROI %5 üzerindeyse kârı korumaya başla
        # ----------------------------------------------------

        if current_max_roi >= OPPORTUNITY_TRAILING_START_ROI:

            if current_max_roi >= OPPORTUNITY_STRONG_TRAILING_ROI:

                protected_roi = (
                    current_max_roi - 3.0
                )

            else:

                # %5 → 0%
                # %10 → %4
                # %15 → %10
                protected_roi = (
                    (current_max_roi - 5.0)
                    * 0.80
                )

            price_move = (
                protected_roi /
                100 /
                leverage
            )

            if side == "long":

                new_sl = (
                    entry_price *
                    (1 + price_move)
                )

            else:

                new_sl = (
                    entry_price *
                    (1 - price_move)
                )

        if new_sl is None:
            return False

        # ----------------------------------------------------
        # Stop mevcut mark fiyatının yanlış tarafına geçmesin
        # ----------------------------------------------------

        if side == "long":

            new_sl = min(
                new_sl,
                mark_price * 0.998
            )

        else:

            new_sl = max(
                new_sl,
                mark_price * 1.002
            )

        # ----------------------------------------------------
        # Stop gerçekten ilerliyorsa güncelle
        # ----------------------------------------------------

        old_sl = pozisyon_son_sl.get(
            symbol
        )

        if old_sl is not None:

            if side == "long" and new_sl <= old_sl:
                return False

            if side == "short" and new_sl >= old_sl:
                return False

        new_sl = float(
            exchange.price_to_precision(
                symbol,
                new_sl
            )
        )

        close_side = (
            "sell"
            if side == "long"
            else "buy"
        )

        # Eski stop yalnızca yeni stop doğrulandıktan
        # hemen önce kaldırılır.
        try:

            exchange.cancel_all_orders(
                symbol
            )

        except Exception:
            pass

        exchange.create_order(
            symbol,
            "stop_market",
            close_side,
            contracts,
            None,
            {
                "stopPrice": new_sl,
                "reduceOnly": True,
                "workingType": "MARK_PRICE"
            }
        )

        pozisyon_son_sl[symbol] = new_sl

        logging.info(
            f"[TRAILING] "
            f"{symbol} | "
            f"MaxROI=%{current_max_roi:.2f} | "
            f"SL={new_sl}"
        )

        return True

    except Exception as e:

        logging.error(
            f"Trailing stop hata {symbol}: {e}"
        )

        return False


# ============================================================
# POZİSYON YÖNETİMİ
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
    # Kapanan pozisyonların state temizliği
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

        pozisyon_son_sl.pop(
            sym,
            None
        )

        pozisyon_son_analiz.pop(
            sym,
            None
        )

    onceki_aktif_pozisyonlar = (
        aktif_semboller.copy()
    )

    # --------------------------------------------------------
    # Pozisyonlar
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

            if (
                entry_price <= 0 or
                mark_price <= 0
            ):
                continue

            p_type = pozisyon_tipini_cozumle(
                p
            )

            roi = float(
                p.get("percentage") or 0
            )

            # ------------------------------------------------
            # Max ROI
            # ------------------------------------------------

            current_max = (
                pozisyon_en_yuksek_kar.get(
                    symbol,
                    0.0
                )
            )

            if roi > current_max:

                pozisyon_en_yuksek_kar[
                    symbol
                ] = roi

                current_max = roi

            logging.info(
                f"[TAKİP] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{side.upper()} | "
                f"ROI=%{roi:.2f} | "
                f"MAX=%{current_max:.2f}"
            )

            # ------------------------------------------------
            # SCALP
            # ------------------------------------------------

            if p_type == "scalp":

                scalp_exit_engine(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    roi,
                    entry_price,
                    mark_price
                )

            # ------------------------------------------------
            # OPPORTUNITY
            # ------------------------------------------------

            else:

                opportunity_reversal_engine(
                    exchange,
                    symbol,
                    side,
                    roi,
                    contracts
                )

                update_trailing_stop(
                    exchange,
                    symbol,
                    contracts,
                    side,
                    entry_price,
                    mark_price,
                    current_max
                )

        except Exception as e:

            logging.error(
                f"Pozisyon yönetim hata {symbol}: {e}"
            )


# ============================================================
# POSITION MONITOR
# ============================================================

def pozisyon_monitor_loop():

    global monitor_basladi

    if (
        not POSITION_MONITOR_ENABLED
        or
        monitor_basladi
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
                f"Monitor bağlantı hatası: {e}"
            )

            exchange = None

        time.sleep(
            POSITION_MONITOR_INTERVAL
        )


def monitor_baslat():

    if POSITION_MONITOR_ENABLED:

        threading.Thread(
            target=pozisyon_monitor_loop,
            daemon=True,
            name="PositionMonitor"
        ).start()


# ============================================================
# ANALİZ DETAYI
# ============================================================

def candidate_detail(candidate):

    expected = candidate.get(
        "expected_move"
    )

    expected_text = ""

    if expected:

        expected_text = (
            f" | ExpectedMove="
            f"{expected['expected_move_pct']:.3f}%"
            f" | Target/Expected="
            f"{expected['target_vs_expected']:.2f}"
        )

    return (
        f"Final={candidate['score']:.2f} | "
        f"Trend={candidate['trend_score']:.1f} | "
        f"Momentum={candidate['momentum_score']:.1f} | "
        f"Acceleration={candidate['acceleration_score']:.1f} | "
        f"Entry={candidate['entry_score']:.1f} | "
        f"Exhaustion={candidate['exhaustion_score']:.1f} | "
        f"Structure={candidate['structure_score']:.1f} | "
        f"Pullback={candidate['pullback_score']:.1f} | "
        f"State={candidate['momentum_state']} | "
        f"BreakoutATR={candidate['breakout_dist']:.2f} | "
        f"Volume={candidate['volume_ratio']:.2f} | "
        f"R²={candidate['reg_r2']:.2f}"
        f"{expected_text}"
    )


# ============================================================
# ANA ANALİZ DÖNGÜSÜ
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
                "============================================================"
            )

            logging.info(
                ">>> V2 HİBRİT MARKET ANALİZİ BAŞLADI <<<"
            )

            logging.info(
                "============================================================"
            )

            anlik_islem_loglari = []
            scalp_takip = []
            firsat_takip = []
            aktif_roi_listesi = []
            aciklama_loglari = []

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
                    pozisyon_en_yuksek_kar.get(
                        sym,
                        0
                    )
                )

                aktif_roi_listesi.append({

                    "symbol": sym,

                    "mod": turu.upper(),

                    "binance_gercek_roi_yuzde":
                        round(roi, 2),

                    "max_gorulen_zirve_kar_yuzde":
                        round(max_roi, 2)

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
                    "[FIRSAT] Aktif pozisyon yok. Tarama başlıyor."
                )

                firsat_listesi = (
                    scan_opportunity_market(
                        exchange
                    )
                )

                for i, cand in enumerate(
                    firsat_listesi,
                    1
                ):

                    detay = candidate_detail(
                        cand
                    )

                    firsat_takip.append({

                        "symbol":
                            cand["symbol"],

                        "skor":
                            cand["score"],

                        "yon":
                            cand["direction"],

                        "trend_score":
                            cand["trend_score"],

                        "momentum_score":
                            cand["momentum_score"],

                        "acceleration_score":
                            cand["acceleration_score"],

                        "entry_score":
                            cand["entry_score"],

                        "exhaustion":
                            cand["exhaustion_score"],

                        "state":
                            cand["momentum_state"],

                        "detay":
                            detay

                    })

                    logging.info(
                        f"[FIRSAT #{i}] "
                        f"{cand['symbol']} | "
                        f"{detay}"
                    )

                # ------------------------------------------------
                # En iyi adaydan başla
                # ------------------------------------------------

                for candidate in firsat_listesi:

                    sym = candidate["symbol"]

                    detay = candidate_detail(
                        candidate
                    )

                    logging.info(
                        f"[FIRSAT TEYİT] "
                        f"{sym} -> {detay}"
                    )

                    basarili = pozisyon_ac(
                        exchange,
                        sym,
                        candidate["direction"],
                        candidate["score"],
                        "opportunity",
                        detay
                    )

                    if basarili:

                        anlik_islem_loglari.append(
                            f"FIRSAT: {sym} "
                            f"{candidate['direction'].upper()} "
                            f"| {detay}"
                        )

                        break

            else:

                logging.info(
                    "[FIRSAT] Zaten aktif pozisyon var."
                )

            # =================================================
            # SCALP
            # =================================================

            if not aktif_scalp_var:

                logging.info(
                    "[SCALP] Aktif pozisyon yok. Tarama başlıyor."
                )

                scalp_listesi = (
                    scan_scalp_market(
                        exchange
                    )
                )

                for i, cand in enumerate(
                    scalp_listesi,
                    1
                ):

                    detay = candidate_detail(
                        cand
                    )

                    scalp_takip.append({

                        "symbol":
                            cand["symbol"],

                        "skor":
                            cand["score"],

                        "yon":
                            cand["direction"],

                        "trend_score":
                            cand["trend_score"],

                        "momentum_score":
                            cand["momentum_score"],

                        "acceleration_score":
                            cand["acceleration_score"],

                        "entry_score":
                            cand["entry_score"],

                        "exhaustion":
                            cand["exhaustion_score"],

                        "state":
                            cand["momentum_state"],

                        "detay":
                            detay

                    })

                    logging.info(
                        f"[SCALP #{i}] "
                        f"{cand['symbol']} | "
                        f"{detay}"
                    )

                for candidate in scalp_listesi:

                    sym = candidate["symbol"]

                    detay = candidate_detail(
                        candidate
                    )

                    logging.info(
                        f"[SCALP TEYİT] "
                        f"{sym} -> {detay}"
                    )

                    basarili = pozisyon_ac(
                        exchange,
                        sym,
                        candidate["direction"],
                        candidate["score"],
                        "scalp",
                        detay
                    )

                    if basarili:

                        anlik_islem_loglari.append(
                            f"SCALP: {sym} "
                            f"{candidate['direction'].upper()} "
                            f"| {detay}"
                        )

                        break

            else:

                logging.info(
                    "[SCALP] Zaten aktif pozisyon var."
                )

            # =================================================
            # RAPOR
            # =================================================

            son_detayli_analiz_raporu = {

                "zaman":
                    pd.Timestamp.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),

                "scalp_takip_listesi":
                    scalp_takip,

                "firsat_takip_listesi":
                    firsat_takip,

                "aktif_pozisyonlar_roi_durumu":
                    aktif_roi_listesi,

                "yapilan_islemler":
                    anlik_islem_loglari,

                "aciklamalar":
                    aciklama_loglari

            }

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
            ">>> ANALİZ TAMAMLANDI. "
            "120 SANİYE SONRA YENİ TARAMA <<<"
        )

        time.sleep(120)


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def index():

    return jsonify({

        "status":
            "Bot V2 aktif - Momentum / Acceleration / Entry Timing",

        "trading_enabled":
            TRADING_ENABLED,

        "monitor_enabled":
            POSITION_MONITOR_ENABLED,

        "active_positions":
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
                pozisyon_tipini_cozumle(
                    p
                )
            )

            entry = float(
                p.get("entryPrice") or 0
            )

            mark = float(
                p.get("markPrice") or 0
            )

            side = p.get("side")

            roi = float(
                p.get("percentage") or 0
            )

            detaylar.append({

                "symbol":
                    sym,

                "mod":
                    p_type.upper(),

                "yon":
                    side.upper(),

                "giris_fiyati":
                    entry,

                "anlik_fiyat":
                    mark,

                "kaldirac":
                    LEVERAGE,

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

                "son_trailing_stop":
                    pozisyon_son_sl.get(
                        sym
                    )

            })

        return jsonify({

            "success":
                True,

            "aktif_islem_sayisi":
                len(detaylar),

            "islemler":
                detaylar

        })

    except Exception as e:

        return jsonify({

            "success":
                False,

            "error":
                str(e)

        })


@app.route("/otomatik-analiz")
def otomatik_analiz():

    return jsonify({

        "success":
            True,

        "mesaj":
            "V2 Momentum / Entry Timing analiz raporu",

        "analiz_raporu":
            son_detayli_analiz_raporu

    })


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    t = threading.Thread(
        target=ana_tarama_dongusu,
        daemon=True,
        name="MainAnalysis"
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