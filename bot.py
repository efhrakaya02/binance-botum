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
# SCALP + OPPORTUNITY + DİNAMİK TRADE PLAN ENGINE
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
SCALP_FEE_BUFFER_USDT = 0.04
SCALP_MAX_HOLD_MINUTES = 35

SCALP_EARLY_PROFIT_PROTECTION_ENABLED = True
SCALP_EARLY_PROFIT_MIN_ROI = 1.8
SCALP_PROFIT_LOCK_ROI = 2.0

# ============================================================
# OPPORTUNITY
# ============================================================

OPPORTUNITY_MAX_HOLD_HOURS = 24
OPPORTUNITY_MOMENTUM_EXIT_ENABLED = True

# ============================================================
# TARAMA & RATE LIMIT OPTİMİZASYONU
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

SCALP_MIN_ACCELERATION = 75
OPPORTUNITY_MIN_ACCELERATION = 70

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

MOMENTUM_ENGINE_ENABLED = True
ENTRY_TIMING_ENABLED = True
ENTRY_REQUIRE_TRIGGER = True
ENTRY_REQUIRE_CONFIRMATION = True
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
pozisyon_son_sl = {}

pozisyon_trade_plan = {}
pozisyon_saglik_loglari = {}

onceki_aktif_pozisyonlar = set()

son_detayli_analiz_raporu = {
    "zaman": "Henüz tarama yapılmadı",
    "scalp_takip_listesi": [],
    "firsat_takip_listesi": [],
    "aktif_pozisyonlar_roi_durumu": [],
    "yapilan_islemler": [],
    "aciklamalar": []
}

# ============================================================
# LOCKLAR
# ============================================================

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

    # Öncelikle RAM'deki kayıt
    if sym in pozisyon_tipleri:
        return pozisyon_tipleri[sym]

    try:
        contracts = abs(float(p.get("contracts") or 0))
        entry_price = float(p.get("entryPrice") or 0)
        leverage = float(p.get("leverage") or LEVERAGE)

        if contracts > 0 and entry_price > 0:
            notional = contracts * entry_price

            if leverage <= 0:
                leverage = LEVERAGE

            margin = notional / leverage

            # 10 USDT scalp / 15 USDT opportunity ayrımı
            # Ortadaki sınır 12.5 USDT
            if margin < 12.5:
                p_type = "scalp"
            else:
                p_type = "opportunity"

            pozisyon_tipleri[sym] = p_type
            return p_type

    except Exception:
        pass

    # Güvenli varsayılan
    return "opportunity"


# ============================================================
# GERÇEK BINANCE POZİSYON LİMİT KONTROLÜ
# ============================================================

def aktif_pozisyonlari_getir(exchange):
    """
    Binance'deki GERÇEK aktif Futures pozisyonlarını getirir.

    RAM state kullanılmaz.
    Railway restart sonrası bile gerçek pozisyonlar görülür.
    """

    try:
        positions = exchange.fetch_positions()

        aktif = []

        for p in positions:
            try:
                contracts = abs(float(p.get("contracts") or 0))

                if contracts <= 0:
                    continue

                symbol = sembol_duzelt(p.get("symbol"))

                if not symbol:
                    continue

                aktif.append(p)

            except Exception:
                continue

        return aktif

    except Exception as e:
        logging.error(f"[POZİSYON KONTROL] Binance pozisyonları alınamadı: {e}")

        # Güvenlik gereği Binance'den pozisyon okunamıyorsa
        # yeni işlem açılmasına izin verme.
        return None


def pozisyon_limit_kontrol(exchange, yeni_tip, yeni_symbol):
    """
    Yeni işlem açılmadan HEMEN ÖNCE çalışır.

    Kurallar:
        MAX_TOTAL_POSITIONS
        MAX_SCALP_POSITIONS
        MAX_OPPORTUNITY_POSITIONS
        Aynı sembolde ikinci işlem yok
    """

    aktif = aktif_pozisyonlari_getir(exchange)

    # Binance'den bilgi alınamıyorsa işlem açma.
    if aktif is None:
        logging.warning(
            "[İŞLEM ENGELLENDİ] Binance aktif pozisyon bilgisi alınamadı."
        )
        return False, "POSITION_DATA_UNAVAILABLE"

    total = len(aktif)
    scalp_count = 0
    opportunity_count = 0
    active_symbols = set()

    for p in aktif:
        symbol = sembol_duzelt(p.get("symbol"))
        active_symbols.add(symbol)

        p_type = pozisyon_tipini_cozumle(p)

        if p_type == "scalp":
            scalp_count += 1
        else:
            opportunity_count += 1

    # --------------------------------------------------------
    # TOPLAM POZİSYON LİMİTİ
    # --------------------------------------------------------

    if total >= MAX_TOTAL_POSITIONS:
        logging.warning(
            f"[LİMİT] Toplam pozisyon limiti dolu: "
            f"{total}/{MAX_TOTAL_POSITIONS} | "
            f"Yeni={yeni_tip.upper()} | {yeni_symbol}"
        )
        return False, "MAX_TOTAL_POSITIONS"

    # --------------------------------------------------------
    # AYNI COIN KONTROLÜ
    # --------------------------------------------------------

    if yeni_symbol in active_symbols:
        logging.warning(
            f"[LİMİT] {yeni_symbol} zaten açık. "
            f"Aynı sembolde ikinci işlem açılmayacak."
        )
        return False, "SYMBOL_ALREADY_OPEN"

    # --------------------------------------------------------
    # SCALP LİMİTİ
    # --------------------------------------------------------

    if yeni_tip == "scalp":

        if scalp_count >= MAX_SCALP_POSITIONS:
            logging.warning(
                f"[LİMİT] Scalp limiti dolu: "
                f"{scalp_count}/{MAX_SCALP_POSITIONS} | "
                f"Yeni={yeni_symbol}"
            )
            return False, "MAX_SCALP_POSITIONS"

    # --------------------------------------------------------
    # OPPORTUNITY LİMİTİ
    # --------------------------------------------------------

    elif yeni_tip == "opportunity":

        if opportunity_count >= MAX_OPPORTUNITY_POSITIONS:
            logging.warning(
                f"[LİMİT] Opportunity limiti dolu: "
                f"{opportunity_count}/{MAX_OPPORTUNITY_POSITIONS} | "
                f"Yeni={yeni_symbol}"
            )
            return False, "MAX_OPPORTUNITY_POSITIONS"

    # --------------------------------------------------------
    # SON DURUM
    # --------------------------------------------------------

    logging.info(
        f"[POZİSYON LİMİT KONTROL] "
        f"Toplam={total}/{MAX_TOTAL_POSITIONS} | "
        f"Scalp={scalp_count}/{MAX_SCALP_POSITIONS} | "
        f"Opportunity={opportunity_count}/{MAX_OPPORTUNITY_POSITIONS} | "
        f"Yeni={yeni_tip.upper()} | {yeni_symbol} → İZİN"
    )

    return True, "OK"


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

        # ----------------------------------------------------
        # ATR
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
        # ADX
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
        ).replace(
            0,
            np.nan
        )

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
            df["close"].pct_change(9) *
            100
        )

        # ----------------------------------------------------
        # BOLLINGER
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
            df["bb_upper"] -
            df["bb_lower"]
        ) / sma20.replace(
            0,
            np.nan
        )

        # ----------------------------------------------------
        # OBV
        # ----------------------------------------------------

        direction_sign = np.sign(
            df["close"].diff()
        )

        df["obv"] = (
            direction_sign *
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
        # CANDLE
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
        # VOLUME
        # ----------------------------------------------------

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
# DİNAMİK TRADE PLAN
# ============================================================

def hesapla_dinamik_trade_plan(
    entry_price,
    side,
    atr,
    p_type,
    leverage
):

    if atr <= 0:
        atr = entry_price * 0.01

    if p_type == "scalp":
        sl_mult = 1.8
        tp_mult = 2.2
    else:
        sl_mult = 2.5
        tp_mult = 5.0

    sl_dist = atr * sl_mult
    tp_dist = atr * tp_mult

    if side in ["buy", "long"]:

        sl_price = (
            entry_price -
            sl_dist
        )

        tp_price = (
            entry_price +
            tp_dist
        )

    else:

        sl_price = (
            entry_price +
            sl_dist
        )

        tp_price = (
            entry_price -
            tp_dist
        )

    # Güvenlik sınırı
    max_risk_pct = 75.0 / max(
        float(leverage),
        1.0
    )

    if side in ["buy", "long"]:

        max_zarar_fiyat = (
            entry_price *
            (1.0 - max_risk_pct / 100)
        )

    else:

        max_zarar_fiyat = (
            entry_price *
            (1.0 + max_risk_pct / 100)
        )

    return {
        "sl_price": float(sl_price),
        "tp_price": float(tp_price),
        "max_zarar_fiyat": float(max_zarar_fiyat),
        "atr_kullanilan": float(atr)
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

    if df is None or len(df) < 5:
        return 50

    last = df.iloc[-2]

    if side in ["buy", "long"]:

        if last["close"] < last["ema21"]:
            score -= 25

        if last["macd_hist"] < 0:
            score -= 20

        if momentum_data.get(
            "exhaustion_score",
            0
        ) > 60:
            score -= 25

    else:

        if last["close"] > last["ema21"]:
            score -= 25

        if last["macd_hist"] > 0:
            score -= 20

        if momentum_data.get(
            "exhaustion_score",
            0
        ) > 60:
            score -= 25

    if current_roi < -3.0:
        score -= 20

    return max(
        0,
        min(100, score)
    )


# ============================================================
# REGRESSION
# ============================================================

def gelismis_regresyon_teyidi(
    df,
    direction,
    periyot=20
):

    if df is None or len(df) < periyot + 2:
        return (
            False,
            0.0,
            0.0,
            "Yetersiz veri."
        )

    closes = df[
        "close"
    ].iloc[
        -periyot - 1:-1
    ].values

    x = np.arange(
        len(closes)
    )

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
            price >= regression_mid -
            atr * 0.5
        )

    else:

        ok = (
            slope < 0 and
            r_squared >= min_r2 and
            price <= regression_mid +
            atr * 0.5
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

    return min(
        max(score, 0),
        100
    )


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

    return min(
        max(score, 0),
        100
    )


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
        50
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

    rng = float(
        last["range"]
    )

    if rng <= 0:
        return 50

    score = 50

    if (
        direction == "buy" and
        last["close"] >
        last["open"]
    ):
        score += 25

    elif (
        direction == "sell" and
        last["close"] <
        last["open"]
    ):
        score += 25

    return min(
        max(score, 0),
        100
    )


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
            momentum += 20

        if last["macd_hist"] > 0:
            momentum += 15

        if last["rsi"] > 50:
            momentum += 15

    else:

        if last["ema9"] < last["ema21"]:
            momentum += 20

        if last["macd_hist"] < 0:
            momentum += 15

        if last["rsi"] < 50:
            momentum += 15

    momentum = min(
        max(momentum, 0),
        100
    )

    # --------------------------------------------------------
    # ACCELERATION
    # --------------------------------------------------------

    adx_now = float(
        last["adx"]
    )

    acceleration = (
        60
        if adx_now > 25
        else 45
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
    # OTHER
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

    # --------------------------------------------------------
    # EXHAUSTION
    # --------------------------------------------------------

    exhaustion = 30 if (
        (
            direction == "buy" and
            last["rsi"] > 75
        )
        or
        (
            direction == "sell" and
            last["rsi"] < 25
        )
    ) else 10

    # --------------------------------------------------------
    # TRIGGER
    # --------------------------------------------------------

    if direction == "buy":
        trigger = (
            70
            if last["macd_hist"] > 0
            else 50
        )
    else:
        trigger = (
            70
            if last["macd_hist"] < 0
            else 50
        )

    # --------------------------------------------------------
    # ENTRY SCORE
    # --------------------------------------------------------

    entry_score = (
        momentum * 0.30 +
        acceleration * 0.30 +
        volume_score * 0.20 +
        structure * 0.20
    )

    entry_score = min(
        max(entry_score, 0),
        100
    )

    if momentum >= 75:
        state = "STRONG"
    elif momentum >= 60:
        state = "BUILDING"
    else:
        state = "WEAK"

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

        "breakout_distance_atr":
            breakout[
                "distance_atr"
            ],

        "breakout_quality":
            breakout[
                "quality"
            ],

        "breakout_fresh":
            breakout[
                "fresh"
            ],

        "volume_ratio":
            round(
                volume_ratio,
                2
            ),

        "volume_score":
            round(
                volume_score,
                2
            ),

        "structure_score":
            round(
                structure,
                2
            ),

        "pullback_score":
            round(
                pullback,
                2
            ),

        "candle_quality":
            round(
                candle,
                2
            ),

        "trend_score":
            70.0,

        "trigger_score":
            float(trigger)
    }


# ============================================================
# TIMEFRAME CONFIRMATIONS
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
            "ok": True,
            "score": 60,
            "trend": "NEUTRAL"
        }

    last = df4.iloc[-2]

    if direction == "buy":

        ok = (
            last["close"] >
            last["ema50"]
        )

    else:

        ok = (
            last["close"] <
            last["ema50"]
        )

    return {
        "ok": bool(ok),
        "score": 75 if ok else 35,
        "trend":
            "CONFIRMED"
            if ok
            else "OPPOSITE"
    }


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
        50
    )

    if df is None:

        return {
            "ok": True,
            "score": 70,
            "state": "OK"
        }

    last = df.iloc[-2]

    if direction == "buy":

        ok = (
            last["ema9"] >
            last["ema21"] and
            last["macd_hist"] > 0
        )

    else:

        ok = (
            last["ema9"] <
            last["ema21"] and
            last["macd_hist"] < 0
        )

    return {
        "ok": bool(ok),
        "score": 80 if ok else 40,
        "state":
            "CONFIRMED"
            if ok
            else "WAIT"
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

    final_score = (
        mom["momentum_score"] +
        mom["entry_score"]
    ) / 2

    if mode == "scalp":

        minimum = SCALP_MIN_FINAL_SCORE

    else:

        minimum = OPPORTUNITY_MIN_FINAL_SCORE

    approved = (
        final_score >= minimum
    )

    return {
        "approved": approved,

        "reason":
            "APPROVED"
            if approved
            else "LOW_SCORE",

        "final_score":
            round(
                final_score,
                2
            ),

        "momentum": mom,

        "regression_r2": 0.85,

        "regression_slope": 0.001,

        "regression_message": "OK"
    }


# ============================================================
# MARKET SCAN
# ============================================================

def scan_scalp_market(exchange):

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
            key=lambda x:
                float(x["percentage"]),
            reverse=True
        )[:GAINER_COUNT]

        target_pool = [
            t["symbol"]
            for t in gainers
        ]

        candidates = []

        for symbol in target_pool[:10]:

            if cooldown_aktif_mi(
                symbol
            ):
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

        return candidates

    except Exception as e:

        logging.error(
            f"Scalp tarama hatası: {e}"
        )

        return []


def scan_opportunity_market(exchange):

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
            key=lambda x:
                float(x["percentage"]),
            reverse=True
        )[:GAINER_COUNT]

        target_pool = [
            t["symbol"]
            for t in gainers
        ]

        candidates = []

        for symbol in target_pool[:10]:

            if cooldown_aktif_mi(
                symbol
            ):
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

        return candidates

    except Exception as e:

        logging.error(
            f"Opportunity tarama hatası: {e}"
        )

        return []


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

    # ========================================================
    # ÇOK ÖNEMLİ:
    # İŞLEM AÇMA SÜRECİNİN TAMAMI LOCK ALTINDA
    # ========================================================

    with islem_acma_lock:

        try:

            symbol = sembol_duzelt(
                symbol
            )

            # ------------------------------------------------
            # COOLDOWN
            # ------------------------------------------------

            if cooldown_aktif_mi(
                symbol
            ):

                logging.info(
                    f"[COOLDOWN] {symbol} "
                    f"işlem açılmadı."
                )

                return False

            # ------------------------------------------------
            # GERÇEK BINANCE POZİSYON LİMİTİ
            # EMİRDEN HEMEN ÖNCE KONTROL
            # ------------------------------------------------

            limit_ok, limit_reason = (
                pozisyon_limit_kontrol(
                    exchange,
                    p_type,
                    symbol
                )
            )

            if not limit_ok:

                logging.warning(
                    f"[İŞLEM ENGELLENDİ] "
                    f"{symbol} | "
                    f"{p_type.upper()} | "
                    f"Sebep={limit_reason}"
                )

                return False

            # ------------------------------------------------
            # MARGIN MODE
            # ------------------------------------------------

            try:

                exchange.set_margin_mode(
                    "isolated",
                    symbol
                )

            except Exception as e:

                logging.warning(
                    f"[MARGIN] Isolated ayarlanamadı "
                    f"{symbol}: {e}"
                )

            # ------------------------------------------------
            # LEVERAGE
            # ------------------------------------------------

            try:

                exchange.set_leverage(
                    LEVERAGE,
                    symbol
                )

            except Exception as e:

                logging.warning(
                    f"[LEVERAGE] {symbol}: {e}"
                )

            # ------------------------------------------------
            # FİYAT
            # ------------------------------------------------

            ticker = exchange.fetch_ticker(
                symbol
            )

            price = float(
                ticker["last"]
            )

            # ------------------------------------------------
            # MARGIN
            # ------------------------------------------------

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

            # ------------------------------------------------
            # MARKET
            # ------------------------------------------------

            market = exchange.market(
                symbol
            )

            min_amount = (
                market["limits"]
                ["amount"]
                ["min"]
            )

            if (
                min_amount is not None
                and
                raw_amount < min_amount
            ):

                logging.warning(
                    f"[İŞLEM ENGELLENDİ] "
                    f"{symbol} | "
                    f"Minimum amount yetersiz."
                )

                return False

            amount = float(
                exchange.amount_to_precision(
                    symbol,
                    raw_amount
                )
            )

            if amount <= 0:
                return False

            # ------------------------------------------------
            # SON KONTROL
            # ------------------------------------------------
            # Emir göndermeden önce tekrar Binance pozisyon
            # kontrolü.
            #
            # Bu ikinci kontrol özellikle önemlidir.
            # ------------------------------------------------

            final_limit_ok, final_reason = (
                pozisyon_limit_kontrol(
                    exchange,
                    p_type,
                    symbol
                )
            )

            if not final_limit_ok:

                logging.warning(
                    f"[SON GÜVENLİK] "
                    f"{symbol} | "
                    f"Emir gönderilmedi | "
                    f"{final_reason}"
                )

                return False

            # ------------------------------------------------
            # SIDE
            # ------------------------------------------------

            side = (
                "buy"
                if direction == "buy"
                else "sell"
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

                logging.error(
                    f"[EMİR HATASI] "
                    f"{symbol} order boş döndü."
                )

                return False

            # ------------------------------------------------
            # ORDER SONRASI GERÇEK ENTRY
            # ------------------------------------------------

            actual_entry = price

            try:

                order_average = order.get(
                    "average"
                )

                if order_average:
                    actual_entry = float(
                        order_average
                    )

            except Exception:
                pass

            # ------------------------------------------------
            # STATE
            # ------------------------------------------------

            pozisyon_tipleri[
                symbol
            ] = p_type

            pozisyon_yonleri[
                symbol
            ] = direction

            pozisyon_giris_fiyatlari[
                symbol
            ] = actual_entry

            pozisyon_en_yuksek_kar[
                symbol
            ] = 0.0

            pozisyon_acilis_zamanlari[
                symbol
            ] = time.time()

            cooldown_baslat(
                symbol
            )

            # ------------------------------------------------
            # ATR
            # ------------------------------------------------

            df_temp = ohlcv_getir(
                exchange,
                symbol,
                SCALP_SCAN_TIMEFRAME,
                50
            )

            if df_temp is not None:

                atr = float(
                    df_temp.iloc[-2]["atr"]
                )

            else:

                atr = (
                    actual_entry *
                    0.01
                )

            # ------------------------------------------------
            # TRADE PLAN
            # ------------------------------------------------

            trade_plan = (
                hesapla_dinamik_trade_plan(
                    actual_entry,
                    side,
                    atr,
                    p_type,
                    LEVERAGE
                )
            )

            pozisyon_trade_plan[
                symbol
            ] = trade_plan

            # ------------------------------------------------
            # KORUYUCU SL
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
                        "stopPrice":
                            trade_plan[
                                "sl_price"
                            ],

                        "reduceOnly": True,

                        "workingType":
                            "MARK_PRICE"
                    }
                )

            except Exception as sl_error:

                logging.error(
                    f"[SL EMİR HATASI] "
                    f"{symbol}: {sl_error}"
                )

            # ------------------------------------------------
            # TAKE PROFIT
            # ------------------------------------------------

            try:

                exchange.create_order(
                    symbol,
                    "take_profit_market",
                    close_side,
                    amount,
                    None,
                    {
                        "stopPrice":
                            trade_plan[
                                "tp_price"
                            ],

                        "reduceOnly": True,

                        "workingType":
                            "MARK_PRICE"
                    }
                )

            except Exception as tp_error:

                logging.error(
                    f"[TP EMİR HATASI] "
                    f"{symbol}: {tp_error}"
                )

            # ------------------------------------------------
            # LOG
            # ------------------------------------------------

            logging.info(
                f"[BAŞARILI İŞLEM] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{side.upper()} | "
                f"Margin={target_margin:.2f} USDT | "
                f"Notional={notional:.2f} USDT | "
                f"Leverage={LEVERAGE}x | "
                f"Entry={actual_entry} | "
                f"SL={trade_plan['sl_price']} | "
                f"TP={trade_plan['tp_price']} | "
                f"ATR={atr:.8f} | "
                f"Score={score:.2f}"
            )

            # ------------------------------------------------
            # EMİR SONRASI POZİSYON SAYISI
            # ------------------------------------------------

            try:

                time.sleep(0.3)

                active_after = (
                    aktif_pozisyonlari_getir(
                        exchange
                    )
                )

                if active_after is not None:

                    logging.info(
                        f"[POZİSYON DURUMU] "
                        f"İşlem sonrası aktif="
                        f"{len(active_after)}"
                        f"/{MAX_TOTAL_POSITIONS}"
                    )

            except Exception:
                pass

            return True

        except Exception as e:

            logging.error(
                f"İşlem açma hata "
                f"{symbol}: {e}"
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
            f"Neden: {reason}"
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
        if abs(
            float(
                p.get("contracts") or 0
            )
        ) > 0
    }

    for p in positions:

        symbol = sembol_duzelt(
            p.get("symbol")
        )

        try:

            contracts = abs(
                float(
                    p.get("contracts") or 0
                )
            )

            if contracts <= 0:
                continue

            side = p.get(
                "side"
            )

            entry_price = float(
                p.get("entryPrice") or 0
            )

            mark_price = float(
                p.get("markPrice") or 0
            )

            roi = float(
                p.get("percentage") or 0
            )

            p_type = (
                pozisyon_tipini_cozumle(
                    p
                )
            )

            # ------------------------------------------------
            # MAX LOSS
            # ------------------------------------------------

            plan = (
                pozisyon_trade_plan.get(
                    symbol
                )
            )

            if plan:

                max_zarar_fiyat = (
                    plan[
                        "max_zarar_fiyat"
                    ]
                )

                if (
                    side == "long"
                    and
                    mark_price <=
                    max_zarar_fiyat
                ):

                    pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "MAX_LOSS_75_PERCENT"
                    )

                    continue

                if (
                    side == "short"
                    and
                    mark_price >=
                    max_zarar_fiyat
                ):

                    pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "MAX_LOSS_75_PERCENT"
                    )

                    continue

            # ------------------------------------------------
            # TRADE HEALTH
            # ------------------------------------------------

            df = ohlcv_getir(
                exchange,
                symbol,
                SCALP_SCAN_TIMEFRAME,
                50
            )

            if df is not None:

                momentum_direction = (
                    "buy"
                    if side == "long"
                    else "sell"
                )

                mom = (
                    calculate_momentum_engine(
                        df,
                        momentum_direction
                    )
                )

                health_score = (
                    trade_health_analizi(
                        df,
                        side,
                        roi,
                        mom
                    )
                )

                pozisyon_saglik_loglari[
                    symbol
                ] = health_score

                if (
                    health_score < 30
                    and
                    roi > 0
                ):

                    pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "TRADE_HEALTH_CRITICAL"
                    )

                    continue

            # ------------------------------------------------
            # ZİRVE ROI
            # ------------------------------------------------

            previous_max = (
                pozisyon_en_yuksek_kar.get(
                    symbol,
                    0.0
                )
            )

            if roi > previous_max:

                pozisyon_en_yuksek_kar[
                    symbol
                ] = roi

                logging.info(
                    f"[ZİRVE] "
                    f"{symbol} | "
                    f"ROI %{roi:.2f}"
                )

            # ------------------------------------------------
            # POZİSYON LOG
            # ------------------------------------------------

            logging.info(
                f"[POZİSYON] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{side.upper()} | "
                f"ROI %{roi:.2f} | "
                f"MAX %{pozisyon_en_yuksek_kar.get(symbol, 0):.2f}"
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
                        if abs(
                            float(
                                p.get(
                                    "contracts"
                                ) or 0
                            )
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

        # Binance rate limit nedeniyle
        # mevcut 15 saniyelik takip korunuyor.
        time.sleep(15.0)


def monitor_baslat():

    if POSITION_MONITOR_ENABLED:

        threading.Thread(
            target=pozisyon_monitor_loop,
            daemon=True,
            name="PositionMonitor"
        ).start()


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
                ">>> HİBRİT ANALİZ TARAMASI BAŞLADI <<<"
            )

            # =================================================
            # ÖN BİLGİ:
            # Mevcut pozisyon sayısını logla
            # =================================================

            aktif_baslangic = (
                aktif_pozisyonlari_getir(
                    exchange
                )
            )

            if aktif_baslangic is not None:

                scalp_count = 0
                opportunity_count = 0

                for p in aktif_baslangic:

                    p_type = (
                        pozisyon_tipini_cozumle(
                            p
                        )
                    )

                    if p_type == "scalp":
                        scalp_count += 1
                    else:
                        opportunity_count += 1

                logging.info(
                    f"[BAŞLANGIÇ POZİSYON] "
                    f"Toplam={len(aktif_baslangic)}/"
                    f"{MAX_TOTAL_POSITIONS} | "
                    f"Scalp={scalp_count}/"
                    f"{MAX_SCALP_POSITIONS} | "
                    f"Opportunity={opportunity_count}/"
                    f"{MAX_OPPORTUNITY_POSITIONS}"
                )

            # =================================================
            # OPPORTUNITY
            # =================================================

            firsat_listesi = (
                scan_opportunity_market(
                    exchange
                )
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

                    success = pozisyon_ac(
                        exchange,
                        candidate["symbol"],
                        candidate["direction"],
                        eval_res["final_score"],
                        "opportunity"
                    )

                    # Bir Opportunity denendiğinde
                    # ikinci aday için devam etmiyoruz.
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

                    success = pozisyon_ac(
                        exchange,
                        candidate["symbol"],
                        candidate["direction"],
                        eval_res["final_score"],
                        "scalp"
                    )

                    break

        except Exception as e:

            logging.error(
                f"Ana döngü hatası: {e}"
            )

        finally:

            gc.collect()

        # =====================================================
        # ANALİZ HER 5 DAKİKADA BİR
        # POZİSYON MONITOR AYRI THREAD
        # =====================================================

        time.sleep(300)


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def index():

    return jsonify({
        "status":
            "Bot aktif - "
            "Position Limit Protected",

        "trading_enabled":
            TRADING_ENABLED,

        "positions":
            list(
                pozisyon_tipleri.keys()
            ),

        "limits": {
            "max_total":
                MAX_TOTAL_POSITIONS,

            "max_scalp":
                MAX_SCALP_POSITIONS,

            "max_opportunity":
                MAX_OPPORTUNITY_POSITIONS,

            "scalp_margin":
                SCALP_MARGIN,

            "opportunity_margin":
                OPPORTUNITY_MARGIN,

            "leverage":
                LEVERAGE
        }
    })


@app.route("/durum")
def durum():

    return jsonify({
        "success": True,

        "aktif_islem_sayisi":
            len(pozisyon_tipleri),

        "saglik_durumlari":
            pozisyon_saglik_loglari,

        "trade_planlari":
            pozisyon_trade_plan,

        "limitler": {
            "MAX_TOTAL_POSITIONS":
                MAX_TOTAL_POSITIONS,

            "MAX_SCALP_POSITIONS":
                MAX_SCALP_POSITIONS,

            "MAX_OPPORTUNITY_POSITIONS":
                MAX_OPPORTUNITY_POSITIONS,

            "SCALP_MARGIN":
                SCALP_MARGIN,

            "OPPORTUNITY_MARGIN":
                OPPORTUNITY_MARGIN,

            "LEVERAGE":
                LEVERAGE
        }
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