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
# RAILWAY & BINANCE HİBRİT FUTURES BOT
# SCALP + OPPORTUNITY
# MULTI-TIMEFRAME TREND + MOMENTUM + ENTRY TIMING
# DYNAMIC PROFIT PROTECTION
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

# ============================================================
# SERMAYE / POZİSYON LİMİTLERİ
# ============================================================

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
# MONITOR
# ============================================================

POSITION_MONITOR_INTERVAL = 10.0

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

MIN_SCORE_THRESHOLD = 78

SCALP_MIN_FINAL_SCORE = 82
OPPORTUNITY_MIN_FINAL_SCORE = 84

SCALP_MIN_ENTRY_SCORE = 82
OPPORTUNITY_MIN_ENTRY_SCORE = 84

SCALP_MIN_MOMENTUM = 75
OPPORTUNITY_MIN_MOMENTUM = 72

SCALP_MIN_ACCELERATION = 72
OPPORTUNITY_MIN_ACCELERATION = 70

SCALP_MAX_EXHAUSTION = 45
OPPORTUNITY_MAX_EXHAUSTION = 50

# ============================================================
# TREND
# ============================================================

SCALP_MIN_TREND_SCORE = 78
OPPORTUNITY_MIN_TREND_SCORE = 78

SCALP_MIN_TREND_BARS = 3
OPPORTUNITY_MIN_TREND_BARS = 3

# ============================================================
# BREAKOUT / HACİM
# ============================================================

SCALP_MAX_BREAKOUT_ATR = 0.90
OPPORTUNITY_MAX_BREAKOUT_ATR = 1.50

IDEAL_BREAKOUT_ATR = 0.65

SCALP_MIN_VOLUME_RATIO = 1.30
OPPORTUNITY_MIN_VOLUME_RATIO = 1.50

# ============================================================
# ENTRY ENGINE
# ============================================================

MOMENTUM_ENGINE_ENABLED = True
ENTRY_TIMING_ENABLED = True

ENTRY_REQUIRE_TRIGGER = True
ENTRY_REQUIRE_CONFIRMATION = True

ENTRY_MAX_CANDLE_ATR = 1.80

# Girişte minimum MTF uyumu
SCALP_MIN_MTF_ALIGNMENT = 3
OPPORTUNITY_MIN_MTF_ALIGNMENT = 3

# ============================================================
# REGRESSION
# ============================================================

SCALP_MIN_REGRESSION_R2 = 0.58
OPPORTUNITY_MIN_REGRESSION_R2 = 0.60

# ============================================================
# ZARAR SONRASI KORUMA
# ============================================================

COOLDOWN_HOURS = 4

LOSS_REENTRY_BLOCK_HOURS = 24

# Aynı coin + aynı yön zarar sonrası bloke.
# Ters yön ise yalnızca güçlü teyitle açılabilir.
LOSS_SAME_DIRECTION_BLOCK = True

cooldown_map = {}

# {
#   "BTC/USDT": {
#       "buy": timestamp,
#       "sell": timestamp
#   }
# }
loss_direction_block = {}

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

# Pozisyon kâra geçtiğinde maksimum ROI
pozisyon_max_roi = {}

# Son momentum değerleri
pozisyon_son_momentum = {}

# Profit protection aktif mi?
pozisyon_profit_lock = {}

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
# LOCK
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

    return (
        now - son_islem
        <
        COOLDOWN_HOURS * 3600
    )


def cooldown_baslat(symbol):

    cooldown_map[symbol] = time.time()


# ============================================================
# LOSS DIRECTION BLOCK
# ============================================================

def zarar_sonrasi_yon_bloklu_mu(
    symbol,
    direction
):

    if not LOSS_SAME_DIRECTION_BLOCK:
        return False

    symbol = sembol_duzelt(symbol)

    data = loss_direction_block.get(
        symbol,
        {}
    )

    timestamp = data.get(direction)

    if timestamp is None:
        return False

    elapsed = time.time() - timestamp

    if elapsed < LOSS_REENTRY_BLOCK_HOURS * 3600:

        remaining = (
            LOSS_REENTRY_BLOCK_HOURS * 3600
            - elapsed
        ) / 3600

        logging.warning(
            f"[ZARAR BLOKU] {symbol} "
            f"{direction.upper()} bloke. "
            f"Kalan={remaining:.1f} saat"
        )

        return True

    return False


def zarar_sonrasi_blok_baslat(
    symbol,
    direction
):

    symbol = sembol_duzelt(symbol)

    if symbol not in loss_direction_block:
        loss_direction_block[symbol] = {}

    loss_direction_block[
        symbol
    ][direction] = time.time()

    logging.warning(
        f"[ZARAR SONRASI BLOK] "
        f"{symbol} | "
        f"{direction.upper()} | "
        f"{LOSS_REENTRY_BLOCK_HOURS} saat"
    )


# ============================================================
# POSITION TYPE
# ============================================================

def pozisyon_tipini_cozumle(p):

    sym = sembol_duzelt(
        p.get("symbol")
    )

    # Önce RAM
    if sym in pozisyon_tipleri:

        return pozisyon_tipleri[sym]

    try:

        contracts = abs(
            float(
                p.get("contracts") or 0
            )
        )

        entry_price = float(
            p.get("entryPrice") or 0
        )

        leverage = float(
            p.get("leverage") or LEVERAGE
        )

        if (
            contracts > 0
            and
            entry_price > 0
        ):

            notional = (
                contracts *
                entry_price
            )

            if leverage <= 0:
                leverage = LEVERAGE

            margin = (
                notional /
                leverage
            )

            if margin < 12.5:

                p_type = "scalp"

            else:

                p_type = "opportunity"

            pozisyon_tipleri[
                sym
            ] = p_type

            return p_type

    except Exception:
        pass

    return "opportunity"


# ============================================================
# GERÇEK AKTİF POZİSYONLAR
# ============================================================

def aktif_pozisyonlari_getir(exchange):

    try:

        positions = (
            exchange.fetch_positions()
        )

        aktif = []

        for p in positions:

            try:

                contracts = abs(
                    float(
                        p.get("contracts")
                        or 0
                    )
                )

                if contracts <= 0:
                    continue

                symbol = sembol_duzelt(
                    p.get("symbol")
                )

                if not symbol:
                    continue

                aktif.append(p)

            except Exception:

                continue

        return aktif

    except Exception as e:

        logging.error(
            f"[POZİSYON KONTROL] "
            f"Binance pozisyonları alınamadı: {e}"
        )

        # Veri okunamıyorsa kesinlikle yeni
        # işlem açma.
        return None


# ============================================================
# POZİSYON LİMİT KONTROLÜ
# ============================================================

def pozisyon_limit_kontrol(
    exchange,
    yeni_tip,
    yeni_symbol
):

    aktif = aktif_pozisyonlari_getir(
        exchange
    )

    if aktif is None:

        logging.warning(
            "[İŞLEM ENGELLENDİ] "
            "Binance pozisyon verisi alınamadı."
        )

        return False, "POSITION_DATA_UNAVAILABLE"

    total = len(aktif)

    scalp_count = 0
    opportunity_count = 0

    active_symbols = set()

    for p in aktif:

        symbol = sembol_duzelt(
            p.get("symbol")
        )

        active_symbols.add(
            symbol
        )

        p_type = (
            pozisyon_tipini_cozumle(p)
        )

        if p_type == "scalp":

            scalp_count += 1

        else:

            opportunity_count += 1

    # TOPLAM
    if total >= MAX_TOTAL_POSITIONS:

        return (
            False,
            "MAX_TOTAL_POSITIONS"
        )

    # AYNI COIN
    if yeni_symbol in active_symbols:

        return (
            False,
            "SYMBOL_ALREADY_OPEN"
        )

    # SCALP
    if (
        yeni_tip == "scalp"
        and
        scalp_count >=
        MAX_SCALP_POSITIONS
    ):

        return (
            False,
            "MAX_SCALP_POSITIONS"
        )

    # OPPORTUNITY
    if (
        yeni_tip == "opportunity"
        and
        opportunity_count >=
        MAX_OPPORTUNITY_POSITIONS
    ):

        return (
            False,
            "MAX_OPPORTUNITY_POSITIONS"
        )

    logging.info(
        f"[LİMİT OK] "
        f"Toplam={total}/{MAX_TOTAL_POSITIONS} | "
        f"Scalp={scalp_count}/{MAX_SCALP_POSITIONS} | "
        f"Opportunity={opportunity_count}/"
        f"{MAX_OPPORTUNITY_POSITIONS} | "
        f"Yeni={yeni_tip.upper()} | "
        f"{yeni_symbol}"
    )

    return True, "OK"


# ============================================================
# OHLCV
# ============================================================

def ohlcv_getir(
    exchange,
    symbol,
    timeframe,
    limit=250
):

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

        # ====================================================
        # EMA
        # ====================================================

        df["ema9"] = (
            df["close"]
            .ewm(
                span=9,
                adjust=False
            )
            .mean()
        )

        df["ema21"] = (
            df["close"]
            .ewm(
                span=21,
                adjust=False
            )
            .mean()
        )

        df["ema50"] = (
            df["close"]
            .ewm(
                span=50,
                adjust=False
            )
            .mean()
        )

        df["ema200"] = (
            df["close"]
            .ewm(
                span=200,
                adjust=False
            )
            .mean()
        )

        # ====================================================
        # MACD
        # ====================================================

        exp12 = (
            df["close"]
            .ewm(
                span=12,
                adjust=False
            )
            .mean()
        )

        exp26 = (
            df["close"]
            .ewm(
                span=26,
                adjust=False
            )
            .mean()
        )

        df["macd"] = (
            exp12 -
            exp26
        )

        df["macd_signal"] = (
            df["macd"]
            .ewm(
                span=9,
                adjust=False
            )
            .mean()
        )

        df["macd_hist"] = (
            df["macd"] -
            df["macd_signal"]
        )

        # ====================================================
        # RSI
        # ====================================================

        delta = df["close"].diff()

        gain = delta.where(
            delta > 0,
            0.0
        )

        loss = -delta.where(
            delta < 0,
            0.0
        )

        avg_gain = (
            gain
            .ewm(
                alpha=1 / 14,
                adjust=False
            )
            .mean()
        )

        avg_loss = (
            loss
            .ewm(
                alpha=1 / 14,
                adjust=False
            )
            .mean()
        )

        rs = (
            avg_gain /
            avg_loss.replace(
                0,
                np.nan
            )
        )

        df["rsi"] = (
            100 -
            (
                100 /
                (1 + rs)
            )
        )

        # ====================================================
        # ATR
        # ====================================================

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

        df["atr"] = (
            tr
            .ewm(
                alpha=1 / 14,
                adjust=False
            )
            .mean()
        )

        # ====================================================
        # ADX
        # ====================================================

        up_move = df["high"].diff()

        down_move = (
            -df["low"].diff()
        )

        plus_dm = np.where(
            (
                up_move > down_move
            )
            &
            (
                up_move > 0
            ),
            up_move,
            0.0
        )

        minus_dm = np.where(
            (
                down_move > up_move
            )
            &
            (
                down_move > 0
            ),
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

        atr_safe = (
            df["atr"]
            .replace(
                0,
                np.nan
            )
        )

        df["plus_di"] = (
            100 *
            plus_dm
            .ewm(
                alpha=1 / 14,
                adjust=False
            )
            .mean() /
            atr_safe
        )

        df["minus_di"] = (
            100 *
            minus_dm
            .ewm(
                alpha=1 / 14,
                adjust=False
            )
            .mean() /
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

        df["adx"] = (
            df["dx"]
            .ewm(
                alpha=1 / 14,
                adjust=False
            )
            .mean()
        )

        # ====================================================
        # ROC
        # ====================================================

        df["roc"] = (
            df["close"]
            .pct_change(9)
            * 100
        )

        df["roc_short"] = (
            df["close"]
            .pct_change(3)
            * 100
        )

        # ====================================================
        # BOLLINGER
        # ====================================================

        sma20 = (
            df["close"]
            .rolling(20)
            .mean()
        )

        std20 = (
            df["close"]
            .rolling(20)
            .std()
        )

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
            )
            /
            sma20.replace(
                0,
                np.nan
            )
        )

        # ====================================================
        # OBV
        # ====================================================

        direction_sign = np.sign(
            df["close"].diff()
        )

        df["obv"] = (
            direction_sign *
            df["volume"]
        ).fillna(0).cumsum()

        df["obv_ema"] = (
            df["obv"]
            .ewm(
                span=20,
                adjust=False
            )
            .mean()
        )

        # ====================================================
        # VWAP
        # ====================================================

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

        # ====================================================
        # CANDLE
        # ====================================================

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
            df[
                ["open", "close"]
            ].max(axis=1)
        )

        df["lower_wick"] = (
            df[
                ["open", "close"]
            ].min(axis=1)
            -
            df["low"]
        )

        # ====================================================
        # VOLUME
        # ====================================================

        df["volume_ma20"] = (
            df["volume"]
            .rolling(20)
            .mean()
        )

        df["volume_ratio"] = (
            df["volume"] /
            df["volume_ma20"]
            .replace(
                0,
                np.nan
            )
        )

        # ====================================================
        # ATR DISTANCE
        # ====================================================

        df["atr_pct"] = (
            df["atr"] /
            df["close"].replace(
                0,
                np.nan
            )
            * 100
        )

        df.replace(
            [
                np.inf,
                -np.inf
            ],
            np.nan,
            inplace=True
        )

        df.ffill(
            inplace=True
        )

        df.bfill(
            inplace=True
        )

        return df

    except Exception as e:

        logging.error(
            f"OHLCV hata "
            f"{symbol} "
            f"{timeframe}: {e}"
        )

        return None


# ============================================================
# SON KAPANMIŞ MUM
# ============================================================

def son_kapanmis_mum(df):

    if df is None or len(df) < 3:
        return None

    return df.iloc[-2]


# ============================================================
# TREND DIRECTION
# ============================================================

def trend_direction(df):

    if df is None or len(df) < 30:
        return "neutral"

    d = df.iloc[:-1]

    last = d.iloc[-1]

    bullish = 0
    bearish = 0

    if last["close"] > last["ema21"]:
        bullish += 1
    else:
        bearish += 1

    if last["ema9"] > last["ema21"]:
        bullish += 1
    else:
        bearish += 1

    if last["ema21"] > last["ema50"]:
        bullish += 1
    else:
        bearish += 1

    if last["ema50"] > last["ema200"]:
        bullish += 1
    else:
        bearish += 1

    if last["macd_hist"] > 0:
        bullish += 1
    else:
        bearish += 1

    if bullish >= 4:
        return "buy"

    if bearish >= 4:
        return "sell"

    return "neutral"


# ============================================================
# TREND SÜRESİ
# ============================================================

def trend_duration(df, direction, max_bars=20):

    if df is None or len(df) < max_bars + 5:
        return 0

    d = df.iloc[:-1]

    count = 0

    for i in range(
        len(d) - 1,
        max(
            -1,
            len(d) - max_bars - 1
        ),
        -1
    ):

        row = d.iloc[i]

        if direction == "buy":

            valid = (
                row["ema9"] >
                row["ema21"]
                and
                row["ema21"] >
                row["ema50"]
            )

        else:

            valid = (
                row["ema9"] <
                row["ema21"]
                and
                row["ema21"] <
                row["ema50"]
            )

        if valid:

            count += 1

        else:

            break

    return count


# ============================================================
# TREND SCORE
# ============================================================

def trend_score(df, direction):

    if df is None or len(df) < 50:
        return 0

    d = df.iloc[:-1]

    last = d.iloc[-1]

    score = 0

    if direction == "buy":

        if last["close"] > last["ema21"]:
            score += 15

        if last["ema9"] > last["ema21"]:
            score += 15

        if last["ema21"] > last["ema50"]:
            score += 15

        if last["ema50"] > last["ema200"]:
            score += 15

        if last["macd_hist"] > 0:
            score += 10

        if last["rsi"] > 50:
            score += 10

        if last["adx"] > 20:
            score += 10

        if last["plus_di"] > last["minus_di"]:
            score += 10

    else:

        if last["close"] < last["ema21"]:
            score += 15

        if last["ema9"] < last["ema21"]:
            score += 15

        if last["ema21"] < last["ema50"]:
            score += 15

        if last["ema50"] < last["ema200"]:
            score += 15

        if last["macd_hist"] < 0:
            score += 10

        if last["rsi"] < 50:
            score += 10

        if last["adx"] > 20:
            score += 10

        if last["minus_di"] > last["plus_di"]:
            score += 10

    return min(
        max(score, 0),
        100
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

    recent_high = (
        d["high"]
        .iloc[-21:-1]
        .max()
    )

    recent_low = (
        d["low"]
        .iloc[-21:-1]
        .min()
    )

    if direction == "buy":

        if last["close"] > last["ema21"]:
            score += 10

        if last["ema21"] > last["ema50"]:
            score += 10

        if last["close"] > prev["close"]:
            score += 5

        if last["high"] > prev["high"]:
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

        if last["low"] < prev["low"]:
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

def breakout_analysis(
    df,
    direction
):

    if df is None or len(df) < 25:

        return {
            "breakout": False,
            "distance_atr": 0,
            "fresh": False,
            "quality": 0
        }

    d = df.iloc[:-1]

    last = d.iloc[-1]

    atr = float(
        last["atr"]
    )

    if atr <= 0:

        return {
            "breakout": False,
            "distance_atr": 0,
            "fresh": False,
            "quality": 0
        }

    previous_high = (
        d["high"]
        .iloc[-21:-1]
        .max()
    )

    previous_low = (
        d["low"]
        .iloc[-21:-1]
        .min()
    )

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

    # Çok ileri kaçmış breakout
    if distance > 1.8:

        quality = 20

    elif distance <= 0.65:

        quality = 95

    elif distance <= 1.0:

        quality = 85

    elif distance <= 1.5:

        quality = 70

    else:

        quality = 40

    return {

        "breakout":
            bool(breakout),

        "distance_atr":
            round(
                float(distance),
                3
            ),

        "fresh":
            bool(
                breakout
            ),

        "quality":
            quality
    }


# ============================================================
# CANDLE QUALITY
# ============================================================

def candle_quality(
    df,
    direction
):

    if df is None or len(df) < 5:
        return 50

    d = df.iloc[:-1]

    last = d.iloc[-1]

    rng = float(
        last["range"]
    )

    atr = float(
        last["atr"]
    )

    if rng <= 0 or atr <= 0:
        return 50

    score = 50

    body_ratio = float(
        last["body_ratio"]
    )

    candle_atr = (
        rng / atr
    )

    if direction == "buy":

        if last["close"] > last["open"]:
            score += 20

        if (
            last["lower_wick"] <
            last["upper_wick"]
        ):
            score += 5

    else:

        if last["close"] < last["open"]:
            score += 20

        if (
            last["upper_wick"] <
            last["lower_wick"]
        ):
            score += 5

    if body_ratio >= 0.55:
        score += 10

    if candle_atr > ENTRY_MAX_CANDLE_ATR:
        score -= 30

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

    if df is None or len(df) < 40:

        return {
            "momentum_score": 50,
            "acceleration_score": 50,
            "exhaustion_score": 50,
            "entry_score": 50,
            "state": "WAIT",
            "breakout_distance_atr": 0,
            "volume_ratio": 1,
            "structure_score": 50,
            "pullback_score": 50,
            "candle_quality": 50,
            "trend_score": 50,
            "trigger_score": 50,
            "trend_duration": 0
        }

    d = df.iloc[:-1]

    last = d.iloc[-1]
    prev = d.iloc[-2]

    # ========================================================
    # MOMENTUM
    # ========================================================

    momentum = 50

    if direction == "buy":

        if last["ema9"] > last["ema21"]:
            momentum += 15

        if last["macd_hist"] > 0:
            momentum += 15

        if last["rsi"] > 50:
            momentum += 10

        if last["roc"] > 0:
            momentum += 10

    else:

        if last["ema9"] < last["ema21"]:
            momentum += 15

        if last["macd_hist"] < 0:
            momentum += 15

        if last["rsi"] < 50:
            momentum += 10

        if last["roc"] < 0:
            momentum += 10

    momentum = min(
        max(momentum, 0),
        100
    )

    # ========================================================
    # ACCELERATION
    # ========================================================

    acceleration = 50

    macd_hist_now = float(
        last["macd_hist"]
    )

    macd_hist_prev = float(
        prev["macd_hist"]
    )

    if direction == "buy":

        if macd_hist_now > macd_hist_prev:
            acceleration += 20

        if last["roc_short"] > 0:
            acceleration += 15

        if last["plus_di"] > last["minus_di"]:
            acceleration += 10

    else:

        if macd_hist_now < macd_hist_prev:
            acceleration += 20

        if last["roc_short"] < 0:
            acceleration += 15

        if last["minus_di"] > last["plus_di"]:
            acceleration += 10

    if last["adx"] > 25:
        acceleration += 5

    acceleration = min(
        max(acceleration, 0),
        100
    )

    # ========================================================
    # VOLUME
    # ========================================================

    volume_ratio = float(
        last["volume_ratio"]
    )

    volume_score = min(
        100,
        max(
            0,
            50 +
            (
                volume_ratio - 1
            ) * 40
        )
    )

    # ========================================================
    # TREND
    # ========================================================

    tr_score = trend_score(
        df,
        direction
    )

    duration = trend_duration(
        df,
        direction
    )

    # ========================================================
    # STRUCTURE
    # ========================================================

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

    # ========================================================
    # EXHAUSTION
    # ========================================================

    exhaustion = 10

    if direction == "buy":

        if last["rsi"] > 70:
            exhaustion += 25

        if last["rsi"] > 78:
            exhaustion += 20

        if (
            last["close"] >
            last["bb_upper"]
        ):
            exhaustion += 20

    else:

        if last["rsi"] < 30:
            exhaustion += 25

        if last["rsi"] < 22:
            exhaustion += 20

        if (
            last["close"] <
            last["bb_lower"]
        ):
            exhaustion += 20

    if (
        last["range"] /
        last["atr"]
        >
        ENTRY_MAX_CANDLE_ATR
    ):

        exhaustion += 15

    exhaustion = min(
        exhaustion,
        100
    )

    # ========================================================
    # TRIGGER
    # ========================================================

    trigger = 50

    if direction == "buy":

        if (
            last["ema9"] >
            last["ema21"]
        ):
            trigger += 15

        if last["macd_hist"] > 0:
            trigger += 15

        if last["close"] > last["open"]:
            trigger += 10

    else:

        if (
            last["ema9"] <
            last["ema21"]
        ):
            trigger += 15

        if last["macd_hist"] < 0:
            trigger += 15

        if last["close"] < last["open"]:
            trigger += 10

    trigger = min(
        max(trigger, 0),
        100
    )

    # ========================================================
    # ENTRY SCORE
    # ========================================================

    entry_score = (
        momentum * 0.25
        +
        acceleration * 0.20
        +
        volume_score * 0.10
        +
        structure * 0.15
        +
        tr_score * 0.20
        +
        trigger * 0.10
    )

    entry_score = min(
        max(entry_score, 0),
        100
    )

    # ========================================================
    # STATE
    # ========================================================

    if (
        momentum >= 80
        and
        acceleration >= 75
        and
        tr_score >= 80
    ):

        state = "STRONG"

    elif (
        momentum >= 70
        and
        acceleration >= 65
    ):

        state = "BUILDING"

    else:

        state = "WEAK"

    return {

        "momentum_score":
            round(
                momentum,
                2
            ),

        "acceleration_score":
            round(
                acceleration,
                2
            ),

        "exhaustion_score":
            round(
                exhaustion,
                2
            ),

        "entry_score":
            round(
                entry_score,
                2
            ),

        "state":
            state,

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
            round(
                tr_score,
                2
            ),

        "trigger_score":
            float(trigger),

        "trend_duration":
            duration
    }


# ============================================================
# REGRESSION
# ============================================================

def gelismis_regresyon_teyidi(
    df,
    direction,
    periyot=20
):

    if df is None or len(df) < periyot + 3:

        return (
            False,
            0.0,
            0.0,
            "Yetersiz veri."
        )

    closes = (
        df["close"]
        .iloc[
            -periyot - 1:-1
        ]
        .values
    )

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

    if (
        np.std(closes) == 0
        or
        np.std(y_pred) == 0
    ):

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
        (
            len(closes) - 1
        )
    )

    atr = float(
        df["atr"].iloc[-2]
    )

    if atr <= 0:
        atr = price * 0.01

    min_r2 = (
        SCALP_MIN_REGRESSION_R2
        if direction in ["buy", "sell"]
        else 0.55
    )

    if direction == "buy":

        ok = (
            slope > 0
            and
            r_squared >= min_r2
            and
            price >= (
                regression_mid -
                atr * 0.6
            )
        )

    else:

        ok = (
            slope < 0
            and
            r_squared >= min_r2
            and
            price <= (
                regression_mid +
                atr * 0.6
            )
        )

    mesaj = (
        f"Eğim={slope:.8f} | "
        f"R²={r_squared:.2f}"
    )

    return (
        ok,
        float(slope),
        float(r_squared),
        mesaj
    )


# ============================================================
# HIGHER TIMEFRAME
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
        150
    )

    if df4 is None:

        return {
            "ok": False,
            "score": 0,
            "trend": "NO_DATA",
            "duration": 0
        }

    last = df4.iloc[-2]

    tr_score = trend_score(
        df4,
        direction
    )

    duration = trend_duration(
        df4,
        direction
    )

    if direction == "buy":

        ok = (
            last["close"] >
            last["ema50"]
            and
            last["ema50"] >
            last["ema200"]
            and
            last["ema9"] >
            last["ema21"]
            and
            last["macd_hist"] > 0
        )

    else:

        ok = (
            last["close"] <
            last["ema50"]
            and
            last["ema50"] <
            last["ema200"]
            and
            last["ema9"] <
            last["ema21"]
            and
            last["macd_hist"] < 0
        )

    return {

        "ok":
            bool(ok),

        "score":
            round(
                tr_score,
                2
            ),

        "trend":
            "CONFIRMED"
            if ok
            else "OPPOSITE",

        "duration":
            duration
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

    last = df.iloc[-2]
    prev = df.iloc[-3]

    score = 50

    if direction == "buy":

        if last["ema9"] > last["ema21"]:
            score += 15

        if last["macd_hist"] > 0:
            score += 15

        if (
            last["macd_hist"] >
            prev["macd_hist"]
        ):
            score += 10

        if last["close"] > last["open"]:
            score += 10

    else:

        if last["ema9"] < last["ema21"]:
            score += 15

        if last["macd_hist"] < 0:
            score += 15

        if (
            last["macd_hist"] <
            prev["macd_hist"]
        ):
            score += 10

        if last["close"] < last["open"]:
            score += 10

    ok = score >= 75

    return {

        "ok":
            bool(ok),

        "score":
            round(
                score,
                2
            ),

        "state":
            "CONFIRMED"
            if ok
            else "WAIT"
    }


# ============================================================
# MULTI TIMEFRAME ALIGNMENT
# ============================================================

def multi_timeframe_confirmation(
    exchange,
    symbol,
    direction,
    mode
):

    if mode == "scalp":

        main_tf = SCALP_SCAN_TIMEFRAME
        trigger_tf = SCALP_TRIGGER_TIMEFRAME

    else:

        main_tf = OPPORTUNITY_SCAN_TIMEFRAME
        trigger_tf = OPPORTUNITY_TRIGGER_TIMEFRAME

    df_main = ohlcv_getir(
        exchange,
        symbol,
        main_tf,
        120
    )

    df_trigger = ohlcv_getir(
        exchange,
        symbol,
        trigger_tf,
        100
    )

    df_high = ohlcv_getir(
        exchange,
        symbol,
        HIGHER_TIMEFRAME,
        120
    )

    if (
        df_main is None
        or
        df_trigger is None
        or
        df_high is None
    ):

        return {
            "ok": False,
            "alignment": 0,
            "details": "MTF_DATA_MISSING"
        }

    main_trend = trend_direction(
        df_main
    )

    trigger_trend = trend_direction(
        df_trigger
    )

    high_trend = trend_direction(
        df_high
    )

    alignment = 0

    if main_trend == direction:
        alignment += 1

    if trigger_trend == direction:
        alignment += 1

    if high_trend == direction:
        alignment += 1

    minimum = (
        SCALP_MIN_MTF_ALIGNMENT
        if mode == "scalp"
        else OPPORTUNITY_MIN_MTF_ALIGNMENT
    )

    return {

        "ok":
            alignment >= minimum,

        "alignment":
            alignment,

        "main":
            main_trend,

        "trigger":
            trigger_trend,

        "higher":
            high_trend,

        "minimum":
            minimum
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

    symbol = sembol_duzelt(symbol)

    # ========================================================
    # ZARAR SONRASI AYNI YÖN BLOĞU
    # ========================================================

    if zarar_sonrasi_yon_bloklu_mu(
        symbol,
        direction
    ):

        return {
            "approved": False,
            "reason":
                "LOSS_DIRECTION_BLOCK"
        }

    # ========================================================
    # DATA
    # ========================================================

    if df is None:

        return {
            "approved": False,
            "reason": "DATA"
        }

    # ========================================================
    # MOMENTUM
    # ========================================================

    mom = calculate_momentum_engine(
        df,
        direction
    )

    # ========================================================
    # REGRESSION
    # ========================================================

    (
        regression_ok,
        regression_slope,
        regression_r2,
        regression_message
    ) = gelismis_regresyon_teyidi(
        df,
        direction,
        20
    )

    # ========================================================
    # HIGHER TIMEFRAME
    # ========================================================

    higher = higher_timeframe_confirmation(
        exchange,
        symbol,
        direction
    )

    # ========================================================
    # TRIGGER
    # ========================================================

    if mode == "scalp":

        trigger_tf = (
            SCALP_TRIGGER_TIMEFRAME
        )

    else:

        trigger_tf = (
            OPPORTUNITY_TRIGGER_TIMEFRAME
        )

    trigger = trigger_confirmation(
        exchange,
        symbol,
        direction,
        trigger_tf
    )

    # ========================================================
    # MTF
    # ========================================================

    mtf = multi_timeframe_confirmation(
        exchange,
        symbol,
        direction,
        mode
    )

    # ========================================================
    # CANDLE
    # ========================================================

    last = df.iloc[-2]

    atr = float(
        last["atr"]
    )

    candle_atr = (
        float(last["range"]) /
        atr
        if atr > 0
        else 99
    )

    # ========================================================
    # MODE PARAMETRELERİ
    # ========================================================

    if mode == "scalp":

        minimum_final = (
            SCALP_MIN_FINAL_SCORE
        )

        minimum_momentum = (
            SCALP_MIN_MOMENTUM
        )

        minimum_acceleration = (
            SCALP_MIN_ACCELERATION
        )

        maximum_exhaustion = (
            SCALP_MAX_EXHAUSTION
        )

        minimum_volume = (
            SCALP_MIN_VOLUME_RATIO
        )

        minimum_trend = (
            SCALP_MIN_TREND_SCORE
        )

        minimum_duration = (
            SCALP_MIN_TREND_BARS
        )

        max_breakout = (
            SCALP_MAX_BREAKOUT_ATR
        )

    else:

        minimum_final = (
            OPPORTUNITY_MIN_FINAL_SCORE
        )

        minimum_momentum = (
            OPPORTUNITY_MIN_MOMENTUM
        )

        minimum_acceleration = (
            OPPORTUNITY_MIN_ACCELERATION
        )

        maximum_exhaustion = (
            OPPORTUNITY_MAX_EXHAUSTION
        )

        minimum_volume = (
            OPPORTUNITY_MIN_VOLUME_RATIO
        )

        minimum_trend = (
            OPPORTUNITY_MIN_TREND_SCORE
        )

        minimum_duration = (
            OPPORTUNITY_MIN_TREND_BARS
        )

        max_breakout = (
            OPPORTUNITY_MAX_BREAKOUT_ATR
        )

    # ========================================================
    # FINAL SCORE
    # ========================================================

    final_score = (
        mom["momentum_score"] * 0.20
        +
        mom["entry_score"] * 0.20
        +
        mom["trend_score"] * 0.20
        +
        trigger["score"] * 0.10
        +
        higher["score"] * 0.15
        +
        (
            regression_r2 * 100
        ) * 0.10
        +
        mom["candle_quality"] * 0.05
    )

    # ========================================================
    # HARD FILTERS
    # ========================================================

    rejection_reasons = []

    if (
        final_score <
        minimum_final
    ):

        rejection_reasons.append(
            "LOW_FINAL_SCORE"
        )

    if (
        mom["momentum_score"] <
        minimum_momentum
    ):

        rejection_reasons.append(
            "LOW_MOMENTUM"
        )

    if (
        mom["acceleration_score"] <
        minimum_acceleration
    ):

        rejection_reasons.append(
            "LOW_ACCELERATION"
        )

    if (
        mom["exhaustion_score"] >
        maximum_exhaustion
    ):

        rejection_reasons.append(
            "EXHAUSTION"
        )

    if (
        mom["volume_ratio"] <
        minimum_volume
    ):

        rejection_reasons.append(
            "LOW_VOLUME"
        )

    if (
        mom["trend_score"] <
        minimum_trend
    ):

        rejection_reasons.append(
            "WEAK_TREND"
        )

    if (
        mom["trend_duration"] <
        minimum_duration
    ):

        rejection_reasons.append(
            "TREND_TOO_NEW"
        )

    if not higher["ok"]:

        rejection_reasons.append(
            "HIGHER_TF_NOT_CONFIRMED"
        )

    if (
        ENTRY_REQUIRE_TRIGGER
        and
        not trigger["ok"]
    ):

        rejection_reasons.append(
            "TRIGGER_NOT_CONFIRMED"
        )

    if (
        ENTRY_REQUIRE_CONFIRMATION
        and
        not mtf["ok"]
    ):

        rejection_reasons.append(
            "MTF_NOT_ALIGNED"
        )

    if (
        ENTRY_TIMING_ENABLED
        and
        candle_atr >
        ENTRY_MAX_CANDLE_ATR
    ):

        rejection_reasons.append(
            "CANDLE_TOO_EXTENDED"
        )

    if (
        regression_r2 <
        (
            SCALP_MIN_REGRESSION_R2
            if mode == "scalp"
            else OPPORTUNITY_MIN_REGRESSION_R2
        )
    ):

        rejection_reasons.append(
            "WEAK_REGRESSION"
        )

    if (
        mom["breakout_distance_atr"]
        >
        max_breakout
    ):

        rejection_reasons.append(
            "LATE_BREAKOUT"
        )

    approved = (
        len(rejection_reasons) == 0
    )

    return {

        "approved":
            approved,

        "reason":
            "APPROVED"
            if approved
            else ",".join(
                rejection_reasons
            ),

        "final_score":
            round(
                final_score,
                2
            ),

        "momentum":
            mom,

        "higher":
            higher,

        "trigger":
            trigger,

        "mtf":
            mtf,

        "regression_r2":
            round(
                regression_r2,
                4
            ),

        "regression_slope":
            regression_slope,

        "regression_message":
            regression_message,

        "candle_atr":
            round(
                candle_atr,
                3
            ),

        "rejection_reasons":
            rejection_reasons
    }


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

        atr = (
            entry_price *
            0.01
        )

    if p_type == "scalp":

        sl_mult = 1.7
        tp_mult = 2.6

    else:

        sl_mult = 2.4
        tp_mult = 5.0

    sl_dist = (
        atr *
        sl_mult
    )

    tp_dist = (
        atr *
        tp_mult
    )

    if side in [
        "buy",
        "long"
    ]:

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

    # Çok uç SL'yi engelle
    max_risk_pct = (
        70.0 /
        max(
            float(leverage),
            1.0
        )
    )

    if side in [
        "buy",
        "long"
    ]:

        max_zarar_fiyat = (
            entry_price *
            (
                1 -
                max_risk_pct / 100
            )
        )

    else:

        max_zarar_fiyat = (
            entry_price *
            (
                1 +
                max_risk_pct / 100
            )
        )

    return {

        "sl_price":
            float(sl_price),

        "tp_price":
            float(tp_price),

        "max_zarar_fiyat":
            float(max_zarar_fiyat),

        "atr_kullanilan":
            float(atr),

        "sl_atr":
            sl_mult,

        "tp_atr":
            tp_mult
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

    if df is None or len(df) < 5:
        return 50

    last = df.iloc[-2]

    score = 100

    direction = (
        "buy"
        if side in ["long", "buy"]
        else "sell"
    )

    if direction == "buy":

        if last["close"] < last["ema21"]:
            score -= 25

        if last["ema9"] < last["ema21"]:
            score -= 20

        if last["macd_hist"] < 0:
            score -= 20

        if (
            momentum_data.get(
                "exhaustion_score",
                0
            )
            >
            65
        ):

            score -= 20

    else:

        if last["close"] > last["ema21"]:
            score -= 25

        if last["ema9"] > last["ema21"]:
            score -= 20

        if last["macd_hist"] > 0:
            score -= 20

        if (
            momentum_data.get(
                "exhaustion_score",
                0
            )
            >
            65
        ):

            score -= 20

    if current_roi < -2:
        score -= 10

    return max(
        0,
        min(
            100,
            score
        )
    )


# ============================================================
# DYNAMIC PROFIT PROTECTION
# ============================================================

def dinamik_kar_koruma(
    exchange,
    symbol,
    side,
    contracts,
    roi,
    p_type,
    momentum
):

    if roi <= 0:
        return False

    max_roi = (
        pozisyon_max_roi.get(
            symbol,
            roi
        )
    )

    # ========================================================
    # SCALP
    # ========================================================

    if p_type == "scalp":

        if roi >= 3.0:

            # Zirveden %20 geri verme
            if (
                max_roi >= 3.0
                and
                roi <= max_roi - 0.60
            ):

                return pozisyon_kapat(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    "SCALP_TRAILING_PROFIT"
                )

        if (
            roi >=
            SCALP_PROFIT_LOCK_ROI
            and
            max_roi >=
            SCALP_PROFIT_LOCK_ROI
        ):

            if (
                momentum <
                60
            ):

                return pozisyon_kapat(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    "SCALP_MOMENTUM_WEAK_PROFIT_LOCK"
                )

    # ========================================================
    # OPPORTUNITY
    # ========================================================

    else:

        if roi >= 2.0:

            # Kâr 2%'yi gördükten sonra
            # ciddi momentum kaybı varsa çık.
            if (
                momentum < 55
                and
                roi > 0.8
            ):

                return pozisyon_kapat(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    "OPPORTUNITY_MOMENTUM_PROFIT_PROTECTION"
                )

        if roi >= 3.5:

            if (
                max_roi >= 3.5
                and
                roi <=
                max_roi - 1.0
            ):

                return pozisyon_kapat(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    "OPPORTUNITY_TRAILING_PROFIT"
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
            if side in [
                "buy",
                "long"
            ]
            else "buy"
        )

        try:

            exchange.cancel_all_orders(
                symbol
            )

        except Exception as e:

            logging.warning(
                f"[ORDER CANCEL] "
                f"{symbol}: {e}"
            )

        time.sleep(
            0.2
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

            symbol = sembol_duzelt(
                symbol
            )

            # =================================================
            # COOLDOWN
            # =================================================

            if cooldown_aktif_mi(
                symbol
            ):

                logging.info(
                    f"[COOLDOWN] "
                    f"{symbol} işlem açılmadı."
                )

                return False

            # =================================================
            # LOSS DIRECTION BLOCK
            # =================================================

            if zarar_sonrasi_yon_bloklu_mu(
                symbol,
                direction
            ):

                return False

            # =================================================
            # GERÇEK POZİSYON LİMİTİ
            # =================================================

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
                    f"{limit_reason}"
                )

                return False

            # =================================================
            # MARGIN
            # =================================================

            try:

                exchange.set_margin_mode(
                    "isolated",
                    symbol
                )

            except Exception as e:

                logging.warning(
                    f"[MARGIN] "
                    f"{symbol}: {e}"
                )

            # =================================================
            # LEVERAGE
            # =================================================

            try:

                exchange.set_leverage(
                    LEVERAGE,
                    symbol
                )

            except Exception as e:

                logging.warning(
                    f"[LEVERAGE] "
                    f"{symbol}: {e}"
                )

            # =================================================
            # PRICE
            # =================================================

            ticker = exchange.fetch_ticker(
                symbol
            )

            price = float(
                ticker["last"]
            )

            if price <= 0:
                return False

            # =================================================
            # MARGIN
            # =================================================

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

            # =================================================
            # MARKET
            # =================================================

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
                raw_amount <
                min_amount
            ):

                logging.warning(
                    f"[İŞLEM ENGELLENDİ] "
                    f"{symbol} minimum amount."
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

            # =================================================
            # SON GÜVENLİK POZİSYON KONTROLÜ
            # =================================================

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
                    f"{symbol} emir gönderilmedi | "
                    f"{final_reason}"
                )

                return False

            # =================================================
            # ORDER SIDE
            # =================================================

            side = (
                "buy"
                if direction == "buy"
                else "sell"
            )

            # =================================================
            # MARKET ORDER
            # =================================================

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
                    f"{symbol} boş order."
                )

                return False

            # =================================================
            # ACTUAL ENTRY
            # =================================================

            actual_entry = price

            try:

                order_average = (
                    order.get(
                        "average"
                    )
                )

                if order_average:

                    actual_entry = float(
                        order_average
                    )

            except Exception:
                pass

            # =================================================
            # STATE
            # =================================================

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

            pozisyon_max_roi[
                symbol
            ] = 0.0

            pozisyon_profit_lock[
                symbol
            ] = False

            pozisyon_acilis_zamanlari[
                symbol
            ] = time.time()

            cooldown_baslat(
                symbol
            )

            # =================================================
            # ATR
            # =================================================

            analysis_tf = (
                SCALP_SCAN_TIMEFRAME
                if p_type == "scalp"
                else OPPORTUNITY_SCAN_TIMEFRAME
            )

            df_temp = ohlcv_getir(
                exchange,
                symbol,
                analysis_tf,
                80
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

            # =================================================
            # TRADE PLAN
            # =================================================

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

            # =================================================
            # PROTECTIVE ORDERS
            # =================================================

            close_side = (
                "sell"
                if side == "buy"
                else "buy"
            )

            sl_ok = False
            tp_ok = False

            # =================================================
            # SL
            # =================================================

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

                        "reduceOnly":
                            True,

                        "workingType":
                            "MARK_PRICE"
                    }
                )

                sl_ok = True

            except Exception as sl_error:

                logging.error(
                    f"[SL HATASI] "
                    f"{symbol}: {sl_error}"
                )

            # =================================================
            # TP
            # =================================================

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

                        "reduceOnly":
                            True,

                        "workingType":
                            "MARK_PRICE"
                    }
                )

                tp_ok = True

            except Exception as tp_error:

                logging.error(
                    f"[TP HATASI] "
                    f"{symbol}: {tp_error}"
                )

            # =================================================
            # KORUMA EMİRLERİNDEN BİRİ YOKSA
            # =================================================

            if not sl_ok:

                logging.critical(
                    f"[KRİTİK] {symbol} "
                    f"SL oluşturulamadı!"
                )

            if not tp_ok:

                logging.warning(
                    f"[UYARI] {symbol} "
                    f"TP oluşturulamadı."
                )

            # =================================================
            # LOG
            # =================================================

            logging.info(
                f"[BAŞARILI İŞLEM] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{side.upper()} | "
                f"Margin={target_margin:.2f} | "
                f"Notional={notional:.2f} | "
                f"Lev={LEVERAGE}x | "
                f"Entry={actual_entry:.8f} | "
                f"SL={trade_plan['sl_price']:.8f} | "
                f"TP={trade_plan['tp_price']:.8f} | "
                f"Score={score:.2f}"
            )

            return True

        except Exception as e:

            logging.error(
                f"İşlem açma hata "
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
                p.get("contracts")
                or 0
            )
        ) > 0
    }

    # ========================================================
    # KAPANAN POZİSYONLARI TESPİT
    # ========================================================

    kapananlar = (
        onceki_aktif_pozisyonlar
        -
        aktif_semboller
    )

    for symbol in kapananlar:

        try:

            # State temizliği
            pozisyon_tipleri.pop(
                symbol,
                None
            )

            pozisyon_yonleri.pop(
                symbol,
                None
            )

            pozisyon_giris_fiyatlari.pop(
                symbol,
                None
            )

            pozisyon_acilis_zamanlari.pop(
                symbol,
                None
            )

            pozisyon_trade_plan.pop(
                symbol,
                None
            )

            pozisyon_saglik_loglari.pop(
                symbol,
                None
            )

            pozisyon_en_yuksek_kar.pop(
                symbol,
                None
            )

            pozisyon_max_roi.pop(
                symbol,
                None
            )

            pozisyon_profit_lock.pop(
                symbol,
                None
            )

        except Exception:
            pass

    # ========================================================
    # AKTİF POZİSYONLAR
    # ========================================================

    for p in positions:

        symbol = sembol_duzelt(
            p.get("symbol")
        )

        try:

            contracts = abs(
                float(
                    p.get("contracts")
                    or 0
                )
            )

            if contracts <= 0:
                continue

            side = p.get(
                "side"
            )

            entry_price = float(
                p.get("entryPrice")
                or 0
            )

            mark_price = float(
                p.get("markPrice")
                or 0
            )

            roi = float(
                p.get("percentage")
                or 0
            )

            p_type = (
                pozisyon_tipini_cozumle(
                    p
                )
            )

            # =================================================
            # MAX ROI
            # =================================================

            previous_max = (
                pozisyon_max_roi.get(
                    symbol,
                    roi
                )
            )

            if roi > previous_max:

                pozisyon_max_roi[
                    symbol
                ] = roi

                pozisyon_en_yuksek_kar[
                    symbol
                ] = roi

            # =================================================
            # TIMEFRAME
            # =================================================

            tf = (
                SCALP_SCAN_TIMEFRAME
                if p_type == "scalp"
                else OPPORTUNITY_SCAN_TIMEFRAME
            )

            df = ohlcv_getir(
                exchange,
                symbol,
                tf,
                80
            )

            if df is None:
                continue

            direction = (
                "buy"
                if side == "long"
                else "sell"
            )

            # =================================================
            # MOMENTUM
            # =================================================

            mom = (
                calculate_momentum_engine(
                    df,
                    direction
                )
            )

            current_momentum = float(
                mom["momentum_score"]
            )

            pozisyon_son_momentum[
                symbol
            ] = current_momentum

            # =================================================
            # HEALTH
            # =================================================

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

            # =================================================
            # MAX LOSS
            # =================================================

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
                        "MAX_LOSS"
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
                        "MAX_LOSS"
                    )

                    continue

            # =================================================
            # KÂR KORUMA
            # =================================================

            if (
                SCALP_EARLY_PROFIT_PROTECTION_ENABLED
                or
                OPPORTUNITY_MOMENTUM_EXIT_ENABLED
            ):

                closed = (
                    dinamik_kar_koruma(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        roi,
                        p_type,
                        current_momentum
                    )
                )

                if closed:
                    continue

            # =================================================
            # TRADE HEALTH CRITICAL
            # =================================================

            if (
                health_score < 30
                and
                roi > 0.5
            ):

                pozisyon_kapat(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    "TRADE_HEALTH_CRITICAL_PROFIT"
                )

                continue

            # =================================================
            # MAX HOLD
            # =================================================

            open_time = (
                pozisyon_acilis_zamanlari.get(
                    symbol
                )
            )

            if open_time:

                elapsed_minutes = (
                    time.time() -
                    open_time
                ) / 60

                if (
                    p_type == "scalp"
                    and
                    elapsed_minutes >
                    SCALP_MAX_HOLD_MINUTES
                    and
                    roi <= 0
                ):

                    pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "SCALP_MAX_HOLD"
                    )

                    continue

                if (
                    p_type == "opportunity"
                    and
                    elapsed_minutes >
                    OPPORTUNITY_MAX_HOLD_HOURS * 60
                ):

                    pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "OPPORTUNITY_MAX_HOLD"
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
                f"ROI={roi:.2f}% | "
                f"MAX={pozisyon_max_roi.get(symbol, 0):.2f}% | "
                f"MOM={current_momentum:.1f} | "
                f"HEALTH={health_score:.1f}"
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
                                )
                                or 0
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

        time.sleep(
            POSITION_MONITOR_INTERVAL
        )


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
# MARKET TICKERS
# ============================================================

def market_tickers_getir(
    exchange
):

    try:

        tickers = (
            exchange.fetch_tickers()
        )

        valid = []

        for t in tickers.values():

            symbol = t.get(
                "symbol"
            )

            percentage = t.get(
                "percentage"
            )

            if not gecerli_kripto_mu(
                symbol
            ):

                continue

            if percentage is None:
                continue

            try:

                pct = float(
                    percentage
                )

            except Exception:

                continue

            valid.append({
                "symbol":
                    sembol_duzelt(
                        symbol
                    ),

                "percentage":
                    pct
            })

        return valid

    except Exception as e:

        logging.error(
            f"Ticker tarama hatası: {e}"
        )

        return []


# ============================================================
# SCALP MARKET
# ============================================================

def scan_scalp_market(
    exchange
):

    try:

        markets = (
            market_tickers_getir(
                exchange
            )
        )

        gainers = sorted(
            markets,
            key=lambda x:
                x["percentage"],
            reverse=True
        )[:GAINER_COUNT]

        losers = sorted(
            markets,
            key=lambda x:
                x["percentage"]
        )[:LOSER_COUNT]

        candidates = []

        seen = set()

        # ====================================================
        # GAINERS -> LONG
        # ====================================================

        for item in gainers:

            symbol = item[
                "symbol"
            ]

            if symbol in seen:
                continue

            if cooldown_aktif_mi(
                symbol
            ):

                continue

            seen.add(
                symbol
            )

            df = ohlcv_getir(
                exchange,
                symbol,
                SCALP_SCAN_TIMEFRAME,
                120
            )

            if df is None:
                continue

            candidates.append({
                "symbol":
                    symbol,

                "direction":
                    "buy",

                "df":
                    df,

                "change":
                    item["percentage"]
            })

        # ====================================================
        # LOSERS -> SHORT
        # ====================================================

        for item in losers:

            symbol = item[
                "symbol"
            ]

            if symbol in seen:
                continue

            if cooldown_aktif_mi(
                symbol
            ):

                continue

            seen.add(
                symbol
            )

            df = ohlcv_getir(
                exchange,
                symbol,
                SCALP_SCAN_TIMEFRAME,
                120
            )

            if df is None:
                continue

            candidates.append({
                "symbol":
                    symbol,

                "direction":
                    "sell",

                "df":
                    df,

                "change":
                    item["percentage"]
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

def scan_opportunity_market(
    exchange
):

    try:

        markets = (
            market_tickers_getir(
                exchange
            )
        )

        gainers = sorted(
            markets,
            key=lambda x:
                x["percentage"],
            reverse=True
        )[:GAINER_COUNT]

        losers = sorted(
            markets,
            key=lambda x:
                x["percentage"]
        )[:LOSER_COUNT]

        candidates = []

        seen = set()

        # LONG
        for item in gainers:

            symbol = item[
                "symbol"
            ]

            if symbol in seen:
                continue

            if cooldown_aktif_mi(
                symbol
            ):

                continue

            seen.add(
                symbol
            )

            df = ohlcv_getir(
                exchange,
                symbol,
                OPPORTUNITY_SCAN_TIMEFRAME,
                150
            )

            if df is None:
                continue

            candidates.append({
                "symbol":
                    symbol,

                "direction":
                    "buy",

                "df":
                    df,

                "change":
                    item["percentage"]
            })

        # SHORT
        for item in losers:

            symbol = item[
                "symbol"
            ]

            if symbol in seen:
                continue

            if cooldown_aktif_mi(
                symbol
            ):

                continue

            seen.add(
                symbol
            )

            df = ohlcv_getir(
                exchange,
                symbol,
                OPPORTUNITY_SCAN_TIMEFRAME,
                150
            )

            if df is None:
                continue

            candidates.append({
                "symbol":
                    symbol,

                "direction":
                    "sell",

                "df":
                    df,

                "change":
                    item["percentage"]
            })

        return candidates

    except Exception as e:

        logging.error(
            f"Opportunity tarama hatası: {e}"
        )

        return []


# ============================================================
# MAIN SCAN
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
                "================================================"
            )

            logging.info(
                ">>> GELİŞMİŞ HİBRİT ANALİZ BAŞLADI <<<"
            )

            logging.info(
                "================================================"
            )

            # =================================================
            # BAŞLANGIÇ POZİSYON DURUMU
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
                    f"[BAŞLANGIÇ] "
                    f"Toplam={len(aktif_baslangic)}/"
                    f"{MAX_TOTAL_POSITIONS} | "
                    f"Scalp={scalp_count}/"
                    f"{MAX_SCALP_POSITIONS} | "
                    f"Opportunity={opportunity_count}/"
                    f"{MAX_OPPORTUNITY_POSITIONS}"
                )

                # =================================================
                # LİMİTLER DOLUYSA GEREKSİZ TARAMA YAPMA
                # =================================================

                if (
                    len(aktif_baslangic)
                    >=
                    MAX_TOTAL_POSITIONS
                ):

                    logging.info(
                        "[TARAMA] "
                        "Toplam pozisyon limiti dolu."
                    )

                    time.sleep(
                        300
                    )

                    continue

            # =================================================
            # OPPORTUNITY
            # =================================================

            firsat_listesi = (
                scan_opportunity_market(
                    exchange
                )
            )

            logging.info(
                f"[OPPORTUNITY] "
                f"{len(firsat_listesi)} aday."
            )

            # En iyi score'a göre sırala
            opportunity_evaluated = []

            for candidate in firsat_listesi:

                result = evaluate_entry(
                    exchange,
                    candidate["symbol"],
                    candidate["direction"],
                    "opportunity",
                    candidate["df"]
                )

                if result.get(
                    "approved"
                ):

                    opportunity_evaluated.append(
                        (
                            result[
                                "final_score"
                            ],
                            candidate,
                            result
                        )
                    )

                else:

                    logging.info(
                        f"[RED OPPORTUNITY] "
                        f"{candidate['symbol']} "
                        f"{candidate['direction']} | "
                        f"{result.get('reason')}"
                    )

            opportunity_evaluated.sort(
                key=lambda x:
                    x[0],
                reverse=True
            )

            for (
                score,
                candidate,
                result
            ) in opportunity_evaluated:

                success = pozisyon_ac(
                    exchange,
                    candidate["symbol"],
                    candidate["direction"],
                    score,
                    "opportunity",
                    str(result)
                )

                if success:

                    logging.info(
                        f"[OPPORTUNITY AÇILDI] "
                        f"{candidate['symbol']} | "
                        f"Score={score:.2f}"
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

            logging.info(
                f"[SCALP] "
                f"{len(scalp_listesi)} aday."
            )

            scalp_evaluated = []

            for candidate in scalp_listesi:

                result = evaluate_entry(
                    exchange,
                    candidate["symbol"],
                    candidate["direction"],
                    "scalp",
                    candidate["df"]
                )

                if result.get(
                    "approved"
                ):

                    scalp_evaluated.append(
                        (
                            result[
                                "final_score"
                            ],
                            candidate,
                            result
                        )
                    )

                else:

                    logging.info(
                        f"[RED SCALP] "
                        f"{candidate['symbol']} "
                        f"{candidate['direction']} | "
                        f"{result.get('reason')}"
                    )

            scalp_evaluated.sort(
                key=lambda x:
                    x[0],
                reverse=True
            )

            for (
                score,
                candidate,
                result
            ) in scalp_evaluated:

                success = pozisyon_ac(
                    exchange,
                    candidate["symbol"],
                    candidate["direction"],
                    score,
                    "scalp",
                    str(result)
                )

                if success:

                    logging.info(
                        f"[SCALP AÇILDI] "
                        f"{candidate['symbol']} | "
                        f"Score={score:.2f}"
                    )

                    break

            # =================================================
            # RAPOR
            # =================================================

            son_detayli_analiz_raporu = {

                "zaman":
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),

                "scalp_takip_listesi":
                    [
                        {
                            "symbol":
                                c["symbol"],

                            "direction":
                                c["direction"]
                        }
                        for c in scalp_listesi[:10]
                    ],

                "firsat_takip_listesi":
                    [
                        {
                            "symbol":
                                c["symbol"],

                            "direction":
                                c["direction"]
                        }
                        for c in firsat_listesi[:10]
                    ],

                "aktif_pozisyonlar_roi_durumu":
                    {
                        symbol:
                            pozisyon_max_roi.get(
                                symbol,
                                0
                            )
                        for symbol in
                        pozisyon_tipleri
                    },

                "yapilan_islemler":
                    [],

                "aciklamalar":
                    [
                        "Multi-timeframe trend teyidi aktif.",
                        "4H trend teyidi aktif.",
                        "Trend süresi kontrolü aktif.",
                        "Momentum acceleration kontrolü aktif.",
                        "Regression teyidi aktif.",
                        "Entry candle extension filtresi aktif.",
                        "Zarar sonrası aynı coin/yön bloklama aktif.",
                        "Dinamik kâr koruma aktif.",
                        "Gerçek Binance pozisyon limiti aktif."
                    ]
            }

        except Exception as e:

            logging.error(
                f"Ana döngü hatası: {e}"
            )

        finally:

            gc.collect()

        # =====================================================
        # 5 DAKİKA ANALİZ
        # MONITOR AYRI THREAD
        # =====================================================

        time.sleep(
            300
        )


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def index():

    return jsonify({

        "status":
            "Bot aktif - "
            "Advanced Risk Protected",

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
        },

        "risk_controls": {

            "loss_same_direction_block":
                LOSS_SAME_DIRECTION_BLOCK,

            "loss_block_hours":
                LOSS_REENTRY_BLOCK_HOURS,

            "cooldown_hours":
                COOLDOWN_HOURS,

            "monitor_interval":
                POSITION_MONITOR_INTERVAL
        }
    })


@app.route("/durum")
def durum():

    return jsonify({

        "success":
            True,

        "aktif_islem_sayisi":
            len(
                pozisyon_tipleri
            ),

        "saglik_durumlari":
            pozisyon_saglik_loglari,

        "momentum":
            pozisyon_son_momentum,

        "max_roi":
            pozisyon_max_roi,

        "trade_planlari":
            pozisyon_trade_plan,

        "loss_direction_blocks":
            loss_direction_block,

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

        "success":
            True,

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