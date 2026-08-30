import os
import ccxt
import pandas as pd
import numpy as np
import time
import threading
import gc
import logging
import json
from flask import Flask, jsonify

# ============================================================
# RAILWAY & BINANCE HİBRİT BOT
# SCALP + OPPORTUNITY
# MULTI-TIMEFRAME TREND + ENTRY TIMING + PROFIT PROTECTION
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

# ------------------------------------------------------------
# FİNANSAL KURALLAR
# ------------------------------------------------------------

SCALP_MARGIN = 10.0
OPPORTUNITY_MARGIN = 15.0

MAX_SCALP_POSITIONS = 1
MAX_OPPORTUNITY_POSITIONS = 1
MAX_TOTAL_POSITIONS = 2

LEVERAGE = 5

MARGIN_MODE = "isolated"

# ============================================================
# SCALP
# ============================================================

SCALP_TARGET_USDT = 0.35
SCALP_FEE_BUFFER_USDT = 0.04

# Net 0.35 USDT hedef için yaklaşık brüt hedef
SCALP_TARGET_ROI = (
    (SCALP_TARGET_USDT + SCALP_FEE_BUFFER_USDT)
    / SCALP_MARGIN
) * 100.0

SCALP_MAX_HOLD_MINUTES = 35

SCALP_EARLY_PROFIT_PROTECTION_ENABLED = True

# Kâr koruma sistemi
SCALP_PROFIT_ARM_ROI = 1.8
SCALP_PROFIT_LOCK_ROI = 1.20

SCALP_TRAILING_START_ROI = 2.5
SCALP_TRAILING_DISTANCE_ROI = 0.85

# ============================================================
# OPPORTUNITY
# ============================================================

OPPORTUNITY_TARGET_ROI = 6.0
OPPORTUNITY_TARGET_USDT = (
    OPPORTUNITY_MARGIN *
    OPPORTUNITY_TARGET_ROI /
    100.0
)

OPPORTUNITY_MAX_HOLD_HOURS = 24

OPPORTUNITY_PROFIT_ARM_ROI = 3.0
OPPORTUNITY_PROFIT_LOCK_ROI = 1.8

OPPORTUNITY_TRAILING_START_ROI = 4.5
OPPORTUNITY_TRAILING_DISTANCE_ROI = 1.5

OPPORTUNITY_MOMENTUM_EXIT_ENABLED = True

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

SCALP_MIN_ACCELERATION = 70
OPPORTUNITY_MIN_ACCELERATION = 68

SCALP_MAX_EXHAUSTION = 45
OPPORTUNITY_MAX_EXHAUSTION = 50

# ============================================================
# BREAKOUT / HACİM
# ============================================================

SCALP_MAX_BREAKOUT_ATR = 0.90
OPPORTUNITY_MAX_BREAKOUT_ATR = 1.40

IDEAL_BREAKOUT_ATR = 0.65

SCALP_MIN_VOLUME_RATIO = 1.25
OPPORTUNITY_MIN_VOLUME_RATIO = 1.40

MOMENTUM_ENGINE_ENABLED = True
ENTRY_TIMING_ENABLED = True

ENTRY_REQUIRE_TRIGGER = True
ENTRY_REQUIRE_CONFIRMATION = True

ENTRY_MAX_CANDLE_ATR = 1.50

# ============================================================
# TREND
# ============================================================

TREND_MIN_ADX = 20
TREND_STRONG_ADX = 25

TREND_MIN_DURATION_CANDLES = 3

# Aynı yönde çok uzamış trendi kovalamamak için
MAX_TREND_EXTENSION_ATR = 2.5

# ============================================================
# COOLDOWN
# ============================================================

COOLDOWN_HOURS = 4
LOSS_DIRECTION_BLOCK_HOURS = 24

cooldown_map = {}

# ------------------------------------------------------------
# Zararla kapanan işlemler:
# symbol -> {
#     direction,
#     timestamp,
#     roi
# }
# ------------------------------------------------------------

loss_direction_map = {}

LOSS_MEMORY_FILE = "trade_memory.json"

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

pozisyon_hedef_roi = {}
pozisyon_hedef_usdt = {}
pozisyon_kilit_roi = {}

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
# TRADE MEMORY
# ============================================================

def trade_memory_yukle():

    global loss_direction_map

    try:

        if not os.path.exists(
            LOSS_MEMORY_FILE
        ):
            return

        with open(
            LOSS_MEMORY_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        loss_direction_map = data.get(
            "loss_direction_map",
            {}
        )

        logging.info(
            f"[MEMORY] {len(loss_direction_map)} "
            f"zarar yön kaydı yüklendi."
        )

    except Exception as e:

        logging.warning(
            f"[MEMORY] Yükleme hatası: {e}"
        )


def trade_memory_kaydet():

    try:

        data = {
            "loss_direction_map":
                loss_direction_map
        }

        with open(
            LOSS_MEMORY_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                data,
                f,
                ensure_ascii=False,
                indent=2
            )

    except Exception as e:

        logging.warning(
            f"[MEMORY] Kaydetme hatası: {e}"
        )


def zararli_yon_bloklu_mu(
    symbol,
    direction
):

    record = loss_direction_map.get(
        symbol
    )

    if not record:
        return False

    if record.get("direction") != direction:
        return False

    timestamp = float(
        record.get("timestamp", 0)
    )

    elapsed = time.time() - timestamp

    if elapsed >= (
        LOSS_DIRECTION_BLOCK_HOURS * 3600
    ):

        return False

    remaining = (
        LOSS_DIRECTION_BLOCK_HOURS * 3600
        - elapsed
    ) / 3600

    logging.warning(
        f"[ZARAR YÖN BLOĞU] "
        f"{symbol} {direction.upper()} "
        f"| Kalan={remaining:.1f} saat"
    )

    return True


def zararli_yon_kaydet(
    symbol,
    direction,
    roi
):

    loss_direction_map[symbol] = {
        "direction": direction,
        "timestamp": time.time(),
        "roi": float(roi)
    }

    trade_memory_kaydet()

    logging.warning(
        f"[ZARAR KAYDI] "
        f"{symbol} {direction.upper()} "
        f"| ROI %{roi:.2f} "
        f"| Aynı yönde yeniden giriş "
        f"{LOSS_DIRECTION_BLOCK_HOURS} saat engellendi."
    )


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

    if (
        not symbol.endswith("/USDT")
        and
        "/USDT:" not in symbol
    ):
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

    son_islem = cooldown_map.get(
        symbol
    )

    if son_islem is None:
        return False

    return (
        now - son_islem
    ) < (
        COOLDOWN_HOURS * 3600
    )


def cooldown_baslat(symbol):

    cooldown_map[
        symbol
    ] = time.time()


# ============================================================
# POSITION TYPE
# ============================================================

def pozisyon_tipini_cozumle(p):

    sym = sembol_duzelt(
        p.get("symbol")
    )

    # --------------------------------------------------------
    # Önce RAM
    # --------------------------------------------------------

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
            p.get("leverage")
            or LEVERAGE
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

            # ------------------------------------------------
            # Kritik:
            # 10 USDT ve 15 USDT ayrımı
            # ------------------------------------------------

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
# GERÇEK BINANCE POZİSYONLARI
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

        return None


# ============================================================
# POZİSYON LİMİT KONTROL
# ============================================================

def pozisyon_limit_kontrol(
    exchange,
    yeni_tip,
    yeni_symbol
):

    aktif = (
        aktif_pozisyonlari_getir(
            exchange
        )
    )

    # Binance cevabı yoksa
    # kesinlikle işlem açma.
    if aktif is None:

        logging.warning(
            "[İŞLEM ENGELLENDİ] "
            "Gerçek Binance pozisyon verisi alınamadı."
        )

        return False, (
            "POSITION_DATA_UNAVAILABLE"
        )

    total = len(aktif)

    scalp_count = 0
    opportunity_count = 0

    active_symbols = set()

    # --------------------------------------------------------
    # GERÇEK POZİSYONLARI SAY
    # --------------------------------------------------------

    for p in aktif:

        symbol = sembol_duzelt(
            p.get("symbol")
        )

        active_symbols.add(
            symbol
        )

        p_type = (
            pozisyon_tipini_cozumle(
                p
            )
        )

        if p_type == "scalp":

            scalp_count += 1

        else:

            opportunity_count += 1

    # --------------------------------------------------------
    # TOPLAM
    # --------------------------------------------------------

    if total >= MAX_TOTAL_POSITIONS:

        logging.warning(
            f"[LİMİT] "
            f"Toplam={total}/"
            f"{MAX_TOTAL_POSITIONS}"
        )

        return False, (
            "MAX_TOTAL_POSITIONS"
        )

    # --------------------------------------------------------
    # AYNI SYMBOL
    # --------------------------------------------------------

    if yeni_symbol in active_symbols:

        logging.warning(
            f"[LİMİT] "
            f"{yeni_symbol} zaten açık."
        )

        return False, (
            "SYMBOL_ALREADY_OPEN"
        )

    # --------------------------------------------------------
    # SCALP
    # --------------------------------------------------------

    if yeni_tip == "scalp":

        if (
            scalp_count
            >=
            MAX_SCALP_POSITIONS
        ):

            logging.warning(
                f"[LİMİT] "
                f"Scalp={scalp_count}/"
                f"{MAX_SCALP_POSITIONS}"
            )

            return False, (
                "MAX_SCALP_POSITIONS"
            )

    # --------------------------------------------------------
    # OPPORTUNITY
    # --------------------------------------------------------

    if yeni_tip == "opportunity":

        if (
            opportunity_count
            >=
            MAX_OPPORTUNITY_POSITIONS
        ):

            logging.warning(
                f"[LİMİT] "
                f"Opportunity={opportunity_count}/"
                f"{MAX_OPPORTUNITY_POSITIONS}"
            )

            return False, (
                "MAX_OPPORTUNITY_POSITIONS"
            )

    logging.info(
        f"[POZİSYON LİMİT] "
        f"Toplam={total}/"
        f"{MAX_TOTAL_POSITIONS} | "
        f"Scalp={scalp_count}/"
        f"{MAX_SCALP_POSITIONS} | "
        f"Opportunity={opportunity_count}/"
        f"{MAX_OPPORTUNITY_POSITIONS} | "
        f"Yeni={yeni_tip.upper()} "
        f"{yeni_symbol} → İZİN"
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

        if (
            not data
            or
            len(data) < 60
        ):

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
            df["close"].ewm(
                span=9,
                adjust=False
            ).mean()
        )

        df["ema21"] = (
            df["close"].ewm(
                span=21,
                adjust=False
            ).mean()
        )

        df["ema50"] = (
            df["close"].ewm(
                span=50,
                adjust=False
            ).mean()
        )

        df["ema200"] = (
            df["close"].ewm(
                span=200,
                adjust=False
            ).mean()
        )

        # ====================================================
        # MACD
        # ====================================================

        exp12 = (
            df["close"].ewm(
                span=12,
                adjust=False
            ).mean()
        )

        exp26 = (
            df["close"].ewm(
                span=26,
                adjust=False
            ).mean()
        )

        df["macd"] = (
            exp12 - exp26
        )

        df["macd_signal"] = (
            df["macd"].ewm(
                span=9,
                adjust=False
            ).mean()
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
            gain.ewm(
                alpha=1 / 14,
                adjust=False
            ).mean()
        )

        avg_loss = (
            loss.ewm(
                alpha=1 / 14,
                adjust=False
            ).mean()
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
            tr.ewm(
                alpha=1 / 14,
                adjust=False
            ).mean()
        )

        # ====================================================
        # ADX
        # ====================================================

        up_move = df["high"].diff()
        down_move = -df["low"].diff()

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
            df["atr"].replace(
                0,
                np.nan
            )
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

        df["adx"] = (
            df["dx"].ewm(
                alpha=1 / 14,
                adjust=False
            ).mean()
        )

        # ====================================================
        # ROC
        # ====================================================

        df["roc"] = (
            df["close"].pct_change(9)
            * 100
        )

        # ====================================================
        # BOLLINGER
        # ====================================================

        sma20 = (
            df["close"].rolling(
                20
            ).mean()
        )

        std20 = (
            df["close"].rolling(
                20
            ).std()
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
            df["volume"].rolling(
                20
            ).mean()
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
            f"{symbol} {timeframe}: {e}"
        )

        return None


def son_kapanmis_mum(df):

    if (
        df is None
        or
        len(df) < 3
    ):

        return None

    return df.iloc[-2]


# ============================================================
# TREND DIRECTION
# ============================================================

def trend_direction_from_df(
    df
):

    if (
        df is None
        or
        len(df) < 10
    ):

        return "neutral"

    last = df.iloc[-2]

    bullish = (
        last["close"] > last["ema21"]
        and
        last["ema21"] > last["ema50"]
        and
        last["ema50"] >= last["ema200"]
    )

    bearish = (
        last["close"] < last["ema21"]
        and
        last["ema21"] < last["ema50"]
        and
        last["ema50"] <= last["ema200"]
    )

    if bullish:
        return "buy"

    if bearish:
        return "sell"

    return "neutral"


# ============================================================
# TREND DURATION
# ============================================================

def trend_duration_score(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 20
    ):

        return 0, 0

    d = df.iloc[:-1]

    lookback = min(
        8,
        len(d) - 2
    )

    count = 0

    for i in range(
        1,
        lookback + 1
    ):

        row = d.iloc[-i]

        if direction == "buy":

            aligned = (
                row["close"] > row["ema21"]
                and
                row["ema21"] > row["ema50"]
            )

        else:

            aligned = (
                row["close"] < row["ema21"]
                and
                row["ema21"] < row["ema50"]
            )

        if aligned:
            count += 1

    duration_score = min(
        100,
        count / max(
            lookback,
            1
        ) * 100
    )

    return (
        round(duration_score, 2),
        count
    )


# ============================================================
# TREND CONFIRMATION
# ============================================================

def multi_timeframe_trend_confirmation(
    exchange,
    symbol,
    direction,
    mode
):

    if mode == "scalp":

        middle_tf = "15m"

    else:

        middle_tf = "1h"

    df4 = ohlcv_getir(
        exchange,
        symbol,
        "4h",
        100
    )

    dfm = ohlcv_getir(
        exchange,
        symbol,
        middle_tf,
        120
    )

    if (
        df4 is None
        or
        dfm is None
    ):

        return {
            "ok": False,
            "score": 0,
            "reason": "MTF_DATA"
        }

    trend4 = trend_direction_from_df(
        df4
    )

    trendm = trend_direction_from_df(
        dfm
    )

    duration_score, duration = (
        trend_duration_score(
            dfm,
            direction
        )
    )

    adx = float(
        dfm.iloc[-2]["adx"]
    )

    if direction == "buy":

        aligned = (
            trend4 == "buy"
            and
            trendm == "buy"
        )

    else:

        aligned = (
            trend4 == "sell"
            and
            trendm == "sell"
        )

    if not aligned:

        return {
            "ok": False,
            "score": 30,
            "reason": (
                f"MTF UYUMSUZ "
                f"4H={trend4} "
                f"{middle_tf}={trendm}"
            ),
            "trend_4h": trend4,
            "trend_middle": trendm,
            "duration_score": duration_score,
            "duration": duration,
            "adx": adx
        }

    score = 65

    score += min(
        20,
        duration_score * 0.20
    )

    if adx >= TREND_STRONG_ADX:

        score += 15

    elif adx >= TREND_MIN_ADX:

        score += 8

    else:

        score -= 15

    score = max(
        0,
        min(
            100,
            score
        )
    )

    return {
        "ok": (
            score >= 72
            and
            adx >= TREND_MIN_ADX
            and
            duration >= TREND_MIN_DURATION_CANDLES
        ),

        "score": round(
            score,
            2
        ),

        "reason": "CONFIRMED",

        "trend_4h": trend4,
        "trend_middle": trendm,

        "duration_score":
            duration_score,

        "duration":
            duration,

        "adx":
            round(
                adx,
                2
            )
    }


# ============================================================
# REGRESSION
# ============================================================

def gelismis_regresyon_teyidi(
    df,
    direction,
    periyot=20
):

    if (
        df is None
        or
        len(df) <
        periyot + 2
    ):

        return (
            False,
            0.0,
            0.0,
            "Yetersiz veri."
        )

    closes = (
        df[
            "close"
        ].iloc[
            -periyot - 1:-1
        ].values
    )

    x = np.arange(
        len(closes)
    )

    slope, intercept = (
        np.polyfit(
            x,
            closes,
            1
        )
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
        (
            len(closes) - 1
        )
    )

    atr = float(
        df["atr"].iloc[-2]
    )

    if atr <= 0:

        atr = price * 0.01

    min_r2 = 0.55

    if direction == "buy":

        ok = (
            slope > 0
            and
            r_squared >= min_r2
            and
            price >=
            regression_mid -
            atr * 0.5
        )

    else:

        ok = (
            slope < 0
            and
            r_squared >= min_r2
            and
            price <=
            regression_mid +
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

def structure_score(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 30
    ):

        return 50

    d = df.iloc[:-1]

    last = d.iloc[-1]
    prev = d.iloc[-2]

    score = 50

    recent_high = (
        d[
            "high"
        ].iloc[
            -20:-1
        ].max()
    )

    recent_low = (
        d[
            "low"
        ].iloc[
            -20:-1
        ].min()
    )

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

def pullback_score(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 10
    ):

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

    if (
        df is None
        or
        len(df) < 25
    ):

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
        d[
            "high"
        ].iloc[
            -21:-1
        ].max()
    )

    previous_low = (
        d[
            "low"
        ].iloc[
            -21:-1
        ].min()
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

    # Son birkaç mum içinde breakout aranıyor.
    fresh = False

    for i in range(
        1,
        min(4, len(d) - 1)
    ):

        row = d.iloc[-i]

        if direction == "buy":

            if row["close"] > previous_high:

                fresh = True
                break

        else:

            if row["close"] < previous_low:

                fresh = True
                break

    quality = 50 if breakout else 30

    if fresh:
        quality += 20

    if (
        distance > 0
        and
        distance <= IDEAL_BREAKOUT_ATR
    ):
        quality += 20

    quality = min(
        100,
        quality
    )

    return {
        "breakout": bool(
            breakout
        ),

        "distance_atr": round(
            float(distance),
            3
        ),

        "fresh": bool(
            fresh
        ),

        "quality": quality
    }


# ============================================================
# CANDLE QUALITY
# ============================================================

def candle_quality(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 5
    ):

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
        direction == "buy"
        and
        last["close"] >
        last["open"]
    ):

        score += 25

    elif (
        direction == "sell"
        and
        last["close"] <
        last["open"]
    ):

        score += 25

    # Aşırı fitil
    if direction == "buy":

        if (
            last["upper_wick"] >
            last["body"] * 1.5
        ):

            score -= 15

    else:

        if (
            last["lower_wick"] >
            last["body"] * 1.5
        ):

            score -= 15

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

    if (
        df is None
        or
        len(df) < 30
    ):

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

    # ========================================================
    # MOMENTUM
    # ========================================================

    momentum = 50

    if direction == "buy":

        if last["ema9"] > last["ema21"]:
            momentum += 18

        if last["macd_hist"] > 0:
            momentum += 15

        if (
            last["macd_hist"] >
            prev["macd_hist"]
        ):
            momentum += 8

        if last["rsi"] > 50:
            momentum += 10

        if last["close"] > last["vwap"]:
            momentum += 7

    else:

        if last["ema9"] < last["ema21"]:
            momentum += 18

        if last["macd_hist"] < 0:
            momentum += 15

        if (
            last["macd_hist"] <
            prev["macd_hist"]
        ):
            momentum += 8

        if last["rsi"] < 50:
            momentum += 10

        if last["close"] < last["vwap"]:
            momentum += 7

    momentum = min(
        max(momentum, 0),
        100
    )

    # ========================================================
    # ACCELERATION
    # ========================================================

    adx_now = float(
        last["adx"]
    )

    adx_prev = float(
        prev["adx"]
    )

    acceleration = 50

    if adx_now >= TREND_STRONG_ADX:
        acceleration += 20

    elif adx_now >= TREND_MIN_ADX:
        acceleration += 10

    if adx_now > adx_prev:
        acceleration += 15

    if direction == "buy":

        if (
            last["ema9"] >
            prev["ema9"]
        ):
            acceleration += 8

    else:

        if (
            last["ema9"] <
            prev["ema9"]
        ):
            acceleration += 8

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
            ) * 35
        )
    )

    # ========================================================
    # DİĞER
    # ========================================================

    breakout = (
        breakout_analysis(
            df,
            direction
        )
    )

    structure = (
        structure_score(
            df,
            direction
        )
    )

    pullback = (
        pullback_score(
            df,
            direction
        )
    )

    candle = (
        candle_quality(
            df,
            direction
        )
    )

    # ========================================================
    # EXHAUSTION
    # ========================================================

    exhaustion = 10

    if direction == "buy":

        if last["rsi"] > 70:
            exhaustion += 20

        if last["rsi"] > 77:
            exhaustion += 25

        if (
            last["close"] -
            last["ema21"]
        ) > (
            last["atr"] * 2.0
        ):

            exhaustion += 20

        if (
            breakout[
                "distance_atr"
            ] >
            0.90
        ):

            exhaustion += 15

    else:

        if last["rsi"] < 30:
            exhaustion += 20

        if last["rsi"] < 23:
            exhaustion += 25

        if (
            last["ema21"] -
            last["close"]
        ) > (
            last["atr"] * 2.0
        ):

            exhaustion += 20

        if (
            breakout[
                "distance_atr"
            ] >
            0.90
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

        if (
            last["macd_hist"] > 0
        ):
            trigger += 15

        if (
            last["close"] >
            prev["close"]
        ):
            trigger += 10

    else:

        if (
            last["ema9"] <
            last["ema21"]
        ):
            trigger += 15

        if (
            last["macd_hist"] < 0
        ):
            trigger += 15

        if (
            last["close"] <
            prev["close"]
        ):
            trigger += 10

    trigger = min(
        max(trigger, 0),
        100
    )

    # ========================================================
    # ENTRY SCORE
    # ========================================================

    entry_score = (
        momentum * 0.25 +
        acceleration * 0.20 +
        volume_score * 0.15 +
        structure * 0.15 +
        pullback * 0.10 +
        candle * 0.05 +
        trigger * 0.10
    )

    # Exhaustion cezası
    if exhaustion > 60:

        entry_score -= 12

    elif exhaustion > 45:

        entry_score -= 6

    entry_score = min(
        max(entry_score, 0),
        100
    )

    if momentum >= 80:

        state = "STRONG"

    elif momentum >= 68:

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
            structure,

        "trigger_score":
            round(
                trigger,
                2
            )
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
            "state": "DATA_ERROR"
        }

    d = df.iloc[:-1]

    if len(d) < 5:

        return {
            "ok": False,
            "score": 0,
            "state": "DATA_ERROR"
        }

    last = d.iloc[-1]
    prev = d.iloc[-2]

    score = 50

    if direction == "buy":

        if (
            last["ema9"] >
            last["ema21"]
        ):
            score += 15

        if (
            last["macd_hist"] > 0
        ):
            score += 15

        if (
            last["close"] >
            prev["close"]
        ):
            score += 10

        if (
            last["close"] >
            last["open"]
        ):
            score += 10

    else:

        if (
            last["ema9"] <
            last["ema21"]
        ):
            score += 15

        if (
            last["macd_hist"] < 0
        ):
            score += 15

        if (
            last["close"] <
            prev["close"]
        ):
            score += 10

        if (
            last["close"] <
            last["open"]
        ):
            score += 10

    return {
        "ok": score >= 80,
        "score": score,
        "state":
            "CONFIRMED"
            if score >= 80
            else "WAIT"
    }


# ============================================================
# ENTRY TIMING
# ============================================================

def entry_timing_analysis(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 20
    ):

        return {
            "ok": False,
            "score": 0,
            "reason": "DATA"
        }

    d = df.iloc[:-1]

    last = d.iloc[-1]

    atr = float(
        last["atr"]
    )

    if atr <= 0:

        return {
            "ok": False,
            "score": 0,
            "reason": "ATR"
        }

    candle_size = (
        last["range"] /
        atr
    )

    # Büyük mumun peşinden girme
    if (
        candle_size >
        ENTRY_MAX_CANDLE_ATR
    ):

        return {
            "ok": False,
            "score": 25,
            "reason":
                "CANDLE_TOO_LARGE"
        }

    exhaustion = 0

    if direction == "buy":

        if last["rsi"] > 70:
            exhaustion += 25

        if last["close"] > (
            last["ema21"] +
            atr * MAX_TREND_EXTENSION_ATR
        ):
            exhaustion += 40

    else:

        if last["rsi"] < 30:
            exhaustion += 25

        if last["close"] < (
            last["ema21"] -
            atr * MAX_TREND_EXTENSION_ATR
        ):
            exhaustion += 40

    score = 80

    if candle_size <= 1.0:
        score += 10

    if exhaustion > 40:
        score -= 30

    score = min(
        max(score, 0),
        100
    )

    return {
        "ok": (
            score >= 70
            and
            exhaustion < 40
        ),

        "score":
            score,

        "candle_atr":
            round(
                candle_size,
                2
            ),

        "exhaustion":
            exhaustion
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

    # --------------------------------------------------------
    # 1. Ana momentum
    # --------------------------------------------------------

    mom = (
        calculate_momentum_engine(
            df,
            direction
        )
    )

    # --------------------------------------------------------
    # 2. Multi timeframe trend
    # --------------------------------------------------------

    trend = (
        multi_timeframe_trend_confirmation(
            exchange,
            symbol,
            direction,
            mode
        )
    )

    if not trend["ok"]:

        return {
            "approved": False,
            "reason":
                "MTF_TREND_REJECT",
            "momentum":
                mom,
            "trend":
                trend
        }

    # --------------------------------------------------------
    # 3. Trigger
    # --------------------------------------------------------

    if mode == "scalp":

        trigger_tf = (
            SCALP_TRIGGER_TIMEFRAME
        )

    else:

        trigger_tf = (
            OPPORTUNITY_TRIGGER_TIMEFRAME
        )

    trigger = (
        trigger_confirmation(
            exchange,
            symbol,
            direction,
            trigger_tf
        )
    )

    if (
        ENTRY_REQUIRE_TRIGGER
        and
        not trigger["ok"]
    ):

        return {
            "approved": False,
            "reason":
                "TRIGGER_WAIT",
            "momentum":
                mom,
            "trend":
                trend,
            "trigger":
                trigger
        }

    # --------------------------------------------------------
    # 4. Entry timing
    # --------------------------------------------------------

    timing = (
        entry_timing_analysis(
            df,
            direction
        )
    )

    if (
        ENTRY_TIMING_ENABLED
        and
        not timing["ok"]
    ):

        return {
            "approved": False,
            "reason":
                "BAD_ENTRY_TIMING",
            "momentum":
                mom,
            "trend":
                trend,
            "trigger":
                trigger,
            "timing":
                timing
        }

    # --------------------------------------------------------
    # 5. Regression
    # --------------------------------------------------------

    (
        regression_ok,
        slope,
        r2,
        regression_message
    ) = gelismis_regresyon_teyidi(
        df,
        direction
    )

    if not regression_ok:

        return {
            "approved": False,
            "reason":
                "REGRESSION_REJECT",
            "momentum":
                mom,
            "trend":
                trend,
            "trigger":
                trigger,
            "timing":
                timing,
            "regression_r2":
                r2
        }

    # --------------------------------------------------------
    # 6. Minimum momentum
    # --------------------------------------------------------

    if mode == "scalp":

        min_momentum = (
            SCALP_MIN_MOMENTUM
        )

        min_acceleration = (
            SCALP_MIN_ACCELERATION
        )

        max_exhaustion = (
            SCALP_MAX_EXHAUSTION
        )

        minimum = (
            SCALP_MIN_FINAL_SCORE
        )

        min_volume = (
            SCALP_MIN_VOLUME_RATIO
        )

    else:

        min_momentum = (
            OPPORTUNITY_MIN_MOMENTUM
        )

        min_acceleration = (
            OPPORTUNITY_MIN_ACCELERATION
        )

        max_exhaustion = (
            OPPORTUNITY_MAX_EXHAUSTION
        )

        minimum = (
            OPPORTUNITY_MIN_FINAL_SCORE
        )

        min_volume = (
            OPPORTUNITY_MIN_VOLUME_RATIO
        )

    # --------------------------------------------------------
    # 7. Sert filtreler
    # --------------------------------------------------------

    if (
        mom["momentum_score"]
        <
        min_momentum
    ):

        return {
            "approved": False,
            "reason":
                "MOMENTUM_LOW",
            "momentum":
                mom
        }

    if (
        mom["acceleration_score"]
        <
        min_acceleration
    ):

        return {
            "approved": False,
            "reason":
                "ACCELERATION_LOW",
            "momentum":
                mom
        }

    if (
        mom["exhaustion_score"]
        >
        max_exhaustion
    ):

        return {
            "approved": False,
            "reason":
                "EXHAUSTION_HIGH",
            "momentum":
                mom
        }

    if (
        mom["volume_ratio"]
        <
        min_volume
    ):

        return {
            "approved": False,
            "reason":
                "VOLUME_LOW",
            "momentum":
                mom
        }

    # --------------------------------------------------------
    # 8. Final score
    # --------------------------------------------------------

    final_score = (
        mom["entry_score"] * 0.45 +
        mom["momentum_score"] * 0.20 +
        trend["score"] * 0.20 +
        trigger["score"] * 0.10 +
        timing["score"] * 0.05
    )

    # Trend exhaustion cezası
    if (
        mom["exhaustion_score"]
        >
        35
    ):

        final_score -= 5

    final_score = min(
        max(
            final_score,
            0
        ),
        100
    )

    approved = (
        final_score >= minimum
    )

    return {

        "approved":
            approved,

        "reason":
            "APPROVED"
            if approved
            else "LOW_FINAL_SCORE",

        "final_score":
            round(
                final_score,
                2
            ),

        "momentum":
            mom,

        "trend":
            trend,

        "trigger":
            trigger,

        "timing":
            timing,

        "regression_r2":
            round(
                r2,
                3
            ),

        "regression_slope":
            float(
                slope
            ),

        "regression_message":
            regression_message
    }


# ============================================================
# MARKET DIRECTION POOL
# ============================================================

def market_pool_getir(
    exchange
):

    try:

        tickers = (
            exchange.fetch_tickers()
        )

        usdt_tickers = []

        for t in tickers.values():

            symbol = t.get(
                "symbol"
            )

            percentage = t.get(
                "percentage"
            )

            if (
                not gecerli_kripto_mu(
                    symbol
                )
                or
                percentage is None
            ):

                continue

            try:

                percentage = float(
                    percentage
                )

            except Exception:

                continue

            t["_pct"] = percentage

            usdt_tickers.append(t)

        gainers = sorted(
            usdt_tickers,
            key=lambda x:
                x["_pct"],
            reverse=True
        )[:GAINER_COUNT]

        losers = sorted(
            usdt_tickers,
            key=lambda x:
                x["_pct"]
        )[:LOSER_COUNT]

        return gainers, losers

    except Exception as e:

        logging.error(
            f"Market pool hata: {e}"
        )

        return [], []


# ============================================================
# SCALP SCAN
# ============================================================

def scan_scalp_market(
    exchange
):

    try:

        gainers, losers = (
            market_pool_getir(
                exchange
            )
        )

        candidates = []

        # ----------------------------------------------------
        # Gainers → LONG adayları
        # Losers → SHORT adayları
        # ----------------------------------------------------

        pools = []

        for t in gainers:

            pools.append(
                (
                    t["symbol"],
                    "buy"
                )
            )

        for t in losers:

            pools.append(
                (
                    t["symbol"],
                    "sell"
                )
            )

        # Aynı symbol tekrarı engellenir
        seen = set()

        for symbol, direction in pools:

            symbol = sembol_duzelt(
                symbol
            )

            if symbol in seen:
                continue

            seen.add(symbol)

            if cooldown_aktif_mi(
                symbol
            ):
                continue

            if zararli_yon_bloklu_mu(
                symbol,
                direction
            ):
                continue

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
                    direction,

                "df":
                    df
            })

        return candidates

    except Exception as e:

        logging.error(
            f"Scalp tarama hatası: {e}"
        )

        return []


# ============================================================
# OPPORTUNITY SCAN
# ============================================================

def scan_opportunity_market(
    exchange
):

    try:

        gainers, losers = (
            market_pool_getir(
                exchange
            )
        )

        candidates = []

        pools = []

        for t in gainers:

            pools.append(
                (
                    t["symbol"],
                    "buy"
                )
            )

        for t in losers:

            pools.append(
                (
                    t["symbol"],
                    "sell"
                )
            )

        seen = set()

        for symbol, direction in pools:

            symbol = sembol_duzelt(
                symbol
            )

            if symbol in seen:
                continue

            seen.add(symbol)

            if cooldown_aktif_mi(
                symbol
            ):
                continue

            if zararli_yon_bloklu_mu(
                symbol,
                direction
            ):
                continue

            df = ohlcv_getir(
                exchange,
                symbol,
                OPPORTUNITY_SCAN_TIMEFRAME,
                120
            )

            if df is None:
                continue

            candidates.append({
                "symbol":
                    symbol,

                "direction":
                    direction,

                "df":
                    df
            })

        return candidates

    except Exception as e:

        logging.error(
            f"Opportunity tarama hatası: {e}"
        )

        return []


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

        sl_mult = 1.8
        tp_mult = 2.2

        target_roi = (
            SCALP_TARGET_ROI
        )

        target_usdt = (
            SCALP_TARGET_USDT
        )

    else:

        sl_mult = 2.5
        tp_mult = 5.0

        target_roi = (
            OPPORTUNITY_TARGET_ROI
        )

        target_usdt = (
            OPPORTUNITY_TARGET_USDT
        )

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

    # --------------------------------------------------------
    # Güvenlik sınırı
    # --------------------------------------------------------

    max_risk_pct = (
        75.0 /
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
                1.0 -
                max_risk_pct /
                100
            )
        )

    else:

        max_zarar_fiyat = (
            entry_price *
            (
                1.0 +
                max_risk_pct /
                100
            )
        )

    return {

        "sl_price":
            float(
                sl_price
            ),

        "tp_price":
            float(
                tp_price
            ),

        "max_zarar_fiyat":
            float(
                max_zarar_fiyat
            ),

        "atr_kullanilan":
            float(
                atr
            ),

        "target_roi":
            float(
                target_roi
            ),

        "target_usdt":
            float(
                target_usdt
            )
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

    if (
        df is None
        or
        len(df) < 5
    ):

        return 50

    last = df.iloc[-2]

    if side in [
        "buy",
        "long"
    ]:

        if last["close"] < last["ema21"]:
            score -= 25

        if last["macd_hist"] < 0:
            score -= 20

        if (
            momentum_data.get(
                "exhaustion_score",
                0
            )
            > 60
        ):

            score -= 25

    else:

        if last["close"] > last["ema21"]:
            score -= 25

        if last["macd_hist"] > 0:
            score -= 20

        if (
            momentum_data.get(
                "exhaustion_score",
                0
            )
            > 60
        ):

            score -= 25

    if current_roi < -3.0:
        score -= 20

    return max(
        0,
        min(
            100,
            score
        )
    )


# ============================================================
# PROFIT PROTECTION
# ============================================================

def kar_koruma_seviyesi(
    symbol,
    p_type,
    max_roi
):

    if p_type == "scalp":

        arm = (
            SCALP_PROFIT_ARM_ROI
        )

        lock = (
            SCALP_PROFIT_LOCK_ROI
        )

        trail_start = (
            SCALP_TRAILING_START_ROI
        )

        trail_distance = (
            SCALP_TRAILING_DISTANCE_ROI
        )

    else:

        arm = (
            OPPORTUNITY_PROFIT_ARM_ROI
        )

        lock = (
            OPPORTUNITY_PROFIT_LOCK_ROI
        )

        trail_start = (
            OPPORTUNITY_TRAILING_START_ROI
        )

        trail_distance = (
            OPPORTUNITY_TRAILING_DISTANCE_ROI
        )

    if max_roi < arm:

        return None

    if max_roi >= trail_start:

        protection = (
            max_roi -
            trail_distance
        )

    else:

        protection = lock

    return round(
        max(
            protection,
            0
        ),
        2
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
                    f"[COOLDOWN] "
                    f"{symbol}"
                )

                return False

            # ------------------------------------------------
            # LOSS DIRECTION
            # ------------------------------------------------

            if zararli_yon_bloklu_mu(
                symbol,
                direction
            ):

                logging.warning(
                    f"[İŞLEM ENGELLENDİ] "
                    f"{symbol} {direction.upper()} "
                    f"| Önceki işlem aynı yönde zararla kapandı."
                )

                return False

            # ------------------------------------------------
            # GERÇEK POZİSYON KONTROL
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
                    f"{limit_reason}"
                )

                return False

            # ------------------------------------------------
            # MARGIN MODE
            # ------------------------------------------------

            try:

                exchange.set_margin_mode(
                    MARGIN_MODE,
                    symbol
                )

            except Exception as e:

                # Zaten isolated ise Binance
                # hata döndürebilir.
                logging.info(
                    f"[MARGIN] "
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
                    f"[LEVERAGE] "
                    f"{symbol}: {e}"
                )

            # ------------------------------------------------
            # FİYAT
            # ------------------------------------------------

            ticker = (
                exchange.fetch_ticker(
                    symbol
                )
            )

            price = float(
                ticker["last"]
            )

            if price <= 0:

                return False

            # ------------------------------------------------
            # MARGIN
            # ------------------------------------------------

            target_margin = (
                OPPORTUNITY_MARGIN
                if p_type ==
                "opportunity"
                else
                SCALP_MARGIN
            )

            # EXACT TARGET NOTIONAL
            notional = (
                target_margin *
                LEVERAGE
            )

            raw_amount = (
                notional /
                price
            )

            market = (
                exchange.market(
                    symbol
                )
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
            # EMİR ÖNCESİ SON POZİSYON KONTROLÜ
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
                    "leverage":
                        LEVERAGE
                }
            )

            if not order:

                logging.error(
                    f"[EMİR HATASI] "
                    f"{symbol}"
                )

                return False

            # ------------------------------------------------
            # GERÇEK ENTRY
            # ------------------------------------------------

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

            # ------------------------------------------------
            # ATR
            # ------------------------------------------------

            df_temp = ohlcv_getir(
                exchange,
                symbol,
                SCALP_SCAN_TIMEFRAME,
                60
            )

            if df_temp is not None:

                atr = float(
                    df_temp.iloc[-2][
                        "atr"
                    ]
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

            pozisyon_trade_plan[
                symbol
            ] = trade_plan

            pozisyon_hedef_roi[
                symbol
            ] = trade_plan[
                "target_roi"
            ]

            pozisyon_hedef_usdt[
                symbol
            ] = trade_plan[
                "target_usdt"
            ]

            pozisyon_kilit_roi[
                symbol
            ] = 0.0

            cooldown_baslat(
                symbol
            )

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

                        "reduceOnly":
                            True,

                        "workingType":
                            "MARK_PRICE"
                    }
                )

            except Exception as sl_error:

                logging.error(
                    f"[SL HATASI] "
                    f"{symbol}: "
                    f"{sl_error}"
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

                        "reduceOnly":
                            True,

                        "workingType":
                            "MARK_PRICE"
                    }
                )

            except Exception as tp_error:

                logging.error(
                    f"[TP HATASI] "
                    f"{symbol}: "
                    f"{tp_error}"
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
                f"Hedef ROI=%"
                f"{trade_plan['target_roi']:.2f} | "
                f"Hedef Net={trade_plan['target_usdt']:.2f} USDT | "
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
# POSITION CLOSE
# ============================================================

def pozisyon_kapat(
    exchange,
    symbol,
    side,
    contracts,
    reason,
    roi=None
):

    try:

        close_side = (
            "sell"
            if side in [
                "buy",
                "long"
            ]
            else
            "buy"
        )

        # ----------------------------------------------------
        # Kapanmadan önce mevcut zararı/kârı kaydet
        # ----------------------------------------------------

        if roi is not None:

            try:

                roi_float = float(
                    roi
                )

                direction = (
                    "buy"
                    if side in [
                        "buy",
                        "long"
                    ]
                    else
                    "sell"
                )

                if roi_float < 0:

                    zararli_yon_kaydet(
                        symbol,
                        direction,
                        roi_float
                    )

            except Exception:
                pass

        # ----------------------------------------------------
        # Önce açık koruyucu emirleri iptal et
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # MARKET CLOSE
        # ----------------------------------------------------

        exchange.create_order(
            symbol,
            "market",
            close_side,
            contracts,
            None,
            {
                "reduceOnly":
                    True
            }
        )

        logging.warning(
            f"[POZİSYON KAPATILDI] "
            f"{symbol} | "
            f"Neden={reason} | "
            f"ROI={roi}"
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
                p.get("contracts")
                or 0
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
                p.get(
                    "entryPrice"
                )
                or 0
            )

            mark_price = float(
                p.get(
                    "markPrice"
                )
                or 0
            )

            roi = float(
                p.get(
                    "percentage"
                )
                or 0
            )

            p_type = (
                pozisyon_tipini_cozumle(
                    p
                )
            )

            # =================================================
            # HEDEF
            # =================================================

            if p_type == "scalp":

                target_roi = (
                    SCALP_TARGET_ROI
                )

                target_usdt = (
                    SCALP_TARGET_USDT
                )

            else:

                target_roi = (
                    OPPORTUNITY_TARGET_ROI
                )

                target_usdt = (
                    OPPORTUNITY_TARGET_USDT
                )

            pozisyon_hedef_roi[
                symbol
            ] = target_roi

            pozisyon_hedef_usdt[
                symbol
            ] = target_usdt

            # =================================================
            # ZİRVE ROI
            # =================================================

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

            max_roi = (
                pozisyon_en_yuksek_kar.get(
                    symbol,
                    roi
                )
            )

            # =================================================
            # KÂR KORUMA
            # =================================================

            protection = (
                kar_koruma_seviyesi(
                    symbol,
                    p_type,
                    max_roi
                )
            )

            if protection is not None:

                pozisyon_kilit_roi[
                    symbol
                ] = protection

            else:

                pozisyon_kilit_roi[
                    symbol
                ] = 0.0

            locked_roi = (
                pozisyon_kilit_roi.get(
                    symbol,
                    0.0
                )
            )

            # -------------------------------------------------
            # Kâr kilidi tetiklenmişse
            # -------------------------------------------------

            if (
                locked_roi > 0
                and
                roi <= locked_roi
                and
                max_roi >=
                (
                    locked_roi + 0.15
                )
            ):

                pozisyon_kapat(
                    exchange,
                    symbol,
                    side,
                    contracts,
                    (
                        "PROFIT_LOCK "
                        f"Max={max_roi:.2f}% "
                        f"Lock={locked_roi:.2f}%"
                    ),
                    roi
                )

                continue

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
                        "MAX_LOSS",
                        roi
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
                        "MAX_LOSS",
                        roi
                    )

                    continue

            # =================================================
            # TRADE HEALTH
            # =================================================

            df = ohlcv_getir(
                exchange,
                symbol,
                SCALP_SCAN_TIMEFRAME,
                60
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

                # ------------------------------------------------
                # Pozitif işlemde momentum tamamen bozulursa
                # ------------------------------------------------

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
                        "TRADE_HEALTH_CRITICAL",
                        roi
                    )

                    continue

            else:

                mom = {}

            # =================================================
            # HOLD TIME
            # =================================================

            opened_at = (
                pozisyon_acilis_zamanlari.get(
                    symbol
                )
            )

            if opened_at:

                elapsed_minutes = (
                    time.time() -
                    opened_at
                ) / 60.0

                if p_type == "scalp":

                    if (
                        elapsed_minutes
                        >=
                        SCALP_MAX_HOLD_MINUTES
                        and
                        roi <= 0
                    ):

                        pozisyon_kapat(
                            exchange,
                            symbol,
                            side,
                            contracts,
                            "SCALP_TIME_LIMIT",
                            roi
                        )

                        continue

                else:

                    elapsed_hours = (
                        elapsed_minutes /
                        60.0
                    )

                    if (
                        elapsed_hours
                        >=
                        OPPORTUNITY_MAX_HOLD_HOURS
                    ):

                        pozisyon_kapat(
                            exchange,
                            symbol,
                            side,
                            contracts,
                            "OPPORTUNITY_TIME_LIMIT",
                            roi
                        )

                        continue

            # =================================================
            # HEDEF KÂR GÖSTERİMİ
            # =================================================

            logging.info(
                f"[POZİSYON] "
                f"{symbol} | "
                f"{p_type.upper()} | "
                f"{side.upper()} | "
                f"ROI=%{roi:.2f} | "
                f"HEDEF=%{target_roi:.2f} | "
                f"HEDEF NET={target_usdt:.2f} USDT | "
                f"ZİRVE=%{max_roi:.2f} | "
                f"KİLİT=%{locked_roi:.2f}"
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

                exchange = (
                    get_exchange()
                )

                exchange.load_markets()

            if (
                pozisyon_monitor_lock.acquire(
                    blocking=False
                )
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

        # ----------------------------------------------------
        # Pozisyon yönetimi analiz döngüsünden bağımsız.
        # ----------------------------------------------------

        time.sleep(
            15.0
        )


def monitor_baslat():

    if POSITION_MONITOR_ENABLED:

        threading.Thread(
            target=pozisyon_monitor_loop,
            daemon=True,
            name="PositionMonitor"
        ).start()


# ============================================================
# ANA TARAMA
# ============================================================

def ana_tarama_dongusu():

    global son_detayli_analiz_raporu

    monitor_baslat()

    trade_memory_yukle()

    while True:

        exchange = None

        try:

            exchange = (
                get_exchange()
            )

            exchange.load_markets()

            logging.info(
                "================================================"
            )

            logging.info(
                ">>> GELİŞMİŞ HİBRİT ANALİZ BAŞLADI <<<"
            )

            logging.info(
                f"[AYAR] "
                f"Scalp Margin={SCALP_MARGIN} | "
                f"Opportunity Margin={OPPORTUNITY_MARGIN} | "
                f"Leverage={LEVERAGE}x | "
                f"Max Total={MAX_TOTAL_POSITIONS}"
            )

            # =================================================
            # GERÇEK BAŞLANGIÇ POZİSYON
            # =================================================

            aktif_baslangic = (
                aktif_pozisyonlari_getir(
                    exchange
                )
            )

            if aktif_baslangic is None:

                logging.warning(
                    "[ANALİZ DURDU] "
                    "Pozisyon verisi alınamadı."
                )

                time.sleep(
                    300
                )

                continue

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
            # OPPORTUNITY
            # =================================================

            if (
                opportunity_count
                <
                MAX_OPPORTUNITY_POSITIONS
                and
                len(aktif_baslangic)
                <
                MAX_TOTAL_POSITIONS
            ):

                firsat_listesi = (
                    scan_opportunity_market(
                        exchange
                    )
                )

                logging.info(
                    f"[OPPORTUNITY] "
                    f"{len(firsat_listesi)} aday."
                )

                opportunity_opened = False

                for candidate in (
                    firsat_listesi
                ):

                    # ------------------------------------------------
                    # Son gerçek pozisyon kontrolü
                    # ------------------------------------------------

                    current_positions = (
                        aktif_pozisyonlari_getir(
                            exchange
                        )
                    )

                    if current_positions is None:

                        break

                    if (
                        len(current_positions)
                        >=
                        MAX_TOTAL_POSITIONS
                    ):

                        break

                    eval_res = (
                        evaluate_entry(
                            exchange,
                            candidate["symbol"],
                            candidate["direction"],
                            "opportunity",
                            candidate["df"]
                        )
                    )

                    logging.info(
                        f"[OPPORTUNITY ANALİZ] "
                        f"{candidate['symbol']} "
                        f"{candidate['direction'].upper()} "
                        f"→ "
                        f"{eval_res.get('reason')} "
                        f"| Score="
                        f"{eval_res.get('final_score', 0)}"
                    )

                    if eval_res[
                        "approved"
                    ]:

                        success = (
                            pozisyon_ac(
                                exchange,
                                candidate[
                                    "symbol"
                                ],
                                candidate[
                                    "direction"
                                ],
                                eval_res[
                                    "final_score"
                                ],
                                "opportunity",
                                str(
                                    eval_res
                                )
                            )
                        )

                        if success:

                            opportunity_opened = True

                        # Bir Opportunity
                        # analiz döngüsünde
                        # yalnızca bir tane.
                        break

            # =================================================
            # SCALP
            # =================================================

            aktif_now = (
                aktif_pozisyonlari_getir(
                    exchange
                )
            )

            if aktif_now is None:

                aktif_now = []

            scalp_count_now = 0

            for p in aktif_now:

                if (
                    pozisyon_tipini_cozumle(
                        p
                    )
                    ==
                    "scalp"
                ):

                    scalp_count_now += 1

            if (
                scalp_count_now
                <
                MAX_SCALP_POSITIONS
                and
                len(aktif_now)
                <
                MAX_TOTAL_POSITIONS
            ):

                scalp_listesi = (
                    scan_scalp_market(
                        exchange
                    )
                )

                logging.info(
                    f"[SCALP] "
                    f"{len(scalp_listesi)} aday."
                )

                for candidate in (
                    scalp_listesi
                ):

                    current_positions = (
                        aktif_pozisyonlari_getir(
                            exchange
                        )
                    )

                    if current_positions is None:

                        break

                    if (
                        len(current_positions)
                        >=
                        MAX_TOTAL_POSITIONS
                    ):

                        break

                    eval_res = (
                        evaluate_entry(
                            exchange,
                            candidate["symbol"],
                            candidate["direction"],
                            "scalp",
                            candidate["df"]
                        )
                    )

                    logging.info(
                        f"[SCALP ANALİZ] "
                        f"{candidate['symbol']} "
                        f"{candidate['direction'].upper()} "
                        f"→ "
                        f"{eval_res.get('reason')} "
                        f"| Score="
                        f"{eval_res.get('final_score', 0)}"
                    )

                    if eval_res[
                        "approved"
                    ]:

                        success = (
                            pozisyon_ac(
                                exchange,
                                candidate[
                                    "symbol"
                                ],
                                candidate[
                                    "direction"
                                ],
                                eval_res[
                                    "final_score"
                                ],
                                "scalp",
                                str(
                                    eval_res
                                )
                            )
                        )

                        if success:

                            break

            # =================================================
            # SON RAPOR
            # =================================================

            final_positions = (
                aktif_pozisyonlari_getir(
                    exchange
                )
            )

            active_report = []

            if final_positions is not None:

                for p in final_positions:

                    symbol = (
                        sembol_duzelt(
                            p.get(
                                "symbol"
                            )
                        )
                    )

                    active_report.append({

                        "symbol":
                            symbol,

                        "type":
                            pozisyon_tipini_cozumle(
                                p
                            ),

                        "side":
                            p.get("side"),

                        "roi":
                            float(
                                p.get(
                                    "percentage"
                                )
                                or 0
                            ),

                        "target_roi":
                            pozisyon_hedef_roi.get(
                                symbol,
                                0
                            ),

                        "target_usdt":
                            pozisyon_hedef_usdt.get(
                                symbol,
                                0
                            ),

                        "max_roi":
                            pozisyon_en_yuksek_kar.get(
                                symbol,
                                0
                            ),

                        "locked_roi":
                            pozisyon_kilit_roi.get(
                                symbol,
                                0
                            )
                    })

            son_detayli_analiz_raporu = {

                "zaman":
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),

                "aktif_pozisyonlar_roi_durumu":
                    active_report,

                "yapilan_islemler":
                    [],

                "aciklamalar": [
                    "Multi-timeframe trend teyidi aktif.",
                    "4H + orta timeframe + trigger timeframe kullanılıyor.",
                    "Zarar sonrası aynı coin/yön yeniden giriş engeli aktif.",
                    "Pozisyon limiti gerçek Binance pozisyonlarından kontrol ediliyor.",
                    "Kâr koruma ve momentum takibi aktif."
                ]
            }

        except Exception as e:

            logging.error(
                f"Ana döngü hatası: {e}"
            )

        finally:

            gc.collect()

        # =====================================================
        # ANALİZ:
        # 5 DAKİKADA BİR
        #
        # MONITOR:
        # AYRI THREAD
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
                LEVERAGE,

            "margin_mode":
                MARGIN_MODE
        },

        "targets": {

            "scalp_target_roi":
                SCALP_TARGET_ROI,

            "scalp_target_usdt":
                SCALP_TARGET_USDT,

            "opportunity_target_roi":
                OPPORTUNITY_TARGET_ROI,

            "opportunity_target_usdt":
                OPPORTUNITY_TARGET_USDT
        },

        "risk_protection": {

            "loss_direction_block_hours":
                LOSS_DIRECTION_BLOCK_HOURS,

            "profit_protection":
                True,

            "multi_timeframe":
                True
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

        "trade_planlari":
            pozisyon_trade_plan,

        "hedefler": {

            "scalp": {

                "margin":
                    SCALP_MARGIN,

                "target_roi":
                    SCALP_TARGET_ROI,

                "target_usdt":
                    SCALP_TARGET_USDT
            },

            "opportunity": {

                "margin":
                    OPPORTUNITY_MARGIN,

                "target_roi":
                    OPPORTUNITY_TARGET_ROI,

                "target_usdt":
                    OPPORTUNITY_TARGET_USDT
            }
        },

        "profit_protection": {

            "pozisyon_zirveleri":
                pozisyon_en_yuksek_kar,

            "kilit_roi":
                pozisyon_kilit_roi
        },

        "zararli_yon_bloklari":
            loss_direction_map,

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
                LEVERAGE,

            "MARGIN_MODE":
                MARGIN_MODE
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

    trade_memory_yukle()

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