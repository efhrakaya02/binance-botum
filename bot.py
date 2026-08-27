import os
import ccxt
import pandas as pd
import numpy as np
import time
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify

# ============================================================
# BINANCE FUTURES BOT
#
# FIRSAT + SCALP AKTİF
#
# CRON:
#   SADECE ANALİZ + YENİ İŞLEM
#
# MONITOR:
#   POZİSYONLARI CRON'DAN BAĞIMSIZ YÖNETİR
#
# SCALP:
#   TP = +3.0 ROI
#   SL = -1.5 ROI
#
# KORUMA:
#   Binance TAKE_PROFIT_MARKET
#   Binance STOP_MARKET
#   Bot market monitor
# ============================================================

app = Flask(__name__)

API_KEY = os.getenv("BINANCE_API_KEY")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    print(
        "UYARI: BINANCE_API_KEY veya BINANCE_SECRET_KEY eksik!",
        flush=True
    )


# ============================================================
# AYARLAR
# ============================================================

# ------------------------------------------------------------
# SCALP
# ------------------------------------------------------------

SCALP_ENABLED = True

SCALP_MARGIN = 10.0
MAX_SCALP_POSITIONS = 2

SCALP_TP_ROI = 3.0
SCALP_SL_ROI = -1.5

POSITION_MONITOR_INTERVAL = 1.0


# ------------------------------------------------------------
# FIRSAT
# ------------------------------------------------------------

OPPORTUNITY_ENABLED = True

OPPORTUNITY_MARGIN = 15.0

MAX_OPPORTUNITY_POSITIONS = 1


# ------------------------------------------------------------
# TOPLAM
# ------------------------------------------------------------

MAX_TOTAL_POSITIONS = 3


# ------------------------------------------------------------
# SKOR
# ------------------------------------------------------------

OPPORTUNITY_MIN_SCORE = 68
SCALP_MIN_SCORE = 72


# ------------------------------------------------------------
# KALDIRAÇ
# ------------------------------------------------------------

MIN_LEVERAGE = 3
MAX_LEVERAGE = 10


# ------------------------------------------------------------
# COOLDOWN
# ------------------------------------------------------------

COOLDOWN_HOURS = 4

cooldown_ms = (
    COOLDOWN_HOURS
    * 60
    * 60
    * 1000
)

son_kapanis_zamanlari = {}


# ============================================================
# RUNTIME STATE
# ============================================================

pozisyon_en_yuksek_kar = {}

pozisyon_stoplari = {}

pozisyon_tp_emirleri = {}

pozisyon_sl_emirleri = {}

pozisyon_tipleri = {}

pozisyon_kapatma_lock = {}


# ============================================================
# LOCKLAR
# ============================================================

analiz_lock = threading.Lock()

pozisyon_monitor_lock = threading.Lock()


# ============================================================
# MONITOR DURUMU
# ============================================================

monitor_basladi = False


# ============================================================
# MAKRO ENGEL
# ============================================================

MACRO_BLOCK_WINDOWS_UTC = os.getenv(
    "MACRO_BLOCK_WINDOWS_UTC",
    ""
).strip()


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
            "adjustForTimeDifference": True
        }
    })


# ============================================================
# SEMBOL
# ============================================================

def sembol_duzelt(symbol):

    if not symbol:
        return symbol

    # Eski Binance sembolü
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

    # Binance Futures CCXT:
    #
    # BTC/USDT:USDT
    #
    # bazı durumlarda:
    #
    # BTC/USDT
    #
    if not (
        symbol.endswith("/USDT")
        or
        symbol.endswith("/USDT:USDT")
    ):
        return False

    for yasak in yasakli:

        if yasak in symbol:
            return False

    return True


# ============================================================
# EMA
# ============================================================

def ema(series, period):

    return series.ewm(
        span=period,
        adjust=False
    ).mean()


# ============================================================
# RSI
# ============================================================

def hesapla_rsi(series, period=14):

    delta = series.diff()

    gain = delta.where(
        delta > 0,
        0.0
    )

    loss = -delta.where(
        delta < 0,
        0.0
    )

    avg_gain = gain.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    rs = (
        avg_gain
        /
        avg_loss.replace(
            0,
            np.nan
        )
    )

    return 100 - (
        100 / (1 + rs)
    )


# ============================================================
# ATR
# ============================================================

def hesapla_atr(df, period=14):

    high_low = (
        df["high"]
        -
        df["low"]
    )

    high_close = abs(
        df["high"]
        -
        df["close"].shift()
    )

    low_close = abs(
        df["low"]
        -
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

    return tr.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()


# ============================================================
# ADX
# ============================================================

def hesapla_adx(df, period=14):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where(
        (up_move > down_move)
        &
        (up_move > 0),
        up_move,
        0
    )

    minus_dm = np.where(
        (down_move > up_move)
        &
        (down_move > 0),
        down_move,
        0
    )

    tr1 = high - low
    tr2 = abs(
        high -
        close.shift()
    )
    tr3 = abs(
        low -
        close.shift()
    )

    tr = pd.concat(
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(axis=1)

    atr = tr.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    plus_dm = pd.Series(
        plus_dm,
        index=df.index
    )

    minus_dm = pd.Series(
        minus_dm,
        index=df.index
    )

    plus_di = (
        100
        *
        plus_dm.ewm(
            alpha=1 / period,
            adjust=False
        ).mean()
        /
        atr
    )

    minus_di = (
        100
        *
        minus_dm.ewm(
            alpha=1 / period,
            adjust=False
        ).mean()
        /
        atr
    )

    denominator = (
        plus_di +
        minus_di
    ).replace(
        0,
        np.nan
    )

    dx = (
        100
        *
        abs(
            plus_di -
            minus_di
        )
        /
        denominator
    )

    adx = dx.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    return (
        adx,
        plus_di,
        minus_di
    )


# ============================================================
# MACD
# ============================================================

def hesapla_macd(df):

    ema12 = ema(
        df["close"],
        12
    )

    ema26 = ema(
        df["close"],
        26
    )

    macd = ema12 - ema26

    signal = macd.ewm(
        span=9,
        adjust=False
    ).mean()

    histogram = macd - signal

    return (
        macd,
        signal,
        histogram
    )


# ============================================================
# OBV
# ============================================================

def hesapla_obv(df):

    direction = np.sign(
        df["close"].diff()
    ).fillna(0)

    return (
        direction
        *
        df["volume"]
    ).cumsum()


# ============================================================
# TEKNİK İNDİKATÖRLER
# ============================================================

def teknik_indikatorleri_hesapla(df):

    df = df.copy()

    df["ema9"] = ema(
        df["close"],
        9
    )

    df["ema21"] = ema(
        df["close"],
        21
    )

    df["ema50"] = ema(
        df["close"],
        50
    )

    df["ema200"] = ema(
        df["close"],
        200
    )

    df["rsi"] = hesapla_rsi(
        df["close"],
        14
    )

    df["atr"] = hesapla_atr(
        df,
        14
    )

    (
        df["adx"],
        df["plus_di"],
        df["minus_di"]
    ) = hesapla_adx(
        df,
        14
    )

    (
        df["macd"],
        df["macd_signal"],
        df["macd_hist"]
    ) = hesapla_macd(df)

    df["obv"] = hesapla_obv(df)

    df["obv_ma"] = (
        df["obv"]
        .rolling(20)
        .mean()
    )

    # --------------------------------------------------------
    # BOLLINGER
    # --------------------------------------------------------

    df["bb_mid"] = (
        df["close"]
        .rolling(20)
        .mean()
    )

    std = (
        df["close"]
        .rolling(20)
        .std()
    )

    df["bb_upper"] = (
        df["bb_mid"]
        +
        2 * std
    )

    df["bb_lower"] = (
        df["bb_mid"]
        -
        2 * std
    )

    df["bb_width"] = (
        (
            df["bb_upper"]
            -
            df["bb_lower"]
        )
        /
        df["bb_mid"]
    )

    df["bb_width_ma"] = (
        df["bb_width"]
        .rolling(50)
        .mean()
    )

    df["squeeze"] = (
        df["bb_width"]
        <
        df["bb_width_ma"]
        *
        0.85
    )

    # --------------------------------------------------------
    # HACİM
    # --------------------------------------------------------

    df["volume_ma20"] = (
        df["volume"]
        .rolling(20)
        .mean()
    )

    df["volume_ratio"] = (
        df["volume"]
        /
        df["volume_ma20"]
    )

    # --------------------------------------------------------
    # SON 20 MUM
    # --------------------------------------------------------

    df["recent_high"] = (
        df["high"]
        .rolling(20)
        .max()
        .shift(1)
    )

    df["recent_low"] = (
        df["low"]
        .rolling(20)
        .min()
        .shift(1)
    )

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    df["roc"] = (
        df["close"]
        .pct_change(5)
        *
        100
    )

    return df


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
            len(data) < 50
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

        return teknik_indikatorleri_hesapla(
            df
        )

    except Exception as e:

        print(
            f"[OHLCV HATA] "
            f"{symbol} "
            f"{timeframe}: {e}",
            flush=True
        )

        return None


# ============================================================
# MAKRO
# ============================================================

def dakika_saat_parse(text):

    try:

        h, m = (
            text
            .strip()
            .split(":")
        )

        return (
            int(h),
            int(m)
        )

    except Exception:

        return None


def makro_zaman_engeli_var_mi():

    now = datetime.now(
        timezone.utc
    )

    current_minutes = (
        now.hour * 60
        +
        now.minute
    )

    if MACRO_BLOCK_WINDOWS_UTC:

        for window in (
            MACRO_BLOCK_WINDOWS_UTC.split(",")
        ):

            try:

                start, end = (
                    window
                    .strip()
                    .split("-")
                )

                parsed_start = (
                    dakika_saat_parse(
                        start
                    )
                )

                parsed_end = (
                    dakika_saat_parse(
                        end
                    )
                )

                if (
                    parsed_start is None
                    or
                    parsed_end is None
                ):
                    continue

                sh, sm = parsed_start
                eh, em = parsed_end

                start_min = (
                    sh * 60 + sm
                )

                end_min = (
                    eh * 60 + em
                )

                if start_min <= end_min:

                    if (
                        start_min
                        <= current_minutes
                        <= end_min
                    ):
                        return True

                else:

                    if (
                        current_minutes
                        >= start_min
                        or
                        current_minutes
                        <= end_min
                    ):
                        return True

            except Exception:

                continue

    # --------------------------------------------------------
    # FUNDING ZAMANLARI
    #
    # 00 / 08 / 16 UTC
    # +/- 5 dakika
    # --------------------------------------------------------

    if (
        now.minute >= 55
        or
        now.minute <= 5
    ) and now.hour in [0, 8, 16]:

        return True

    return False


# ============================================================
# COOLDOWN
# ============================================================

def cooldown_aktif_mi(symbol):

    symbol = sembol_duzelt(
        symbol
    )

    son = (
        son_kapanis_zamanlari.get(
            symbol
        )
    )

    if not son:
        return False

    return (
        int(
            time.time() * 1000
        )
        -
        son
        <
        cooldown_ms
    )


# ============================================================
# POZİSYON TİPİ
# ============================================================

def pozisyon_tipi(p):

    symbol = sembol_duzelt(
        p.get("symbol")
    )

    # Önce runtime kayıt

    if symbol in pozisyon_tipleri:

        return pozisyon_tipleri[
            symbol
        ]

    try:

        margin = float(
            p.get(
                "initialMargin"
            )
            or 0
        )

    except Exception:

        margin = 0

    # --------------------------------------------------------
    # SCALP
    # --------------------------------------------------------

    if abs(
        margin -
        SCALP_MARGIN
    ) <= 2.0:

        return "scalp"

    # --------------------------------------------------------
    # OPPORTUNITY
    # --------------------------------------------------------

    if abs(
        margin -
        OPPORTUNITY_MARGIN
    ) <= 2.5:

        return "opportunity"

    return "unknown"


# ============================================================
# PUANLAMA
# ============================================================

def skorla_coin(
    exchange,
    symbol
):

    result = {

        "symbol": symbol,

        "long_score": 0,
        "short_score": 0,

        "long_reasons": [],
        "short_reasons": [],

        "direction": None,
        "score": 0,

        "atr": None,
        "price": None,

        "rsi_5m": None,
        "rsi_15m": None,
        "rsi_1h": None,

        "adx_1h": None,

        "funding": 0
    }

    try:

        # ====================================================
        # FUNDING
        # ====================================================

        try:

            funding_data = (
                exchange.fetch_funding_rate(
                    symbol
                )
            )

            funding = float(
                funding_data.get(
                    "fundingRate",
                    0
                )
                or 0
            )

            result["funding"] = funding

            if abs(funding) >= 0.0015:

                return None

        except Exception:

            funding = 0

        # ====================================================
        # VERİ
        # ====================================================

        df5 = ohlcv_getir(
            exchange,
            symbol,
            "5m",
            100
        )

        df15 = ohlcv_getir(
            exchange,
            symbol,
            "15m",
            100
        )

        df1 = ohlcv_getir(
            exchange,
            symbol,
            "1h",
            250
        )

        df4 = ohlcv_getir(
            exchange,
            symbol,
            "4h",
            250
        )

        if (
            df5 is None
            or
            df15 is None
            or
            df1 is None
            or
            df4 is None
        ):

            return None

        d5 = df5.iloc[-2]
        d15 = df15.iloc[-2]
        d1 = df1.iloc[-2]
        d4 = df4.iloc[-2]

        price = float(
            d5["close"]
        )

        atr = float(
            d1["atr"]
        )

        result["price"] = price
        result["atr"] = atr

        if not np.isfinite(price):
            return None

        if not np.isfinite(atr):
            return None

        # ====================================================
        # VOLATİLİTE
        # ====================================================

        atr_pct = (
            atr /
            price *
            100
        )

        if atr_pct > 12:

            return None

        # ====================================================
        # HACİM
        # ====================================================

        volume_ratio_5 = float(
            d5["volume_ratio"]
        )

        volume_ratio_15 = float(
            d15["volume_ratio"]
        )

        if (
            not np.isfinite(
                volume_ratio_5
            )
            or
            not np.isfinite(
                volume_ratio_15
            )
        ):

            return None

        if (
            volume_ratio_5 < 0.65
            and
            volume_ratio_15 < 0.65
        ):

            return None

        # ====================================================
        # TREND
        # ====================================================

        trend4_long = (
            d4["close"]
            >
            d4["ema50"]
            and
            d4["ema50"]
            >
            d4["ema200"]
        )

        trend4_short = (
            d4["close"]
            <
            d4["ema50"]
            and
            d4["ema50"]
            <
            d4["ema200"]
        )

        trend1_long = (
            d1["close"]
            >
            d1["ema50"]
            and
            d1["ema9"]
            >
            d1["ema21"]
        )

        trend1_short = (
            d1["close"]
            <
            d1["ema50"]
            and
            d1["ema9"]
            <
            d1["ema21"]
        )

        # ====================================================
        # ADX
        # ====================================================

        adx = float(
            d1["adx"]
        )

        if not np.isfinite(adx):

            return None

        if adx < 15:

            return None

        # ====================================================
        # RSI
        # ====================================================

        rsi1 = float(
            d1["rsi"]
        )

        rsi15 = float(
            d15["rsi"]
        )

        rsi5 = float(
            d5["rsi"]
        )

        result["rsi_1h"] = rsi1
        result["rsi_15m"] = rsi15
        result["rsi_5m"] = rsi5
        result["adx_1h"] = adx

        # ====================================================
        # LONG
        # ====================================================

        long_score = 0
        long_reasons = []

        if trend4_long:

            long_score += 18

            long_reasons.append(
                "4H trend yukarı"
            )

        elif d4["close"] > d4["ema50"]:

            long_score += 8

            long_reasons.append(
                "4H EMA50 üstü"
            )

        if trend1_long:

            long_score += 16

            long_reasons.append(
                "1H EMA trendi yukarı"
            )

        elif d1["close"] > d1["ema50"]:

            long_score += 7

        if d1["ema9"] > d1["ema21"]:

            long_score += 7

        if (
            d1["macd"]
            >
            d1["macd_signal"]
        ):

            long_score += 10

            long_reasons.append(
                "MACD pozitif"
            )

        if d1["macd_hist"] > 0:

            long_score += 4

        if (
            d1["plus_di"]
            >
            d1["minus_di"]
        ):

            long_score += 8

            long_reasons.append(
                "+DI üstün"
            )

        if adx >= 25:

            long_score += 8

            long_reasons.append(
                "ADX güçlü"
            )

        elif adx >= 20:

            long_score += 4

        if 48 <= rsi1 <= 65:

            long_score += 10

            long_reasons.append(
                "RSI sağlıklı"
            )

        elif 42 <= rsi1 < 48:

            long_score += 5

        if rsi1 > 72:

            long_score -= 15

        if 45 <= rsi15 <= 68:

            long_score += 6

        if 50 <= rsi5 <= 70:

            long_score += 5

        if d1["obv"] > d1["obv_ma"]:

            long_score += 8

            long_reasons.append(
                "OBV destekliyor"
            )

        if volume_ratio_5 >= 1.20:

            long_score += 7

            long_reasons.append(
                "5m hacim artışı"
            )

        elif volume_ratio_5 >= 1.0:

            long_score += 3

        if price > d1["recent_high"]:

            long_score += 10

            long_reasons.append(
                "1H breakout"
            )

        if price > d1["bb_mid"]:

            long_score += 4

        if bool(d1["squeeze"]):

            long_score += 5

            long_reasons.append(
                "Squeeze"
            )

        # ====================================================
        # SHORT
        # ====================================================

        short_score = 0
        short_reasons = []

        if trend4_short:

            short_score += 18

            short_reasons.append(
                "4H trend aşağı"
            )

        elif d4["close"] < d4["ema50"]:

            short_score += 8

            short_reasons.append(
                "4H EMA50 altı"
            )

        if trend1_short:

            short_score += 16

            short_reasons.append(
                "1H EMA trendi aşağı"
            )

        elif d1["close"] < d1["ema50"]:

            short_score += 7

        if d1["ema9"] < d1["ema21"]:

            short_score += 7

        if (
            d1["macd"]
            <
            d1["macd_signal"]
        ):

            short_score += 10

            short_reasons.append(
                "MACD negatif"
            )

        if d1["macd_hist"] < 0:

            short_score += 4

        if (
            d1["minus_di"]
            >
            d1["plus_di"]
        ):

            short_score += 8

            short_reasons.append(
                "-DI üstün"
            )

        if adx >= 25:

            short_score += 8

            short_reasons.append(
                "ADX güçlü"
            )

        elif adx >= 20:

            short_score += 4

        if 35 <= rsi1 <= 52:

            short_score += 10

            short_reasons.append(
                "RSI short için uygun"
            )

        elif 52 < rsi1 <= 58:

            short_score += 4

        if rsi1 < 28:

            short_score -= 15

        if 32 <= rsi15 <= 55:

            short_score += 6

        if 30 <= rsi5 <= 52:

            short_score += 5

        if d1["obv"] < d1["obv_ma"]:

            short_score += 8

            short_reasons.append(
                "OBV zayıf"
            )

        if volume_ratio_5 >= 1.20:

            short_score += 7

            short_reasons.append(
                "5m hacim artışı"
            )

        elif volume_ratio_5 >= 1.0:

            short_score += 3

        if price < d1["recent_low"]:

            short_score += 10

            short_reasons.append(
                "1H breakdown"
            )

        if price < d1["bb_mid"]:

            short_score += 4

        if bool(d1["squeeze"]):

            short_score += 5

            short_reasons.append(
                "Squeeze"
            )

        # ====================================================
        # YÖN
        # ====================================================

        if long_score >= short_score:

            direction = "buy"

            score = long_score

            reasons = long_reasons

        else:

            direction = "sell"

            score = short_score

            reasons = short_reasons

        if abs(
            long_score -
            short_score
        ) < 8:

            return None

        result["long_score"] = long_score
        result["short_score"] = short_score

        result["direction"] = direction
        result["score"] = score

        result["reasons"] = reasons

        return result

    except Exception as e:

        print(
            f"[SKOR HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return None


# ============================================================
# KALDIRAÇ
# ============================================================

def kaldirac_belirle(score):

    if score >= 92:
        return 10

    if score >= 85:
        return 8

    if score >= 78:
        return 6

    if score >= 72:
        return 5

    if score >= 68:
        return 4

    return 3


# ============================================================
# MİKTAR
# ============================================================

def miktar_hesapla(
    exchange,
    symbol,
    margin,
    leverage,
    price
):

    market = exchange.market(
        symbol
    )

    min_amount = (
        market
        .get("limits", {})
        .get("amount", {})
        .get("min")
    )

    if not min_amount:

        min_amount = 0.001

    notional = (
        margin
        *
        leverage
    )

    raw_amount = (
        notional
        /
        price
    )

    amount = max(
        raw_amount,
        float(min_amount)
    )

    try:

        amount = float(
            exchange.amount_to_precision(
                symbol,
                amount
            )
        )

    except Exception:

        amount = float(
            amount
        )

    return amount


# ============================================================
# ISOLATED + LEVERAGE
# ============================================================

def isolated_ve_kaldirac_ayarla(
    exchange,
    symbol,
    leverage
):

    try:

        exchange.set_margin_mode(
            "isolated",
            symbol
        )

        print(
            f"[MARGIN] "
            f"{symbol} -> ISOLATED",
            flush=True
        )

    except Exception as e:

        text = str(e).lower()

        if (
            "no need" not in text
            and
            "already" not in text
            and
            "isolated" not in text
        ):

            print(
                f"[MARGIN UYARI] "
                f"{symbol}: {e}",
                flush=True
            )

    try:

        exchange.set_leverage(
            leverage,
            symbol
        )

        print(
            f"[LEVERAGE] "
            f"{symbol} -> {leverage}x",
            flush=True
        )

    except Exception as e:

        print(
            f"[LEVERAGE HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return False

    return True


# ============================================================
# EMİR TİPİ
# ============================================================

def emir_stop_mu(order):

    order_type = (
        order.get("type")
        or ""
    ).upper()

    return order_type in [
        "STOP_MARKET",
        "STOP"
    ]


def emir_tp_mi(order):

    order_type = (
        order.get("type")
        or ""
    ).upper()

    return order_type in [
        "TAKE_PROFIT_MARKET",
        "TAKE_PROFIT"
    ]


# ============================================================
# KORUMA EMİRLERİNİ GETİR
# ============================================================

def koruma_emirlerini_getir(
    exchange,
    symbol
):

    try:

        orders = (
            exchange.fetch_open_orders(
                symbol
            )
        )

        result = []

        for order in orders:

            if (
                emir_stop_mu(order)
                or
                emir_tp_mi(order)
            ):

                result.append(
                    order
                )

        return result

    except Exception as e:

        print(
            f"[KORUMA EMİR LİSTE HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return []


# ============================================================
# KORUMA EMİRLERİNİ TEMİZLE
# ============================================================

def koruma_emirlerini_temizle(
    exchange,
    symbol
):

    try:

        orders = (
            koruma_emirlerini_getir(
                exchange,
                symbol
            )
        )

        for order in orders:

            try:

                exchange.cancel_order(
                    order["id"],
                    symbol
                )

                print(
                    f"[EMİR İPTAL] "
                    f"{symbol} "
                    f"{order.get('type')} "
                    f"{order['id']}",
                    flush=True
                )

            except Exception as e:

                print(
                    f"[EMİR İPTAL HATA] "
                    f"{symbol}: {e}",
                    flush=True
                )

    except Exception as e:

        print(
            f"[KORUMA TEMİZLE HATA] "
            f"{symbol}: {e}",
            flush=True
        )


# ============================================================
# STOP EMRİ
# ============================================================

def stop_emri_koy(
    exchange,
    symbol,
    side,
    amount,
    stop_price
):

    close_side = (
        "sell"
        if side == "buy"
        else "buy"
    )

    try:

        stop_price = float(
            exchange.price_to_precision(
                symbol,
                stop_price
            )
        )

        order = exchange.create_order(
            symbol,
            "STOP_MARKET",
            close_side,
            amount,
            None,
            {
                "stopPrice": stop_price,
                "reduceOnly": True,
                "workingType": "MARK_PRICE"
            }
        )

        print(
            f"[STOP MARKET] "
            f"{symbol} -> "
            f"{stop_price}",
            flush=True
        )

        pozisyon_stoplari[
            symbol
        ] = stop_price

        pozisyon_sl_emirleri[
            symbol
        ] = order["id"]

        return order

    except Exception as e:

        print(
            f"[STOP HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return None


# ============================================================
# TAKE PROFIT
# ============================================================

def tp_emri_koy(
    exchange,
    symbol,
    side,
    amount,
    tp_price
):

    close_side = (
        "sell"
        if side == "buy"
        else "buy"
    )

    try:

        tp_price = float(
            exchange.price_to_precision(
                symbol,
                tp_price
            )
        )

        order = exchange.create_order(
            symbol,
            "TAKE_PROFIT_MARKET",
            close_side,
            amount,
            None,
            {
                "stopPrice": tp_price,
                "reduceOnly": True,
                "workingType": "MARK_PRICE"
            }
        )

        print(
            f"[TP MARKET] "
            f"{symbol} -> "
            f"{tp_price}",
            flush=True
        )

        pozisyon_tp_emirleri[
            symbol
        ] = order["id"]

        return order

    except Exception as e:

        print(
            f"[TP HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return None


# ============================================================
# ROI FİYATI
# ============================================================

def roi_fiyat_hesapla(
    side,
    entry_price,
    leverage,
    target_roi
):

    if leverage <= 0:

        leverage = 1

    price_change_pct = (
        target_roi
        /
        leverage
    )

    if side == "buy":

        return (
            entry_price
            *
            (
                1
                +
                price_change_pct
                /
                100
            )
        )

    else:

        return (
            entry_price
            *
            (
                1
                -
                price_change_pct
                /
                100
            )
        )


# ============================================================
# SCALP KORUMA
# ============================================================

def scalp_koruma_emirlerini_kur(
    exchange,
    symbol,
    side,
    amount,
    entry_price,
    leverage
):

    try:

        koruma_emirlerini_temizle(
            exchange,
            symbol
        )

        tp_price = roi_fiyat_hesapla(
            side,
            entry_price,
            leverage,
            SCALP_TP_ROI
        )

        sl_price = roi_fiyat_hesapla(
            side,
            entry_price,
            leverage,
            SCALP_SL_ROI
        )

        print(
            f"[SCALP KORUMA] "
            f"{symbol} "
            f"Entry={entry_price} "
            f"TP ROI=+{SCALP_TP_ROI}% "
            f"TP={tp_price} "
            f"SL ROI={SCALP_SL_ROI}% "
            f"SL={sl_price}",
            flush=True
        )

        tp = tp_emri_koy(
            exchange,
            symbol,
            side,
            amount,
            tp_price
        )

        sl = stop_emri_koy(
            exchange,
            symbol,
            side,
            amount,
            sl_price
        )

        if tp is None:

            print(
                f"[UYARI] "
                f"{symbol} scalp TP "
                f"Binance'a yerleştirilemedi!",
                flush=True
            )

        if sl is None:

            print(
                f"[UYARI] "
                f"{symbol} scalp SL "
                f"Binance'a yerleştirilemedi!",
                flush=True
            )

        return (
            tp is not None
            and
            sl is not None
        )

    except Exception as e:

        print(
            f"[SCALP KORUMA HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return False


# ============================================================
# İLK STOP
# ============================================================

def ilk_stop_yerlestir(
    exchange,
    symbol,
    side,
    amount,
    stop_price
):

    try:

        koruma_emirlerini_temizle(
            exchange,
            symbol
        )

        return stop_emri_koy(
            exchange,
            symbol,
            side,
            amount,
            stop_price
        )

    except Exception as e:

        print(
            f"[İLK STOP HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return None


# ============================================================
# STOP GÜNCELLE
#
# SADECE STOP İPTAL EDİLİR.
# TP'YE DOKUNULMAZ.
# ============================================================

def stop_guncelle(
    exchange,
    symbol,
    side,
    amount,
    yeni_stop
):

    try:

        mevcut = (
            pozisyon_stoplari.get(
                symbol
            )
        )

        if mevcut is not None:

            if side == "buy":

                if yeni_stop <= mevcut:

                    return False

            else:

                if yeni_stop >= mevcut:

                    return False

        ticker = (
            exchange.fetch_ticker(
                symbol
            )
        )

        current_price = float(
            ticker["last"]
        )

        # ----------------------------------------------------
        # Yanlış tarafa stop koyma
        # ----------------------------------------------------

        if side == "buy":

            if yeni_stop >= current_price:

                yeni_stop = (
                    current_price
                    *
                    0.998
                )

        else:

            if yeni_stop <= current_price:

                yeni_stop = (
                    current_price
                    *
                    1.002
                )

        # ----------------------------------------------------
        # SADECE STOP EMİRLERİ
        # ----------------------------------------------------

        orders = (
            koruma_emirlerini_getir(
                exchange,
                symbol
            )
        )

        for order in orders:

            if not emir_stop_mu(
                order
            ):

                continue

            try:

                exchange.cancel_order(
                    order["id"],
                    symbol
                )

            except Exception:

                pass

        order = stop_emri_koy(
            exchange,
            symbol,
            side,
            amount,
            yeni_stop
        )

        if order:

            print(
                f"[STOP YENİLENDİ] "
                f"{symbol} -> "
                f"{yeni_stop}",
                flush=True
            )

            return True

    except Exception as e:

        print(
            f"[STOP GÜNCELLEME HATA] "
            f"{symbol}: {e}",
            flush=True
        )

    return False


# ============================================================
# FIRSAT BAŞLANGIÇ STOP
# ============================================================

def baslangic_stop_hesapla(
    side,
    entry_price,
    atr,
    score
):

    if score >= 90:

        multiplier = 2.2

    elif score >= 80:

        multiplier = 2.0

    elif score >= 72:

        multiplier = 1.8

    else:

        multiplier = 1.6

    if side == "buy":

        return (
            entry_price
            -
            atr * multiplier
        )

    else:

        return (
            entry_price
            +
            atr * multiplier
        )


# ============================================================
# ROI
# ============================================================

def roi_hesapla(
    side,
    current_price,
    entry_price,
    leverage
):

    if entry_price <= 0:

        return 0

    if side == "long":

        price_change = (
            current_price
            -
            entry_price
        ) / entry_price

    else:

        price_change = (
            entry_price
            -
            current_price
        ) / entry_price

    return (
        price_change
        *
        100
        *
        leverage
    )


# ============================================================
# TRAILING STOP
# ============================================================

def trailing_stop_fiyat(
    side,
    entry_price,
    roi,
    leverage
):

    if roi < 5:

        return None

    if roi >= 20:

        locked_roi = 15

    elif roi >= 15:

        locked_roi = 10

    elif roi >= 10:

        locked_roi = 5

    elif roi >= 5:

        locked_roi = 0

    else:

        return None

    price_pct = (
        locked_roi
        /
        max(
            leverage,
            1
        )
    )

    if side == "long":

        return (
            entry_price
            *
            (
                1
                +
                price_pct
                /
                100
            )
        )

    else:

        return (
            entry_price
            *
            (
                1
                -
                price_pct
                /
                100
            )
        )


# ============================================================
# MARKET POZİSYON KAPAT
# ============================================================

def market_pozisyon_kapat(
    exchange,
    symbol,
    side,
    amount,
    sebep,
    roi=None
):

    close_side = (
        "sell"
        if side == "long"
        else "buy"
    )

    try:

        if (
            pozisyon_kapatma_lock.get(
                symbol
            )
        ):

            return False

        pozisyon_kapatma_lock[
            symbol
        ] = True

        print(
            f"[MARKET KAPATMA] "
            f"{symbol} "
            f"Sebep={sebep} "
            f"ROI="
            f"{'%.2f' % roi if roi is not None else 'N/A'}",
            flush=True
        )

        order = exchange.create_order(
            symbol,
            "market",
            close_side,
            amount,
            None,
            {
                "reduceOnly": True
            }
        )

        print(
            f"[POZİSYON KAPANDI] "
            f"{symbol} "
            f"{sebep}",
            flush=True
        )

        time.sleep(0.2)

        koruma_emirlerini_temizle(
            exchange,
            symbol
        )

        son_kapanis_zamanlari[
            symbol
        ] = int(
            time.time() * 1000
        )

        pozisyon_en_yuksek_kar.pop(
            symbol,
            None
        )

        pozisyon_stoplari.pop(
            symbol,
            None
        )

        pozisyon_tp_emirleri.pop(
            symbol,
            None
        )

        pozisyon_sl_emirleri.pop(
            symbol,
            None
        )

        pozisyon_tipleri.pop(
            symbol,
            None
        )

        return True

    except Exception as e:

        print(
            f"[MARKET KAPATMA HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return False

    finally:

        pozisyon_kapatma_lock.pop(
            symbol,
            None
        )


# ============================================================
# POZİSYON YÖNETİMİ
# ============================================================

def pozisyonlari_yonet(
    exchange,
    positions
):

    aktif_semboller = []

    for p in positions:

        try:

            contracts = float(
                p.get(
                    "contracts"
                )
                or 0
            )

        except Exception:

            contracts = 0

        if contracts <= 0:

            continue

        symbol = sembol_duzelt(
            p["symbol"]
        )

        aktif_semboller.append(
            symbol
        )

    # ========================================================
    # KAPANAN POZİSYONLAR
    # ========================================================

    for symbol in list(
        pozisyon_en_yuksek_kar.keys()
    ):

        if symbol not in aktif_semboller:

            son_kapanis_zamanlari[
                symbol
            ] = int(
                time.time() * 1000
            )

            pozisyon_en_yuksek_kar.pop(
                symbol,
                None
            )

            pozisyon_stoplari.pop(
                symbol,
                None
            )

            pozisyon_tp_emirleri.pop(
                symbol,
                None
            )

            pozisyon_sl_emirleri.pop(
                symbol,
                None
            )

            pozisyon_tipleri.pop(
                symbol,
                None
            )

    # ========================================================
    # AKTİF POZİSYONLAR
    # ========================================================

    for p in positions:

        try:

            contracts = float(
                p.get(
                    "contracts"
                )
                or 0
            )

            if contracts <= 0:

                continue

            symbol = sembol_duzelt(
                p["symbol"]
            )

            side = p["side"]

            entry = float(
                p["entryPrice"]
            )

            leverage = float(
                p.get("leverage")
                or 1
            )

            mark_price = p.get(
                "markPrice"
            )

            if mark_price:

                current = float(
                    mark_price
                )

            else:

                ticker = (
                    exchange.fetch_ticker(
                        symbol
                    )
                )

                current = float(
                    ticker["last"]
                )

            roi = roi_hesapla(
                side,
                current,
                entry,
                leverage
            )

            ptype = pozisyon_tipi(
                p
            )

            pozisyon_tipleri[
                symbol
            ] = ptype

            # =================================================
            # MAX ROI
            # =================================================

            if symbol not in (
                pozisyon_en_yuksek_kar
            ):

                pozisyon_en_yuksek_kar[
                    symbol
                ] = roi

            elif roi > (
                pozisyon_en_yuksek_kar[
                    symbol
                ]
            ):

                pozisyon_en_yuksek_kar[
                    symbol
                ] = roi

            highest = (
                pozisyon_en_yuksek_kar[
                    symbol
                ]
            )

            print(
                f"[POZİSYON] "
                f"{symbol} "
                f"{side.upper()} "
                f"{ptype} "
                f"ROI %{roi:.2f} "
                f"Max %{highest:.2f}",
                flush=True
            )

            # =================================================
            # SCALP
            # =================================================

            if ptype == "scalp":

                # ------------------------------------------------
                # +3 ROI
                # ------------------------------------------------

                if roi >= SCALP_TP_ROI:

                    market_pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "SCALP TP +3 ROI",
                        roi
                    )

                    continue

                # ------------------------------------------------
                # -1.5 ROI
                # ------------------------------------------------

                if roi <= SCALP_SL_ROI:

                    market_pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "SCALP SL -1.5 ROI",
                        roi
                    )

                    continue

                # ------------------------------------------------
                # KORUMA KONTROL
                # ------------------------------------------------

                try:

                    orders = (
                        koruma_emirlerini_getir(
                            exchange,
                            symbol
                        )
                    )

                    has_tp = any(
                        emir_tp_mi(o)
                        for o in orders
                    )

                    has_sl = any(
                        emir_stop_mu(o)
                        for o in orders
                    )

                    if (
                        not has_tp
                        or
                        not has_sl
                    ):

                        print(
                            f"[SCALP KORUMA EKSİK] "
                            f"{symbol} "
                            f"TP={has_tp} "
                            f"SL={has_sl} "
                            f"yeniden kuruluyor...",
                            flush=True
                        )

                        scalp_koruma_emirlerini_kur(
                            exchange,
                            symbol,
                            side,
                            contracts,
                            entry,
                            leverage
                        )

                except Exception as e:

                    print(
                        f"[SCALP KORUMA KONTROL HATA] "
                        f"{symbol}: {e}",
                        flush=True
                    )

                continue

            # =================================================
            # FIRSAT
            # =================================================

            if ptype == "opportunity":

                # ------------------------------------------------
                # ACİL KORUMA
                # ------------------------------------------------

                if roi <= -25:

                    market_pozisyon_kapat(
                        exchange,
                        symbol,
                        side,
                        contracts,
                        "ACİL KORUMA",
                        roi
                    )

                    continue

                # ------------------------------------------------
                # TRAILING
                # ------------------------------------------------

                new_stop = (
                    trailing_stop_fiyat(
                        side,
                        entry,
                        roi,
                        leverage
                    )
                )

                if new_stop is not None:

                    stop_guncelle(
                        exchange,
                        symbol,
                        "buy"
                        if side == "long"
                        else "sell",
                        contracts,
                        new_stop
                    )

                # ------------------------------------------------
                # ZİRVE GERİ ÇEKİLME
                # ------------------------------------------------

                if highest >= 20:

                    if (
                        highest
                        -
                        roi
                        >= 10
                    ):

                        market_pozisyon_kapat(
                            exchange,
                            symbol,
                            side,
                            contracts,
                            "TRAIL EXIT",
                            roi
                        )

                        continue

        except Exception as e:

            print(
                f"[POZİSYON YÖNETİM HATA] "
                f"{p.get('symbol')}: {e}",
                flush=True
            )


# ============================================================
# BAĞIMSIZ POSITION MONITOR
# ============================================================

def pozisyon_monitor_loop():

    global monitor_basladi

    if monitor_basladi:

        return

    monitor_basladi = True

    print(
        "[MONITOR] "
        "Bağımsız pozisyon takip sistemi başladı.",
        flush=True
    )

    exchange = None

    while True:

        try:

            if exchange is None:

                exchange = get_exchange()

                exchange.load_markets()

            if not pozisyon_monitor_lock.acquire(
                blocking=False
            ):

                time.sleep(
                    POSITION_MONITOR_INTERVAL
                )

                continue

            try:

                positions = (
                    exchange.fetch_positions()
                )

                active_positions = []

                for p in positions:

                    try:

                        contracts = float(
                            p.get(
                                "contracts"
                            )
                            or 0
                        )

                    except Exception:

                        contracts = 0

                    if contracts > 0:

                        active_positions.append(
                            p
                        )

                if active_positions:

                    pozisyonlari_yonet(
                        exchange,
                        active_positions
                    )

            finally:

                try:

                    pozisyon_monitor_lock.release()

                except Exception:

                    pass

        except Exception as e:

            print(
                f"[MONITOR HATA] "
                f"{e}",
                flush=True
            )

            exchange = None

        time.sleep(
            POSITION_MONITOR_INTERVAL
        )


# ============================================================
# MONITOR BAŞLAT
# ============================================================

def monitor_baslat():

    global monitor_basladi

    if monitor_basladi:

        return

    thread = threading.Thread(
        target=pozisyon_monitor_loop,
        daemon=True,
        name="PositionMonitor"
    )

    thread.start()


# ============================================================
# FIRSAT BUL
# ============================================================

def en_iyi_firsat_bul(
    exchange,
    symbols
):

    adaylar = []

    print(
        f"[FIRSAT] "
        f"{len(symbols)} coin analiz ediliyor...",
        flush=True
    )

    for symbol in symbols:

        if cooldown_aktif_mi(
            symbol
        ):

            continue

        try:

            result = skorla_coin(
                exchange,
                symbol
            )

            if result is None:

                continue

            if (
                result["score"]
                <
                OPPORTUNITY_MIN_SCORE
            ):

                continue

            result["type"] = (
                "opportunity"
            )

            adaylar.append(
                result
            )

            print(
                f"[FIRSAT ADAY] "
                f"{symbol} "
                f"{result['direction']} "
                f"SKOR="
                f"{result['score']}",
                flush=True
            )

        except Exception as e:

            print(
                f"[FIRSAT COIN HATA] "
                f"{symbol}: {e}",
                flush=True
            )

    if not adaylar:

        return None

    adaylar.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    return adaylar[0]


# ============================================================
# SCALP BUL
# ============================================================

def vur_kac_sinyali_bul(
    exchange,
    symbols
):

    adaylar = []

    print(
        f"[SCALP] "
        f"{len(symbols)} coin taranıyor...",
        flush=True
    )

    for symbol in symbols:

        if cooldown_aktif_mi(
            symbol
        ):

            continue

        try:

            result = skorla_coin(
                exchange,
                symbol
            )

            if result is None:

                continue

            if (
                result["score"]
                <
                SCALP_MIN_SCORE
            ):

                continue

            result["type"] = "scalp"

            adaylar.append(
                result
            )

            print(
                f"[SCALP ADAY] "
                f"{symbol} "
                f"{result['direction']} "
                f"SKOR="
                f"{result['score']}",
                flush=True
            )

        except Exception as e:

            print(
                f"[SCALP COIN HATA] "
                f"{symbol}: {e}",
                flush=True
            )

    if not adaylar:

        return None

    adaylar.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    return adaylar[0]


# ============================================================
# POZİSYON AÇ
# ============================================================

def pozisyon_ac(
    exchange,
    candidate,
    margin,
    position_type
):

    if (
        position_type == "scalp"
        and
        not SCALP_ENABLED
    ):

        print(
            "[SCALP KAPALI]",
            flush=True
        )

        return False

    if (
        position_type == "opportunity"
        and
        not OPPORTUNITY_ENABLED
    ):

        print(
            "[FIRSAT KAPALI]",
            flush=True
        )

        return False

    symbol = candidate[
        "symbol"
    ]

    direction = candidate[
        "direction"
    ]

    leverage = kaldirac_belirle(
        candidate["score"]
    )

    # Scalp maksimum 5x

    if position_type == "scalp":

        leverage = min(
            leverage,
            5
        )

    leverage = max(
        MIN_LEVERAGE,
        min(
            MAX_LEVERAGE,
            leverage
        )
    )

    print(
        f"[İŞLEM HAZIR] "
        f"{symbol} "
        f"{direction.upper()} "
        f"{position_type.upper()} "
        f"SKOR="
        f"{candidate['score']} "
        f"KALDIRAÇ="
        f"{leverage}x",
        flush=True
    )

    # ========================================================
    # ISOLATED
    # ========================================================

    if not isolated_ve_kaldirac_ayarla(
        exchange,
        symbol,
        leverage
    ):

        print(
            f"[İŞLEM İPTAL] "
            f"{symbol} "
            f"isolated/leverage ayarlanamadı.",
            flush=True
        )

        return False

    try:

        ticker = (
            exchange.fetch_ticker(
                symbol
            )
        )

        price = float(
            ticker["last"]
        )

        amount = miktar_hesapla(
            exchange,
            symbol,
            margin,
            leverage,
            price
        )

        if amount <= 0:

            return False

        estimated_margin = (
            amount
            *
            price
            /
            leverage
        )

        if (
            estimated_margin
            >
            margin * 1.20
        ):

            print(
                f"[MARGIN KORUMA] "
                f"{symbol} "
                f"{estimated_margin:.2f} "
                f"> "
                f"{margin:.2f}",
                flush=True
            )

            return False

        # ====================================================
        # MARKET
        # ====================================================

        order = exchange.create_order(
            symbol,
            "market",
            direction,
            amount,
            None,
            {
                "newClientOrderId":
                    (
                        "BOT_F_"
                        if position_type
                        ==
                        "opportunity"
                        else
                        "BOT_S_"
                    )
                    +
                    str(
                        int(
                            time.time()
                            *
                            1000
                        )
                    )
            }
        )

        print(
            f"!!! "
            f"{position_type.upper()} AÇILDI !!! "
            f"{symbol} "
            f"{direction.upper()} "
            f"Margin="
            f"{estimated_margin:.2f} "
            f"Leverage="
            f"{leverage}x "
            f"Score="
            f"{candidate['score']}",
            flush=True
        )

        # ====================================================
        # GERÇEK ENTRY
        # ====================================================

        time.sleep(0.5)

        positions = (
            exchange.fetch_positions(
                [symbol]
            )
        )

        real_position = None

        for p in positions:

            try:

                contracts = float(
                    p.get(
                        "contracts"
                    )
                    or 0
                )

            except Exception:

                contracts = 0

            if contracts > 0:

                real_position = p

                break

        if real_position:

            real_entry = float(
                real_position[
                    "entryPrice"
                ]
            )

            real_amount = float(
                real_position[
                    "contracts"
                ]
            )

            real_leverage = float(
                real_position.get(
                    "leverage"
                )
                or leverage
            )

        else:

            real_entry = price
            real_amount = amount
            real_leverage = leverage

        # Runtime tip

        pozisyon_tipleri[
            symbol
        ] = position_type

        # ====================================================
        # SCALP KORUMA
        # ====================================================

        if position_type == "scalp":

            success = (
                scalp_koruma_emirlerini_kur(
                    exchange,
                    symbol,
                    direction,
                    real_amount,
                    real_entry,
                    real_leverage
                )
            )

            if not success:

                print(
                    f"[SCALP KORUMA KRİTİK] "
                    f"{symbol} "
                    f"TP/SL emirlerinden biri "
                    f"kurulamadı.",
                    flush=True
                )

        # ====================================================
        # FIRSAT STOP
        # ====================================================

        else:

            stop_price = (
                baslangic_stop_hesapla(
                    direction,
                    real_entry,
                    candidate["atr"],
                    candidate["score"]
                )
            )

            ilk_stop_yerlestir(
                exchange,
                symbol,
                direction,
                real_amount,
                stop_price
            )

        return True

    except Exception as e:

        print(
            f"[POZİSYON AÇMA HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return False


# ============================================================
# GAINERS / LOSERS
#
# DÜZELTİLMİŞ FUTURES TARAMASI
# ============================================================

def top_gainers_losers(
    exchange,
    markets
):

    try:

        tickers = (
            exchange.fetch_tickers()
        )

    except Exception as e:

        print(
            f"[TICKER HATA] "
            f"{e}",
            flush=True
        )

        return [], []

    coin_list = []

    # DEBUG SAYACLARI

    total_tickers = len(
        tickers
    )

    market_match = 0
    active_match = 0
    futures_match = 0
    usdt_match = 0
    valid_symbol_match = 0
    volume_match = 0

    for symbol, ticker in tickers.items():

        original_symbol = symbol

        try:

            symbol = sembol_duzelt(
                symbol
            )

            # ------------------------------------------------
            # MARKET
            # ------------------------------------------------

            if symbol not in markets:

                continue

            market_match += 1

            market = markets[
                symbol
            ]

            # ------------------------------------------------
            # ACTIVE
            # ------------------------------------------------

            if not market.get(
                "active",
                True
            ):

                continue

            active_match += 1

            # ------------------------------------------------
            # CONTRACT
            # ------------------------------------------------

            if not market.get(
                "contract",
                False
            ):

                continue

            # ------------------------------------------------
            # SWAP / PERPETUAL
            # ------------------------------------------------

            if not market.get(
                "swap",
                False
            ):

                continue

            futures_match += 1

            # ------------------------------------------------
            # QUOTE
            # ------------------------------------------------

            quote = market.get(
                "quote"
            )

            if quote != "USDT":

                continue

            usdt_match += 1

            # ------------------------------------------------
            # SEMBOL
            # ------------------------------------------------

            if not gecerli_kripto_mu(
                symbol
            ):

                continue

            valid_symbol_match += 1

            # ------------------------------------------------
            # CHANGE
            # ------------------------------------------------

            change = ticker.get(
                "percentage"
            )

            if change is None:

                info = ticker.get(
                    "info",
                    {}
                )

                change = info.get(
                    "priceChangePercent"
                )

            if change is None:

                continue

            change = float(
                change
            )

            if not np.isfinite(
                change
            ):

                continue

            # ------------------------------------------------
            # HACİM
            # ------------------------------------------------

            quote_volume = (
                ticker.get(
                    "quoteVolume"
                )
                or 0
            )

            try:

                quote_volume = float(
                    quote_volume
                )

            except Exception:

                continue

            if not np.isfinite(
                quote_volume
            ):

                continue

            if quote_volume < 1_000_000:

                continue

            volume_match += 1

            coin_list.append({

                "symbol": symbol,

                "change": change,

                "quoteVolume":
                    quote_volume

            })

        except Exception as e:

            print(
                f"[TICKER COIN HATA] "
                f"{original_symbol}: {e}",
                flush=True
            )

            continue

    # ========================================================
    # TARAMA DEBUG
    # ========================================================

    print(
        f"[TICKER DEBUG] "
        f"Toplam={total_tickers} "
        f"Market={market_match} "
        f"Aktif={active_match} "
        f"Futures={futures_match} "
        f"USDT={usdt_match} "
        f"Geçerli={valid_symbol_match} "
        f"Hacim={volume_match}",
        flush=True
    )

    print(
        f"[TICKER] "
        f"Uygun Futures coin="
        f"{len(coin_list)}",
        flush=True
    )

    # ========================================================
    # GAINERS
    # ========================================================

    coin_list.sort(
        key=lambda x: x["change"],
        reverse=True
    )

    gainers = [
        x["symbol"]
        for x in coin_list[:25]
    ]

    # ========================================================
    # LOSERS
    # ========================================================

    coin_list.sort(
        key=lambda x: x["change"]
    )

    losers = [
        x["symbol"]
        for x in coin_list[:25]
    ]

    return (
        gainers,
        losers
    )


# ============================================================
# UNIQUE
# ============================================================

def unique_symbols(
    gainers,
    losers
):

    result = []

    for symbol in (
        gainers
        +
        losers
    ):

        if symbol not in result:

            result.append(
                symbol
            )

    return result


# ============================================================
# AKTİF POZİSYONLAR
# ============================================================

def aktif_pozisyonlari_getir(
    exchange
):

    positions = (
        exchange.fetch_positions()
    )

    active = []

    for p in positions:

        try:

            contracts = float(
                p.get(
                    "contracts"
                )
                or 0
            )

        except Exception:

            contracts = 0

        if contracts > 0:

            active.append(
                p
            )

    return active


# ============================================================
# ANA ANALİZ
#
# CRON SADECE:
#   ANALİZ
#   YENİ İŞLEM
#
# POZİSYON YÖNETİMİ YOK
# ============================================================

def arka_plan_analiz_islem():

    if not analiz_lock.acquire(
        blocking=False
    ):

        print(
            "[CRON] Önceki analiz hâlâ "
            "çalışıyor. Atlandı.",
            flush=True
        )

        return

    try:

        print(
            "\n======================================",
            flush=True
        )

        print(
            "BOT ANALİZ BAŞLADI",
            datetime.now(
                timezone.utc
            ).isoformat(),
            flush=True
        )

        print(
            "======================================",
            flush=True
        )

        exchange = get_exchange()

        markets = (
            exchange.load_markets()
        )

        # ====================================================
        # BAKİYE
        # ====================================================

        balance = (
            exchange.fetch_balance()
        )

        usdt_info = balance.get(
            "USDT",
            {}
        )

        free_balance = float(
            usdt_info.get(
                "free",
                0
            )
            or 0
        )

        total_balance = float(
            usdt_info.get(
                "total",
                free_balance
            )
            or free_balance
        )

        print(
            f"[BAKİYE] "
            f"Free={free_balance:.2f} "
            f"Total={total_balance:.2f}",
            flush=True
        )

        # ====================================================
        # AKTİF POZİSYONLAR
        # ====================================================

        active_positions = (
            aktif_pozisyonlari_getir(
                exchange
            )
        )

        print(
            f"[POZİSYON] "
            f"Toplam aktif="
            f"{len(active_positions)}",
            flush=True
        )

        scalp_positions = [
            p
            for p in active_positions
            if pozisyon_tipi(p)
            ==
            "scalp"
        ]

        opportunity_positions = [
            p
            for p in active_positions
            if pozisyon_tipi(p)
            ==
            "opportunity"
        ]

        print(
            f"[SAYIM] "
            f"Fırsat="
            f"{len(opportunity_positions)} "
            f"Scalp="
            f"{len(scalp_positions)} "
            f"Toplam="
            f"{len(active_positions)}",
            flush=True
        )

        # ====================================================
        # MAX POZİSYON
        # ====================================================

        if (
            len(active_positions)
            >=
            MAX_TOTAL_POSITIONS
        ):

            print(
                "[KORUMA] "
                "Maksimum pozisyon sayısı aktif.",
                flush=True
            )

            return

        # ====================================================
        # GAINERS / LOSERS
        # ====================================================

        gainers, losers = (
            top_gainers_losers(
                exchange,
                markets
            )
        )

        print(
            f"[GAINERS] "
            f"{gainers}",
            flush=True
        )

        print(
            f"[LOSERS] "
            f"{losers}",
            flush=True
        )

        target_symbols = (
            unique_symbols(
                gainers,
                losers
            )
        )

        # ----------------------------------------------------
        # AKTİF COINLERİ ÇIKAR
        # ----------------------------------------------------

        active_symbols = [
            sembol_duzelt(
                p["symbol"]
            )
            for p in active_positions
        ]

        target_symbols = [
            s
            for s in target_symbols
            if s not in active_symbols
        ]

        print(
            f"[TARAMA] "
            f"{len(target_symbols)} "
            f"benzersiz coin",
            flush=True
        )

        # ====================================================
        # MAKRO
        # ====================================================

        if makro_zaman_engeli_var_mi():

            print(
                "[MAKRO ENGEL] "
                "Yeni işlem açılmayacak. "
                "Pozisyonlar monitor tarafından "
                "yönetiliyor.",
                flush=True
            )

            return

        # ====================================================
        # SCALP
        # ====================================================

        if (
            SCALP_ENABLED
            and
            len(scalp_positions)
            <
            MAX_SCALP_POSITIONS
        ):

            scalp_candidate = (
                vur_kac_sinyali_bul(
                    exchange,
                    target_symbols
                )
            )

            if scalp_candidate:

                print(
                    f"[EN İYİ SCALP] "
                    f"{scalp_candidate['symbol']} "
                    f"{scalp_candidate['direction'].upper()} "
                    f"SKOR="
                    f"{scalp_candidate['score']}",
                    flush=True
                )

                if pozisyon_ac(
                    exchange,
                    scalp_candidate,
                    SCALP_MARGIN,
                    "scalp"
                ):

                    active_positions = (
                        aktif_pozisyonlari_getir(
                            exchange
                        )
                    )

                    if (
                        len(active_positions)
                        >=
                        MAX_TOTAL_POSITIONS
                    ):

                        return

                    scalp_positions = [
                        p
                        for p in active_positions
                        if
                        pozisyon_tipi(p)
                        ==
                        "scalp"
                    ]

            else:

                print(
                    "[SCALP] "
                    "Uygun sinyal bulunamadı.",
                    flush=True
                )

        # ====================================================
        # FIRSAT
        # ====================================================

        active_positions = (
            aktif_pozisyonlari_getir(
                exchange
            )
        )

        opportunity_positions = [
            p
            for p in active_positions
            if
            pozisyon_tipi(p)
            ==
            "opportunity"
        ]

        if (
            len(active_positions)
            >=
            MAX_TOTAL_POSITIONS
        ):

            return

        if (
            len(opportunity_positions)
            >=
            MAX_OPPORTUNITY_POSITIONS
        ):

            print(
                "[FIRSAT] "
                "Zaten aktif fırsat pozisyonu var.",
                flush=True
            )

            return

        if not OPPORTUNITY_ENABLED:

            print(
                "[FIRSAT] "
                "Fırsat modu kapalı.",
                flush=True
            )

            return

        # ====================================================
        # FIRSAT ARA
        # ====================================================

        opportunity_candidate = (
            en_iyi_firsat_bul(
                exchange,
                target_symbols
            )
        )

        if not opportunity_candidate:

            print(
                "[FIRSAT] "
                "Uygun sinyal bulunamadı.",
                flush=True
            )

            return

        print(
            f"[EN İYİ FIRSAT] "
            f"{opportunity_candidate['symbol']} "
            f"{opportunity_candidate['direction'].upper()} "
            f"SKOR="
            f"{opportunity_candidate['score']} "
            f"LONG="
            f"{opportunity_candidate['long_score']} "
            f"SHORT="
            f"{opportunity_candidate['short_score']}",
            flush=True
        )

        print(
            f"[NEDENLER] "
            f"{opportunity_candidate.get('reasons', [])}",
            flush=True
        )

        # ====================================================
        # BAKİYE KORUMA
        # ====================================================

        used_margin = 0

        for p in active_positions:

            try:

                used_margin += float(
                    p.get(
                        "initialMargin",
                        0
                    )
                    or 0
                )

            except Exception:

                pass

        if (
            used_margin
            +
            OPPORTUNITY_MARGIN
            >
            total_balance * 0.60
        ):

            print(
                "[BAKİYE KORUMA] "
                "Yeni fırsat işlemi açılmadı.",
                flush=True
            )

            return

        # ====================================================
        # FIRSAT AÇ
        # ====================================================

        pozisyon_ac(
            exchange,
            opportunity_candidate,
            OPPORTUNITY_MARGIN,
            "opportunity"
        )

        print(
            "======================================",
            flush=True
        )

        print(
            "BOT ANALİZ TAMAMLANDI",
            flush=True
        )

        print(
            "======================================",
            flush=True
        )

    except Exception as e:

        print(
            f"[GENEL BOT HATASI] "
            f"{e}",
            flush=True
        )

    finally:

        try:

            analiz_lock.release()

        except Exception:

            pass


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def health_check():

    return (
        "Binance Futures Bot Aktif - "
        "Scalp + Fırsat Aktif - "
        "Bağımsız Position Monitor",
        200
    )


# ============================================================
# OTOMATİK ANALİZ
#
# SADECE ANALİZİ BAŞLATIR
# ============================================================

@app.route("/otomatik-analiz")
def otomatik_analiz():

    if analiz_lock.locked():

        return jsonify({
            "durum": "Atlandi",
            "mesaj":
                "Önceki analiz hâlâ çalışıyor."
        }), 200

    thread = threading.Thread(
        target=arka_plan_analiz_islem,
        daemon=True
    )

    thread.start()

    return jsonify({
        "durum": "Basarili",
        "mesaj":
            "Analiz arka planda başlatıldı. "
            "Pozisyon monitorü cron'dan "
            "bağımsız çalışıyor."
    })


# ============================================================
# DURUM
# ============================================================

@app.route("/durum")
def durum():

    return jsonify({

        "bot": "aktif",

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "scalp_enabled":
            SCALP_ENABLED,

        "opportunity_enabled":
            OPPORTUNITY_ENABLED,

        "scalp_margin":
            SCALP_MARGIN,

        "scalp_tp_roi":
            SCALP_TP_ROI,

        "scalp_sl_roi":
            SCALP_SL_ROI,

        "opportunity_margin":
            OPPORTUNITY_MARGIN,

        "max_total_positions":
            MAX_TOTAL_POSITIONS,

        "max_scalp_positions":
            MAX_SCALP_POSITIONS,

        "max_opportunity_positions":
            MAX_OPPORTUNITY_POSITIONS,

        "position_monitor":
            monitor_basladi,

        "position_monitor_interval":
            POSITION_MONITOR_INTERVAL
    })


# ============================================================
# RAILWAY
# ============================================================

if __name__ == "__main__":

    # --------------------------------------------------------
    # POZİSYON MONITOR CRON'DAN ÖNCE BAŞLAR
    # --------------------------------------------------------

    monitor_baslat()

    port = int(
        os.environ.get(
            "PORT",
            8080
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )