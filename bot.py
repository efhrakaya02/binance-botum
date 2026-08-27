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
# FIRSAT MODU AKTİF
# SCALP MODU GEÇİCİ OLARAK DEVRE DIŞI
# Railway / Flask / Cron uyumlu
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
# ANA AYARLAR
# ============================================================

# ------------------------------------------------------------
# SCALP
# ------------------------------------------------------------
# ŞİMDİLİK TAMAMEN KAPALI
# ------------------------------------------------------------

SCALP_ENABLED = False

SCALP_MARGIN = 10.0
MAX_SCALP_POSITIONS = 2

# Bu değerler ileride scalp tekrar açıldığında kullanılacak.
SCALP_TP_ROI = 3.0
SCALP_SL_ROI = -1.5

# ------------------------------------------------------------
# FIRSAT
# ------------------------------------------------------------

OPPORTUNITY_ENABLED = True

OPPORTUNITY_MARGIN = 15.0

MAX_TOTAL_POSITIONS = 3

# Aynı anda sadece 1 fırsat pozisyonu
MAX_OPPORTUNITY_POSITIONS = 1

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
cooldown_ms = COOLDOWN_HOURS * 60 * 60 * 1000

son_kapanis_zamanlari = {}

# Pozisyonun gördüğü maksimum ROI
pozisyon_en_yuksek_kar = {}

# Son yerleştirilen stoplar
pozisyon_stoplari = {}

# Aynı anda iki cron çağrısını engeller
analiz_lock = threading.Lock()

# ============================================================
# MAKRO ENGEL
# ============================================================

MACRO_BLOCK_WINDOWS = os.getenv(
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

    if not symbol.endswith("/USDT"):
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

    rs = avg_gain / avg_loss.replace(
        0,
        np.nan
    )

    return 100 - (
        100 / (1 + rs)
    )


# ============================================================
# ATR
# ============================================================

def hesapla_atr(df, period=14):

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
        & (up_move > 0),
        up_move,
        0
    )

    minus_dm = np.where(
        (down_move > up_move)
        & (down_move > 0),
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
        100 *
        plus_dm.ewm(
            alpha=1 / period,
            adjust=False
        ).mean()
        / atr
    )

    minus_di = (
        100 *
        minus_dm.ewm(
            alpha=1 / period,
            adjust=False
        ).mean()
        / atr
    )

    denominator = (
        plus_di +
        minus_di
    ).replace(
        0,
        np.nan
    )

    dx = (
        100 *
        abs(
            plus_di -
            minus_di
        )
        / denominator
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

    histogram = (
        macd -
        signal
    )

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
        direction *
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

    # Bollinger

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
        df["bb_mid"] +
        2 * std
    )

    df["bb_lower"] = (
        df["bb_mid"] -
        2 * std
    )

    df["bb_width"] = (
        (
            df["bb_upper"] -
            df["bb_lower"]
        )
        / df["bb_mid"]
    )

    df["bb_width_ma"] = (
        df["bb_width"]
        .rolling(50)
        .mean()
    )

    df["squeeze"] = (
        df["bb_width"]
        <
        df["bb_width_ma"] * 0.85
    )

    # Hacim

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

    # Son 20 mum

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

    # Momentum

    df["roc"] = (
        df["close"]
        .pct_change(5)
        * 100
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
            or len(data) < 50
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
# MAKRO ZAMAN
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
        now.hour * 60 +
        now.minute
    )

    if MACRO_BLOCK_WINDOWS:

        for window in (
            MACRO_BLOCK_WINDOWS
            .split(",")
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

    # Funding zamanları
    # 00 / 08 / 16 UTC
    # +/- 5 dakika

    if (
        now.minute >= 55
        or now.minute <= 5
    ) and now.hour in [
        0,
        8,
        16
    ]:

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
        son_kapanis_zamanlari
        .get(symbol)
    )

    if not son:
        return False

    return (
        int(time.time() * 1000)
        - son
        <
        cooldown_ms
    )


# ============================================================
# POZİSYON TİPİ
# ============================================================

def pozisyon_tipi(p):

    try:

        margin = float(
            p.get(
                "initialMargin"
            ) or 0
        )

    except Exception:

        margin = 0

    if (
        abs(
            margin -
            SCALP_MARGIN
        ) <= 2.0
    ):

        return "scalp"

    if (
        abs(
            margin -
            OPPORTUNITY_MARGIN
        ) <= 2.5
    ):

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
                ) or 0
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
            or df15 is None
            or df1 is None
            or df4 is None
        ):

            return None

        # Son kapanmış mum

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
        # TREND 4H
        # ====================================================

        trend4_long = (
            d4["close"]
            > d4["ema50"]
            and
            d4["ema50"]
            > d4["ema200"]
        )

        trend4_short = (
            d4["close"]
            < d4["ema50"]
            and
            d4["ema50"]
            < d4["ema200"]
        )

        # ====================================================
        # TREND 1H
        # ====================================================

        trend1_long = (
            d1["close"]
            > d1["ema50"]
            and
            d1["ema9"]
            > d1["ema21"]
        )

        trend1_short = (
            d1["close"]
            < d1["ema50"]
            and
            d1["ema9"]
            < d1["ema21"]
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

        if d1["macd"] > d1["macd_signal"]:

            long_score += 10

            long_reasons.append(
                "MACD pozitif"
            )

        if d1["macd_hist"] > 0:

            long_score += 4

        if d1["plus_di"] > d1["minus_di"]:

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

        if d1["macd"] < d1["macd_signal"]:

            short_score += 10

            short_reasons.append(
                "MACD negatif"
            )

        if d1["macd_hist"] < 0:

            short_score += 4

        if d1["minus_di"] > d1["plus_di"]:

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

        # ====================================================
        # ÇELİŞKİ
        # ====================================================

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
        margin *
        leverage
    )

    raw_amount = (
        notional /
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

        amount = float(amount)

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
            f"[MARGIN] {symbol} -> ISOLATED",
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
# AÇIK STOP EMİRLERİ
# ============================================================

def mevcut_stop_emirlerini_getir(
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

            order_type = (
                order.get("type")
                or ""
            ).upper()

            if order_type in [
                "STOP_MARKET",
                "STOP",
                "TAKE_PROFIT_MARKET",
                "TAKE_PROFIT"
            ]:

                result.append(order)

        return result

    except Exception as e:

        print(
            f"[STOP LISTE HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return []


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
            f"[STOP] "
            f"{symbol} -> {stop_price}",
            flush=True
        )

        return order

    except Exception as e:

        print(
            f"[STOP HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return None


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

        stop_price = float(
            exchange.price_to_precision(
                symbol,
                stop_price
            )
        )

        eski = (
            mevcut_stop_emirlerini_getir(
                exchange,
                symbol
            )
        )

        for order in eski:

            try:

                exchange.cancel_order(
                    order["id"],
                    symbol
                )

            except Exception:

                pass

        close_side = (
            "sell"
            if side == "buy"
            else "buy"
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

        pozisyon_stoplari[
            symbol
        ] = stop_price

        print(
            f"[İLK STOP] "
            f"{symbol} "
            f"{stop_price}",
            flush=True
        )

        return order

    except Exception as e:

        print(
            f"[İLK STOP HATA] "
            f"{symbol}: {e}",
            flush=True
        )

        return None


# ============================================================
# STOP GÜNCELLE
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

        # Stop kötüleşmesin

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

        # Fiyatın yanlış tarafına stop koyma

        if side == "buy":

            if yeni_stop >= current_price:

                yeni_stop = (
                    current_price *
                    0.998
                )

        else:

            if yeni_stop <= current_price:

                yeni_stop = (
                    current_price *
                    1.002
                )

        eski = (
            mevcut_stop_emirlerini_getir(
                exchange,
                symbol
            )
        )

        for order in eski:

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

            pozisyon_stoplari[
                symbol
            ] = yeni_stop

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
            entry_price -
            atr * multiplier
        )

    else:

        return (
            entry_price +
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
            current_price -
            entry_price
        ) / entry_price

    else:

        price_change = (
            entry_price -
            current_price
        ) / entry_price

    return (
        price_change *
        100 *
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

    locked_roi = (
        int(roi // 5) - 1
    ) * 5

    if locked_roi < 0:

        locked_roi = 0

    price_pct = (
        locked_roi /
        max(leverage, 1)
    )

    if side == "long":

        return (
            entry_price *
            (
                1 +
                price_pct /
                100
            )
        )

    else:

        return (
            entry_price *
            (
                1 -
                price_pct /
                100
            )
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
                ) or 0
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
    # KAPANANLAR
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

    # ========================================================
    # AKTİF POZİSYONLAR
    # ========================================================

    for p in positions:

        try:

            contracts = float(
                p.get(
                    "contracts"
                ) or 0
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
                p.get(
                    "leverage"
                ) or 1
            )

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

            close_side = (
                "sell"
                if side == "long"
                else "buy"
            )

            # =================================================
            # SCALP
            # =================================================
            #
            # SCALP_ENABLED = False olduğu için
            # YENİ SCALP AÇILMAZ.
            #
            # Fakat hesapta eski bir scalp pozisyonu
            # kaldıysa burada yönetilir.
            # =================================================

            if ptype == "scalp":

                print(
                    f"[SCALP] "
                    f"Yeni scalp açılması KAPALI. "
                    f"Mevcut pozisyon yönetiliyor.",
                    flush=True
                )

                # %3 ROI TP

                if roi >= SCALP_TP_ROI:

                    try:

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

                        print(
                            f"[SCALP TP] "
                            f"{symbol} "
                            f"ROI %{roi:.2f} "
                            f"-> KAPATILDI",
                            flush=True
                        )

                        son_kapanis_zamanlari[
                            symbol
                        ] = int(
                            time.time() * 1000
                        )

                        continue

                    except Exception as e:

                        print(
                            f"[SCALP TP HATA] "
                            f"{symbol}: {e}",
                            flush=True
                        )

                # -%1.5 ROI SL

                if roi <= SCALP_SL_ROI:

                    try:

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

                        print(
                            f"[SCALP SL] "
                            f"{symbol} "
                            f"ROI %{roi:.2f} "
                            f"-> KAPATILDI",
                            flush=True
                        )

                        son_kapanis_zamanlari[
                            symbol
                        ] = int(
                            time.time() * 1000
                        )

                        continue

                    except Exception as e:

                        print(
                            f"[SCALP SL HATA] "
                            f"{symbol}: {e}",
                            flush=True
                        )

                continue

            # =================================================
            # FIRSAT
            # =================================================

            if ptype == "opportunity":

                # Acil koruma

                if roi <= -25:

                    try:

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

                        print(
                            f"[ACİL KORUMA] "
                            f"{symbol} "
                            f"ROI %{roi:.2f}",
                            flush=True
                        )

                        son_kapanis_zamanlari[
                            symbol
                        ] = int(
                            time.time() * 1000
                        )

                        continue

                    except Exception as e:

                        print(
                            f"[ACİL KORUMA HATA] "
                            f"{symbol}: {e}",
                            flush=True
                        )

                # Trailing

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

                # Zirveden geri çekilme

                if highest >= 20:

                    if (
                        highest -
                        roi
                        >= 10
                    ):

                        try:

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

                            print(
                                f"[TRAIL EXIT] "
                                f"{symbol} "
                                f"Max %{highest:.2f} "
                                f"Current %{roi:.2f}",
                                flush=True
                            )

                            son_kapanis_zamanlari[
                                symbol
                            ] = int(
                                time.time() * 1000
                            )

                            continue

                        except Exception as e:

                            print(
                                f"[TRAIL EXIT HATA] "
                                f"{symbol}: {e}",
                                flush=True
                            )

        except Exception as e:

            print(
                f"[POZİSYON YÖNETİM HATA] "
                f"{p.get('symbol')}: {e}",
                flush=True
            )


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

    # ========================================================
    # SCALP TAMAMEN KAPALI
    # ========================================================

    print(
        "[SCALP] MOD DEVRE DIŞI. "
        "Yeni scalp işlemi aranmayacak.",
        flush=True
    )

    return None


# ============================================================
# POZİSYON AÇ
# ============================================================

def pozisyon_ac(
    exchange,
    candidate,
    margin,
    position_type
):

    # SCALP KORUMASI

    if (
        position_type == "scalp"
        and
        not SCALP_ENABLED
    ):

        print(
            "[SCALP KAPALI] "
            "İşlem açılması engellendi.",
            flush=True
        )

        return False

    # FIRSAT KORUMASI

    if (
        position_type == "opportunity"
        and
        not OPPORTUNITY_ENABLED
    ):

        print(
            "[FIRSAT KAPALI] "
            "İşlem açılması engellendi.",
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
            amount *
            price /
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
        # MARKET EMİR
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
                        if
                        position_type
                        ==
                        "opportunity"
                        else
                        "BOT_S_"
                    )
                    +
                    str(
                        int(
                            time.time()
                            * 1000
                        )
                    )
            }
        )

        print(
            f"!!! "
            f"{position_type.upper()} "
            f"AÇILDI !!! "
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
                    ) or 0
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

        else:

            real_entry = price
            real_amount = amount

        # ====================================================
        # STOP
        # ====================================================

        if (
            position_type
            ==
            "opportunity"
        ):

            stop_price = (
                baslangic_stop_hesapla(
                    direction,
                    real_entry,
                    candidate["atr"],
                    candidate["score"]
                )
            )

        else:

            # Scalp şu an kapalı.
            # Güvenlik amaçlı stop hesaplama.

            if direction == "buy":

                stop_price = (
                    real_entry *
                    (
                        1 -
                        0.015 /
                        leverage
                    )
                )

            else:

                stop_price = (
                    real_entry *
                    (
                        1 +
                        0.015 /
                        leverage
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
            f"[TICKER HATA] {e}",
            flush=True
        )

        return [], []

    coin_list = []

    for symbol, ticker in (
        tickers.items()
    ):

        try:

            symbol = sembol_duzelt(
                symbol
            )

            if symbol not in markets:

                continue

            if not gecerli_kripto_mu(
                symbol
            ):

                continue

            market = markets[
                symbol
            ]

            if not market.get(
                "active",
                True
            ):

                continue

            info = ticker.get(
                "info",
                {}
            )

            change = info.get(
                "priceChangePercent"
            )

            if change is None:

                change = ticker.get(
                    "percentage"
                )

            if change is None:

                continue

            change = float(
                change
            )

            quote_volume = (
                ticker.get(
                    "quoteVolume"
                )
                or 0
            )

            quote_volume = float(
                quote_volume
            )

            if quote_volume < 1_000_000:

                continue

            coin_list.append({
                "symbol": symbol,
                "change": change,
                "quoteVolume":
                    quote_volume
            })

        except Exception:

            continue

    coin_list.sort(
        key=lambda x: x["change"],
        reverse=True
    )

    gainers = [
        x["symbol"]
        for x in coin_list[:25]
    ]

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
        gainers +
        losers
    ):

        if symbol not in result:

            result.append(
                symbol
            )

    return result


# ============================================================
# ANA BOT
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
            ) or 0
        )

        total_balance = float(
            usdt_info.get(
                "total",
                free_balance
            ) or free_balance
        )

        print(
            f"[BAKİYE] "
            f"Free={free_balance:.2f} "
            f"Total={total_balance:.2f}",
            flush=True
        )

        # ====================================================
        # POZİSYONLAR
        # ====================================================

        positions = (
            exchange.fetch_positions()
        )

        active_positions = []

        for p in positions:

            try:

                contracts = float(
                    p.get(
                        "contracts"
                    ) or 0
                )

            except Exception:

                contracts = 0

            if contracts > 0:

                active_positions.append(
                    p
                )

        print(
            f"[POZİSYON] "
            f"Toplam aktif="
            f"{len(active_positions)}",
            flush=True
        )

        # ====================================================
        # POZİSYONLARI YÖNET
        # ====================================================

        pozisyonlari_yonet(
            exchange,
            active_positions
        )

        # ====================================================
        # YENİDEN POZİSYONLARI AL
        # ====================================================

        positions = (
            exchange.fetch_positions()
        )

        active_positions = []

        for p in positions:

            try:

                contracts = float(
                    p.get(
                        "contracts"
                    ) or 0
                )

            except Exception:

                contracts = 0

            if contracts > 0:

                active_positions.append(
                    p
                )

        # ====================================================
        # SAYIM
        # ====================================================

        scalp_positions = [
            p
            for p in active_positions
            if
            pozisyon_tipi(p)
            ==
            "scalp"
        ]

        opportunity_positions = [
            p
            for p in active_positions
            if
            pozisyon_tipi(p)
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
        # SCALP DURUMU
        # ====================================================

        print(
            "[SCALP] "
            "Yeni scalp işlemleri "
            "GEÇİCİ OLARAK DEVRE DIŞI.",
            flush=True
        )

        # ====================================================
        # MAKS POZİSYON
        # ====================================================

        if (
            len(active_positions)
            >= MAX_TOTAL_POSITIONS
        ):

            print(
                "[KORUMA] "
                "Maksimum pozisyon sayısı "
                "aktif. Yeni işlem yok.",
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
            f"[GAINERS] {gainers}",
            flush=True
        )

        print(
            f"[LOSERS] {losers}",
            flush=True
        )

        target_symbols = (
            unique_symbols(
                gainers,
                losers
            )
        )

        # Aktif sembolleri çıkar

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
                "Mevcut pozisyonlar yönetildi.",
                flush=True
            )

            return

        # ====================================================
        # SCALP BURADA BİLEREK ÇAĞRILMIYOR
        # ====================================================

        if not SCALP_ENABLED:

            print(
                "[SCALP] "
                "Tarama yapılmadı. "
                "SCALP_ENABLED=False",
                flush=True
            )

        # ====================================================
        # FIRSAT POZİSYONU KONTROL
        # ====================================================

        if (
            len(opportunity_positions)
            >=
            MAX_OPPORTUNITY_POSITIONS
        ):

            print(
                "[FIRSAT] "
                "Zaten aktif fırsat "
                "pozisyonu var.",
                flush=True
            )

            return

        # ====================================================
        # POZİSYON SAYISI
        # ====================================================

        positions_now = (
            exchange.fetch_positions()
        )

        active_now = []

        for p in positions_now:

            try:

                contracts = float(
                    p.get(
                        "contracts"
                    ) or 0
                )

            except Exception:

                contracts = 0

            if contracts > 0:

                active_now.append(
                    p
                )

        if (
            len(active_now)
            >=
            MAX_TOTAL_POSITIONS
        ):

            return

        # ====================================================
        # FIRSAT ARA
        # ====================================================

        if not OPPORTUNITY_ENABLED:

            print(
                "[FIRSAT] "
                "Fırsat modu kapalı.",
                flush=True
            )

            return

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

        for p in active_now:

            try:

                used_margin += float(
                    p.get(
                        "initialMargin",
                        0
                    ) or 0
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
        "Scalp Kapalı / Fırsat Açık",
        200
    )


# ============================================================
# OTOMATİK ANALİZ
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
            "Analiz arka planda başlatıldı."
    }), 200


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

        "opportunity_margin":
            OPPORTUNITY_MARGIN,

        "scalp_margin":
            SCALP_MARGIN,

        "scalp_tp_roi":
            SCALP_TP_ROI,

        "scalp_sl_roi":
            SCALP_SL_ROI,

        "max_total_positions":
            MAX_TOTAL_POSITIONS,

        "max_scalp_positions":
            MAX_SCALP_POSITIONS
    })


# ============================================================
# RAILWAY
# ============================================================

if __name__ == "__main__":

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