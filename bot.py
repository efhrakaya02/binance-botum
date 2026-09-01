import os
import time
import math
import logging
import threading
import traceback
from datetime import datetime, timezone, timedelta

import ccxt
import pandas as pd
import numpy as np
from flask import Flask, jsonify


# ============================================================
# BINANCE FUTURES EARLY-MOMENTUM BOT
# ============================================================
#
# GAINERS  : ilk 25
# LOSERS   : ilk 25
# VOLUME   : ilk 25
#
# BTC / XAU hariç
#
# Maksimum:
#   2 açık pozisyon
#   10 USDT margin / pozisyon
#   maksimum 5x
#
# LONG / SHORT
#
# 1m + 5m + 15m + 1h teyit
#
# ATR + momentum trailing stop
#
# DRY_RUN = TRUE
#
# EKLENDİ:
#   SAATLİK İŞLEM ÖZET RAPORU
#
# ============================================================


# ============================================================
# CONFIG
# ============================================================

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")

TIMEFRAME_FAST = "1m"
TIMEFRAME_ENTRY = "5m"
TIMEFRAME_CONFIRM = "15m"
TIMEFRAME_TREND = "1h"

SCAN_INTERVAL = 20
POSITION_MONITOR_INTERVAL = 1.0

MARGIN_PER_POSITION = 10.0

MAX_LEVERAGE = 5
MIN_LEVERAGE = 2

MAX_POSITIONS = 2

MIN_LONG_SCORE = 72
MIN_SHORT_SCORE = 72

EARLY_ENTRY_SCORE = 76

MAX_ABS_FUNDING = 0.0015

COOLDOWN_MINUTES = 60

MIN_QUOTE_VOLUME = 2_000_000

MAX_SPREAD_PERCENT = 0.15

MIN_PROFIT_TO_TRAIL = 0.004
HARD_STOP_ATR = 1.8

TRAIL_ATR_MULTIPLIER = 1.35
TRAIL_ATR_TIGHT = 1.05

TRAIL_LEVEL_1 = 0.008
TRAIL_LEVEL_2 = 0.015
TRAIL_LEVEL_3 = 0.025

EMERGENCY_REVERSE_THRESHOLD = 0.007

MAX_CORRELATED_SIDE = 2

OHLCV_CACHE_SECONDS = 12

MAX_DETAILED_CANDIDATES = 45

LIST_LIMIT = 25

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# ============================================================
# HOURLY REPORT SETTINGS
# ============================================================

# Rapor tam saat başlarında yazılır.
HOURLY_REPORT_ENABLED = True

# Kaç işlem geçmişte tutulacak
MAX_TRADE_HISTORY = 1000

# Raporun saat başına ne kadar yakın çalışacağı
HOURLY_REPORT_INTERVAL = 5


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("EARLY_MOMENTUM_BOT")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# EXCHANGE
# ============================================================

exchange = ccxt.binance({
    "apiKey": API_KEY,
    "secret": API_SECRET,

    "enableRateLimit": True,

    "options": {
        "defaultType": "future",
        "adjustForTimeDifference": True,
    },
})


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.RLock()

positions = {}

cooldowns = {}

ohlcv_cache = {}

trade_history = []

last_scan_time = None

last_hourly_report_hour = None

bot_started_at = datetime.now(timezone.utc).isoformat()


stats = {
    "scans": 0,
    "signals": 0,

    "simulated_entries": 0,
    "simulated_exits": 0,

    "wins": 0,
    "losses": 0,

    "total_realized_pnl": 0.0,

    "total_volume": 0.0,

    "total_trade_seconds": 0.0,
}


# ============================================================
# HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default

        return float(value)

    except Exception:
        return default


def clamp(value, low, high):
    return max(low, min(high, value))


def pct_change(a, b):

    if not a:
        return 0.0

    return ((b - a) / a) * 100.0


def symbol_clean(symbol):

    return (
        symbol
        .replace("/", "")
        .replace(":USDT", "")
    )


# ============================================================
# DURATION
# ============================================================

def calculate_duration_seconds(opened_at, closed_at):

    try:

        start = datetime.fromisoformat(
            opened_at
        )

        end = datetime.fromisoformat(
            closed_at
        )

        return max(
            0,
            (end - start).total_seconds()
        )

    except Exception:

        return 0


def format_duration(seconds):

    seconds = int(max(0, seconds))

    days = seconds // 86400
    seconds %= 86400

    hours = seconds // 3600
    seconds %= 3600

    minutes = seconds // 60
    seconds %= 60

    if days > 0:
        return (
            f"{days}g "
            f"{hours}s "
            f"{minutes}dk"
        )

    if hours > 0:
        return (
            f"{hours}s "
            f"{minutes}dk "
            f"{seconds}sn"
        )

    if minutes > 0:
        return (
            f"{minutes}dk "
            f"{seconds}sn"
        )

    return f"{seconds}sn"


# ============================================================
# SYMBOL FILTER
# ============================================================

def valid_symbol(symbol, market=None):

    try:

        if not symbol:
            return False

        s = symbol.upper()

        if "/USDT" not in s:
            return False

        banned = [
            "BTC/USDT",
            "XAU/USDT",
            "XAUT/USDT",
            "BTCDOM",
            "UP/",
            "DOWN/",
            "BULL/",
            "BEAR/",
            "_",
        ]

        for x in banned:

            if x in s:
                return False

        if market:

            if not market.get(
                "active",
                True
            ):
                return False

            if market.get(
                "quote"
            ) != "USDT":
                return False

            if market.get(
                "settle"
            ) not in (
                None,
                "USDT"
            ):
                return False

        return True

    except Exception:

        return False


# ============================================================
# MARKETS
# ============================================================

def load_markets():

    logger.info(
        "Binance Futures marketleri yükleniyor..."
    )

    markets = exchange.load_markets()

    logger.info(
        "Toplam market: %s",
        len(markets)
    )

    return markets


# ============================================================
# TICKERS
# ============================================================

def get_futures_tickers():

    try:

        tickers = exchange.fetch_tickers()

        result = {}

        for symbol, ticker in tickers.items():

            market = exchange.markets.get(
                symbol
            )

            if not valid_symbol(
                symbol,
                market
            ):
                continue

            quote_volume = safe_float(
                ticker.get(
                    "quoteVolume"
                )
            )

            last = safe_float(
                ticker.get("last")
            )

            percentage = safe_float(
                ticker.get(
                    "percentage"
                )
            )

            bid = safe_float(
                ticker.get("bid")
            )

            ask = safe_float(
                ticker.get("ask")
            )

            if last <= 0:
                continue

            if quote_volume < MIN_QUOTE_VOLUME:
                continue

            spread = 0.0

            if bid > 0 and ask > 0:

                spread = (
                    (ask - bid)
                    /
                    ((ask + bid) / 2)
                ) * 100

            if spread > MAX_SPREAD_PERCENT:
                continue

            result[symbol] = {
                "symbol": symbol,
                "last": last,
                "percentage": percentage,
                "quoteVolume": quote_volume,
                "bid": bid,
                "ask": ask,
                "spread": spread,
                "high": safe_float(
                    ticker.get("high")
                ),
                "low": safe_float(
                    ticker.get("low")
                ),
                "open": safe_float(
                    ticker.get("open")
                ),
            }

        return result

    except Exception as e:

        logger.error(
            "Ticker alınamadı: %s",
            e
        )

        return {}


# ============================================================
# RANK LISTS
# ============================================================

def build_rank_lists(tickers):

    data = list(
        tickers.values()
    )

    gainers = sorted(
        data,
        key=lambda x: x["percentage"],
        reverse=True
    )[:LIST_LIMIT]

    losers = sorted(
        data,
        key=lambda x: x["percentage"]
    )[:LIST_LIMIT]

    volumes = sorted(
        data,
        key=lambda x: x["quoteVolume"],
        reverse=True
    )[:LIST_LIMIT]

    return gainers, losers, volumes


# ============================================================
# CANDIDATE POOL
# ============================================================

def build_candidate_pool(
    gainers,
    losers,
    volumes
):

    candidates = {}

    def add(items, source):

        for rank, item in enumerate(
            items,
            start=1
        ):

            symbol = item["symbol"]

            if symbol not in candidates:

                candidates[symbol] = {
                    "symbol": symbol,
                    "sources": [],
                    "gainer_rank": None,
                    "loser_rank": None,
                    "volume_rank": None,
                    "ticker": item,
                }

            candidates[
                symbol
            ]["sources"].append(
                source
            )

            if source == "GAINER":

                candidates[
                    symbol
                ]["gainer_rank"] = rank

            elif source == "LOSER":

                candidates[
                    symbol
                ]["loser_rank"] = rank

            elif source == "VOLUME":

                candidates[
                    symbol
                ]["volume_rank"] = rank

    add(
        gainers,
        "GAINER"
    )

    add(
        losers,
        "LOSER"
    )

    add(
        volumes,
        "VOLUME"
    )

    return candidates


# ============================================================
# OHLCV CACHE
# ============================================================

def fetch_ohlcv_cached(
    symbol,
    timeframe,
    limit=160
):

    key = (
        symbol,
        timeframe
    )

    current = time.time()

    cached = ohlcv_cache.get(
        key
    )

    if cached:

        timestamp, data = cached

        if (
            current - timestamp
            <
            OHLCV_CACHE_SECONDS
        ):

            return data

    try:

        data = exchange.fetch_ohlcv(
            symbol,
            timeframe=timeframe,
            limit=limit
        )

        if not data:
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

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            unit="ms",
            utc=True
        )

        for col in [
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        df = (
            df
            .dropna()
            .reset_index(
                drop=True
            )
        )

        ohlcv_cache[key] = (
            current,
            df
        )

        return df

    except Exception as e:

        logger.debug(
            "OHLCV hata %s %s: %s",
            symbol,
            timeframe,
            e
        )

        return None


# ============================================================
# INDICATORS
# ============================================================

def ema(
    series,
    period
):

    return series.ewm(
        span=period,
        adjust=False
    ).mean()


def rsi(
    series,
    period=14
):

    delta = series.diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

    avg_gain = gain.ewm(
        alpha=1 / period,
        min_periods=period,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / period,
        min_periods=period,
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

    result = (
        100
        -
        (
            100
            /
            (1 + rs)
        )
    )

    return result.fillna(50)


def atr(
    df,
    period=14
):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    previous_close = (
        close.shift(1)
    )

    tr = pd.concat(
        [
            high - low,
            (
                high
                -
                previous_close
            ).abs(),
            (
                low
                -
                previous_close
            ).abs(),
        ],
        axis=1
    ).max(axis=1)

    return tr.ewm(
        alpha=1 / period,
        min_periods=period,
        adjust=False
    ).mean()


def macd(series):

    fast = ema(
        series,
        12
    )

    slow = ema(
        series,
        26
    )

    line = fast - slow

    signal = ema(
        line,
        9
    )

    histogram = (
        line - signal
    )

    return (
        line,
        signal,
        histogram
    )


def bollinger(
    series,
    period=20,
    std_mult=2
):

    mid = (
        series
        .rolling(period)
        .mean()
    )

    std = (
        series
        .rolling(period)
        .std()
    )

    upper = (
        mid
        +
        std_mult * std
    )

    lower = (
        mid
        -
        std_mult * std
    )

    return (
        mid,
        upper,
        lower
    )


def adx(
    df,
    period=14
):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where(
        (
            up_move
            >
            down_move
        )
        &
        (
            up_move
            >
            0
        ),
        up_move,
        0
    )

    minus_dm = np.where(
        (
            down_move
            >
            up_move
        )
        &
        (
            down_move
            >
            0
        ),
        down_move,
        0
    )

    tr1 = high - low

    tr2 = (
        high
        -
        close.shift()
    ).abs()

    tr3 = (
        low
        -
        close.shift()
    ).abs()

    tr = pd.concat(
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(axis=1)

    atr_val = tr.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    plus_di = (
        100
        *
        pd.Series(
            plus_dm,
            index=df.index
        )
        .ewm(
            alpha=1 / period,
            adjust=False
        )
        .mean()
        /
        atr_val
    )

    minus_di = (
        100
        *
        pd.Series(
            minus_dm,
            index=df.index
        )
        .ewm(
            alpha=1 / period,
            adjust=False
        )
        .mean()
        /
        atr_val
    )

    dx = (
        100
        *
        (
            plus_di
            -
            minus_di
        ).abs()
        /
        (
            plus_di
            +
            minus_di
        ).replace(
            0,
            np.nan
        )
    )

    return dx.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()


# ============================================================
# INDICATOR FRAME
# ============================================================

def calculate_indicators(df):

    if (
        df is None
        or
        len(df) < 80
    ):
        return None

    x = df.copy()

    x["ema9"] = ema(
        x["close"],
        9
    )

    x["ema21"] = ema(
        x["close"],
        21
    )

    x["ema50"] = ema(
        x["close"],
        50
    )

    x["ema200"] = ema(
        x["close"],
        200
    )

    x["rsi"] = rsi(
        x["close"]
    )

    x["atr"] = atr(x)

    (
        macd_line,
        macd_signal,
        macd_hist
    ) = macd(
        x["close"]
    )

    x["macd"] = macd_line
    x["macd_signal"] = macd_signal
    x["macd_hist"] = macd_hist

    x["adx"] = adx(x)

    (
        bb_mid,
        bb_upper,
        bb_lower
    ) = bollinger(
        x["close"]
    )

    x["bb_mid"] = bb_mid
    x["bb_upper"] = bb_upper
    x["bb_lower"] = bb_lower

    x["volume_ma20"] = (
        x["volume"]
        .rolling(20)
        .mean()
    )

    x["volume_ratio"] = (
        x["volume"]
        /
        x["volume_ma20"]
        .replace(
            0,
            np.nan
        )
    )

    x["roc5"] = (
        x["close"]
        .pct_change(5)
        * 100
    )

    x["roc10"] = (
        x["close"]
        .pct_change(10)
        * 100
    )

    x["high20"] = (
        x["high"]
        .rolling(20)
        .max()
        .shift(1)
    )

    x["low20"] = (
        x["low"]
        .rolling(20)
        .min()
        .shift(1)
    )

    x["high50"] = (
        x["high"]
        .rolling(50)
        .max()
        .shift(1)
    )

    x["low50"] = (
        x["low"]
        .rolling(50)
        .min()
        .shift(1)
    )

    x["range"] = (
        x["high"]
        -
        x["low"]
    )

    x["body"] = (
        x["close"]
        -
        x["open"]
    ).abs()

    x["body_ratio"] = (
        x["body"]
        /
        x["range"]
        .replace(
            0,
            np.nan
        )
    )

    x["bb_width"] = (
        (
            x["bb_upper"]
            -
            x["bb_lower"]
        )
        /
        x["bb_mid"]
        .replace(
            0,
            np.nan
        )
    )

    return x


# ============================================================
# CANDLE QUALITY
# ============================================================

def bullish_reversal(df):

    if len(df) < 3:
        return False

    a = df.iloc[-2]
    b = df.iloc[-1]

    return (
        a["close"]
        <
        a["open"]
        and
        b["close"]
        >
        b["open"]
        and
        b["close"]
        >
        a["open"]
    )


def bearish_reversal(df):

    if len(df) < 3:
        return False

    a = df.iloc[-2]
    b = df.iloc[-1]

    return (
        a["close"]
        >
        a["open"]
        and
        b["close"]
        <
        b["open"]
        and
        b["close"]
        <
        a["open"]
    )


# ============================================================
# LONG SCORE
# ============================================================

def score_long(
    df1,
    df5,
    df15,
    df1h
):

    score = 0
    reasons = []

    a = df1.iloc[-1]
    b = df5.iloc[-1]
    c = df15.iloc[-1]
    d = df1h.iloc[-1]

    # 1H
    if d["ema21"] > d["ema50"]:
        score += 8
        reasons.append(
            "1H EMA21>EMA50"
        )

    if d["close"] > d["ema50"]:
        score += 6
        reasons.append(
            "1H above EMA50"
        )

    if d["ema9"] > d["ema21"]:
        score += 4
        reasons.append(
            "1H short trend up"
        )

    # 15M
    if c["ema9"] > c["ema21"]:
        score += 7
        reasons.append(
            "15M EMA9>EMA21"
        )

    if c["close"] > c["ema21"]:
        score += 4
        reasons.append(
            "15M above EMA21"
        )

    if c["rsi"] > 52:
        score += 4
        reasons.append(
            "15M RSI bullish"
        )

    if c["macd_hist"] > 0:
        score += 4
        reasons.append(
            "15M MACD positive"
        )

    if c["adx"] > 18:
        score += 4
        reasons.append(
            "15M ADX"
        )

    # 5M
    if b["ema9"] > b["ema21"]:
        score += 7
        reasons.append(
            "5M EMA bullish"
        )

    if b["close"] > b["ema9"]:
        score += 3
        reasons.append(
            "5M above EMA9"
        )

    if b["rsi"] > 52:
        score += 4
        reasons.append(
            "5M RSI"
        )

    if 52 <= b["rsi"] <= 72:
        score += 3
        reasons.append(
            "5M RSI optimal"
        )

    if b["macd_hist"] > 0:
        score += 5
        reasons.append(
            "5M MACD"
        )

    if (
        b["macd_hist"]
        >
        df5["macd_hist"].iloc[-2]
    ):
        score += 3
        reasons.append(
            "MACD accelerating"
        )

    # Volume
    if b["volume_ratio"] >= 1.30:
        score += 6
        reasons.append(
            "Volume expansion"
        )

    elif b["volume_ratio"] >= 1.10:
        score += 3
        reasons.append(
            "Volume improving"
        )

    # Breakout
    if b["close"] > b["high20"]:
        score += 8
        reasons.append(
            "20 candle breakout"
        )

    elif b["high"] > b["high20"]:
        score += 4
        reasons.append(
            "Breakout attempt"
        )

    # Early momentum
    if 0.15 <= b["roc5"] <= 2.5:
        score += 4
        reasons.append(
            "Early ROC"
        )

    if b["roc10"] > 0:
        score += 3
        reasons.append(
            "ROC10 positive"
        )

    if bullish_reversal(df5):
        score += 6
        reasons.append(
            "Bullish reversal"
        )

    # 1M
    if a["ema9"] > a["ema21"]:
        score += 4
        reasons.append(
            "1M momentum"
        )

    if a["macd_hist"] > 0:
        score += 3
        reasons.append(
            "1M MACD"
        )

    # Overextension
    if b["rsi"] > 78:
        score -= 10
        reasons.append(
            "Overbought penalty"
        )

    if c["rsi"] > 78:
        score -= 8
        reasons.append(
            "15M overbought"
        )

    return score, reasons


# ============================================================
# SHORT SCORE
# ============================================================

def score_short(
    df1,
    df5,
    df15,
    df1h
):

    score = 0
    reasons = []

    a = df1.iloc[-1]
    b = df5.iloc[-1]
    c = df15.iloc[-1]
    d = df1h.iloc[-1]

    # 1H
    if d["ema21"] < d["ema50"]:
        score += 8
        reasons.append(
            "1H EMA21<EMA50"
        )

    if d["close"] < d["ema50"]:
        score += 6
        reasons.append(
            "1H below EMA50"
        )

    if d["ema9"] < d["ema21"]:
        score += 4
        reasons.append(
            "1H short trend down"
        )

    # 15M
    if c["ema9"] < c["ema21"]:
        score += 7
        reasons.append(
            "15M EMA bearish"
        )

    if c["close"] < c["ema21"]:
        score += 4
        reasons.append(
            "15M below EMA21"
        )

    if c["rsi"] < 48:
        score += 4
        reasons.append(
            "15M RSI bearish"
        )

    if c["macd_hist"] < 0:
        score += 4
        reasons.append(
            "15M MACD negative"
        )

    if c["adx"] > 18:
        score += 4
        reasons.append(
            "15M ADX"
        )

    # 5M
    if b["ema9"] < b["ema21"]:
        score += 7
        reasons.append(
            "5M EMA bearish"
        )

    if b["close"] < b["ema9"]:
        score += 3
        reasons.append(
            "5M below EMA9"
        )

    if b["rsi"] < 48:
        score += 4
        reasons.append(
            "5M RSI"
        )

    if 28 <= b["rsi"] <= 48:
        score += 3
        reasons.append(
            "5M RSI optimal"
        )

    if b["macd_hist"] < 0:
        score += 5
        reasons.append(
            "5M MACD"
        )

    if (
        b["macd_hist"]
        <
        df5["macd_hist"].iloc[-2]
    ):
        score += 3
        reasons.append(
            "MACD accelerating down"
        )

    # Volume
    if b["volume_ratio"] >= 1.30:
        score += 6
        reasons.append(
            "Volume expansion"
        )

    elif b["volume_ratio"] >= 1.10:
        score += 3
        reasons.append(
            "Volume improving"
        )

    # Breakdown
    if b["close"] < b["low20"]:
        score += 8
        reasons.append(
            "20 candle breakdown"
        )

    elif b["low"] < b["low20"]:
        score += 4
        reasons.append(
            "Breakdown attempt"
        )

    # Momentum
    if -2.5 <= b["roc5"] <= -0.15:
        score += 4
        reasons.append(
            "Early negative ROC"
        )

    if b["roc10"] < 0:
        score += 3
        reasons.append(
            "ROC10 negative"
        )

    if bearish_reversal(df5):
        score += 6
        reasons.append(
            "Bearish reversal"
        )

    # 1M
    if a["ema9"] < a["ema21"]:
        score += 4
        reasons.append(
            "1M momentum"
        )

    if a["macd_hist"] < 0:
        score += 3
        reasons.append(
            "1M MACD"
        )

    # Oversold
    if b["rsi"] < 22:
        score -= 10
        reasons.append(
            "Oversold penalty"
        )

    if c["rsi"] < 22:
        score -= 8
        reasons.append(
            "15M oversold"
        )

    return score, reasons


# ============================================================
# FUNDING
# ============================================================

def get_funding(symbol):

    try:

        data = exchange.fetch_funding_rate(
            symbol
        )

        return safe_float(
            data.get(
                "fundingRate"
            )
        )

    except Exception:

        return 0.0


# ============================================================
# PRE SCORE
# ============================================================

def preliminary_score(
    candidate
):

    ticker = candidate["ticker"]

    score = 0

    pct = ticker["percentage"]

    volume = ticker[
        "quoteVolume"
    ]

    sources = candidate[
        "sources"
    ]

    if "GAINER" in sources:
        score += 8

    if "LOSER" in sources:
        score += 8

    if "VOLUME" in sources:
        score += 8

    if abs(pct) >= 3:
        score += 10

    elif abs(pct) >= 2:
        score += 7

    elif abs(pct) >= 1:
        score += 4

    if volume >= 100_000_000:
        score += 8

    elif volume >= 50_000_000:
        score += 6

    elif volume >= 20_000_000:
        score += 4

    return score


# ============================================================
# COOLDOWN
# ============================================================

def is_cooldown(symbol):

    t = cooldowns.get(
        symbol
    )

    if not t:
        return False

    return (
        time.time()
        <
        t
    )


def set_cooldown(symbol):

    cooldowns[
        symbol
    ] = (
        time.time()
        +
        COOLDOWN_MINUTES * 60
    )


# ============================================================
# POSITION COUNT
# ============================================================

def current_position_count():

    with state_lock:

        return len(
            positions
        )


# ============================================================
# SIDE COUNT
# ============================================================

def side_count(side):

    with state_lock:

        return sum(
            1
            for p in positions.values()
            if p["side"] == side
        )


# ============================================================
# LEVERAGE
# ============================================================

def choose_leverage(
    score,
    atr_percent
):

    leverage = 3

    if score >= 88:
        leverage = 5

    elif score >= 82:
        leverage = 4

    elif score >= 76:
        leverage = 3

    else:
        leverage = 2

    if atr_percent >= 2.5:
        leverage -= 1

    if atr_percent >= 4:
        leverage -= 1

    leverage = int(
        clamp(
            leverage,
            MIN_LEVERAGE,
            MAX_LEVERAGE
        )
    )

    return leverage


# ============================================================
# CAN OPEN
# ============================================================

def can_open_position(
    symbol,
    side
):

    with state_lock:

        if symbol in positions:
            return False

        if (
            len(positions)
            >=
            MAX_POSITIONS
        ):
            return False

        if (
            side_count(side)
            >=
            MAX_CORRELATED_SIDE
        ):
            return False

    return True


# ============================================================
# QUANTITY
# ============================================================

def calculate_quantity(
    symbol,
    price,
    leverage
):

    notional = (
        MARGIN_PER_POSITION
        *
        leverage
    )

    raw_qty = (
        notional
        /
        price
    )

    try:

        return float(
            exchange.amount_to_precision(
                symbol,
                raw_qty
            )
        )

    except Exception:

        return raw_qty


# ============================================================
# TARGET ROI
# ============================================================
#
# Bu "hedef" sabit TP emri değildir.
# Trailing sisteminin beklenen minimum kâr hedefidir.
#
# Raporlarda hangi hedefle açıldığını görmek için
# pozisyon içine kaydedilir.
# ============================================================

def calculate_target_roi(
    score,
    atr_percent
):

    # Minimum hedef
    target = 0.012

    if score >= 90:
        target = 0.030

    elif score >= 85:
        target = 0.025

    elif score >= 80:
        target = 0.020

    elif score >= 76:
        target = 0.015

    # Çok volatil piyasada hedefi artır
    if atr_percent >= 2.5:
        target += 0.005

    if atr_percent >= 4:
        target += 0.005

    return target


# ============================================================
# DRY RUN OPEN
# ============================================================

def dry_run_open(
    symbol,
    side,
    price,
    score,
    reasons,
    atr_value,
    leverage,
    target_roi
):

    quantity = calculate_quantity(
        symbol,
        price,
        leverage
    )

    notional = (
        quantity
        *
        price
    )

    if quantity <= 0:
        return False

    if side == "LONG":

        stop_price = (
            price
            -
            atr_value
            *
            HARD_STOP_ATR
        )

    else:

        stop_price = (
            price
            +
            atr_value
            *
            HARD_STOP_ATR
        )

    opened_at = (
        now_utc()
        .isoformat()
    )

    position = {

        "symbol": symbol,

        "side": side,

        "entry": price,

        "current_price": price,

        "margin": MARGIN_PER_POSITION,

        "leverage": leverage,

        "notional": notional,

        "quantity": quantity,

        "score": score,

        "entry_reason": reasons,

        "atr": atr_value,

        "highest": price,

        "lowest": price,

        "stop_price": stop_price,

        "initial_stop": stop_price,

        "trailing_active": False,

        "unrealized_pnl": 0.0,

        "unrealized_roi": 0.0,

        "opened_at": opened_at,

        "last_update": opened_at,

        "peak_roi": 0.0,

        # ----------------------------------------------------
        # RAPORLAMA
        # ----------------------------------------------------

        "target_roi": target_roi,

        "target_roi_percent":
            target_roi * 100,

        "entry_price":
            price,

        "entry_notional":
            notional,
    }

    with state_lock:

        positions[
            symbol
        ] = position

    stats[
        "simulated_entries"
    ] += 1

    stats[
        "signals"
    ] += 1

    logger.warning(
        "DRY RUN ENTRY | %s | %s | "
        "price=%.8f | score=%s | "
        "lev=%sx | margin=$%.2f | "
        "notional=$%.2f | qty=%s | "
        "TARGET ROI=%.2f%%",
        side,
        symbol,
        price,
        score,
        leverage,
        MARGIN_PER_POSITION,
        notional,
        quantity,
        target_roi * 100
    )

    logger.warning(
        "ENTRY REASONS | %s",
        " | ".join(
            reasons[:15]
        )
    )

    return True


# ============================================================
# LIVE OPEN
# ============================================================

def live_open_position(
    symbol,
    side,
    price,
    score,
    reasons,
    atr_value,
    leverage,
    target_roi
):

    if DRY_RUN:

        return dry_run_open(
            symbol,
            side,
            price,
            score,
            reasons,
            atr_value,
            leverage,
            target_roi
        )

    try:

        try:

            exchange.set_margin_mode(
                "isolated",
                symbol
            )

        except Exception as e:

            logger.debug(
                "Margin mode: %s",
                e
            )

        exchange.set_leverage(
            leverage,
            symbol
        )

        quantity = calculate_quantity(
            symbol,
            price,
            leverage
        )

        if quantity <= 0:
            return False

        order_side = (
            "buy"
            if side == "LONG"
            else "sell"
        )

        order = exchange.create_order(
            symbol,
            "market",
            order_side,
            quantity
        )

        logger.warning(
            "LIVE ENTRY | %s | %s | %s",
            side,
            symbol,
            order
        )

        opened_at = (
            now_utc()
            .isoformat()
        )

        if side == "LONG":

            initial_stop = (
                price
                -
                atr_value
                *
                HARD_STOP_ATR
            )

        else:

            initial_stop = (
                price
                +
                atr_value
                *
                HARD_STOP_ATR
            )

        position = {

            "symbol": symbol,

            "side": side,

            "entry": price,

            "current_price": price,

            "margin":
                MARGIN_PER_POSITION,

            "leverage": leverage,

            "notional":
                quantity * price,

            "quantity": quantity,

            "score": score,

            "entry_reason": reasons,

            "atr": atr_value,

            "highest": price,

            "lowest": price,

            "stop_price":
                initial_stop,

            "initial_stop":
                initial_stop,

            "trailing_active":
                False,

            "unrealized_pnl":
                0.0,

            "unrealized_roi":
                0.0,

            "opened_at":
                opened_at,

            "last_update":
                opened_at,

            "peak_roi":
                0.0,

            "target_roi":
                target_roi,

            "target_roi_percent":
                target_roi * 100,

            "entry_price":
                price,

            "entry_notional":
                quantity * price,
        }

        with state_lock:

            positions[
                symbol
            ] = position

        return True

    except Exception as e:

        logger.error(
            "LIVE ENTRY ERROR %s: %s",
            symbol,
            e
        )

        return False


# ============================================================
# PNL
# ============================================================

def calculate_pnl(
    position,
    current_price
):

    entry = position[
        "entry"
    ]

    leverage = position[
        "leverage"
    ]

    margin = position[
        "margin"
    ]

    if position[
        "side"
    ] == "LONG":

        price_change = (
            current_price
            -
            entry
        ) / entry

    else:

        price_change = (
            entry
            -
            current_price
        ) / entry

    roi = (
        price_change
        *
        leverage
    )

    pnl = (
        margin
        *
        roi
    )

    return pnl, roi


# ============================================================
# UPDATE TRAILING
# ============================================================

def update_trailing_stop(
    position,
    current_price
):

    side = position[
        "side"
    ]

    atr_value = position[
        "atr"
    ]

    pnl, roi = calculate_pnl(
        position,
        current_price
    )

    position[
        "current_price"
    ] = current_price

    position[
        "unrealized_pnl"
    ] = pnl

    position[
        "unrealized_roi"
    ] = roi

    position[
        "peak_roi"
    ] = max(
        position.get(
            "peak_roi",
            0
        ),
        roi
    )

    if side == "LONG":

        position[
            "highest"
        ] = max(
            position["highest"],
            current_price
        )

        if (
            roi
            >=
            MIN_PROFIT_TO_TRAIL
        ):

            position[
                "trailing_active"
            ] = True

        if position[
            "trailing_active"
        ]:

            if roi >= TRAIL_LEVEL_3:
                multiplier = (
                    TRAIL_ATR_TIGHT
                )

            elif roi >= TRAIL_LEVEL_2:
                multiplier = 1.20

            elif roi >= TRAIL_LEVEL_1:
                multiplier = 1.30

            else:
                multiplier = (
                    TRAIL_ATR_MULTIPLIER
                )

            trailing_stop = (
                position["highest"]
                -
                atr_value
                *
                multiplier
            )

            position[
                "stop_price"
            ] = max(
                position[
                    "stop_price"
                ],
                trailing_stop
            )

    else:

        position[
            "lowest"
        ] = min(
            position["lowest"],
            current_price
        )

        if (
            roi
            >=
            MIN_PROFIT_TO_TRAIL
        ):

            position[
                "trailing_active"
            ] = True

        if position[
            "trailing_active"
        ]:

            if roi >= TRAIL_LEVEL_3:
                multiplier = (
                    TRAIL_ATR_TIGHT
                )

            elif roi >= TRAIL_LEVEL_2:
                multiplier = 1.20

            elif roi >= TRAIL_LEVEL_1:
                multiplier = 1.30

            else:
                multiplier = (
                    TRAIL_ATR_MULTIPLIER
                )

            trailing_stop = (
                position["lowest"]
                +
                atr_value
                *
                multiplier
            )

            position[
                "stop_price"
            ] = min(
                position[
                    "stop_price"
                ],
                trailing_stop
            )

    position[
        "last_update"
    ] = (
        now_utc()
        .isoformat()
    )


# ============================================================
# SAVE CLOSED TRADE
# ============================================================

def save_closed_trade(
    position,
    exit_price,
    exit_reason
):

    closed_at = (
        now_utc()
        .isoformat()
    )

    duration = calculate_duration_seconds(
        position["opened_at"],
        closed_at
    )

    pnl, roi = calculate_pnl(
        position,
        exit_price
    )

    trade = {

        "symbol":
            position["symbol"],

        "side":
            position["side"],

        "margin":
            position["margin"],

        "leverage":
            position["leverage"],

        "notional":
            position["notional"],

        "quantity":
            position["quantity"],

        "entry_price":
            position["entry"],

        "exit_price":
            exit_price,

        "target_roi":
            position.get(
                "target_roi",
                0
            ),

        "target_roi_percent":
            position.get(
                "target_roi",
                0
            ) * 100,

        "realized_roi":
            roi,

        "realized_roi_percent":
            roi * 100,

        "pnl":
            pnl,

        "duration_seconds":
            duration,

        "duration":
            format_duration(
                duration
            ),

        "exit_reason":
            exit_reason,

        "score":
            position.get(
                "score",
                0
            ),

        "peak_roi":
            position.get(
                "peak_roi",
                0
            ),

        "peak_roi_percent":
            position.get(
                "peak_roi",
                0
            ) * 100,

        "opened_at":
            position["opened_at"],

        "closed_at":
            closed_at,
    }

    with state_lock:

        trade_history.append(
            trade
        )

        if (
            len(trade_history)
            >
            MAX_TRADE_HISTORY
        ):

            del trade_history[
                :
                len(trade_history)
                -
                MAX_TRADE_HISTORY
            ]

    stats[
        "total_trade_seconds"
    ] += duration

    stats[
        "total_volume"
    ] += position[
        "notional"
    ]

    if pnl >= 0:

        stats["wins"] += 1

    else:

        stats["losses"] += 1

    return trade


# ============================================================
# DRY RUN CLOSE
# ============================================================

def dry_run_close(
    symbol,
    reason
):

    with state_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        exit_price = (
            position[
                "current_price"
            ]
        )

        trade = save_closed_trade(
            position,
            exit_price,
            reason
        )

        del positions[
            symbol
        ]

    stats[
        "simulated_exits"
    ] += 1

    stats[
        "total_realized_pnl"
    ] += trade["pnl"]

    set_cooldown(
        symbol
    )

    logger.warning(
        "DRY RUN EXIT | %s | %s | "
        "exit=%.8f | PNL=$%.4f | "
        "ROI=%.2f%% | target=%.2f%% | "
        "duration=%s | reason=%s",
        trade["side"],
        symbol,
        trade["exit_price"],
        trade["pnl"],
        trade["realized_roi_percent"],
        trade["target_roi_percent"],
        trade["duration"],
        reason
    )


# ============================================================
# LIVE CLOSE
# ============================================================

def live_close(
    symbol,
    reason
):

    if DRY_RUN:

        dry_run_close(
            symbol,
            reason
        )

        return

    with state_lock:

        position = positions.get(
            symbol
        )

    if not position:
        return

    try:

        side = position[
            "side"
        ]

        order_side = (
            "sell"
            if side == "LONG"
            else "buy"
        )

        exchange.create_order(
            symbol,
            "market",
            order_side,
            position[
                "quantity"
            ],
            None,
            {
                "reduceOnly": True
            }
        )

        exit_price = (
            position[
                "current_price"
            ]
        )

        trade = save_closed_trade(
            position,
            exit_price,
            reason
        )

        with state_lock:

            positions.pop(
                symbol,
                None
            )

        stats[
            "total_realized_pnl"
        ] += trade["pnl"]

        set_cooldown(
            symbol
        )

        logger.warning(
            "LIVE EXIT | %s | %s | "
            "ROI=%.2f%% | PNL=$%.4f | "
            "duration=%s | reason=%s",
            side,
            symbol,
            trade[
                "realized_roi_percent"
            ],
            trade["pnl"],
            trade["duration"],
            reason
        )

    except Exception as e:

        logger.error(
            "LIVE EXIT ERROR %s: %s",
            symbol,
            e
        )


# ============================================================
# POSITION MONITOR
# ============================================================

def position_monitor():

    logger.info(
        "Position monitor başlatıldı."
    )

    while True:

        try:

            with state_lock:

                symbols = list(
                    positions.keys()
                )

            if not symbols:

                time.sleep(
                    POSITION_MONITOR_INTERVAL
                )

                continue

            tickers = (
                exchange.fetch_tickers(
                    symbols
                )
            )

            for symbol in symbols:

                ticker = tickers.get(
                    symbol
                )

                if not ticker:
                    continue

                price = safe_float(
                    ticker.get(
                        "last"
                    )
                )

                if price <= 0:
                    continue

                with state_lock:

                    position = positions.get(
                        symbol
                    )

                    if not position:
                        continue

                    update_trailing_stop(
                        position,
                        price
                    )

                    stop_price = (
                        position[
                            "stop_price"
                        ]
                    )

                    side = (
                        position[
                            "side"
                        ]
                    )

                    roi = (
                        position[
                            "unrealized_roi"
                        ]
                    )

                    trailing = (
                        position[
                            "trailing_active"
                        ]
                    )

                stop_hit = False

                if side == "LONG":

                    if price <= stop_price:
                        stop_hit = True

                else:

                    if price >= stop_price:
                        stop_hit = True

                if stop_hit:

                    live_close(
                        symbol,
                        "TRAILING_STOP"
                    )

                    continue

                logger.info(
                    "POSITION | %s | %s | "
                    "entry=%.8f | price=%.8f | "
                    "ROI=%.2f%% | PNL=$%.3f | "
                    "target=%.2f%% | "
                    "stop=%.8f | trail=%s",
                    side,
                    symbol,
                    position[
                        "entry"
                    ],
                    price,
                    roi * 100,
                    position[
                        "unrealized_pnl"
                    ],
                    position[
                        "target_roi"
                    ] * 100,
                    stop_price,
                    trailing
                )

        except Exception as e:

            logger.error(
                "Monitor error: %s",
                e
            )

        time.sleep(
            POSITION_MONITOR_INTERVAL
        )


# ============================================================
# HOURLY TRADE REPORT
# ============================================================

def generate_hourly_report():

    report_time = now_utc()

    current_hour_start = (
        report_time
        .replace(
            minute=0,
            second=0,
            microsecond=0
        )
    )

    previous_hour_start = (
        current_hour_start
        -
        timedelta(hours=1)
    )

    previous_hour_end = (
        current_hour_start
    )

    with state_lock:

        history_snapshot = list(
            trade_history
        )

        open_positions_snapshot = [
            dict(p)
            for p in positions.values()
        ]

    # --------------------------------------------------------
    # SON SAATTE KAPANANLAR
    # --------------------------------------------------------

    hourly_trades = []

    for trade in history_snapshot:

        try:

            closed_at = datetime.fromisoformat(
                trade["closed_at"]
            )

            if (
                previous_hour_start
                <=
                closed_at
                <
                previous_hour_end
            ):

                hourly_trades.append(
                    trade
                )

        except Exception:

            continue

    # --------------------------------------------------------
    # RAPOR
    # --------------------------------------------------------

    logger.warning("")
    logger.warning(
        "╔══════════════════════════════════════════════════════════════════════╗"
    )

    logger.warning(
        "║                 SAATLİK İŞLEM ÖZET RAPORU                          ║"
    )

    logger.warning(
        "╚══════════════════════════════════════════════════════════════════════╝"
    )

    logger.warning(
        "Rapor zamanı : %s",
        report_time.strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    )

    logger.warning(
        "Rapor dönemi : %s → %s UTC",
        previous_hour_start.strftime(
            "%H:%M"
        ),
        previous_hour_end.strftime(
            "%H:%M"
        )
    )

    logger.warning(
        "DRY RUN      : %s",
        DRY_RUN
    )

    logger.warning(
        "Açık işlem   : %s / %s",
        len(open_positions_snapshot),
        MAX_POSITIONS
    )

    # --------------------------------------------------------
    # SAATLİK ÖZET
    # --------------------------------------------------------

    hourly_count = len(
        hourly_trades
    )

    hourly_wins = sum(
        1
        for t in hourly_trades
        if t["pnl"] >= 0
    )

    hourly_losses = (
        hourly_count
        -
        hourly_wins
    )

    hourly_pnl = sum(
        t["pnl"]
        for t in hourly_trades
    )

    hourly_volume = sum(
        t["notional"]
        for t in hourly_trades
    )

    if hourly_count > 0:

        hourly_win_rate = (
            hourly_wins
            /
            hourly_count
        ) * 100

        avg_hourly_roi = (
            sum(
                t[
                    "realized_roi"
                ]
                for t in hourly_trades
            )
            /
            hourly_count
        ) * 100

        avg_duration = (
            sum(
                t[
                    "duration_seconds"
                ]
                for t in hourly_trades
            )
            /
            hourly_count
        )

    else:

        hourly_win_rate = 0.0
        avg_hourly_roi = 0.0
        avg_duration = 0.0

    logger.warning("")
    logger.warning(
        "SAATLİK ÖZET"
    )

    logger.warning(
        "İşlem sayısı      : %s",
        hourly_count
    )

    logger.warning(
        "Kazanan           : %s",
        hourly_wins
    )

    logger.warning(
        "Kaybeden          : %s",
        hourly_losses
    )

    logger.warning(
        "Win rate          : %.2f%%",
        hourly_win_rate
    )

    logger.warning(
        "Saatlik PNL       : $%.4f",
        hourly_pnl
    )

    logger.warning(
        "Saatlik işlem hacmi: $%.2f",
        hourly_volume
    )

    logger.warning(
        "Ortalama ROI      : %.2f%%",
        avg_hourly_roi
    )

    logger.warning(
        "Ortalama süre     : %s",
        format_duration(
            avg_duration
        )
    )

    # --------------------------------------------------------
    # İŞLEM DETAYLARI
    # --------------------------------------------------------

    logger.warning("")
    logger.warning(
        "SON SAATTE KAPANAN İŞLEMLER"
    )

    if not hourly_trades:

        logger.warning(
            "Son 1 saatte kapanan işlem yok."
        )

    else:

        for index, trade in enumerate(
            hourly_trades,
            start=1
        ):

            result_symbol = (
                "KAR"
                if trade["pnl"] >= 0
                else "ZARAR"
            )

            logger.warning(
                ""
            )

            logger.warning(
                "[%s] %s",
                index,
                result_symbol
            )

            logger.warning(
                "Coin              : %s",
                trade["symbol"]
            )

            logger.warning(
                "Yön               : %s",
                trade["side"]
            )

            logger.warning(
                "Margin            : $%.2f",
                trade["margin"]
            )

            logger.warning(
                "Kaldıraç          : %sx",
                trade["leverage"]
            )

            logger.warning(
                "İşlem büyüklüğü   : $%.2f",
                trade["notional"]
            )

            logger.warning(
                "Giriş fiyatı      : %.10f",
                trade["entry_price"]
            )

            logger.warning(
                "Çıkış fiyatı      : %.10f",
                trade["exit_price"]
            )

            logger.warning(
                "Hedef ROI         : %.2f%%",
                trade["target_roi_percent"]
            )

            logger.warning(
                "Gerçekleşen ROI   : %.2f%%",
                trade["realized_roi_percent"]
            )

            logger.warning(
                "En yüksek ROI     : %.2f%%",
                trade["peak_roi_percent"]
            )

            logger.warning(
                "PNL               : $%.4f",
                trade["pnl"]
            )

            logger.warning(
                "İşlem süresi      : %s",
                trade["duration"]
            )

            logger.warning(
                "Sinyal skoru      : %s",
                trade["score"]
            )

            logger.warning(
                "Kapanış nedeni    : %s",
                trade["exit_reason"]
            )

            logger.warning(
                "Açılış            : %s",
                trade["opened_at"]
            )

            logger.warning(
                "Kapanış            : %s",
                trade["closed_at"]
            )

    # --------------------------------------------------------
    # HALEN AÇIK POZİSYONLAR
    # --------------------------------------------------------

    logger.warning("")
    logger.warning(
        "HALEN AÇIK POZİSYONLAR"
    )

    if not open_positions_snapshot:

        logger.warning(
            "Açık pozisyon yok."
        )

    else:

        for p in open_positions_snapshot:

            current_pnl = (
                p["unrealized_pnl"]
            )

            current_roi = (
                p["unrealized_roi"]
                *
                100
            )

            logger.warning(
                ""
            )

            logger.warning(
                "Coin            : %s",
                p["symbol"]
            )

            logger.warning(
                "Yön             : %s",
                p["side"]
            )

            logger.warning(
                "Margin          : $%.2f",
                p["margin"]
            )

            logger.warning(
                "Leverage        : %sx",
                p["leverage"]
            )

            logger.warning(
                "Giriş           : %.10f",
                p["entry"]
            )

            logger.warning(
                "Son fiyat       : %.10f",
                p["current_price"]
            )

            logger.warning(
                "Hedef ROI       : %.2f%%",
                p["target_roi"]
                * 100
            )

            logger.warning(
                "Anlık ROI       : %.2f%%",
                current_roi
            )

            logger.warning(
                "Anlık PNL       : $%.4f",
                current_pnl
            )

            logger.warning(
                "Peak ROI        : %.2f%%",
                p["peak_roi"] * 100
            )

            logger.warning(
                "Trailing aktif  : %s",
                p["trailing_active"]
            )

            logger.warning(
                "Stop            : %.10f",
                p["stop_price"]
            )

            opened = datetime.fromisoformat(
                p["opened_at"]
            )

            open_duration = (
                report_time - opened
            ).total_seconds()

            logger.warning(
                "Açık kalma süresi: %s",
                format_duration(
                    open_duration
                )
            )

    # --------------------------------------------------------
    # GENEL İSTATİSTİK
    # --------------------------------------------------------

    total_trades = (
        stats["wins"]
        +
        stats["losses"]
    )

    if total_trades > 0:

        total_win_rate = (
            stats["wins"]
            /
            total_trades
        ) * 100

        avg_trade_duration = (
            stats[
                "total_trade_seconds"
            ]
            /
            total_trades
        )

    else:

        total_win_rate = 0.0
        avg_trade_duration = 0.0

    logger.warning("")
    logger.warning(
        "TOPLAM BOT İSTATİSTİĞİ"
    )

    logger.warning(
        "Toplam kapanan işlem : %s",
        total_trades
    )

    logger.warning(
        "Toplam kazanan       : %s",
        stats["wins"]
    )

    logger.warning(
        "Toplam kaybeden      : %s",
        stats["losses"]
    )

    logger.warning(
        "Toplam win rate      : %.2f%%",
        total_win_rate
    )

    logger.warning(
        "Toplam gerçekleşen PNL: $%.4f",
        stats[
            "total_realized_pnl"
        ]
    )

    logger.warning(
        "Toplam işlem hacmi   : $%.2f",
        stats[
            "total_volume"
        ]
    )

    logger.warning(
        "Ort. işlem süresi    : %s",
        format_duration(
            avg_trade_duration
        )
    )

    logger.warning(
        "Toplam tarama        : %s",
        stats["scans"]
    )

    logger.warning(
        "Toplam sinyal        : %s",
        stats["signals"]
    )

    logger.warning(
        "╔══════════════════════════════════════════════════════════════════════╗"
    )

    logger.warning(
        "║                       RAPOR SONU                                   ║"
    )

    logger.warning(
        "╚══════════════════════════════════════════════════════════════════════╝"
    )

    logger.warning("")


# ============================================================
# HOURLY REPORT THREAD
# ============================================================

def hourly_report_loop():

    global last_hourly_report_hour

    logger.info(
        "Saatlik işlem raporu başlatıldı."
    )

    while True:

        try:

            if not HOURLY_REPORT_ENABLED:

                time.sleep(
                    HOURLY_REPORT_INTERVAL
                )

                continue

            current = now_utc()

            current_hour = (
                current.strftime(
                    "%Y-%m-%d-%H"
                )
            )

            # İlk çalışmada mevcut saati kaydet.
            # Bot başlar başlamaz geçmiş saat raporu üretmez.
            if (
                last_hourly_report_hour
                is None
            ):

                last_hourly_report_hour = (
                    current_hour
                )

            elif (
                current.minute == 0
                and
                current.second
                <
                HOURLY_REPORT_INTERVAL
                and
                current_hour
                !=
                last_hourly_report_hour
            ):

                generate_hourly_report()

                last_hourly_report_hour = (
                    current_hour
                )

        except Exception as e:

            logger.error(
                "Hourly report error: %s",
                e
            )

            traceback.print_exc()

        time.sleep(
            HOURLY_REPORT_INTERVAL
        )


# ============================================================
# ANALYZE SYMBOL
# ============================================================

def analyze_symbol(
    candidate
):

    symbol = candidate[
        "symbol"
    ]

    if is_cooldown(
        symbol
    ):
        return None

    ticker = candidate[
        "ticker"
    ]

    funding = get_funding(
        symbol
    )

    if (
        abs(funding)
        >=
        MAX_ABS_FUNDING
    ):

        logger.info(
            "SKIP FUNDING | %s | %.6f",
            symbol,
            funding
        )

        return None

    df1 = fetch_ohlcv_cached(
        symbol,
        TIMEFRAME_FAST,
        120
    )

    df5 = fetch_ohlcv_cached(
        symbol,
        TIMEFRAME_ENTRY,
        160
    )

    df15 = fetch_ohlcv_cached(
        symbol,
        TIMEFRAME_CONFIRM,
        160
    )

    df1h = fetch_ohlcv_cached(
        symbol,
        TIMEFRAME_TREND,
        160
    )

    if any(
        x is None
        for x in [
            df1,
            df5,
            df15,
            df1h
        ]
    ):
        return None

    df1 = calculate_indicators(
        df1
    )

    df5 = calculate_indicators(
        df5
    )

    df15 = calculate_indicators(
        df15
    )

    df1h = calculate_indicators(
        df1h
    )

    if any(
        x is None
        for x in [
            df1,
            df5,
            df15,
            df1h
        ]
    ):
        return None

    price = safe_float(
        ticker["last"]
    )

    atr_value = safe_float(
        df5["atr"].iloc[-1]
    )

    if (
        price <= 0
        or
        atr_value <= 0
    ):
        return None

    atr_percent = (
        atr_value
        /
        price
    ) * 100

    if atr_percent < 0.08:
        return None

    if atr_percent > 8:
        return None

    long_score, long_reasons = (
        score_long(
            df1,
            df5,
            df15,
            df1h
        )
    )

    short_score, short_reasons = (
        score_short(
            df1,
            df5,
            df15,
            df1h
        )
    )

    side = None
    score = 0
    reasons = []

    if (
        long_score
        >=
        MIN_LONG_SCORE
        and
        long_score
        >
        short_score + 5
    ):

        side = "LONG"

        score = long_score

        reasons = long_reasons

    elif (
        short_score
        >=
        MIN_SHORT_SCORE
        and
        short_score
        >
        long_score + 5
    ):

        side = "SHORT"

        score = short_score

        reasons = short_reasons

    else:

        return None

    if side == "LONG":

        if ticker[
            "percentage"
        ] < -5:

            return None

    else:

        if ticker[
            "percentage"
        ] > 5:

            return None

    leverage = choose_leverage(
        score,
        atr_percent
    )

    target_roi = (
        calculate_target_roi(
            score,
            atr_percent
        )
    )

    return {

        "symbol":
            symbol,

        "side":
            side,

        "score":
            score,

        "price":
            price,

        "atr":
            atr_value,

        "atr_percent":
            atr_percent,

        "leverage":
            leverage,

        "funding":
            funding,

        "target_roi":
            target_roi,

        "reasons":
            reasons,

        "long_score":
            long_score,

        "short_score":
            short_score,

        "ticker_percentage":
            ticker[
                "percentage"
            ],

        "volume":
            ticker[
                "quoteVolume"
            ],

        "sources":
            candidate[
                "sources"
            ],
    }


# ============================================================
# FIND BEST OPPORTUNITY
# ============================================================

def find_best_signal(
    candidates
):

    ranked = []

    for symbol, candidate in (
        candidates.items()
    ):

        candidate[
            "pre_score"
        ] = preliminary_score(
            candidate
        )

        ranked.append(
            candidate
        )

    ranked.sort(
        key=lambda x:
            x["pre_score"],
        reverse=True
    )

    ranked = ranked[
        :MAX_DETAILED_CANDIDATES
    ]

    results = []

    logger.info(
        "Detaylı analiz: %s coin",
        len(ranked)
    )

    for candidate in ranked:

        try:

            result = analyze_symbol(
                candidate
            )

            if result:

                results.append(
                    result
                )

                logger.info(
                    "SIGNAL CANDIDATE | "
                    "%s | %s | "
                    "score=%s | "
                    "24h=%.2f%% | "
                    "ATR=%.2f%% | "
                    "target=%.2f%%",
                    result["side"],
                    result["symbol"],
                    result["score"],
                    result[
                        "ticker_percentage"
                    ],
                    result[
                        "atr_percent"
                    ],
                    result[
                        "target_roi"
                    ] * 100
                )

        except Exception as e:

            logger.error(
                "Analyze error %s: %s",
                candidate[
                    "symbol"
                ],
                e
            )

    if not results:

        return []

    results.sort(
        key=lambda x:
            x["score"],
        reverse=True
    )

    return results


# ============================================================
# FINAL ENTRY VALIDATION
# ============================================================

def final_entry_validation(
    signal
):

    symbol = signal[
        "symbol"
    ]

    side = signal[
        "side"
    ]

    if not can_open_position(
        symbol,
        side
    ):
        return False

    if (
        signal["score"]
        <
        EARLY_ENTRY_SCORE
    ):
        return False

    if (
        abs(signal["funding"])
        >=
        MAX_ABS_FUNDING
    ):
        return False

    if not (
        0.08
        <=
        signal[
            "atr_percent"
        ]
        <=
        8
    ):
        return False

    try:

        ticker = exchange.fetch_ticker(
            symbol
        )

        fresh_price = safe_float(
            ticker.get(
                "last"
            )
        )

        if fresh_price <= 0:
            return False

        original = signal[
            "price"
        ]

        move = (
            abs(
                fresh_price
                -
                original
            )
            /
            original
        )

        if move > 0.012:

            logger.info(
                "ENTRY SKIP | %s | "
                "price moved %.2f%% "
                "before entry",
                symbol,
                move * 100
            )

            return False

        signal[
            "price"
        ] = fresh_price

    except Exception:

        return False

    return True


# ============================================================
# EXECUTE SIGNAL
# ============================================================

def execute_signal(
    signal
):

    if (
        current_position_count()
        >=
        MAX_POSITIONS
    ):
        return False

    if not final_entry_validation(
        signal
    ):
        return False

    logger.warning(
        "ENTRY CONFIRMED | %s | %s | "
        "score=%s | leverage=%sx | "
        "target ROI=%.2f%%",
        signal["side"],
        signal["symbol"],
        signal["score"],
        signal["leverage"],
        signal["target_roi"] * 100
    )

    return live_open_position(
        symbol=signal[
            "symbol"
        ],

        side=signal[
            "side"
        ],

        price=signal[
            "price"
        ],

        score=signal[
            "score"
        ],

        reasons=signal[
            "reasons"
        ],

        atr_value=signal[
            "atr"
        ],

        leverage=signal[
            "leverage"
        ],

        target_roi=signal[
            "target_roi"
        ]
    )


# ============================================================
# SCAN CYCLE
# ============================================================

def scan_cycle():

    global last_scan_time

    last_scan_time = (
        now_utc()
        .isoformat()
    )

    stats[
        "scans"
    ] += 1

    logger.info("")

    logger.info(
        "=" * 75
    )

    logger.info(
        "BOT ANALİZ BAŞLADI | %s",
        last_scan_time
    )

    logger.info(
        "=" * 75
    )

    tickers = (
        get_futures_tickers()
    )

    if not tickers:

        logger.warning(
            "Ticker alınamadı."
        )

        return

    (
        gainers,
        losers,
        volumes
    ) = build_rank_lists(
        tickers
    )

    logger.info(
        "GAINERS: %s",
        [
            x["symbol"]
            for x in gainers
        ]
    )

    logger.info(
        "LOSERS: %s",
        [
            x["symbol"]
            for x in losers
        ]
    )

    logger.info(
        "VOLUME: %s",
        [
            x["symbol"]
            for x in volumes
        ]
    )

    candidates = (
        build_candidate_pool(
            gainers,
            losers,
            volumes
        )
    )

    logger.info(
        "Benzersiz aday: %s",
        len(candidates)
    )

    if (
        current_position_count()
        >=
        MAX_POSITIONS
    ):

        logger.info(
            "2 pozisyon zaten açık. "
            "Yeni işlem aranmayacak."
        )

        return

    signals = (
        find_best_signal(
            candidates
        )
    )

    if not signals:

        logger.info(
            "Uygun sinyal bulunamadı."
        )

        return

    logger.info(
        "Toplam sinyal: %s",
        len(signals)
    )

    for signal in signals[:5]:

        logger.info(
            "TOP SIGNAL | %s | %s | "
            "score=%s | "
            "24h=%.2f%% | "
            "ATR=%.2f%% | "
            "target=%.2f%% | "
            "sources=%s",
            signal["side"],
            signal["symbol"],
            signal["score"],
            signal[
                "ticker_percentage"
            ],
            signal[
                "atr_percent"
            ],
            signal[
                "target_roi"
            ] * 100,
            signal[
                "sources"
            ]
        )

    for signal in signals:

        if (
            current_position_count()
            >=
            MAX_POSITIONS
        ):
            break

        if execute_signal(
            signal
        ):

            time.sleep(
                0.5
            )


# ============================================================
# BOT LOOP
# ============================================================

def bot_loop():

    logger.warning("")

    logger.warning(
        "=" * 75
    )

    logger.warning(
        "EARLY MOMENTUM FUTURES BOT"
    )

    logger.warning(
        "=" * 75
    )

    logger.warning(
        "DRY_RUN = %s",
        DRY_RUN
    )

    logger.warning(
        "Margin = $%.2f",
        MARGIN_PER_POSITION
    )

    logger.warning(
        "Max leverage = %sx",
        MAX_LEVERAGE
    )

    logger.warning(
        "Max positions = %s",
        MAX_POSITIONS
    )

    logger.warning(
        "Gainers / Losers / Volume = "
        "%s / %s / %s",
        LIST_LIMIT,
        LIST_LIMIT,
        LIST_LIMIT
    )

    logger.warning(
        "Hourly report = %s",
        HOURLY_REPORT_ENABLED
    )

    logger.warning(
        "=" * 75
    )

    load_markets()

    while True:

        started = time.time()

        try:

            scan_cycle()

        except Exception as e:

            logger.error(
                "SCAN ERROR: %s",
                e
            )

            traceback.print_exc()

        elapsed = (
            time.time()
            -
            started
        )

        sleep_for = max(
            1,
            SCAN_INTERVAL
            -
            elapsed
        )

        logger.info(
            "Next scan %.1f saniye sonra.",
            sleep_for
        )

        time.sleep(
            sleep_for
        )


# ============================================================
# STATUS
# ============================================================

@app.route("/")
def home():

    return jsonify({

        "bot":
            "EARLY_MOMENTUM_FUTURES_BOT",

        "status":
            "running",

        "dry_run":
            DRY_RUN,

        "positions":
            len(positions),

        "max_positions":
            MAX_POSITIONS,

        "margin_per_position":
            MARGIN_PER_POSITION,

        "max_leverage":
            MAX_LEVERAGE,

        "last_scan":
            last_scan_time,

        "started_at":
            bot_started_at,

        "hourly_report":
            HOURLY_REPORT_ENABLED,
    })


# ============================================================
# STATUS API
# ============================================================

@app.route("/status")
def status():

    with state_lock:

        position_data = {}

        for symbol, position in (
            positions.items()
        ):

            position_data[
                symbol
            ] = {

                "side":
                    position["side"],

                "entry":
                    position["entry"],

                "current":
                    position[
                        "current_price"
                    ],

                "score":
                    position["score"],

                "leverage":
                    position["leverage"],

                "margin":
                    position["margin"],

                "notional":
                    position["notional"],

                "pnl":
                    position[
                        "unrealized_pnl"
                    ],

                "roi":
                    position[
                        "unrealized_roi"
                    ] * 100,

                "target_roi":
                    position[
                        "target_roi"
                    ] * 100,

                "stop":
                    position[
                        "stop_price"
                    ],

                "trailing":
                    position[
                        "trailing_active"
                    ],

                "peak_roi":
                    position[
                        "peak_roi"
                    ] * 100,

                "opened_at":
                    position[
                        "opened_at"
                    ],
            }

        recent_trades = (
            trade_history[
                -20:
            ]
        )

    return jsonify({

        "dry_run":
            DRY_RUN,

        "positions":
            position_data,

        "recent_trades":
            recent_trades,

        "stats":
            stats,

        "last_scan":
            last_scan_time,
    })


# ============================================================
# TRADE HISTORY API
# ============================================================

@app.route("/trades")
def trades():

    with state_lock:

        history = list(
            trade_history
        )

    return jsonify({

        "count":
            len(history),

        "trades":
            history
    })


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    return jsonify({

        "ok":
            True,

        "timestamp":
            now_utc()
            .isoformat()
    })


# ============================================================
# START BACKGROUND THREADS
# ============================================================

def start_background_bot():

    # --------------------------------------------------------
    # POZİSYON MONITOR
    # --------------------------------------------------------

    monitor = threading.Thread(
        target=position_monitor,
        daemon=True,
        name="PositionMonitor"
    )

    monitor.start()

    # --------------------------------------------------------
    # ANALYSIS LOOP
    # --------------------------------------------------------

    bot = threading.Thread(
        target=bot_loop,
        daemon=True,
        name="AnalysisLoop"
    )

    bot.start()

    # --------------------------------------------------------
    # HOURLY REPORT
    # --------------------------------------------------------

    report = threading.Thread(
        target=hourly_report_loop,
        daemon=True,
        name="HourlyReport"
    )

    report.start()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_background_bot()

    port = int(
        os.getenv(
            "PORT",
            "8080"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )