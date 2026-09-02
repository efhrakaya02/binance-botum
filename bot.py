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
# BINANCE FUTURES PURE PRICE ACTION BOT
# ============================================================
#
# COIN HAVUZU
#
# GAINERS  : ilk 50
# LOSERS   : ilk 50
# VOLUME   : ilk 50
#
# BTC / XAU işlem evreninden hariç (yalnızca BTC piyasa yönü
# teyidi ve korelasyon kontrolü için kullanılır)
#
# İŞLEM KURALLARI
#
# Maksimum:
#   2 açık pozisyon
#   10 USDT margin / pozisyon
#   maksimum 5x
#
# LONG / SHORT
#
# ANALİZ:
#
#   SADECE PRICE ACTION
#
#   1H  = Ana market structure
#   15M = Orta yapı / breakout
#   5M  = Setup / retest
#   1M  = Entry trigger
#
# KULLANILAN PRICE ACTION:
#
#   HH / HL
#   LH / LL
#   Swing High / Swing Low
#   Breakout
#   Breakdown
#   Retest
#   Failed Breakout
#   Failed Breakdown
#   Engulfing
#   Rejection Candle
#   Momentum Candle
#   Compression
#   Impulse
#   Pullback
#   Continuation
#   Volume confirmation
#   Move position / exhaustion
#
# KULLANILMAYANLAR:
#
#   EMA
#   RSI
#   MACD
#   ADX
#   Bollinger
#   Ichimoku
#   Fibonacci
#   StochRSI
#   Elder-Ray
#   ROC
#
# DRY RUN = TRUE
#
# ============================================================


# ============================================================
# CONFIG
# ============================================================

DRY_RUN = os.getenv(
    "DRY_RUN",
    "true"
).lower() == "true"

API_KEY = os.getenv(
    "BINANCE_API_KEY",
    ""
)

API_SECRET = os.getenv(
    "BINANCE_API_SECRET",
    ""
)


# ------------------------------------------------------------
# TIMEFRAMES
# ------------------------------------------------------------

TIMEFRAME_FAST = "1m"
TIMEFRAME_ENTRY = "5m"
TIMEFRAME_CONFIRM = "15m"
TIMEFRAME_TREND = "1h"


# ------------------------------------------------------------
# BOT
# ------------------------------------------------------------

SCAN_INTERVAL = 20

POSITION_MONITOR_INTERVAL = 1.0

MARGIN_PER_POSITION = 10.0

MAX_LEVERAGE = 5
MIN_LEVERAGE = 2

MAX_POSITIONS = 2

MIN_LONG_SCORE = 72
MIN_SHORT_SCORE = 72

EARLY_ENTRY_SCORE = 72

MAX_ABS_FUNDING = 0.0015

COOLDOWN_MINUTES = 60

MIN_QUOTE_VOLUME = 2_000_000

MAX_SPREAD_PERCENT = 0.15


# ------------------------------------------------------------
# POSITION MANAGEMENT
# ------------------------------------------------------------

MIN_PROFIT_TO_TRAIL = 0.004

HARD_STOP_ATR = 1.8

TRAIL_ATR_MULTIPLIER = 1.35
TRAIL_ATR_TIGHT = 1.05

TRAIL_LEVEL_1 = 0.008
TRAIL_LEVEL_2 = 0.015
TRAIL_LEVEL_3 = 0.025

EMERGENCY_REVERSE_THRESHOLD = 0.007

MAX_CORRELATED_SIDE = 2


# ------------------------------------------------------------
# CACHE
# ------------------------------------------------------------

OHLCV_CACHE_SECONDS = 12

MAX_DETAILED_CANDIDATES = 70


# ------------------------------------------------------------
# COIN POOL  (GENİŞLETİLDİ — YENİ)
# ------------------------------------------------------------
# Gainers / Losers / Volume artık ilk 50'şer coini kapsıyor
# (önceki sürümde gainers/losers 10-35 aralığı, volume ilk 25'ti).

RANK_START = 1
RANK_END = 50

# Volume ilk 50
VOLUME_LIMIT = 50

# Binance'den en az bu kadar coin çek
LIST_LIMIT = 50


LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "INFO"
).upper()


# ------------------------------------------------------------
# HOURLY REPORT
# ------------------------------------------------------------

HOURLY_REPORT_ENABLED = True

MAX_TRADE_HISTORY = 1000

HOURLY_REPORT_INTERVAL = 5


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=getattr(
        logging,
        LOG_LEVEL,
        logging.INFO
    ),
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(
    "PURE_PRICE_ACTION_BOT"
)


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

bot_started_at = (
    datetime.now(
        timezone.utc
    ).isoformat()
)


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

    return datetime.now(
        timezone.utc
    )


def safe_float(
    value,
    default=0.0
):

    try:

        if value is None:
            return default

        return float(value)

    except Exception:

        return default


def clamp(
    value,
    low,
    high
):

    return max(
        low,
        min(
            high,
            value
        )
    )


def pct_change(
    a,
    b
):

    if not a:
        return 0.0

    return (
        (b - a) / a
    ) * 100.0


def symbol_clean(
    symbol
):

    return (
        symbol
        .replace(
            "/",
            ""
        )
        .replace(
            ":USDT",
            ""
        )
    )


# ============================================================
# DURATION
# ============================================================

def calculate_duration_seconds(
    opened_at,
    closed_at
):

    try:

        start = datetime.fromisoformat(
            opened_at
        )

        end = datetime.fromisoformat(
            closed_at
        )

        return max(
            0,
            (
                end - start
            ).total_seconds()
        )

    except Exception:

        return 0


def format_duration(
    seconds
):

    seconds = int(
        max(
            0,
            seconds
        )
    )

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

def valid_symbol(
    symbol,
    market=None
):

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
                ticker.get(
                    "last"
                )
            )

            percentage = safe_float(
                ticker.get(
                    "percentage"
                )
            )

            bid = safe_float(
                ticker.get(
                    "bid"
                )
            )

            ask = safe_float(
                ticker.get(
                    "ask"
                )
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

                "symbol":
                    symbol,

                "last":
                    last,

                "percentage":
                    percentage,

                "quoteVolume":
                    quote_volume,

                "bid":
                    bid,

                "ask":
                    ask,

                "spread":
                    spread,

                "high":
                    safe_float(
                        ticker.get(
                            "high"
                        )
                    ),

                "low":
                    safe_float(
                        ticker.get(
                            "low"
                        )
                    ),

                "open":
                    safe_float(
                        ticker.get(
                            "open"
                        )
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

def build_rank_lists(
    tickers
):

    data = list(
        tickers.values()
    )

    gainers = sorted(
        data,
        key=lambda x:
            x["percentage"],
        reverse=True
    )

    losers = sorted(
        data,
        key=lambda x:
            x["percentage"]
    )

    volumes = sorted(
        data,
        key=lambda x:
            x["quoteVolume"],
        reverse=True
    )

    # --------------------------------------------------------
    # GAINERS / LOSERS: ilk RANK_END (varsayılan 1-50)
    # --------------------------------------------------------

    gainers = gainers[
        RANK_START - 1:
        RANK_END
    ]

    losers = losers[
        RANK_START - 1:
        RANK_END
    ]

    # --------------------------------------------------------
    # VOLUME: ilk VOLUME_LIMIT
    # --------------------------------------------------------

    volumes = volumes[
        :VOLUME_LIMIT
    ]

    return (
        gainers,
        losers,
        volumes
    )


# ============================================================
# CANDIDATE POOL
# ============================================================

def build_candidate_pool(
    gainers,
    losers,
    volumes
):

    candidates = {}

    def add(
        items,
        source,
        rank_offset=0
    ):

        for local_rank, item in enumerate(
            items,
            start=1
        ):

            symbol = item[
                "symbol"
            ]

            actual_rank = (
                local_rank
                +
                rank_offset
            )

            if symbol not in candidates:

                candidates[
                    symbol
                ] = {

                    "symbol":
                        symbol,

                    "sources":
                        [],

                    "gainer_rank":
                        None,

                    "loser_rank":
                        None,

                    "volume_rank":
                        None,

                    "ticker":
                        item,
                }

            candidates[
                symbol
            ][
                "sources"
            ].append(
                source
            )

            if source == "GAINER":

                candidates[
                    symbol
                ][
                    "gainer_rank"
                ] = actual_rank

            elif source == "LOSER":

                candidates[
                    symbol
                ][
                    "loser_rank"
                ] = actual_rank

            elif source == "VOLUME":

                candidates[
                    symbol
                ][
                    "volume_rank"
                ] = actual_rank

    add(
        gainers,
        "GAINER",
        RANK_START - 1
    )

    add(
        losers,
        "LOSER",
        RANK_START - 1
    )

    add(
        volumes,
        "VOLUME",
        0
    )

    return candidates


# ============================================================
# OHLCV CACHE
# ============================================================

def fetch_ohlcv_cached(
    symbol,
    timeframe,
    limit=220
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

        df[
            "timestamp"
        ] = pd.to_datetime(
            df[
                "timestamp"
            ],
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

        ohlcv_cache[
            key
        ] = (
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
# BASIC PRICE ACTION HELPERS
# ============================================================

def candle_stats(
    candle
):

    high = safe_float(
        candle["high"]
    )

    low = safe_float(
        candle["low"]
    )

    open_price = safe_float(
        candle["open"]
    )

    close = safe_float(
        candle["close"]
    )

    candle_range = (
        high - low
    )

    body = abs(
        close - open_price
    )

    if candle_range <= 0:

        return {
            "range": 0.0,
            "body": 0.0,
            "body_ratio": 0.0,
            "upper_wick": 0.0,
            "lower_wick": 0.0,
        }

    upper_wick = (
        high
        -
        max(
            open_price,
            close
        )
    )

    lower_wick = (
        min(
            open_price,
            close
        )
        -
        low
    )

    return {

        "range":
            candle_range,

        "body":
            body,

        "body_ratio":
            body / candle_range,

        "upper_wick":
            max(
                0.0,
                upper_wick
            ),

        "lower_wick":
            max(
                0.0,
                lower_wick
            ),
    }


# ============================================================
# SWING DETECTION
# ============================================================

def find_swing_highs(
    df,
    left=2,
    right=2,
    lookback=80
):

    if df is None:
        return []

    data = df.tail(
        lookback
    ).reset_index(
        drop=True
    )

    highs = []

    if len(data) < (
        left + right + 1
    ):

        return highs

    for i in range(
        left,
        len(data) - right
    ):

        value = safe_float(
            data.iloc[i]["high"]
        )

        left_highs = data.iloc[
            i-left:i
        ]["high"]

        right_highs = data.iloc[
            i+1:i+1+right
        ]["high"]

        if (
            value > left_highs.max()
            and
            value >= right_highs.max()
        ):

            highs.append({
                "index": i,
                "price": value,
                "timestamp":
                    data.iloc[
                        i
                    ]["timestamp"]
            })

    return highs


def find_swing_lows(
    df,
    left=2,
    right=2,
    lookback=80
):

    if df is None:
        return []

    data = df.tail(
        lookback
    ).reset_index(
        drop=True
    )

    lows = []

    if len(data) < (
        left + right + 1
    ):

        return lows

    for i in range(
        left,
        len(data) - right
    ):

        value = safe_float(
            data.iloc[i]["low"]
        )

        left_lows = data.iloc[
            i-left:i
        ]["low"]

        right_lows = data.iloc[
            i+1:i+1+right
        ]["low"]

        if (
            value < left_lows.min()
            and
            value <= right_lows.min()
        ):

            lows.append({
                "index": i,
                "price": value,
                "timestamp":
                    data.iloc[
                        i
                    ]["timestamp"]
            })

    return lows


# ============================================================
# MARKET STRUCTURE
# ============================================================

def market_structure(
    df,
    lookback=80
):

    highs = find_swing_highs(
        df,
        2,
        2,
        lookback
    )

    lows = find_swing_lows(
        df,
        2,
        2,
        lookback
    )

    structure = "NEUTRAL"

    hh = False
    hl = False
    lh = False
    ll = False

    if len(highs) >= 2:

        previous_high = highs[-2][
            "price"
        ]

        latest_high = highs[-1][
            "price"
        ]

        if latest_high > previous_high:

            hh = True

        else:

            lh = True

    if len(lows) >= 2:

        previous_low = lows[-2][
            "price"
        ]

        latest_low = lows[-1][
            "price"
        ]

        if latest_low > previous_low:

            hl = True

        else:

            ll = True

    if hh and hl:

        structure = "BULLISH"

    elif lh and ll:

        structure = "BEARISH"

    elif hh or hl:

        structure = "BULLISH_WEAK"

    elif lh or ll:

        structure = "BEARISH_WEAK"

    return {

        "structure":
            structure,

        "hh":
            hh,

        "hl":
            hl,

        "lh":
            lh,

        "ll":
            ll,

        "highs":
            highs,

        "lows":
            lows,
    }


# ============================================================
# CANDLE PATTERNS
# ============================================================

def bullish_engulfing(
    df
):

    if len(df) < 3:
        return False

    a = df.iloc[-2]
    b = df.iloc[-1]

    return (

        a["close"] < a["open"]

        and

        b["close"] > b["open"]

        and

        b["open"] <= a["close"]

        and

        b["close"] >= a["open"]

    )


def bearish_engulfing(
    df
):

    if len(df) < 3:
        return False

    a = df.iloc[-2]
    b = df.iloc[-1]

    return (

        a["close"] > a["open"]

        and

        b["close"] < b["open"]

        and

        b["open"] >= a["close"]

        and

        b["close"] <= a["open"]

    )


def bullish_rejection(
    df
):

    if len(df) < 2:
        return False

    c = df.iloc[-1]

    s = candle_stats(c)

    return (

        c["close"] > c["open"]

        and

        s["lower_wick"]
        >=
        s["body"] * 1.2

        and

        c["close"]
        >=
        c["low"]
        +
        s["range"] * 0.60

    )


def bearish_rejection(
    df
):

    if len(df) < 2:
        return False

    c = df.iloc[-1]

    s = candle_stats(c)

    return (

        c["close"] < c["open"]

        and

        s["upper_wick"]
        >=
        s["body"] * 1.2

        and

        c["close"]
        <=
        c["low"]
        +
        s["range"] * 0.40

    )


def strong_bullish_candle(
    df
):

    if len(df) < 2:
        return False

    c = df.iloc[-1]

    s = candle_stats(c)

    return (

        c["close"] > c["open"]

        and

        s["body_ratio"] >= 0.60

        and

        c["close"]
        >=
        c["low"]
        +
        s["range"] * 0.75

    )


def strong_bearish_candle(
    df
):

    if len(df) < 2:
        return False

    c = df.iloc[-1]

    s = candle_stats(c)

    return (

        c["close"] < c["open"]

        and

        s["body_ratio"] >= 0.60

        and

        c["close"]
        <=
        c["low"]
        +
        s["range"] * 0.25

    )


# ============================================================
# VOLUME PRICE ACTION CONFIRMATION
# ============================================================

def volume_confirmation(
    df
):

    if len(df) < 21:
        return 1.0

    current = safe_float(
        df["volume"].iloc[-1]
    )

    previous = safe_float(
        df["volume"].iloc[-2]
    )

    average = safe_float(
        df["volume"].iloc[-21:-1].mean()
    )

    if average <= 0:
        return 1.0

    current_ratio = (
        current / average
    )

    previous_ratio = (
        previous / average
    )

    return max(
        current_ratio,
        previous_ratio
    )


# ============================================================
# IMPULSE DETECTION
# ============================================================

def bullish_impulse(
    df,
    candles=3
):

    if len(df) < candles + 2:
        return False

    recent = df.iloc[
        -candles:
    ]

    bullish_count = (
        recent["close"]
        >
        recent["open"]
    ).sum()

    if bullish_count < 2:
        return False

    first_open = safe_float(
        recent["open"].iloc[0]
    )

    last_close = safe_float(
        recent["close"].iloc[-1]
    )

    if first_open <= 0:
        return False

    move = (
        last_close
        -
        first_open
    ) / first_open

    return move > 0.002


def bearish_impulse(
    df,
    candles=3
):

    if len(df) < candles + 2:
        return False

    recent = df.iloc[
        -candles:
    ]

    bearish_count = (
        recent["close"]
        <
        recent["open"]
    ).sum()

    if bearish_count < 2:
        return False

    first_open = safe_float(
        recent["open"].iloc[0]
    )

    last_close = safe_float(
        recent["close"].iloc[-1]
    )

    if first_open <= 0:
        return False

    move = (
        last_close
        -
        first_open
    ) / first_open

    return move < -0.002


# ============================================================
# COMPRESSION
# ============================================================

def compression_detected(
    df,
    lookback=8
):

    if len(df) < 25:
        return False

    recent = df.iloc[
        -lookback:
    ]

    previous = df.iloc[
        -25:-lookback
    ]

    if len(previous) < 5:
        return False

    recent_ranges = (
        recent["high"]
        -
        recent["low"]
    )

    previous_ranges = (
        previous["high"]
        -
        previous["low"]
    )

    recent_avg = safe_float(
        recent_ranges.mean()
    )

    previous_avg = safe_float(
        previous_ranges.mean()
    )

    if previous_avg <= 0:
        return False

    return (
        recent_avg
        <
        previous_avg * 0.75
    )


# ============================================================
# BREAKOUT ANALYSIS
# ============================================================

def breakout_analysis(
    df,
    lookback=20
):

    if len(df) < lookback + 5:

        return {

            "bull_breakout":
                False,

            "bear_breakdown":
                False,

            "bull_attempt":
                False,

            "bear_attempt":
                False,

            "level_high":
                None,

            "level_low":
                None,

        }

    previous = df.iloc[
        -(lookback + 1):-1
    ]

    current = df.iloc[-1]

    level_high = safe_float(
        previous["high"].max()
    )

    level_low = safe_float(
        previous["low"].min()
    )

    current_close = safe_float(
        current["close"]
    )

    current_high = safe_float(
        current["high"]
    )

    current_low = safe_float(
        current["low"]
    )

    return {

        "bull_breakout":
            current_close > level_high,

        "bear_breakdown":
            current_close < level_low,

        "bull_attempt":
            current_high > level_high,

        "bear_attempt":
            current_low < level_low,

        "level_high":
            level_high,

        "level_low":
            level_low,
    }


# ============================================================
# RETEST DETECTION
# ============================================================

def bullish_retest(
    df,
    level,
    tolerance=0.0035
):

    if level is None:
        return False

    if len(df) < 5:
        return False

    recent = df.iloc[-4:]

    touched = False

    for _, c in recent.iterrows():

        low = safe_float(
            c["low"]
        )

        high = safe_float(
            c["high"]
        )

        distance = abs(
            low - level
        ) / level

        if (
            distance <= tolerance
            or
            (
                low <= level
                <= high
            )
        ):

            touched = True
            break

    last = df.iloc[-1]

    return (

        touched

        and

        safe_float(
            last["close"]
        ) > level

        and

        (
            bullish_rejection(df)
            or
            bullish_engulfing(df)
            or
            strong_bullish_candle(df)
        )

    )


def bearish_retest(
    df,
    level,
    tolerance=0.0035
):

    if level is None:
        return False

    if len(df) < 5:
        return False

    recent = df.iloc[-4:]

    touched = False

    for _, c in recent.iterrows():

        low = safe_float(
            c["low"]
        )

        high = safe_float(
            c["high"]
        )

        distance = abs(
            high - level
        ) / level

        if (
            distance <= tolerance
            or
            (
                low <= level
                <= high
            )
        ):

            touched = True
            break

    last = df.iloc[-1]

    return (

        touched

        and

        safe_float(
            last["close"]
        ) < level

        and

        (
            bearish_rejection(df)
            or
            bearish_engulfing(df)
            or
            strong_bearish_candle(df)
        )

    )


# ============================================================
# FAILED BREAKOUT / BREAKDOWN
# ============================================================

def failed_bull_breakout(
    df,
    level
):

    if level is None:
        return False

    if len(df) < 3:
        return False

    previous = df.iloc[-2]

    current = df.iloc[-1]

    return (

        previous["high"] > level

        and

        previous["close"] <= level

        and

        current["close"] < level

    )


def failed_bear_breakdown(
    df,
    level
):

    if level is None:
        return False

    if len(df) < 3:
        return False

    previous = df.iloc[-2]

    current = df.iloc[-1]

    return (

        previous["low"] < level

        and

        previous["close"] >= level

        and

        current["close"] > level

    )


# ============================================================
# PRICE POSITION
# ============================================================

def move_position(
    df,
    lookback=50
):

    if len(df) < lookback:
        return 0.5

    recent = df.iloc[
        -lookback:
    ]

    high = safe_float(
        recent["high"].max()
    )

    low = safe_float(
        recent["low"].min()
    )

    close = safe_float(
        recent["close"].iloc[-1]
    )

    if high <= low:
        return 0.5

    return clamp(
        (
            close - low
        )
        /
        (
            high - low
        ),
        0.0,
        1.0
    )


# ============================================================
# LATE ENTRY / EXHAUSTION
# ============================================================

def late_long_move(
    df,
    lookback=12
):

    if len(df) < lookback + 1:
        return False

    recent = df.iloc[
        -lookback:
    ]

    start = safe_float(
        recent["open"].iloc[0]
    )

    close = safe_float(
        recent["close"].iloc[-1]
    )

    if start <= 0:
        return False

    move = (
        close - start
    ) / start

    return move >= 0.035


def late_short_move(
    df,
    lookback=12
):

    if len(df) < lookback + 1:
        return False

    recent = df.iloc[
        -lookback:
    ]

    start = safe_float(
        recent["open"].iloc[0]
    )

    close = safe_float(
        recent["close"].iloc[-1]
    )

    if start <= 0:
        return False

    move = (
        close - start
    ) / start

    return move <= -0.035


# ============================================================
# PRICE ACTION SCORE - LONG
# ============================================================

def score_long(
    df1,
    df5,
    df15,
    df1h,
    btc_context=None
):

    score = 0

    reasons = []

    structure_1h = market_structure(
        df1h
    )

    if structure_1h[
        "structure"
    ] == "BULLISH":

        score += 18

        reasons.append(
            "1H bullish structure"
        )

    elif structure_1h[
        "structure"
    ] == "BULLISH_WEAK":

        score += 9

        reasons.append(
            "1H bullish structure weak"
        )

    elif structure_1h[
        "structure"
    ] == "BEARISH":

        score -= 14

        reasons.append(
            "1H bearish structure"
        )

    structure_15 = market_structure(
        df15
    )

    if structure_15[
        "structure"
    ] == "BULLISH":

        score += 16

        reasons.append(
            "15M bullish structure"
        )

    elif structure_15[
        "structure"
    ] == "BULLISH_WEAK":

        score += 8

        reasons.append(
            "15M bullish structure"
        )

    elif structure_15[
        "structure"
    ] == "BEARISH":

        score -= 12

        reasons.append(
            "15M bearish structure"
        )

    structure_5 = market_structure(
        df5
    )

    if structure_5[
        "structure"
    ] == "BULLISH":

        score += 12

        reasons.append(
            "5M bullish structure"
        )

    elif structure_5[
        "structure"
    ] == "BULLISH_WEAK":

        score += 6

        reasons.append(
            "5M bullish structure"
        )

    br15 = breakout_analysis(
        df15,
        20
    )

    if br15[
        "bull_breakout"
    ]:

        score += 12

        reasons.append(
            "15M confirmed breakout"
        )

    elif br15[
        "bull_attempt"
    ]:

        score += 5

        reasons.append(
            "15M breakout attempt"
        )

    br5 = breakout_analysis(
        df5,
        20
    )

    if br5[
        "bull_breakout"
    ]:

        score += 12

        reasons.append(
            "5M confirmed breakout"
        )

    elif br5[
        "bull_attempt"
    ]:

        score += 5

        reasons.append(
            "5M breakout attempt"
        )

    retest_5 = False

    if br5[
        "level_high"
    ] is not None:

        retest_5 = bullish_retest(
            df5,
            br5["level_high"]
        )

    retest_15 = False

    if br15[
        "level_high"
    ] is not None:

        retest_15 = bullish_retest(
            df15,
            br15["level_high"]
        )

    if retest_5:

        score += 15

        reasons.append(
            "5M breakout retest"
        )

    if retest_15:

        score += 10

        reasons.append(
            "15M breakout retest"
        )

    if bullish_engulfing(
        df5
    ):

        score += 8

        reasons.append(
            "5M bullish engulfing"
        )

    elif bullish_rejection(
        df5
    ):

        score += 6

        reasons.append(
            "5M bullish rejection"
        )

    elif strong_bullish_candle(
        df5
    ):

        score += 5

        reasons.append(
            "5M strong bullish candle"
        )

    if bullish_engulfing(
        df1
    ):

        score += 5

        reasons.append(
            "1M bullish trigger"
        )

    elif bullish_rejection(
        df1
    ):

        score += 4

        reasons.append(
            "1M bullish rejection"
        )

    if bullish_impulse(
        df5
    ):

        score += 7

        reasons.append(
            "5M bullish impulse"
        )

    if compression_detected(
        df5
    ):

        score += 4

        reasons.append(
            "5M compression"
        )

    volume_ratio = volume_confirmation(
        df5
    )

    if volume_ratio >= 1.50:

        score += 7

        reasons.append(
            "Breakout volume strong"
        )

    elif volume_ratio >= 1.20:

        score += 4

        reasons.append(
            "Volume confirmation"
        )

    position = move_position(
        df5,
        50
    )

    if 0.35 <= position <= 0.70:

        score += 7

        reasons.append(
            "Long entry not late"
        )

    elif 0.70 < position <= 0.82:

        score += 2

        reasons.append(
            "Long move advanced"
        )

    elif position > 0.82:

        score -= 12

        reasons.append(
            "Long entry too late"
        )

    if late_long_move(
        df5
    ):

        score -= 12

        reasons.append(
            "Late long move penalty"
        )

    if failed_bull_breakout(
        df5,
        br5["level_high"]
    ):

        score -= 18

        reasons.append(
            "Failed bullish breakout"
        )

    last_1m = df1.iloc[-1]

    if (
        last_1m["close"]
        >
        last_1m["open"]
    ):

        score += 3

        reasons.append(
            "1M bullish close"
        )

    # --------------------------------------------------------
    # BTC MARKET CONTEXT  (YENİ)
    # --------------------------------------------------------
    # BTC işlem evresinde değildir; yalnızca piyasa yönü teyidi
    # (context) olarak kullanılır. Sert bir veto DEĞİL, kalite
    # puanına ekleme/çıkarma yapan bir bağlam modifikatörüdür.

    if btc_context:

        if btc_context["direction"] == "LONG":

            bonus = round(btc_context["strength"] / 100 * 10)

            score += bonus

            reasons.append(
                f"BTC context destekliyor (+{bonus})"
            )

        elif btc_context["direction"] == "SHORT":

            penalty = round(btc_context["strength"] / 100 * 10)

            score -= penalty

            reasons.append(
                f"BTC context ters yönde (-{penalty})"
            )

    return score, reasons


# ============================================================
# PRICE ACTION SCORE - SHORT
# ============================================================

def score_short(
    df1,
    df5,
    df15,
    df1h,
    btc_context=None
):

    score = 0

    reasons = []

    structure_1h = market_structure(
        df1h
    )

    if structure_1h[
        "structure"
    ] == "BEARISH":

        score += 18

        reasons.append(
            "1H bearish structure"
        )

    elif structure_1h[
        "structure"
    ] == "BEARISH_WEAK":

        score += 9

        reasons.append(
            "1H bearish structure weak"
        )

    elif structure_1h[
        "structure"
    ] == "BULLISH":

        score -= 14

        reasons.append(
            "1H bullish structure"
        )

    structure_15 = market_structure(
        df15
    )

    if structure_15[
        "structure"
    ] == "BEARISH":

        score += 16

        reasons.append(
            "15M bearish structure"
        )

    elif structure_15[
        "structure"
    ] == "BEARISH_WEAK":

        score += 8

        reasons.append(
            "15M bearish structure"
        )

    elif structure_15[
        "structure"
    ] == "BULLISH":

        score -= 12

        reasons.append(
            "15M bullish structure"
        )

    structure_5 = market_structure(
        df5
    )

    if structure_5[
        "structure"
    ] == "BEARISH":

        score += 12

        reasons.append(
            "5M bearish structure"
        )

    elif structure_5[
        "structure"
    ] == "BEARISH_WEAK":

        score += 6

        reasons.append(
            "5M bearish structure"
        )

    br15 = breakout_analysis(
        df15,
        20
    )

    if br15[
        "bear_breakdown"
    ]:

        score += 12

        reasons.append(
            "15M confirmed breakdown"
        )

    elif br15[
        "bear_attempt"
    ]:

        score += 5

        reasons.append(
            "15M breakdown attempt"
        )

    br5 = breakout_analysis(
        df5,
        20
    )

    if br5[
        "bear_breakdown"
    ]:

        score += 12

        reasons.append(
            "5M confirmed breakdown"
        )

    elif br5[
        "bear_attempt"
    ]:

        score += 5

        reasons.append(
            "5M breakdown attempt"
        )

    retest_5 = False

    if br5[
        "level_low"
    ] is not None:

        retest_5 = bearish_retest(
            df5,
            br5["level_low"]
        )

    retest_15 = False

    if br15[
        "level_low"
    ] is not None:

        retest_15 = bearish_retest(
            df15,
            br15["level_low"]
        )

    if retest_5:

        score += 15

        reasons.append(
            "5M breakdown retest"
        )

    if retest_15:

        score += 10

        reasons.append(
            "15M breakdown retest"
        )

    if bearish_engulfing(
        df5
    ):

        score += 8

        reasons.append(
            "5M bearish engulfing"
        )

    elif bearish_rejection(
        df5
    ):

        score += 6

        reasons.append(
            "5M bearish rejection"
        )

    elif strong_bearish_candle(
        df5
    ):

        score += 5

        reasons.append(
            "5M strong bearish candle"
        )

    if bearish_engulfing(
        df1
    ):

        score += 5

        reasons.append(
            "1M bearish trigger"
        )

    elif bearish_rejection(
        df1
    ):

        score += 4

        reasons.append(
            "1M bearish rejection"
        )

    if bearish_impulse(
        df5
    ):

        score += 7

        reasons.append(
            "5M bearish impulse"
        )

    if compression_detected(
        df5
    ):

        score += 4

        reasons.append(
            "5M compression"
        )

    volume_ratio = volume_confirmation(
        df5
    )

    if volume_ratio >= 1.50:

        score += 7

        reasons.append(
            "Breakdown volume strong"
        )

    elif volume_ratio >= 1.20:

        score += 4

        reasons.append(
            "Volume confirmation"
        )

    position = move_position(
        df5,
        50
    )

    if 0.30 <= position <= 0.65:

        score += 7

        reasons.append(
            "Short entry not late"
        )

    elif 0.18 <= position < 0.30:

        score += 2

        reasons.append(
            "Short move advanced"
        )

    elif position < 0.18:

        score -= 12

        reasons.append(
            "Short entry too late"
        )

    if late_short_move(
        df5
    ):

        score -= 12

        reasons.append(
            "Late short move penalty"
        )

    if failed_bear_breakdown(
        df5,
        br5["level_low"]
    ):

        score -= 18

        reasons.append(
            "Failed bearish breakdown"
        )

    last_1m = df1.iloc[-1]

    if (
        last_1m["close"]
        <
        last_1m["open"]
    ):

        score += 3

        reasons.append(
            "1M bearish close"
        )

    # --------------------------------------------------------
    # BTC MARKET CONTEXT  (YENİ)
    # --------------------------------------------------------

    if btc_context:

        if btc_context["direction"] == "SHORT":

            bonus = round(btc_context["strength"] / 100 * 10)

            score += bonus

            reasons.append(
                f"BTC context destekliyor (+{bonus})"
            )

        elif btc_context["direction"] == "LONG":

            penalty = round(btc_context["strength"] / 100 * 10)

            score -= penalty

            reasons.append(
                f"BTC context ters yönde (-{penalty})"
            )

    return score, reasons


# ============================================================
# FUNDING
# ============================================================

def get_funding(
    symbol
):

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

    ticker = candidate[
        "ticker"
    ]

    score = 0

    pct = ticker[
        "percentage"
    ]

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

def is_cooldown(
    symbol
):

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


def set_cooldown(
    symbol
):

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

def side_count(
    side
):

    with state_lock:

        return sum(
            1
            for p
            in positions.values()
            if p["side"] == side
        )


# ============================================================
# LEVERAGE
# ============================================================

def choose_leverage(
    score,
    atr_percent,
    btc_context=None
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

    if btc_context and btc_context.get("strength", 0) >= 70:
        leverage += 1

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
        notional / price
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

def calculate_target_roi(
    score,
    atr_percent,
    ticker_percentage
):

    base_move = max(abs(ticker_percentage) / 100.0, 0.01)

    score_factor = score / 100.0

    target = max(0.01, base_move * score_factor)

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
        quantity * price
    )

    if quantity <= 0:

        return False

    max_loss_pnl_fraction = 0.50
    target_pnl_fraction = target_roi * leverage
    allowed_max_loss_fraction = target_pnl_fraction * max_loss_pnl_fraction

    max_loss_stop_distance_pct = allowed_max_loss_fraction / leverage

    calculated_hard_stop_distance = atr_value * HARD_STOP_ATR / price
    if calculated_hard_stop_distance > max_loss_stop_distance_pct:
        effective_hard_stop_distance = max_loss_stop_distance_pct * price
    else:
        effective_hard_stop_distance = atr_value * HARD_STOP_ATR

    if side == "LONG":

        stop_price = (
            price
            -
            effective_hard_stop_distance
        )

    else:

        stop_price = (
            price
            +
            effective_hard_stop_distance
        )

    opened_at = (
        now_utc()
        .isoformat()
    )

    position = {

        "symbol":
            symbol,

        "side":
            side,

        "entry":
            price,

        "current_price":
            price,

        "margin":
            MARGIN_PER_POSITION,

        "leverage":
            leverage,

        "notional":
            notional,

        "quantity":
            quantity,

        "score":
            score,

        "entry_reason":
            reasons,

        "atr":
            atr_value,

        "highest":
            price,

        "lowest":
            price,

        "stop_price":
            stop_price,

        "initial_stop":
            stop_price,

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
            reasons[:20]
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

        max_loss_pnl_fraction = 0.50
        target_pnl_fraction = target_roi * leverage
        allowed_max_loss_fraction = target_pnl_fraction * max_loss_pnl_fraction
        max_loss_stop_distance_pct = allowed_max_loss_fraction / leverage

        calculated_hard_stop_distance = atr_value * HARD_STOP_ATR / price
        if calculated_hard_stop_distance > max_loss_stop_distance_pct:
            effective_hard_stop_distance = max_loss_stop_distance_pct * price
        else:
            effective_hard_stop_distance = atr_value * HARD_STOP_ATR

        if side == "LONG":

            initial_stop = (
                price
                -
                effective_hard_stop_distance
            )

        else:

            initial_stop = (
                price
                +
                effective_hard_stop_distance
            )

        position = {

            "symbol":
                symbol,

            "side":
                side,

            "entry":
                price,

            "current_price":
                price,

            "margin":
                MARGIN_PER_POSITION,

            "leverage":
                leverage,

            "notional":
                quantity * price,

            "quantity":
                quantity,

            "score":
                score,

            "entry_reason":
                reasons,

            "atr":
                atr_value,

            "highest":
                price,

            "lowest":
                price,

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

    entry = position[
        "entry"
    ]

    target_roi = position.get("target_roi", 0.012)
    leverage = position["leverage"]

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
            position[
                "highest"
            ],
            current_price
        )

        # DÜZELTME: Erken kilitlenmeyi önlemek için adım bazlı stop seviyesinin 
        # devreye girmesi için ROI'nin anlamlı bir eşiği (hedefin en az %40'ı veya %3 minimum) aşması şartı eklendi.
        activation_limit = max(0.03, target_roi * 0.40)
        if roi >= activation_limit:
            steps_reached = int(roi // 0.01)
            if steps_reached > 0:
                step_stop = entry * (1.0 + (steps_reached * 0.01 / leverage))
                position["stop_price"] = max(position["stop_price"], step_stop)

            position["trailing_active"] = True

            dynamic_atr_distance = atr_value * TRAIL_ATR_TIGHT
            dynamic_stop = position["highest"] - dynamic_atr_distance
            position["stop_price"] = max(position["stop_price"], dynamic_stop)

    else:

        position[
            "lowest"
        ] = min(
            position[
                "lowest"
            ],
            current_price
        )

        activation_limit = max(0.03, target_roi * 0.40)
        if roi >= activation_limit:
            steps_reached = int(roi // 0.01)
            if steps_reached > 0:
                step_stop = entry * (1.0 - (steps_reached * 0.01 / leverage))
                position["stop_price"] = min(position["stop_price"], step_stop)

            position["trailing_active"] = True

            dynamic_atr_distance = atr_value * TRAIL_ATR_TIGHT
            dynamic_stop = position["lowest"] + dynamic_atr_distance
            position["stop_price"] = min(position["stop_price"], dynamic_stop)

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

    duration = (
        calculate_duration_seconds(
            position[
                "opened_at"
            ],
            closed_at
        )
    )

    pnl, roi = calculate_pnl(
        position,
        exit_price
    )

    trade = {

        "symbol":
            position[
                "symbol"
            ],

        "side":
            position[
                "side"
            ],

        "margin":
            position[
                "margin"
            ],

        "leverage":
            position[
                "leverage"
            ],

        "notional":
            position[
                "notional"
            ],

        "quantity":
            position[
                "quantity"
            ],

        "entry_price":
            position[
                "entry"
            ],

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
            position[
                "opened_at"
            ],

        "closed_at":
            closed_at,
    }

    with state_lock:

        trade_history.append(
            trade
        )

        if len(
            trade_history
        ) > MAX_TRADE_HISTORY:

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

        stats[
            "wins"
        ] += 1

    else:

        stats[
            "losses"
        ] += 1

    return trade


# ============================================================
# POZİSYON KAPANIŞ ÖZETİ  (YENİ)
# ============================================================
# Her kapanan pozisyon için: coin, margin, kaldıraç, giriş/çıkış
# fiyatı, hedeflenen ve gerçekleşen kazanç tek bir kompakt blokta.

def log_position_close_summary(
    trade
):

    result = (
        "KAR"
        if trade["pnl"] >= 0
        else "ZARAR"
    )

    logger.warning(
        "┌─ POZİSYON KAPANDI [%s] ─────────────────────────",
        result
    )

    logger.warning(
        "│ Coin        : %s (%s)",
        trade["symbol"],
        trade["side"]
    )

    logger.warning(
        "│ Margin      : $%.2f | Kaldıraç: %sx",
        trade["margin"],
        trade["leverage"]
    )

    logger.warning(
        "│ Giriş       : %.8f  →  Çıkış: %.8f",
        trade["entry_price"],
        trade["exit_price"]
    )

    logger.warning(
        "│ Hedef ROI   : %.2f%%  |  Gerçekleşen ROI: %.2f%%  |  Peak: %.2f%%",
        trade["target_roi_percent"],
        trade["realized_roi_percent"],
        trade["peak_roi_percent"]
    )

    logger.warning(
        "│ PNL         : $%.4f  |  Süre: %s  |  Sebep: %s",
        trade["pnl"],
        trade["duration"],
        trade["exit_reason"]
    )

    logger.warning(
        "└──────────────────────────────────────────────────"
    )


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

        exit_price = position[
            "current_price"
        ]

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
    ] += trade[
        "pnl"
    ]

    set_cooldown(
        symbol
    )

    log_position_close_summary(
        trade
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
                "reduceOnly":
                    True
            }
        )

        exit_price = position[
            "current_price"
        ]

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
        ] += trade[
            "pnl"
        ]

        set_cooldown(
            symbol
        )

        log_position_close_summary(
            trade
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

                    if (
                        price
                        <=
                        stop_price
                    ):

                        stop_hit = True

                else:

                    if (
                        price
                        >=
                        stop_price
                    ):

                        stop_hit = True

                if stop_hit:

                    live_close(
                        symbol,
                        "TRAILING_STOP"
                    )

                    continue

                logger.info(
                    "POSITION | %s | %s | "
                    "entry=%.8f | "
                    "price=%.8f | "
                    "ROI=%.2f%% | "
                    "PNL=$%.3f | "
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
# HOURLY REPORT  (KISA ÖZET — YENİ)
# ============================================================
# Uzun, işlem işlem dökülen eski rapor yerine: kaç işlem açıldı,
# kaçı kârla / kaçı zararla kapandı, net kâr/zarar — tek bakışta.

def generate_hourly_report():

    report_time = now_utc()

    current_hour_start = (
        report_time.replace(
            minute=0,
            second=0,
            microsecond=0
        )
    )

    previous_hour_start = (
        current_hour_start
        -
        timedelta(
            hours=1
        )
    )

    previous_hour_end = (
        current_hour_start
    )

    with state_lock:

        history_snapshot = list(
            trade_history
        )

        open_count = len(
            positions
        )

    hourly_trades = []

    for trade in history_snapshot:

        try:

            closed_at = datetime.fromisoformat(
                trade[
                    "closed_at"
                ]
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

    hourly_count = len(
        hourly_trades
    )

    hourly_wins = sum(
        1
        for t
        in hourly_trades
        if t["pnl"] >= 0
    )

    hourly_losses = (
        hourly_count
        -
        hourly_wins
    )

    hourly_pnl = sum(
        t["pnl"]
        for t
        in hourly_trades
    )

    logger.warning(
        "═══ SAATLİK ÖZET | %s → %s UTC ═══",
        previous_hour_start.strftime("%H:%M"),
        previous_hour_end.strftime("%H:%M")
    )

    logger.warning(
        "İşlem: %s açıldı | %s kâr, %s zarar | Net PNL: $%.4f | Açık pozisyon: %s/%s",
        hourly_count,
        hourly_wins,
        hourly_losses,
        hourly_pnl,
        open_count,
        MAX_POSITIONS
    )

    total_trades = (
        stats["wins"]
        +
        stats["losses"]
    )

    total_win_rate = (
        (stats["wins"] / total_trades * 100)
        if total_trades > 0
        else 0.0
    )

    logger.warning(
        "TOPLAM (bot başlangıcından beri): %s işlem | win rate %.1f%% | net PNL $%.4f",
        total_trades,
        total_win_rate,
        stats["total_realized_pnl"]
    )


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
# BTC MARKET CONTEXT  (YENİ)
# ============================================================
# BTC işlem evreninden tamamen hariç tutulur (valid_symbol zaten
# banned listesinde tutuyor). Burada BTC SADECE piyasa yönü
# teyidi (context) ve korelasyon kontrolü için kullanılır — asla
# doğrudan işlem adayı olmaz.

BTC_SYMBOL = "BTC/USDT"

CORRELATION_MAX_ALLOWED = 0.85

_btc_context_cache = {
    "timestamp": 0,
    "context": {"direction": "NEUTRAL", "strength": 0},
}

BTC_CONTEXT_CACHE_SECONDS = 30


def compute_btc_context():
    """BTC 1H market structure + kısa vadeli impulse'a göre basit
    bir 'piyasa yönü' bağlamı üretir. Sert bir veto değildir —
    score_long/score_short içinde küçük bir bonus/penalty olarak
    kullanılır."""

    df1h = fetch_ohlcv_cached(
        BTC_SYMBOL,
        TIMEFRAME_TREND,
        220
    )

    df15 = fetch_ohlcv_cached(
        BTC_SYMBOL,
        TIMEFRAME_CONFIRM,
        220
    )

    if df1h is None or df15 is None:
        return {"direction": "NEUTRAL", "strength": 0}

    structure_1h = market_structure(df1h)

    direction = "NEUTRAL"
    strength = 0

    if structure_1h["structure"] == "BULLISH":
        direction = "LONG"
        strength = 70
    elif structure_1h["structure"] == "BULLISH_WEAK":
        direction = "LONG"
        strength = 35
    elif structure_1h["structure"] == "BEARISH":
        direction = "SHORT"
        strength = 70
    elif structure_1h["structure"] == "BEARISH_WEAK":
        direction = "SHORT"
        strength = 35

    if direction == "LONG" and bullish_impulse(df15):
        strength = min(100, strength + 20)
    elif direction == "SHORT" and bearish_impulse(df15):
        strength = min(100, strength + 20)

    return {"direction": direction, "strength": strength}


def get_btc_context():
    """BTC context'i her sembol için değil, tarama döngüsü başına
    bir kez hesaplayıp kısa süreliğine cache'ler (gereksiz API
    yükünü azaltmak için)."""

    current = time.time()

    if (
        current - _btc_context_cache["timestamp"]
        < BTC_CONTEXT_CACHE_SECONDS
    ):
        return _btc_context_cache["context"]

    try:
        context = compute_btc_context()
    except Exception as e:
        logger.warning("BTC context hesaplanamadı: %s", e)
        context = {"direction": "NEUTRAL", "strength": 0}

    _btc_context_cache["timestamp"] = current
    _btc_context_cache["context"] = context

    return context


# ============================================================
# KORELASYON KONTROLÜ  (YENİ)
# ============================================================
# BTC ve açık pozisyonlarla yüksek korelasyonlu yeni işlem
# açılmasını engeller — aynı hareketin tekrar tekrar
# fiyatlanmasını önlemeye çalışır.

def get_recent_returns(symbol, timeframe="1h", n=30):
    try:
        df = fetch_ohlcv_cached(symbol, timeframe, n + 5)
        if df is None or len(df) < n:
            return None
        closes = df["close"].tail(n).values
        returns = np.diff(closes) / closes[:-1]
        return returns
    except Exception:
        return None


def is_correlation_blocked(symbol):
    """Aday, açık pozisyonlardan biriyle çok yüksek korelasyonlu
    mu? (Aynı BTC hareketine bağımlı çoklu pozisyon açmayı önlemek
    için.)"""

    candidate_returns = get_recent_returns(symbol)

    if candidate_returns is None:
        return False

    with state_lock:
        open_symbols = list(positions.keys())

    for open_symbol in open_symbols:

        if open_symbol == symbol:
            continue

        other_returns = get_recent_returns(open_symbol)

        if other_returns is None or len(other_returns) != len(candidate_returns):
            continue

        try:
            corr = np.corrcoef(candidate_returns, other_returns)[0, 1]
        except Exception:
            continue

        if not math.isnan(corr) and corr >= CORRELATION_MAX_ALLOWED:
            logger.info(
                "KORELASYON RED | %s açık pozisyon %s ile yüksek korelasyonlu (%.2f)",
                symbol, open_symbol, corr
            )
            return True

    return False


# ============================================================
# ANALYZE SYMBOL
# ============================================================

def analyze_symbol(
    candidate,
    btc_context=None
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
        220
    )

    df5 = fetch_ohlcv_cached(
        symbol,
        TIMEFRAME_ENTRY,
        220
    )

    df15 = fetch_ohlcv_cached(
        symbol,
        TIMEFRAME_CONFIRM,
        220
    )

    df1h = fetch_ohlcv_cached(
        symbol,
        TIMEFRAME_TREND,
        220
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

    if any(
        len(x) < 80
        for x in [
            df1,
            df5,
            df15,
            df1h
        ]
    ):

        return None

    price = safe_float(
        ticker[
            "last"
        ]
    )

    if price <= 0:

        return None

    true_range = pd.concat(
        [
            df5["high"]
            -
            df5["low"],

            (
                df5["high"]
                -
                df5["close"].shift(1)
            ).abs(),

            (
                df5["low"]
                -
                df5["close"].shift(1)
            ).abs(),
        ],
        axis=1
    ).max(
        axis=1
    )

    atr_value = safe_float(
        true_range.ewm(
            alpha=1 / 14,
            adjust=False
        ).mean().iloc[-1]
    )

    if atr_value <= 0:

        return None

    atr_percent = (
        atr_value
        /
        price
    ) * 100

    long_score, long_reasons = (
        score_long(
            df1,
            df5,
            df15,
            df1h,
            btc_context
        )
    )

    short_score, short_reasons = (
        score_short(
            df1,
            df5,
            df15,
            df1h,
            btc_context
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
        short_score + 7
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
        long_score + 7
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

    # --------------------------------------------------------
    # KORELASYON KONTROLÜ  (YENİ)
    # --------------------------------------------------------

    if is_correlation_blocked(symbol):
        return None

    leverage = choose_leverage(
        score,
        atr_percent,
        btc_context
    )

    target_roi = (
        calculate_target_roi(
            score,
            atr_percent,
            ticker["percentage"]
        )
    )

    if target_roi < 0.01:
        return None

    structure_1h = market_structure(
        df1h
    )

    structure_15 = market_structure(
        df15
    )

    structure_5 = market_structure(
        df5
    )

    position = move_position(
        df5,
        50
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

        "structure_1h":
            structure_1h[
                "structure"
            ],

        "structure_15m":
            structure_15[
                "structure"
            ],

        "structure_5m":
            structure_5[
                "structure"
            ],

        "move_position":
            position,

        "breakout_5m":
            breakout_analysis(
                df5,
                20
            ),

        "breakout_15m":
            breakout_analysis(
                df15,
                20
            ),
    }


# ============================================================
# FIND BEST OPPORTUNITY
# ============================================================

def find_best_signal(
    candidates,
    btc_context=None
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
        "Detaylı price action analiz: %s coin",
        len(ranked)
    )

    for candidate in ranked:

        try:

            result = analyze_symbol(
                candidate,
                btc_context
            )

            if result:

                results.append(
                    result
                )

                logger.info(
                    "PA SIGNAL CANDIDATE | "
                    "%s | %s | "
                    "score=%s | "
                    "1H=%s | "
                    "15M=%s | "
                    "5M=%s | "
                    "24h=%.2f%% | "
                    "move_pos=%.2f | "
                    "target=%.2f%%",
                    result["side"],
                    result["symbol"],
                    result["score"],
                    result["structure_1h"],
                    result["structure_15m"],
                    result["structure_5m"],
                    result[
                        "ticker_percentage"
                    ],
                    result[
                        "move_position"
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
        abs(
            signal[
                "funding"
            ]
        )
        >=
        MAX_ABS_FUNDING
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

        if move > 0.015:

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

    df1 = fetch_ohlcv_cached(
        symbol,
        TIMEFRAME_FAST,
        80
    )

    df5 = fetch_ohlcv_cached(
        symbol,
        TIMEFRAME_ENTRY,
        120
    )

    if (
        df1 is None
        or
        df5 is None
    ):

        return False

    if side == "LONG":

        trigger = (

            bullish_engulfing(
                df1
            )

            or

            bullish_rejection(
                df1
            )

            or

            strong_bullish_candle(
                df1
            )

            or

            bullish_engulfing(
                df5
            )

            or

            bullish_rejection(
                df5
            )

        )

        if not trigger:

            logger.info(
                "ENTRY SKIP | %s | "
                "final LONG price action trigger yok",
                symbol
            )

            return False

    else:

        trigger = (

            bearish_engulfing(
                df1
            )

            or

            bearish_rejection(
                df1
            )

            or

            strong_bearish_candle(
                df1
            )

            or

            bearish_engulfing(
                df5
            )

            or

            bearish_rejection(
                df5
            )

        )

        if not trigger:

            logger.info(
                "ENTRY SKIP | %s | "
                "final SHORT price action trigger yok",
                symbol
            )

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
        "PA SCORE=%s | leverage=%sx | "
        "target ROI=%.2f%% | "
        "1H=%s | 15M=%s | 5M=%s",
        signal["side"],
        signal["symbol"],
        signal["score"],
        signal["leverage"],
        signal["target_roi"] * 100,
        signal["structure_1h"],
        signal["structure_15m"],
        signal["structure_5m"]
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
        "PURE PRICE ACTION ANALİZ BAŞLADI | %s",
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
        "GAINERS %s-%s: %s",
        RANK_START,
        RANK_END,
        [
            x["symbol"]
            for x in gainers
        ]
    )

    logger.info(
        "LOSERS %s-%s: %s",
        RANK_START,
        RANK_END,
        [
            x["symbol"]
            for x in losers
        ]
    )

    logger.info(
        "24H VOLUME 1-%s: %s",
        VOLUME_LIMIT,
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
        "Benzersiz PA aday havuzu: %s",
        len(candidates)
    )

    if (
        current_position_count()
        >=
        MAX_POSITIONS
    ):

        logger.info(
            "%s pozisyon zaten açık. "
            "Yeni işlem aranmayacak.",
            MAX_POSITIONS
        )

        return

    # --------------------------------------------------------
    # BTC MARKET CONTEXT — döngü başına bir kez  (YENİ)
    # --------------------------------------------------------

    btc_context = get_btc_context()

    logger.info(
        "BTC CONTEXT | yön=%s | güç=%s",
        btc_context["direction"],
        btc_context["strength"]
    )

    signals = (
        find_best_signal(
            candidates,
            btc_context
        )
    )

    if not signals:

        logger.info(
            "Uygun price action sinyali bulunamadı."
        )

        return

    logger.info(
        "Toplam PA sinyali: %s",
        len(signals)
    )

    for signal in signals[:5]:

        logger.info(
            "TOP PA SIGNAL | %s | %s | "
            "score=%s | "
            "1H=%s | "
            "15M=%s | "
            "5M=%s | "
            "24h=%.2f%% | "
            "move=%.2f | "
            "target=%.2f%% | "
            "sources=%s",
            signal["side"],
            signal["symbol"],
            signal["score"],
            signal["structure_1h"],
            signal["structure_15m"],
            signal["structure_5m"],
            signal[
                "ticker_percentage"
            ],
            signal[
                "move_position"
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
        "PURE PRICE ACTION FUTURES BOT"
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
        "GAINERS = %s-%s",
        RANK_START,
        RANK_END
    )

    logger.warning(
        "LOSERS = %s-%s",
        RANK_START,
        RANK_END
    )

    logger.warning(
        "24H VOLUME = 1-%s",
        VOLUME_LIMIT
    )

    logger.warning(
        "SIGNAL ENGINE = PURE PRICE ACTION + BTC CONTEXT"
    )

    logger.warning(
        "1H / 15M / 5M / 1M"
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
            "PURE_PRICE_ACTION_FUTURES_BOT",

        "status":
            "running",

        "dry_run":
            DRY_RUN,

        "positions":
            len(
                positions
            ),

        "max_positions":
            MAX_POSITIONS,

        "margin_per_position":
            MARGIN_PER_POSITION,

        "max_leverage":
            MAX_LEVERAGE,

        "gainers_range":
            f"{RANK_START}-{RANK_END}",

        "losers_range":
            f"{RANK_START}-{RANK_END}",

        "volume_range":
            f"1-{VOLUME_LIMIT}",

        "signal_engine":
            "PURE_PRICE_ACTION + BTC_CONTEXT",

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

        "signal_engine":
            "PURE_PRICE_ACTION + BTC_CONTEXT",

        "coin_pool": {

            "gainers":
                f"{RANK_START}-{RANK_END}",

            "losers":
                f"{RANK_START}-{RANK_END}",

            "volume":
                f"1-{VOLUME_LIMIT}",
        },

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

    monitor = threading.Thread(
        target=position_monitor,
        daemon=True,
        name="PositionMonitor"
    )

    monitor.start()

    bot = threading.Thread(
        target=bot_loop,
        daemon=True,
        name="AnalysisLoop"
    )

    bot.start()

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
