import os
import time
import math
import logging
import threading
import traceback
from datetime import datetime, timezone

import ccxt
import numpy as np
import pandas as pd
from flask import Flask, jsonify


# ============================================================
# BINANCE FUTURES EARLY-MOMENTUM BOT
# ============================================================
#
# AMAÇ:
# Binance Futures üzerinde:
#
#   GAINERS  : ilk 25
#   LOSERS   : ilk 25
#   VOLUME   : ilk 25
#
# coinlerini tarar.
#
# BTC ve XAU hariç tutulur.
#
# Erken momentum / reversal / breakout fırsatlarını;
#
#   1m
#   5m
#   15m
#   1h
#
# zaman dilimleriyle teyit eder.
#
# MAKSİMUM:
#   2 açık pozisyon
#   10 USDT margin / pozisyon
#   maksimum 5x leverage
#
# Pozisyonlar:
#   LONG / SHORT
#
# Takip:
#   ATR + momentum tabanlı trailing stop
#
# ÖNEMLİ:
# DRY_RUN = True
#
# Bu nedenle hiçbir gerçek emir gönderilmez.
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

# Minimum skor
MIN_LONG_SCORE = 72
MIN_SHORT_SCORE = 72

# Daha erken giriş için:
EARLY_ENTRY_SCORE = 76

# Funding filtresi
MAX_ABS_FUNDING = 0.0015

# Aynı coin tekrar işlem açmadan önce
COOLDOWN_MINUTES = 60

# Minimum 24h quote volume
MIN_QUOTE_VOLUME = 2_000_000

# Spread filtresi
MAX_SPREAD_PERCENT = 0.15

# Pozisyon yönetimi
MIN_PROFIT_TO_TRAIL = 0.004
HARD_STOP_ATR = 1.8

# Trailing
TRAIL_ATR_MULTIPLIER = 1.35
TRAIL_ATR_TIGHT = 1.05

# Kâr arttıkça trailing sıkılaşır
TRAIL_LEVEL_1 = 0.008
TRAIL_LEVEL_2 = 0.015
TRAIL_LEVEL_3 = 0.025

# Pozisyon çok kısa sürede tersine dönerse
EMERGENCY_REVERSE_THRESHOLD = 0.007

# Aynı anda birbirine çok benzeyen coinlerde
MAX_CORRELATED_SIDE = 2

# Analiz cache
OHLCV_CACHE_SECONDS = 12

# Adayların ayrıntılı analizinde en fazla
# kaç coin değerlendirilsin
MAX_DETAILED_CANDIDATES = 45

# Gainer / loser / volume listeleri
LIST_LIMIT = 25

# Log
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


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

last_scan_time = None

bot_started_at = datetime.now(timezone.utc).isoformat()

stats = {
    "scans": 0,
    "signals": 0,
    "simulated_entries": 0,
    "simulated_exits": 0,
    "wins": 0,
    "losses": 0,
    "total_realized_pnl": 0.0,
}


# ============================================================
# POSITION STRUCTURE
# ============================================================

# positions[symbol] =
#
# {
#     symbol,
#     side,
#     entry,
#     current_price,
#     margin,
#     leverage,
#     notional,
#     quantity,
#     score,
#     entry_reason,
#
#     atr,
#     highest,
#     lowest,
#
#     stop_price,
#     trailing_active,
#
#     unrealized_pnl,
#     unrealized_roi,
#
#     opened_at,
#     last_update
# }


# ============================================================
# BASIC HELPERS
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
    return symbol.replace("/", "").replace(":USDT", "")


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
            if not market.get("active", True):
                return False

            if market.get("quote") != "USDT":
                return False

            if market.get("settle") not in (None, "USDT"):
                return False

        return True

    except Exception:
        return False


# ============================================================
# MARKETS
# ============================================================

def load_markets():
    logger.info("Binance Futures marketleri yükleniyor...")

    markets = exchange.load_markets()

    logger.info(
        "Toplam market: %s",
        len(markets)
    )

    return markets


# ============================================================
# 24H TICKERS
# ============================================================

def get_futures_tickers():
    try:
        tickers = exchange.fetch_tickers()

        result = {}

        for symbol, ticker in tickers.items():

            market = exchange.markets.get(symbol)

            if not valid_symbol(symbol, market):
                continue

            quote_volume = safe_float(
                ticker.get("quoteVolume")
            )

            last = safe_float(
                ticker.get("last")
            )

            percentage = safe_float(
                ticker.get("percentage")
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
                spread = ((ask - bid) / ((ask + bid) / 2)) * 100

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
                "high": safe_float(ticker.get("high")),
                "low": safe_float(ticker.get("low")),
                "open": safe_float(ticker.get("open")),
            }

        return result

    except Exception as e:

        logger.error(
            "Ticker alınamadı: %s",
            e
        )

        return {}


# ============================================================
# BUILD TOP 25 LISTS
# ============================================================

def build_rank_lists(tickers):

    data = list(tickers.values())

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

def build_candidate_pool(gainers, losers, volumes):

    candidates = {}

    def add(items, source):

        for rank, item in enumerate(items, start=1):

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

            candidates[symbol]["sources"].append(source)

            if source == "GAINER":
                candidates[symbol]["gainer_rank"] = rank

            elif source == "LOSER":
                candidates[symbol]["loser_rank"] = rank

            elif source == "VOLUME":
                candidates[symbol]["volume_rank"] = rank

    add(gainers, "GAINER")
    add(losers, "LOSER")
    add(volumes, "VOLUME")

    return candidates


# ============================================================
# OHLCV CACHE
# ============================================================

def fetch_ohlcv_cached(symbol, timeframe, limit=160):

    key = (symbol, timeframe)

    current = time.time()

    cached = ohlcv_cache.get(key)

    if cached:
        timestamp, data = cached

        if current - timestamp < OHLCV_CACHE_SECONDS:
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

        df = df.dropna().reset_index(drop=True)

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

def ema(series, period):

    return series.ewm(
        span=period,
        adjust=False
    ).mean()


def rsi(series, period=14):

    delta = series.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

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

    rs = avg_gain / avg_loss.replace(0, np.nan)

    result = 100 - (100 / (1 + rs))

    return result.fillna(50)


def atr(df, period=14):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    previous_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1
    ).max(axis=1)

    return tr.ewm(
        alpha=1 / period,
        min_periods=period,
        adjust=False
    ).mean()


def macd(series):

    fast = ema(series, 12)
    slow = ema(series, 26)

    line = fast - slow
    signal = ema(line, 9)

    histogram = line - signal

    return line, signal, histogram


def bollinger(series, period=20, std_mult=2):

    mid = series.rolling(period).mean()
    std = series.rolling(period).std()

    upper = mid + std_mult * std
    lower = mid - std_mult * std

    return mid, upper, lower


def adx(df, period=14):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where(
        (up_move > down_move) & (up_move > 0),
        up_move,
        0
    )

    minus_dm = np.where(
        (down_move > up_move) & (down_move > 0),
        down_move,
        0
    )

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()

    tr = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    atr_val = tr.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    plus_di = (
        100 *
        pd.Series(plus_dm, index=df.index)
        .ewm(alpha=1 / period, adjust=False)
        .mean()
        / atr_val
    )

    minus_di = (
        100 *
        pd.Series(minus_dm, index=df.index)
        .ewm(alpha=1 / period, adjust=False)
        .mean()
        / atr_val
    )

    dx = (
        100 *
        (plus_di - minus_di).abs()
        /
        (plus_di + minus_di).replace(0, np.nan)
    )

    return dx.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()


# ============================================================
# INDICATOR FRAME
# ============================================================

def calculate_indicators(df):

    if df is None or len(df) < 80:
        return None

    x = df.copy()

    x["ema9"] = ema(x["close"], 9)
    x["ema21"] = ema(x["close"], 21)
    x["ema50"] = ema(x["close"], 50)
    x["ema200"] = ema(x["close"], 200)

    x["rsi"] = rsi(x["close"])

    x["atr"] = atr(x)

    macd_line, macd_signal, macd_hist = macd(
        x["close"]
    )

    x["macd"] = macd_line
    x["macd_signal"] = macd_signal
    x["macd_hist"] = macd_hist

    x["adx"] = adx(x)

    bb_mid, bb_upper, bb_lower = bollinger(
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
        x["volume_ma20"].replace(0, np.nan)
    )

    x["roc5"] = (
        x["close"].pct_change(5) * 100
    )

    x["roc10"] = (
        x["close"].pct_change(10) * 100
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
        x["high"] - x["low"]
    )

    x["body"] = (
        x["close"] - x["open"]
    ).abs()

    x["body_ratio"] = (
        x["body"]
        /
        x["range"].replace(0, np.nan)
    )

    x["bb_width"] = (
        (x["bb_upper"] - x["bb_lower"])
        /
        x["bb_mid"].replace(0, np.nan)
    )

    return x


# ============================================================
# CANDLE QUALITY
# ============================================================

def candle_direction(df):

    if len(df) < 3:
        return 0

    last = df.iloc[-1]

    if last["close"] > last["open"]:
        return 1

    if last["close"] < last["open"]:
        return -1

    return 0


def bullish_reversal(df):

    if len(df) < 3:
        return False

    a = df.iloc[-2]
    b = df.iloc[-1]

    return (
        a["close"] < a["open"]
        and
        b["close"] > b["open"]
        and
        b["close"] > a["open"]
    )


def bearish_reversal(df):

    if len(df) < 3:
        return False

    a = df.iloc[-2]
    b = df.iloc[-1]

    return (
        a["close"] > a["open"]
        and
        b["close"] < b["open"]
        and
        b["close"] < a["open"]
    )


# ============================================================
# EARLY LONG SCORE
# ============================================================

def score_long(df1, df5, df15, df1h):

    score = 0
    reasons = []

    a = df1.iloc[-1]
    b = df5.iloc[-1]
    c = df15.iloc[-1]
    d = df1h.iloc[-1]

    # --------------------------------------------------------
    # 1H TREND
    # --------------------------------------------------------

    if d["ema21"] > d["ema50"]:
        score += 8
        reasons.append("1H EMA21>EMA50")

    if d["close"] > d["ema50"]:
        score += 6
        reasons.append("1H above EMA50")

    if d["ema9"] > d["ema21"]:
        score += 4
        reasons.append("1H short trend up")

    # --------------------------------------------------------
    # 15M TREND
    # --------------------------------------------------------

    if c["ema9"] > c["ema21"]:
        score += 7
        reasons.append("15M EMA9>EMA21")

    if c["close"] > c["ema21"]:
        score += 4
        reasons.append("15M above EMA21")

    if c["rsi"] > 52:
        score += 4
        reasons.append("15M RSI bullish")

    if c["macd_hist"] > 0:
        score += 4
        reasons.append("15M MACD positive")

    if c["adx"] > 18:
        score += 4
        reasons.append("15M ADX")

    # --------------------------------------------------------
    # 5M MOMENTUM
    # --------------------------------------------------------

    if b["ema9"] > b["ema21"]:
        score += 7
        reasons.append("5M EMA bullish")

    if b["close"] > b["ema9"]:
        score += 3
        reasons.append("5M above EMA9")

    if b["rsi"] > 52:
        score += 4
        reasons.append("5M RSI")

    if 52 <= b["rsi"] <= 72:
        score += 3
        reasons.append("5M RSI optimal")

    if b["macd_hist"] > 0:
        score += 5
        reasons.append("5M MACD")

    if b["macd_hist"] > df5["macd_hist"].iloc[-2]:
        score += 3
        reasons.append("MACD accelerating")

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    if b["volume_ratio"] >= 1.30:
        score += 6
        reasons.append("Volume expansion")

    elif b["volume_ratio"] >= 1.10:
        score += 3
        reasons.append("Volume improving")

    # --------------------------------------------------------
    # BREAKOUT
    # --------------------------------------------------------

    if b["close"] > b["high20"]:
        score += 8
        reasons.append("20 candle breakout")

    elif b["high"] > b["high20"]:
        score += 4
        reasons.append("Breakout attempt")

    # --------------------------------------------------------
    # EARLY MOMENTUM
    # --------------------------------------------------------

    if 0.15 <= b["roc5"] <= 2.5:
        score += 4
        reasons.append("Early ROC")

    if b["roc10"] > 0:
        score += 3
        reasons.append("ROC10 positive")

    # --------------------------------------------------------
    # REVERSAL
    # --------------------------------------------------------

    if bullish_reversal(df5):
        score += 6
        reasons.append("Bullish reversal")

    # --------------------------------------------------------
    # 1M ENTRY
    # --------------------------------------------------------

    if a["ema9"] > a["ema21"]:
        score += 4
        reasons.append("1M momentum")

    if a["macd_hist"] > 0:
        score += 3
        reasons.append("1M MACD")

    # --------------------------------------------------------
    # AVOID OVEREXTENSION
    # --------------------------------------------------------

    if b["rsi"] > 78:
        score -= 10
        reasons.append("Overbought penalty")

    if c["rsi"] > 78:
        score -= 8
        reasons.append("15M overbought")

    return score, reasons


# ============================================================
# EARLY SHORT SCORE
# ============================================================

def score_short(df1, df5, df15, df1h):

    score = 0
    reasons = []

    a = df1.iloc[-1]
    b = df5.iloc[-1]
    c = df15.iloc[-1]
    d = df1h.iloc[-1]

    # --------------------------------------------------------
    # 1H TREND
    # --------------------------------------------------------

    if d["ema21"] < d["ema50"]:
        score += 8
        reasons.append("1H EMA21<EMA50")

    if d["close"] < d["ema50"]:
        score += 6
        reasons.append("1H below EMA50")

    if d["ema9"] < d["ema21"]:
        score += 4
        reasons.append("1H short trend down")

    # --------------------------------------------------------
    # 15M
    # --------------------------------------------------------

    if c["ema9"] < c["ema21"]:
        score += 7
        reasons.append("15M EMA bearish")

    if c["close"] < c["ema21"]:
        score += 4
        reasons.append("15M below EMA21")

    if c["rsi"] < 48:
        score += 4
        reasons.append("15M RSI bearish")

    if c["macd_hist"] < 0:
        score += 4
        reasons.append("15M MACD negative")

    if c["adx"] > 18:
        score += 4
        reasons.append("15M ADX")

    # --------------------------------------------------------
    # 5M
    # --------------------------------------------------------

    if b["ema9"] < b["ema21"]:
        score += 7
        reasons.append("5M EMA bearish")

    if b["close"] < b["ema9"]:
        score += 3
        reasons.append("5M below EMA9")

    if b["rsi"] < 48:
        score += 4
        reasons.append("5M RSI")

    if 28 <= b["rsi"] <= 48:
        score += 3
        reasons.append("5M RSI optimal")

    if b["macd_hist"] < 0:
        score += 5
        reasons.append("5M MACD")

    if b["macd_hist"] < df5["macd_hist"].iloc[-2]:
        score += 3
        reasons.append("MACD accelerating down")

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    if b["volume_ratio"] >= 1.30:
        score += 6
        reasons.append("Volume expansion")

    elif b["volume_ratio"] >= 1.10:
        score += 3
        reasons.append("Volume improving")

    # --------------------------------------------------------
    # BREAKDOWN
    # --------------------------------------------------------

    if b["close"] < b["low20"]:
        score += 8
        reasons.append("20 candle breakdown")

    elif b["low"] < b["low20"]:
        score += 4
        reasons.append("Breakdown attempt")

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    if -2.5 <= b["roc5"] <= -0.15:
        score += 4
        reasons.append("Early negative ROC")

    if b["roc10"] < 0:
        score += 3
        reasons.append("ROC10 negative")

    # --------------------------------------------------------
    # REVERSAL
    # --------------------------------------------------------

    if bearish_reversal(df5):
        score += 6
        reasons.append("Bearish reversal")

    # --------------------------------------------------------
    # 1M
    # --------------------------------------------------------

    if a["ema9"] < a["ema21"]:
        score += 4
        reasons.append("1M momentum")

    if a["macd_hist"] < 0:
        score += 3
        reasons.append("1M MACD")

    # --------------------------------------------------------
    # AVOID OVEREXTENSION
    # --------------------------------------------------------

    if b["rsi"] < 22:
        score -= 10
        reasons.append("Oversold penalty")

    if c["rsi"] < 22:
        score -= 8
        reasons.append("15M oversold")

    return score, reasons


# ============================================================
# FUNDING
# ============================================================

def get_funding(symbol):

    try:

        data = exchange.fetch_funding_rate(symbol)

        rate = safe_float(
            data.get("fundingRate")
        )

        return rate

    except Exception:

        return 0.0


# ============================================================
# PRE-SCORE
# ============================================================

def preliminary_score(candidate):

    ticker = candidate["ticker"]

    score = 0

    pct = ticker["percentage"]

    volume = ticker["quoteVolume"]

    sources = candidate["sources"]

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

    t = cooldowns.get(symbol)

    if not t:
        return False

    return time.time() < t


def set_cooldown(symbol):

    cooldowns[symbol] = (
        time.time()
        +
        COOLDOWN_MINUTES * 60
    )


# ============================================================
# POSITION COUNT
# ============================================================

def current_position_count():

    with state_lock:
        return len(positions)


# ============================================================
# POSITION SIDE COUNT
# ============================================================

def side_count(side):

    with state_lock:

        return sum(
            1
            for p in positions.values()
            if p["side"] == side
        )


# ============================================================
# LEVERAGE CALCULATION
# ============================================================

def choose_leverage(score, atr_percent):

    leverage = 3

    if score >= 88:
        leverage = 5

    elif score >= 82:
        leverage = 4

    elif score >= 76:
        leverage = 3

    else:
        leverage = 2

    # Volatilite yükseldikçe leverage düşür
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
# CORRELATION / DUPLICATE FILTER
# ============================================================

def can_open_position(symbol, side):

    with state_lock:

        if symbol in positions:
            return False

        if len(positions) >= MAX_POSITIONS:
            return False

        if side_count(side) >= MAX_CORRELATED_SIDE:
            return False

    return True


# ============================================================
# QUANTITY
# ============================================================

def calculate_quantity(symbol, price, leverage):

    notional = (
        MARGIN_PER_POSITION
        *
        leverage
    )

    raw_qty = notional / price

    try:

        qty = float(
            exchange.amount_to_precision(
                symbol,
                raw_qty
            )
        )

        return qty

    except Exception:

        return raw_qty


# ============================================================
# DRY RUN ENTRY
# ============================================================

def dry_run_open(
    symbol,
    side,
    price,
    score,
    reasons,
    atr_value,
    leverage
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

    # İlk hard stop
    if side == "LONG":

        stop_price = (
            price
            -
            atr_value * HARD_STOP_ATR
        )

    else:

        stop_price = (
            price
            +
            atr_value * HARD_STOP_ATR
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

        "opened_at": now_utc().isoformat(),
        "last_update": now_utc().isoformat(),

        "peak_roi": 0.0,
    }

    with state_lock:
        positions[symbol] = position

    stats["simulated_entries"] += 1
    stats["signals"] += 1

    logger.warning(
        "DRY RUN ENTRY | %s | %s | "
        "price=%.8f | score=%s | lev=%sx | "
        "margin=$%.2f | qty=%s",
        side,
        symbol,
        price,
        score,
        leverage,
        MARGIN_PER_POSITION,
        quantity
    )

    logger.warning(
        "ENTRY REASONS | %s",
        " | ".join(reasons[:15])
    )

    return True


# ============================================================
# LIVE ENTRY
# ============================================================

def live_open_position(
    symbol,
    side,
    price,
    score,
    reasons,
    atr_value,
    leverage
):

    if DRY_RUN:
        return dry_run_open(
            symbol,
            side,
            price,
            score,
            reasons,
            atr_value,
            leverage
        )

    try:

        # ----------------------------------------------------
        # ISOLATED
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # LEVERAGE
        # ----------------------------------------------------

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

        # Canlı mod için pozisyonu local takipte başlat
        position = {
            "symbol": symbol,
            "side": side,
            "entry": price,
            "current_price": price,
            "margin": MARGIN_PER_POSITION,
            "leverage": leverage,
            "notional": quantity * price,
            "quantity": quantity,
            "score": score,
            "entry_reason": reasons,
            "atr": atr_value,
            "highest": price,
            "lowest": price,
            "stop_price": (
                price - atr_value * HARD_STOP_ATR
                if side == "LONG"
                else price + atr_value * HARD_STOP_ATR
            ),
            "initial_stop": (
                price - atr_value * HARD_STOP_ATR
                if side == "LONG"
                else price + atr_value * HARD_STOP_ATR
            ),
            "trailing_active": False,
            "unrealized_pnl": 0,
            "unrealized_roi": 0,
            "opened_at": now_utc().isoformat(),
            "last_update": now_utc().isoformat(),
            "peak_roi": 0,
        }

        with state_lock:
            positions[symbol] = position

        return True

    except Exception as e:

        logger.error(
            "LIVE ENTRY ERROR %s: %s",
            symbol,
            e
        )

        return False


# ============================================================
# PNL CALCULATION
# ============================================================

def calculate_pnl(position, current_price):

    entry = position["entry"]
    leverage = position["leverage"]
    margin = position["margin"]

    if position["side"] == "LONG":

        price_change = (
            current_price - entry
        ) / entry

    else:

        price_change = (
            entry - current_price
        ) / entry

    roi = price_change * leverage

    pnl = margin * roi

    return pnl, roi


# ============================================================
# TRAILING STOP
# ============================================================

def update_trailing_stop(position, current_price):

    side = position["side"]
    entry = position["entry"]
    atr_value = position["atr"]

    pnl, roi = calculate_pnl(
        position,
        current_price
    )

    position["current_price"] = current_price
    position["unrealized_pnl"] = pnl
    position["unrealized_roi"] = roi

    position["peak_roi"] = max(
        position.get("peak_roi", 0),
        roi
    )

    # --------------------------------------------------------
    # LONG
    # --------------------------------------------------------

    if side == "LONG":

        position["highest"] = max(
            position["highest"],
            current_price
        )

        # trailing activation
        if roi >= MIN_PROFIT_TO_TRAIL:

            position["trailing_active"] = True

        if position["trailing_active"]:

            if roi >= TRAIL_LEVEL_3:
                multiplier = TRAIL_ATR_TIGHT

            elif roi >= TRAIL_LEVEL_2:
                multiplier = 1.20

            elif roi >= TRAIL_LEVEL_1:
                multiplier = 1.30

            else:
                multiplier = TRAIL_ATR_MULTIPLIER

            trailing_stop = (
                position["highest"]
                -
                atr_value * multiplier
            )

            # Stop sadece yukarı hareket edebilir
            position["stop_price"] = max(
                position["stop_price"],
                trailing_stop
            )

    # --------------------------------------------------------
    # SHORT
    # --------------------------------------------------------

    else:

        position["lowest"] = min(
            position["lowest"],
            current_price
        )

        if roi >= MIN_PROFIT_TO_TRAIL:

            position["trailing_active"] = True

        if position["trailing_active"]:

            if roi >= TRAIL_LEVEL_3:
                multiplier = TRAIL_ATR_TIGHT

            elif roi >= TRAIL_LEVEL_2:
                multiplier = 1.20

            elif roi >= TRAIL_LEVEL_1:
                multiplier = 1.30

            else:
                multiplier = TRAIL_ATR_MULTIPLIER

            trailing_stop = (
                position["lowest"]
                +
                atr_value * multiplier
            )

            # Stop sadece aşağı hareket edebilir
            position["stop_price"] = min(
                position["stop_price"],
                trailing_stop
            )

    position["last_update"] = now_utc().isoformat()


# ============================================================
# DRY RUN CLOSE
# ============================================================

def dry_run_close(symbol, reason):

    with state_lock:

        position = positions.get(symbol)

        if not position:
            return

        pnl = position["unrealized_pnl"]
        roi = position["unrealized_roi"]

        del positions[symbol]

    stats["simulated_exits"] += 1
    stats["total_realized_pnl"] += pnl

    if pnl >= 0:
        stats["wins"] += 1
    else:
        stats["losses"] += 1

    set_cooldown(symbol)

    logger.warning(
        "DRY RUN EXIT | %s | %s | "
        "PNL=$%.4f | ROI=%.2f%% | reason=%s",
        position["side"],
        symbol,
        pnl,
        roi * 100,
        reason
    )


# ============================================================
# LIVE CLOSE
# ============================================================

def live_close(symbol, reason):

    if DRY_RUN:
        dry_run_close(
            symbol,
            reason
        )
        return

    with state_lock:
        position = positions.get(symbol)

    if not position:
        return

    try:

        side = position["side"]

        order_side = (
            "sell"
            if side == "LONG"
            else "buy"
        )

        exchange.create_order(
            symbol,
            "market",
            order_side,
            position["quantity"],
            None,
            {
                "reduceOnly": True
            }
        )

        logger.warning(
            "LIVE EXIT | %s | %s | reason=%s",
            side,
            symbol,
            reason
        )

        with state_lock:
            positions.pop(symbol, None)

        set_cooldown(symbol)

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

            tickers = exchange.fetch_tickers(
                symbols
            )

            for symbol in symbols:

                ticker = tickers.get(symbol)

                if not ticker:
                    continue

                price = safe_float(
                    ticker.get("last")
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
                        position["stop_price"]
                    )

                    side = position["side"]

                    roi = (
                        position["unrealized_roi"]
                    )

                    trailing = (
                        position["trailing_active"]
                    )

                # ------------------------------------------------
                # STOP CHECK
                # ------------------------------------------------

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

                # ------------------------------------------------
                # LOG ACTIVE POSITION
                # ------------------------------------------------

                logger.info(
                    "POSITION | %s | %s | "
                    "entry=%.8f price=%.8f "
                    "ROI=%.2f%% PNL=$%.3f "
                    "stop=%.8f trail=%s",
                    side,
                    symbol,
                    position["entry"],
                    price,
                    roi * 100,
                    position["unrealized_pnl"],
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
# ANALYZE ONE SYMBOL
# ============================================================

def analyze_symbol(candidate):

    symbol = candidate["symbol"]

    if is_cooldown(symbol):
        return None

    ticker = candidate["ticker"]

    # --------------------------------------------------------
    # FUNDING
    # --------------------------------------------------------

    funding = get_funding(symbol)

    if abs(funding) >= MAX_ABS_FUNDING:

        logger.info(
            "SKIP FUNDING | %s | %.6f",
            symbol,
            funding
        )

        return None

    # --------------------------------------------------------
    # OHLCV
    # --------------------------------------------------------

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
        for x in [df1, df5, df15, df1h]
    ):
        return None

    df1 = calculate_indicators(df1)
    df5 = calculate_indicators(df5)
    df15 = calculate_indicators(df15)
    df1h = calculate_indicators(df1h)

    if any(
        x is None
        for x in [df1, df5, df15, df1h]
    ):
        return None

    # --------------------------------------------------------
    # ATR
    # --------------------------------------------------------

    price = safe_float(
        ticker["last"]
    )

    atr_value = safe_float(
        df5["atr"].iloc[-1]
    )

    if price <= 0 or atr_value <= 0:
        return None

    atr_percent = (
        atr_value / price
    ) * 100

    # Çok düşük volatilite
    if atr_percent < 0.08:
        return None

    # Aşırı volatilite
    if atr_percent > 8:
        return None

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    long_score, long_reasons = score_long(
        df1,
        df5,
        df15,
        df1h
    )

    short_score, short_reasons = score_short(
        df1,
        df5,
        df15,
        df1h
    )

    # --------------------------------------------------------
    # SELECT SIDE
    # --------------------------------------------------------

    side = None
    score = 0
    reasons = []

    if (
        long_score >= MIN_LONG_SCORE
        and
        long_score > short_score + 5
    ):

        side = "LONG"
        score = long_score
        reasons = long_reasons

    elif (
        short_score >= MIN_SHORT_SCORE
        and
        short_score > long_score + 5
    ):

        side = "SHORT"
        score = short_score
        reasons = short_reasons

    else:
        return None

    # --------------------------------------------------------
    # MOMENTUM SANITY
    # --------------------------------------------------------

    if side == "LONG":

        if ticker["percentage"] < -5:
            return None

    else:

        if ticker["percentage"] > 5:
            return None

    # --------------------------------------------------------
    # LEVERAGE
    # --------------------------------------------------------

    leverage = choose_leverage(
        score,
        atr_percent
    )

    return {
        "symbol": symbol,
        "side": side,
        "score": score,
        "price": price,
        "atr": atr_value,
        "atr_percent": atr_percent,
        "leverage": leverage,
        "funding": funding,
        "reasons": reasons,
        "long_score": long_score,
        "short_score": short_score,
        "ticker_percentage": ticker["percentage"],
        "volume": ticker["quoteVolume"],
        "sources": candidate["sources"],
    }


# ============================================================
# FIND BEST OPPORTUNITY
# ============================================================

def find_best_signal(candidates):

    ranked = []

    for symbol, candidate in candidates.items():

        candidate["pre_score"] = (
            preliminary_score(candidate)
        )

        ranked.append(candidate)

    # Öncelikle piyasada gerçekten hareket edenler
    ranked.sort(
        key=lambda x: x["pre_score"],
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

                results.append(result)

                logger.info(
                    "SIGNAL CANDIDATE | %s | %s | "
                    "score=%s | 24h=%.2f%% | ATR=%.2f%%",
                    result["side"],
                    result["symbol"],
                    result["score"],
                    result["ticker_percentage"],
                    result["atr_percent"]
                )

        except Exception as e:

            logger.error(
                "Analyze error %s: %s",
                candidate["symbol"],
                e
            )

    if not results:
        return []

    results.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    return results


# ============================================================
# ENTRY VALIDATION
# ============================================================

def final_entry_validation(signal):

    symbol = signal["symbol"]
    side = signal["side"]

    if not can_open_position(
        symbol,
        side
    ):
        return False

    # --------------------------------------------------------
    # Minimum score
    # --------------------------------------------------------

    if signal["score"] < EARLY_ENTRY_SCORE:
        return False

    # --------------------------------------------------------
    # Funding
    # --------------------------------------------------------

    if abs(signal["funding"]) >= MAX_ABS_FUNDING:
        return False

    # --------------------------------------------------------
    # ATR
    # --------------------------------------------------------

    if not (
        0.08
        <=
        signal["atr_percent"]
        <=
        8
    ):
        return False

    # --------------------------------------------------------
    # Fresh price
    # --------------------------------------------------------

    try:

        ticker = exchange.fetch_ticker(
            symbol
        )

        fresh_price = safe_float(
            ticker.get("last")
        )

        if fresh_price <= 0:
            return False

        # Çok hızlı hareket etmişse
        # market order ile kovalamıyoruz.
        original = signal["price"]

        move = abs(
            fresh_price - original
        ) / original

        if move > 0.012:
            logger.info(
                "ENTRY SKIP | %s | "
                "price moved %.2f%% before entry",
                symbol,
                move * 100
            )

            return False

        signal["price"] = fresh_price

    except Exception:

        return False

    return True


# ============================================================
# EXECUTE SIGNAL
# ============================================================

def execute_signal(signal):

    if current_position_count() >= MAX_POSITIONS:
        return False

    if not final_entry_validation(
        signal
    ):
        return False

    logger.warning(
        "ENTRY CONFIRMED | %s | %s | "
        "score=%s | leverage=%sx",
        signal["side"],
        signal["symbol"],
        signal["score"],
        signal["leverage"]
    )

    return live_open_position(
        symbol=signal["symbol"],
        side=signal["side"],
        price=signal["price"],
        score=signal["score"],
        reasons=signal["reasons"],
        atr_value=signal["atr"],
        leverage=signal["leverage"]
    )


# ============================================================
# SCAN CYCLE
# ============================================================

def scan_cycle():

    global last_scan_time

    last_scan_time = now_utc().isoformat()

    stats["scans"] += 1

    logger.info("")
    logger.info("=" * 75)
    logger.info(
        "BOT ANALİZ BAŞLADI | %s",
        last_scan_time
    )
    logger.info("=" * 75)

    # --------------------------------------------------------
    # TICKERS
    # --------------------------------------------------------

    tickers = get_futures_tickers()

    if not tickers:

        logger.warning(
            "Ticker alınamadı."
        )

        return

    # --------------------------------------------------------
    # RANK LISTS
    # --------------------------------------------------------

    gainers, losers, volumes = (
        build_rank_lists(tickers)
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

    # --------------------------------------------------------
    # CANDIDATES
    # --------------------------------------------------------

    candidates = build_candidate_pool(
        gainers,
        losers,
        volumes
    )

    logger.info(
        "Benzersiz aday: %s",
        len(candidates)
    )

    # --------------------------------------------------------
    # MAX POSITION
    # --------------------------------------------------------

    if current_position_count() >= MAX_POSITIONS:

        logger.info(
            "2 pozisyon zaten açık. "
            "Yeni işlem aranmayacak."
        )

        return

    # --------------------------------------------------------
    # FIND SIGNALS
    # --------------------------------------------------------

    signals = find_best_signal(
        candidates
    )

    if not signals:

        logger.info(
            "Uygun sinyal bulunamadı."
        )

        return

    # --------------------------------------------------------
    # BEST SIGNALS
    # --------------------------------------------------------

    logger.info(
        "Toplam sinyal: %s",
        len(signals)
    )

    for signal in signals[:5]:

        logger.info(
            "TOP SIGNAL | %s | %s | "
            "score=%s | 24h=%.2f%% | "
            "ATR=%.2f%% | sources=%s",
            signal["side"],
            signal["symbol"],
            signal["score"],
            signal["ticker_percentage"],
            signal["atr_percent"],
            signal["sources"]
        )

    # --------------------------------------------------------
    # OPEN UP TO 2
    # --------------------------------------------------------

    for signal in signals:

        if current_position_count() >= MAX_POSITIONS:
            break

        # Aynı anda iki ters sinyal arasında
        # aşırı hızlı işlem açma
        if execute_signal(signal):

            time.sleep(0.5)


# ============================================================
# BOT MAIN LOOP
# ============================================================

def bot_loop():

    logger.warning("")
    logger.warning("=" * 75)
    logger.warning(
        "EARLY MOMENTUM FUTURES BOT"
    )
    logger.warning("=" * 75)

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
        "Gainers / Losers / Volume = %s / %s / %s",
        LIST_LIMIT,
        LIST_LIMIT,
        LIST_LIMIT
    )

    logger.warning("=" * 75)

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

        elapsed = time.time() - started

        sleep_for = max(
            1,
            SCAN_INTERVAL - elapsed
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
        "bot": "EARLY_MOMENTUM_FUTURES_BOT",
        "status": "running",
        "dry_run": DRY_RUN,
        "positions": len(positions),
        "max_positions": MAX_POSITIONS,
        "margin_per_position": MARGIN_PER_POSITION,
        "max_leverage": MAX_LEVERAGE,
        "last_scan": last_scan_time,
        "started_at": bot_started_at,
    })


@app.route("/status")
def status():

    with state_lock:

        position_data = {}

        for symbol, position in positions.items():

            position_data[symbol] = {
                "side": position["side"],
                "entry": position["entry"],
                "current": position["current_price"],
                "score": position["score"],
                "leverage": position["leverage"],
                "margin": position["margin"],
                "notional": position["notional"],
                "pnl": position["unrealized_pnl"],
                "roi": position["unrealized_roi"] * 100,
                "stop": position["stop_price"],
                "trailing": position["trailing_active"],
                "peak_roi": position["peak_roi"] * 100,
                "opened_at": position["opened_at"],
            }

    return jsonify({
        "dry_run": DRY_RUN,
        "positions": position_data,
        "stats": stats,
        "last_scan": last_scan_time,
    })


@app.route("/health")
def health():

    return jsonify({
        "ok": True,
        "timestamp": now_utc().isoformat()
    })


# ============================================================
# START
# ============================================================

def start_background_bot():

    monitor = threading.Thread(
        target=position_monitor,
        daemon=True
    )

    monitor.start()

    bot = threading.Thread(
        target=bot_loop,
        daemon=True
    )

    bot.start()


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