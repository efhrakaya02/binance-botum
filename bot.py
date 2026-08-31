import os
import time
import math
import gc
import logging
import threading
from datetime import datetime, timezone

import ccxt
import numpy as np
import pandas as pd
from flask import Flask, jsonify


# ============================================================
# BINANCE FUTURES MOMENTUM BOT V2
# ============================================================
#
# ANA MANTIK
# ------------------------------------------------------------
# - Cron YOK
# - Bot kendi başına sürekli çalışır
# - Binance Futures
# - ISOLATED margin
# - Gainers Top 25 + Losers Top 25
# - 5m / 15m / 1h / 4h analiz
# - Trend + momentum + hacim + fiyat yapısı
# - Entry timing confirmation
# - Scalp: kısa/dinamik TP
# - Opportunity: daha geniş hedef
# - Dinamik trailing
# - Kâr kilitleme
# - Momentum/trend bozulmasında erken çıkış
# - Başlangıç SL <= TP'nin %60'ı
# - Maksimum 1 Scalp + 1 Opportunity
# - Maksimum toplam 2 pozisyon
# - Ayrı pozisyon monitor thread
#
# ============================================================


# ============================================================
# ENV / CONFIG
# ============================================================

API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")

TESTNET = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

PORT = int(os.getenv("PORT", "8080"))

# ------------------------------------------------------------
# Pozisyon
# ------------------------------------------------------------

SCALP_ENABLED = True
OPPORTUNITY_ENABLED = True

SCALP_MARGIN = 10.0
OPPORTUNITY_MARGIN = 15.0

MAX_SCALP_POSITIONS = 1
MAX_OPPORTUNITY_POSITIONS = 1
MAX_TOTAL_POSITIONS = 2

MIN_LEVERAGE = 3
MAX_LEVERAGE = 5

# ------------------------------------------------------------
# Skorlar
# ------------------------------------------------------------

SCALP_MIN_SCORE = 68
OPPORTUNITY_MIN_SCORE = 74

# ------------------------------------------------------------
# Risk
# ------------------------------------------------------------

MAX_LOSS_TO_TARGET_RATIO = 0.60

# Maksimum başlangıç SL
MIN_SCALP_TP_PCT = 0.80
MAX_SCALP_TP_PCT = 4.50

MIN_OPP_TP_PCT = 2.00
MAX_OPP_TP_PCT = 10.00

# ------------------------------------------------------------
# Monitor
# ------------------------------------------------------------

POSITION_MONITOR_INTERVAL = 1.0
ANALYSIS_INTERVAL = 300
NO_SIGNAL_INTERVAL = 60

# ------------------------------------------------------------
# Aday sayısı / Likidite / BTC rejimi (YENİ)
# ------------------------------------------------------------

TOP_N_CANDIDATES = 5
MIN_QUOTE_VOLUME_USDT = 2_000_000
BTC_SYMBOL = "BTC/USDT"
BTC_REGIME_MIN_STRENGTH = 60

# ------------------------------------------------------------
# Cooldown
# ------------------------------------------------------------

COOLDOWN_HOURS = 4
COOLDOWN_MS = COOLDOWN_HOURS * 60 * 60 * 1000

# ------------------------------------------------------------
# Funding
# ------------------------------------------------------------

FUNDING_SKIP_THRESHOLD = 0.0015

# ------------------------------------------------------------
# Binance
# ------------------------------------------------------------

OHLCV_LIMIT = 250

# ------------------------------------------------------------
# Technical thresholds
# ------------------------------------------------------------

ADX_STRONG = 25
ADX_VERY_STRONG = 35

RSI_LONG_MIN = 52
RSI_LONG_MAX = 72

RSI_SHORT_MIN = 28
RSI_SHORT_MAX = 48

VOLUME_CONFIRMATION = 1.15

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("MOMENTUM_BOT")


# ============================================================
# GLOBAL STATE
# ============================================================

exchange = None

state_lock = threading.RLock()

running = True

last_analysis_time = 0
last_successful_analysis = None

cooldowns = {}

signal_cache = {}

bot_stats = {
    "analysis_count": 0,
    "signals_found": 0,
    "orders": 0,
    "closed_positions": 0,
    "errors": 0,
}

local_positions = {}

candidate_lock = threading.Lock()


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "bot": "Binance Futures Momentum Bot V2",
        "dry_run": DRY_RUN,
        "testnet": TESTNET,
        "positions": get_local_positions(),
        "stats": bot_stats,
        "last_analysis": last_successful_analysis
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "running": running,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })


@app.route("/status")
def status():
    return jsonify({
        "dry_run": DRY_RUN,
        "positions": get_local_positions(),
        "stats": bot_stats,
        "cooldowns": cooldowns
    })


# ============================================================
# EXCHANGE
# ============================================================

def create_exchange():
    global exchange

    if not API_KEY or not API_SECRET:
        raise RuntimeError(
            "BINANCE_API_KEY / BINANCE_API_SECRET tanımlı değil."
        )

    exchange = ccxt.binance({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "adjustForTimeDifference": True,
        },
        "timeout": 20000,
    })

    if TESTNET:
        exchange.set_sandbox_mode(True)

    exchange.load_markets()

    logger.info(
        "Binance bağlantısı hazır | TESTNET=%s | DRY_RUN=%s",
        TESTNET,
        DRY_RUN
    )

    return exchange


# ============================================================
# SAFE API
# ============================================================

def safe_call(fn, *args, retries=3, delay=1, **kwargs):
    last_error = None

    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)

        except Exception as e:
            last_error = e
            bot_stats["errors"] += 1

            logger.warning(
                "API hata (%s/%s): %s",
                attempt + 1,
                retries,
                e
            )

            time.sleep(delay * (attempt + 1))

    raise last_error


# ============================================================
# UTILS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def clamp(value, low, high):
    return max(low, min(high, value))


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default

        result = float(value)

        if not math.isfinite(result):
            return default

        return result

    except Exception:
        return default


def normalize_symbol(symbol):
    if not symbol:
        return None

    return symbol.replace("/", "").replace(":USDT", "").upper()


def symbol_is_valid(symbol):
    if not symbol:
        return False

    s = symbol.upper()

    if "/USDT" not in s:
        return False

    blacklist = [
        "UP/",
        "DOWN/",
        "BEAR/",
        "BULL/",
        "_",
        "BID/",
        "ASK/",
    ]

    return not any(x in s for x in blacklist)


def get_price_precision(symbol):
    try:
        market = exchange.market(symbol)
        return market["precision"]["price"]
    except Exception:
        return 8


def get_amount_precision(symbol):
    try:
        market = exchange.market(symbol)
        return market["precision"]["amount"]
    except Exception:
        return 6


def format_price(symbol, price):
    try:
        return exchange.price_to_precision(symbol, price)
    except Exception:
        precision = get_price_precision(symbol)
        return f"{price:.{precision}f}"


def format_amount(symbol, amount):
    try:
        return exchange.amount_to_precision(symbol, amount)
    except Exception:
        precision = get_amount_precision(symbol)
        return f"{amount:.{precision}f}"


# ============================================================
# INDICATORS
# ============================================================

def ema(series, period):
    return series.ewm(
        span=period,
        adjust=False,
        min_periods=period
    ).mean()


def rsi(series, period=14):
    delta = series.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period
    ).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)

    result = 100 - (100 / (1 + rs))

    return result.fillna(50)


def atr(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()

    tr = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    return tr.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period
    ).mean()


def macd(series):
    fast = ema(series, 12)
    slow = ema(series, 26)

    macd_line = fast - slow
    signal = ema(macd_line, 9)

    histogram = macd_line - signal

    return macd_line, signal, histogram


def adx(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    plus_dm = high.diff()
    minus_dm = -low.diff()

    plus_dm = plus_dm.where(
        (plus_dm > minus_dm) & (plus_dm > 0),
        0
    )

    minus_dm = minus_dm.where(
        (minus_dm > plus_dm) & (minus_dm > 0),
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
        adjust=False,
        min_periods=period
    ).mean()

    plus_di = (
        100 *
        plus_dm.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period
        ).mean() /
        atr_val.replace(0, np.nan)
    )

    minus_di = (
        100 *
        minus_dm.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period
        ).mean() /
        atr_val.replace(0, np.nan)
    )

    dx = (
        100 *
        (plus_di - minus_di).abs() /
        (plus_di + minus_di).replace(0, np.nan)
    )

    adx_val = dx.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period
    ).mean().fillna(0)

    return adx_val, plus_di.fillna(0), minus_di.fillna(0)


def bollinger(series, period=20, std_mult=2):
    middle = series.rolling(period).mean()
    std = series.rolling(period).std()

    upper = middle + std_mult * std
    lower = middle - std_mult * std

    return middle, upper, lower


def obv(df):
    direction = np.sign(df["close"].diff()).fillna(0)

    return (
        direction *
        df["volume"]
    ).cumsum()


def roc(series, period=10):
    return (
        series.pct_change(periods=period) * 100
    )


# ============================================================
# DATA
# ============================================================

def fetch_ohlcv(symbol, timeframe, limit=OHLCV_LIMIT):
    try:
        data = safe_call(
            exchange.fetch_ohlcv,
            symbol,
            timeframe,
            None,
            limit
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

        df.dropna(inplace=True)

        if len(df) < 100:
            return None

        return df

    except Exception as e:
        logger.warning(
            "%s %s OHLCV alınamadı: %s",
            symbol,
            timeframe,
            e
        )

        return None


def enrich_dataframe(df):
    df = df.copy()

    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["ema50"] = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)

    df["rsi"] = rsi(df["close"])

    df["atr"] = atr(df)

    (
        df["macd"],
        df["macd_signal"],
        df["macd_hist"]
    ) = macd(df["close"])

    df["adx"], df["plus_di"], df["minus_di"] = adx(df)

    (
        df["bb_mid"],
        df["bb_upper"],
        df["bb_lower"]
    ) = bollinger(df["close"])

    df["obv"] = obv(df)

    df["volume_ma20"] = (
        df["volume"]
        .rolling(20)
        .mean()
    )

    df["volume_ratio"] = (
        df["volume"] /
        df["volume_ma20"].replace(0, np.nan)
    )

    df["roc"] = roc(df["close"], 10)

    df["recent_high"] = (
        df["high"]
        .rolling(20)
        .max()
    )

    df["recent_low"] = (
        df["low"]
        .rolling(20)
        .min()
    )

    df["atr_pct"] = (
        df["atr"] /
        df["close"] *
        100
    )

    return df


# ============================================================
# GAINERS / LOSERS  (+ LIKIDITE FILTRESI)
# ============================================================

def get_top_movers():
    tickers = safe_call(
        exchange.fetch_tickers
    )

    rows = []

    for symbol, ticker in tickers.items():

        if not symbol_is_valid(symbol):
            continue

        try:
            last = safe_float(
                ticker.get("last")
            )

            percentage = safe_float(
                ticker.get("percentage")
            )

            quote_volume = safe_float(
                ticker.get("quoteVolume")
            )

            if last <= 0:
                continue

            rows.append({
                "symbol": symbol,
                "percentage": percentage,
                "quoteVolume": quote_volume
            })

        except Exception:
            continue

    if not rows:
        return [], [], []

    df = pd.DataFrame(rows)

    # Likidite filtresi (eski: sadece > 0, yeni: minimum eşik)
    df = df[df["quoteVolume"] >= MIN_QUOTE_VOLUME_USDT]

    if df.empty:
        logger.warning(
            "[LIKIDITE] MIN_QUOTE_VOLUME_USDT=%s eşiğini geçen coin yok.",
            MIN_QUOTE_VOLUME_USDT
        )
        return [], [], []

    gainers = (
        df.sort_values("percentage", ascending=False)
        .head(25)["symbol"].tolist()
    )

    losers = (
        df.sort_values("percentage", ascending=True)
        .head(25)["symbol"].tolist()
    )

    # 24s hacim lideri top 25 (YENİ)
    # Gainers/losers %değişime göre seçilir, bu yüzden yüksek
    # likiditeli ama o an aşırı %hareket göstermeyen (dolayısıyla
    # gainers/losers listesine girmeyen) coinleri kaçırabilir.
    # Hacme göre ayrı bir top-25 bu boşluğu kapatır.
    volume_leaders = (
        df.sort_values("quoteVolume", ascending=False)
        .head(25)["symbol"].tolist()
    )

    return gainers, losers, volume_leaders


# ============================================================
# FUNDING
# ============================================================

def get_funding(symbol):
    try:
        funding = safe_call(
            exchange.fetch_funding_rate,
            symbol
        )

        return safe_float(
            funding.get("fundingRate")
        )

    except Exception:
        return 0.0


# ============================================================
# TAKER BUY/SELL RATIO
# ============================================================
# Hacim artışının gerçekten alıcı mı satıcı mı kaynaklı olduğunu
# ayırt etmek için Binance Futures'ın taker long/short ratio
# verisini kullanıyoruz. Bu veri her coin için ekstra bir API
# çağrısı gerektirdiğinden yalnızca ön elemeyi geçen adaylarda
# (top-5 aşamasında) kullanılması önerilir.
#
# NOT: Bu endpoint ccxt'de farklı sürümlerde farklı implicit
# metot adlarıyla bulunabiliyor (bazı sürümlerde hiç yok). Bu
# yüzden birkaç aday metot adını sırayla deneyip hangisi
# kullanılabilir buluyoruz; hiçbiri yoksa BİR KEZ uyarı basıp
# bu veriyi sessizce atlıyoruz (skor/işlem akışını etkilemez).

TAKER_RATIO_METHOD_CANDIDATES = [
    "fapiDataGetTakerlongshortRatio",
    "fapiDataGetDeliveryPriceTakerlongshortRatio",
    "fapiPublicGetFuturesDataTakerlongshortRatio",
]

_taker_ratio_method_name = None
_taker_ratio_checked = False


def _resolve_taker_ratio_method():
    global _taker_ratio_method_name, _taker_ratio_checked

    if _taker_ratio_checked:
        return _taker_ratio_method_name

    _taker_ratio_checked = True

    for name in TAKER_RATIO_METHOD_CANDIDATES:
        if hasattr(exchange, name):
            _taker_ratio_method_name = name
            logger.info("[TAKER RATIO] Kullanılacak ccxt metodu bulundu: %s", name)
            return name

    logger.warning(
        "[TAKER RATIO] Bu ccxt sürümünde uygun endpoint bulunamadı, "
        "taker ratio filtresi bu çalıştırmada pas geçilecek (diğer analizleri etkilemez)."
    )
    return None


def get_taker_ratio(symbol, period="15m"):
    method_name = _resolve_taker_ratio_method()

    if not method_name:
        return None

    try:
        market_symbol = normalize_symbol(symbol)
        fn = getattr(exchange, method_name)

        raw = safe_call(
            fn,
            {
                "symbol": market_symbol,
                "period": period,
                "limit": 5
            }
        )

        if not raw:
            return None

        last = raw[-1]

        buy_vol = safe_float(last.get("buyVol"))
        sell_vol = safe_float(last.get("sellVol"))

        if buy_vol <= 0 and sell_vol <= 0:
            return None

        total = buy_vol + sell_vol

        if total <= 0:
            return None

        return {
            "buy_ratio": buy_vol / total,
            "sell_ratio": sell_vol / total,
        }

    except Exception as e:
        logger.info(
            "%s taker ratio alınamadı (opsiyonel veri): %s",
            symbol,
            e
        )
        return None


# ============================================================
# BTC PIYASA REJIMI FILTRESI
# ============================================================
# Altcoinler büyük ölçüde BTC ile korelasyonlu hareket eder.
# BTC net bir trend içindeyken bu trende ters yönde açılan
# işlemler istatistiksel olarak dezavantajlıdır. Bu fonksiyon
# analysis_cycle başında BIR KEZ çağrılır ve tüm adaylara
# parametre olarak geçirilir (her coin için tekrar tekrar
# çağırmak gereksiz API yükü yaratır).

def get_btc_regime():
    try:
        df15 = fetch_ohlcv(BTC_SYMBOL, "15m", 100)
        df1h = fetch_ohlcv(BTC_SYMBOL, "1h", 100)

        if df15 is None or df1h is None:
            return {"direction": "neutral", "strength": 0}

        df15 = enrich_dataframe(df15)
        df1h = enrich_dataframe(df1h)

        t15 = timeframe_trend(df15)
        t1h = timeframe_trend(df1h)

        if (
            t15["direction"] == t1h["direction"]
            and t15["direction"] != "neutral"
        ):
            strength = (t15["strength"] + t1h["strength"]) / 2
            return {"direction": t15["direction"], "strength": strength}

        return {"direction": "neutral", "strength": 0}

    except Exception as e:
        logger.warning("BTC rejim analizi başarısız: %s", e)
        return {"direction": "neutral", "strength": 0}


# ============================================================
# ANOMALI / PUMP-DUMP FILTRESI
# ============================================================
# Gainers/losers listesi doğası gereği ani, sert hareketli
# coinleri getirir. Bunların bir kısmı düşük likiditeli
# coinlerde manipülatif (pump&dump) olabilir. Son birkaç mumda
# hacim teyidi OLMADAN aşırı sert tek yönlü hareket varsa
# bu coini eliyoruz.

def detect_anomaly(df):
    if df is None or len(df) < 10:
        return False

    last5 = df.tail(5)

    price_change_pct = (
        (last5["close"].iloc[-1] - last5["close"].iloc[0])
        / last5["close"].iloc[0] * 100
    )

    avg_volume_ratio = safe_float(
        last5["volume_ratio"].mean(),
        1
    )

    # Son 5 mumda %8'den fazla hareket ama hacim teyidi zayıfsa
    # (ör. ortalama hacim oranı 1.3'ün altındaysa) şüpheli kabul et
    if abs(price_change_pct) >= 8 and avg_volume_ratio < 1.3:
        return True

    return False


# ============================================================
# TREND ANALYSIS
# ============================================================

def timeframe_trend(df):
    if df is None or len(df) < 205:
        return {
            "direction": "neutral",
            "strength": 0
        }

    x = df.iloc[-1]

    price = safe_float(x["close"])

    e9 = safe_float(x["ema9"])
    e21 = safe_float(x["ema21"])
    e50 = safe_float(x["ema50"])
    e200 = safe_float(x["ema200"])

    adx_val = safe_float(x["adx"])
    plus_di = safe_float(x.get("plus_di"))
    minus_di = safe_float(x.get("minus_di"))

    bullish = (
        price > e21 >
        e50 >
        e200
    )

    bearish = (
        price < e21 <
        e50 <
        e200
    )

    strength = 0

    if bullish or bearish:
        strength += 40

    if adx_val >= ADX_STRONG:
        strength += 30

    if adx_val >= ADX_VERY_STRONG:
        strength += 20

    # +DI / -DI yön teyidi: trend yönü DI'lar tarafından
    # doğrulanmıyorsa (ör. bullish görünüyor ama -DI > +DI ise)
    # güç puanını cezalandır.
    if bullish and plus_di <= minus_di:
        strength -= 20

    if bearish and minus_di <= plus_di:
        strength -= 20

    strength = clamp(strength, 0, 100)

    if bullish:
        return {
            "direction": "long",
            "strength": strength
        }

    if bearish:
        return {
            "direction": "short",
            "strength": strength
        }

    # Daha yumuşak trend
    if price > e50 and e9 > e21:
        soft_strength = 35
        if plus_di <= minus_di:
            soft_strength -= 15
        return {
            "direction": "long",
            "strength": clamp(soft_strength, 0, 100)
        }

    if price < e50 and e9 < e21:
        soft_strength = 35
        if minus_di <= plus_di:
            soft_strength -= 15
        return {
            "direction": "short",
            "strength": clamp(soft_strength, 0, 100)
        }

    return {
        "direction": "neutral",
        "strength": 0
    }


# ============================================================
# MOMENTUM
# ============================================================

def momentum_analysis(df):
    if df is None or len(df) < 50:
        return {
            "direction": "neutral",
            "strength": 0
        }

    x = df.iloc[-1]
    p = df.iloc[-2]

    rsi_val = safe_float(x["rsi"])
    macd_hist = safe_float(x["macd_hist"])
    prev_hist = safe_float(p["macd_hist"])

    roc_val = safe_float(x["roc"])
    volume_ratio = safe_float(
        x["volume_ratio"],
        1
    )

    long_points = 0
    short_points = 0

    if 52 <= rsi_val <= 70:
        long_points += 20

    if 30 <= rsi_val <= 48:
        short_points += 20

    if macd_hist > 0:
        long_points += 20

    if macd_hist < 0:
        short_points += 20

    if macd_hist > prev_hist:
        long_points += 15

    if macd_hist < prev_hist:
        short_points += 15

    if roc_val > 0:
        long_points += 15

    if roc_val < 0:
        short_points += 15

    if volume_ratio >= VOLUME_CONFIRMATION:
        if long_points >= short_points:
            long_points += 15
        else:
            short_points += 15

    if long_points > short_points:
        return {
            "direction": "long",
            "strength": clamp(long_points, 0, 100)
        }

    if short_points > long_points:
        return {
            "direction": "short",
            "strength": clamp(short_points, 0, 100)
        }

    return {
        "direction": "neutral",
        "strength": 0
    }


# ============================================================
# PRICE STRUCTURE
# ============================================================

def structure_analysis(df):
    if df is None or len(df) < 30:
        return {
            "direction": "neutral",
            "strength": 0,
            "state": "unknown"
        }

    x = df.iloc[-1]

    close = safe_float(x["close"])
    high20 = safe_float(x["recent_high"])
    low20 = safe_float(x["recent_low"])

    atr_val = safe_float(x["atr"])

    if atr_val <= 0:
        return {
            "direction": "neutral",
            "strength": 0,
            "state": "unknown"
        }

    previous_high = safe_float(df.iloc[-2]["recent_high"])
    previous_low = safe_float(df.iloc[-2]["recent_low"])

    if close > previous_high:
        return {"direction": "long", "strength": 80, "state": "breakout"}

    if close < previous_low:
        return {"direction": "short", "strength": 80, "state": "breakdown"}

    distance_high = high20 - close
    distance_low = close - low20

    if distance_high <= atr_val * 1.2:
        return {"direction": "long", "strength": 60, "state": "pullback_long"}

    if distance_low <= atr_val * 1.2:
        return {"direction": "short", "strength": 60, "state": "pullback_short"}

    return {"direction": "neutral", "strength": 25, "state": "range"}


# ============================================================
# ENTRY TIMING
# ============================================================

def entry_timing(df, direction):
    if df is None or len(df) < 30:
        return {"confirmed": False, "score": 0, "reason": "insufficient_data"}

    x = df.iloc[-1]
    p = df.iloc[-2]

    close = safe_float(x["close"])
    ema9 = safe_float(x["ema9"])
    ema21 = safe_float(x["ema21"])

    prev_close = safe_float(p["close"])
    prev_ema9 = safe_float(p["ema9"])

    volume_ratio = safe_float(x["volume_ratio"], 1)

    macd_hist = safe_float(x["macd_hist"])
    prev_macd_hist = safe_float(p["macd_hist"])

    score = 0
    reasons = []

    if direction == "long":

        if close > ema21:
            score += 20
            reasons.append("price_above_ema21")

        if ema9 > ema21:
            score += 20
            reasons.append("ema_alignment")

        if close > prev_close:
            score += 15
            reasons.append("price_acceleration")

        if macd_hist > prev_macd_hist:
            score += 20
            reasons.append("macd_acceleration")

        if volume_ratio >= 1.15:
            score += 15
            reasons.append("volume_confirmation")

        if prev_ema9 <= ema21 and ema9 > ema21:
            score += 10
            reasons.append("ema_cross")

    else:

        if close < ema21:
            score += 20
            reasons.append("price_below_ema21")

        if ema9 < ema21:
            score += 20
            reasons.append("ema_alignment")

        if close < prev_close:
            score += 15
            reasons.append("price_acceleration")

        if macd_hist < prev_macd_hist:
            score += 20
            reasons.append("macd_acceleration")

        if volume_ratio >= 1.15:
            score += 15
            reasons.append("volume_confirmation")

        if prev_ema9 >= ema21 and ema9 < ema21:
            score += 10
            reasons.append("ema_cross")

    return {
        "confirmed": score >= 60,
        "score": score,
        "reason": ",".join(reasons)
    }


# ============================================================
# OVEREXTENSION FILTER
# ============================================================

def is_overextended(df, direction):
    if df is None or len(df) < 30:
        return True

    x = df.iloc[-1]

    close = safe_float(x["close"])
    atr_val = safe_float(x["atr"])

    ema21 = safe_float(x["ema21"])
    ema50 = safe_float(x["ema50"])

    if close <= 0 or atr_val <= 0:
        return True

    if direction == "long":
        distance = close - ema21
        if distance > atr_val * 2.4:
            return True
        distance50 = close - ema50
        if distance50 > atr_val * 4.0:
            return True
    else:
        distance = ema21 - close
        if distance > atr_val * 2.4:
            return True
        distance50 = ema50 - close
        if distance50 > atr_val * 4.0:
            return True

    return False


# ============================================================
# FORMASYON TESPİTİ (OBO / TOBO / BAYRAK)
# ============================================================
# Swing high/low (yerel tepe/dip) tespitine dayalı basit ama
# etkili bir formasyon tanıma katmanı. Kesin/akademik formasyon
# tanıma yerine, "olası çıkış/iniş sinyali" için ek bir onay/bonus
# puanı üretmeyi amaçlar — tek başına işlem açma kriteri değildir.

def find_swing_points(df, window=3):
    highs = df["high"].values
    lows = df["low"].values
    n = len(df)

    swing_highs = []
    swing_lows = []

    for i in range(window, n - window):
        if highs[i] == max(highs[i - window:i + window + 1]):
            swing_highs.append((i, highs[i]))
        if lows[i] == min(lows[i - window:i + window + 1]):
            swing_lows.append((i, lows[i]))

    return swing_highs, swing_lows


def detect_head_shoulders(df):
    """OBO (Omuz-Baş-Omuz -> short sinyali) ve
    TOBO (Ters Omuz-Baş-Omuz -> long sinyali) tespiti."""
    if df is None or len(df) < 40:
        return None

    swing_highs, swing_lows = find_swing_points(df, window=3)

    if len(swing_highs) >= 3:
        (i1, h1), (i2, h2), (i3, h3) = swing_highs[-3:]

        if h2 > h1 and h2 > h3:
            shoulder_diff = abs(h1 - h3) / h2 if h2 else 1

            if shoulder_diff < 0.035 and h2 > h1 * 1.008 and h2 > h3 * 1.008:
                neckline = min(
                    df["low"].iloc[i1:i2].min(),
                    df["low"].iloc[i2:i3].min()
                )
                last_close = df["close"].iloc[-1]

                if last_close < neckline:
                    return {"pattern": "OBO", "direction": "short", "confidence": 75}

                return {"pattern": "OBO", "direction": "short", "confidence": 40}

    if len(swing_lows) >= 3:
        (i1, l1), (i2, l2), (i3, l3) = swing_lows[-3:]

        if l2 < l1 and l2 < l3:
            shoulder_diff = abs(l1 - l3) / l2 if l2 else 1

            if shoulder_diff < 0.035 and l2 < l1 * 0.992 and l2 < l3 * 0.992:
                neckline = max(
                    df["high"].iloc[i1:i2].max(),
                    df["high"].iloc[i2:i3].max()
                )
                last_close = df["close"].iloc[-1]

                if last_close > neckline:
                    return {"pattern": "TOBO", "direction": "long", "confidence": 75}

                return {"pattern": "TOBO", "direction": "long", "confidence": 40}

    return None


def detect_flag(df):
    """BAYRAK: güçlü tek yönlü hareket (direk) + dar, ters/yatay
    eğimli konsolidasyon (bayrak)."""
    if df is None or len(df) < 30:
        return None

    window = df.tail(20)

    pole = window.iloc[:8]
    flag_part = window.iloc[8:18]

    if pole["close"].iloc[0] == 0:
        return None

    pole_change = (
        (pole["close"].iloc[-1] - pole["close"].iloc[0])
        / pole["close"].iloc[0] * 100
    )

    if flag_part["close"].mean() == 0:
        return None

    flag_range = (
        (flag_part["high"].max() - flag_part["low"].min())
        / flag_part["close"].mean() * 100
    )

    flag_slope = (
        (flag_part["close"].iloc[-1] - flag_part["close"].iloc[0])
        / flag_part["close"].iloc[0] * 100
    )

    if abs(pole_change) >= 4 and flag_range < abs(pole_change) * 0.65:

        if pole_change > 0 and flag_slope <= 1.2:
            return {"pattern": "BAYRAK", "direction": "long", "confidence": 60}

        if pole_change < 0 and flag_slope >= -1.2:
            return {"pattern": "BAYRAK", "direction": "short", "confidence": 60}

    return None


def detect_chart_patterns(df):
    hs = detect_head_shoulders(df)
    flag = detect_flag(df)

    candidates = [p for p in [hs, flag] if p]

    if not candidates:
        return None

    return max(candidates, key=lambda p: p["confidence"])


# ============================================================
# MULTI TIMEFRAME SCORE
# ============================================================

def analyze_coin(symbol, mode, btc_regime=None):
    timeframes = ["5m", "15m", "1h", "4h"]

    data = {}

    for tf in timeframes:
        df = fetch_ohlcv(symbol, tf)

        if df is None:
            return None

        data[tf] = enrich_dataframe(df)

    # --------------------------------------------------------
    # Anomali (pump/dump) filtresi — 15m üzerinde kontrol
    # --------------------------------------------------------

    if detect_anomaly(data["15m"]):
        return None

    trend = {}
    momentum = {}

    for tf in timeframes:
        trend[tf] = timeframe_trend(data[tf])
        momentum[tf] = momentum_analysis(data[tf])

    # --------------------------------------------------------
    # Direction voting
    # --------------------------------------------------------

    long_votes = 0
    short_votes = 0

    for tf in timeframes:

        if trend[tf]["direction"] == "long":
            long_votes += 1
        elif trend[tf]["direction"] == "short":
            short_votes += 1

        if momentum[tf]["direction"] == "long":
            long_votes += 0.5
        elif momentum[tf]["direction"] == "short":
            short_votes += 0.5

    if long_votes > short_votes:
        direction = "long"
    elif short_votes > long_votes:
        direction = "short"
    else:
        return None

    # --------------------------------------------------------
    # BTC piyasa rejimi filtresi (YENİ)
    # --------------------------------------------------------
    # BTC net bir trend içindeyse ve bu coin BTC'ye ters yöndeyse
    # işlemi ele. BTC_SYMBOL'ün kendisini analiz ederken bu
    # kontrolü atla (sonsuz döngü olmasın diye).

    if btc_regime and symbol != BTC_SYMBOL:
        if (
            btc_regime["direction"] != "neutral"
            and btc_regime["strength"] >= BTC_REGIME_MIN_STRENGTH
            and btc_regime["direction"] != direction
        ):
            return None

    # --------------------------------------------------------
    # Weighted trend
    # --------------------------------------------------------

    trend_score = (
        trend["5m"]["strength"] * 0.15 +
        trend["15m"]["strength"] * 0.25 +
        trend["1h"]["strength"] * 0.30 +
        trend["4h"]["strength"] * 0.30
    )

    momentum_score = (
        momentum["5m"]["strength"] * 0.25 +
        momentum["15m"]["strength"] * 0.30 +
        momentum["1h"]["strength"] * 0.30 +
        momentum["4h"]["strength"] * 0.15
    )

    # --------------------------------------------------------
    # Alignment
    # --------------------------------------------------------

    alignment = 0

    for tf in timeframes:
        if (
            trend[tf]["direction"] == direction and
            momentum[tf]["direction"] == direction
        ):
            alignment += 25

    alignment = clamp(alignment, 0, 100)

    # --------------------------------------------------------
    # Current structure
    # --------------------------------------------------------

    structure = structure_analysis(data["15m"])

    structure_score = (
        structure["strength"] if structure["direction"] == direction else 0
    )

    # --------------------------------------------------------
    # Formasyon tespiti (OBO / TOBO / BAYRAK) — YENİ
    # --------------------------------------------------------
    # 1h verisinde formasyon aranır. Tespit edilen formasyon işlem
    # yönüyle aynıysa küçük bir bonus puan eklenir; tek başına
    # işlem açtırmaz, sadece mevcut sinyali güçlendirir.

    pattern = detect_chart_patterns(data["1h"])
    pattern_label = "yok"
    pattern_score = 0

    if pattern:
        pattern_label = pattern["pattern"]
        if pattern["direction"] == direction:
            pattern_score = pattern["confidence"]

    # --------------------------------------------------------
    # Entry timing
    # --------------------------------------------------------

    timing = entry_timing(data["5m"], direction)

    # --------------------------------------------------------
    # Volume
    # --------------------------------------------------------

    current = data["15m"].iloc[-1]

    volume_ratio = safe_float(current["volume_ratio"], 1)

    volume_score = clamp((volume_ratio - 1) * 80, 0, 100)

    # --------------------------------------------------------
    # ADX
    # --------------------------------------------------------

    adx_val = safe_float(current["adx"])

    adx_score = clamp((adx_val / 45) * 100, 0, 100)

    # --------------------------------------------------------
    # Raw score
    # --------------------------------------------------------

    score = (
        trend_score * 0.25 +
        momentum_score * 0.25 +
        alignment * 0.20 +
        structure_score * 0.10 +
        timing["score"] * 0.10 +
        volume_score * 0.05 +
        adx_score * 0.05
    )

    # Formasyon bonusu: ağırlıklı skorun dışında, küçük bir ek puan
    # (maks +11.25) — mevcut kalibrasyonu bozmadan destekleyici katkı.
    score += pattern_score * 0.15

    score = round(clamp(score, 0, 100), 2)

    # --------------------------------------------------------
    # Funding
    # --------------------------------------------------------

    funding = get_funding(symbol)

    if abs(funding) >= FUNDING_SKIP_THRESHOLD:
        return None

    # --------------------------------------------------------
    # Overextension
    # --------------------------------------------------------

    if is_overextended(data["15m"], direction):
        return None

    # --------------------------------------------------------
    # Strict confirmation
    # --------------------------------------------------------

    minimum = SCALP_MIN_SCORE if mode == "scalp" else OPPORTUNITY_MIN_SCORE

    if score < minimum:
        return None

    for tf in ["1h", "4h"]:
        if (
            trend[tf]["direction"] != direction
            and trend[tf]["strength"] >= 65
        ):
            return None

    if momentum["15m"]["direction"] != direction:
        return None

    if not timing["confirmed"]:
        return None

    if mode == "opportunity":
        if alignment < 60:
            return None
        if trend["1h"]["strength"] < 48:
            return None
        if trend["4h"]["strength"] < 48:
            return None
        if momentum["1h"]["strength"] < 48:
            return None
        if adx_val < 18:
            return None

    # --------------------------------------------------------
    # Taker buy/sell ratio (YENİ) — sadece skoru geçen adaylarda
    # ekstra API çağrısı yapıyoruz, tüm coinlerde değil.
    # --------------------------------------------------------

    taker = get_taker_ratio(symbol)
    taker_note = "n/a"

    if taker:
        if direction == "long" and taker["buy_ratio"] < 0.45:
            return None
        if direction == "short" and taker["sell_ratio"] < 0.45:
            return None
        taker_note = (
            f"buy={taker['buy_ratio']:.2f} sell={taker['sell_ratio']:.2f}"
        )

    # --------------------------------------------------------
    # Dynamic TP / SL
    # --------------------------------------------------------

    price = safe_float(current["close"])

    atr_pct = safe_float(current["atr_pct"])
    atr_pct = clamp(atr_pct, 0.15, 6.0)

    if mode == "scalp":
        tp_pct = (
            atr_pct * 1.20 +
            momentum_score * 0.012 +
            adx_score * 0.006
        )
        tp_pct = clamp(tp_pct, MIN_SCALP_TP_PCT, MAX_SCALP_TP_PCT)
    else:
        tp_pct = (
            atr_pct * 2.00 +
            momentum_score * 0.020 +
            alignment * 0.010
        )
        tp_pct = clamp(tp_pct, MIN_OPP_TP_PCT, MAX_OPP_TP_PCT)

    max_sl_pct = tp_pct * MAX_LOSS_TO_TARGET_RATIO

    volatility_sl = atr_pct * (0.85 if mode == "scalp" else 1.10)

    sl_pct = min(volatility_sl, max_sl_pct)
    sl_pct = max(sl_pct, 0.35)

    if sl_pct > tp_pct * MAX_LOSS_TO_TARGET_RATIO:
        tp_pct = sl_pct / MAX_LOSS_TO_TARGET_RATIO

        if mode == "scalp":
            tp_pct = clamp(tp_pct, MIN_SCALP_TP_PCT, MAX_SCALP_TP_PCT)
        else:
            tp_pct = clamp(tp_pct, MIN_OPP_TP_PCT, MAX_OPP_TP_PCT)

        sl_pct = min(sl_pct, tp_pct * MAX_LOSS_TO_TARGET_RATIO)

    return {
        "symbol": symbol,
        "mode": mode,
        "direction": direction,
        "score": score,
        "trend_score": round(trend_score, 2),
        "momentum_score": round(momentum_score, 2),
        "alignment": alignment,
        "structure": structure["state"],
        "pattern": pattern_label,
        "timing_score": timing["score"],
        "timing_reason": timing["reason"],
        "volume_ratio": volume_ratio,
        "adx": adx_val,
        "atr_pct": atr_pct,
        "funding": funding,
        "taker_note": taker_note,
        "price": price,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "data": data,
    }


# ============================================================
# COOLDOWN
# ============================================================

def is_on_cooldown(symbol):
    t = cooldowns.get(normalize_symbol(symbol))

    if not t:
        return False

    return (now_ms() - t) < COOLDOWN_MS


def set_cooldown(symbol):
    cooldowns[normalize_symbol(symbol)] = now_ms()


# ============================================================
# POSITION STATE
# ============================================================

def get_local_positions():
    with state_lock:
        return {k: dict(v) for k, v in local_positions.items()}


def local_position_count(mode=None):
    with state_lock:
        if mode is None:
            return len(local_positions)
        return sum(1 for p in local_positions.values() if p.get("mode") == mode)


def has_local_symbol(symbol):
    normalized = normalize_symbol(symbol)
    with state_lock:
        return any(
            normalize_symbol(p["symbol"]) == normalized
            for p in local_positions.values()
        )


# ============================================================
# BINANCE POSITIONS
# ============================================================

def fetch_real_positions():
    try:
        positions = safe_call(exchange.fetch_positions)

        active = []

        for p in positions:
            contracts = safe_float(p.get("contracts"))

            if abs(contracts) <= 0:
                continue

            symbol = p.get("symbol")

            if not symbol:
                continue

            active.append({
                "symbol": symbol,
                "side": p.get("side"),
                "contracts": contracts,
                "entryPrice": safe_float(p.get("entryPrice")),
                "markPrice": safe_float(p.get("markPrice")),
                "unrealizedPnl": safe_float(p.get("unrealizedPnl")),
                "leverage": safe_float(p.get("leverage")),
            })

        return active

    except Exception as e:
        logger.warning("Gerçek pozisyonlar alınamadı: %s", e)
        return []


def sync_real_positions():
    if DRY_RUN:
        return

    real = fetch_real_positions()

    with state_lock:
        real_symbols = {normalize_symbol(p["symbol"]) for p in real}

        remove = [
            key for key, local in local_positions.items()
            if normalize_symbol(local["symbol"]) not in real_symbols
        ]

        for key in remove:
            local_positions.pop(key, None)


# ============================================================
# POSITION LIMIT
# ============================================================

def can_open_mode(mode):
    total = local_position_count()

    if total >= MAX_TOTAL_POSITIONS:
        return False

    if mode == "scalp":
        return local_position_count("scalp") < MAX_SCALP_POSITIONS

    return local_position_count("opportunity") < MAX_OPPORTUNITY_POSITIONS


# ============================================================
# LEVERAGE  (tavan artık 5x)
# ============================================================

def calculate_leverage(signal):
    score = signal["score"]
    adx_val = signal["adx"]
    atr_pct = signal["atr_pct"]

    leverage = MIN_LEVERAGE

    if score >= 85:
        leverage += 1

    if score >= 92:
        leverage += 1

    if adx_val >= 30:
        leverage += 1

    if atr_pct > 4:
        leverage -= 1

    return int(clamp(leverage, MIN_LEVERAGE, MAX_LEVERAGE))


def set_isolated_and_leverage(symbol, leverage):
    try:
        try:
            safe_call(exchange.set_margin_mode, "isolated", symbol)
        except Exception as e:
            text = str(e).lower()
            if "already" not in text and "no need" not in text:
                logger.warning("%s isolated ayarlanamadı: %s", symbol, e)

        try:
            safe_call(exchange.set_leverage, leverage, symbol)
        except Exception as e:
            logger.warning("%s leverage ayarlanamadı: %s", symbol, e)

    except Exception as e:
        logger.warning("%s margin/leverage hatası: %s", symbol, e)


# ============================================================
# ENTRY PRICE
# ============================================================

def fetch_current_price(symbol):
    ticker = safe_call(exchange.fetch_ticker, symbol)
    return safe_float(ticker.get("last"))


def fetch_current_price_fast(symbol):
    """Pozisyon izleme döngüsü için hızlı fiyat sorgusu — az retry,
    az bekleme. Amaç: yazılım tabanlı stop kontrolünün API
    gecikmesinden dolayı geç kalmasını en aza indirmek."""
    try:
        ticker = safe_call(exchange.fetch_ticker, symbol, retries=1, delay=0.3)
        return safe_float(ticker.get("last"))
    except Exception:
        return 0.0


# ============================================================
# BORSA SEVIYESINDE FAILSAFE STOP EMRI  (YENİ)
# ============================================================
# Botun yazılım tabanlı (polling) stop-loss mantığı; API gecikmesi,
# rate limit veya bot çökmesi durumunda geç kalabilir. Bu yüzden
# gerçek işlemlerde, pozisyon açılır açılmaz Binance'e GERÇEK bir
# STOP_MARKET emri de gönderiyoruz. Bu emir borsa sunucusunda durur
# ve bot ne yaparsa yapsın (çöksün, yavaşlasın, internetsiz kalsın)
# devrede kalır. Yazılımsal SL/trailing/kâr kilitleme normal şartlarda
# bundan önce tetiklenip pozisyonu daha erken/akıllıca kapatacaktır;
# bu native emir sadece SON ÇARE (failsafe) güvenlik ağıdır — bu
# yüzden yazılımsal SL'den biraz daha geniş (HARD_STOP_BUFFER) tutulur.

HARD_STOP_BUFFER = 1.15  # native stop, yazılımsal SL'den %15 daha geniş


def place_stop_market_order(symbol, entry_side, amount, stop_price):
    try:
        close_side = "sell" if entry_side == "buy" else "buy"

        order = safe_call(
            exchange.create_order,
            symbol, "STOP_MARKET", close_side, amount, None,
            {
                "stopPrice": format_price(symbol, stop_price),
                "reduceOnly": True,
                "positionSide": "BOTH"
            }
        )

        logger.warning(
            "[FAILSAFE STOP] %s | stop_price=%s | side=%s",
            symbol, stop_price, close_side
        )

        return order.get("id")

    except Exception as e:
        logger.error(
            "%s FAILSAFE STOP_MARKET emri yerleştirilemedi (yazılımsal SL yine de aktif): %s",
            symbol, e
        )
        return None


def cancel_stop_order(symbol, order_id):
    if not order_id:
        return
    try:
        safe_call(exchange.cancel_order, order_id, symbol)
    except Exception as e:
        logger.info("%s failsafe stop iptali başarısız (muhtemelen zaten tetiklenmiş/yok): %s", symbol, e)


# ============================================================
# ORDER QUANTITY
# ============================================================

def calculate_amount(symbol, margin, leverage, price):
    notional = margin * leverage
    return notional / price


# ============================================================
# DRY RUN ORDER
# ============================================================

def create_dry_run_position(signal):
    symbol = signal["symbol"]
    direction = signal["direction"]
    mode = signal["mode"]
    price = signal["price"]

    leverage = calculate_leverage(signal)

    margin = SCALP_MARGIN if mode == "scalp" else OPPORTUNITY_MARGIN

    amount = calculate_amount(symbol, margin, leverage, price)

    key = f"{mode}:{normalize_symbol(symbol)}"

    with state_lock:
        if key in local_positions:
            return False
        if not can_open_mode(mode):
            return False

        local_positions[key] = {
            "key": key,
            "symbol": symbol,
            "mode": mode,
            "side": direction,
            "entry_price": price,
            "amount": amount,
            "margin": margin,
            "leverage": leverage,
            "tp_pct": signal["tp_pct"],
            "sl_pct": signal["sl_pct"],
            "initial_sl_pct": signal["sl_pct"],
            "highest_roi": 0.0,
            "lowest_roi": 0.0,
            "locked_roi": None,
            "trailing_active": False,
            "trail_distance_pct": None,
            "opened_at": now_ms(),
            "last_monitor": now_ms(),
            "last_trend_check": 0,
            "stop_order_id": None,
        }

    logger.warning(
        "[DRY RUN] %s %s açıldı | score=%.2f | TP=%.2f%% | SL=%.2f%% | lev=%sx | margin=%s$",
        mode.upper(), symbol, signal["score"], signal["tp_pct"], signal["sl_pct"], leverage, margin
    )

    bot_stats["orders"] += 1
    return True


# ============================================================
# REAL MARKET ORDER
# ============================================================

def create_real_position(signal):
    symbol = signal["symbol"]
    direction = signal["direction"]
    mode = signal["mode"]

    key = f"{mode}:{normalize_symbol(symbol)}"

    with state_lock:
        if len(local_positions) >= MAX_TOTAL_POSITIONS:
            return False
        if not can_open_mode(mode):
            return False
        if has_local_symbol(symbol):
            return False

    real_positions = fetch_real_positions()
    normalized = normalize_symbol(symbol)

    for p in real_positions:
        if (
            normalize_symbol(p["symbol"]) == normalized
            and abs(safe_float(p["contracts"])) > 0
        ):
            logger.info("%s zaten açık pozisyon. Emir iptal.", symbol)
            return False

    leverage = calculate_leverage(signal)
    margin = SCALP_MARGIN if mode == "scalp" else OPPORTUNITY_MARGIN

    price = fetch_current_price(symbol)

    if price <= 0:
        return False

    amount = calculate_amount(symbol, margin, leverage, price)
    amount = float(format_amount(symbol, amount))

    if amount <= 0:
        return False

    set_isolated_and_leverage(symbol, leverage)

    side = "buy" if direction == "long" else "sell"

    order = safe_call(
        exchange.create_order,
        symbol, "market", side, amount, None,
        {"positionSide": "BOTH"}
    )

    entry_price = price

    try:
        filled = safe_float(order.get("average"))
        if filled > 0:
            entry_price = filled
    except Exception:
        pass

    with state_lock:
        local_positions[key] = {
            "key": key,
            "symbol": symbol,
            "mode": mode,
            "side": direction,
            "entry_price": entry_price,
            "amount": amount,
            "margin": margin,
            "leverage": leverage,
            "tp_pct": signal["tp_pct"],
            "sl_pct": signal["sl_pct"],
            "initial_sl_pct": signal["sl_pct"],
            "highest_roi": 0.0,
            "lowest_roi": 0.0,
            "locked_roi": None,
            "trailing_active": False,
            "trail_distance_pct": None,
            "opened_at": now_ms(),
            "last_monitor": now_ms(),
            "last_trend_check": 0,
            "stop_order_id": None,
        }

    # --------------------------------------------------------
    # Borsa seviyesinde failsafe stop emri
    # --------------------------------------------------------
    # initial_sl_pct bir ROI% değeridir (kaldıraçlı). Native emir
    # ham fiyat seviyesinde çalıştığından, ROI hedefini gerçek fiyat
    # mesafesine çeviriyoruz: fiyat_mesafesi = sl_roi% / 100 / kaldıraç.
    # Yazılımsal SL'den daha geç tetiklensin diye HARD_STOP_BUFFER ile
    # biraz genişletiyoruz (bu yüzden normal şartlarda asla tetiklenmemesi
    # beklenir — sadece bot/ağ arızasında devreye girer).

    try:
        price_distance_fraction = (
            (signal["sl_pct"] * HARD_STOP_BUFFER) / 100 / leverage
        )

        if direction == "long":
            stop_price = entry_price * (1 - price_distance_fraction)
        else:
            stop_price = entry_price * (1 + price_distance_fraction)

        stop_order_id = place_stop_market_order(symbol, side, amount, stop_price)

        with state_lock:
            if key in local_positions:
                local_positions[key]["stop_order_id"] = stop_order_id

    except Exception as e:
        logger.error("%s failsafe stop hesaplanamadı: %s", symbol, e)

    logger.warning(
        "[REAL] %s %s açıldı | entry=%s | score=%.2f | TP=%.2f%% | SL=%.2f%% | lev=%sx | margin=%s$",
        mode.upper(), symbol, entry_price, signal["score"], signal["tp_pct"], signal["sl_pct"], leverage, margin
    )

    bot_stats["orders"] += 1
    return True


# ============================================================
# OPEN POSITION
# ============================================================

def open_position(signal):
    if not signal:
        return False

    symbol = signal["symbol"]
    mode = signal["mode"]

    if is_on_cooldown(symbol):
        return False
    if not can_open_mode(mode):
        return False
    if has_local_symbol(symbol):
        return False

    result = create_dry_run_position(signal) if DRY_RUN else create_real_position(signal)

    if result:
        set_cooldown(symbol)

    return result


# ============================================================
# ROI
# ============================================================

def calculate_roi(position, price):
    entry = safe_float(position["entry_price"])
    leverage = safe_float(position["leverage"], 1)

    if entry <= 0:
        return 0.0

    side = position["side"]

    if side == "long":
        price_change = (price - entry) / entry
    else:
        price_change = (entry - price) / entry

    return price_change * leverage * 100


# ============================================================
# TREND / MOMENTUM LIVE CHECK
# ============================================================

def live_signal_check(symbol, direction):
    try:
        df5 = fetch_ohlcv(symbol, "5m", 100)
        df15 = fetch_ohlcv(symbol, "15m", 100)

        if df5 is None or df15 is None:
            return {"trend_ok": True, "momentum_ok": True, "strength": 50}

        df5 = enrich_dataframe(df5)
        df15 = enrich_dataframe(df15)

        t5 = timeframe_trend(df5)
        t15 = timeframe_trend(df15)

        m5 = momentum_analysis(df5)
        m15 = momentum_analysis(df15)

        # "ok" (bozulma yok) sayılma eşiği yükseltildi: ters yönde
        # sadece belirgin güçte (>=55) bir sinyal varsa "bozuldu"
        # sayılsın; zayıf/geçici dalgalanmalar erken çıkışı tetiklemesin.
        trend_ok = (t5["direction"] == direction or t5["strength"] < 55)
        trend_ok = trend_ok and (t15["direction"] == direction or t15["strength"] < 55)

        momentum_ok = (m5["direction"] == direction or m5["strength"] < 55)

        strength = (
            t5["strength"] * 0.25 +
            t15["strength"] * 0.35 +
            m5["strength"] * 0.20 +
            m15["strength"] * 0.20
        )

        return {"trend_ok": trend_ok, "momentum_ok": momentum_ok, "strength": strength}

    except Exception as e:
        logger.warning("%s live trend kontrolü başarısız: %s", symbol, e)
        return {"trend_ok": True, "momentum_ok": True, "strength": 50}


# ============================================================
# DYNAMIC POSITION MANAGEMENT
# ============================================================

def update_position_management(position, price):
    roi = calculate_roi(position, price)
    position["current_roi"] = roi

    if roi > position["highest_roi"]:
        position["highest_roi"] = roi
    if roi < position["lowest_roi"]:
        position["lowest_roi"] = roi

    tp = position["tp_pct"]

    if roi >= tp * 0.30:
        if position["locked_roi"] is None or position["locked_roi"] < 0:
            position["locked_roi"] = 0.0

    if roi >= tp * 0.45:
        lock = tp * 0.15
        if position["locked_roi"] is None or position["locked_roi"] < lock:
            position["locked_roi"] = lock

    if roi >= tp * 0.60:
        lock = tp * 0.30
        if position["locked_roi"] is None or position["locked_roi"] < lock:
            position["locked_roi"] = lock

    if roi >= tp * 0.75:
        lock = tp * 0.45
        if position["locked_roi"] is None or position["locked_roi"] < lock:
            position["locked_roi"] = lock

    if roi >= tp * 0.90:
        lock = tp * 0.60
        if position["locked_roi"] is None or position["locked_roi"] < lock:
            position["locked_roi"] = lock

    if roi >= tp * 0.55:
        position["trailing_active"] = True

        # Daha geniş mesafe: normal geri çekilmelerde hedefe giden
        # işlemin erken kesilmesini önlemek için trailing daha geç
        # aktifleşir ve daha toleranslı takip eder.
        if position["mode"] == "scalp":
            trail = max(0.45, tp * 0.35)
        else:
            trail = max(0.75, tp * 0.40)

        if roi >= tp * 0.75:
            trail *= 0.80
        if roi >= tp * 0.92:
            trail *= 0.70

        position["trail_distance_pct"] = trail

    return roi


# ============================================================
# EXIT DECISION
# ============================================================

def should_close_position(position, price):
    roi = update_position_management(position, price)

    tp = position["tp_pct"]
    initial_sl = position["initial_sl_pct"]

    # Mutlak güvenlik tavanı: hiçbir hesaplama/gecikme bu seviyeyi
    # aşarak pozisyonu açık tutamaz. initial_sl'nin normalde çok
    # altında olması beklenir; bu sadece son çare bir sigortadır.
    hard_cap = initial_sl * 2.0

    if roi <= -hard_cap:
        return True, "HARD_FAILSAFE_STOP"

    if roi <= -initial_sl:
        return True, "INITIAL_STOP"

    locked = position.get("locked_roi")
    if locked is not None and roi <= locked:
        return True, "PROFIT_LOCK"

    if position.get("trailing_active"):
        peak = position["highest_roi"]
        distance = position.get("trail_distance_pct")
        if distance:
            if peak - roi >= distance and roi > 0:
                return True, "TRAILING_STOP"

    current_time = now_ms()

    if (current_time - position.get("last_trend_check", 0)) >= 15000:
        position["last_trend_check"] = current_time

        check = live_signal_check(position["symbol"], position["side"])
        position["live_strength"] = check["strength"]

        # Opportunity modu daha uzun soluklu bir strateji olduğu için
        # erken çıkış eşikleri Scalp'e göre daha sabırlı (geç tetiklenir).
        # Eşikler yükseltildi: küçük geri çekilmelerde değil, sadece
        # gerçekten belirgin bir bozulmada erken çıkılsın.
        if position["mode"] == "opportunity":
            reversal_trigger = 0.50
            fade_trigger = 0.75
            fade_strength_floor = 20
        else:
            reversal_trigger = 0.40
            fade_trigger = 0.65
            fade_strength_floor = 25

        if roi > tp * reversal_trigger and not check["trend_ok"] and not check["momentum_ok"]:
            return True, "TREND_MOMENTUM_REVERSAL"

        if roi > tp * fade_trigger and check["strength"] < fade_strength_floor:
            return True, "MOMENTUM_FADE"

    if roi >= tp:
        return True, "TARGET_REACHED"

    return False, None


# ============================================================
# CLOSE POSITION
# ============================================================

def close_dry_position(key, reason):
    with state_lock:
        position = local_positions.get(key)
        if not position:
            return False

        roi = position.get("current_roi", 0)
        symbol = position["symbol"]
        mode = position["mode"]

        local_positions.pop(key, None)

    logger.warning(
        "[DRY RUN] %s %s kapandı | ROI=%.2f%% | sebep=%s",
        mode.upper(), symbol, roi, reason
    )

    bot_stats["closed_positions"] += 1
    return True


def close_real_position(key, reason):
    with state_lock:
        position = local_positions.get(key)
        if not position:
            return False

    symbol = position["symbol"]

    try:
        amount = position["amount"]
        side = "sell" if position["side"] == "long" else "buy"

        safe_call(
            exchange.create_order,
            symbol, "market", side, amount, None,
            {"reduceOnly": True, "positionSide": "BOTH"}
        )

        # Bot bilinçli olarak pozisyonu kapattı; artık gerek kalmayan
        # failsafe stop emrini borsadan iptal ediyoruz.
        cancel_stop_order(symbol, position.get("stop_order_id"))

        with state_lock:
            local_positions.pop(key, None)

        logger.warning(
            "[REAL] %s %s kapandı | ROI=%.2f%% | sebep=%s",
            position["mode"].upper(), symbol, position.get("current_roi", 0), reason
        )

        bot_stats["closed_positions"] += 1
        return True

    except Exception as e:
        logger.error("%s kapatma hatası: %s", symbol, e)
        return False


def close_position(key, reason):
    return close_dry_position(key, reason) if DRY_RUN else close_real_position(key, reason)


# ============================================================
# POSITION MONITOR
# ============================================================

def monitor_positions():
    logger.info("POSITION MONITOR başlatıldı.")

    while running:
        try:
            sync_real_positions()

            with state_lock:
                positions = [dict(p) for p in local_positions.values()]

            if not positions:
                time.sleep(POSITION_MONITOR_INTERVAL)
                continue

            for position in positions:
                symbol = position["symbol"]

                try:
                    price = fetch_current_price_fast(symbol)

                    if price <= 0:
                        continue

                    key = position["key"]

                    with state_lock:
                        current = local_positions.get(key)

                        if not current:
                            continue

                        should_close, reason = should_close_position(current, price)

                        roi = current.get("current_roi", 0)
                        peak = current.get("highest_roi", 0)
                        tp = current["tp_pct"]
                        sl = current["sl_pct"]

                    logger.info(
                        "[MONITOR] %s | %s | price=%s | ROI=%+.2f%% | peak=%+.2f%% | "
                        "TP=%.2f%% | SL=%.2f%% | lock=%s | trail=%s",
                        symbol, position["mode"].upper(), price, roi, peak, tp, sl,
                        current.get("locked_roi"), current.get("trail_distance_pct")
                    )

                    if should_close:
                        close_position(key, reason)

                except Exception as e:
                    logger.warning("%s monitor hatası: %s", symbol, e)

            time.sleep(POSITION_MONITOR_INTERVAL)

        except Exception as e:
            logger.error("Monitor ana hata: %s", e)
            time.sleep(2)


# ============================================================
# CANDIDATE ANALYSIS
# ============================================================

def analyze_candidates(symbols, mode, btc_regime=None):
    candidates = []

    for symbol in symbols:
        if not symbol_is_valid(symbol):
            continue
        if is_on_cooldown(symbol):
            continue
        if has_local_symbol(symbol):
            continue

        try:
            result = analyze_coin(symbol, mode, btc_regime=btc_regime)

            if result:
                candidates.append(result)

                logger.info(
                    "[%s] %s | score=%.2f | trend=%.2f | momentum=%.2f | "
                    "align=%s | timing=%s | taker=%s | formasyon=%s | TP=%.2f%% SL=%.2f%%",
                    mode.upper(), symbol, result["score"], result["trend_score"],
                    result["momentum_score"], result["alignment"], result["timing_score"],
                    result["taker_note"], result["pattern"], result["tp_pct"], result["sl_pct"]
                )

        except Exception as e:
            logger.warning("%s analiz hatası: %s", symbol, e)

        time.sleep(0.05)

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates


# ============================================================
# FINAL ENTRY CONFIRMATION
# ============================================================

def final_entry_confirmation(signal):
    symbol = signal["symbol"]
    direction = signal["direction"]

    try:
        df = fetch_ohlcv(symbol, "5m", 100)

        if df is None:
            return False

        df = enrich_dataframe(df)

        timing = entry_timing(df, direction)

        if not timing["confirmed"]:
            logger.info("[ENTRY RED] %s timing teyidi yok.", symbol)
            return False

        if is_overextended(df, direction):
            logger.info("[ENTRY RED] %s aşırı uzamış.", symbol)
            return False

        last = df.iloc[-1]
        close = safe_float(last["close"])
        ema21 = safe_float(last["ema21"])
        atr_val = safe_float(last["atr"])

        if direction == "long":
            if close - ema21 > atr_val * 2.0:
                return False
        else:
            if ema21 - close > atr_val * 2.0:
                return False

        funding = get_funding(symbol)
        if abs(funding) >= FUNDING_SKIP_THRESHOLD:
            return False

        return True

    except Exception as e:
        logger.warning("%s final confirmation hatası: %s", symbol, e)
        return False


# ============================================================
# TOP-N ADAY ÜZERINDEN SIRALI TEYIT  (YENİ)
# ============================================================
# Sadece en yüksek skorlu tek adaya bakmak yerine, top-5 adayı
# skora göre sırayla dener. İlk teyidi geçen (ve emir başarıyla
# açılan) adayda durur. Hiçbiri geçemezse o döngüde işlem açılmaz.

def try_open_from_candidates(signals, mode):
    top_candidates = signals[:TOP_N_CANDIDATES]

    logger.info(
        "[%s] Top %s aday: %s",
        mode.upper(),
        len(top_candidates),
        [(c["symbol"], c["direction"], c["score"]) for c in top_candidates]
    )

    for candidate in top_candidates:
        logger.info(
            "[%s TEYİT DENENİYOR] %s | yön=%s | skor=%.2f | trend=%.2f | "
            "momentum=%.2f | align=%s | timing=%s | taker=%s | funding=%.5f | "
            "TP=%.2f%% SL=%.2f%%",
            mode.upper(), candidate["symbol"], candidate["direction"], candidate["score"],
            candidate["trend_score"], candidate["momentum_score"], candidate["alignment"],
            candidate["timing_score"], candidate["taker_note"], candidate["funding"],
            candidate["tp_pct"], candidate["sl_pct"]
        )

        if final_entry_confirmation(candidate):
            logger.warning(
                "[%s AÇILIYOR] %s %s | skor=%.2f | gerekçe: trend+momentum hizalı, "
                "son teyit geçti, BTC rejimine ters değil",
                mode.upper(), candidate["direction"], candidate["symbol"], candidate["score"]
            )

            if open_position(candidate):
                return True
        else:
            logger.info(
                "[%s RED] %s son teyidi geçemedi, sıradaki adaya geçiliyor.",
                mode.upper(), candidate["symbol"]
            )

    logger.info("[%s] Top-%s aday içinde teyidi geçen olmadı.", mode.upper(), TOP_N_CANDIDATES)
    return False


# ============================================================
# ANALYSIS CYCLE
# ============================================================

def analysis_cycle():
    global last_analysis_time
    global last_successful_analysis

    bot_stats["analysis_count"] += 1
    last_analysis_time = now_ms()

    logger.info("=" * 70)
    logger.info("BOT ANALİZ BAŞLADI | %s", datetime.now(timezone.utc).isoformat())

    sync_real_positions()

    total = local_position_count()
    scalp_count = local_position_count("scalp")
    opp_count = local_position_count("opportunity")

    logger.info(
        "[POZİSYON] toplam=%s | scalp=%s | opportunity=%s",
        total, scalp_count, opp_count
    )

    if total >= MAX_TOTAL_POSITIONS:
        logger.info("[DOLU] Maksimum pozisyon sayısına ulaşıldı, analiz atlanıyor.")
        return

    # --------------------------------------------------------
    # BTC piyasa rejimi — bu döngü için bir kez hesaplanır
    # --------------------------------------------------------

    btc_regime = get_btc_regime()

    logger.info(
        "[BTC REJİM] yön=%s | güç=%.1f",
        btc_regime["direction"], btc_regime["strength"]
    )

    # --------------------------------------------------------
    # Movers
    # --------------------------------------------------------

    gainers, losers, volume_leaders = get_top_movers()

    logger.info("[GAINERS TOP25] %s", gainers)
    logger.info("[LOSERS TOP25] %s", losers)
    logger.info("[VOLUME TOP25] %s", volume_leaders)

    candidates = []
    seen = set()

    btc_normalized = normalize_symbol(BTC_SYMBOL)

    for symbol in gainers + losers + volume_leaders:
        normalized = normalize_symbol(symbol)
        if normalized in seen:
            continue
        if normalized == btc_normalized:
            # BTC yalnızca piyasa rejimi (yön) belirlemek için
            # kullanılır, min notional / margin uyumsuzluğu
            # nedeniyle işlem adayı olarak taranmaz.
            continue
        seen.add(normalized)
        candidates.append(symbol)

    logger.info("[TARAMA] %s benzersiz coin (likidite filtresinden geçen, BTC hariç).", len(candidates))

    # --------------------------------------------------------
    # Scalp (bağımsız tarama — opportunity dolu olsa bile çalışır)
    # --------------------------------------------------------

    if SCALP_ENABLED and scalp_count < MAX_SCALP_POSITIONS and total < MAX_TOTAL_POSITIONS:
        logger.info("[SCALP] %s coin analiz ediliyor...", len(candidates))

        scalp_signals = analyze_candidates(candidates, "scalp", btc_regime=btc_regime)

        if scalp_signals:
            try_open_from_candidates(scalp_signals, "scalp")
        else:
            logger.info("[SCALP] Uygun sinyal yok.")

    # --------------------------------------------------------
    # Opportunity (bağımsız tarama)
    # --------------------------------------------------------

    total = local_position_count()
    opp_count = local_position_count("opportunity")

    if OPPORTUNITY_ENABLED and opp_count < MAX_OPPORTUNITY_POSITIONS and total < MAX_TOTAL_POSITIONS:
        logger.info("[OPPORTUNITY] %s coin analiz ediliyor...", len(candidates))

        opp_signals = analyze_candidates(candidates, "opportunity", btc_regime=btc_regime)

        if opp_signals:
            try_open_from_candidates(opp_signals, "opportunity")
        else:
            logger.info("[OPPORTUNITY] Uygun sinyal yok.")

    last_successful_analysis = datetime.now(timezone.utc).isoformat()

    logger.info("BOT ANALİZ BİTTİ")
    logger.info("=" * 70)


# ============================================================
# ANALYSIS LOOP
# ============================================================

def analysis_loop():
    logger.info("ANALYSIS LOOP başlatıldı.")

    while running:
        start = time.time()

        try:
            analysis_cycle()
        except Exception as e:
            logger.exception("Analiz döngüsü hatası: %s", e)

        elapsed = time.time() - start

        wait_time = max(NO_SIGNAL_INTERVAL, ANALYSIS_INTERVAL - elapsed)

        for _ in range(int(wait_time)):
            if not running:
                break
            time.sleep(1)


# ============================================================
# MEMORY CLEANUP
# ============================================================

def cleanup_loop():
    while running:
        try:
            gc.collect()

            cutoff = now_ms() - (30 * 60 * 1000)

            with candidate_lock:
                old = [
                    k for k, v in signal_cache.items()
                    if v.get("timestamp", 0) < cutoff
                ]
                for k in old:
                    signal_cache.pop(k, None)

        except Exception:
            pass

        time.sleep(300)


# ============================================================
# START
# ============================================================

def start_bot():
    global running

    logger.info("=" * 70)
    logger.info("BINANCE FUTURES MOMENTUM BOT V2")
    logger.info("=" * 70)
    logger.info("DRY_RUN=%s | TESTNET=%s | MAX_LEVERAGE=%sx | ANALYSIS_INTERVAL=%ss",
                DRY_RUN, TESTNET, MAX_LEVERAGE, ANALYSIS_INTERVAL)

    create_exchange()

    monitor_thread = threading.Thread(target=monitor_positions, name="PositionMonitor", daemon=True)
    monitor_thread.start()

    analysis_thread = threading.Thread(target=analysis_loop, name="AnalysisLoop", daemon=True)
    analysis_thread.start()

    cleanup_thread = threading.Thread(target=cleanup_loop, name="Cleanup", daemon=True)
    cleanup_thread.start()

    logger.info("BOT TAMAMEN AKTİF.")

    return monitor_thread, analysis_thread, cleanup_thread


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    try:
        threads = start_bot()

        flask_thread = threading.Thread(
            target=lambda: app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False),
            name="Flask",
            daemon=True
        )
        flask_thread.start()

        logger.info("Health server :%s üzerinde çalışıyor.", PORT)

        while True:
            time.sleep(60)

    except KeyboardInterrupt:
        logger.warning("BOT DURDURULUYOR...")
        running = False

    except Exception as e:
        logger.exception("FATAL BOT HATASI: %s", e)
        running = False
        raise
