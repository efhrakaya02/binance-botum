import os
import time
import math
import gc
import json
import logging
import threading
from datetime import datetime, timezone

import ccxt
import numpy as np
import pandas as pd
from flask import Flask, jsonify


# ============================================================
# BINANCE FUTURES — HIGH-CONVICTION PULLBACK & BREAKOUT BOT V3.5 — RELAXED + PATTERNS + FULL DIAGNOSTICS + BTC/XAU EXCLUDED
# ============================================================
#
# STRATEJİ ÖZETİ
# ------------------------------------------------------------
# Tek strateji:
#   MARKET REGIME -> TREND -> PULLBACK -> PULLBACK EXHAUSTION ->
#   MOMENTUM REVERSAL -> MOMENTUM ACCELERATION -> MICRO STRUCTURE
#   BREAK -> BREAKOUT CONFIRMATION -> VOLUME CONFIRMATION ->
#   CANDLE CLOSE -> EXPECTED MOVE -> ENTRY
#
# Giriş motoru dört kavramsal aşamadan oluşur:
#   SETUP (aday)  -> WATCH (pullback bitiyor mu) ->
#   ARM (momentum dönüyor mu) -> FIRE (yapı kırılımı + hacim + mum
#   kapanışı teyidiyle giriş)
#
# SETUP_SCORE (4H/1H/15M zemin kalitesi) ve TRIGGER_SCORE (5M giriş
# zamanlaması kalitesi) ayrı ayrı hesaplanır; ikisi de eşiği geçmeden
# işlem açılmaz.
#
# Pozisyon açıldıktan sonra: ATR + yapı bazlı dinamik stop, hedefin
# (min +%10 ROI) %75'inde kâr kilitleme + ATR trailing (asla pozisyon
# aleyhine gevşetilmez), gerçek bir trend/momentum/yapı çöküşü
# olmadıkça küçük kârla erken kapatma yapılmaz.
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
# Pozisyon parametreleri
# ------------------------------------------------------------

MARGIN_PER_TRADE = 10.0     # Her işlem sabit ~10 USDT margin kullanır
MAX_LEVERAGE = 5             # Kaldıraç ASLA bunu geçmez (1x-5x arası dinamik)
MIN_LEVERAGE = 1
MAX_OPEN_POSITIONS = 3       # Aynı anda en fazla 3 açık pozisyon

# ------------------------------------------------------------
# Hedef / Kâr Kilitleme / Risk
# ------------------------------------------------------------

MIN_TARGET_ROI = 10.0                 # İlk kâr milestone'u (%), kapanış tetiği DEĞİL
PROFIT_LOCK_TRIGGER_RATIO = 0.75      # Hedefin %75'inde kâr kilitleme + trailing aktifleşir
MAX_LOSS_TO_TARGET_RATIO = 0.50       # Maksimum zarar, hedefin %50'sini geçemez (1:2 R/R tabanı)

# ------------------------------------------------------------
# Setup / Trigger skorları
# ------------------------------------------------------------

MIN_SETUP_SCORE = 68
MIN_TRIGGER_SCORE = 68

MIN_BREAKOUT_VOLUME_RATIO = 1.10
REQUIRED_REVERSAL_CONFIRMATIONS = 2

PATTERN_MIN_SCORE = 62
PATTERN_MIN_TRIGGER_SCORE = 64
PATTERN_VOLUME_RATIO = 1.05
PATTERN_LOOKBACK = 90
PATTERN_MAX_SHOULDER_DIFF = 0.035
PATTERN_MAX_HEAD_SHOULDER_DIFF = 0.020
FLAG_MAX_RETRACE = 0.55
FLAG_MIN_IMPULSE_ATR = 1.5

# ------------------------------------------------------------
# ATR bazlı stop / trailing
# ------------------------------------------------------------

INITIAL_STOP_ATR_MULTIPLIER = 1.8
TRAILING_ATR_MULTIPLIER = 1.5
MAX_ENTRY_CHASE_ATR = 1.8             
EARLY_TRIGGER_MAX_ATR = 1.20          
EARLY_TRIGGER_MIN_CONFIRMATIONS = 2   
EARLY_TRIGGER_MIN_SCORE = 58           

# ------------------------------------------------------------
# Aday sayısı / Likidite / BTC rejimi
# ------------------------------------------------------------

TOP_N_CANDIDATES = 5
MIN_QUOTE_VOLUME_USDT = 2_000_000
BTC_SYMBOL = "BTC/USDT"
EXCLUDED_TRADE_SYMBOLS = {"BTC/USDT:USDT", "BTC/USDT", "XAU/USDT:USDT", "XAU/USDT"}
BTC_REGIME_MIN_STRENGTH = 60
CORRELATION_MAX_ALLOWED = 0.85        

# ------------------------------------------------------------
# Monitor / Döngü
# ------------------------------------------------------------

POSITION_MONITOR_INTERVAL = 1.0
ANALYSIS_INTERVAL = 300
NO_SIGNAL_INTERVAL = 60
LIVE_CHECK_INTERVAL_MS = 15000

# ------------------------------------------------------------
# Cooldown / Funding
# ------------------------------------------------------------

COOLDOWN_HOURS = 4
COOLDOWN_MS = COOLDOWN_HOURS * 60 * 60 * 1000

FUNDING_SKIP_THRESHOLD = 0.0015

# ------------------------------------------------------------
# AYRINTILI ANALİZ / REJECTION DIAGNOSTICS
# ------------------------------------------------------------
DETAILED_DIAGNOSTICS = os.getenv("DETAILED_DIAGNOSTICS", "true").lower() == "true"
LOG_EVERY_CANDIDATE_STAGE = os.getenv("LOG_EVERY_CANDIDATE_STAGE", "true").lower() == "true"

DIAGNOSTIC_KEYS = [
    "scanned", "excluded_symbol", "invalid_symbol", "cooldown", "already_position", "data_missing",
    "anomaly", "trend_neutral", "trend_weak", "momentum_conflict", "htf_conflict",
    "funding", "no_setup", "pullback_unhealthy", "no_15m_reversal", "pattern_none",
    "pattern_BULL_FLAG", "pattern_BEAR_FLAG", "pattern_TOBO", "pattern_OBO",
    "pattern_other", "pattern_breakout", "pattern_scanned", "pattern_candidate", "pattern_valid", "no_breakout", "breakout_body",
    "breakout_volume", "breakout_atr", "breakout_close", "entry_chase",
    "invalid_atr", "setup_score_low", "trigger_score_low", "trigger_confirmations", "final_reason_no_data", "final_reason_breakout", "final_reason_chase", "final_reason_momentum", "final_reason_rsi", "final_reason_volume", "final_reason_retest", "final_reason_funding", "final_reason_other",
    "expected_move", "correlation", "btc_risk_adjusted", "final_confirmation",
    "opened", "setup_candidates", "breakout_confirmed", "analysis_error"
]

def new_diagnostics():
    return {k: 0 for k in DIAGNOSTIC_KEYS}

def diag_inc(key, amount=1):
    if "diagnostics" not in bot_stats:
        bot_stats["diagnostics"] = new_diagnostics()
    bot_stats["diagnostics"][key] = bot_stats["diagnostics"].get(key, 0) + amount

def reset_cycle_diagnostics():
    bot_stats["diagnostics"] = new_diagnostics()

def diag_stage(symbol, stage, message=""):
    if LOG_EVERY_CANDIDATE_STAGE:
        suffix = f" | {message}" if message else ""
        logger.info("[HC STAGE] %s | %-22s%s", symbol, stage, suffix)

def diag_reject(symbol, reason, message=""):
    diag_inc(reason)
    if LOG_EVERY_CANDIDATE_STAGE:
        suffix = f" | {message}" if message else ""
        logger.info("[HC REJECT] %s | %-22s%s", symbol, reason, suffix)

def pattern_diag_key(pattern_type):
    key = f"pattern_{pattern_type}"
    return key if key in DIAGNOSTIC_KEYS else "pattern_other"

def log_cycle_diagnostics(scanned, final_candidates, opened):
    d = bot_stats.get("diagnostics", new_diagnostics())
    logger.info("=" * 70)
    logger.info("[HC DIAGNOSTICS] Opportunity Funnel")
    logger.info("taranan=%s | final_aday=%s | açılan=%s", scanned, final_candidates, opened)
    logger.info(
        "[HC DIAGNOSTICS] Ön eleme | excluded=%s invalid=%s cooldown=%s açık_pozisyon=%s data_missing=%s "
        "anomaly=%s trend_neutral=%s trend_weak=%s momentum_conflict=%s htf_conflict=%s funding=%s",
        d.get("excluded_symbol",0), d.get("invalid_symbol",0), d.get("cooldown",0), d.get("already_position",0),
        d.get("data_missing",0), d.get("anomaly",0), d.get("trend_neutral",0),
        d.get("trend_weak",0), d.get("momentum_conflict",0), d.get("htf_conflict",0),
        d.get("funding",0)
    )
    logger.info(
        "[HC DIAGNOSTICS] Setup | no_setup=%s pullback_unhealthy=%s no_15m_reversal=%s setup_score_low=%s",
        d.get("no_setup",0), d.get("pullback_unhealthy",0),
        d.get("no_15m_reversal",0), d.get("setup_score_low",0)
    )
    logger.info(
        "[HC DIAGNOSTICS] Pattern | scanned=%s candidate=%s valid=%s BULL_FLAG=%s BEAR_FLAG=%s TOBO=%s OBO=%s other=%s pattern_breakout=%s",
        d.get("pattern_scanned",0), d.get("pattern_candidate",0), d.get("pattern_valid",0),
        d.get("pattern_BULL_FLAG",0), d.get("pattern_BEAR_FLAG",0),
        d.get("pattern_TOBO",0), d.get("pattern_OBO",0),
        d.get("pattern_other",0), d.get("pattern_breakout",0)
    )
    logger.info(
        "[HC DIAGNOSTICS] Breakout | no_breakout=%s body=%s volume=%s atr=%s close=%s chase=%s confirmed=%s",
        d.get("no_breakout",0), d.get("breakout_body",0), d.get("breakout_volume",0),
        d.get("breakout_atr",0), d.get("breakout_close",0), d.get("entry_chase",0),
        d.get("breakout_confirmed",0)
    )
    logger.info(
        "[HC DIAGNOSTICS] Trigger | score_low=%s confirmations=%s expected_move=%s "
        "correlation=%s btc_risk_adjusted=%s final_confirmation=%s",
        d.get("trigger_score_low",0), d.get("trigger_confirmations",0),
        d.get("expected_move",0), d.get("correlation",0),
        d.get("btc_risk_adjusted",0), d.get("final_confirmation",0)
    )
    logger.info("[HC DIAGNOSTICS] Hatalar=%s", d.get("analysis_error",0))
    logger.info("=" * 70)


TAKER_FEE_PCT = 0.05
OHLCV_LIMIT = 250
ADX_STRONG = 25
ADX_VERY_STRONG = 35
VOLUME_CONFIRMATION = 1.15
HARD_STOP_BUFFER = 1.15
TRADE_JOURNAL_PATH = os.getenv("TRADE_JOURNAL_PATH", "/tmp/trade_journal.jsonl")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("HC_BOT")


# ============================================================
# GLOBAL STATE
# ============================================================

exchange = None
state_lock = threading.RLock()
running = True
last_analysis_time = 0
last_successful_analysis = None
cooldowns = {}
bot_stats = {
    "analysis_count": 0,
    "signals_found": 0,
    "orders": 0,
    "closed_positions": 0,
    "errors": 0,
    "diagnostics": new_diagnostics(),
}
local_positions = {}


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "bot": "High-Conviction Pullback & Breakout Bot V3.5",
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
# EXCHANGE & SAFE API
# ============================================================

def create_exchange():
    global exchange
    if not API_KEY or not API_SECRET:
        raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET tanımlı değil.")

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
        "Binance bağlantısı hazır | TESTNET=%s | DRY_RUN=%s | MAX_LEVERAGE=%sx | MAX_OPEN_POSITIONS=%s",
        TESTNET, DRY_RUN, MAX_LEVERAGE, MAX_OPEN_POSITIONS
    )
    return exchange

def safe_call(fn, *args, retries=3, delay=1, **kwargs):
    last_error = None
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_error = e
            bot_stats["errors"] += 1
            logger.warning("API hata (%s/%s): %s", attempt + 1, retries, e)
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
    if normalize_symbol(s) in {normalize_symbol(x) for x in EXCLUDED_TRADE_SYMBOLS}:
        return False
    if "/USDT" not in s:
        return False
    blacklist = ["UP/", "DOWN/", "BEAR/", "BULL/", "_", "BID/", "ASK/"]
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
    return series.ewm(span=period, adjust=False, min_periods=period).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
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
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

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
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_val = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr_val.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr_val.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx_val = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean().fillna(0)
    return adx_val, plus_di.fillna(0), minus_di.fillna(0)

def bollinger(series, period=20, std_mult=2):
    middle = series.rolling(period).mean()
    std = series.rolling(period).std()
    return middle, middle + std_mult * std, middle - std_mult * std

def obv(df):
    direction = np.sign(df["close"].diff()).fillna(0)
    return (direction * df["volume"]).cumsum()

def roc(series, period=10):
    return series.pct_change(periods=period) * 100


# ============================================================
# DATA & ENRICHMENT
# ============================================================

def fetch_ohlcv(symbol, timeframe, limit=OHLCV_LIMIT):
    try:
        data = safe_call(exchange.fetch_ohlcv, symbol, timeframe, None, limit)
        if not data:
            return None
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(inplace=True)
        if len(df) < 100:
            return None
        return df
    except Exception as e:
        logger.warning("%s %s OHLCV alınamadı: %s", symbol, timeframe, e)
        return None

def fetch_ohlcv_closed(symbol, timeframe, limit=OHLCV_LIMIT):
    df = fetch_ohlcv(symbol, timeframe, limit)
    if df is None or len(df) < 101:
        return None
    return df.iloc[:-1].reset_index(drop=True)

def enrich_dataframe(df):
    df = df.copy()
    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["ema50"] = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["ema9_slope"] = df["ema9"].diff()
    df["ema21_slope"] = df["ema21"].diff()
    df["rsi"] = rsi(df["close"])
    df["rsi_slope"] = df["rsi"].diff()
    df["atr"] = atr(df)
    df["macd"], df["macd_signal"], df["macd_hist"] = macd(df["close"])
    df["macd_hist_slope"] = df["macd_hist"].diff()
    df["adx"], df["plus_di"], df["minus_di"] = adx(df)
    df["bb_mid"], df["bb_upper"], df["bb_lower"] = bollinger(df["close"])
    df["obv"] = obv(df)
    df["volume_ma20"] = df["volume"].rolling(20).mean()
    df["volume_ratio"] = df["volume"] / df["volume_ma20"].replace(0, np.nan)
    df["roc"] = roc(df["close"], 10)
    df["recent_high"] = df["high"].rolling(20).max()
    df["recent_low"] = df["low"].rolling(20).min()
    df["atr_pct"] = df["atr"] / df["close"] * 100
    candle_range = (df["high"] - df["low"]).replace(0, np.nan)
    df["body_ratio"] = (df["close"] - df["open"]).abs() / candle_range
    return df


# ============================================================
# GAINERS / LOSERS / VOLUME
# ============================================================

def get_top_movers():
    tickers = safe_call(exchange.fetch_tickers)
    rows = []
    for symbol, ticker in tickers.items():
        if not symbol_is_valid(symbol):
            continue
        try:
            last = safe_float(ticker.get("last"))
            percentage = safe_float(ticker.get("percentage"))
            quote_volume = safe_float(ticker.get("quoteVolume"))
            if last <= 0:
                continue
            rows.append({"symbol": symbol, "percentage": percentage, "quoteVolume": quote_volume})
        except Exception:
            continue
    if not rows:
        return [], [], []
    df = pd.DataFrame(rows)
    df = df[df["quoteVolume"] >= MIN_QUOTE_VOLUME_USDT]
    if df.empty:
        return [], [], []
    gainers = df.sort_values("percentage", ascending=False).head(25)["symbol"].tolist()
    losers = df.sort_values("percentage", ascending=True).head(25)["symbol"].tolist()
    volume_leaders = df.sort_values("quoteVolume", ascending=False).head(25)["symbol"].tolist()
    return gainers, losers, volume_leaders


# ============================================================
# FUNDING & BTC REGIME
# ============================================================

def get_funding(symbol):
    try:
        funding = safe_call(exchange.fetch_funding_rate, symbol)
        return safe_float(funding.get("fundingRate"))
    except Exception:
        return 0.0

def get_btc_regime():
    try:
        df15 = fetch_ohlcv_closed(BTC_SYMBOL, "15m", 100)
        df1h = fetch_ohlcv_closed(BTC_SYMBOL, "1h", 100)
        if df15 is None or df1h is None:
            return {"direction": "neutral", "strength": 0}
        df15 = enrich_dataframe(df15)
        df1h = enrich_dataframe(df1h)
        t15 = timeframe_trend(df15)
        t1h = timeframe_trend(df1h)
        if t15["direction"] == t1h["direction"] and t15["direction"] != "neutral":
            strength = (t15["strength"] + t1h["strength"]) / 2
            return {"direction": t15["direction"], "strength": strength}
        return {"direction": "neutral", "strength": 0}
    except Exception as e:
        logger.warning("BTC rejim analizi başarısız: %s", e)
        return {"direction": "neutral", "strength": 0}

def detect_anomaly(df):
    if df is None or len(df) < 10:
        return False
    last5 = df.tail(5)
    price_change_pct = ((last5["close"].iloc[-1] - last5["close"].iloc[0]) / last5["close"].iloc[0] * 100)
    avg_volume_ratio = safe_float(last5["volume_ratio"].mean(), 1)
    if abs(price_change_pct) >= 8 and avg_volume_ratio < 1.3:
        return True
    return False


# ============================================================
# TREND & SWING ANALYSIS
# ============================================================

def timeframe_trend(df):
    if df is None or len(df) < 205:
        return {"direction": "neutral", "strength": 0}
    x = df.iloc[-1]
    price = safe_float(x["close"])
    e9 = safe_float(x["ema9"])
    e21 = safe_float(x["ema21"])
    e50 = safe_float(x["ema50"])
    e200 = safe_float(x["ema200"])
    adx_val = safe_float(x["adx"])
    plus_di = safe_float(x.get("plus_di"))
    minus_di = safe_float(x.get("minus_di"))
    bullish = price > e21 > e50 > e200
    bearish = price < e21 < e50 < e200
    strength = 0
    if bullish or bearish:
        strength += 40
    if adx_val >= ADX_STRONG:
        strength += 30
    if adx_val >= ADX_VERY_STRONG:
        strength += 20
    if bullish and plus_di <= minus_di:
        strength -= 20
    if bearish and minus_di <= plus_di:
        strength -= 20
    strength = clamp(strength, 0, 100)
    if bullish:
        return {"direction": "long", "strength": strength}
    if bearish:
        return {"direction": "short", "strength": strength}
    if price > e50 and e9 > e21:
        soft = 35 - (15 if plus_di <= minus_di else 0)
        return {"direction": "long", "strength": clamp(soft, 0, 100)}
    if price < e50 and e9 < e21:
        soft = 35 - (15 if minus_di <= plus_di else 0)
        return {"direction": "short", "strength": clamp(soft, 0, 100)}
    return {"direction": "neutral", "strength": 0}

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


# ============================================================
# PULLBACK & PATTERN ENGINE
# ============================================================

def detect_pullback(df, direction):
    if df is None or len(df) < 30:
        return False
    x = df.iloc[-1]
    close = safe_float(x["close"])
    ema21 = safe_float(x["ema21"])
    ema50 = safe_float(x["ema50"])
    recent = df.tail(6)
    change = (recent["close"].iloc[-1] - recent["close"].iloc[0]) / recent["close"].iloc[0] * 100
    if direction == "long":
        return close > ema50 and (change <= 0.15 or close <= ema21 * 1.01)
    return close < ema50 and (change >= -0.15 or close >= ema21 * 0.99)

def assess_pullback_quality(df, direction):
    if df is None or len(df) < 30:
        return {"healthy": False, "score": 0, "issues": ["yetersiz veri"]}
    x = df.iloc[-1]
    recent = df.tail(6)
    atr_val = safe_float(x["atr"])
    close = safe_float(x["close"])
    if atr_val <= 0 or close <= 0:
        return {"healthy": False, "score": 0, "issues": ["ATR geçersiz"]}
    score = 0
    issues = []
    pullback_move = abs(recent["close"].iloc[-1] - recent["close"].iloc[0])
    if pullback_move <= atr_val * 2.5:
        score += 30
    elif pullback_move <= atr_val * 3.25:
        score += 15
    else:
        issues.append("aşırı sert karşı hareket")
    vol_recent = recent["volume"].tail(3).mean()
    vol_prior = recent["volume"].head(3).mean()
    if vol_prior > 0 and vol_recent <= vol_prior * 1.15:
        score += 25
    elif vol_prior > 0 and vol_recent <= vol_prior * 1.35:
        score += 12
    else:
        issues.append("pullback hacmi belirgin artıyor")
    bodies = (recent["close"] - recent["open"]).abs()
    if bodies.iloc[-1] <= bodies.iloc[:3].mean() * 1.10:
        score += 20
    else:
        issues.append("karşı mum gövdeleri güçlü")
    adx_now = safe_float(x["adx"])
    adx_prev = safe_float(df.iloc[-4]["adx"]) if len(df) > 4 else adx_now
    if adx_now <= adx_prev * 1.20:
        score += 25
    elif adx_now <= adx_prev * 1.35:
        score += 12
    else:
        issues.append("karşı yönde ADX hızlanıyor")
    healthy = score >= 48 and len(issues) <= 1
    return {"healthy": healthy, "score": clamp(score, 0, 100), "issues": issues}

def _pct_diff(a, b):
    base = max(abs(a), abs(b), 1e-12)
    return abs(a - b) / base

def detect_chart_patterns(df, direction):
    result = {"detected": False, "type": None, "direction": direction, "score": 0, "break_level": None, "details": {}}
    if df is None or len(df) < 45:
        return result
    x = df.iloc[-1]
    atr_val = safe_float(x.get("atr"))
    close = safe_float(x.get("close"))
    if atr_val <= 0 or close <= 0:
        return result
    work = df.tail(PATTERN_LOOKBACK).reset_index(drop=True)
    highs, lows = find_swing_points(work, window=2)

    if direction == "short" and len(highs) >= 3:
        a, b, c = highs[-3:]
        if b[1] > a[1] and b[1] > c[1] and _pct_diff(a[1], c[1]) <= PATTERN_MAX_SHOULDER_DIFF:
            low_candidates = [(i, v) for i, v in lows if a[0] < i < c[0]]
            if len(low_candidates) >= 2:
                n1 = min(low_candidates, key=lambda z: z[0])
                n2 = max(low_candidates, key=lambda z: z[0])
                neckline = (n1[1] + n2[1]) / 2
                if close <= neckline * 1.003:
                    score = 70
                    if close < neckline:
                        score += 10
                    result.update({"detected": True, "type": "OBO", "score": clamp(score, 0, 100), "break_level": neckline, "details": {"left_shoulder": a[1], "head": b[1], "right_shoulder": c[1], "neckline": neckline}})

    if not result["detected"] and direction == "long" and len(lows) >= 3:
        a, b, c = lows[-3:]
        if b[1] < a[1] and b[1] < c[1] and _pct_diff(a[1], c[1]) <= PATTERN_MAX_SHOULDER_DIFF:
            high_candidates = [(i, v) for i, v in highs if a[0] < i < c[0]]
            if len(high_candidates) >= 2:
                n1 = min(high_candidates, key=lambda z: z[0])
                n2 = max(high_candidates, key=lambda z: z[0])
                neckline = (n1[1] + n2[1]) / 2
                if close >= neckline * 0.997:
                    score = 70
                    if close > neckline:
                        score += 10
                    result.update({"detected": True, "type": "TOBO", "score": clamp(score, 0, 100), "break_level": neckline, "details": {"left_shoulder": a[1], "head": b[1], "right_shoulder": c[1], "neckline": neckline}})

    if not result["detected"] and len(work) >= 30:
        impulse = work.iloc[-30:-10]
        flag = work.iloc[-10:]
        impulse_move = (impulse["close"].iloc[-1] - impulse["close"].iloc[0]) / impulse["close"].iloc[0]
        impulse_atr = abs(impulse["close"].iloc[-1] - impulse["close"].iloc[0]) / max(atr_val, 1e-12)
        flag_high = safe_float(flag["high"].max())
        flag_low = safe_float(flag["low"].min())
        flag_range = flag_high - flag_low
        flag_start = safe_float(flag["close"].iloc[0])
        flag_end = safe_float(flag["close"].iloc[-1])
        flag_change = (flag_end - flag_start) / max(flag_start, 1e-12)
        same_direction_impulse = (direction == "long" and impulse_move > 0) or (direction == "short" and impulse_move < 0)
        counter_drift = (direction == "long" and flag_change <= 0.01) or (direction == "short" and flag_change >= -0.01)
        range_ok = flag_range <= abs(impulse["close"].iloc[-1] - impulse["close"].iloc[0]) * FLAG_MAX_RETRACE
        if same_direction_impulse and impulse_atr >= FLAG_MIN_IMPULSE_ATR and counter_drift and range_ok:
            breakout_level = flag_high if direction == "long" else flag_low
            broken = (close > breakout_level) if direction == "long" else (close < breakout_level)
            score = 62 + (12 if broken else 0)
            result.update({"detected": True, "type": "BULL_FLAG" if direction == "long" else "BEAR_FLAG", "score": clamp(score, 0, 100), "break_level": breakout_level, "details": {"impulse_atr": impulse_atr, "flag_range": flag_range, "broken": broken}})

    return result

def confirm_pattern_breakout(df, pattern, direction):
    if not pattern or not pattern.get("detected") or pattern.get("break_level") is None:
        return {"confirmed": False, "type": None}
    if df is None or len(df) < 5:
        return {"confirmed": False, "type": None}
    last = df.iloc[-1]
    level = safe_float(pattern["break_level"])
    close = safe_float(last["close"])
    open_ = safe_float(last["open"])
    volume_ratio = safe_float(last.get("volume_ratio"), 1.0)
    body_ratio = safe_float(last.get("body_ratio"), 0.0)
    atr_val = safe_float(last.get("atr"))
    if level <= 0 or atr_val <= 0:
        return {"confirmed": False, "type": None}
    if direction == "long":
        body_break = close > level and close > open_
        meaningful = close - level >= atr_val * 0.08
    else:
        body_break = close < level and close < open_
        meaningful = level - close >= atr_val * 0.08
    volume_ok = volume_ratio >= PATTERN_VOLUME_RATIO
    body_ok = body_ratio >= 0.25
    if body_break and meaningful and volume_ok and body_ok:
        return {"confirmed": True, "type": f"{pattern['type']}_BREAKOUT"}
    return {"confirmed": False, "type": None}


# ============================================================
# MOMENTUM & BREAKOUT ENGINES
# ============================================================

def detect_momentum_reversal(df, direction):
    if df is None or len(df) < 20:
        return False
    last4 = df.tail(4)
    rsi_vals = last4["rsi"].values
    hist_vals = last4["macd_hist"].values
    if direction == "long":
        rsi_turning = rsi_vals[-1] > rsi_vals[-2] and rsi_vals[-2] >= rsi_vals[-3] - 1
        hist_improving = hist_vals[-1] > hist_vals[-2]
        return bool(rsi_turning and hist_improving)
    else:
        rsi_turning = rsi_vals[-1] < rsi_vals[-2] and rsi_vals[-2] <= rsi_vals[-3] + 1
        hist_weakening = hist_vals[-1] < hist_vals[-2]
        return bool(rsi_turning and hist_weakening)

def calculate_momentum_acceleration(df, direction):
    if df is None or len(df) < 6:
        return {"accelerating": False, "score": 0}
    hist = df["macd_hist"].tail(4).values
    rsi_vals = df["rsi"].tail(4).values
    slopes = np.diff(hist)
    if direction == "long":
        consistent = sum(1 for s in slopes if s > 0)
        rsi_slope_positive = rsi_vals[-1] > rsi_vals[0]
    else:
        consistent = sum(1 for s in slopes if s < 0)
        rsi_slope_positive = rsi_vals[-1] < rsi_vals[0]
    score = (consistent / len(slopes)) * 60
    if rsi_slope_positive:
        score += 25
    if len(slopes) >= 2:
        accel_of_accel = slopes[-1] - slopes[-2]
        if (direction == "long" and accel_of_accel > 0) or (direction == "short" and accel_of_accel < 0):
            score += 15
    score = clamp(score, 0, 100)
    return {"accelerating": score >= 60, "score": score}

def detect_micro_structure_break(df, direction):
    if df is None or len(df) < 30:
        return {"broken": False, "level": None}
    swing_highs, swing_lows = find_swing_points(df, window=2)
    last_close = df["close"].iloc[-1]
    if direction == "long":
        if len(swing_highs) < 2:
            return {"broken": False, "level": None}
        (i1, h1), (i2, h2) = swing_highs[-2:]
        if h2 < h1:
            if last_close > h1:
                return {"broken": True, "level": h1}
        return {"broken": False, "level": h1}
    else:
        if len(swing_lows) < 2:
            return {"broken": False, "level": None}
        (i1, l1), (i2, l2) = swing_lows[-2:]
        if l2 > l1:
            if last_close < l1:
                return {"broken": True, "level": l1}
        return {"broken": False, "level": l1}

def confirm_breakout(df, level, direction):
    if df is None or level is None or len(df) < 5:
        return {"confirmed": False, "type": None}
    last = df.iloc[-1]
    close = safe_float(last["close"])
    open_ = safe_float(last["open"])
    atr_val = safe_float(last["atr"])
    volume_ratio = safe_float(last["volume_ratio"], 1)
    body_ratio = safe_float(last.get("body_ratio"), 0)
    if atr_val <= 0:
        return {"confirmed": False, "type": None}
    if direction == "long":
        body_breaks = close > level and open_ < close
        meaningful = (close - level) >= atr_val * 0.15
    else:
        body_breaks = close < level and open_ > close
        meaningful = (level - close) >= atr_val * 0.15
    volume_ok = volume_ratio >= MIN_BREAKOUT_VOLUME_RATIO
    body_ok = body_ratio >= 0.35
    reasons = []
    if not body_breaks:
        reasons.append("body")
        diag_inc("breakout_body")
    if not meaningful:
        reasons.append("atr")
        diag_inc("breakout_atr")
    if not volume_ok:
        reasons.append("volume")
        diag_inc("breakout_volume")
    if not body_ok:
        reasons.append("close_body_ratio")
        diag_inc("breakout_close")
    if body_breaks and meaningful and volume_ok and body_ok:
        diag_inc("breakout_confirmed")
        return {"confirmed": True, "type": "aggressive_breakout", "reasons": []}
    return {"confirmed": False, "type": None, "reasons": reasons}

def confirm_breakout_retest(df, level, direction):
    if df is None or level is None or len(df) < 8:
        return {"confirmed": False, "type": None}
    last3 = df.tail(3)
    last = df.iloc[-1]
    volume_ratio = safe_float(last["volume_ratio"], 1)
    macd_hist = safe_float(last["macd_hist"])
    prev_macd_hist = safe_float(df.iloc[-2]["macd_hist"])
    if direction == "long":
        retested = last3["low"].min() <= level * 1.005
        held = last3["close"].iloc[-1] > level
        momentum_resumed = macd_hist > prev_macd_hist
    else:
        retested = last3["high"].max() >= level * 0.995
        held = last3["close"].iloc[-1] < level
        momentum_resumed = macd_hist < prev_macd_hist
    if retested and held and momentum_resumed and volume_ratio >= 1.0:
        return {"confirmed": True, "type": "confirmed_retest"}
    return {"confirmed": False, "type": None}

def is_entry_chasing(df, direction, break_level):
    if df is None or break_level is None:
        return True
    last = df.iloc[-1]
    close = safe_float(last["close"])
    atr_val = safe_float(last["atr"])
    if atr_val <= 0:
        return True
    if direction == "long":
        distance_atr = (close - break_level) / atr_val
    else:
        distance_atr = (break_level - close) / atr_val
    return distance_atr > MAX_ENTRY_CHASE_ATR

def calculate_expected_move(df, direction, entry_price, leverage):
    if df is None or entry_price <= 0 or leverage <= 0:
        return {"sufficient": False, "required_move_pct": None, "nearest_level": None}
    required_move_pct = MIN_TARGET_ROI / leverage
    swing_highs, swing_lows = find_swing_points(df, window=3)
    if direction == "long":
        levels_above = [h for _, h in swing_highs if h > entry_price]
        nearest_level = min(levels_above) if levels_above else None
        available_move_pct = (nearest_level - entry_price) / entry_price * 100 if nearest_level else required_move_pct * 3
    else:
        levels_below = [l for _, l in swing_lows if l < entry_price]
        nearest_level = max(levels_below) if levels_below else None
        available_move_pct = (entry_price - nearest_level) / entry_price * 100 if nearest_level else required_move_pct * 3
    sufficient = available_move_pct >= required_move_pct
    return {"sufficient": sufficient, "required_move_pct": required_move_pct, "available_move_pct": available_move_pct, "nearest_level": nearest_level}

def momentum_analysis(df):
    if df is None or len(df) < 50:
        return {"direction": "neutral", "strength": 0}
    x = df.iloc[-1]
    p = df.iloc[-2]
    rsi_val = safe_float(x["rsi"])
    macd_hist = safe_float(x["macd_hist"])
    prev_hist = safe_float(p["macd_hist"])
    roc_val = safe_float(x["roc"])
    volume_ratio = safe_float(x["volume_ratio"], 1)
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
        return {"direction": "long", "strength": clamp(long_points, 0, 100)}
    if short_points > long_points:
        return {"direction": "short", "strength": clamp(short_points, 0, 100)}
    return {"direction": "neutral", "strength": 0}


# ============================================================
# SCORING & PIPELINE
# ============================================================

def calculate_setup_score(direction, trend4h, trend1h, pullback_quality, atr_pct, funding, volume_ratio, market_regime_ok):
    score = 0
    breakdown = {}
    regime_pts = 15 if market_regime_ok else 5
    score += regime_pts
    breakdown["market_regime"] = regime_pts
    trend4h_pts = (trend4h["strength"] / 100) * 15 if trend4h["direction"] == direction else 0
    score += trend4h_pts
    breakdown["4h_trend"] = round(trend4h_pts, 1)
    trend1h_pts = (trend1h["strength"] / 100) * 15 if trend1h["direction"] == direction else 0
    score += trend1h_pts
    breakdown["1h_trend"] = round(trend1h_pts, 1)
    structure_pts = 15 if pullback_quality.get("healthy") else 5
    score += structure_pts
    breakdown["15m_structure"] = structure_pts
    pullback_pts = (pullback_quality.get("score", 0) / 100) * 20
    score += pullback_pts
    breakdown["pullback_quality"] = round(pullback_pts, 1)
    volume_pts = clamp((volume_ratio - 0.8) * 25, 0, 10)
    score += volume_pts
    breakdown["volume_env"] = round(volume_pts, 1)
    volat_pts = 5 if 0.3 <= atr_pct <= 4.0 else (1 if atr_pct < 0.3 else 2)
    score += volat_pts
    breakdown["volatility"] = volat_pts
    funding_pts = 5 if abs(funding) < FUNDING_SKIP_THRESHOLD * 0.5 else 2
    score += funding_pts
    breakdown["funding"] = funding_pts
    return round(clamp(score, 0, 100), 2), breakdown

def evaluate_early_trigger(df5, direction, level):
    try:
        current = df5.iloc[-1]
        atr = safe_float(current.get("atr"))
        price = safe_float(current.get("close"))
        if atr <= 0 or price <= 0 or level is None:
            return {"eligible": False, "score": 0, "confirmations": 0, "distance_atr": None, "reasons": []}
        distance_atr = abs(price - level) / atr
        if distance_atr > EARLY_TRIGGER_MAX_ATR:
            return {"eligible": False, "score": 0, "confirmations": 0, "distance_atr": distance_atr, "reasons": ["yapıdan uzak"]}
        momentum = calculate_momentum_acceleration(df5, direction)
        rsi = df5["rsi"].tail(4).values if "rsi" in df5.columns else []
        rsi_turn = bool(len(rsi) >= 2 and ((rsi[-1] > rsi[-2]) if direction == "long" else (rsi[-1] < rsi[-2])))
        ema9 = safe_float(current.get("ema9_slope"))
        ema21 = safe_float(current.get("ema21_slope"))
        ema_ok = ((ema9 > 0 and ema21 >= -abs(ema9) * 0.5) if direction == "long" else (ema9 < 0 and ema21 <= abs(ema9) * 0.5))
        body = safe_float(current.get("close")) - safe_float(current.get("open"))
        candle_ok = body > 0 if direction == "long" else body < 0
        checks = {"momentum": bool(momentum.get("accelerating")), "rsi": rsi_turn, "ema": ema_ok, "candle": candle_ok}
        confirmations = sum(checks.values())
        score = (30 if checks["momentum"] else 0) + (22 if checks["rsi"] else 0) + (16 if checks["ema"] else 0) + (12 if checks["candle"] else 0) + max(0, 20 - (distance_atr / EARLY_TRIGGER_MAX_ATR) * 20)
        eligible = confirmations >= EARLY_TRIGGER_MIN_CONFIRMATIONS and score >= EARLY_TRIGGER_MIN_SCORE
        reasons = [k for k, v in checks.items() if v]
        return {"eligible": eligible, "score": round(clamp(score, 0, 100), 2), "confirmations": confirmations, "distance_atr": distance_atr, "reasons": reasons, "momentum": momentum, "rsi_turn": rsi_turn, "ema_ok": ema_ok, "candle_ok": candle_ok}
    except Exception as e:
        logger.debug("early trigger değerlendirme hatası: %s", e)
        return {"eligible": False, "score": 0, "confirmations": 0, "distance_atr": None, "reasons": []}

def calculate_trigger_score(momentum_accel, rsi_turning, ema_slope_ok, structure_break, breakout_result, atr_position_ok):
    score = 0
    breakdown = {}
    confirmations = 0
    macd_pts = (momentum_accel.get("score", 0) / 100) * 20
    score += macd_pts
    breakdown["macd_acceleration"] = round(macd_pts, 1)
    if momentum_accel.get("accelerating"):
        confirmations += 1
    rsi_pts = 15 if rsi_turning else 0
    score += rsi_pts
    breakdown["rsi_turn"] = rsi_pts
    if rsi_turning:
        confirmations += 1
    ema_pts = 10 if ema_slope_ok else 0
    score += ema_pts
    breakdown["ema_slope"] = ema_pts
    structure_pts = 25 if structure_break.get("broken") else 0
    score += structure_pts
    breakdown["structure_break"] = structure_pts
    if structure_break.get("broken"):
        confirmations += 1
    breakout_pts = 15 if breakout_result.get("confirmed") else 0
    score += breakout_pts
    breakdown["breakout_volume"] = breakout_pts
    candle_pts = 10 if breakout_result.get("confirmed") else 0
    score += candle_pts
    breakdown["candle_close"] = candle_pts
    atr_pos_pts = 5 if atr_position_ok else 0
    score += atr_pos_pts
    breakdown["atr_position"] = atr_pos_pts
    return round(clamp(score, 0, 100), 2), breakdown, confirmations

def analyze_high_conviction(symbol, btc_regime=None):
    tf_data = {}
    for tf in ["4h", "1h", "15m", "5m"]:
        df = fetch_ohlcv_closed(symbol, tf)
        if df is None:
            diag_reject(symbol, "data_missing", f"timeframe={tf}")
            return None
        tf_data[tf] = enrich_dataframe(df)

    if detect_anomaly(tf_data["15m"]):
        diag_reject(symbol, "anomaly", "15m anomaly")
        return None

    trend4h = timeframe_trend(tf_data["4h"])
    trend1h = timeframe_trend(tf_data["1h"])
    mom1h = momentum_analysis(tf_data["1h"])

    if trend1h["direction"] == "neutral":
        diag_reject(symbol, "trend_neutral", f"1h={trend1h['strength']:.1f}")
        return None
    if trend1h["strength"] < 30:
        diag_reject(symbol, "trend_weak", f"1h={trend1h['strength']:.1f}")
        return None
    diag_stage(symbol, "TREND_OK", f"1h={trend1h['direction']}/{trend1h['strength']:.1f} 4h={trend4h['direction']}/{trend4h['strength']:.1f}")
    direction = trend1h["direction"]
    if mom1h["direction"] != "neutral" and mom1h["direction"] != direction and mom1h["strength"] >= 55:
        diag_reject(symbol, "momentum_conflict", f"1h={mom1h['direction']}/{mom1h['strength']:.1f}")
        return None

    setup_type = "continuation"
    if trend4h["direction"] != "neutral" and trend4h["direction"] != direction:
        if trend4h["strength"] >= 68:
            diag_reject(symbol, "htf_conflict", f"4h={trend4h['direction']}/{trend4h['strength']:.1f}")
            return None
        setup_type = "reversal"

    btc_risk_factor = 0.85
    btc_alignment = "neutral"
    if btc_regime and symbol != BTC_SYMBOL:
        if btc_regime["direction"] == direction and btc_regime["strength"] >= BTC_REGIME_MIN_STRENGTH:
            btc_risk_factor = 1.00
            btc_alignment = "aligned"
        elif btc_regime["direction"] != "neutral" and btc_regime["strength"] >= BTC_REGIME_MIN_STRENGTH:
            btc_risk_factor = 0.65
            btc_alignment = "opposing"
            diag_inc("btc_risk_adjusted")
    market_regime_ok = True
    diag_stage(symbol, "BTC_CONTEXT", f"{btc_alignment} risk={btc_risk_factor:.2f}")

    funding = get_funding(symbol)
    if abs(funding) >= FUNDING_SKIP_THRESHOLD:
        diag_reject(symbol, "funding", f"funding={funding:.6f}")
        return None

    pattern = detect_chart_patterns(tf_data["15m"], direction)
    diag_inc("pattern_scanned")
    if pattern.get("detected"):
        diag_inc("pattern_candidate")
        diag_inc("pattern_valid")
        diag_inc(pattern_diag_key(pattern.get("type")))
        if pattern.get("details", {}).get("broken"):
            diag_inc("pattern_breakout")
        diag_stage(symbol, "PATTERN_OK", f"type={pattern.get('type')} score={safe_float(pattern.get('score')):.1f} break={pattern.get('break_level')}")
    else:
        diag_stage(symbol, "PATTERN_NONE")

    pullback_detected = detect_pullback(tf_data["15m"], direction)
    pullback_quality = assess_pullback_quality(tf_data["15m"], direction) if pullback_detected else {"healthy": False, "score": 0, "issues": []}
    pattern_setup = bool(pattern.get("detected"))
    valid_setup = pullback_quality.get("healthy") or pattern_setup
    if not valid_setup:
        diag_inc("pullback_unhealthy")
        diag_reject(symbol, "no_setup", f"pullback_score={pullback_quality.get('score',0)} issues={pullback_quality.get('issues',[])}")
        return None
    diag_stage(symbol, "SETUP_OK", f"pullback={pullback_quality.get('score',0):.1f} pattern={pattern.get('type')}")

    momentum_reversal_15m = detect_momentum_reversal(tf_data["15m"], direction)
    if not momentum_reversal_15m and not pattern_setup:
        diag_reject(symbol, "no_15m_reversal")
        return None
    diag_stage(symbol, "WATCH_OK", f"15m_reversal={momentum_reversal_15m}")

    momentum_accel = calculate_momentum_acceleration(tf_data["5m"], direction)
    rsi_vals_5m = tf_data["5m"]["rsi"].tail(4).values
    rsi_turning = (rsi_vals_5m[-1] > rsi_vals_5m[0]) if direction == "long" else (rsi_vals_5m[-1] < rsi_vals_5m[0])

    ema9_slope = safe_float(tf_data["5m"].iloc[-1]["ema9_slope"])
    ema21_slope = safe_float(tf_data["5m"].iloc[-1]["ema21_slope"])
    ema_slope_ok = ((ema9_slope > 0 and ema21_slope >= -abs(ema9_slope) * 0.5) if direction == "long" else (ema9_slope < 0 and ema21_slope <= abs(ema9_slope) * 0.5))

    structure_break = detect_micro_structure_break(tf_data["5m"], direction)
    pattern_breakout = confirm_pattern_breakout(tf_data["5m"], pattern, direction)

    if structure_break["broken"]:
        level = structure_break["level"]
        breakout_result = confirm_breakout(tf_data["5m"], level, direction)
        if not breakout_result["confirmed"]:
            breakout_result = confirm_breakout_retest(tf_data["5m"], level, direction)
        breakout_type = breakout_result.get("type")
    elif pattern_breakout["confirmed"]:
        level = pattern["break_level"]
        breakout_result = pattern_breakout
        breakout_type = pattern_breakout.get("type")
    else:
        early = evaluate_early_trigger(tf_data["5m"], direction, pattern.get("break_level") if pattern_setup and pattern.get("break_level") else structure_break.get("level"))
        if early.get("eligible"):
            level = pattern.get("break_level") if pattern_setup and pattern.get("break_level") else structure_break.get("level")
            breakout_result = {"confirmed": True, "type": "early_momentum", "reasons": early.get("reasons", [])}
            breakout_type = "early_momentum"
            diag_stage(symbol, "EARLY_TRIGGER_OK", f"score={early['score']:.1f} confirm={early['confirmations']} distance_atr={early.get('distance_atr', 0):.2f}")
        else:
            diag_inc("no_breakout")
            diag_reject(symbol, "no_breakout", "micro_structure/pattern breakout yok; early trigger da yetersiz")
            return None

    if breakout_result.get("confirmed"):
        diag_inc("breakout_confirmed")
    else:
        diag_inc("no_breakout")
        diag_reject(symbol, "no_breakout", f"type={breakout_type}")
        return None

    current_5m = tf_data["5m"].iloc[-1]
    atr_val = safe_float(current_5m["atr"])
    atr_pct = clamp(safe_float(current_5m["atr_pct"]), 0.15, 6.0)
    price = safe_float(current_5m["close"])
    volume_ratio = safe_float(current_5m["volume_ratio"], 1)

    if level is None or atr_val <= 0:
        return None
    distance_atr = abs(price - level) / atr_val
    atr_position_ok = distance_atr <= MAX_ENTRY_CHASE_ATR
    if not atr_position_ok:
        diag_reject(symbol, "entry_chase", f"distance_atr={distance_atr:.2f}")
        return None
    diag_stage(symbol, "CHASE_OK", f"distance_atr={distance_atr:.2f}")

    setup_score, setup_breakdown = calculate_setup_score(
        direction, trend4h, trend1h, pullback_quality, atr_pct, funding, volume_ratio, market_regime_ok
    )
    if pattern_setup:
        setup_score = max(setup_score, pattern.get("score", 0))
        setup_breakdown["chart_pattern"] = pattern.get("score", 0)

    min_setup = PATTERN_MIN_SCORE if pattern_setup else (MIN_SETUP_SCORE + (3 if setup_type == "reversal" else 0))
    if setup_score < min_setup:
        diag_reject(symbol, "setup_score_low", f"{setup_score:.1f}<{min_setup:.1f}")
        return None
    diag_stage(symbol, "SETUP_SCORE_OK", f"{setup_score:.1f}/{min_setup:.1f}")

    trigger_score, trigger_breakdown, confirmations = calculate_trigger_score(
        momentum_accel, rsi_turning, ema_slope_ok,
        {"broken": structure_break["broken"] or pattern_breakout["confirmed"]},
        breakout_result, atr_position_ok
    )
    if pattern_setup:
        trigger_score += 8
        trigger_breakdown["chart_pattern_bonus"] = 8
        trigger_score = clamp(trigger_score, 0, 100)

    min_trigger = PATTERN_MIN_TRIGGER_SCORE if pattern_setup else (MIN_TRIGGER_SCORE + (3 if setup_type == "reversal" else 0))
    if trigger_score < min_trigger:
        diag_reject(symbol, "trigger_score_low", f"{trigger_score:.1f}<{min_trigger:.1f}")
        return None
    diag_stage(symbol, "TRIGGER_SCORE_OK", f"{trigger_score:.1f}/{min_trigger:.1f}")

    if pattern_setup:
        if confirmations < 1 or not (momentum_accel.get("accelerating") or rsi_turning):
            diag_reject(symbol, "trigger_confirmations", f"confirmations={confirmations}")
            return None
    elif confirmations < 1:
        diag_reject(symbol, "trigger_confirmations", f"confirmations={confirmations}")
        return None
    diag_stage(symbol, "FIRE_OK", f"confirmations={confirmations}")

    leverage = calculate_leverage(setup_score, trigger_score, atr_pct)
    expected_move = calculate_expected_move(tf_data["1h"], direction, price, leverage)
    if not expected_move["sufficient"]:
        diag_inc("expected_move")
        diag_reject(symbol, "expected_move", f"available={safe_float(expected_move.get('available_move_pct')):.2f}%")
        return None
    diag_stage(symbol, "EXPECTED_MOVE_OK", f"available={expected_move.get('available_move_pct'):.2f}%")

    return {
        "symbol": symbol,
        "direction": direction,
        "setup_type": setup_type,
        "setup_score": setup_score,
        "trigger_score": trigger_score,
        "setup_breakdown": setup_breakdown,
        "trigger_breakdown": trigger_breakdown,
        "confirmations": confirmations,
        "breakout_type": breakout_type,
        "chart_pattern": pattern.get("type") if pattern_setup else None,
        "pattern_details": pattern.get("details", {}) if pattern_setup else {},
        "price": price,
        "atr": atr_val,
        "atr_pct": atr_pct,
        "structure_level": level,
        "leverage": leverage,
        "funding": funding,
        "expected_move": expected_move,
        "pullback_quality": pullback_quality,
        "trend4h": trend4h,
        "trend1h": trend1h,
        "data_1h": tf_data["1h"],
        "data_5m": tf_data["5m"],
    }


# ============================================================
# EXECUTION & POSITION MANAGEMENT
# ============================================================

def calculate_leverage(setup_score, trigger_score, atr_pct):
    combined = (setup_score + trigger_score) / 2
    leverage = MIN_LEVERAGE + 1
    if combined >= 85:
        leverage += 2
    elif combined >= 78:
        leverage += 1
    if atr_pct > 4:
        leverage -= 1
    if atr_pct < 1.0:
        leverage += 1
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

def calculate_dynamic_atr_stop(df, direction, entry_price, leverage, structure_level):
    x = df.iloc[-1]
    atr_val = safe_float(x["atr"])
    if atr_val <= 0 or entry_price <= 0:
        atr_val = entry_price * 0.01
    atr_distance = atr_val * INITIAL_STOP_ATR_MULTIPLIER
    if direction == "long":
        structure_distance = max(entry_price - structure_level, atr_val * 0.5) if structure_level else atr_distance
    else:
        structure_distance = max(structure_level - entry_price, atr_val * 0.5) if structure_level else atr_distance
    natural_distance = max(atr_distance, structure_distance)
    max_loss_roi = MIN_TARGET_ROI * MAX_LOSS_TO_TARGET_RATIO
    max_risk_distance = (max_loss_roi / 100 / leverage) * entry_price
    final_distance = min(natural_distance, max_risk_distance)
    final_distance = max(final_distance, entry_price * 0.0015)
    if direction == "long":
        stop_price = entry_price - final_distance
    else:
        stop_price = entry_price + final_distance
    return {
        "stop_price": stop_price,
        "distance": final_distance,
        "atr_distance": atr_distance,
        "structure_distance": structure_distance,
        "max_risk_distance": max_risk_distance,
    }

def get_recent_returns(symbol, timeframe="1h", n=30):
    try:
        df = fetch_ohlcv_closed(symbol, timeframe, n + 5)
        if df is None or len(df) < n:
            return None
        closes = df["close"].tail(n).values
        returns = np.diff(closes) / closes[:-1]
        return returns
    except Exception:
        return None

def is_correlated_with_open_positions(symbol):
    with state_lock:
        open_symbols = [p["symbol"] for p in local_positions.values()]
    if not open_symbols:
        return False
    candidate_returns = get_recent_returns(symbol)
    if candidate_returns is None:
        return False
    for open_symbol in open_symbols:
        if normalize_symbol(open_symbol) == normalize_symbol(symbol):
            continue
        other_returns = get_recent_returns(open_symbol)
        if other_returns is None or len(other_returns) != len(candidate_returns):
            continue
        try:
            corr = np.corrcoef(candidate_returns, other_returns)[0, 1]
        except Exception:
            continue
        if not math.isnan(corr) and corr >= CORRELATION_MAX_ALLOWED:
            logger.info("[KORELASYON] %s açık pozisyon ile yüksek korelasyonlu (%.2f) — elendi.", symbol, corr)
            return True
    return False

def is_on_cooldown(symbol):
    t = cooldowns.get(normalize_symbol(symbol))
    if not t:
        return False
    return (now_ms() - t) < COOLDOWN_MS

def set_cooldown(symbol):
    cooldowns[normalize_symbol(symbol)] = now_ms()

def get_local_positions():
    with state_lock:
        return {k: dict(v) for k, v in local_positions.items()}

def local_position_count():
    with state_lock:
        return len(local_positions)

def has_local_symbol(symbol):
    normalized = normalize_symbol(symbol)
    with state_lock:
        return any(normalize_symbol(p["symbol"]) == normalized for p in local_positions.values())

class PositionFetchError(Exception):
    pass

def fetch_real_positions():
    try:
        positions = safe_call(exchange.fetch_positions)
    except Exception as e:
        raise PositionFetchError(str(e))
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

def fetch_real_positions_safe():
    try:
        return fetch_real_positions()
    except PositionFetchError:
        return None

def sync_real_positions():
    if DRY_RUN:
        return
    try:
        real = fetch_real_positions()
    except PositionFetchError as e:
        logger.warning("Pozisyon senkronizasyonu atlandı: %s", e)
        return
    with state_lock:
        real_symbols = {normalize_symbol(p["symbol"]) for p in real}
        remove = [key for key, local in local_positions.items() if normalize_symbol(local["symbol"]) not in real_symbols]
        for key in remove:
            logger.warning("[SENKRON] %s borsada kapatılmış, local state'ten temizleniyor.", local_positions[key]["symbol"])
            local_positions.pop(key, None)

def recover_positions_from_exchange():
    if DRY_RUN:
        return
    try:
        real = fetch_real_positions()
    except PositionFetchError as e:
        logger.error("[RECOVERY] Açık pozisyonlar okunamadı: %s", e)
        return
    if not real:
        return
    with state_lock:
        for p in real:
            symbol = p["symbol"]
            side = "long" if p["side"] == "long" else "short"
            key = f"hc:{normalize_symbol(symbol)}"
            if key in local_positions:
                continue
            entry_price = p["entryPrice"]
            leverage = p["leverage"] if p["leverage"] > 0 else MIN_LEVERAGE
            local_positions[key] = {
                "key": key,
                "symbol": symbol,
                "mode": "hc",
                "side": side,
                "entry_price": entry_price,
                "amount": abs(p["contracts"]),
                "margin": MARGIN_PER_TRADE,
                "leverage": leverage,
                "target_roi": MIN_TARGET_ROI,
                "initial_stop_price": None,
                "current_stop_price": None,
                "profit_lock_active": False,
                "peak_price": entry_price,
                "trough_price": entry_price,
                "opened_at": now_ms(),
                "last_monitor": now_ms(),
                "last_trend_check": 0,
                "stop_order_id": None,
                "recovered": True,
            }
            logger.warning("[RECOVERY] %s %s pozisyonu geri yüklendi.", side.upper(), symbol)

def can_open_more():
    return local_position_count() < MAX_OPEN_POSITIONS

def fetch_current_price(symbol):
    ticker = safe_call(exchange.fetch_ticker, symbol)
    return safe_float(ticker.get("last"))

def calculate_amount(margin, leverage, price):
    notional = margin * leverage
    return notional / price

def place_stop_market_order(symbol, position_side, amount, stop_price):
    try:
        close_side = "sell" if position_side == "long" else "buy"
        order = safe_call(
            exchange.create_order,
            symbol, "STOP_MARKET", close_side, amount, None,
            {"stopPrice": format_price(symbol, stop_price), "reduceOnly": True, "positionSide": "BOTH"}
        )
        return order.get("id")
    except Exception as e:
        logger.error("%s FAILSAFE STOP emri yerleştirilemedi: %s", symbol, e)
        return None

def write_trade_journal(entry):
    logger.warning("[JOURNAL] %s", json.dumps(entry, default=str, ensure_ascii=False))
    try:
        with open(TRADE_JOURNAL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.info("Trade journal yazılamadı: %s", e)

def estimate_net_roi(gross_roi, leverage):
    fee_roi_cost = TAKER_FEE_PCT * 2 * leverage
    return gross_roi - fee_roi_cost

def open_position(candidate):
    symbol = candidate["symbol"]
    direction = candidate["direction"]
    if is_on_cooldown(symbol) or not can_open_more() or has_local_symbol(symbol) or is_correlated_with_open_positions(symbol):
        return False

    leverage = candidate["leverage"]
    price = candidate["price"]
    stop_info = calculate_dynamic_atr_stop(candidate["data_5m"], direction, price, leverage, candidate["structure_level"])
    key = f"hc:{normalize_symbol(symbol)}"

    if DRY_RUN:
        amount = calculate_amount(MARGIN_PER_TRADE, leverage, price)
        entry_price = price
        stop_order_id = None
    else:
        set_isolated_and_leverage(symbol, leverage)
        fresh_price = fetch_current_price(symbol)
        if fresh_price <= 0:
            return False
        amount = float(format_amount(symbol, calculate_amount(MARGIN_PER_TRADE, leverage, fresh_price)))
        if amount <= 0:
            return False
        side = "buy" if direction == "long" else "sell"
        order = safe_call(exchange.create_order, symbol, "market", side, amount, None, {"positionSide": "BOTH"})
        entry_price = safe_float(order.get("average"), fresh_price)
        stop_info = calculate_dynamic_atr_stop(candidate["data_5m"], direction, entry_price, leverage, candidate["structure_level"])
        native_stop_distance = min(stop_info["distance"] * HARD_STOP_BUFFER, stop_info["max_risk_distance"])
        native_stop_price = entry_price - native_stop_distance if direction == "long" else entry_price + native_stop_distance
        stop_order_id = place_stop_market_order(symbol, direction, amount, native_stop_price)

    with state_lock:
        if key in local_positions or len(local_positions) >= MAX_OPEN_POSITIONS:
            return False
        local_positions[key] = {
            "key": key,
            "symbol": symbol,
            "mode": "hc",
            "side": direction,
            "entry_price": entry_price,
            "amount": amount,
            "margin": MARGIN_PER_TRADE,
            "leverage": leverage,
            "target_roi": MIN_TARGET_ROI,
            "initial_stop_price": stop_info["stop_price"],
            "current_stop_price": stop_info["stop_price"],
            "profit_lock_active": False,
            "peak_price": entry_price,
            "trough_price": entry_price,
            "opened_at": now_ms(),
            "last_monitor": now_ms(),
            "last_trend_check": 0,
            "stop_order_id": stop_order_id,
            "recovered": False,
            "setup_score": candidate["setup_score"],
            "trigger_score": candidate["trigger_score"],
            "setup_type": candidate["setup_type"],
            "breakout_type": candidate["breakout_type"],
            "chart_pattern": candidate.get("chart_pattern"),
            "pattern_details": candidate.get("pattern_details", {}),
        }

    set_cooldown(symbol)
    bot_stats["orders"] += 1
    logger.warning("[%s AÇILDI] %s %s | entry=%s | stop=%s | lev=%sx", "DRY RUN" if DRY_RUN else "REAL", direction.upper(), symbol, entry_price, stop_info["stop_price"], leverage)
    return True

def calculate_roi(position, price):
    entry = safe_float(position["entry_price"])
    leverage = safe_float(position["leverage"], 1)
    if entry <= 0:
        return 0.0
    price_change = (price - entry) / entry if position["side"] == "long" else (entry - price) / entry
    return price_change * leverage * 100

def live_reversal_check(symbol, direction):
    try:
        df5 = fetch_ohlcv_closed(symbol, "5m", 100)
        df15 = fetch_ohlcv_closed(symbol, "15m", 100)
        if df5 is None or df15 is None:
            return {"confirmations": 0, "details": {}}
        df5 = enrich_dataframe(df5)
        df15 = enrich_dataframe(df15)
        t5 = timeframe_trend(df5)
        t15 = timeframe_trend(df15)
        m5 = momentum_analysis(df5)
        confirmations = 0
        details = {}
        trend_reversed = (t5["direction"] not in ("neutral", direction) and t5["strength"] >= 55 and t15["direction"] not in ("neutral", direction) and t15["strength"] >= 50)
        details["trend_reversed"] = trend_reversed
        if trend_reversed: confirmations += 1
        momentum_reversed = m5["direction"] not in ("neutral", direction) and m5["strength"] >= 55
        details["momentum_reversed"] = momentum_reversed
        if momentum_reversed: confirmations += 1
        last = df5.iloc[-1]
        close = safe_float(last["close"])
        structure_failed = close < safe_float(df5.iloc[-2]["recent_low"]) if direction == "long" else close > safe_float(df5.iloc[-2]["recent_high"])
        details["structure_failed"] = structure_failed
        if structure_failed: confirmations += 1
        return {"confirmations": confirmations, "details": details}
    except Exception as e:
        logger.warning("%s canlı reversal kontrolü başarısız: %s", symbol, e)
        return {"confirmations": 0, "details": {}}

def update_profit_lock(position, roi):
    target = position["target_roi"]
    trigger = target * PROFIT_LOCK_TRIGGER_RATIO
    if roi >= trigger and not position["profit_lock_active"]:
        position["profit_lock_active"] = True
        logger.warning("[PROFIT LOCK AKTİF] %s | ROI=%.2f%%", position["symbol"], roi)

def update_atr_trailing_stop(position, price, current_atr):
    if not position["profit_lock_active"]:
        return
    distance = current_atr * TRAILING_ATR_MULTIPLIER
    if position["side"] == "long":
        if price > position["peak_price"]: position["peak_price"] = price
        candidate_stop = position["peak_price"] - distance
        floor_stop = max(position["current_stop_price"], position["entry_price"])
        position["current_stop_price"] = max(floor_stop, candidate_stop)
    else:
        if price < position["trough_price"]: position["trough_price"] = price
        candidate_stop = position["trough_price"] + distance
        ceiling_stop = min(position["current_stop_price"], position["entry_price"])
        position["current_stop_price"] = min(ceiling_stop, candidate_stop)

def should_close_position(position, price):
    roi = calculate_roi(position, price)
    position["current_roi"] = roi
    if position["side"] == "long":
        if price > position["peak_price"]: position["peak_price"] = price
    else:
        if price < position["trough_price"]: position["trough_price"] = price

    if position.get("initial_stop_price") is None:
        try:
            df5 = fetch_ohlcv_closed(position["symbol"], "5m", 60)
            if df5 is not None:
                df5 = enrich_dataframe(df5)
                stop_info = calculate_dynamic_atr_stop(df5, position["side"], position["entry_price"], position["leverage"], None)
                position["initial_stop_price"] = stop_info["stop_price"]
                position["current_stop_price"] = stop_info["stop_price"]
        except Exception:
            pass

    target = position["target_roi"]
    max_loss_roi = target * MAX_LOSS_TO_TARGET_RATIO
    if roi <= -max_loss_roi:
        return True, "HARD_FAILSAFE_STOP"

    initial_stop = position.get("initial_stop_price")
    if initial_stop and not position["profit_lock_active"]:
        if position["side"] == "long" and price <= initial_stop: return True, "INITIAL_STOP"
        if position["side"] == "short" and price >= initial_stop: return True, "INITIAL_STOP"

    update_profit_lock(position, roi)

    if (now_ms() - position.get("last_trend_check", 0)) >= LIVE_CHECK_INTERVAL_MS:
        position["last_trend_check"] = now_ms()
        try:
            df5 = fetch_ohlcv_closed(position["symbol"], "5m", 60)
            if df5 is not None:
                df5 = enrich_dataframe(df5)
                current_atr = safe_float(df5.iloc[-1]["atr"])
                if position["profit_lock_active"] and current_atr > 0:
                    update_atr_trailing_stop(position, price, current_atr)
        except Exception:
            pass

        reversal = live_reversal_check(position["symbol"], position["side"])
        if reversal["confirmations"] >= REQUIRED_REVERSAL_CONFIRMATIONS:
            return True, "TREND_MOMENTUM_STRUCTURE_REVERSAL"

    if position["profit_lock_active"]:
        stop = position["current_stop_price"]
        if position["side"] == "long" and price <= stop: return True, "PROFIT_LOCK_TRAILING_STOP"
        if position["side"] == "short" and price >= stop: return True, "PROFIT_LOCK_TRAILING_STOP"

    return False, None

def build_journal_entry(position, exit_price, reason):
    roi = calculate_roi(position, exit_price)
    net_roi = estimate_net_roi(roi, position["leverage"])
    holding_ms = now_ms() - position["opened_at"]
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": position["symbol"],
        "side": position["side"],
        "leverage": position["leverage"],
        "margin": position["margin"],
        "entry": position["entry_price"],
        "exit": exit_price,
        "gross_roi": round(roi, 3),
        "estimated_net_roi": round(net_roi, 3),
        "holding_time_sec": round(holding_ms / 1000, 1),
        "exit_reason": reason,
        "dry_run": DRY_RUN,
    }

def close_dry_position(key, reason, exit_price):
    with state_lock:
        position = local_positions.get(key)
        if not position:
            return False
        entry = dict(position)
        local_positions.pop(key, None)
    journal_entry = build_journal_entry(entry, exit_price, reason)
    write_trade_journal(journal_entry)
    bot_stats["closed_positions"] += 1
    logger.warning("[DRY KAPANDI] %s | neden=%s | ROI=%.2f%%", entry["symbol"], reason, journal_entry["gross_roi"])
    return True

def close_real_position(key, reason, exit_price):
    with state_lock:
        position = local_positions.get(key)
        if not position:
            return False
        symbol = position["symbol"]
        amount = position["amount"]
        side = position["side"]
        stop_order_id = position.get("stop_order_id")

    if stop_order_id:
        cancel_stop_order(symbol, stop_order_id)

    close_side = "sell" if side == "long" else "buy"
    try:
        safe_call(exchange.create_order, symbol, "market", close_side, amount, None, {"reduceOnly": True, "positionSide": "BOTH"})
    except Exception as e:
        logger.error("%s pozisyon kapatma emri hatası: %s", symbol, e)

    with state_lock:
        local_positions.pop(key, None)

    journal_entry = build_journal_entry(position, exit_price, reason)
    write_trade_journal(journal_entry)
    bot_stats["closed_positions"] += 1
    logger.warning("[REAL KAPANDI] %s | neden=%s", symbol, reason)
    return True


# ============================================================
# BACKGROUND LOOPS & MAIN WORKER
# ============================================================

def position_monitor_loop():
    while running:
        time.sleep(POSITION_MONITOR_INTERVAL)
        positions = get_local_positions()
        if not positions:
            continue
        sync_real_positions()
        for key, pos in positions.items():
            try:
                price = fetch_current_price_fast(pos["symbol"])
                if price <= 0:
                    continue
                should_close, reason = should_close_position(pos, price)
                if should_close:
                    if DRY_RUN:
                        close_dry_position(key, reason, price)
                    else:
                        close_real_position(key, reason, price)
            except Exception as e:
                logger.warning("Monitor hata (%s): %s", pos["symbol"], e)

def market_scan_loop():
    global last_analysis_time, last_successful_analysis
    while running:
        try:
            bot_stats["analysis_count"] += 1
            reset_cycle_diagnostics()
            btc_reg = get_btc_regime()
            gainers, losers, vol_leaders = get_top_movers()
            candidates = list(dict.fromkeys(gainers + vol_leaders))
            scanned_count = 0
            final_candidates = 0
            opened_count = 0

            for symbol in candidates:
                if not running:
                    break
                if has_local_symbol(symbol) or is_on_cooldown(symbol):
                    continue
                scanned_count += 1
                diag_inc("scanned")
                try:
                    analysis = analyze_high_conviction(symbol, btc_reg)
                    if analysis:
                        final_candidates += 1
                        diag_inc("setup_candidates")
                        if open_position(analysis):
                            opened_count += 1
                            bot_stats["signals_found"] += 1
                except Exception as e:
                    diag_inc("analysis_error")
                    logger.warning("Analiz hatası (%s): %s", symbol, e)

            log_cycle_diagnostics(scanned_count, final_candidates, opened_count)
            last_analysis_time = time.time()
            last_successful_analysis = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            logger.error("Market scan döngü hatası: %s", e)
        time.sleep(ANALYSIS_INTERVAL)

def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

def main():
    global exchange
    exchange = create_exchange()
    recover_positions_from_exchange()

    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=position_monitor_loop, daemon=True).start()
    
    logger.info("Bot eksiksiz arka plan tarama ve analiz döngüsü ile başlatılıyor...")
    market_scan_loop()

if __name__ == "__main__":
    main()
