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
# BINANCE FUTURES — HIGH-CONVICTION PULLBACK & BREAKOUT BOT V3.5 — OPTIMIZED
# ============================================================
#
# STRATEJİ ÖZETİ
# ------------------------------------------------------------
# MARKET REGIME -> TREND -> PULLBACK -> PULLBACK EXHAUSTION ->
# MOMENTUM REVERSAL -> MOMENTUM ACCELERATION -> MICRO STRUCTURE
# BREAK -> BREAKOUT CONFIRMATION -> VOLUME CONFIRMATION ->
# CANDLE CLOSE -> EXPECTED MOVE -> ENTRY
#
# SETUP_SCORE (4H/1H/15M zemin kalitesi) ve TRIGGER_SCORE (5M giriş
# zamanlaması kalitesi) ayrı ayrı hesaplanır.
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
logger = linker = logging.getLogger("HC_BOT")


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
    return jsonify({"status": "ok", "running": running, "timestamp": datetime.now(timezone.utc).isoformat()})

@app.route("/status")
def status():
    return jsonify({"dry_run": DRY_RUN, "positions": get_local_positions(), "stats": bot_stats, "cooldowns": cooldowns})


# ============================================================
# EXCHANGE & UTILS
# ============================================================

def create_exchange():
    global exchange
    if not API_KEY or not API_SECRET:
        raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET tanımlı değil.")

    exchange = ccxt.binance({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "future", "adjustForTimeDifference": True},
        "timeout": 20000,
    })
    if TESTNET:
        exchange.set_sandbox_mode(True)
    exchange.load_markets()
    logger.info("Binance bağlantısı hazır | TESTNET=%s | DRY_RUN=%s", TESTNET, DRY_RUN)
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

def now_ms():
    return int(time.time() * 1000)

def clamp(value, low, high):
    return max(low, min(high, value))

def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        res = float(value)
        return res if math.isfinite(res) else default
    except Exception:
        return default

def normalize_symbol(symbol):
    return symbol.replace("/", "").replace(":USDT", "").upper() if symbol else None

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
    try: return exchange.market(symbol)["precision"]["price"]
    except Exception: return 8

def get_amount_precision(symbol):
    try: return exchange.market(symbol)["precision"]["amount"]
    except Exception: return 6

def format_price(symbol, price):
    try: return exchange.price_to_precision(symbol, price)
    except Exception: return f"{price:.{get_price_precision(symbol)}f}"

def format_amount(symbol, amount):
    try: return exchange.amount_to_precision(symbol, amount)
    except Exception: return f"{amount:.{get_amount_precision(symbol)}f}"


# ============================================================
# INDICATORS & DATA PIPELINE
# ============================================================

def ema(series, period):
    return series.ewm(span=period, adjust=False, min_periods=period).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50)

def atr(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

def macd(series):
    fast = ema(series, 12)
    slow = ema(series, 26)
    m_line = fast - slow
    sig = ema(m_line, 9)
    return m_line, sig, m_line - sig

def adx(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    plus_dm = high.diff().where((high.diff() > -low.diff()) & (high.diff() > 0), 0)
    minus_dm = (-low.diff()).where((-low.diff() > high.diff()) & (-low.diff() > 0), 0)
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr_v = tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr_v.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr_v.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1/period, adjust=False, min_periods=period).mean().fillna(0), plus_di.fillna(0), minus_di.fillna(0)

def bollinger(series, period=20, std_mult=2):
    mid = series.rolling(period).mean()
    std = series.rolling(period).std()
    return mid, mid + std_mult * std, mid - std_mult * std

def obv(df):
    return ((np.sign(df["close"].diff()).fillna(0)) * df["volume"]).cumsum()

def roc(series, period=10):
    return series.pct_change(periods=period) * 100

def fetch_ohlcv(symbol, timeframe, limit=OHLCV_LIMIT):
    try:
        data = safe_call(exchange.fetch_ohlcv, symbol, timeframe, None, limit)
        if not data: return None
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(inplace=True)
        return df if len(df) >= 100 else None
    except Exception as e:
        logger.warning("%s %s OHLCV alınamadı: %s", symbol, timeframe, e)
        return None

def fetch_ohlcv_closed(symbol, timeframe, limit=OHLCV_LIMIT):
    df = fetch_ohlcv(symbol, timeframe, limit)
    return df.iloc[:-1].reset_index(drop=True) if df is not None and len(df) >= 101 else None

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
    df["body_ratio"] = (df["close"] - df["open"]).abs() / (df["high"] - df["low"]).replace(0, np.nan)
    return df


# ============================================================
# MARKET ANALYSIS & PATTERNS
# ============================================================

def get_top_movers():
    tickers = safe_call(exchange.fetch_tickers)
    rows = []
    for symbol, ticker in tickers.items():
        if not symbol_is_valid(symbol): continue
        try:
            last = safe_float(ticker.get("last"))
            percentage = safe_float(ticker.get("percentage"))
            quote_volume = safe_float(ticker.get("quoteVolume"))
            if last > 0 and quote_volume >= MIN_QUOTE_VOLUME_USDT:
                rows.append({"symbol": symbol, "percentage": percentage, "quoteVolume": quote_volume})
        except Exception: continue
    if not rows: return [], [], []
    df = pd.DataFrame(rows)
    return (
        df.sort_values("percentage", ascending=False).head(25)["symbol"].tolist(),
        df.sort_values("percentage", ascending=True).head(25)["symbol"].tolist(),
        df.sort_values("quoteVolume", ascending=False).head(25)["symbol"].tolist()
    )

def get_funding(symbol):
    try: return safe_float(safe_call(exchange.fetch_funding_rate, symbol).get("fundingRate"))
    except Exception: return 0.0

def get_btc_regime():
    try:
        df15, df1h = fetch_ohlcv_closed(BTC_SYMBOL, "15m", 100), fetch_ohlcv_closed(BTC_SYMBOL, "1h", 100)
        if df15 is None or df1h is None: return {"direction": "neutral", "strength": 0}
        t15, t1h = timeframe_trend(enrich_dataframe(df15)), timeframe_trend(enrich_dataframe(df1h))
        if t15["direction"] == t1h["direction"] and t15["direction"] != "neutral":
            return {"direction": t15["direction"], "strength": (t15["strength"] + t1h["strength"]) / 2}
        return {"direction": "neutral", "strength": 0}
    except Exception: return {"direction": "neutral", "strength": 0}

def detect_anomaly(df):
    if df is None or len(df) < 10: return False
    last5 = df.tail(5)
    pct = (last5["close"].iloc[-1] - last5["close"].iloc[0]) / last5["close"].iloc[0] * 100
    return abs(pct) >= 8 and safe_float(last5["volume_ratio"].mean(), 1) < 1.3

def timeframe_trend(df):
    if df is None or len(df) < 205: return {"direction": "neutral", "strength": 0}
    x = df.iloc[-1]
    price, e9, e21, e50, e200 = safe_float(x["close"]), safe_float(x["ema9"]), safe_float(x["ema21"]), safe_float(x["ema50"]), safe_float(x["ema200"])
    adx_val, plus_di, minus_di = safe_float(x["adx"]), safe_float(x.get("plus_di")), safe_float(x.get("minus_di"))
    bullish, bearish = price > e21 > e50 > e200, price < e21 < e50 < e200
    strength = (40 if (bullish or bearish) else 0) + (30 if adx_val >= ADX_STRONG else 0) + (20 if adx_val >= ADX_VERY_STRONG else 0)
    if bullish and plus_di <= minus_di: strength -= 20
    if bearish and minus_di <= plus_di: strength -= 20
    strength = clamp(strength, 0, 100)
    if bullish: return {"direction": "long", "strength": strength}
    if bearish: return {"direction": "short", "strength": strength}
    return {"direction": "neutral", "strength": 0}

def find_swing_points(df, window=3):
    highs, lows, n = df["high"].values, df["low"].values, len(df)
    return [(i, highs[i]) for i in range(window, n - window) if highs[i] == max(highs[i - window:i + window + 1])], \
           [(i, lows[i]) for i in range(window, n - window) if lows[i] == min(lows[i - window:i + window + 1])]

def detect_pullback(df, direction):
    if df is None or len(df) < 30: return False
    x, recent = df.iloc[-1], df.tail(6)
    close, ema21, ema50 = safe_float(x["close"]), safe_float(x["ema21"]), safe_float(x["ema50"])
    change = (recent["close"].iloc[-1] - recent["close"].iloc[0]) / recent["close"].iloc[0] * 100
    return (close > ema50 and (change <= 0.15 or close <= ema21 * 1.01)) if direction == "long" else \
           (close < ema50 and (change >= -0.15 or close >= ema21 * 0.99))

def assess_pullback_quality(df, direction):
    if df is None or len(df) < 30: return {"healthy": False, "score": 0, "issues": ["yetersiz veri"]}
    x, recent = df.iloc[-1], df.tail(6)
    atr_val, close = safe_float(x["atr"]), safe_float(x["close"])
    if atr_val <= 0 or close <= 0: return {"healthy": False, "score": 0, "issues": ["ATR geçersiz"]}
    score, issues = 0, []
    pullback_move = abs(recent["close"].iloc[-1] - recent["close"].iloc[0])
    score += 30 if pullback_move <= atr_val * 2.5 else (15 if pullback_move <= atr_val * 3.25 else 0)
    if pullback_move > atr_val * 3.25: issues.append("aşırı sert karşı hareket")
    vol_recent, vol_prior = recent["volume"].tail(3).mean(), recent["volume"].head(3).mean()
    score += 25 if vol_recent <= vol_prior * 1.15 else (12 if vol_recent <= vol_prior * 1.35 else 0)
    score += 20 if (recent["close"] - recent["open"]).abs().iloc[-1] <= (recent["close"] - recent["open"]).abs().iloc[:3].mean() * 1.10 else 0
    adx_now, adx_prev = safe_float(x["adx"]), safe_float(df.iloc[-4]["adx"]) if len(df) > 4 else safe_float(x["adx"])
    score += 25 if adx_now <= adx_prev * 1.20 else (12 if adx_now <= adx_prev * 1.35 else 0)
    return {"healthy": score >= 48 and len(issues) <= 1, "score": clamp(score, 0, 100), "issues": issues}

def _pct_diff(a, b):
    return abs(a - b) / max(abs(a), abs(b), 1e-12)

def detect_chart_patterns(df, direction):
    result = {"detected": False, "type": None, "direction": direction, "score": 0, "break_level": None, "details": {}}
    if df is None or len(df) < 45: return result
    x = df.iloc[-1]
    atr_val, close = safe_float(x.get("atr")), safe_float(x.get("close"))
    if atr_val <= 0 or close <= 0: return result
    work = df.tail(PATTERN_LOOKBACK).reset_index(drop=True)
    highs, lows = find_swing_points(work, window=2)

    if direction == "short" and len(highs) >= 3:
        a, b, c = highs[-3:]
        if b[1] > a[1] and b[1] > c[1] and _pct_diff(a[1], c[1]) <= PATTERN_MAX_SHOULDER_DIFF:
            lows_c = [(i, v) for i, v in lows if a[0] < i < c[0]]
            if len(lows_c) >= 2:
                neckline = (min(lows_c, key=lambda z: z[0])[1] + max(lows_c, key=lambda z: z[0])[1]) / 2
                if close <= neckline * 1.003:
                    result.update({"detected": True, "type": "OBO", "score": clamp(70 + (10 if close < neckline else 0), 0, 100), "break_level": neckline})

    if not result["detected"] and direction == "long" and len(lows) >= 3:
        a, b, c = lows[-3:]
        if b[1] < a[1] and b[1] < c[1] and _pct_diff(a[1], c[1]) <= PATTERN_MAX_SHOULDER_DIFF:
            highs_c = [(i, v) for i, v in highs if a[0] < i < c[0]]
            if len(highs_c) >= 2:
                neckline = (min(highs_c, key=lambda z: z[0])[1] + max(highs_c, key=lambda z: z[0])[1]) / 2
                if close >= neckline * 0.997:
                    result.update({"detected": True, "type": "TOBO", "score": clamp(70 + (10 if close > neckline else 0), 0, 100), "break_level": neckline})

    return result

def confirm_pattern_breakout(df, pattern, direction):
    if not pattern or not pattern.get("detected") or pattern.get("break_level") is None or df is None or len(df) < 5:
        return {"confirmed": False, "type": None}
    last, level = df.iloc[-1], safe_float(pattern["break_level"])
    close, open_, vol_ratio, body_ratio, atr_val = safe_float(last["close"]), safe_float(last["open"]), safe_float(last.get("volume_ratio"), 1.0), safe_float(last.get("body_ratio"), 0.0), safe_float(last.get("atr"))
    if level <= 0 or atr_val <= 0: return {"confirmed": False, "type": None}
    body_break = (close > level and close > open_) if direction == "long" else (close < level and close < open_)
    meaningful = (close - level >= atr_val * 0.08) if direction == "long" else (level - close >= atr_val * 0.08)
    if body_break and meaningful and vol_ratio >= PATTERN_VOLUME_RATIO and body_ratio >= 0.25:
        return {"confirmed": True, "type": f"{pattern['type']}_BREAKOUT"}
    return {"confirmed": False, "type": None}

def detect_momentum_reversal(df, direction):
    if df is None or len(df) < 20: return False
    last4 = df.tail(4)
    rsi_v, hist_v = last4["rsi"].values, last4["macd_hist"].values
    return bool(rsi_v[-1] > rsi_v[-2] and hist_v[-1] > hist_v[-2]) if direction == "long" else \
           bool(rsi_v[-1] < rsi_v[-2] and hist_v[-1] < hist_v[-2])

def calculate_momentum_acceleration(df, direction):
    if df is None or len(df) < 6: return {"accelerating": False, "score": 0}
    hist, rsi_v = df["macd_hist"].tail(4).values, df["rsi"].tail(4).values
    slopes = np.diff(hist)
    consistent = sum(1 for s in slopes if s > 0) if direction == "long" else sum(1 for s in slopes if s < 0)
    score = (consistent / len(slopes)) * 60 + (25 if (rsi_v[-1] > rsi_v[0] if direction == "long" else rsi_v[-1] < rsi_v[0]) else 0)
    return {"accelerating": score >= 60, "score": clamp(score, 0, 100)}

def detect_micro_structure_break(df, direction):
    if df is None or len(df) < 30: return {"broken": False, "level": None}
    s_highs, s_lows = find_swing_points(df, window=2)
    last_close = df["close"].iloc[-1]
    if direction == "long" and len(s_highs) >= 2:
        h1, h2 = s_highs[-2][1], s_highs[-1][1]
        if h2 < h1 and last_close > h1: return {"broken": True, "level": h1}
        return {"broken": False, "level": h1 if s_highs else None}
    elif direction == "short" and len(s_lows) >= 2:
        l1, l2 = s_lows[-2][1], s_lows[-1][1]
        if l2 > l1 and last_close < l1: return {"broken": True, "level": l1}
        return {"broken": False, "level": l1 if s_lows else None}
    return {"broken": False, "level": None}

def confirm_breakout(df, level, direction):
    if df is None or level is None or len(df) < 5: return {"confirmed": False, "type": None}
    last = df.iloc[-1]
    close, open_, atr_val, vol_ratio, body_ratio = safe_float(last["close"]), safe_float(last["open"]), safe_float(last["atr"]), safe_float(last["volume_ratio"], 1), safe_float(last.get("body_ratio"), 0)
    if atr_val <= 0: return {"confirmed": False, "type": None}
    body_breaks = (close > level and open_ < close) if direction == "long" else (close < level and open_ > close)
    meaningful = (close - level >= atr_val * 0.15) if direction == "long" else (level - close >= atr_val * 0.15)
    if body_breaks and meaningful and vol_ratio >= MIN_BREAKOUT_VOLUME_RATIO and body_ratio >= 0.35:
        return {"confirmed": True, "type": "aggressive_breakout"}
    return {"confirmed": False, "type": None}

def confirm_breakout_retest(df, level, direction):
    if df is None or level is None or len(df) < 8: return {"confirmed": False, "type": None}
    last3, last = df.tail(3), df.iloc[-1]
    retested = (last3["low"].min() <= level * 1.005) if direction == "long" else (last3["high"].max() >= level * 0.995)
    held = (last3["close"].iloc[-1] > level) if direction == "long" else (last3["close"].iloc[-1] < level)
    momentum_resumed = (safe_float(last["macd_hist"]) > safe_float(df.iloc[-2]["macd_hist"])) if direction == "long" else \
                       (safe_float(last["macd_hist"]) < safe_float(df.iloc[-2]["macd_hist"]))
    if retested and held and momentum_resumed and safe_float(last["volume_ratio"], 1) >= 1.0:
        return {"confirmed": True, "type": "confirmed_retest"}
    return {"confirmed": False, "type": None}

def calculate_expected_move(df, direction, entry_price, leverage):
    if df is None or entry_price <= 0 or leverage <= 0: return {"sufficient": False}
    req = MIN_TARGET_ROI / leverage
    s_highs, s_lows = find_swing_points(df, window=3)
    if direction == "long":
        nearest = min([h for _, h in s_highs if h > entry_price]) if [h for _, h in s_highs if h > entry_price] else None
        avail = (nearest - entry_price) / entry_price * 100 if nearest else req * 3
    else:
        nearest = max([l for _, l in s_lows if l < entry_price]) if [l for _, l in s_lows if l < entry_price] else None
        avail = (entry_price - nearest) / entry_price * 100 if nearest else req * 3
    return {"sufficient": avail >= req, "required_move_pct": req, "available_move_pct": avail}

def momentum_analysis(df):
    if df is None or len(df) < 50: return {"direction": "neutral", "strength": 0}
    x, p = df.iloc[-1], df.iloc[-2]
    rsi_v, macd_h, prev_h, roc_v, vol_r = safe_float(x["rsi"]), safe_float(x["macd_hist"]), safe_float(p["macd_hist"]), safe_float(x["roc"]), safe_float(x["volume_ratio"], 1)
    lp = (20 if 52 <= rsi_v <= 70 else 0) + (20 if macd_h > 0 else 0) + (15 if macd_h > prev_h else 0) + (15 if roc_v > 0 else 0)
    sp = (20 if 30 <= rsi_v <= 48 else 0) + (20 if macd_h < 0 else 0) + (15 if macd_h < prev_h else 0) + (15 if roc_v < 0 else 0)
    if lp > sp: return {"direction": "long", "strength": clamp(lp, 0, 100)}
    if sp > lp: return {"direction": "short", "strength": clamp(sp, 0, 100)}
    return {"direction": "neutral", "strength": 0}

def evaluate_early_trigger(df5, direction, level):
    try:
        current = df5.iloc[-1]
        atr, price = safe_float(current.get("atr")), safe_float(current.get("close"))
        if atr <= 0 or price <= 0 or level is None: return {"eligible": False}
        dist_atr = abs(price - level) / atr
        if dist_atr > EARLY_TRIGGER_MAX_ATR: return {"eligible": False}
        momentum = calculate_momentum_acceleration(df5, direction)
        rsi_t = bool(df5["rsi"].iloc[-1] > df5["rsi"].iloc[-2] if direction == "long" else df5["rsi"].iloc[-1] < df5["rsi"].iloc[-2])
        checks = {"momentum": bool(momentum.get("accelerating")), "rsi": rsi_t}
        conf = sum(checks.values())
        return {"eligible": conf >= EARLY_TRIGGER_MIN_CONFIRMATIONS, "score": 60, "confirmations": conf, "reasons": list(checks.keys())}
    except Exception: return {"eligible": False}

def calculate_setup_score(direction, t4h, t1h, pb_q, atr_pct, funding, vol_r, regime_ok):
    score = (15 if regime_ok else 5) + ((t4h["strength"] / 100) * 15 if t4h["direction"] == direction else 0) + \
            ((t1h["strength"] / 100) * 15 if t1h["direction"] == direction else 0) + (15 if pb_q.get("healthy") else 5) + \
            ((pb_q.get("score", 0) / 100) * 20) + clamp((vol_r - 0.8) * 25, 0, 10) + (5 if 0.3 <= atr_pct <= 4.0 else 2) + (5 if abs(funding) < 0.00075 else 2)
    return round(clamp(score, 0, 100), 2), {}

def calculate_trigger_score(momentum_accel, rsi_turn, ema_slope_ok, structure_break, breakout_result, atr_pos_ok):
    score = ((momentum_accel.get("score", 0) / 100) * 20) + (15 if rsi_turn else 0) + (10 if ema_slope_ok else 0) + \
            (25 if structure_break.get("broken") else 0) + (15 if breakout_result.get("confirmed") else 0) + \
            (10 if breakout_result.get("confirmed") else 0) + (5 if atr_pos_ok else 0)
    conf = sum([bool(momentum_accel.get("accelerating")), rsi_turn, bool(structure_break.get("broken")), bool(breakout_result.get("confirmed"))])
    return round(clamp(score, 0, 100), 2), {}, conf


# ============================================================
# ANALYSIS PIPELINE (OPTIMIZED LOGGING)
# ============================================================

def analyze_high_conviction(symbol, btc_regime=None):
    diag_stage(symbol, "scanned")
    tf_data = {}
    for tf in ["4h", "1h", "15m", "5m"]:
        df = fetch_ohlcv_closed(symbol, tf)
        if df is None:
            diag_reject(symbol, "data_missing", f"{tf} veri eksik")
            return None
        tf_data[tf] = enrich_dataframe(df)

    if detect_anomaly(tf_data["15m"]):
        diag_reject(symbol, "anomaly", "15m anomali tespit edildi")
        return None
        
    t1h = timeframe_trend(tf_data["1h"])
    if t1h["direction"] == "neutral" or t1h["strength"] < 30:
        diag_reject(symbol, "trend_neutral", f"1H trend yetersiz: {t1h}")
        return None
    direction = t1h["direction"]
    diag_stage(symbol, "trend_ok", f"Yön: {direction.upper()} (Güç: {t1h['strength']})")

    pattern = detect_chart_patterns(tf_data["15m"], direction)
    pb_detected = detect_pullback(tf_data["15m"], direction)
    pb_quality = assess_pullback_quality(tf_data["15m"], direction) if pb_detected else {"healthy": False, "score": 0}
    pattern_setup = bool(pattern.get("detected"))
    
    if not pb_quality.get("healthy") and not pattern_setup:
        diag_reject(symbol, "no_setup", "Pullback sağlıklı değil ve formasyon bulunamadı")
        return None
        
    if pattern_setup:
        diag_stage(symbol, "pattern_candidate", f"Formasyon: {pattern.get('type')}")
    if pb_quality.get("healthy"):
        diag_stage(symbol, "pullback_healthy", f"Pullback Skor: {pb_quality.get('score')}")

    momentum_accel = calculate_momentum_acceleration(tf_data["5m"], direction)
    rsi_v = tf_data["5m"]["rsi"].tail(4).values
    rsi_turn = (rsi_v[-1] > rsi_v[0]) if direction == "long" else (rsi_v[-1] < rsi_v[0])

    structure_break = detect_micro_structure_break(tf_data["5m"], direction)
    pattern_breakout = confirm_pattern_breakout(tf_data["5m"], pattern, direction)

    if structure_break["broken"]:
        level = structure_break["level"]
        breakout_result = confirm_breakout(tf_data["5m"], level, direction)
        if not breakout_result["confirmed"]: breakout_result = confirm_breakout_retest(tf_data["5m"], level, direction)
        breakout_type = breakout_result.get("type")
    elif pattern_breakout["confirmed"]:
        level = pattern["break_level"]
        breakout_result = pattern_breakout
        breakout_type = pattern_breakout.get("type")
    else:
        early = evaluate_early_trigger(tf_data["5m"], direction, pattern.get("break_level") if pattern_setup else structure_break.get("level"))
        if early.get("eligible"):
            level = pattern.get("break_level") if pattern_setup else structure_break.get("level")
            breakout_result, breakout_type = {"confirmed": True, "type": "early_momentum"}, "early_momentum"
        else:
            diag_reject(symbol, "no_breakout", "Yapı kırılımı veya formasyon breakout doğrulanamadı")
            return None

    if not breakout_result.get("confirmed"):
        diag_reject(symbol, "breakout_unconfirmed", "Breakout onaylanmadı")
        return None
        
    diag_stage(symbol, "breakout_confirmed", f"Tip: {breakout_type} | Seviye: {level}")

    current_5m = tf_data["5m"].iloc[-1]
    price, atr_val = safe_float(current_5m["close"]), safe_float(current_5m["atr"])
    if level is None or atr_val <= 0 or (abs(price - level) / atr_val) > MAX_ENTRY_CHASE_ATR:
        diag_reject(symbol, "entry_chase", "Fiyat kırılma seviyesinden çok uzaklaşmış (Chase)")
        return None

    setup_score, setup_bd = calculate_setup_score(direction, timeframe_trend(tf_data["4h"]), t1h, pb_quality, safe_float(current_5m["atr_pct"]), get_funding(symbol), safe_float(current_5m["volume_ratio"], 1), True)
    trigger_score, trigger_bd, confs = calculate_trigger_score(momentum_accel, rsi_turn, True, {"broken": structure_break["broken"] or pattern_breakout["confirmed"]}, breakout_result, True)

    if setup_score < MIN_SETUP_SCORE:
        diag_reject(symbol, "setup_score_low", f"Setup Skor düşük: {setup_score} < {MIN_SETUP_SCORE}")
        return None
    if trigger_score < MIN_TRIGGER_SCORE:
        diag_reject(symbol, "trigger_score_low", f"Trigger Skor düşük: {trigger_score} < {MIN_TRIGGER_SCORE}")
        return None

    diag_stage(symbol, "scores_passed", f"Setup: {setup_score} | Trigger: {trigger_score}")

    leverage = calculate_leverage(setup_score, trigger_score, safe_float(current_5m["atr_pct"]))
    expected_move = calculate_expected_move(tf_data["1h"], direction, price, leverage)
    if not expected_move["sufficient"]:
        diag_reject(symbol, "expected_move", "Beklenen hareket hedef ROI için yetersiz")
        return None

    diag_stage(symbol, "final_candidate", f"Tüm filtreler geçti! Kaldıraç: {leverage}x")
    return {
        "symbol": symbol, "direction": direction, "setup_type": "continuation", "setup_score": setup_score,
        "trigger_score": trigger_score, "price": price, "atr": atr_val, "structure_level": level, "leverage": leverage,
        "expected_move": expected_move, "data_5m": tf_data["5m"], "breakout_type": breakout_type, "chart_pattern": pattern.get("type") if pattern_setup else None
    }


# ============================================================
# EXECUTION & POSITION MANAGEMENT
# ============================================================

def calculate_leverage(setup_score, trigger_score, atr_pct):
    combined = (setup_score + trigger_score) / 2
    lev = MIN_LEVERAGE + 1
    if combined >= 85: lev += 2
    elif combined >= 78: lev += 1
    return int(clamp(lev, MIN_LEVERAGE, MAX_LEVERAGE))

def set_isolated_and_leverage(symbol, leverage):
    try:
        exchange.set_margin_mode("isolated", symbol)
        exchange.set_leverage(leverage, symbol)
    except Exception as e:
        logger.warning("%s margin/leverage ayar hatası: %s", symbol, e)

def calculate_dynamic_atr_stop(df, direction, entry_price, leverage, structure_level):
    atr_val = safe_float(df.iloc[-1]["atr"]) if df is not None and not df.empty else entry_price * 0.01
    atr_dist = atr_val * INITIAL_STOP_ATR_MULTIPLIER
    struct_dist = max(entry_price - structure_level, atr_val * 0.5) if structure_level and direction == "long" else \
                  max(structure_level - entry_price, atr_val * 0.5) if structure_level else atr_dist
    natural_dist = max(atr_dist, struct_dist)
    max_risk_dist = ((MIN_TARGET_ROI * MAX_LOSS_TO_TARGET_RATIO) / 100 / leverage) * entry_price
    final_dist = min(natural_dist, max_risk_dist)
    return {
        "stop_price": entry_price - final_dist if direction == "long" else entry_price + final_dist,
        "distance": final_dist, "max_risk_distance": max_risk_dist
    }

def get_local_positions():
    with state_lock: return {k: dict(v) for k, v in local_positions.items()}

def local_position_count():
    with state_lock: return len(local_positions)

def has_local_symbol(symbol):
    norm = normalize_symbol(symbol)
    with state_lock: return any(normalize_symbol(p["symbol"]) == norm for p in local_positions.values())

def fetch_real_positions():
    try:
        positions = safe_call(exchange.fetch_positions)
        return [{"symbol": p["symbol"], "side": p["side"], "contracts": safe_float(p.get("contracts")), "entryPrice": safe_float(p.get("entryPrice")), "leverage": safe_float(p.get("leverage"))} for p in positions if abs(safe_float(p.get("contracts"))) > 0]
    except Exception as e:
        raise Exception(f"Position fetch failed: {e}")

def place_stop_market_order(symbol, position_side, amount, stop_price):
    try:
        close_side = "sell" if position_side == "long" else "buy"
        order = safe_call(exchange.create_order, symbol, "STOP_MARKET", close_side, amount, None, {"stopPrice": format_price(symbol, stop_price), "reduceOnly": True, "positionSide": "BOTH"})
        return order.get("id")
    except Exception as e:
        logger.error("%s failsafe stop hatası: %s", symbol, e)
        return None

def open_position(candidate):
    symbol, direction, leverage, price = candidate["symbol"], candidate["direction"], candidate["leverage"], candidate["price"]
    if local_position_count() >= MAX_OPEN_POSITIONS or has_local_symbol(symbol): return False

    stop_info = calculate_dynamic_atr_stop(candidate["data_5m"], direction, price, leverage, candidate["structure_level"])
    amount = (MARGIN_PER_TRADE * leverage) / price

    if not DRY_RUN:
        set_isolated_and_leverage(symbol, leverage)
        order = safe_call(exchange.create_order, symbol, "market", "buy" if direction == "long" else "sell", format_amount(symbol, amount), None, {"positionSide": "BOTH"})
        entry_price = safe_float(order.get("average"), price)
        stop_info = calculate_dynamic_atr_stop(candidate["data_5m"], direction, entry_price, leverage, candidate["structure_level"])
        stop_order_id = place_stop_market_order(symbol, direction, amount, stop_info["stop_price"])
    else:
        entry_price = price
        stop_order_id = None

    key = f"hc:{normalize_symbol(symbol)}"
    with state_lock:
        local_positions[key] = {
            "key": key, "symbol": symbol, "side": direction, "entry_price": entry_price, "amount": amount,
            "margin": MARGIN_PER_TRADE, "leverage": leverage, "target_roi": MIN_TARGET_ROI,
            "initial_stop_price": stop_info["stop_price"], "current_stop_price": stop_info["stop_price"],
            "profit_lock_active": False, "peak_price": entry_price, "trough_price": entry_price,
            "opened_at": now_ms(), "stop_order_id": stop_order_id
        }
    bot_stats["orders"] += 1
    logger.warning("[%s POZİSYON AÇILDI] %s %s | Giriş: %s | Stop: %s", "DRY" if DRY_RUN else "REAL", direction.upper(), symbol, entry_price, stop_info["stop_price"])
    return True

def calculate_roi(position, price):
    entry, lev = safe_float(position["entry_price"]), safe_float(position["leverage"], 1)
    if entry <= 0: return 0.0
    change = (price - entry) / entry if position["side"] == "long" else (entry - price) / entry
    return change * lev * 100

def should_close_position(position, price):
    roi = calculate_roi(position, price)
    position["current_roi"] = roi
    if position["side"] == "long" and price > position["peak_price"]: position["peak_price"] = price
    if position["side"] == "short" and price < position["trough_price"]: position["trough_price"] = price

    # Zarar Tavanı (Hard Loss Cap)
    if roi <= -(MIN_TARGET_ROI * MAX_LOSS_TO_TARGET_RATIO): return True, "MAX_LOSS_CAP"

    # İlk Stop Kontrolü
    init_stop = position.get("initial_stop_price")
    if init_stop and not position["profit_lock_active"]:
        if position["side"] == "long" and price <= init_stop: return True, "INITIAL_STOP"
        if position["side"] == "short" and price >= init_stop: return True, "INITIAL_STOP"

    # Profit Lock ve Trailing Stop Kontrolü
    if roi >= MIN_TARGET_ROI * PROFIT_LOCK_TRIGGER_RATIO and not position["profit_lock_active"]:
        position["profit_lock_active"] = True

    if position["profit_lock_active"]:
        if position["side"] == "long" and price <= position["current_stop_price"]: return True, "TRAILING_STOP"
        if position["side"] == "short" and price >= position["current_stop_price"]: return True, "TRAILING_STOP"

    return False, None

def close_position(position, reason="MANUAL"):
    symbol, side, amount = position["symbol"], position["side"], position["amount"]
    
    # Kapanış fiyatını alabilmek için son 1m mumu çekelim
    last_df = fetch_ohlcv(symbol, "1m", 5)
    close_price = safe_float(last_df.iloc[-1]["close"]) if last_df is not None and not last_df.empty else position["entry_price"]

    if not DRY_RUN:
        try:
            order = safe_call(exchange.create_order, symbol, "market", "sell" if side == "long" else "buy", format_amount(symbol, amount), None, {"positionSide": "BOTH"})
            close_price = safe_float(order.get("average"), close_price)
        except Exception as e:
            logger.error("%s pozisyon kapatma emri hatası: %s", symbol, e)

    # İşlem Sonuç Formu Hesaplamaları
    entry_price = safe_float(position["entry_price"])
    leverage = safe_float(position["leverage"], 1)
    margin = safe_float(position["margin"])
    
    final_roi = calculate_roi(position, close_price)
    profit_usdt = margin * (final_roi / 100.0)
    
    opened_at = position.get("opened_at", now_ms())
    closed_at = now_ms()
    duration_sec = max(1, (closed_at - opened_at) // 1000)
    duration_min = duration_sec / 60.0

    with state_lock:
        local_positions.pop(position["key"], None)
    bot_stats["closed_positions"] += 1

    # İşlem Sonuç Formu Log Formatı
    logger.warning("=" * 70)
    logger.warning("[İŞLEM SONUÇ FORMU] 📋 Rapor Özeti")
    logger.warning("Sembol            : %s (%s)", symbol, side.upper())
    logger.warning("Giriş Fiyatı      : %s", entry_price)
    logger.warning("Kapanış Fiyatı    : %s", close_price)
    logger.warning("İşlem Süresi      : %.2f dakika (%d sn)", duration_min, duration_sec)
    logger.warning("Hedef ROI         : %%%.2f", MIN_TARGET_ROI)
    logger.warning("Gerçekleşen ROI   : %%%.2f", final_roi)
    logger.warning("Elde Edilen Kazanç: %.2f USDT", profit_usdt)
    logger.warning("Kapanış Nedeni    : %s", reason)
    logger.warning("=" * 70)

    # İsteğe bağlı olarak journal dosyasına da yazalım (Trade Journal Path)
    try:
        journal_entry = {
            "symbol": symbol, "side": side, "entry_price": entry_price, "close_price": close_price,
            "leverage": leverage, "margin": margin, "roi": final_roi, "profit_usdt": profit_usdt,
            "duration_sec": duration_sec, "reason": reason, "opened_at": opened_at, "closed_at": closed_at
        }
        with open(TRADE_JOURNAL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(journal_entry) + "\n")
    except Exception as je:
        logger.error("Trade journal kayıt hatası: %s", je)


# ============================================================
# MAIN LOOP THREADS
# ============================================================

def monitor_loop():
    while running:
        try:
            positions = get_local_positions()
            for key, pos in positions.items():
                df_m = fetch_ohlcv(pos["symbol"], "1m", 5)
                if df_m is None or df_m.empty: continue
                price = safe_float(df_m.iloc[-1]["close"])
                if price <= 0: continue
                should_close, reason = should_close_position(pos, price)
                if should_close:
                    close_position(pos, reason)
        except Exception as e:
            logger.error("Monitor loop hatası: %s", e)
        time.sleep(POSITION_MONITOR_INTERVAL)

def strategy_loop():
    global last_successful_analysis
    while running:
        try:
            reset_cycle_diagnostics()
            scanned_count = 0
            final_candidates_count = 0
            opened_count = 0

            if local_position_count() < MAX_OPEN_POSITIONS:
                gainers, losers, vol_leaders = get_top_movers()
                candidates = list(set(gainers[:TOP_N_CANDIDATES] + vol_leaders[:TOP_N_CANDIDATES]))
                btc_regime = get_btc_regime()
                scanned_count = len(candidates)

                for symbol in candidates:
                    if has_local_symbol(symbol) or local_position_count() >= MAX_OPEN_POSITIONS: continue
                    analysis = analyze_high_conviction(symbol, btc_regime)
                    if analysis:
                        final_candidates_count += 1
                        if open_position(analysis):
                            opened_count += 1
                            
            log_cycle_diagnostics(scanned_count, final_candidates_count, opened_count)
            last_successful_analysis = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            logger.error("Strategy loop hatası: %s", e)
        time.sleep(ANALYSIS_INTERVAL)

def main():
    create_exchange()
    threading.Thread(target=strategy_loop, daemon=True).start()
    threading.Thread(target=monitor_loop, daemon=True).start()
    logger.info("Bot başlatıldı, Flask sunucusu çalışıyor... Port: %s", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
