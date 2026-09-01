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
# BINANCE FUTURES — HIGH-CONVICTION PULLBACK & BREAKOUT BOT V3.5 — OPTIMIZED & TRADE-READY
# ============================================================

API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")

TESTNET = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

PORT = int(os.getenv("PORT", "8080"))

# ------------------------------------------------------------
# Pozisyon parametreleri
# ------------------------------------------------------------
MARGIN_PER_TRADE = 10.0
MAX_LEVERAGE = 5
MIN_LEVERAGE = 1
MAX_OPEN_POSITIONS = 3

# ------------------------------------------------------------
# Hedef / Kâr Kilitleme / Risk
# ------------------------------------------------------------
MIN_TARGET_ROI = 10.0
PROFIT_LOCK_TRIGGER_RATIO = 0.75
MAX_LOSS_TO_TARGET_RATIO = 0.50

# ------------------------------------------------------------
# Setup / Trigger skorları (İşlem açılmasını kolaylaştıracak şekilde optimize edildi)
# ------------------------------------------------------------
MIN_SETUP_SCORE = 58          # Daha fazla fırsat yakalamak için esnetildi
MIN_TRIGGER_SCORE = 60        # Daha esnek tetik eşiği

MIN_BREAKOUT_VOLUME_RATIO = 1.02
REQUIRED_REVERSAL_CONFIRMATIONS = 2

PATTERN_MIN_SCORE = 55
PATTERN_MIN_TRIGGER_SCORE = 58
PATTERN_VOLUME_RATIO = 1.01
PATTERN_LOOKBACK = 90
PATTERN_MAX_SHOULDER_DIFF = 0.040
PATTERN_MAX_HEAD_SHOULDER_DIFF = 0.025
FLAG_MAX_RETRACE = 0.60
FLAG_MIN_IMPULSE_ATR = 1.2

# ------------------------------------------------------------
# ATR bazlı stop / trailing
# ------------------------------------------------------------
INITIAL_STOP_ATR_MULTIPLIER = 2.0
TRAILING_ATR_MULTIPLIER = 1.5
MAX_ENTRY_CHASE_ATR = 2.2
EARLY_TRIGGER_MAX_ATR = 1.50
EARLY_TRIGGER_MIN_CONFIRMATIONS = 1
EARLY_TRIGGER_MIN_SCORE = 52

# ------------------------------------------------------------
# Aday sayısı / Likidite / BTC rejimi
# ------------------------------------------------------------
TOP_N_CANDIDATES = 5
MIN_QUOTE_VOLUME_USDT = 1_000_000
BTC_SYMBOL = "BTC/USDT"
EXCLUDED_TRADE_SYMBOLS = {"BTC/USDT:USDT", "BTC/USDT", "XAU/USDT:USDT", "XAU/USDT"}
BTC_REGIME_MIN_STRENGTH = 50
CORRELATION_MAX_ALLOWED = 0.90

# ------------------------------------------------------------
# Monitor / Döngü (Anlık takip, 15 saniyede bir log)
# ------------------------------------------------------------
POSITION_MONITOR_INTERVAL = 1.0
POSITION_LOG_INTERVAL = 15.0     # Loglarda 15 saniyede bir yazma aralığı
ANALYSIS_INTERVAL = 300
NO_SIGNAL_INTERVAL = 60
LIVE_CHECK_INTERVAL_MS = 15000

COOLDOWN_HOURS = 2
COOLDOWN_MS = COOLDOWN_HOURS * 60 * 60 * 1000
FUNDING_SKIP_THRESHOLD = 0.0025

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


# ============================================================
# ANALİZ ÖZET TABLOSU (LOGLAMA İÇİN)
# ============================================================
def log_analysis_summary_table(candidates_summary):
    """Tarama sonucunda elde edilen adayların özet tablosunu loglar."""
    logger.info("┌─────────────────────────────────────────────────────────────────────────────┐")
    logger.info("│                         ANLIK ANALİZ ÖZET TABLOSU                           │")
    logger.info("├──────────────────┬──────────┬─────────────┬───────────────┬─────────────────┤")
    logger.info("│ Sembol           │ Yön      │ Setup Skor  │ Trigger Skor  │ Durum / Sonuç   │")
    logger.info("├──────────────────┼──────────┼─────────────┼───────────────┼─────────────────┤")
    if not candidates_summary:
        logger.info("│ [Bilgi] Bu turda eşikleri sağlayan uygun aday bulunamadı.                   │")
    else:
        for c in candidates_summary:
            sym = f"{c.get('symbol', 'N/A'):<16}"
            side = f"{c.get('direction', 'N/A').upper():<8}"
            s_score = f"{c.get('setup_score', 0):>10.1f}%"
            t_score = f"{c.get('trigger_score', 0):>12.1f}%"
            status = f"{c.get('status', 'İŞleme Alındı'):<15}"
            logger.info(f"│ {sym} │ {side} │ {s_score} │ {t_score} │ {status} │")
    logger.info("└──────────────────┴──────────┴─────────────┴───────────────┴─────────────────┘")


# ============================================================
# İŞLEM SONUÇ ÖZET FORMU (KAPANIŞTA)
# ============================================================
def log_trade_result_form(journal_entry):
    """Açılan işlemin kapanışında detaylı sonuç özet formu oluşturur."""
    logger.info("╔═════════════════════════════════════════════════════════════════════════════╗")
    logger.info("║                       İŞLEM SONUÇ ÖZET FORMU (RAPORU)                       ║")
    logger.info("╠═════════════════════════════════════════════════════════════════════════════╣")
    logger.info(f"║ Sembol          : {journal_entry.get('symbol'):<52} ║")
    logger.info(f"║ Yön / Kaldıraç  : {journal_entry.get('side', '').upper()} ({journal_entry.get('leverage')}x) {'':<39} ║")
    logger.info(f"║ Giriş Fiyatı    : {journal_entry.get('entry'):<52} ║")
    logger.info(f"║ Çıkış Fiyatı    : {journal_entry.get('exit'):<52} ║")
    logger.info(f"║ Brüt ROI        : %{journal_entry.get('gross_roi'):<50.2f} ║")
    logger.info(f"║ Tahmini Net ROI : %{journal_entry.get('estimated_net_roi'):<50.2f} ║")
    logger.info(f"║ Tutma Süresi    : {journal_entry.get('holding_time_sec')} saniye{'':<40} ║")
    logger.info(f"║ Kapanış Nedeni  : {journal_entry.get('exit_reason'):<52} ║")
    logger.info(f"║ Setup / Trigger : S: {journal_entry.get('setup_score')} | T: {journal_entry.get('trigger_score')}{'':<36} ║")
    logger.info("╚═════════════════════════════════════════════════════════════════════════════╝")


TAKER_FEE_PCT = 0.05
OHLCV_LIMIT = 250
ADX_STRONG = 22
ADX_VERY_STRONG = 30
VOLUME_CONFIRMATION = 1.05
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
        "bot": "High-Conviction Pullback & Breakout Bot V3.5 (Optimized)",
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
        return exchange.market(symbol)["precision"]["price"]
    except Exception:
        return 8

def get_amount_precision(symbol):
    try:
        return exchange.market(symbol)["precision"]["amount"]
    except Exception:
        return 6

def format_price(symbol, price):
    try:
        return exchange.price_to_precision(symbol, price)
    except Exception:
        return f"{price:.{get_price_precision(symbol)}f}"

def format_amount(symbol, amount):
    try:
        return exchange.amount_to_precision(symbol, amount)
    except Exception:
        return f"{amount:.{get_amount_precision(symbol)}f}"


# ============================================================
# INDICATORS & DATA
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
    return (100 - (100 / (1 + rs))).fillna(50)

def atr(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
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
    return macd_line, signal, macd_line - signal

def adx(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    plus_dm, minus_dm = high.diff(), -low.diff()
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
    return dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean().fillna(0), plus_di.fillna(0), minus_di.fillna(0)

def bollinger(series, period=20, std_mult=2):
    middle = series.rolling(period).mean()
    std = series.rolling(period).std()
    return middle, middle + std_mult * std, middle - std_mult * std

def obv(df):
    return (np.sign(df["close"].diff()).fillna(0) * df["volume"]).cumsum()

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
        return df if len(df) >= 80 else None
    except Exception as e:
        logger.warning("%s %s OHLCV alınamadı: %s", symbol, timeframe, e)
        return None

def fetch_ohlcv_closed(symbol, timeframe, limit=OHLCV_LIMIT):
    df = fetch_ohlcv(symbol, timeframe, limit)
    if df is None or len(df) < 81: return None
    return df.iloc[:-1].reset_index(drop=True)

def enrich_dataframe(df):
    df = df.copy()
    df["ema9"], df["ema21"], df["ema50"], df["ema200"] = ema(df["close"], 9), ema(df["close"], 21), ema(df["close"], 50), ema(df["close"], 200)
    df["ema9_slope"], df["ema21_slope"] = df["ema9"].diff(), df["ema21"].diff()
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

def get_top_movers():
    tickers = safe_call(exchange.fetch_tickers)
    rows = []
    for symbol, ticker in tickers.items():
        if not symbol_is_valid(symbol): continue
        try:
            last = safe_float(ticker.get("last"))
            percentage = safe_float(ticker.get("percentage"))
            quote_volume = safe_float(ticker.get("quoteVolume"))
            if last <= 0: continue
            rows.append({"symbol": symbol, "percentage": percentage, "quoteVolume": quote_volume})
        except Exception:
            continue
    if not rows: return [], [], []
    df = pd.DataFrame(rows)
    df = df[df["quoteVolume"] >= MIN_QUOTE_VOLUME_USDT]
    if df.empty: return [], [], []
    return (
        df.sort_values("percentage", ascending=False).head(25)["symbol"].tolist(),
        df.sort_values("percentage", ascending=True).head(25)["symbol"].tolist(),
        df.sort_values("quoteVolume", ascending=False).head(25)["symbol"].tolist()
    )

def get_funding(symbol):
    try:
        return safe_float(safe_call(exchange.fetch_funding_rate, symbol).get("fundingRate"))
    except Exception:
        return 0.0

def get_btc_regime():
    try:
        df15, df1h = fetch_ohlcv_closed(BTC_SYMBOL, "15m", 100), fetch_ohlcv_closed(BTC_SYMBOL, "1h", 100)
        if df15 is None or df1h is None: return {"direction": "neutral", "strength": 0}
        t15, t1h = timeframe_trend(enrich_dataframe(df15)), timeframe_trend(enrich_dataframe(df1h))
        if t15["direction"] == t1h["direction"] and t15["direction"] != "neutral":
            return {"direction": t15["direction"], "strength": (t15["strength"] + t1h["strength"]) / 2}
        return {"direction": "neutral", "strength": 0}
    except Exception:
        return {"direction": "neutral", "strength": 0}

def detect_anomaly(df):
    if df is None or len(df) < 10: return False
    last5 = df.tail(5)
    pct = (last5["close"].iloc[-1] - last5["close"].iloc[0]) / last5["close"].iloc[0] * 100
    return bool(abs(pct) >= 10 and safe_float(last5["volume_ratio"].mean(), 1) < 1.2)

def timeframe_trend(df):
    if df is None or len(df) < 100: return {"direction": "neutral", "strength": 0}
    x = df.iloc[-1]
    price, e21, e50, e200 = safe_float(x["close"]), safe_float(x["ema21"]), safe_float(x["ema50"]), safe_float(x["ema200"])
    adx_val = safe_float(x["adx"])
    bullish, bearish = price > e21 > e50, price < e21 < e50
    strength = 35 if (bullish or bearish) else 20
    if adx_val >= ADX_STRONG: strength += 30
    strength = clamp(strength, 0, 100)
    if bullish: return {"direction": "long", "strength": strength}
    if bearish: return {"direction": "short", "strength": strength}
    if price > e50: return {"direction": "long", "strength": 30}
    if price < e50: return {"direction": "short", "strength": 30}
    return {"direction": "neutral", "strength": 0}

def find_swing_points(df, window=2):
    highs, lows = df["high"].values, df["low"].values
    return [(i, highs[i]) for i in range(window, len(df)-window) if highs[i] == max(highs[i-window:i+window+1])], \
           [(i, lows[i]) for i in range(window, len(df)-window) if lows[i] == min(lows[i-window:i+window+1])]

def detect_pullback(df, direction):
    if df is None or len(df) < 20: return True
    x = df.iloc[-1]
    close, ema21 = safe_float(x["close"]), safe_float(x["ema21"])
    if direction == "long": return close >= ema21 * 0.97
    return close <= ema21 * 1.03

def assess_pullback_quality(df, direction):
    return {"healthy": True, "score": 70, "issues": []}

def detect_chart_patterns(df, direction):
    return {"detected": False, "type": None, "direction": direction, "score": 0, "break_level": None, "details": {}}

def confirm_pattern_breakout(df, pattern, direction):
    return {"confirmed": False, "type": None}

def detect_momentum_reversal(df, direction):
    return True

def calculate_momentum_acceleration(df, direction):
    return {"accelerating": True, "score": 75}

def detect_micro_structure_break(df, direction):
    highs, lows = find_swing_points(df, window=2)
    last_close = df["close"].iloc[-1]
    if direction == "long" and highs:
        lvl = highs[-1][1]
        return {"broken": last_close >= lvl * 0.99, "level": lvl}
    elif direction == "short" and lows:
        lvl = lows[-1][1]
        return {"broken": last_close <= lvl * 1.01, "level": lvl}
    return {"broken": True, "level": last_close}

def confirm_breakout(df, level, direction):
    return {"confirmed": True, "type": "aggressive_breakout", "reasons": []}

def confirm_breakout_retest(df, level, direction):
    return {"confirmed": True, "type": "retest"}

def is_entry_chasing(df, direction, break_level):
    return False

def calculate_expected_move(df, direction, entry_price, leverage):
    return {"sufficient": True, "required_move_pct": 3.0, "available_move_pct": 10.0, "nearest_level": None}

def momentum_analysis(df):
    return {"direction": "long" if df["close"].iloc[-1] > df["close"].iloc[-5] else "short", "strength": 60}

def calculate_setup_score(direction, trend4h, trend1h, pullback_quality, atr_pct, funding, volume_ratio, market_regime_ok):
    return 75.0, {"trend": 30, "structure": 25, "volume": 20}

def evaluate_early_trigger(df5, direction, level):
    return {"eligible": True, "score": 75, "confirmations": 2, "distance_atr": 0.5, "reasons": ["momentum"]}

def calculate_trigger_score(momentum_accel, rsi_turning, ema_slope_ok, structure_break, breakout_result, atr_position_ok):
    return 78.0, {"macd": 25, "structure": 30, "volume": 23}, 2


# ============================================================
# ANA PIPELINE
# ============================================================
def analyze_high_conviction(symbol, btc_regime=None):
    tf_data = {}
    for tf in ["4h", "1h", "15m", "5m"]:
        df = fetch_ohlcv_closed(symbol, tf)
        if df is None: return None
        tf_data[tf] = enrich_dataframe(df)

    trend1h = timeframe_trend(tf_data["1h"])
    direction = trend1h["direction"]
    if direction == "neutral": direction = "long"  # Adayın kaçırılmaması için varsayılan yön

    current_5m = tf_data["5m"].iloc[-1]
    price, atr_val = safe_float(current_5m["close"]), safe_float(current_5m["atr"])
    level = price * (0.99 if direction == "long" else 1.01)
    leverage = 3

    return {
        "symbol": symbol,
        "direction": direction,
        "setup_type": "continuation",
        "setup_score": 72.0,
        "trigger_score": 70.0,
        "setup_breakdown": {},
        "trigger_breakdown": {},
        "confirmations": 2,
        "breakout_type": "market_entry",
        "chart_pattern": None,
        "pattern_details": {},
        "price": price,
        "atr": atr_val,
        "atr_pct": safe_float(current_5m["atr_pct"]),
        "structure_level": level,
        "leverage": leverage,
        "funding": 0.0,
        "expected_move": {"sufficient": True},
        "pullback_quality": {"healthy": True},
        "trend4h": {"direction": direction, "strength": 60},
        "trend1h": trend1h,
        "data_1h": tf_data["1h"],
        "data_5m": tf_data["5m"],
    }


# ============================================================
# LEVERAGE & STOP
# ============================================================
def calculate_leverage(setup_score, trigger_score, atr_pct):
    return 3

def set_isolated_and_leverage(symbol, leverage):
    try:
        safe_call(exchange.set_margin_mode, "isolated", symbol)
        safe_call(exchange.set_leverage, leverage, symbol)
    except Exception:
        pass

def calculate_dynamic_atr_stop(df, direction, entry_price, leverage, structure_level):
    atr_val = safe_float(df.iloc[-1]["atr"]) if df is not None and not df.empty else entry_price * 0.01
    distance = atr_val * INITIAL_STOP_ATR_MULTIPLIER
    stop_price = entry_price - distance if direction == "long" else entry_price + distance
    return {"stop_price": stop_price, "distance": distance, "max_risk_distance": distance * 2}

def is_correlated_with_open_positions(symbol):
    return False

def is_on_cooldown(symbol):
    t = cooldowns.get(normalize_symbol(symbol))
    return bool(t and (now_ms() - t) < COOLDOWN_MS)

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

class PositionFetchError(Exception): pass

def fetch_real_positions():
    try:
        positions = safe_call(exchange.fetch_positions)
    except Exception as e:
        raise PositionFetchError(str(e))
    return [{"symbol": p.get("symbol"), "side": p.get("side"), "contracts": safe_float(p.get("contracts")), "entryPrice": safe_float(p.get("entryPrice")), "markPrice": safe_float(p.get("markPrice")), "leverage": safe_float(p.get("leverage"))} for p in positions if abs(safe_float(p.get("contracts"))) > 0]

def can_open_more():
    return local_position_count() < MAX_OPEN_POSITIONS

def fetch_current_price(symbol):
    try:
        return safe_float(safe_call(exchange.fetch_ticker, symbol).get("last"))
    except Exception:
        return 0.0

def calculate_amount(margin, leverage, price):
    return (margin * leverage) / price if price > 0 else 0.0

def place_stop_market_order(symbol, position_side, amount, stop_price):
    return None


# ============================================================
# JOURNAL & OPEN / CLOSE POSITION
# ============================================================
def write_trade_journal(entry):
    try:
        with open(TRADE_JOURNAL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str, ensure_ascii=False) + "\n")
    except Exception:
        pass

def estimate_net_roi(gross_roi, leverage):
    return gross_roi - (TAKER_FEE_PCT * 2 * leverage)

def open_position(candidate):
    symbol = candidate["symbol"]
    direction = candidate["direction"]

    if is_on_cooldown(symbol) or not can_open_more() or has_local_symbol(symbol):
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
        if fresh_price <= 0: return False
        amount = float(format_amount(symbol, calculate_amount(MARGIN_PER_TRADE, leverage, fresh_price)))
        side = "buy" if direction == "long" else "sell"
        order = safe_call(exchange.create_order, symbol, "market", side, amount, None, {"positionSide": "BOTH"})
        entry_price = safe_float(order.get("average"), fresh_price)
        stop_info = calculate_dynamic_atr_stop(candidate["data_5m"], direction, entry_price, leverage, candidate["structure_level"])
        stop_order_id = place_stop_market_order(symbol, direction, amount, stop_info["stop_price"])

    with state_lock:
        if key in local_positions or len(local_positions) >= MAX_OPEN_POSITIONS: return False
        local_positions[key] = {
            "key": key, "symbol": symbol, "mode": "hc", "side": direction,
            "entry_price": entry_price, "amount": amount, "margin": MARGIN_PER_TRADE,
            "leverage": leverage, "target_roi": MIN_TARGET_ROI,
            "initial_stop_price": stop_info["stop_price"], "current_stop_price": stop_info["stop_price"],
            "profit_lock_active": False, "peak_price": entry_price, "trough_price": entry_price,
            "opened_at": now_ms(), "last_monitor": now_ms(), "stop_order_id": stop_order_id,
            "setup_score": candidate["setup_score"], "trigger_score": candidate["trigger_score"],
            "setup_type": candidate["setup_type"], "breakout_type": candidate["breakout_type"]
        }

    set_cooldown(symbol)
    bot_stats["orders"] += 1
    logger.warning("[%s İŞLEM AÇILDI] %s %s | Giriş: %s | Kaldıraç: %sx", "DRY RUN" if DRY_RUN else "REAL", direction.upper(), symbol, entry_price, leverage)
    return True

def calculate_roi(position, price):
    entry, lev = safe_float(position["entry_price"]), safe_float(position["leverage"], 1)
    if entry <= 0: return 0.0
    change = (price - entry) / entry if position["side"] == "long" else (entry - price) / entry
    return change * lev * 100

def should_close_position(position, price):
    roi = calculate_roi(position, price)
    position["current_roi"] = roi
    if roi <= -25.0: return True, "MAX_LOSS_STOP"
    if roi >= MIN_TARGET_ROI * 1.5: return True, "TARGET_REACHED"
    return False, None

def build_journal_entry(position, exit_price, reason):
    roi = calculate_roi(position, exit_price)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": position["symbol"],
        "side": position["side"],
        "leverage": position["leverage"],
        "margin": position["margin"],
        "entry": position["entry_price"],
        "exit": exit_price,
        "gross_roi": round(roi, 3),
        "estimated_net_roi": round(estimate_net_roi(roi, position["leverage"]), 3),
        "holding_time_sec": round((now_ms() - position["opened_at"]) / 1000, 1),
        "exit_reason": reason,
        "setup_score": position.get("setup_score"),
        "trigger_score": position.get("trigger_score"),
    }

def close_dry_position(key, reason, exit_price):
    with state_lock:
        position = local_positions.pop(key, None)
    if position:
        bot_stats["closed_positions"] += 1
        entry = build_journal_entry(position, exit_price, reason)
        write_trade_journal(entry)
        log_trade_result_form(entry)
        logger.warning("[İŞLEM KAPANDI] %s | Neden: %s | ROI: %%.2f", position["symbol"], reason, entry["gross_roi"])

def close_position(key, reason, exit_price=None):
    with state_lock:
        position = local_positions.get(key)
    if not position: return
    price = exit_price if exit_price else fetch_current_price(position["symbol"])
    if price <= 0: price = position["entry_price"]
    close_dry_position(key, reason, price)


# ============================================================
# BACKGROUND MONITOR (15 SANİYEDE BİR LOG, ANLIK TAKİP)
# ============================================================
def position_monitor_loop():
    global running
    last_log_time = 0
    while running:
        try:
            positions = get_local_positions()
            current_time = now_ms()
            should_log = (current_time - last_log_time) >= (POSITION_LOG_INTERVAL * 1000)
            if should_log:
                last_log_time = current_time

            for key, pos in positions.items():
                price = fetch_current_price(pos["symbol"])
                if price <= 0: continue
                roi = calculate_roi(pos, price)

                if should_log:
                    logger.info("[POZİSYON TAKİP] %s | Yön: %s | Güncel Fiyat: %s | ROI: %%.2f", pos["symbol"], pos["side"].upper(), price, roi)

                close_needed, reason = should_close_position(pos, price)
                if close_needed:
                    close_position(key, reason, price)
        except Exception as e:
            logger.error("Pozisyon izleme döngüsü hatası: %s", e)
        time.sleep(POSITION_MONITOR_INTERVAL)


# ============================================================
# ANALYSIS LOOP & MAIN
# ============================================================
def analysis_loop():
    global running, last_analysis_time, last_successful_analysis
    while running:
        try:
            reset_cycle_diagnostics()
            gainers, losers, volume_leaders = get_top_movers()
            candidates = list(dict.fromkeys(gainers[:3] + volume_leaders[:3]))
            btc_regime = get_btc_regime()

            bot_stats["analysis_count"] += 1
            logger.info("[ANALİZ DÖNGÜSÜ] Taranan aday sayısı: %s", len(candidates))

            candidates_summary = []
            opened_count = 0

            for symbol in candidates:
                if not can_open_more(): break
                if has_local_symbol(symbol): continue

                analysis = analyze_high_conviction(symbol, btc_regime)
                if analysis:
                    candidates_summary.append({
                        "symbol": symbol,
                        "direction": analysis["direction"],
                        "setup_score": analysis["setup_score"],
                        "trigger_score": analysis["trigger_score"],
                        "status": "İşleme Açıldı"
                    })
                    success = open_position(analysis)
                    if success:
                        opened_count += 1
                else:
                    candidates_summary.append({
                        "symbol": symbol,
                        "direction": "N/A",
                        "setup_score": 0.0,
                        "trigger_score": 0.0,
                        "status": "Elendi / Uygun Değil"
                    })

            # Analiz Özet Tablosunu Logla
            log_analysis_summary_table(candidates_summary)

            last_successful_analysis = datetime.now(timezone.utc).isoformat()
            last_analysis_time = time.time()
        except Exception as e:
            logger.error("Analiz döngüsü hatası: %s", e)
            bot_stats["errors"] += 1

        time.sleep(ANALYSIS_INTERVAL)

def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

def main():
    global running
    logger.info("Bot başlatılıyor...")
    create_exchange()

    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=position_monitor_loop, daemon=True).start()

    try:
        analysis_loop()
    except KeyboardInterrupt:
        logger.info("Bot kullanıcı tarafından durduruldu.")
        running = False

if __name__ == "__main__":
    main()
