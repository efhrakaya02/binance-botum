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
# Eski SCALP / OPPORTUNITY ayrımı kaldırıldı. Tek strateji:
#
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
#
# KORUNAN ALTYAPI: ccxt/Binance Futures bağlantısı, OHLCV/indikatör
# hesaplama, logging, DRY_RUN, thread/monitor iskeleti, native
# STOP_MARKET failsafe, funding kontrolleri, BTC rejim mantığı,
# env variable tabanlı config yaklaşımı.
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
# Pozisyon parametreleri  (YENİ — tek strateji)
# ------------------------------------------------------------

MARGIN_PER_TRADE = 10.0     # Her işlem sabit ~10 USDT margin kullanır
MAX_LEVERAGE = 5             # Kaldıraç ASLA bunu geçmez (1x-5x arası dinamik)
MIN_LEVERAGE = 1
MAX_OPEN_POSITIONS = 3       # Aynı anda en fazla 3 açık pozisyon

# ------------------------------------------------------------
# Hedef / Kâr Kilitleme / Risk  (YENİ)
# ------------------------------------------------------------

MIN_TARGET_ROI = 10.0                 # İlk kâr milestone'u (%), kapanış tetiği DEĞİL
PROFIT_LOCK_TRIGGER_RATIO = 0.75      # Hedefin %75'inde kâr kilitleme + trailing aktifleşir
MAX_LOSS_TO_TARGET_RATIO = 0.50       # Maksimum zarar, hedefin %50'sini geçemez (1:2 R/R tabanı)

# ------------------------------------------------------------
# Setup / Trigger skorları  (YENİ)
# ------------------------------------------------------------

MIN_SETUP_SCORE = 68
MIN_TRIGGER_SCORE = 68

MIN_BREAKOUT_VOLUME_RATIO = 1.10
REQUIRED_REVERSAL_CONFIRMATIONS = 2

# Daha esnek giriş: pattern breakoutları, klasik pullback kadar katı olmayan
# ama yine de breakout + momentum/volume teyidi isteyen ikinci fırsat yolu.
PATTERN_MIN_SCORE = 62
PATTERN_MIN_TRIGGER_SCORE = 64
PATTERN_VOLUME_RATIO = 1.05
PATTERN_LOOKBACK = 90
PATTERN_MAX_SHOULDER_DIFF = 0.035
PATTERN_MAX_HEAD_SHOULDER_DIFF = 0.020
FLAG_MAX_RETRACE = 0.55
FLAG_MIN_IMPULSE_ATR = 1.5   # Erken çıkış için gereken ters sinyal sayısı (trend/momentum/yapı)

# ------------------------------------------------------------
# ATR bazlı stop / trailing  (YENİ)
# ------------------------------------------------------------

INITIAL_STOP_ATR_MULTIPLIER = 1.8
TRAILING_ATR_MULTIPLIER = 1.5
MAX_ENTRY_CHASE_ATR = 1.8             # Kırılımdan bu kadar ATR uzaklaşmışsa artık "geç kalınmış" sayılır

# ------------------------------------------------------------
# Aday sayısı / Likidite / BTC rejimi
# ------------------------------------------------------------

TOP_N_CANDIDATES = 5
MIN_QUOTE_VOLUME_USDT = 2_000_000
BTC_SYMBOL = "BTC/USDT"
EXCLUDED_TRADE_SYMBOLS = {"BTC/USDT:USDT", "BTC/USDT", "XAU/USDT:USDT", "XAU/USDT"}
BTC_REGIME_MIN_STRENGTH = 60
CORRELATION_MAX_ALLOWED = 0.85        # Açık pozisyonlarla bu korelasyonun üzerindeki adaylar elenir

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


def log_final_decision(symbol, direction, level, df5, breakout_result,
                       momentum_accel=None, rsi_turn=None, volume_ok=None,
                       retest=None, funding=None, accepted=False, reason=""):
    """Final confirmation telemetry only; does not alter entry logic."""
    try:
        current = float(df5["close"].iloc[-1])
        atr = float(df5["atr"].iloc[-1]) if "atr" in df5.columns else 0.0
        distance_atr = abs(current - float(level)) / atr if atr > 0 and level is not None else None
    except Exception:
        current, atr, distance_atr = None, None, None

    logger.info(
        "[HC FINAL DETAIL] %s | dir=%s | level=%s | price=%s | ATR=%s | distance_ATR=%s | "
        "breakout=%s | momentum_accel=%s | rsi_turn=%s | volume_ok=%s | retest=%s | funding=%s",
        symbol, direction, level, current, atr, distance_atr,
        bool(breakout_result and breakout_result.get("confirmed")),
        momentum_accel, rsi_turn, volume_ok, retest, funding
    )
    logger.info(
        "[HC DECISION] %s | %s | primary=%s",
        symbol, "PASS" if accepted else "REJECT", reason or "NONE"
    )

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
        "correlation=%s btc_risk_adjusted=%s final_confirmation=%s | "
        "final_no_data=%s breakout=%s chase=%s momentum=%s rsi=%s volume=%s retest=%s funding=%s",
        d.get("trigger_score_low",0), d.get("trigger_confirmations",0),
        d.get("expected_move",0), d.get("correlation",0),
        d.get("btc_risk_adjusted",0), d.get("final_confirmation",0),
        d.get("final_reason_no_data",0), d.get("final_reason_breakout",0),
        d.get("final_reason_chase",0), d.get("final_reason_momentum",0),
        d.get("final_reason_rsi",0), d.get("final_reason_volume",0),
        d.get("final_reason_retest",0), d.get("final_reason_funding",0)
    )
    logger.info("[HC DIAGNOSTICS] Hatalar=%s", d.get("analysis_error",0))
    logger.info("=" * 70)


# ------------------------------------------------------------
# Komisyon (tahmini net ROI hesaplaması için)  (YENİ)
# ------------------------------------------------------------

TAKER_FEE_PCT = 0.05   # Binance Futures taker ~%0.05 (giriş+çıkış, entry+exit ayrı ayrı uygulanır)

# ------------------------------------------------------------
# Binance / Teknik eşikler
# ------------------------------------------------------------

OHLCV_LIMIT = 250

ADX_STRONG = 25
ADX_VERY_STRONG = 35

VOLUME_CONFIRMATION = 1.15

# ------------------------------------------------------------
# Failsafe stop
# ------------------------------------------------------------

HARD_STOP_BUFFER = 1.15  # native stop, yazılımsal SL'den %15 daha geniş

# ------------------------------------------------------------
# Trade journal
# ------------------------------------------------------------

TRADE_JOURNAL_PATH = os.getenv("TRADE_JOURNAL_PATH", "/tmp/trade_journal.jsonl")

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

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
        "bot": "High-Conviction Pullback & Breakout Bot V3",
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
        "Binance bağlantısı hazır | TESTNET=%s | DRY_RUN=%s | MAX_LEVERAGE=%sx | MAX_OPEN_POSITIONS=%s",
        TESTNET, DRY_RUN, MAX_LEVERAGE, MAX_OPEN_POSITIONS
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
# DATA  (LOOK-AHEAD BIAS KORUMASI DAHİL)
# ============================================================
# Binance'in son döndürdüğü mum, HENÜZ KAPANMAMIŞ olan o anki
# candle'dır. Sinyal üretiminde bu mumu kullanmak look-ahead bias
# yaratır (henüz oluşmamış bir sonucu "biliyormuş" gibi davranmak).
# Bu yüzden fetch_ohlcv_closed() sinyal/indikatör hesaplamaları için
# HER ZAMAN son (kapanmamış) satırı atar. Yalnızca anlık fiyat
# okumaları (fetch_current_price) için ham/güncel veri kullanılır.

def fetch_ohlcv(symbol, timeframe, limit=OHLCV_LIMIT):
    try:
        data = safe_call(exchange.fetch_ohlcv, symbol, timeframe, None, limit)

        if not data:
            return None

        df = pd.DataFrame(
            data,
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

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
    """Sinyal/indikatör hesaplaması için: son (kapanmamış) mum atılır."""
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

    # EMA slope (yön ve hız) — YENİ
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

    # Mum gövde/aralık oranı — breakout kalitesi için (YENİ)
    candle_range = (df["high"] - df["low"]).replace(0, np.nan)
    df["body_ratio"] = (df["close"] - df["open"]).abs() / candle_range

    return df


# ============================================================
# GAINERS / LOSERS / VOLUME  (+ LIKIDITE FILTRESI)
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
        logger.warning("[LIKIDITE] MIN_QUOTE_VOLUME_USDT=%s eşiğini geçen coin yok.", MIN_QUOTE_VOLUME_USDT)
        return [], [], []

    gainers = df.sort_values("percentage", ascending=False).head(25)["symbol"].tolist()
    losers = df.sort_values("percentage", ascending=True).head(25)["symbol"].tolist()
    volume_leaders = df.sort_values("quoteVolume", ascending=False).head(25)["symbol"].tolist()

    return gainers, losers, volume_leaders


# ============================================================
# FUNDING
# ============================================================

def get_funding(symbol):
    try:
        funding = safe_call(exchange.fetch_funding_rate, symbol)
        return safe_float(funding.get("fundingRate"))
    except Exception:
        return 0.0


# ============================================================
# BTC PIYASA REJIMI
# ============================================================

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


# ============================================================
# ANOMALI / PUMP-DUMP FILTRESI
# ============================================================

def detect_anomaly(df):
    if df is None or len(df) < 10:
        return False

    last5 = df.tail(5)

    price_change_pct = (
        (last5["close"].iloc[-1] - last5["close"].iloc[0]) / last5["close"].iloc[0] * 100
    )

    avg_volume_ratio = safe_float(last5["volume_ratio"].mean(), 1)

    if abs(price_change_pct) >= 8 and avg_volume_ratio < 1.3:
        return True

    return False


# ============================================================
# TREND ANALYSIS (4H / 1H ana yön için kullanılır)
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


# ============================================================
# SWING POINTS
# ============================================================

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
# PULLBACK TESPİTİ VE KALİTESİ
# ============================================================

def detect_pullback(df, direction):
    """Ana trend içinde kısa vadeli geri çekilme arar.

    Önceki sürümde son 6 mumun toplam değişiminin kesinlikle ters yönde
    olması gerekiyordu. Bu, dönüşün başladığı ilk mumlarda setup'ları
    gereksiz yere kaçırabiliyordu. Artık hafif geri çekilme veya yatay
    sıkışma da kabul edilir; kaliteyi assess_pullback_quality belirler.
    """
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
    """Sağlıklı pullback için yumuşatılmış kalite skoru.

    Amaç setup havuzunu biraz genişletmek; buna karşılık gerçek giriş
    trigger'ı hâlâ 5M breakout/momentum teyidi istemeye devam eder.
    """
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


# ============================================================
# CHART PATTERN ENGINE — OBO / TOBO / FLAG
# ============================================================

def _pct_diff(a, b):
    base = max(abs(a), abs(b), 1e-12)
    return abs(a - b) / base


def detect_chart_patterns(df, direction):
    """Klasik indikatör teyidine alternatif fırsat yolu.

    OBO/TOBO için swing noktalarını, flag için impulse + sıkışma + breakout
    yapısını kullanır. Pattern tek başına giriş değildir; analyze pipeline
    pattern breakoutunu ayrıca volume/momentum/candle ile teyit eder.
    """
    result = {"detected": False, "type": None, "direction": direction,
              "score": 0, "break_level": None, "details": {}}
    if df is None or len(df) < 45:
        return result

    x = df.iloc[-1]
    atr_val = safe_float(x.get("atr"))
    close = safe_float(x.get("close"))
    if atr_val <= 0 or close <= 0:
        return result

    work = df.tail(PATTERN_LOOKBACK).reset_index(drop=True)
    highs, lows = find_swing_points(work, window=2)

    # OBO (bearish): left shoulder < head, right shoulder yaklaşık left shoulder.
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
                    result.update({"detected": True, "type": "OBO", "score": clamp(score, 0, 100),
                                   "break_level": neckline,
                                   "details": {"left_shoulder": a[1], "head": b[1],
                                               "right_shoulder": c[1], "neckline": neckline}})

    # TOBO / inverse H&S (bullish).
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
                    result.update({"detected": True, "type": "TOBO", "score": clamp(score, 0, 100),
                                   "break_level": neckline,
                                   "details": {"left_shoulder": a[1], "head": b[1],
                                               "right_shoulder": c[1], "neckline": neckline}})

    # Flag: son impulse + kontrollü daralan karşı eğimli konsolidasyon.
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
            result.update({"detected": True, "type": "BULL_FLAG" if direction == "long" else "BEAR_FLAG",
                           "score": clamp(score, 0, 100), "break_level": breakout_level,
                           "details": {"impulse_atr": impulse_atr, "flag_range": flag_range, "broken": broken}})

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
# MOMENTUM REVERSAL / ACCELERATION
# ============================================================

def detect_momentum_reversal(df, direction):
    """WATCH aşaması: karşı yönlü momentum zayıflıyor mu, RSI dipten
    dönüyor mu, MACD histogram iyileşiyor mu?"""
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
    """ARM aşaması: son 3-4 mumun MACD histogram DEĞİŞİM HIZI.
    Sadece pozitif/negatif değil, ivmelenme derecesini 0-100 arası
    puanlar."""
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

    # İvmenin kendisi büyüyor mu (acceleration of acceleration)?
    if len(slopes) >= 2:
        accel_of_accel = slopes[-1] - slopes[-2]
        if (direction == "long" and accel_of_accel > 0) or (direction == "short" and accel_of_accel < 0):
            score += 15

    score = clamp(score, 0, 100)

    return {"accelerating": score >= 60, "score": score}


# ============================================================
# MICRO STRUCTURE BREAK
# ============================================================

def detect_micro_structure_break(df, direction):
    """LONG: son lower-high (LH) kırılmalı (fiyat kapanışı LH'nin
    üzerinde). SHORT: son higher-low (HL) kırılmalı."""
    if df is None or len(df) < 30:
        return {"broken": False, "level": None}

    swing_highs, swing_lows = find_swing_points(df, window=2)

    last_close = df["close"].iloc[-1]

    if direction == "long":
        if len(swing_highs) < 2:
            return {"broken": False, "level": None}

        (i1, h1), (i2, h2) = swing_highs[-2:]

        # Lower-high paterni: son tepe bir öncekinden düşük olmalı
        if h2 < h1:
            if last_close > h1:
                return {"broken": True, "level": h1}

        return {"broken": False, "level": h1}

    else:
        if len(swing_lows) < 2:
            return {"broken": False, "level": None}

        (i1, l1), (i2, l2) = swing_lows[-2:]

        # Higher-low paterni: son dip bir öncekinden yüksek olmalı
        if l2 > l1:
            if last_close < l1:
                return {"broken": True, "level": l1}

        return {"broken": False, "level": l1}


# ============================================================
# BREAKOUT CONFIRMATION / RETEST
# ============================================================

def confirm_breakout(df, level, direction):
    """FALSE BREAKOUT FİLTRESİ dahil: sadece fitil (wick) değil,
    mum GÖVDESİ kırılım seviyesini geçmiş olmalı; hacim ve ATR'ye
    göre anlamlı bir mum olmalı."""
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
        body_breaks = close > level and open_ < close  # yeşil mum, kapanış seviyenin üstünde
        meaningful = (close - level) >= atr_val * 0.15
    else:
        body_breaks = close < level and open_ > close
        meaningful = (level - close) >= atr_val * 0.15

    volume_ok = volume_ratio >= MIN_BREAKOUT_VOLUME_RATIO
    body_ok = body_ratio >= 0.35  # zayıf/fitilli mumları ele

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
    """Kırılım sonrası seviyeye geri dönüş (retest), seviyenin
    tutması ve momentumun yeniden başlaması. Daha yüksek kaliteli
    ama daha az sıklıkta oluşan bir giriş tipi."""
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


# ============================================================
# ENTRY CHASING FİLTRESİ
# ============================================================

def is_entry_chasing(df, direction, break_level):
    """Kırılım gerçekleşti ama fiyat zaten ATR'nin çok üzerinde
    uzaklaşmışsa (geç kalınmış giriş / chase) işlem açma."""
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


# ============================================================
# EXPECTED MOVE
# ============================================================

def calculate_expected_move(df, direction, entry_price, leverage):
    """Entry'den itibaren MIN_TARGET_ROI'yi (%10 ROI) sağlayacak
    fiyat hareketi var mı? En yakın destek/direnç (swing high/low)
    bu hareketten önce geliyorsa işlem reddedilir."""
    if df is None or entry_price <= 0 or leverage <= 0:
        return {"sufficient": False, "required_move_pct": None, "nearest_level": None}

    required_move_pct = MIN_TARGET_ROI / leverage  # ROI% -> ham fiyat %

    swing_highs, swing_lows = find_swing_points(df, window=3)

    if direction == "long":
        levels_above = [h for _, h in swing_highs if h > entry_price]
        nearest_level = min(levels_above) if levels_above else None

        if nearest_level:
            available_move_pct = (nearest_level - entry_price) / entry_price * 100
        else:
            available_move_pct = required_move_pct * 3  # üstte belirgin bir engel yok

    else:
        levels_below = [l for _, l in swing_lows if l < entry_price]
        nearest_level = max(levels_below) if levels_below else None

        if nearest_level:
            available_move_pct = (entry_price - nearest_level) / entry_price * 100
        else:
            available_move_pct = required_move_pct * 3

    sufficient = available_move_pct >= required_move_pct

    return {
        "sufficient": sufficient,
        "required_move_pct": required_move_pct,
        "available_move_pct": available_move_pct,
        "nearest_level": nearest_level,
    }


# ============================================================
# MOMENTUM (yön oylaması için — eski koddan korunan yardımcı)
# ============================================================

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
# SETUP SCORE  (4H/1H/15M zemin kalitesi)
# ============================================================

def calculate_setup_score(direction, trend4h, trend1h, pullback_quality, atr_pct, funding, volume_ratio, market_regime_ok):
    score = 0
    breakdown = {}

    # Market regime (BTC ile uyum) — 15
    regime_pts = 15 if market_regime_ok else 5
    score += regime_pts
    breakdown["market_regime"] = regime_pts

    # 4H trend — 15
    trend4h_pts = (trend4h["strength"] / 100) * 15 if trend4h["direction"] == direction else 0
    score += trend4h_pts
    breakdown["4h_trend"] = round(trend4h_pts, 1)

    # 1H trend — 15
    trend1h_pts = (trend1h["strength"] / 100) * 15 if trend1h["direction"] == direction else 0
    score += trend1h_pts
    breakdown["1h_trend"] = round(trend1h_pts, 1)

    # 15M yapı (pullback var olması + kaliteli olması) — 15
    structure_pts = 15 if pullback_quality.get("healthy") else 5
    score += structure_pts
    breakdown["15m_structure"] = structure_pts

    # Pullback quality — 20
    pullback_pts = (pullback_quality.get("score", 0) / 100) * 20
    score += pullback_pts
    breakdown["pullback_quality"] = round(pullback_pts, 1)

    # Volume environment — 10
    volume_pts = clamp((volume_ratio - 0.8) * 25, 0, 10)
    score += volume_pts
    breakdown["volume_env"] = round(volume_pts, 1)

    # Volatilite (ne çok düşük ne kaotik) — 5
    if 0.3 <= atr_pct <= 4.0:
        volat_pts = 5
    elif atr_pct < 0.3:
        volat_pts = 1
    else:
        volat_pts = 2
    score += volat_pts
    breakdown["volatility"] = volat_pts

    # Funding — 5
    funding_pts = 5 if abs(funding) < FUNDING_SKIP_THRESHOLD * 0.5 else 2
    score += funding_pts
    breakdown["funding"] = funding_pts

    return round(clamp(score, 0, 100), 2), breakdown


# ============================================================
# TRIGGER SCORE  (5M giriş zamanlaması kalitesi)
# ============================================================

def calculate_trigger_score(momentum_accel, rsi_turning, ema_slope_ok, structure_break, breakout_result, atr_position_ok):
    score = 0
    breakdown = {}
    confirmations = 0

    # MACD acceleration — 20
    macd_pts = (momentum_accel.get("score", 0) / 100) * 20
    score += macd_pts
    breakdown["macd_acceleration"] = round(macd_pts, 1)
    if momentum_accel.get("accelerating"):
        confirmations += 1

    # RSI directional turn — 15
    rsi_pts = 15 if rsi_turning else 0
    score += rsi_pts
    breakdown["rsi_turn"] = rsi_pts
    if rsi_turning:
        confirmations += 1

    # EMA slope — 10
    ema_pts = 10 if ema_slope_ok else 0
    score += ema_pts
    breakdown["ema_slope"] = ema_pts

    # Micro structure break — 25
    structure_pts = 25 if structure_break.get("broken") else 0
    score += structure_pts
    breakdown["structure_break"] = structure_pts
    if structure_break.get("broken"):
        confirmations += 1

    # Breakout volume — 15
    breakout_pts = 15 if breakout_result.get("confirmed") else 0
    score += breakout_pts
    breakdown["breakout_volume"] = breakout_pts

    # Candle close (body bazlı — breakout_result içinde zaten kontrol edildi) — 10
    candle_pts = 10 if breakout_result.get("confirmed") else 0
    score += candle_pts
    breakdown["candle_close"] = candle_pts

    # ATR position (aşırı uzamamış olmak) — 5
    atr_pos_pts = 5 if atr_position_ok else 0
    score += atr_pos_pts
    breakdown["atr_position"] = atr_pos_pts

    return round(clamp(score, 0, 100), 2), breakdown, confirmations


# ============================================================
# ANA PIPELINE: SETUP -> WATCH -> ARM -> FIRE
# ============================================================

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

    # --------------------------------------------------------
    # SETUP: klasik pullback veya chart pattern
    # Pattern motoru pullback filtresinden bağımsız taranır; böylece
    # pattern kaynaklı fırsatlar diagnostics'te gerçekten görünür.
    # --------------------------------------------------------
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
    # Patternlerde klasik 15M reversal zorunlu değil; patternin yapısı zaten
    # setup teyidini sağlıyor. Yine de mevcut momentum yönü güçlü şekilde tersse reddet.
    if not momentum_reversal_15m and not pattern_setup:
        diag_reject(symbol, "no_15m_reversal")
        return None
    diag_stage(symbol, "WATCH_OK", f"15m_reversal={momentum_reversal_15m}")

    # --------------------------------------------------------
    # ARM — 5M momentum
    # --------------------------------------------------------
    momentum_accel = calculate_momentum_acceleration(tf_data["5m"], direction)
    rsi_vals_5m = tf_data["5m"]["rsi"].tail(4).values
    rsi_turning = (rsi_vals_5m[-1] > rsi_vals_5m[0]) if direction == "long" else (rsi_vals_5m[-1] < rsi_vals_5m[0])

    ema9_slope = safe_float(tf_data["5m"].iloc[-1]["ema9_slope"])
    ema21_slope = safe_float(tf_data["5m"].iloc[-1]["ema21_slope"])
    ema_slope_ok = ((ema9_slope > 0 and ema21_slope >= -abs(ema9_slope) * 0.5)
                    if direction == "long" else
                    (ema9_slope < 0 and ema21_slope <= abs(ema9_slope) * 0.5))

    # --------------------------------------------------------
    # FIRE — klasik micro break veya pattern breakout
    # --------------------------------------------------------
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
        diag_inc("no_breakout")
        diag_reject(symbol, "no_breakout", "micro_structure/pattern breakout yok")
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

    # --------------------------------------------------------
    # SCORE — biraz gevşetilmiş, pattern için ayrı alt sınır
    # --------------------------------------------------------
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

    # Klasik pullbackta 2 teyit yerine 1 güçlü trigger yeterli olabilir;
    # pattern breakoutunda ise breakout + en az bir momentum/RSI teyidi gerekir.
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
        diag_reject(symbol, "expected_move", f"available={safe_float(expected_move.get('available_move_pct')):.2f}% required={safe_float(expected_move.get('required_move_pct')):.2f}%")
        return None
    diag_stage(symbol, "EXPECTED_MOVE_OK", f"available={expected_move.get('available_move_pct'):.2f}% required={expected_move.get('required_move_pct'):.2f}%")

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
# LEVERAGE
# ============================================================

def calculate_leverage(setup_score, trigger_score, atr_pct):
    combined = (setup_score + trigger_score) / 2

    leverage = MIN_LEVERAGE + 1  # taban 2x

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


# ============================================================
# DINAMIK ATR STOP
# ============================================================
# "ATR stop daha yakınsa onu kullan, çok uzaksa maksimum risk
# sınırına göre daralt" mantığı:
#   1) natural_distance = max(ATR mesafesi, yapı mesafesi) — gerçek
#      geçersizlik noktasını temsil eder (ikisinin de ötesi = trend bozuk)
#   2) max_risk_distance = hedefin (%10 ROI) %50'si karşılığı ham fiyat mesafesi
#   3) final = min(natural_distance, max_risk_distance)
#      -> natural zaten dar ise onu kullan, genişse risk sınırına daralt

def calculate_dynamic_atr_stop(df, direction, entry_price, leverage, structure_level):
    x = df.iloc[-1]
    atr_val = safe_float(x["atr"])

    if atr_val <= 0 or entry_price <= 0:
        atr_val = entry_price * 0.01  # aşırı durumda %1 fallback

    atr_distance = atr_val * INITIAL_STOP_ATR_MULTIPLIER

    if direction == "long":
        structure_distance = max(entry_price - structure_level, atr_val * 0.5) if structure_level else atr_distance
    else:
        structure_distance = max(structure_level - entry_price, atr_val * 0.5) if structure_level else atr_distance

    natural_distance = max(atr_distance, structure_distance)

    max_loss_roi = MIN_TARGET_ROI * MAX_LOSS_TO_TARGET_RATIO  # örn. %5
    max_risk_distance = (max_loss_roi / 100 / leverage) * entry_price

    final_distance = min(natural_distance, max_risk_distance)
    final_distance = max(final_distance, entry_price * 0.0015)  # aşırı küçük stop'u engelle

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


# ============================================================
# KORELASYON KONTROLÜ
# ============================================================

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
    """Yeni aday, açık pozisyonlardan biriyle yüksek korelasyonlu mu?
    (Aynı BTC hareketine bağımlı 3 pozisyon açmayı önlemek için.)"""
    with state_lock:
        open_symbols = [p["symbol"] for p in local_positions.values()]

    if not open_symbols:
        return False

    candidate_returns = get_recent_returns(symbol)

    if candidate_returns is None:
        return False  # veri yoksa engelleme, sessizce izin ver

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
            logger.info(
                "[KORELASYON] %s, açık pozisyon %s ile yüksek korelasyonlu (%.2f) — elendi.",
                symbol, open_symbol, corr
            )
            return True

    return False


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


def local_position_count():
    with state_lock:
        return len(local_positions)


def has_local_symbol(symbol):
    normalized = normalize_symbol(symbol)
    with state_lock:
        return any(normalize_symbol(p["symbol"]) == normalized for p in local_positions.values())


# ============================================================
# BINANCE POSITIONS  (API FAILURE != ZERO POSITION)
# ============================================================

class PositionFetchError(Exception):
    pass


def fetch_real_positions():
    """API çağrısı başarısız olursa exception fırlatır — ASLA
    sessizce boş liste döndürmez. Çünkü 'API hatası' ile 'gerçekten
    pozisyon yok' birbirinden kesinlikle ayrılmalıdır; aksi halde
    geçici bir API hatasında bot açık pozisyonları local state'ten
    yanlışlıkla silebilir."""
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


def sync_real_positions():
    if DRY_RUN:
        return

    try:
        real = fetch_real_positions()
    except PositionFetchError as e:
        logger.warning("Pozisyon senkronizasyonu atlandı (API hatası, state korunuyor): %s", e)
        return

    with state_lock:
        real_symbols = {normalize_symbol(p["symbol"]) for p in real}
        remove = [
            key for key, local in local_positions.items()
            if normalize_symbol(local["symbol"]) not in real_symbols
        ]
        for key in remove:
            logger.warning(
                "[SENKRON] %s borsada artık açık değil (muhtemelen native stop/TP tetiklendi), local state'ten kaldırılıyor.",
                local_positions[key]["symbol"]
            )
            local_positions.pop(key, None)


def recover_positions_from_exchange():
    """RESTART / POSITION RECOVERY: Bot yeniden başladığında Binance'te
    açık olan gerçek pozisyonları okuyup local state'e geri yükler.
    RAM tek gerçek kaynak DEĞİLDİR."""
    if DRY_RUN:
        logger.info("[RECOVERY] DRY_RUN aktif, gerçek pozisyon kurtarma atlanıyor.")
        return

    try:
        real = fetch_real_positions()
    except PositionFetchError as e:
        logger.error(
            "[RECOVERY] Açık pozisyonlar okunamadı (%s) — bot, mevcut pozisyonlar hakkında "
            "BİLGİSİZ başlıyor. Manuel kontrol önerilir.", e
        )
        return

    if not real:
        logger.info("[RECOVERY] Borsada açık pozisyon bulunamadı, temiz başlangıç.")
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

            # Orijinal setup verisi bilinmediğinden konservatif
            # varsayılan hedef/stop ile geri yükleniyor.
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
                "initial_stop_price": None,  # bilinmiyor — failsafe/ATR ile yeniden kurulacak
                "current_stop_price": None,
                "profit_lock_active": False,
                "peak_price": entry_price,
                "trough_price": entry_price,
                "opened_at": now_ms(),
                "last_monitor": now_ms(),
                "last_trend_check": 0,
                "stop_order_id": None,
                "chart_pattern": None,
                "pattern_details": {},
                "recovered": True,
            }

            logger.warning(
                "[RECOVERY] %s %s pozisyonu geri yüklendi | entry=%s | lev=%sx | "
                "(orijinal stop/hedef bilinmiyor, ATR ile yeniden hesaplanacak)",
                side.upper(), symbol, entry_price, leverage
            )


# ============================================================
# POSITION LIMIT
# ============================================================

def can_open_more():
    return local_position_count() < MAX_OPEN_POSITIONS


# ============================================================
# ENTRY PRICE / QUANTITY
# ============================================================

def fetch_current_price(symbol):
    ticker = safe_call(exchange.fetch_ticker, symbol)
    return safe_float(ticker.get("last"))


def fetch_current_price_fast(symbol):
    try:
        ticker = safe_call(exchange.fetch_ticker, symbol, retries=1, delay=0.3)
        return safe_float(ticker.get("last"))
    except Exception:
        return 0.0


def calculate_amount(margin, leverage, price):
    notional = margin * leverage
    return notional / price


# ============================================================
# BORSA SEVIYESINDE FAILSAFE STOP EMRI
# ============================================================
# Botun yazılım tabanlı takibi API gecikmesi/rate limit/çökme
# durumunda geç kalabilir. Bu yüzden gerçek işlemlerde pozisyon
# açılır açılmaz Binance'e GERÇEK bir STOP_MARKET emri gönderilir.
# side/positionSide/reduceOnly/quantity/stopPrice parametreleri
# pozisyon yönüne göre KESİN doğru olacak şekilde ayarlanır.

HARD_STOP_BUFFER_LOCAL = HARD_STOP_BUFFER


def place_stop_market_order(symbol, position_side, amount, stop_price):
    """position_side: 'long' ya da 'short' (pozisyonun kendi yönü,
    emrin side'ı değil). Emrin side'ı bunun tersidir (kapatma emri)."""
    try:
        close_side = "sell" if position_side == "long" else "buy"

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
            "[FAILSAFE STOP] %s | pozisyon_yönü=%s | emir_yönü=%s | stop_price=%s",
            symbol, position_side, close_side, stop_price
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
# TRADE JOURNAL
# ============================================================

def write_trade_journal(entry):
    logger.warning("[JOURNAL] %s", json.dumps(entry, default=str, ensure_ascii=False))

    try:
        with open(TRADE_JOURNAL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.info("Trade journal dosyaya yazılamadı (best-effort, sorun değil): %s", e)


def estimate_net_roi(gross_roi, leverage):
    """Tahmini net ROI: giriş + çıkış taker komisyonu, kaldıraçlı
    ROI üzerinden yaklaşık olarak düşülür."""
    fee_roi_cost = TAKER_FEE_PCT * 2 * leverage
    return gross_roi - fee_roi_cost


# ============================================================
# POZİSYON AÇMA
# ============================================================

def open_position(candidate):
    symbol = candidate["symbol"]
    direction = candidate["direction"]

    if is_on_cooldown(symbol):
        return False
    if not can_open_more():
        return False
    if has_local_symbol(symbol):
        return False
    if is_correlated_with_open_positions(symbol):
        return False

    leverage = candidate["leverage"]
    price = candidate["price"]

    stop_info = calculate_dynamic_atr_stop(
        candidate["data_5m"], direction, price, leverage, candidate["structure_level"]
    )

    key = f"hc:{normalize_symbol(symbol)}"

    if DRY_RUN:
        amount = calculate_amount(MARGIN_PER_TRADE, leverage, price)
        entry_price = price
        stop_order_id = None
    else:
        with state_lock:
            if len(local_positions) >= MAX_OPEN_POSITIONS or has_local_symbol(symbol):
                return False

        real_positions = fetch_real_positions_safe()
        if real_positions is None:
            logger.warning("%s pozisyon açma iptal — API durumu belirsiz.", symbol)
            return False

        normalized = normalize_symbol(symbol)
        for p in real_positions:
            if normalize_symbol(p["symbol"]) == normalized and abs(safe_float(p["contracts"])) > 0:
                logger.info("%s zaten açık pozisyon. Emir iptal.", symbol)
                return False

        set_isolated_and_leverage(symbol, leverage)

        fresh_price = fetch_current_price(symbol)
        if fresh_price <= 0:
            return False

        amount = calculate_amount(MARGIN_PER_TRADE, leverage, fresh_price)
        amount = float(format_amount(symbol, amount))

        if amount <= 0:
            return False

        side = "buy" if direction == "long" else "sell"

        order = safe_call(
            exchange.create_order,
            symbol, "market", side, amount, None,
            {"positionSide": "BOTH"}
        )

        entry_price = fresh_price
        try:
            filled = safe_float(order.get("average"))
            if filled > 0:
                entry_price = filled
        except Exception:
            pass

        # Stop fiyatını gerçek entry'ye göre yeniden hesapla
        stop_info = calculate_dynamic_atr_stop(
            candidate["data_5m"], direction, entry_price, leverage, candidate["structure_level"]
        )

        # Native failsafe, yazılımsal risk tavanını ASLA aşmamalı.
        native_stop_distance = min(
            stop_info["distance"] * HARD_STOP_BUFFER_LOCAL,
            stop_info["max_risk_distance"]
        )
        if direction == "long":
            native_stop_price = entry_price - native_stop_distance
        else:
            native_stop_price = entry_price + native_stop_distance

        stop_order_id = place_stop_market_order(symbol, direction, amount, native_stop_price)

    with state_lock:
        if key in local_positions:
            return False
        if len(local_positions) >= MAX_OPEN_POSITIONS:
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

    logger.warning(
        "[%s AÇILDI] %s %s | entry=%s | stop=%s | lev=%sx | margin=%s$ | "
        "setup=%.1f trigger=%.1f | tip=%s/%s | pattern=%s",
        "DRY RUN" if DRY_RUN else "REAL", direction.upper(), symbol,
        entry_price, stop_info["stop_price"], leverage, MARGIN_PER_TRADE,
        candidate["setup_score"], candidate["trigger_score"],
        candidate["setup_type"], candidate["breakout_type"], candidate.get("chart_pattern")
    )

    return True


def fetch_real_positions_safe():
    try:
        return fetch_real_positions()
    except PositionFetchError:
        return None


# ============================================================
# ROI
# ============================================================

def calculate_roi(position, price):
    entry = safe_float(position["entry_price"])
    leverage = safe_float(position["leverage"], 1)

    if entry <= 0:
        return 0.0

    if position["side"] == "long":
        price_change = (price - entry) / entry
    else:
        price_change = (entry - price) / entry

    return price_change * leverage * 100


# ============================================================
# CANLI TREND/MOMENTUM/YAPI KONTROLÜ (erken çıkış teyidi için)
# ============================================================

def live_reversal_check(symbol, direction):
    """Trend, momentum ve yapı bozulmasını AYRI AYRI değerlendirir;
    erken çıkış için REQUIRED_REVERSAL_CONFIRMATIONS kadar ters
    sinyal aynı anda gerekir — tek bir zayıf sinyal yetmez."""
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

        # 1) Trend reversal: 5m VE 15m ikisi de belirgin ters yönde
        trend_reversed = (
            t5["direction"] not in ("neutral", direction) and t5["strength"] >= 55
            and t15["direction"] not in ("neutral", direction) and t15["strength"] >= 50
        )
        details["trend_reversed"] = trend_reversed
        if trend_reversed:
            confirmations += 1

        # 2) Momentum reversal: 5m momentum belirgin ters yönde
        momentum_reversed = m5["direction"] not in ("neutral", direction) and m5["strength"] >= 55
        details["momentum_reversed"] = momentum_reversed
        if momentum_reversed:
            confirmations += 1

        # 3) Structure failure: fiyat son 5m yapısal seviyeyi (recent_high/low) ters yönde kırdı
        last = df5.iloc[-1]
        close = safe_float(last["close"])

        if direction == "long":
            structure_level = safe_float(df5.iloc[-2]["recent_low"])
            structure_failed = close < structure_level
        else:
            structure_level = safe_float(df5.iloc[-2]["recent_high"])
            structure_failed = close > structure_level

        details["structure_failed"] = structure_failed
        if structure_failed:
            confirmations += 1

        return {"confirmations": confirmations, "details": details}

    except Exception as e:
        logger.warning("%s canlı reversal kontrolü başarısız: %s", symbol, e)
        return {"confirmations": 0, "details": {}}


# ============================================================
# PROFIT LOCK
# ============================================================

def update_profit_lock(position, roi):
    target = position["target_roi"]
    trigger = target * PROFIT_LOCK_TRIGGER_RATIO

    if roi >= trigger and not position["profit_lock_active"]:
        position["profit_lock_active"] = True
        logger.warning(
            "[PROFIT LOCK AKTİF] %s | ROI=%.2f%% (tetik=%.2f%%)",
            position["symbol"], roi, trigger
        )


# ============================================================
# ATR TRAILING STOP
# ============================================================
# Profit lock aktifleştikten sonra devreye girer. Stop, ATR'ye göre
# fiyatı takip eder ama ASLA pozisyon aleyhine gevşetilmez (LONG'da
# geri düşemez, SHORT'ta geri yükselemez).

def update_atr_trailing_stop(position, price, current_atr):
    if not position["profit_lock_active"]:
        return

    distance = current_atr * TRAILING_ATR_MULTIPLIER

    if position["side"] == "long":
        if price > position["peak_price"]:
            position["peak_price"] = price

        candidate_stop = position["peak_price"] - distance

        # Stop asla mevcut stop'un altına inemez (gevşetilemez) VE
        # asla entry'nin altında kalamaz (profit lock sonrası en az
        # breakeven garanti edilir).
        floor_stop = max(position["current_stop_price"], position["entry_price"])
        position["current_stop_price"] = max(floor_stop, candidate_stop)

    else:
        if price < position["trough_price"]:
            position["trough_price"] = price

        candidate_stop = position["trough_price"] + distance

        ceiling_stop = min(position["current_stop_price"], position["entry_price"])
        position["current_stop_price"] = min(ceiling_stop, candidate_stop)


# ============================================================
# EXIT DECISION
# ============================================================

def should_close_position(position, price):
    roi = calculate_roi(position, price)
    position["current_roi"] = roi

    if position["side"] == "long":
        if price > position["peak_price"]:
            position["peak_price"] = price
    else:
        if price < position["trough_price"]:
            position["trough_price"] = price

    # RECOVERY pozisyonlarında orijinal stop bilinmiyordu — ilk
    # kontrolde ATR bazlı bir stop hesaplayıp atıyoruz (yapı seviyesi
    # olmadan, sadece ATR ile — konservatif tahmin).
    if position.get("initial_stop_price") is None:
        try:
            df5 = fetch_ohlcv_closed(position["symbol"], "5m", 60)
            if df5 is not None:
                df5 = enrich_dataframe(df5)
                stop_info = calculate_dynamic_atr_stop(
                    df5, position["side"], position["entry_price"],
                    position["leverage"], None
                )
                position["initial_stop_price"] = stop_info["stop_price"]
                position["current_stop_price"] = stop_info["stop_price"]
                logger.warning(
                    "[RECOVERY STOP] %s için ATR bazlı stop hesaplandı: %s",
                    position["symbol"], stop_info["stop_price"]
                )
        except Exception as e:
            logger.warning("%s recovery stop hesaplanamadı: %s", position["symbol"], e)

    target = position["target_roi"]
    max_loss_roi = target * MAX_LOSS_TO_TARGET_RATIO

    # Mutlak güvenlik tavanı: hedefin %50'si.
    # API/fiyat gecikmesi nedeniyle gerçekleşen zarar farklı olabilir, ancak
    # yazılımsal karar katmanı bu seviyenin ötesini kabul etmez.
    hard_cap = max_loss_roi
    if roi <= -hard_cap:
        return True, "HARD_FAILSAFE_STOP"

    # --------------------------------------------------------
    # Aşama 1 — Initial Risk (ATR + yapı bazlı stop, fiyat seviyesinde)
    # --------------------------------------------------------

    initial_stop = position.get("initial_stop_price")

    if initial_stop and not position["profit_lock_active"]:
        if position["side"] == "long" and price <= initial_stop:
            return True, "INITIAL_STOP"
        if position["side"] == "short" and price >= initial_stop:
            return True, "INITIAL_STOP"

    # ROI bazlı ek güvenlik (stop hesaplanamadıysa dahi asla aşılmasın)
    if roi <= -max_loss_roi:
        return True, "MAX_LOSS_CAP"

    # --------------------------------------------------------
    # Aşama 2/3 — Profit lock + ATR trailing
    # --------------------------------------------------------

    update_profit_lock(position, roi)

    current_time = now_ms()

    # ATR'yi periyodik olarak tazele (her check'te ağır hesap yapmamak için)
    if (current_time - position.get("last_trend_check", 0)) >= LIVE_CHECK_INTERVAL_MS:
        position["last_trend_check"] = current_time

        try:
            df5 = fetch_ohlcv_closed(position["symbol"], "5m", 60)
            if df5 is not None:
                df5 = enrich_dataframe(df5)
                current_atr = safe_float(df5.iloc[-1]["atr"])

                if position["profit_lock_active"] and current_atr > 0:
                    update_atr_trailing_stop(position, price, current_atr)
        except Exception as e:
            logger.warning("%s ATR tazeleme hatası: %s", position["symbol"], e)

        # --------------------------------------------------------
        # Aşama 4 — Gerçek trend/momentum/yapı çöküşü kontrolü
        # --------------------------------------------------------
        # Küçük bir geri çekilme yüzünden kazanan pozisyon kapatılmaz.
        # Sadece REQUIRED_REVERSAL_CONFIRMATIONS kadar (varsayılan 2)
        # bağımsız ters sinyal AYNI ANDA varsa erken çıkılır.

        reversal = live_reversal_check(position["symbol"], position["side"])

        if reversal["confirmations"] >= REQUIRED_REVERSAL_CONFIRMATIONS:
            logger.warning(
                "[REVERSAL TEYİDİ] %s | %s/%s ters sinyal: %s",
                position["symbol"], reversal["confirmations"],
                REQUIRED_REVERSAL_CONFIRMATIONS, reversal["details"]
            )
            return True, "TREND_MOMENTUM_STRUCTURE_REVERSAL"

    # Profit lock sonrası trailing stop'a değdi mi?
    if position["profit_lock_active"]:
        stop = position["current_stop_price"]
        if position["side"] == "long" and price <= stop:
            return True, "PROFIT_LOCK_TRAILING_STOP"
        if position["side"] == "short" and price >= stop:
            return True, "PROFIT_LOCK_TRAILING_STOP"

    return False, None


# ============================================================
# CLOSE POSITION
# ============================================================

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
        "target_roi": position.get("target_roi"),
        "initial_stop": position.get("initial_stop_price"),
        "final_stop": position.get("current_stop_price"),
        "peak_price": position.get("peak_price"),
        "trough_price": position.get("trough_price"),
        "gross_roi": round(roi, 3),
        "estimated_net_roi": round(net_roi, 3),
        "holding_time_sec": round(holding_ms / 1000, 1),
        "exit_reason": reason,
        "setup_score": position.get("setup_score"),
        "trigger_score": position.get("trigger_score"),
        "setup_type": position.get("setup_type"),
        "breakout_type": position.get("breakout_type"),
        "chart_pattern": position.get("chart_pattern"),
        "pattern_details": position.get("pattern_details", {}),
        "profit_lock_active": position.get("profit_lock_active"),
        "recovered": position.get("recovered", False),
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

    logger.warning(
        "[DRY RUN KAPANDI] %s %s | gross_roi=%.2f%% | net_roi=%.2f%% | sebep=%s",
        entry["side"].upper(), entry["symbol"],
        journal_entry["gross_roi"], journal_entry["estimated_net_roi"], reason
    )

    bot_stats["closed_positions"] += 1
    return True


def close_real_position(key, reason, exit_price):
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

        cancel_stop_order(symbol, position.get("stop_order_id"))

        with state_lock:
            local_positions.pop(key, None)

        journal_entry = build_journal_entry(position, exit_price, reason)
        write_trade_journal(journal_entry)

        logger.warning(
            "[REAL KAPANDI] %s %s | gross_roi=%.2f%% | net_roi=%.2f%% | sebep=%s",
            position["side"].upper(), symbol,
            journal_entry["gross_roi"], journal_entry["estimated_net_roi"], reason
        )

        bot_stats["closed_positions"] += 1
        return True

    except Exception as e:
        logger.error("%s kapatma hatası: %s", symbol, e)
        return False


def close_position(key, reason, exit_price):
    return close_dry_position(key, reason, exit_price) if DRY_RUN else close_real_position(key, reason, exit_price)


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
                        peak = current.get("peak_price")
                        stop = current.get("current_stop_price")
                        lock = current.get("profit_lock_active")

                    logger.info(
                        "[MONITOR] %s | %s | price=%s | ROI=%+.2f%% | peak/trough=%s | "
                        "stop=%s | lock=%s | hedef=%.1f%%",
                        symbol, position["side"].upper(), price, roi, peak, stop, lock,
                        current.get("target_roi", MIN_TARGET_ROI)
                    )

                    if should_close:
                        close_position(key, reason, price)

                except Exception as e:
                    logger.warning("%s monitor hatası: %s", symbol, e)

            time.sleep(POSITION_MONITOR_INTERVAL)

        except Exception as e:
            logger.error("Monitor ana hata: %s", e)
            time.sleep(2)


# ============================================================
# FINAL ENTRY CONFIRMATION
# ============================================================

def final_entry_confirmation(candidate):
    """Emirden hemen önce en güncel kapanmış 5m verisiyle son teyit.
    Karar ve red nedeni diagnostics'e açık şekilde yazılır."""
    symbol = candidate["symbol"]
    direction = candidate["direction"]
    reason = ""
    try:
        if normalize_symbol(symbol) in {normalize_symbol(x) for x in EXCLUDED_TRADE_SYMBOLS}:
            diag_inc("final_confirmation")
            logger.info("[HC FINAL] %s | EXCLUDED_TRADE_SYMBOL", symbol)
            return False

        df5 = fetch_ohlcv_closed(symbol, "5m", 100)
        if df5 is None:
            diag_inc("final_confirmation")
            diag_inc("final_reason_no_data")
            log_final_decision(symbol, direction, None, pd.DataFrame(), None, accepted=False, reason="NO_DATA")
            return False
        df5 = enrich_dataframe(df5)

        structure_break = detect_micro_structure_break(df5, direction)
        pattern = detect_chart_patterns(df5, direction) if candidate.get("chart_pattern") else {"detected": False}
        level = None
        breakout_result = {"confirmed": False, "type": None, "reasons": []}

        if structure_break.get("broken"):
            level = structure_break.get("level")
            breakout_result = confirm_breakout(df5, level, direction)
            if not breakout_result.get("confirmed"):
                breakout_result = confirm_breakout_retest(df5, level, direction)
            if not breakout_result.get("confirmed") and pattern.get("detected"):
                level = pattern.get("break_level")
                breakout_result = confirm_pattern_breakout(df5, pattern, direction)
        elif pattern.get("detected"):
            level = pattern.get("break_level")
            breakout_result = confirm_pattern_breakout(df5, pattern, direction)
        else:
            reason = "BREAKOUT"
            diag_inc("final_confirmation")
            diag_inc("final_reason_breakout")
            log_final_decision(symbol, direction, level, df5, breakout_result, accepted=False, reason=reason)
            return False

        current = df5.iloc[-1]
        momentum_accel = calculate_momentum_acceleration(df5, direction)
        rsi_vals = df5["rsi"].tail(4).values if "rsi" in df5.columns else []
        rsi_turn = bool(len(rsi_vals) >= 2 and ((rsi_vals[-1] > rsi_vals[-2]) if direction == "long" else (rsi_vals[-1] < rsi_vals[-2])))
        volume_ok = safe_float(current.get("volume_ratio"), 1.0) >= (PATTERN_VOLUME_RATIO if pattern.get("detected") else MIN_BREAKOUT_VOLUME_RATIO)
        retest = "retest" in str(breakout_result.get("type", "")).lower()
        funding = get_funding(symbol)

        if not breakout_result.get("confirmed"):
            reason = "BREAKOUT"
            diag_inc("final_confirmation")
            diag_inc("final_reason_breakout")
        elif is_entry_chasing(df5, direction, level):
            reason = "CHASE"
            diag_inc("final_confirmation")
            diag_inc("entry_chase")
            diag_inc("final_reason_chase")
        elif abs(funding) >= FUNDING_SKIP_THRESHOLD:
            reason = "FUNDING"
            diag_inc("final_confirmation")
            diag_inc("final_reason_funding")
        else:
            log_final_decision(symbol, direction, level, df5, breakout_result,
                               momentum_accel=momentum_accel.get("accelerating"),
                               rsi_turn=rsi_turn, volume_ok=volume_ok, retest=retest,
                               funding=funding, accepted=True, reason="PASS")
            return True

        log_final_decision(symbol, direction, level, df5, breakout_result,
                           momentum_accel=momentum_accel.get("accelerating"),
                           rsi_turn=rsi_turn, volume_ok=volume_ok, retest=retest,
                           funding=funding, accepted=False, reason=reason)
        return False

    except Exception as e:
        diag_inc("analysis_error")
        diag_inc("final_confirmation")
        logger.warning("%s final confirmation hatası: %s", symbol, e)
        return False


# ============================================================
# CANDIDATE ANALYSIS + TOP-N SIRALI TEYIT
# ============================================================

def analyze_candidates(symbols, btc_regime=None):
    candidates = []

    for symbol in symbols:
        diag_inc("scanned")
        if normalize_symbol(symbol) in {normalize_symbol(x) for x in EXCLUDED_TRADE_SYMBOLS}:
            diag_reject(symbol, "excluded_symbol", "işlem evreni dışında")
            continue
        if not symbol_is_valid(symbol):
            diag_reject(symbol, "invalid_symbol")
            continue
        if is_on_cooldown(symbol):
            continue
        if has_local_symbol(symbol):
            continue

        try:
            result = analyze_high_conviction(symbol, btc_regime=btc_regime)

            if result:
                candidates.append(result)

                logger.info(
                    "[HC] %s | %s | setup=%.1f | trigger=%.1f | confirm=%s | tip=%s/%s | pattern=%s | "
                    "expected_move=%.2f%% (gerekli=%.2f%%)",
                    symbol, result["direction"], result["setup_score"], result["trigger_score"],
                    result["confirmations"], result["setup_type"], result["breakout_type"],
                    result.get("chart_pattern"),
                    safe_float(result["expected_move"].get("available_move_pct")),
                    safe_float(result["expected_move"].get("required_move_pct"))
                )

        except Exception as e:
            logger.warning("%s analiz hatası: %s", symbol, e)

        time.sleep(0.05)

    combined_score = lambda c: (c["setup_score"] + c["trigger_score"]) / 2
    candidates.sort(key=combined_score, reverse=True)

    return candidates


def try_open_from_candidates(candidates):
    top_candidates = candidates[:TOP_N_CANDIDATES]

    logger.info(
        "[HC] Top %s aday: %s",
        len(top_candidates),
        [(c["symbol"], c["direction"], round((c["setup_score"] + c["trigger_score"]) / 2, 1)) for c in top_candidates]
    )

    opened_any = False

    for candidate in top_candidates:
        if not can_open_more():
            break

        logger.info(
            "[HC TEYİT DENENİYOR] %s | yön=%s | setup=%.1f | trigger=%.1f | "
            "yapı_seviyesi=%s | tip=%s/%s | pattern=%s",
            candidate["symbol"], candidate["direction"], candidate["setup_score"],
            candidate["trigger_score"], candidate["structure_level"],
            candidate["setup_type"], candidate["breakout_type"], candidate.get("chart_pattern")
        )

        if final_entry_confirmation(candidate):
            logger.warning(
                "[HC AÇILIYOR] %s %s | setup=%.1f trigger=%.1f | gerekçe: pullback sağlıklı, "
                "momentum ivmeleniyor, yapı kırılımı+breakout teyitli, expected move yeterli",
                candidate["direction"], candidate["symbol"],
                candidate["setup_score"], candidate["trigger_score"]
            )

            if open_position(candidate):
                opened_any = True
        else:
            logger.info("[HC RED] %s son teyidi geçemedi, sıradaki adaya geçiliyor.", candidate["symbol"])

    if not opened_any:
        logger.info("[HC] Top-%s aday içinde teyidi geçen olmadı.", TOP_N_CANDIDATES)

    return opened_any


# ============================================================
# ANALYSIS CYCLE
# ============================================================

def analysis_cycle():
    global last_analysis_time
    global last_successful_analysis

    bot_stats["analysis_count"] += 1
    reset_cycle_diagnostics()
    last_analysis_time = now_ms()

    logger.info("=" * 70)
    logger.info("BOT ANALİZ BAŞLADI | %s", datetime.now(timezone.utc).isoformat())

    sync_real_positions()

    total = local_position_count()
    logger.info("[POZİSYON] açık=%s / maksimum=%s", total, MAX_OPEN_POSITIONS)

    if total >= MAX_OPEN_POSITIONS:
        logger.info("[DOLU] Maksimum pozisyon sayısına ulaşıldı, analiz atlanıyor.")
        return

    btc_regime = get_btc_regime()
    logger.info("[BTC REJİM] yön=%s | güç=%.1f", btc_regime["direction"], btc_regime["strength"])

    gainers, losers, volume_leaders = get_top_movers()

    logger.info("[GAINERS TOP25] %s", gainers)
    logger.info("[LOSERS TOP25] %s", losers)
    logger.info("[VOLUME TOP25] %s", volume_leaders)

    candidates_pool = []
    seen = set()
    excluded_normalized = {normalize_symbol(x) for x in EXCLUDED_TRADE_SYMBOLS}

    for symbol in gainers + losers + volume_leaders:
        normalized = normalize_symbol(symbol)
        if normalized in seen or normalized in excluded_normalized:
            continue
        seen.add(normalized)
        candidates_pool.append(symbol)

    logger.info("[TARAMA] %s benzersiz coin (işlem evreni filtresinden geçen, BTC/XAU hariç).", len(candidates_pool))

    signals = analyze_candidates(candidates_pool, btc_regime=btc_regime)

    opened = False
    if signals:
        opened = try_open_from_candidates(signals)
    else:
        logger.info("[HC] Uygun setup/trigger bulunamadı.")

    log_cycle_diagnostics(len(candidates_pool), len(signals), int(bool(opened)))
    last_successful_analysis = datetime.now(timezone.utc).isoformat()

    logger.info("BOT ANALİZ BİTTİ")
    logger.info("=" * 70)


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
        except Exception:
            pass
        time.sleep(300)


# ============================================================
# START
# ============================================================

def start_bot():
    global running

    logger.info("=" * 70)
    logger.info("HIGH-CONVICTION PULLBACK & BREAKOUT BOT V3.1 — RELAXED + PATTERNS")
    logger.info("=" * 70)
    logger.info(
        "DRY_RUN=%s | TESTNET=%s | MAX_LEVERAGE=%sx | MAX_OPEN_POSITIONS=%s | "
        "MIN_TARGET_ROI=%.1f%% | ANALYSIS_INTERVAL=%ss",
        DRY_RUN, TESTNET, MAX_LEVERAGE, MAX_OPEN_POSITIONS, MIN_TARGET_ROI, ANALYSIS_INTERVAL
    )

    create_exchange()

    # RESTART / POSITION RECOVERY — thread'ler başlamadan önce
    recover_positions_from_exchange()

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
