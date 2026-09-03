import os
import asyncio
import time
from datetime import datetime, timezone

import pandas as pd
import ccxt.async_support as ccxt
from loguru import logger
from dotenv import load_dotenv
from pydantic_settings import BaseSettings


load_dotenv()


# ============================================================
# 1. KONFİGÜRASYON
# ============================================================

class Settings(BaseSettings):

    BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
    BINANCE_SECRET_KEY: str = os.getenv("BINANCE_SECRET_KEY", "")

    # ========================================================
    # TEST MODU
    # True  = GERÇEK VERİ + SANAL İŞLEM
    # False = GERÇEK EMİR
    # ========================================================

    DRY_RUN: bool = (
        os.getenv("DRY_RUN", "True").lower() == "true"
    )

    # ========================================================
    # POZİSYON
    # ========================================================

    MAX_CONCURRENT_TRADES: int = int(
        os.getenv("MAX_CONCURRENT_TRADES", "3")
    )

    LEVERAGE: int = int(
        os.getenv("LEVERAGE", "5")
    )

    MARGIN_PER_TRADE: float = float(
        os.getenv("MARGIN_PER_TRADE", "10.0")
    )

    MARGIN_MODE: str = os.getenv(
        "MARGIN_MODE",
        "isolated"
    )

    # ========================================================
    # TIMEFRAME
    # ========================================================

    MACRO_TF: str = "4h"
    SETUP_TF: str = "1h"
    EXEC_TF: str = "15m"

    CANDLE_LIMIT: int = 150

    # ========================================================
    # DİNAMİK HAVUZ
    # ========================================================

    GAINER_LIMIT: int = 50
    LOSER_LIMIT: int = 50
    VOLUME_LIMIT: int = 50

    # ========================================================
    # TARAMA
    # ========================================================

    SCAN_INTERVAL: int = 180

    # ========================================================
    # POZİSYON MONITOR
    # ========================================================

    POSITION_MONITOR_INTERVAL: int = 3
    HEARTBEAT_INTERVAL: int = 60

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()


# ============================================================
# YARDIMCI FONKSİYONLAR
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def now_text():
    return utc_now().strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def safe_float(value, default=0.0):

    try:

        if value is None:
            return default

        return float(value)

    except Exception:

        return default


# ============================================================
# 2. BINANCE EXCHANGE
# ============================================================

class BinanceExchange:

    def __init__(self):

        self.exchange = ccxt.binance({

            "apiKey":
                settings.BINANCE_API_KEY,

            "secret":
                settings.BINANCE_SECRET_KEY,

            "enableRateLimit":
                True,

            "options": {

                "defaultType":
                    "future",

                "adjustForTimeDifference":
                    True,
            }
        })

    # --------------------------------------------------------
    # MARKETLER
    # --------------------------------------------------------

    async def load_markets(self):

        try:

            await self.exchange.load_markets()

            logger.success(
                f"Binance Futures marketleri yüklendi | "
                f"{len(self.exchange.markets)} market"
            )

        except Exception as e:

            logger.error(
                f"Market yükleme hatası: {e}"
            )

            raise

    # --------------------------------------------------------
    # DINAMIK HAVUZ
    # --------------------------------------------------------

    async def get_dynamic_pool(self):

        try:

            tickers = (
                await self.exchange.fetch_tickers()
            )

            data = []

            for symbol, ticker in tickers.items():

                if not (
                    symbol.endswith("/USDT:USDT")
                    or symbol.endswith("/USDT")
                ):
                    continue

                upper_symbol = symbol.upper()

                # İstenmeyen ürünleri çıkar
                if any(
                    token in upper_symbol
                    for token in [
                        "UP/",
                        "DOWN/",
                        "BULL/",
                        "BEAR/",
                        "_",
                        "BID/",
                        "ASK/"
                    ]
                ):
                    continue

                change = safe_float(
                    ticker.get("percentage")
                )

                volume = safe_float(
                    ticker.get("quoteVolume")
                )

                data.append({

                    "symbol":
                        symbol,

                    "change":
                        change,

                    "volume":
                        volume
                })

            df = pd.DataFrame(data)

            if df.empty:

                logger.warning(
                    "Dinamik havuz boş."
                )

                return []

            gainers = (
                df.sort_values(
                    "change",
                    ascending=False
                )
                .head(settings.GAINER_LIMIT)
                ["symbol"]
                .tolist()
            )

            losers = (
                df.sort_values(
                    "change",
                    ascending=True
                )
                .head(settings.LOSER_LIMIT)
                ["symbol"]
                .tolist()
            )

            volume_leaders = (
                df.sort_values(
                    "volume",
                    ascending=False
                )
                .head(settings.VOLUME_LIMIT)
                ["symbol"]
                .tolist()
            )

            # Öncelik sırasını koru
            pool = list(
                dict.fromkeys(
                    gainers
                    +
                    losers
                    +
                    volume_leaders
                )
            )

            logger.info(
                f"📊 HAVUZ | "
                f"Gainers={len(gainers)} | "
                f"Losers={len(losers)} | "
                f"Volume={len(volume_leaders)} | "
                f"Unique={len(pool)}"
            )

            return pool

        except Exception as e:

            logger.error(
                f"Havuz çekme hatası: {e}"
            )

            return []

    # --------------------------------------------------------
    # OHLCV
    # --------------------------------------------------------

    async def fetch_ohlcv(
        self,
        symbol,
        timeframe,
        limit
    ):

        try:

            ohlcv = (
                await self.exchange.fetch_ohlcv(
                    symbol,
                    timeframe,
                    limit=limit
                )
            )

            if not ohlcv:

                return pd.DataFrame()

            df = pd.DataFrame(
                ohlcv,
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

            df = df.dropna()

            return df

        except Exception as e:

            logger.debug(
                f"OHLCV hata | "
                f"{symbol} {timeframe}: {e}"
            )

            return pd.DataFrame()

    # --------------------------------------------------------
    # ANLIK FİYAT
    # --------------------------------------------------------

    async def get_current_price(
        self,
        symbol
    ):

        try:

            ticker = (
                await self.exchange.fetch_ticker(
                    symbol
                )
            )

            return safe_float(
                ticker.get("last")
            )

        except Exception as e:

            logger.debug(
                f"Fiyat alınamadı {symbol}: {e}"
            )

            return 0.0

    # --------------------------------------------------------
    # MİKTAR
    # --------------------------------------------------------

    def amount_to_precision(
        self,
        symbol,
        amount
    ):

        try:

            return float(
                self.exchange.amount_to_precision(
                    symbol,
                    amount
                )
            )

        except Exception:

            return amount

    # --------------------------------------------------------
    # LONG AÇ
    # --------------------------------------------------------

    async def open_long(
        self,
        symbol,
        qty,
        sl_price
    ):

        if settings.DRY_RUN:

            price = (
                await self.get_current_price(
                    symbol
                )
            )

            logger.info(
                f"🧪 [DRY_RUN] LONG SANAL GİRİŞ | "
                f"{symbol} | "
                f"Price={price:.8f} | "
                f"Qty={qty} | "
                f"SL={sl_price:.8f}"
            )

            return {

                "id":
                    "DRY_LONG",

                "price":
                    price,

                "average":
                    price,

                "filled":
                    qty
            }

        try:

            await self.exchange.set_margin_mode(
                settings.MARGIN_MODE.upper(),
                symbol
            )

        except Exception as e:

            logger.warning(
                f"Margin mode uyarısı "
                f"{symbol}: {e}"
            )

        await self.exchange.set_leverage(
            settings.LEVERAGE,
            symbol
        )

        order = (
            await self.exchange.create_order(
                symbol,
                "market",
                "buy",
                qty
            )
        )

        execution_price = safe_float(
            order.get("average")
            or order.get("price")
        )

        if execution_price <= 0:

            execution_price = (
                await self.get_current_price(
                    symbol
                )
            )

        # Gerçek koruma stopu
        try:

            sl_rounded = float(
                self.exchange.price_to_precision(
                    symbol,
                    sl_price
                )
            )

            await self.exchange.create_order(
                symbol=symbol,
                type="STOP_MARKET",
                side="sell",
                amount=qty,
                price=None,
                params={
                    "stopPrice":
                        sl_rounded,

                    "reduceOnly":
                        True
                }
            )

            logger.success(
                f"🛡️ LONG STOP gönderildi | "
                f"{symbol} | SL={sl_rounded}"
            )

        except Exception as e:

            logger.error(
                f"LONG STOP gönderilemedi "
                f"{symbol}: {e}"
            )

        return order

    # --------------------------------------------------------
    # SHORT AÇ
    # --------------------------------------------------------

    async def open_short(
        self,
        symbol,
        qty,
        sl_price
    ):

        if settings.DRY_RUN:

            price = (
                await self.get_current_price(
                    symbol
                )
            )

            logger.info(
                f"🧪 [DRY_RUN] SHORT SANAL GİRİŞ | "
                f"{symbol} | "
                f"Price={price:.8f} | "
                f"Qty={qty} | "
                f"SL={sl_price:.8f}"
            )

            return {

                "id":
                    "DRY_SHORT",

                "price":
                    price,

                "average":
                    price,

                "filled":
                    qty
            }

        try:

            await self.exchange.set_margin_mode(
                settings.MARGIN_MODE.upper(),
                symbol
            )

        except Exception as e:

            logger.warning(
                f"Margin mode uyarısı "
                f"{symbol}: {e}"
            )

        await self.exchange.set_leverage(
            settings.LEVERAGE,
            symbol
        )

        order = (
            await self.exchange.create_order(
                symbol,
                "market",
                "sell",
                qty
            )
        )

        execution_price = safe_float(
            order.get("average")
            or order.get("price")
        )

        if execution_price <= 0:

            execution_price = (
                await self.get_current_price(
                    symbol
                )
            )

        # SHORT stop = BUY
        try:

            sl_rounded = float(
                self.exchange.price_to_precision(
                    symbol,
                    sl_price
                )
            )

            await self.exchange.create_order(
                symbol=symbol,
                type="STOP_MARKET",
                side="buy",
                amount=qty,
                price=None,
                params={
                    "stopPrice":
                        sl_rounded,

                    "reduceOnly":
                        True
                }
            )

            logger.success(
                f"🛡️ SHORT STOP gönderildi | "
                f"{symbol} | SL={sl_rounded}"
            )

        except Exception as e:

            logger.error(
                f"SHORT STOP gönderilemedi "
                f"{symbol}: {e}"
            )

        return order

    # --------------------------------------------------------
    # POZİSYON KAPAT
    # --------------------------------------------------------

    async def close_position(
        self,
        symbol,
        qty,
        side
    ):

        if settings.DRY_RUN:

            logger.info(
                f"🧪 [DRY_RUN] "
                f"{side.upper()} SANAL POZİSYON KAPATILIYOR | "
                f"{symbol} | Qty={qty}"
            )

            return

        try:

            try:

                await self.exchange.cancel_all_orders(
                    symbol
                )

            except Exception:

                pass

            # LONG kapat = SELL
            # SHORT kapat = BUY
            close_side = (
                "sell"
                if side == "long"
                else "buy"
            )

            await self.exchange.create_order(
                symbol=symbol,
                type="market",
                side=close_side,
                amount=qty,
                params={
                    "reduceOnly":
                        True
                }
            )

        except Exception as e:

            logger.error(
                f"Pozisyon kapatma hatası "
                f"{symbol}: {e}"
            )

    # --------------------------------------------------------
    # CLOSE
    # --------------------------------------------------------

    async def close(self):

        try:

            await self.exchange.close()

        except Exception as e:

            logger.warning(
                f"Exchange kapatma hatası: {e}"
            )


# ============================================================
# 3. STRATEJİ MOTORU
# ============================================================

class StrategyEngine:

    def __init__(
        self,
        symbol,
        df_4h,
        df_1h,
        df_exec,
        btc_df
    ):

        self.symbol = symbol
        self.df_4h = df_4h
        self.df_1h = df_1h
        self.df_exec = df_exec
        self.btc_df = btc_df

    # --------------------------------------------------------
    # BTC MOMENTUM
    # --------------------------------------------------------

    def btc_change(self):

        if (
            self.btc_df.empty
            or len(self.btc_df) < 5
        ):

            return 0.0

        old_price = (
            self.btc_df["close"].iloc[-5]
        )

        new_price = (
            self.btc_df["close"].iloc[-1]
        )

        if old_price <= 0:

            return 0.0

        return (
            (
                new_price
                -
                old_price
            )
            /
            old_price
            *
            100
        )

    # --------------------------------------------------------
    # ANA ANALİZ
    # --------------------------------------------------------

    def analyze(self):

        # ====================================================
        # VERİ KONTROLÜ
        # ====================================================

        if (
            self.df_exec.empty
            or len(self.df_exec) < 50
            or self.df_4h.empty
            or len(self.df_4h) < 20
            or self.df_1h.empty
            or len(self.df_1h) < 20
        ):

            return {
                "action":
                    "WAIT",

                "reason":
                    "Yetersiz veri"
            }

        # ====================================================
        # SADECE KAPANMIŞ MUM
        # ====================================================

        df_4h = self.df_4h.iloc[:-1].copy()
        df_1h = self.df_1h.iloc[:-1].copy()
        df_15m = self.df_exec.iloc[:-1].copy()

        # ====================================================
        # 4H EMA
        # ====================================================

        df_4h["ema20"] = (
            df_4h["close"]
            .ewm(
                span=20,
                adjust=False
            )
            .mean()
        )

        df_4h["ema50"] = (
            df_4h["close"]
            .ewm(
                span=50,
                adjust=False
            )
            .mean()
        )

        ema20 = df_4h["ema20"].iloc[-1]
        ema50 = df_4h["ema50"].iloc[-1]

        current_4h = (
            df_4h["close"].iloc[-1]
        )

        previous_4h = (
            df_4h["close"].iloc[-3]
        )

        macro_slope = (
            current_4h
            -
            previous_4h
        )

        # ====================================================
        # 4H YÖNÜ
        # ====================================================

        bullish_macro = (
            ema20 > ema50
            and
            macro_slope > 0
        )

        bearish_macro = (
            ema20 < ema50
            and
            macro_slope < 0
        )

        # ====================================================
        # 1H SETUP
        # ====================================================

        df_1h["sma20"] = (
            df_1h["close"]
            .rolling(20)
            .mean()
        )

        sma20_1h = (
            df_1h["sma20"].iloc[-1]
        )

        current_1h = (
            df_1h["close"].iloc[-1]
        )

        bullish_1h = (
            current_1h
            >=
            sma20_1h * 0.995
        )

        bearish_1h = (
            current_1h
            <=
            sma20_1h * 1.005
        )

        # ====================================================
        # 15M YAPI
        # ====================================================

        recent_20 = (
            df_15m.iloc[-20:]
        )

        resistance = (
            recent_20["high"].max()
        )

        support = (
            recent_20["low"].min()
        )

        current_price = (
            df_15m["close"].iloc[-1]
        )

        # ====================================================
        # 15M MOMENTUM
        # ====================================================

        previous_close = (
            df_15m["close"].iloc[-2]
        )

        momentum_pct = (
            (
                current_price
                -
                previous_close
            )
            /
            previous_close
            *
            100
        )

        # ====================================================
        # VOLUME
        # ====================================================

        volume_ma = (
            df_15m["volume"]
            .rolling(20)
            .mean()
            .iloc[-1]
        )

        current_volume = (
            df_15m["volume"].iloc[-1]
        )

        volume_ratio = (
            current_volume / volume_ma
            if volume_ma > 0
            else 0
        )

        # ====================================================
        # BTC
        # ====================================================

        btc_pct = self.btc_change()

        # ====================================================
        # ====================================================
        # LONG SETUP
        # ====================================================
        # ====================================================

        if bullish_macro and bullish_1h:

            runway_long = (
                (
                    resistance
                    -
                    current_price
                )
                /
                current_price
                *
                100
            )

            if runway_long < 1.0:

                long_reason = (
                    f"LONG runway düşük "
                    f"%{runway_long:.2f}"
                )

            else:

                # BTC ciddi düşüyorsa LONG daha riskli
                if btc_pct < -3.0:

                    long_reason = (
                        f"BTC risk yüksek "
                        f"(%{btc_pct:.2f})"
                    )

                else:

                    # LONG SL
                    sl_long = (
                        support
                        *
                        (
                            1
                            -
                            (
                                0.005
                                *
                                (
                                    0.7
                                    if btc_pct < -3
                                    else 1.0
                                )
                            )
                        )
                    )

                    tp_long = resistance

                    risk_long = (
                        current_price
                        -
                        sl_long
                    )

                    reward_long = (
                        tp_long
                        -
                        current_price
                    )

                    if (
                        risk_long <= 0
                        or reward_long <= 0
                    ):

                        long_reason = (
                            "LONG geçersiz fiyatlar"
                        )

                    else:

                        rr_long = (
                            risk_long
                            /
                            reward_long
                        )

                        if rr_long > 0.5:

                            long_reason = (
                                f"LONG R/R zayıf "
                                f"{rr_long:.2f}"
                            )

                        else:

                            logger.info(
                                f"🟢 LONG ADAYI | "
                                f"{self.symbol} | "
                                f"4H bullish | "
                                f"1H bullish | "
                                f"Momentum=%{momentum_pct:+.2f} | "
                                f"Volume={volume_ratio:.2f}x | "
                                f"Runway=%{runway_long:.2f}"
                            )

                            return {

                                "action":
                                    "ENTER_LONG",

                                "symbol":
                                    self.symbol,

                                "side":
                                    "long",

                                "price":
                                    current_price,

                                "sl":
                                    sl_long,

                                "tp":
                                    tp_long,

                                "runway":
                                    runway_long,

                                "risk_reward":
                                    rr_long,

                                "momentum":
                                    momentum_pct,

                                "volume_ratio":
                                    volume_ratio,

                                "btc_change":
                                    btc_pct,

                                "explanation":
                                    (
                                        f"\n"
                                        f"╔══════════════════════════════════════╗\n"
                                        f"║          🟢 LONG ONAYLANDI           ║\n"
                                        f"╠══════════════════════════════════════╣\n"
                                        f"║ Coin        : {self.symbol}\n"
                                        f"║ Giriş       : {current_price:.8f}\n"
                                        f"║ SL          : {sl_long:.8f}\n"
                                        f"║ TP          : {tp_long:.8f}\n"
                                        f"║ Runway      : %{runway_long:.2f}\n"
                                        f"║ R/R         : {rr_long:.2f}\n"
                                        f"║ 15M Momentum: %{momentum_pct:+.2f}\n"
                                        f"║ Volume      : {volume_ratio:.2f}x\n"
                                        f"║ BTC         : %{btc_pct:+.2f}\n"
                                        f"╠══════════════════════════════════════╣\n"
                                        f"║ 4H Trend    : BULLISH\n"
                                        f"║ 1H Setup    : BULLISH\n"
                                        f"║ 15M         : LONG EXECUTION\n"
                                        f"╚══════════════════════════════════════╝"
                                    )
                            }

        # ====================================================
        # ====================================================
        # SHORT SETUP
        # ====================================================
        # ====================================================

        if bearish_macro and bearish_1h:

            runway_short = (
                (
                    current_price
                    -
                    support
                )
                /
                current_price
                *
                100
            )

            if runway_short < 1.0:

                short_reason = (
                    f"SHORT runway düşük "
                    f"%{runway_short:.2f}"
                )

            else:

                # BTC çok güçlü yükseliyorsa SHORT riskli
                if btc_pct > 3.0:

                    short_reason = (
                        f"BTC risk yüksek "
                        f"(%{btc_pct:+.2f})"
                    )

                else:

                    # SHORT SL = direnç üstü
                    sl_short = (
                        resistance
                        *
                        (
                            1
                            +
                            0.005
                        )
                    )

                    tp_short = support

                    risk_short = (
                        sl_short
                        -
                        current_price
                    )

                    reward_short = (
                        current_price
                        -
                        tp_short
                    )

                    if (
                        risk_short <= 0
                        or reward_short <= 0
                    ):

                        short_reason = (
                            "SHORT geçersiz fiyatlar"
                        )

                    else:

                        rr_short = (
                            risk_short
                            /
                            reward_short
                        )

                        if rr_short > 0.5:

                            short_reason = (
                                f"SHORT R/R zayıf "
                                f"{rr_short:.2f}"
                            )

                        else:

                            logger.info(
                                f"🔴 SHORT ADAYI | "
                                f"{self.symbol} | "
                                f"4H bearish | "
                                f"1H bearish | "
                                f"Momentum=%{momentum_pct:+.2f} | "
                                f"Volume={volume_ratio:.2f}x | "
                                f"Runway=%{runway_short:.2f}"
                            )

                            return {

                                "action":
                                    "ENTER_SHORT",

                                "symbol":
                                    self.symbol,

                                "side":
                                    "short",

                                "price":
                                    current_price,

                                "sl":
                                    sl_short,

                                "tp":
                                    tp_short,

                                "runway":
                                    runway_short,

                                "risk_reward":
                                    rr_short,

                                "momentum":
                                    momentum_pct,

                                "volume_ratio":
                                    volume_ratio,

                                "btc_change":
                                    btc_pct,

                                "explanation":
                                    (
                                        f"\n"
                                        f"╔══════════════════════════════════════╗\n"
                                        f"║          🔴 SHORT ONAYLANDI          ║\n"
                                        f"╠══════════════════════════════════════╣\n"
                                        f"║ Coin        : {self.symbol}\n"
                                        f"║ Giriş       : {current_price:.8f}\n"
                                        f"║ SL          : {sl_short:.8f}\n"
                                        f"║ TP          : {tp_short:.8f}\n"
                                        f"║ Runway      : %{runway_short:.2f}\n"
                                        f"║ R/R         : {rr_short:.2f}\n"
                                        f"║ 15M Momentum: %{momentum_pct:+.2f}\n"
                                        f"║ Volume      : {volume_ratio:.2f}x\n"
                                        f"║ BTC         : %{btc_pct:+.2f}\n"
                                        f"╠══════════════════════════════════════╣\n"
                                        f"║ 4H Trend    : BEARISH\n"
                                        f"║ 1H Setup    : BEARISH\n"
                                        f"║ 15M         : SHORT EXECUTION\n"
                                        f"╚══════════════════════════════════════╝"
                                    )
                            }

        # ====================================================
        # BEKLE
        # ====================================================

        return {

            "action":
                "WAIT",

            "reason":
                "LONG veya SHORT tüm trend koşulları aynı anda oluşmadı"
        }


# ============================================================
# 4. POZİSYON MONITOR
# ============================================================

async def position_monitor_loop(
    exchange,
    active_trades,
    trade_lock
):

    last_heartbeat = time.time()

    logger.info(
        "👁️ Bağımsız pozisyon monitorü aktif."
    )

    while True:

        try:

            await asyncio.sleep(
                settings.POSITION_MONITOR_INTERVAL
            )

            async with trade_lock:

                symbols = list(
                    active_trades.keys()
                )

            if not symbols:
                continue

            for symbol in symbols:

                async with trade_lock:

                    if symbol not in active_trades:
                        continue

                    trade = dict(
                        active_trades[symbol]
                    )

                side = trade["side"]

                current_price = (
                    await exchange.get_current_price(
                        symbol
                    )
                )

                if current_price <= 0:
                    continue

                # =================================================
                # POZİSYON YAŞI
                # =================================================

                hold_minutes = (
                    (
                        time.time()
                        -
                        trade["opened_at"]
                    )
                    /
                    60
                )

                # =================================================
                # 15M YAPI
                # =================================================

                df_live = (
                    await exchange.fetch_ohlcv(
                        symbol,
                        settings.EXEC_TF,
                        30
                    )
                )

                active_support = None
                active_resistance = None

                if not df_live.empty:

                    closed = (
                        df_live.iloc[:-1]
                    )

                    if len(closed) >= 10:

                        active_support = (
                            closed[
                                "low"
                            ]
                            .iloc[-10:]
                            .min()
                        )

                        active_resistance = (
                            closed[
                                "high"
                            ]
                            .iloc[-10:]
                            .max()
                        )

                        # =================================================
                        # DİNAMİK TP — LONG
                        # =================================================

                        if side == "long":

                            new_runway = (
                                (
                                    active_resistance
                                    -
                                    current_price
                                )
                                /
                                current_price
                                *
                                100
                            )

                            if (
                                active_resistance
                                >
                                trade["tp"]
                                and
                                new_runway >= 0.8
                            ):

                                async with trade_lock:

                                    if symbol in active_trades:

                                        active_trades[
                                            symbol
                                        ]["tp"] = (
                                            active_resistance
                                        )

                                trade["tp"] = (
                                    active_resistance
                                )

                                logger.info(
                                    f"🚀 [LONG DİNAMİK TP] "
                                    f"{symbol} | "
                                    f"TP={active_resistance:.8f}"
                                )

                        # =================================================
                        # DİNAMİK TP — SHORT
                        # =================================================

                        else:

                            new_runway = (
                                (
                                    current_price
                                    -
                                    active_support
                                )
                                /
                                current_price
                                *
                                100
                            )

                            if (
                                active_support
                                <
                                trade["tp"]
                                and
                                new_runway >= 0.8
                            ):

                                async with trade_lock:

                                    if symbol in active_trades:

                                        active_trades[
                                            symbol
                                        ]["tp"] = (
                                            active_support
                                        )

                                trade["tp"] = (
                                    active_support
                                )

                                logger.info(
                                    f"🚀 [SHORT DİNAMİK TP] "
                                    f"{symbol} | "
                                    f"TP={active_support:.8f}"
                                )

                        # =================================================
                        # SWEEP / YAPISAL BOZULMA
                        # =================================================

                        sweep_trigger = False

                        if side == "long":

                            if (
                                current_price
                                <
                                active_support
                                *
                                0.992
                            ):

                                sweep_trigger = True

                        else:

                            if (
                                current_price
                                >
                                active_resistance
                                *
                                1.008
                            ):

                                sweep_trigger = True

                        if sweep_trigger:

                            if side == "long":

                                raw_change = (
                                    (
                                        current_price
                                        -
                                        trade["price"]
                                    )
                                    /
                                    trade["price"]
                                )

                            else:

                                raw_change = (
                                    (
                                        trade["price"]
                                        -
                                        current_price
                                    )
                                    /
                                    trade["price"]
                                )

                            pnl_pct = (
                                raw_change
                                *
                                100
                                *
                                settings.LEVERAGE
                            )

                            pnl_usdt = (
                                settings.MARGIN_PER_TRADE
                                *
                                pnl_pct
                                /
                                100
                            )

                            await exchange.close_position(
                                symbol,
                                trade["qty"],
                                side
                            )

                            logger.warning(
                                f"\n"
                                f"📊 SWEEP / GÜVENLİ ÇIKIŞ 🟠\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"Sembol : {symbol}\n"
                                f"Yön    : {side.upper()}\n"
                                f"Giriş  : {trade['price']:.8f}\n"
                                f"Çıkış  : {current_price:.8f}\n"
                                f"PnL    : ${pnl_usdt:+.2f}\n"
                                f"Hold   : {hold_minutes:.1f} dk\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                            )

                            async with trade_lock:

                                active_trades.pop(
                                    symbol,
                                    None
                                )

                            continue

                # =================================================
                # PNL
                # =================================================

                if side == "long":

                    raw_change_pct = (
                        (
                            current_price
                            -
                            trade["price"]
                        )
                        /
                        trade["price"]
                        *
                        100
                    )

                else:

                    raw_change_pct = (
                        (
                            trade["price"]
                            -
                            current_price
                        )
                        /
                        trade["price"]
                        *
                        100
                    )

                pnl_pct = (
                    raw_change_pct
                    *
                    settings.LEVERAGE
                )

                pnl_usdt = (
                    settings.MARGIN_PER_TRADE
                    *
                    pnl_pct
                    /
                    100
                )

                # =================================================
                # DİNAMİK KÂR KİLİTLEME
                # =================================================

                updated_sl = trade["sl"]

                # -------------------------------------------------
                # BREAKEVEN
                # -------------------------------------------------

                if (
                    raw_change_pct >= 1.2
                    and
                    (
                        (
                            side == "long"
                            and
                            updated_sl
                            <
                            trade["price"]
                        )
                        or
                        (
                            side == "short"
                            and
                            updated_sl
                            >
                            trade["price"]
                        )
                    )
                ):

                    updated_sl = (
                        trade["price"]
                    )

                    logger.success(
                        f"🛡️ [BREAKEVEN] "
                        f"{symbol} | "
                        f"{side.upper()} | "
                        f"Ham hareket=%{raw_change_pct:.2f}"
                    )

                # -------------------------------------------------
                # KADEMELİ KÂR
                # -------------------------------------------------

                elif pnl_pct >= 7.0:

                    excess = (
                        pnl_pct - 7.0
                    )

                    step_index = int(
                        excess // 3.0
                    )

                    locked_pnl = (
                        7.0
                        +
                        step_index * 3.0
                    )

                    target_raw = (
                        locked_pnl
                        /
                        settings.LEVERAGE
                    )

                    if side == "long":

                        locked_price = (
                            trade["price"]
                            +
                            (
                                target_raw
                                /
                                100
                                *
                                trade["price"]
                                *
                                0.5
                            )
                        )

                        if (
                            locked_price
                            >
                            updated_sl
                        ):

                            updated_sl = (
                                locked_price
                            )

                    else:

                        locked_price = (
                            trade["price"]
                            -
                            (
                                target_raw
                                /
                                100
                                *
                                trade["price"]
                                *
                                0.5
                            )
                        )

                        if (
                            locked_price
                            <
                            updated_sl
                        ):

                            updated_sl = (
                                locked_price
                            )

                    if updated_sl != trade["sl"]:

                        logger.success(
                            f"🔒 [KÂR KİLİDİ] "
                            f"{symbol} | "
                            f"{side.upper()} | "
                            f"PnL=%{pnl_pct:.2f} | "
                            f"Yeni SL={updated_sl:.8f}"
                        )

                # =================================================
                # SL GÜNCELLE
                # =================================================

                if updated_sl != trade["sl"]:

                    async with trade_lock:

                        if symbol in active_trades:

                            active_trades[
                                symbol
                            ]["sl"] = updated_sl

                    trade["sl"] = updated_sl

                # =================================================
                # TP KONTROLÜ
                # =================================================

                tp_hit = False

                if side == "long":

                    if (
                        current_price
                        >=
                        trade["tp"]
                    ):

                        tp_hit = True

                else:

                    if (
                        current_price
                        <=
                        trade["tp"]
                    ):

                        tp_hit = True

                if tp_hit:

                    await exchange.close_position(
                        symbol,
                        trade["qty"],
                        side
                    )

                    logger.success(
                        f"\n"
                        f"📊 TP KAPANIŞI 🟢\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Sembol : {symbol}\n"
                        f"Yön    : {side.upper()}\n"
                        f"Giriş  : {trade['price']:.8f}\n"
                        f"Çıkış  : {current_price:.8f}\n"
                        f"PnL    : ${pnl_usdt:+.2f}\n"
                        f"PnL %  : %{pnl_pct:+.2f}\n"
                        f"Hold   : {hold_minutes:.1f} dk\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    )

                    async with trade_lock:

                        active_trades.pop(
                            symbol,
                            None
                        )

                    continue

                # =================================================
                # SL KONTROLÜ
                # =================================================

                sl_hit = False

                if side == "long":

                    if (
                        current_price
                        <=
                        trade["sl"]
                    ):

                        sl_hit = True

                else:

                    if (
                        current_price
                        >=
                        trade["sl"]
                    ):

                        sl_hit = True

                if sl_hit:

                    profit_lock = (
                        (
                            side == "long"
                            and
                            trade["sl"]
                            >
                            trade["price"]
                        )
                        or
                        (
                            side == "short"
                            and
                            trade["sl"]
                            <
                            trade["price"]
                        )
                    )

                    result = (
                        "KÂRLI TRAILING KAPANIŞ 🟢"
                        if profit_lock
                        else
                        "STOP 🔴"
                    )

                    await exchange.close_position(
                        symbol,
                        trade["qty"],
                        side
                    )

                    logger.warning(
                        f"\n"
                        f"📊 {result}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"Sembol : {symbol}\n"
                        f"Yön    : {side.upper()}\n"
                        f"Giriş  : {trade['price']:.8f}\n"
                        f"Çıkış  : {current_price:.8f}\n"
                        f"SL     : {trade['sl']:.8f}\n"
                        f"PnL    : ${pnl_usdt:+.2f}\n"
                        f"PnL %  : %{pnl_pct:+.2f}\n"
                        f"Hold   : {hold_minutes:.1f} dk\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    )

                    async with trade_lock:

                        active_trades.pop(
                            symbol,
                            None
                        )

                    continue

            # =================================================
            # HEARTBEAT
            # =================================================

            if (
                time.time()
                -
                last_heartbeat
                >=
                settings.HEARTBEAT_INTERVAL
            ):

                heartbeat = []

                async with trade_lock:

                    snapshot = dict(
                        active_trades
                    )

                for symbol, trade in (
                    snapshot.items()
                ):

                    current_price = (
                        await exchange.get_current_price(
                            symbol
                        )
                    )

                    if current_price <= 0:
                        continue

                    if trade["side"] == "long":

                        raw_pct = (
                            (
                                current_price
                                -
                                trade["price"]
                            )
                            /
                            trade["price"]
                            *
                            100
                        )

                    else:

                        raw_pct = (
                            (
                                trade["price"]
                                -
                                current_price
                            )
                            /
                            trade["price"]
                            *
                            100
                        )

                    leveraged_pct = (
                        raw_pct
                        *
                        settings.LEVERAGE
                    )

                    pnl_value = (
                        settings.MARGIN_PER_TRADE
                        *
                        leveraged_pct
                        /
                        100
                    )

                    heartbeat.append(
                        f"{symbol} "
                        f"[{trade['side'].upper()}] "
                        f"Price={current_price:.8f} "
                        f"PnL=%{leveraged_pct:+.2f} "
                        f"(${pnl_value:+.2f}) "
                        f"SL={trade['sl']:.8f} "
                        f"TP={trade['tp']:.8f}"
                    )

                if heartbeat:

                    logger.info(
                        "\n"
                        f"💓 HEARTBEAT | {now_text()}\n"
                        +
                        "\n".join(
                            heartbeat
                        )
                    )

                last_heartbeat = time.time()

        except Exception as e:

            logger.exception(
                f"Monitor hatası: {e}"
            )

            await asyncio.sleep(5)


# ============================================================
# 5. TARAMA LOOP
# ============================================================

async def scan_loop(
    exchange,
    active_trades,
    trade_lock
):

    logger.info(
        "🔎 Tarama motoru başlatıldı."
    )

    while True:

        start_time = time.time()

        try:

            logger.info(
                "\n"
                "════════════════════════════════════════\n"
                f"🔎 YENİ ANALİZ | {now_text()}\n"
                "════════════════════════════════════════"
            )

            pool = (
                await exchange.get_dynamic_pool()
            )

            if not pool:

                await asyncio.sleep(30)
                continue

            btc_df = (
                await exchange.fetch_ohlcv(
                    "BTC/USDT:USDT",
                    settings.SETUP_TF,
                    50
                )
            )

            signals = 0
            checked = 0
            skipped = 0

            for symbol in pool:

                async with trade_lock:

                    if (
                        len(active_trades)
                        >=
                        settings.MAX_CONCURRENT_TRADES
                    ):

                        logger.info(
                            "🛑 Maksimum pozisyon "
                            "sayısına ulaşıldı."
                        )

                        break

                    if symbol in active_trades:

                        continue

                checked += 1

                # =================================================
                # VERİ
                # =================================================

                df_4h = (
                    await exchange.fetch_ohlcv(
                        symbol,
                        settings.MACRO_TF,
                        60
                    )
                )

                df_1h = (
                    await exchange.fetch_ohlcv(
                        symbol,
                        settings.SETUP_TF,
                        60
                    )
                )

                df_15m = (
                    await exchange.fetch_ohlcv(
                        symbol,
                        settings.EXEC_TF,
                        settings.CANDLE_LIMIT
                    )
                )

                if (
                    df_4h.empty
                    or df_1h.empty
                    or df_15m.empty
                ):

                    skipped += 1

                    continue

                # =================================================
                # STRATEJİ
                # =================================================

                strategy = StrategyEngine(
                    symbol,
                    df_4h,
                    df_1h,
                    df_15m,
                    btc_df
                )

                result = strategy.analyze()

                # =================================================
                # LONG / SHORT
                # =================================================

                if result["action"] in [
                    "ENTER_LONG",
                    "ENTER_SHORT"
                ]:

                    signals += 1

                    side = result["side"]

                    current_price = (
                        await exchange.get_current_price(
                            symbol
                        )
                    )

                    if current_price <= 0:

                        continue

                    # =================================================
                    # NOTIONAL
                    # =================================================

                    notional = (
                        settings.MARGIN_PER_TRADE
                        *
                        settings.LEVERAGE
                    )

                    raw_qty = (
                        notional
                        /
                        current_price
                    )

                    qty = (
                        exchange.amount_to_precision(
                            symbol,
                            raw_qty
                        )
                    )

                    if qty <= 0:

                        logger.warning(
                            f"{symbol} qty geçersiz."
                        )

                        continue

                    logger.info(
                        result["explanation"]
                    )

                    # =================================================
                    # EMİR
                    # =================================================

                    try:

                        if side == "long":

                            order = (
                                await exchange.open_long(
                                    symbol,
                                    qty,
                                    result["sl"]
                                )
                            )

                        else:

                            order = (
                                await exchange.open_short(
                                    symbol,
                                    qty,
                                    result["sl"]
                                )
                            )

                        if not order:

                            continue

                        execution_price = safe_float(
                            order.get("average")
                            or order.get("price")
                        )

                        if execution_price <= 0:

                            execution_price = (
                                await exchange.get_current_price(
                                    symbol
                                )
                            )

                        # =================================================
                        # AKTİF POZİSYON
                        # =================================================

                        async with trade_lock:

                            # Son güvenlik kontrolü
                            if (
                                len(active_trades)
                                <
                                settings.MAX_CONCURRENT_TRADES
                                and
                                symbol
                                not in
                                active_trades
                            ):

                                active_trades[
                                    symbol
                                ] = {

                                    "side":
                                        side,

                                    "price":
                                        execution_price,

                                    "sl":
                                        result["sl"],

                                    "tp":
                                        result["tp"],

                                    "runway":
                                        result["runway"],

                                    "risk_reward":
                                        result["risk_reward"],

                                    "qty":
                                        qty,

                                    "opened_at":
                                        time.time(),

                                    "opened_at_text":
                                        now_text(),

                                    "margin":
                                        settings.MARGIN_PER_TRADE,

                                    "leverage":
                                        settings.LEVERAGE
                                }

                        logger.success(
                            f"\n"
                            f"🟢 POZİSYON AKTİF\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"Sembol   : {symbol}\n"
                            f"Yön      : {side.upper()}\n"
                            f"Giriş    : {execution_price:.8f}\n"
                            f"Qty      : {qty}\n"
                            f"Margin   : {settings.MARGIN_PER_TRADE:.2f} USDT\n"
                            f"Leverage : {settings.LEVERAGE}x\n"
                            f"SL       : {result['sl']:.8f}\n"
                            f"TP       : {result['tp']:.8f}\n"
                            f"Runway   : %{result['runway']:.2f}\n"
                            f"R/R      : {result['risk_reward']:.2f}\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                        )

                    except Exception as order_error:

                        logger.exception(
                            f"Emir işleme hatası "
                            f"{symbol}: {order_error}"
                        )

                else:

                    logger.debug(
                        f"⏭️ {symbol} | "
                        f"{result.get('action')} | "
                        f"{result.get('reason', '')}"
                    )

                await asyncio.sleep(
                    0.15
                )

            # =================================================
            # TARAMA RAPORU
            # =================================================

            async with trade_lock:

                active_count = (
                    len(active_trades)
                )

            elapsed = (
                time.time()
                -
                start_time
            )

            logger.info(
                "\n"
                "════════════════════════════════════════\n"
                f"📊 TARAMA TAMAMLANDI\n"
                f"Havuz            : {len(pool)}\n"
                f"Kontrol edilen   : {checked}\n"
                f"Veri eksik       : {skipped}\n"
                f"Yeni sinyal      : {signals}\n"
                f"Aktif pozisyon   : "
                f"{active_count}/{settings.MAX_CONCURRENT_TRADES}\n"
                f"Süre             : {elapsed:.1f} sn\n"
                "════════════════════════════════════════"
            )

            await asyncio.sleep(
                settings.SCAN_INTERVAL
            )

        except Exception as e:

            logger.exception(
                f"Tarama döngüsü hatası: {e}"
            )

            await asyncio.sleep(30)


# ============================================================
# 6. MAIN
# ============================================================

async def main():

    logger.info(
        "\n"
        "╔══════════════════════════════════════════╗\n"
        "║       BINANCE FUTURES BOT               ║\n"
        "║       LONG + SHORT TEST ENGINE          ║\n"
        "╠══════════════════════════════════════════╣\n"
        f"║ DRY RUN      : {settings.DRY_RUN}\n"
        f"║ MAX POS      : {settings.MAX_CONCURRENT_TRADES}\n"
        f"║ MARGIN       : {settings.MARGIN_PER_TRADE:.2f} USDT\n"
        f"║ LEVERAGE     : {settings.LEVERAGE}x\n"
        f"║ MARGIN MODE  : {settings.MARGIN_MODE}\n"
        f"║ MACRO        : {settings.MACRO_TF}\n"
        f"║ SETUP        : {settings.SETUP_TF}\n"
        f"║ EXEC         : {settings.EXEC_TF}\n"
        "╚══════════════════════════════════════════╝"
    )

    if settings.DRY_RUN:

        logger.success(
            "\n"
            "🧪 TEST MODU AKTİF\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "✓ Binance gerçek verileri kullanılacak\n"
            "✓ LONG + SHORT aktif\n"
            "✓ Gerçek emir GÖNDERİLMEYECEK\n"
            "✓ Pozisyonlar sanal takip edilecek\n"
            "✓ Gerçek piyasa fiyatları kullanılacak\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )

    else:

        logger.warning(
            "\n"
            "⚠️⚠️ GERÇEK EMİR MODU ⚠️⚠️\n"
            "Binance Futures üzerinde gerçek emir gönderilecek!"
        )

    exchange = BinanceExchange()

    active_trades = {}

    trade_lock = asyncio.Lock()

    try:

        await exchange.load_markets()

        await asyncio.gather(

            scan_loop(
                exchange,
                active_trades,
                trade_lock
            ),

            position_monitor_loop(
                exchange,
                active_trades,
                trade_lock
            )
        )

    except KeyboardInterrupt:

        logger.info(
            "Bot kullanıcı tarafından durduruldu."
        )

    except Exception as e:

        logger.exception(
            f"Ana program hatası: {e}"
        )

    finally:

        await exchange.close()


# ============================================================
# 7. BAŞLAT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        logger.info(
            "Bot kapatıldı."
        )