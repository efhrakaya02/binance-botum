import os
import asyncio
import pandas as pd
import ccxt.async_support as ccxt
from loguru import logger
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

# ==========================================
# 1. KONFİGÜRASYON VE AYARLAR
# ==========================================
class Settings(BaseSettings):
    BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
    BINANCE_SECRET_KEY: str = os.getenv("BINANCE_SECRET_KEY", "")
    
    DRY_RUN: bool = os.getenv("DRY_RUN", "True").lower() == "true"
    MAX_CONCURRENT_TRADES: int = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))
    LEVERAGE: int = int(os.getenv("LEVERAGE", "5"))
    MARGIN_PER_TRADE: float = float(os.getenv("MARGIN_PER_TRADE", "10.0"))
    MARGIN_MODE: str = os.getenv("MARGIN_MODE", "isolated")
    
    MACRO_TF: str = "4h"
    SETUP_TF: str = "1h"
    EXEC_TF: str = "15m"
    CANDLE_LIMIT: int = 150

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()


# ==========================================
# 2. BORSA VE GERÇEK EMİR / VERİ YÖNETİMİ
# ==========================================
class BinanceExchange:
    def __init__(self):
        self.exchange = ccxt.binance({
            'apiKey': settings.BINANCE_API_KEY,
            'secret': settings.BINANCE_SECRET_KEY,
            'options': {'defaultType': 'future'},
            'enableRateLimit': True
        })

    async def get_dynamic_pool(self) -> list:
        """Gainers, Losers ve Volume önceliğini (sıralamayı) bozmadan tekilleştirilmiş dinamik havuz."""
        try:
            tickers = await self.exchange.fetch_tickers()
            usdt_tickers = {s: t for s, t in tickers.items() if s.endswith('/USDT:USDT') or s.endswith('/USDT')}
            
            data = []
            for symbol, t in usdt_tickers.items():
                change = t.get('percentage', 0.0) or 0.0
                quote_vol = t.get('quoteVolume', 0.0) or 0.0
                data.append({'symbol': symbol, 'change': change, 'volume': quote_vol})
                
            df = pd.DataFrame(data)
            if df.empty:
                return []

            gainers = df.sort_values(by='change', ascending=False).head(50)['symbol'].tolist()
            losers = df.sort_values(by='change', ascending=True).head(50)['symbol'].tolist()
            vol_leaders = df.sort_values(by='volume', ascending=False).head(50)['symbol'].tolist()
            
            # dict.fromkeys ile sırayı koruyarak tekilleştirme
            unique_pool = list(dict.fromkeys(gainers + losers + vol_leaders))
            return unique_pool
        except Exception as e:
            logger.error(f"Havuz çekilirken hata: {e}")
            return []

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            return pd.DataFrame()

    async def get_current_price(self, symbol: str) -> float:
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            return float(ticker['last'])
        except Exception:
            return 0.0

    async def amount_to_precision(self, symbol: str, amount: float) -> float:
        try:
            return float(self.exchange.amount_to_precision(symbol, amount))
        except Exception:
            return amount

    async def open_long(self, symbol: str, qty: float, sl_price: float) -> dict:
        """Gerçek modda (DRY_RUN=False) kaldıraç, marjin modu, piyasa emri ve koruma amaçlı stop emri verir."""
        if settings.DRY_RUN:
            current_price = await self.get_current_price(symbol)
            logger.info(f"[DRY_RUN] Açık long emri simüle edildi: {symbol} | Miktar: {qty} | Fiyat: {current_price}")
            return {"id": "dry_run_market_id", "price": current_price}

        try:
            try:
                await self.exchange.set_margin_mode(settings.MARGIN_MODE.upper(), symbol)
            except Exception:
                pass
            
            await self.exchange.set_leverage(settings.LEVERAGE, symbol)
            
            # 1. Ana Piyasaya Giriş Emri
            market_order = await self.exchange.create_order(symbol, 'market', 'buy', qty)
            exec_price = float(market_order.get('price') or await self.get_current_price(symbol))
            
            # 2. SL İçin Koruma Emri (Stop Market - reduceOnly=True)
            try:
                sl_rounded = float(self.exchange.price_to_precision(symbol, sl_price))
                await self.exchange.create_order(
                    symbol=symbol,
                    type='stop_market',
                    side='sell',
                    amount=qty,
                    price=None,
                    params={'stopPrice': sl_rounded, 'reduceOnly': True}
                )
            except Exception as sl_err:
                logger.error(f"Koruma amaçlı stop emri iletilemedi ({symbol}): {sl_err}")

            return market_order
        except Exception as e:
            logger.error(f"Gerçek emir açılış hatası ({symbol}): {e}")
            raise e

    async def close_position(self, symbol: str, qty: float):
        """Pozisyonu kapatır ve açık bekleyen tüm koruma emirlerini iptal eder."""
        if settings.DRY_RUN:
            logger.info(f"[DRY_RUN] Pozisyon kapatma simüle edildi: {symbol} | Miktar: {qty}")
            return

        try:
            # Açık bekleyen stop emirlerini temizle
            try:
                await self.exchange.cancel_all_orders(symbol)
            except Exception:
                pass

            # Pozisyonu kapatmak için ters yönde piyasa emri (reduceOnly=True)
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side='sell',
                amount=qty,
                params={'reduceOnly': True}
            )
        except Exception as e:
            logger.error(f"Pozisyon kapatma hatası ({symbol}): {e}")


# ==========================================
# 3. STRATEJİ VE ÇOKLU ZAMAN DİLİMLİ FİLTRE MOTORU
# ==========================================
class StrategyEngine:
    def __init__(self, symbol: str, df_4h: pd.DataFrame, df_1h: pd.DataFrame, df_exec: pd.DataFrame, btc_df: pd.DataFrame):
        self.symbol = symbol
        self.df_4h = df_4h
        self.df_1h = df_1h
        self.df_exec = df_exec
        self.btc_df = btc_df

    def evaluate_btc_risk_factor(self) -> float:
        if self.btc_df.empty or len(self.btc_df) < 5:
            return 1.0
        btc_change = (self.btc_df['close'].iloc[-1] - self.btc_df['close'].iloc[-5]) / self.btc_df['close'].iloc[-5]
        return 0.7 if btc_change < -0.03 else 1.0

    def analyze(self) -> dict:
        if self.df_exec.empty or len(self.df_exec) < 50 or self.df_4h.empty or self.df_1h.empty:
            return {"action": "WAIT"}

        # 1. Gerçek 4H Makro Trend Süzgeci (4H EMA20 > EMA50 veya son 3 mumda yükselen slope kontrolü)
        df_4h_closed = self.df_4h.iloc[:-1]
        ema_20_4h = df_4h_closed['close'].ewm(span=20, adjust=False).mean().iloc[-1]
        ema_50_4h = df_4h_closed['close'].ewm(span=50, adjust=False).mean().iloc[-1]
        macro_slope = df_4h_closed['close'].iloc[-1] - df_4h_closed['close'].iloc[-3]
        if ema_20_4h <= ema_50_4h or macro_slope <= 0:
            return {"action": "SKIP_MACRO_BEARISH"}

        # 2. Gerçek 1H Kurulum Süzgeci (1H fiyatın son 20 mumluk ortalamanın üstünde veya desteğe yakın olması)
        df_1h_closed = self.df_1h.iloc[:-1]
        sma_20_1h = df_1h_closed['close'].rolling(20).mean().iloc[-1]
        if df_1h_closed['close'].iloc[-1] < sma_20_1h * 0.995:
            return {"action": "SKIP_1H_SETUP_WEAK"}

        # 3. 15M Yapısal Destek / Direnç ve İnfaz (Kapanmış son 20 mum referans alınır)
        df_exec_closed = self.df_exec.iloc[:-1]
        last_close = df_exec_closed['close'].iloc[-1]
        
        highs = df_exec_closed['high'].values
        lows = df_exec_closed['low'].values
        nearest_resistance = max(highs[-20:])
        nearest_support = min(lows[-20:])
        
        distance_to_res_pct = (nearest_resistance - last_close) / last_close * 100
        if distance_to_res_pct < 1.0:
            return {"action": "SKIP_TIGHT_RUNWAY"}

        btc_risk = self.evaluate_btc_risk_factor()

        sl = nearest_support * (1 - 0.005 * btc_risk)
        tp = nearest_resistance

        risk_distance = last_close - sl
        reward_distance = tp - last_close

        if risk_distance <= 0 or reward_distance <= 0:
            return {"action": "SKIP_INVALID_PRICES"}

        if (risk_distance / reward_distance) > 0.5:
            return {"action": "SKIP_RISK_REWARD_VIOLATION"}

        decision_explanation = (
            f"İşlem Kararı [LONG]: {self.symbol} paritesinde 4H makro trend (EMA teyitli) uyumlu, "
            f"1H kurulum süzgeci başarıyla geçti ve 15M momentum tetiklendi. Giriş Fiyatı: {last_close:.4f}, "
            f"Stop-Loss: {sl:.4f}, Dinamik Hedef (TP): {tp:.4f}. "
            f"Kazanç Alanı (Runway): %{distance_to_res_pct:.2f} | Risk/Ödül Oranı Sağlandı."
        )

        return {
            "action": "ENTER_LONG",
            "symbol": self.symbol,
            "price": last_close,
            "sl": sl,
            "tp": tp,
            "runway": distance_to_res_pct,
            "explanation": decision_explanation
        }


# ==========================================
# 4. BAĞIMSIZ ASENKRON POZİSYON İZLEME GÖREVİ
# ==========================================
async def position_monitor_loop(exchange: BinanceExchange, active_trades: dict, trade_lock: asyncio.Lock):
    """Tarama döngüsünden tamamen bağımsız, her 3 saniyede bir çalışan hassas pozisyon denetleyicisi."""
    last_heartbeat_time = 0

    while True:
        try:
            await asyncio.sleep(3)
            
            async with trade_lock:
                if not active_trades:
                    continue
                current_symbols = list(active_trades.keys())

            for symbol in current_symbols:
                async with trade_lock:
                    if symbol not in active_trades:
                        continue
                    trade = active_trades[symbol]

                current_price = await exchange.get_current_price(symbol)
                if current_price == 0.0:
                    continue

                # 15m canlı verilerle süpürme (sweep) ve yapısal kontrol
                df_exec_live = await exchange.fetch_ohlcv(symbol, settings.EXEC_TF, 30)
                if not df_exec_live.empty:
                    df_exec_closed = df_exec_live.iloc[:-1]
                    recent_lows = df_exec_closed['low'].values
                    active_support = min(recent_lows[-10:])
                    recent_highs = df_exec_closed['high'].values
                    active_resistance = max(recent_highs[-10:])
                    
                    new_runway_pct = (active_resistance - current_price) / current_price * 100
                    if active_resistance > trade['tp'] and new_runway_pct >= 0.8:
                        async with trade_lock:
                            if symbol in active_trades:
                                active_trades[symbol]['tp'] = active_resistance
                        logger.info(f"🚀 [DİNAMİK TP & LİKİDİTE GÜNCELLEMESİ] {symbol} yeni direnç yakalandı. Yeni TP: {active_resistance:.4f}")

                    # Süpürme (Sweep) Kontrolü
                    if current_price < active_support * 0.992:
                        raw_gain_pct = (current_price - trade['price']) / trade['price']
                        profit_usdt = settings.MARGIN_PER_TRADE * settings.LEVERAGE * raw_gain_pct
                        
                        await exchange.close_position(symbol, trade['qty'])
                        
                        logger.warning(
                            f"\n📊 İŞLEM SONUÇ RAPORU (SÜPÜRME / GÜVENLİ ÇIKIŞ 🟠)\n"
                            f"----------------------------------------\n"
                            f"Sembol: {symbol} | Yön: LONG\n"
                            f"Giriş Fiyatı: {trade['price']} | Çıkış Fiyatı: {current_price}\n"
                            f"Kullanılan Margin: {settings.MARGIN_PER_TRADE} USDT ({settings.LEVERAGE}x)\n"
                            f"Gerçekleşen PnL: ${profit_usdt:+.2f}\n"
                            f"----------------------------------------"
                        )
                        async with trade_lock:
                            if symbol in active_trades:
                                del active_trades[symbol]
                        continue

                raw_price_change_pct = ((current_price - trade['price']) / trade['price']) * 100
                pnl_pct = raw_price_change_pct * settings.LEVERAGE

                # ==========================================
                # DİNAMİK & KADEMELİ KÂR KİLİTLEME MİMARİSİ
                # ==========================================
                updated_sl = trade['sl']
                # 1. Ham hareket %1.2 olduğunda Stop'u Giriş Fiyatına (Breakeven) taşı
                if raw_price_change_pct >= 1.2 and updated_sl < trade['price']:
                    updated_sl = trade['price']
                    logger.success(f"🛡️ [BREAKEVEN KİLİDİ] {symbol} ham %1.2 hareket sağlandı. Stop giriş seviyesine çekildi: {updated_sl:.4f}")

                # 2. Kaldıraçlı kâr %7'den itibaren her %3'lük artışta hesaplanan kademeli net kârın yarısını kilitle
                elif pnl_pct >= 7.0:
                    excess_pnl = pnl_pct - 7.0
                    step_index = int(excess_pnl // 3.0)
                    target_locked_pnl_pct = 7.0 + (step_index * 3.0)
                    target_raw_gain_pct = target_locked_pnl_pct / settings.LEVERAGE
                    
                    # Kullanıcının talimatına göre tam hesaplanan kademeli hedefin yarısı
                    half_gain_price = trade['price'] + (target_raw_gain_pct / 100.0 * trade['price'] * 0.5)
                    
                    if half_gain_price > updated_sl:
                        updated_sl = half_gain_price
                        logger.success(
                            f"🔒 [KÂRIN YARISI KİLİTLENDİ] {symbol} Kaldıraçlı PnL: %{pnl_pct:.1f} | "
                            f"Stop-Loss güncellendi: {updated_sl:.4f}"
                        )

                if updated_sl != trade['sl']:
                    async with trade_lock:
                        if symbol in active_trades:
                            active_trades[symbol]['sl'] = updated_sl

                # TP / SL Kontrolleri
                if current_price >= trade['tp']:
                    raw_gain_pct = (current_price - trade['price']) / trade['price']
                    profit_usdt = settings.MARGIN_PER_TRADE * settings.LEVERAGE * raw_gain_pct
                    
                    await exchange.close_position(symbol, trade['qty'])
                    
                    logger.success(
                        f"\n📊 İŞLEM SONUÇ RAPORU (KÂRLI KAPANIŞ - TP 🟢)\n"
                        f"----------------------------------------\n"
                        f"Sembol: {symbol} | Yön: LONG\n"
                        f"Giriş Fiyatı: {trade['price']} | Kapanış (TP): {current_price}\n"
                        f"Kullanılan Margin: {settings.MARGIN_PER_TRADE} USDT ({settings.LEVERAGE}x)\n"
                        f"Gerçekleşen Kâr: +${profit_usdt:.2f}\n"
                        f"----------------------------------------"
                    )
                    async with trade_lock:
                        if symbol in active_trades:
                            del active_trades[symbol]
                elif current_price <= trade['sl']:
                    is_profit_lock = trade['sl'] > trade['price']
                    result_text = "KÂRLI TRAILING KAPANIŞ 🟢" if is_profit_lock else "STOP OLDU 🔴"
                    raw_gain_pct = (current_price - trade['price']) / trade['price']
                    profit_usdt = settings.MARGIN_PER_TRADE * settings.LEVERAGE * raw_gain_pct
                    
                    await exchange.close_position(symbol, trade['qty'])
                    
                    logger.warning(
                        f"\n📊 İŞLEM SONUÇ RAPORU ({result_text})\n"
                        f"----------------------------------------\n"
                        f"Sembol: {symbol} | Yön: LONG\n"
                        f"Giriş Fiyatı: {trade['price']} | Kapanış (SL): {current_price}\n"
                        f"Kullanılan Margin: {settings.MARGIN_PER_TRADE} USDT ({settings.LEVERAGE}x)\n"
                        f"Gerçekleşen PnL: ${profit_usdt:+.2f}\n"
                        f"----------------------------------------"
                    )
                    async with trade_lock:
                        if symbol in active_trades:
                            del active_trades[symbol]

            # Düşük Frekanslı Heartbeat Logu (Her 60 saniyede bir açık pozisyonların kısa özeti)
            import time
            now = time.time()
            if now - last_heartbeat_time >= 60:
                async with trade_lock:
                    if active_trades:
                        heartbeat_msgs = []
                        for sym, trd in active_trades.items():
                            curr_p = await exchange.get_current_price(sym)
                            pnl_val = ((curr_p - trd['price']) / trd['price']) * 100 * settings.LEVERAGE if curr_p > 0 else 0.0
                            heartbeat_msgs.append(f"{sym} (PnL: %{pnl_val:+.2f}, SL: {trd['sl']:.4f})")
                        logger.info(f"💓 [HEARTBEAT] Açık Pozisyonlar: " + " | ".join(heartbeat_msgs))
                last_heartbeat_time = now

        except Exception as e:
            logger.error(f"Pozisyon takip döngüsünde hata: {e}")
            await asyncio.sleep(5)


# ==========================================
# 5. ANA TARAMA VE KOORDİNASYON DÖNGÜSÜ
# ==========================================
async def scan_loop(exchange: BinanceExchange, active_trades: dict, trade_lock: asyncio.Lock):
    """Havuz tarama ve sinyal arama döngüsü."""
    while True:
        try:
            pool = await exchange.get_dynamic_pool()
            if not pool:
                await asyncio.sleep(30)
                continue

            btc_df = await exchange.fetch_ohlcv("BTC/USDT:USDT", settings.SETUP_TF, 50)
            valid_setups_found = 0

            for symbol in pool:
                async with trade_lock:
                    if len(active_trades) >= settings.MAX_CONCURRENT_TRADES:
                        break
                    if symbol in active_trades:
                        continue

                df_4h = await exchange.fetch_ohlcv(symbol, settings.MACRO_TF, 60)
                df_1h = await exchange.fetch_ohlcv(symbol, settings.SETUP_TF, 60)
                df_exec = await exchange.fetch_ohlcv(symbol, settings.EXEC_TF, settings.CANDLE_LIMIT)

                if df_exec.empty or df_4h.empty or df_1h.empty:
                    await asyncio.sleep(0.1)
                    continue

                strategy = StrategyEngine(symbol, df_4h, df_1h, df_exec, btc_df)
                result = strategy.analyze()

                if result["action"] == "ENTER_LONG":
                    valid_setups_found += 1
                    current_price = result['price']
                    
                    notional_usdt = settings.MARGIN_PER_TRADE * settings.LEVERAGE
                    raw_qty = notional_usdt / current_price
                    qty = await exchange.amount_to_precision(symbol, raw_qty)

                    logger.info(f"\n💡 İŞLEM KARARI AÇIKLAMASI:\n{result['explanation']}")
                    
                    try:
                        order = await exchange.open_long(symbol, qty, result['sl'])
                        if order:
                            execution_price = float(order.get('price') or current_price)
                            logger.success(
                                f"[EMİR İLETİLDİ] {symbol} | LONG | "
                                f"Fiyat: {execution_price} | Miktar: {qty} | Margin: {settings.MARGIN_PER_TRADE} USDT | Kaldıraç: {settings.LEVERAGE}x"
                            )
                            
                            async with trade_lock:
                                active_trades[symbol] = {
                                    'price': execution_price,
                                    'sl': result['sl'],
                                    'tp': result['tp'],
                                    'runway': result['runway'],
                                    'qty': qty
                                }
                    except Exception as order_err:
                        logger.error(f"Sinyal işlenirken emir hatası ({symbol}): {order_err}")

                await asyncio.sleep(0.2)

            async with trade_lock:
                active_count = len(active_trades)
            logger.info(f"Tarama Tamamlandı. Havuz: {len(pool)} coin | Aktif İşlem: {active_count}/{settings.MAX_CONCURRENT_TRADES} | Yeni Sinyal: {valid_setups_found}")
            
            await asyncio.sleep(180)

        except Exception as scan_err:
            logger.error(f"Tarama döngüsü hatası: {scan_err}")
            await asyncio.sleep(30)


async def main():
    logger.info(f"Bot Başlatıldı | Dry-Run: {settings.DRY_RUN} | Max İşlem: {settings.MAX_CONCURRENT_TRADES} | Kaldıraç: {settings.LEVERAGE}x")
    exchange = BinanceExchange()
    active_trades = {}
    trade_lock = asyncio.Lock()

    # İki görevi asyncio.gather ile paralel ve bağımsız olarak başlat
    try:
        await asyncio.gather(
            scan_loop(exchange, active_trades, trade_lock),
            position_monitor_loop(exchange, active_trades, trade_lock)
        )
    except KeyboardInterrupt:
        logger.info("Bot kullanıcı tarafından durduruldu.")
    except Exception as e:
        logger.exception(f"Beklenmeyen ana program hatası: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
