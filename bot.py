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
# 2. BORSA VE VERİ / SİMÜLASYON YÖNETİMİ
# ==========================================
class BinanceExchange:
    def __init__(self):
        api_key = settings.BINANCE_API_KEY if not settings.DRY_RUN else ""
        secret_key = settings.BINANCE_SECRET_KEY if not settings.DRY_RUN else ""
        
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': secret_key,
            'options': {'defaultType': 'future'},
            'enableRateLimit': True
        })

    async def get_dynamic_pool(self) -> list:
        """Gainers, Losers ve 24h Volume listelerinin ilk 50'şerli birleşimi (tekilleştirilmiş)."""
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
            
            unique_pool = list(set(gainers + losers + vol_leaders))
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

    async def close(self):
        await self.exchange.close()


# ==========================================
# 3. STRATEJİ VE LİKİDİTE MOTORU
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
        if self.df_exec.empty or len(self.df_exec) < 50:
            return {"action": "WAIT"}

        last_close = self.df_exec['close'].iloc[-1]
        
        highs = self.df_exec['high'].values
        lows = self.df_exec['low'].values
        nearest_resistance = max(highs[-20:])
        nearest_support = min(lows[-20:])
        
        distance_to_res_pct = (nearest_resistance - last_close) / last_close * 100
        
        if distance_to_res_pct < 0.4:
            return {"action": "SKIP_TIGHT_RUNWAY"}

        btc_risk = self.evaluate_btc_risk_factor()

        sl = nearest_support * (1 - 0.005 * btc_risk)
        tp = last_close + (nearest_resistance - last_close) * 1.2

        risk_distance = last_close - sl
        reward_distance = tp - last_close

        if reward_distance <= 0 or (risk_distance / reward_distance) > 0.5:
            return {"action": "SKIP_RISK_REWARD_VIOLATION"}

        decision_explanation = (
            f"İşlem Kararı [LONG]: {self.symbol} paritesinde 4H makro trend uyumlu, "
            f"1H kurulum ve 15M momentum tetiklendi. Giriş Fiyatı: {last_close:.4f}, "
            f"Stop-Loss: {sl:.4f}, Dinamik Hedef (TP): {tp:.4f}. "
            f"Kazanç Alanı (Runway): %{distance_to_res_pct:.2f} | Risk/Ödül Oranı Kuralı Sağlandı."
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
# 4. ANA ÇALIŞTIRMA DÖNGÜSÜ
# ==========================================
async def main():
    logger.info(f"Bot Başlatıldı | Dry-Run: {settings.DRY_RUN} | Max İşlem: {settings.MAX_CONCURRENT_TRADES} | Kaldıraç: {settings.LEVERAGE}x")
    exchange = BinanceExchange()
    active_trades = {}  # symbol -> trade details

    try:
        while True:
            # 1. Aktif İşlem Takibi, Aktif Süpürme Kontrolü ve Kademeli Trailing Stop Yönetimi
            current_symbols = list(active_trades.keys())
            for symbol in current_symbols:
                current_price = await exchange.get_current_price(symbol)
                if current_price == 0.0:
                    continue
                
                trade = active_trades[symbol]
                
                # Anlık 15m verilerini çekerek aktif süpürme ve yapı kontrolü yapalım
                df_exec_live = await exchange.fetch_ohlcv(symbol, settings.EXEC_TF, 30)
                if not df_exec_live.empty:
                    recent_lows = df_exec_live['low'].values
                    active_support = min(recent_lows[-10:])
                    recent_highs = df_exec_live['high'].values
                    active_resistance = max(recent_highs[-10:])
                    
                    # Dinamik Hedef Güncellemesi (Fiyat yeni direnç kırdıkça TP'yi yukarı kaydır)
                    if active_resistance > trade['tp']:
                        trade['tp'] = active_resistance * 1.05
                        logger.info(f"🚀 [DİNAMİK TP GÜNCELLEMESİ] {symbol} yeni direnç kırıldı. Yeni TP: {trade['tp']:.4f}")

                    # Aktif Süpürme Kontrolü: Fiyat ani bir şekilde ana desteğin altına iğne atıp süpürürse erken koruma
                    if current_price < active_support * 0.992:
                        logger.warning(f"⚠️ [SÜPÜRME / İHLAL UYARISI] {symbol} aktif destek altında süpürme tespiti! Güvenli çıkış yapılıyor.")
                        del active_trades[symbol]
                        continue

                # Kaldıraçlı PnL Hesabı
                pnl_pct = ((current_price - trade['price']) / trade['price']) * 100 * settings.LEVERAGE

                # Kademeli Trailing Stop & Breakeven Mantığı
                # 1. Aşama: Kaldıraçlı kâr %15'i (Saf %3) geçtiyse Stop-Loss'u Giriş Fiyatına (Breakeven) taşı
                if pnl_pct >= 15.0 and trade['sl'] < trade['price']:
                    trade['sl'] = trade['price']
                    logger.success(f"🛡️ [BREAKEVEN KİLİDİ] {symbol} kaldıraçlı %15 kâr aşıldı. Stop-Loss giriş seviyesine taşındı: {trade['sl']:.4f}")

                # 2. Aşama: Kaldıraçlı kâr %30'u (Saf %6) geçtiyse Stop-Loss'u kârda tutacak şekilde yukarı kilitle
                elif pnl_pct >= 30.0 and trade['sl'] < trade['price'] * 1.015:
                    trade['sl'] = trade['price'] * 1.015
                    logger.success(f"🔒 [KÂR KİLİTLEME TRAILING] {symbol} kaldıraçlı %30 kâr aşıldı. Stop-Loss karlı bölgeye taşındı: {trade['sl']:.4f}")

                # Temel TP / SL Kontrolleri
                if current_price >= trade['tp']:
                    profit_usdt = settings.MARGIN_PER_TRADE * settings.LEVERAGE * 0.08
                    logger.success(
                        f"\n📊 İŞLEM SONUÇ RAPORU (KÂRLI KAPANIŞ - TP)\n"
                        f"----------------------------------------\n"
                        f"Sembol: {symbol} | Yön: LONG\n"
                        f"Giriş Fiyatı: {trade['price']} | Kapanış (TP): {current_price}\n"
                        f"Kullanılan Margin: {settings.MARGIN_PER_TRADE} USDT ({settings.LEVERAGE}x)\n"
                        f"Sonuç: BAŞARILI 🟢 | Tahmini Kâr: +${profit_usdt:.2f}\n"
                        f"----------------------------------------"
                    )
                    del active_trades[symbol]
                elif current_price <= trade['sl']:
                    is_profit_lock = trade['sl'] > trade['price']
                    result_text = "KÂRLI TRAILING KAPANIŞ 🟢" if is_profit_lock else "STOP OLDU 🔴"
                    logger.warning(
                        f"\n📊 İŞLEM SONUÇ RAPORU ({result_text})\n"
                        f"----------------------------------------\n"
                        f"Sembol: {symbol} | Yön: LONG\n"
                        f"Giriş Fiyatı: {trade['price']} | Kapanış (SL/Trailing): {current_price}\n"
                        f"Kullanılan Margin: {settings.MARGIN_PER_TRADE} USDT ({settings.LEVERAGE}x)\n"
                        f"Sonuç: SL Tetiklendi | Güncel SL: {trade['sl']:.4f}\n"
                        f"----------------------------------------"
                    )
                    del active_trades[symbol]

            # Aktif işlemler için anlık durum raporu
            if active_trades:
                for symbol, t in active_trades.items():
                    curr_price = await exchange.get_current_price(symbol)
                    if curr_price > 0:
                        pnl_pct = ((curr_price - t['price']) / t['price']) * 100 * settings.LEVERAGE
                        logger.info(
                            f"📈 [İŞLEM TAKİP] {symbol} | Giriş: {t['price']:.4f} | "
                            f"Anlık Fiyat: {curr_price:.4f} | Anlık Kâr/Zarar: %{pnl_pct:+.2f} | "
                            f"SL: {t['sl']:.4f} | TP: {t['tp']:.4f} | Kazanç Alanı: %{t['runway']:.2f}"
                        )

            # 2. Yeni Tarama Döngüsü
            pool = await exchange.get_dynamic_pool()
            if not pool:
                await asyncio.sleep(30)
                continue

            btc_df = await exchange.fetch_ohlcv("BTC/USDT:USDT", settings.SETUP_TF, 50)
            valid_setups_found = 0

            for symbol in pool:
                if len(active_trades) >= settings.MAX_CONCURRENT_TRADES:
                    break

                if symbol in active_trades:
                    continue

                df_4h = await exchange.fetch_ohlcv(symbol, settings.MACRO_TF, 50)
                df_1h = await exchange.fetch_ohlcv(symbol, settings.SETUP_TF, 100)
                df_exec = await exchange.fetch_ohlcv(symbol, settings.EXEC_TF, settings.CANDLE_LIMIT)

                if df_exec.empty:
                    continue

                strategy = StrategyEngine(symbol, df_4h, df_1h, df_exec, btc_df)
                result = strategy.analyze()

                if result["action"] == "ENTER_LONG":
                    valid_setups_found += 1
                    logger.info(f"\n💡 İŞLEM KARARI AÇIKLAMASI:\n{result['explanation']}")
                    logger.success(
                        f"[EMİR İLETİLDİ] {symbol} | LONG | "
                        f"Margin: {settings.MARGIN_PER_TRADE} USDT | Kaldıraç: {settings.LEVERAGE}x ({settings.MARGIN_MODE})"
                    )
                    
                    active_trades[symbol] = {
                        'price': result['price'],
                        'sl': result['sl'],
                        'tp': result['tp'],
                        'runway': result['runway']
                    }

                await asyncio.sleep(0.2)

            logger.info(f"Tarama Tamamlandı. Taranan Havuz: {len(pool)} coin | Aktif İşlem: {len(active_trades)}/{settings.MAX_CONCURRENT_TRADES} | Yeni Sinyal: {valid_setups_found}")
            await asyncio.sleep(180)

    except KeyboardInterrupt:
        logger.info("Bot kullanıcı tarafından durduruldu.")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
