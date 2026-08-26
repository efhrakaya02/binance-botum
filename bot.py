import os
import ccxt
import pandas as pd
import numpy as np
import time
import threading
from flask import Flask, jsonify

app = Flask(__name__)

API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
ORDER_SIZE = 12.0  # Net Anapara (Margin) - Ana Fırsat (KESİNLİKLE AŞILMAZ)
MAX_POSITIONS = 3  

# VUR-KAÇ (SCALP) PARAMETRELERI
SCALP_ENABLED = True
SCALP_MARGIN_SIZE = 15.0  # Scalp için net 15 USDT anapara
SCALP_LEVERAGE = 5
SCALP_TARGET_PROFIT_PCT = 1.0  
SCALP_STOP_LOSS_PCT = 0.5                     

pozisyon_en_yuksek_kar = {}

# SOĞUMA SÜRESİ (COOLDOWN) MEKANİZMASI
cooldown_suresi_ms = 4 * 3600 * 1000  # 4 Saatlik soğuma süresi
son_kapanis_zamanlari = {}

def get_exchange():
    return ccxt.binance({
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })

def sembol_duzelt(symbol):
    if symbol == 'BCC/USDT':
        return 'BCH/USDT'
    return symbol

def gecerli_kripto_mu(symbol):
    yasakli_ifadeler = ['UP/', 'DOWN/', 'BEAR/', 'BULL/', '_', 'BID', 'ASK']
    if not symbol.endswith('USDT'):
        return False
    for yasak in yasakli_ifadeler:
        if yasak in symbol:
            return False
    return True

def hesapla_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def hesapla_obv(close, volume):
    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    return obv

def hesapla_adx(df, period=14):
    """Trendin gücünü ölçmek için ADX ve DMI hesaplar"""
    df = df.copy()
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = abs(df['high'] - df['close'].shift())
    df['tr3'] = abs(df['low'] - df['close'].shift())
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    
    df['up_move'] = df['high'] - df['high'].shift()
    df['down_move'] = df['low'].shift() - df['low']
    
    df['plus_dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0.0)
    df['minus_dm'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0.0)
    
    df['atr'] = df['tr'].rolling(window=period).mean()
    df['plus_di'] = 100 * (df['plus_dm'].rolling(window=period).mean() / df['atr'])
    df['minus_di'] = 100 * (df['minus_dm'].rolling(window=period).mean() / df['atr'])
    
    df['dx'] = 100 * abs(df['plus_di'] - df['minus_di']) / (df['plus_di'] + df['minus_di'])
    df['adx'] = df['dx'].rolling(window=period).mean()
    return df

def hesapla_macd(df, fast=12, slow=26, signal=9):
    df = df.copy()
    df['ema_fast'] = df['close'].ewm(span=fast, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=slow, adjust=False).mean()
    df['macd'] = df['ema_fast'] - df['ema_slow']
    df['macd_signal'] = df['macd'].ewm(span=signal, adjust=False).mean()
    return df

def hesapla_regresyon_bantlari(df, period=50, std_multiplier=2.0):
    df = df.copy()
    y = df['close'].values
    x = np.arange(len(y))
    
    reg_upper = []
    reg_lower = []
    reg_mid = []
    
    for i in range(len(df)):
        if i < period:
            reg_mid.append(np.nan)
            reg_upper.append(np.nan)
            reg_lower.append(np.nan)
        else:
            y_window = y[i-period+1:i+1]
            x_window = np.arange(period)
            
            slope, intercept = np.polyfit(x_window, y_window, 1)
            mid_val = slope * (period - 1) + intercept
            
            residuals = y_window - (slope * x_window + intercept)
            std_val = np.std(residuals)
            
            reg_mid.append(mid_val)
            reg_upper.append(mid_val + (std_multiplier * std_val))
            reg_lower.append(mid_val - (std_multiplier * std_val))
            
    df['reg_middle'] = reg_mid
    df['reg_upper'] = reg_upper
    df['reg_lower'] = reg_lower
    return df

def formasyon_ve_sikisma_tara(df):
    df['middle_band'] = df['close'].rolling(window=20).mean()
    rolling_std = df['close'].rolling(window=20).std()
    df['upper_band'] = df['middle_band'] + (2 * rolling_std)
    df['lower_band'] = df['middle_band'] - (2 * rolling_std)
    df['bandwidth'] = (df['upper_band'] - df['lower_band']) / df['middle_band']
    df['squeeze'] = df['bandwidth'] < df['bandwidth'].rolling(window=50).mean()
    df['obv'] = hesapla_obv(df['close'], df['volume'])
    df['obv_ma'] = df['obv'].rolling(window=10).mean()
    df['obv_trend'] = df['obv'] > df['obv_ma']
    
    df = hesapla_regresyon_bantlari(df, period=50, std_multiplier=2.0)
    return df

@app.route('/')
def health_check():
    return "Gelişmiş ADX & EMA Trend Analizli, Sıkı Risk Yönetimli ve Threading Destekli Bot Aktif", 200

def arka_plan_analiz_islem():
    print("--- Gelişmiş Trend Analizli Arka Plan Döngüsü Başlatıldı ---", flush=True)
    try:
        exchange = get_exchange()
        markets = exchange.load_markets()
        
        balance_info = exchange.fetch_balance()['USDT']
        total_balance = balance_info['free'] + balance_info['used']
        
        positions = exchange.fetch_positions()
        acik_pozisyonlar = [p for p in positions if float(p['contracts']) > 0]
        print(f"Aktif Açık Toplam Pozisyon Sayısı: {len(acik_pozisyonlar)}", flush=True)
        
        aktif_semboller = [sembol_duzelt(p['symbol']) for p in acik_pozisyonlar]
        for s in list(pozisyon_en_yuksek_kar.keys()):
            s_fixed = sembol_duzelt(s)
            if s_fixed not in aktif_semboller:
                son_kapanis_zamanlari[s_fixed] = int(time.time() * 1000)
                if s in pozisyon_en_yuksek_kar:
                    del pozisyon_en_yuksek_kar[s]

        # 1. Pozisyon Yönetimi ve Erken Risk Koruması
        try:
            for p in acik_pozisyonlar:
                symbol = sembol_duzelt(p['symbol'])
                initial_margin = float(p['initialMargin'])
                entry_price = float(p['entryPrice'])
                side = p['side']
                contracts = float(p['contracts'])
                
                is_scalp_position = (abs(initial_margin - SCALP_MARGIN_SIZE) < 3.0)
                
                if initial_margin > 0 and entry_price > 0:
                    ticker = exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    
                    if side == 'long':
                        kar_yuzdesi = ((current_price - entry_price) / entry_price) * 100
                    else:
                        kar_yuzdesi = ((entry_price - current_price) / entry_price) * 100
                        
                    close_side = 'sell' if side == 'long' else 'buy'
                    
                    # --- KATI MAKSİMUM ZARAR KORUMASI (%5) ---
                    if kar_yuzdesi <= -5.0:
                        exchange.create_order(
                            symbol=symbol, type='market', side=close_side, amount=contracts, params={'reduceOnly': True}
                        )
                        print(f"[ACİL STOP - %5 SINIRI] {symbol} %5 zarar sınırına ulaştı! Kapatıldı. Zarar: %{kar_yuzdesi:.2f}", flush=True)
                        son_kapanis_zamanlari[symbol] = int(time.time() * 1000)
                        continue

                    # --- SCALP POZİSYON YÖNETİMİ ---
                    if is_scalp_position:
                        if kar_yuzdesi >= SCALP_TARGET_PROFIT_PCT:
                            exchange.create_order(
                                symbol=symbol, type='market', side=close_side, amount=contracts, params={'reduceOnly': True}
                            )
                            print(f"[SCALP] {symbol} hedef kör oranına ulaştı! Kapatıldı. Kâr: %{kar_yuzdesi:.2f}", flush=True)
                            son_kapanis_zamanlari[symbol] = int(time.time() * 1000)
                            continue
                        elif kar_yuzdesi <= -SCALP_STOP_LOSS_PCT:
                            exchange.create_order(
                                symbol=symbol, type='market', side=close_side, amount=contracts, params={'reduceOnly': True}
                            )
                            print(f"[SCALP] {symbol} stop loss sınırına ulaştı! Kapatıldı. Zarar: %{kar_yuzdesi:.2f}", flush=True)
                            son_kapanis_zamanlari[symbol] = int(time.time() * 1000)
                            continue
                        else:
                            print(f"[SCALP Takipte] {symbol} | Yön: {side.upper()} | Anlık Kâr: %{kar_yuzdesi:.2f}", flush=True)
                            continue 

                    # --- ANA TREND POZİSYON YÖNETİMİ & ERKEN HASSAS STOP ---
                    if symbol not in pozisyon_en_yuksek_kar:
                        pozisyon_en_yuksek_kar[symbol] = kar_yuzdesi
                    else:
                        if kar_yuzdesi > pozisyon_en_yuksek_kar[symbol]:
                            pozisyon_en_yuksek_kar[symbol] = kar_yuzdesi
                            
                    en_yuksek_kar = pozisyon_en_yuksek_kar[symbol]
                    
                    if en_yuksek_kar >= 5.0 and (en_yuksek_kar - kar_yuzdesi >= 3.5): 
                        exchange.create_order(
                            symbol=symbol, type='market', side=close_side, amount=contracts, params={'reduceOnly': True}
                        )
                        print(f"[{symbol}] Zirveden erken kar koruması! En yüksek: %{en_yuksek_kar:.2f}, Kapatıldı.", flush=True)
                        son_kapanis_zamanlari[symbol] = int(time.time() * 1000)
                        continue
                    
                    ohlcv_1h = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
                    df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df_1h['ema9'] = df_1h['close'].ewm(span=9, adjust=False).mean()
                    df_1h['ema21'] = df_1h['close'].ewm(span=21, adjust=False).mean()
                    df_1h = hesapla_macd(df_1h)
                    
                    ema9_val = df_1h['ema9'].iloc[-1]
                    ema21_val = df_1h['ema21'].iloc[-1]
                    macd_val = df_1h['macd'].iloc[-1]
                    macd_sig = df_1h['macd_signal'].iloc[-1]
                    
                    erken_kesin_stop = False
                    if side == 'long' and (ema9_val < ema21_val or macd_val < macd_sig):
                        erken_kesin_stop = True
                    elif side == 'short' and (ema9_val > ema21_val or macd_val > macd_sig):
                        erken_kesin_stop = True
                        
                    if erken_kesin_stop:
                        exchange.create_order(
                            symbol=symbol, type='market', side=close_side, amount=contracts, params={'reduceOnly': True}
                        )
                        print(f"[{symbol}] Hassas EMA/MACD erken risk koruması ile kapatıldı! Kâr/Zarar: %{kar_yuzdesi:.2f}", flush=True)
                        son_kapanis_zamanlari[symbol] = int(time.time() * 1000)
                        continue
                        
                    try:
                        hedef_stop_fiyat = None
                        if kar_yuzdesi >= 20:
                            hedef_stop_fiyat = entry_price * (1.15 if side == 'long' else 0.85)
                        elif kar_yuzdesi >= 15:
                            hedef_stop_fiyat = entry_price * (1.10 if side == 'long' else 0.90)
                        elif kar_yuzdesi >= 10:
                            hedef_stop_fiyat = entry_price * (1.05 if side == 'long' else 0.95)
                        elif kar_yuzdesi >= 5:
                            hedef_stop_fiyat = entry_price
                            
                        if hedef_stop_fiyat is not None:
                            open_orders = exchange.fetch_open_orders(symbol)
                            for o in open_orders:
                                if o['type'] == 'STOP_MARKET':
                                    exchange.cancel_order(o['id'], symbol)
                                    
                            stop_params = {
                                'stopPrice': float(exchange.price_to_precision(symbol, hedef_stop_fiyat)),
                                'reduceOnly': True
                            }
                            exchange.create_order(symbol, 'STOP_MARKET', close_side, contracts, params=stop_params)
                            print(f"[{symbol}] Kademeli Stop Güncellendi: {hedef_stop_fiyat:.4f}", flush=True)
                    except Exception as stop_up_err:
                        print(f"Kademeli stop güncelleme hatası: {stop_up_err}", flush=True)
                        
                    print(f"[{symbol}] Pozisyon takipte ({side.upper()}). Kâr: %{kar_yuzdesi:.2f}", flush=True)
        except Exception as pos_err:
            print(f"Pozisyon yönetimi hatası: {pos_err}", flush=True)

        gecerli_coin_listesi = [sembol_duzelt(s) for s in markets.keys() if gecerli_kripto_mu(s)]

        # SOĞUMA SÜRESİ KONTROLÜ
        simdiki_zaman = int(time.time() * 1000)
        gecerli_coin_listesi = [
            s for s in gecerli_coin_listesi 
            if s not in son_kapanis_zamanlari or (simdiki_zaman - son_kapanis_zamanlari[s]) > cooldown_suresi_ms
        ]

        # 2. VUR-KAÇ (SCALP) MODÜLÜ
        scalp_aktif_var = any(abs(float(p['initialMargin']) - SCALP_MARGIN_SIZE) < 3.0 for p in acik_pozisyonlar)
        
        if SCALP_ENABLED and not scalp_aktif_var:
            try:
                for symbol in gecerli_coin_listesi[:35]:
                    symbol = sembol_duzelt(symbol)
                    if symbol in aktif_semboller:
                        continue
                    
                    market = markets.get(symbol, {})
                    limits = market.get('limits', {})
                    min_amount = limits.get('amount', {}).get('min', 0.001)
                    
                    ohlcv_5m = exchange.fetch_ohlcv(symbol, timeframe='5m', limit=40)
                    if len(ohlcv_5m) < 40:
                        continue
                    df_5m = pd.DataFrame(ohlcv_5m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    cur_close = df_5m['close'].iloc[-1]
                    
                    notional_target_scalp = SCALP_MARGIN_SIZE * SCALP_LEVERAGE
                    raw_amount_scalp = notional_target_scalp / cur_close
                    amount_scalp = float(exchange.amount_to_precision(symbol, max(raw_amount_scalp, min_amount)))
                    gerceklesen_marj = (amount_scalp * cur_close) / SCALP_LEVERAGE
                    if gerceklesen_marj > (SCALP_MARGIN_SIZE * 1.35): 
                        continue

                    avg_vol_5m = df_5m['volume'].mean()
                    if df_5m['volume'].iloc[-1] < (avg_vol_5m * 1.3):
                        continue
                        
                    df_5m['rsi'] = hesapla_rsi(df_5m['close'], period=14)
                    df_5m['ema20'] = df_5m['close'].ewm(span=20, adjust=False).mean()
                    
                    std_5m = df_5m['close'].rolling(window=20).std()
                    df_5m['middle'] = df_5m['close'].rolling(window=20).mean()
                    df_5m['upper'] = df_5m['middle'] + (2 * std_5m)
                    df_5m['lower'] = df_5m['middle'] - (2 * std_5m)
                    
                    prev_close = df_5m['close'].iloc[-2]
                    cur_lower = df_5m['lower'].iloc[-1]
                    cur_upper = df_5m['upper'].iloc[-1]
                    cur_rsi = df_5m['rsi'].iloc[-1]
                    ema20_5m = df_5m['ema20'].iloc[-1]
                    
                    ohlcv_15m = exchange.fetch_ohlcv(symbol, timeframe='15m', limit=30)
                    if len(ohlcv_15m) < 30:
                        continue
                    df_15m = pd.DataFrame(ohlcv_15m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df_15m['rsi'] = hesapla_rsi(df_15m['close'], period=14)
                    rsi_15m = df_15m['rsi'].iloc[-1]
                    
                    scalp_yon = None
                    long_sart_5m = ((prev_close <= cur_lower or cur_close <= cur_lower) or cur_rsi < 38) and (cur_close >= ema20_5m * 0.99)
                    long_sart_15m = (rsi_15m < 60)
                    
                    if long_sart_5m and long_sart_15m:
                        scalp_yon = 'buy'
                    else:
                        short_sart_5m = ((cur_close >= cur_upper) or cur_rsi > 62) and (cur_close <= ema20_5m * 1.01)
                        short_sart_15m = (rsi_15m > 40)
                        if short_sart_5m and short_sart_15m:
                            scalp_yon = 'sell'
                        
                    if scalp_yon:
                        try:
                            exchange.set_margin_mode('isolated', symbol)
                        except:
                            pass
                        try:
                            exchange.set_leverage(SCALP_LEVERAGE, symbol)
                        except:
                            pass
                            
                        exchange.create_order(symbol, 'market', scalp_yon, amount_scalp)
                        print(f"!!! SCALP AÇILDI: {symbol} | Yön: {scalp_yon.upper()} | Miktar: {amount_scalp} !!!", flush=True)
                        break 
            except Exception as scalp_err:
                print(f"Vur-Kaç tarama hatası: {scalp_err}", flush=True)

        # 3. ANA FIRSAT MODÜLÜ (Gelişmiş ADX ve EMA Trend Filtreli)
        normal_acik_sayisi = len([p for p in acik_pozisyonlar if not abs(float(p['initialMargin']) - SCALP_MARGIN_SIZE) < 3.0])
        
        if normal_acik_sayisi >= MAX_POSITIONS:
            print("Scalp çalışıyor, ana pozisyonlar dolu.", flush=True)
            return

        coin_listesi = []
        for symbol in gecerli_coin_listesi:
            symbol = sembol_duzelt(symbol)
            try:
                t = exchange.fetch_ticker(symbol)
                degisim_yuzdesi = 0.0
                if 'info' in t and t['info'] is not None:
                    degisim_yuzdesi = float(t['info'].get('priceChangePercent', 0) or 0)
                else:
                    last_p = t.get('last', 0) or 0
                    open_p = t.get('open', 0) or last_p
                    if open_p > 0 and last_p > 0:
                        degisim_yuzdesi = ((last_p - open_p) / open_p) * 100
                coin_listesi.append({'symbol': symbol, 'change': degisim_yuzdesi})
            except Exception:
                continue

        coin_listesi.sort(key=lambda x: x['change'], reverse=True)
        top_gainers = [item['symbol'] for item in coin_listesi[:20]]
        top_losers = [item['symbol'] for item in coin_listesi[-20:]]
        target_symbols = list(set(top_gainers + top_losers))
        
        en_iyi_fırsat = None
        
        for symbol in target_symbols[:15]:
            symbol = sembol_duzelt(symbol)
            try:
                time.sleep(0.1)
                if symbol in [sembol_duzelt(p['symbol']) for p in acik_pozisyonlar]:
                    continue
                
                market = markets.get(symbol, {})
                limits = market.get('limits', {})
                min_amount = limits.get('amount', {}).get('min', 0.001)
                
                t_check = exchange.fetch_ticker(symbol)
                current_price_check = t_check['last']
                
                notional_target_check = ORDER_SIZE * 10 
                raw_amount_check = notional_target_check / current_price_check
                amount_check = float(exchange.amount_to_precision(symbol, max(raw_amount_check, min_amount)))
                gerceklesen_marj_check = (amount_check * current_price_check) / 10
                if gerceklesen_marj_check > (ORDER_SIZE * 1.35): 
                    continue

                funding_info = exchange.fetch_funding_rate(symbol)
                funding_rate = funding_info.get('fundingRate', 0) or 0
                if funding_rate > 0.002 or funding_rate < -0.002:
                    continue
                
                ohlcv_1h = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=60)
                df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                
                ortalama_hacim = df_1h['volume'].mean()
                son_hacim = df_1h['volume'].iloc[-1]
                if son_hacim < (ortalama_hacim * 1.2):
                    continue
                
                df_1h['candle_size'] = abs(df_1h['close'] - df_1h['open'])
                avg_candle_size = df_1h['candle_size'].rolling(window=15).mean().iloc[-1]
                if df_1h['candle_size'].iloc[-1] > (avg_candle_size * 3.5):
                    continue 
                
                df_1h = formasyon_ve_sikisma_tara(df_1h)
                df_1h = hesapla_adx(df_1h)
                df_1h = hesapla_macd(df_1h)
                
                obv_onay = df_1h['obv_trend'].iloc[-1]
                df_1h['rsi'] = hesapla_rsi(df_1h['close'], period=14)
                current_rsi = df_1h['rsi'].iloc[-1]
                
                # ADX Güçlü Trend Kontrolü (ADX > 20 olmalı ki testere piyasaya girmesin)
                current_adx = df_1h['adx'].iloc[-1]
                if pd.isna(current_adx) or current_adx < 20:
                    continue
                
                ohlcv_4h = exchange.fetch_ohlcv(symbol, timeframe='4h', limit=60)
                df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df_4h['ema50'] = df_4h['close'].ewm(span=50, adjust=False).mean()
                trend_4h_up = df_4h['close'].iloc[-1] > df_4h['ema50'].iloc[-1]
                
                df_1h['ema9'] = df_1h['close'].ewm(span=9, adjust=False).mean()
                df_1h['ema21'] = df_1h['close'].ewm(span=21, adjust=False).mean()
                
                current_price = df_1h['close'].iloc[-1]
                ema9_1h = df_1h['ema9'].iloc[-1]
                ema21_1h = df_1h['ema21'].iloc[-1]
                macd_val = df_1h['macd'].iloc[-1]
                macd_sig = df_1h['macd_signal'].iloc[-1]
                
                reg_upper_val = df_1h['reg_upper'].iloc[-1]
                reg_lower_val = df_1h['reg_lower'].iloc[-1]
                
                if pd.isna(ema9_1h) or pd.isna(current_rsi) or pd.isna(reg_upper_val):
                    continue
                
                kalite_puani = 0
                # Gelişmiş Long Şartı (ADX > 20, EMA9 > EMA21, MACD pozitif, RSI 45-65)
                if trend_4h_up and (ema9_1h > ema21_1h) and (macd_val > macd_sig) and (45 <= current_rsi <= 65):
                    if current_price < reg_upper_val:
                        kalite_puani += 1
                        if obv_onay: kalite_puani += 1
                        if current_rsi >= 50 and current_rsi <= 60: kalite_puani += 1
                        if df_1h['squeeze'].iloc[-1]: kalite_puani += 1
                        en_iyi_fırsat = {'symbol': symbol, 'side': 'buy', 'price': current_price, 'score': max(1, kalite_puani)}
                        break
                # Gelişmiş Short Şartı (ADX > 20, EMA9 < EMA21, MACD negatif, RSI 35-55)
                elif not trend_4h_up and (ema9_1h < ema21_1h) and (macd_val < macd_sig) and (35 <= current_rsi <= 55):
                    if current_price > reg_lower_val:
                        kalite_puani += 1
                        if not obv_onay: kalite_puani += 1
                        if current_rsi >= 40 and current_rsi <= 50: kalite_puani += 1
                        if df_1h['squeeze'].iloc[-1]: kalite_puani += 1
                        en_iyi_fırsat = {'symbol': symbol, 'side': 'sell', 'price': current_price, 'score': max(1, kalite_puani)}
                        break
            except Exception:
                continue

        if en_iyi_fırsat:
            symbol = sembol_duzelt(en_iyi_fırsat['symbol'])
            side = en_iyi_fırsat['side']
            current_price = en_iyi_fırsat['price']
            score = en_iyi_fırsat['score']
            
            if score >= 4:
                hesaplanan_kaldirac = 10
            elif score == 3:
                hesaplanan_kaldirac = 7
            elif score == 2:
                hesaplanan_kaldirac = 5
            else:
                hesaplanan_kaldirac = 10  
                
            try:
                exchange.set_margin_mode('isolated', symbol)
            except Exception:
                pass
                
            try:
                exchange.set_leverage(hesaplanan_kaldirac, symbol)
            except Exception:
                pass

            market = markets.get(symbol, {})
            notional_target = ORDER_SIZE * hesaplanan_kaldirac
            raw_amount = notional_target / current_price
            
            limits = market.get('limits', {})
            min_amount = limits.get('amount', {}).get('min', 0.001)
            amount = float(exchange.amount_to_precision(symbol, max(raw_amount, min_amount)))
            
            toplam_kullanilan = sum([float(p['initialMargin']) for p in acik_pozisyonlar])
            max_aktif_limit = total_balance * 0.6
            
            if toplam_kullanilan + ORDER_SIZE <= max_aktif_limit:
                exchange.create_order(symbol, 'market', side, amount)
                print(f"!!! ANA FIRSAT AÇILDI: {symbol} | Yön: {side.upper()} | Miktar: {amount} !!!", flush=True)
                
                try:
                    ohlcv_stop = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=20)
                    df_s = pd.DataFrame(ohlcv_stop, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    tr_s = pd.concat([df_s['high'] - df_s['low'], abs(df_s['high'] - df_s['close'].shift()), abs(df_s['low'] - df_s['close'].shift())], axis=1).max(axis=1)
                    atr_val = tr_s.rolling(window=10).mean().iloc[-1]
                    
                    stop_side = 'sell' if side == 'buy' else 'buy'
                    stop_price = current_price - (2.0 * atr_val) if side == 'buy' else current_price + (2.0 * atr_val)
                        
                    stop_params = {
                        'stopPrice': float(exchange.price_to_precision(symbol, stop_price)),
                        'reduceOnly': True
                    }
                    exchange.create_order(symbol, 'STOP_MARKET', stop_side, amount, params=stop_params)
                    print(f"[{symbol}] Güvenlik stop emri yerleştirildi.", flush=True)
                except Exception as borsa_stop_err:
                    print(f"Borsa stop emri hatası: {borsa_stop_err}", flush=True)

        print("--- Gelişmiş Trend Analiz Döngüsü Tamamlandı ---", flush=True)
        
    except Exception as e:
        print(f"Genel arka plan analiz döngüsü hatası: {str(e)}", flush=True)

@app.route('/otomatik-analiz')
def otomatik_analiz():
    thread = threading.Thread(target=arka_plan_analiz_islem)
    thread.daemon = True
    thread.start()
    return jsonify({"durum": "Basarili", "mesaj": "Gelişmiş trend analizli tarama arka planda başlatıldı."}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
