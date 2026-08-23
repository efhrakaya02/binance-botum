import os
import ccxt
import pandas as pd
import numpy as np
import time
from flask import Flask, jsonify

app = Flask(__name__)

API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
ORDER_SIZE = 12.0  
MAX_POSITIONS = 2  

def get_exchange():
    return ccxt.binance({
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })

def hesapla_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def hesapla_supertrend(df, period=10, multiplier=3):
    high = df['high']
    low = df['low']
    close = df['close']
    
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    
    hl2 = (high + low) / 2
    final_upperband = hl2 + (multiplier * atr)
    final_lowerband = hl2 - (multiplier * atr)
    
    supertrend = [True] * len(df)
    
    for i in range(period, len(df)):
        curr_close = close.iloc[i]
        prev_close = close.iloc[i-1]
        
        if final_upperband.iloc[i] < final_upperband.iloc[i-1] or prev_close > final_upperband.iloc[i-1]:
            pass
        else:
            final_upperband.iloc[i] = final_upperband.iloc[i-1]
            
        if final_lowerband.iloc[i] > final_lowerband.iloc[i-1] or prev_close < final_lowerband.iloc[i-1]:
            pass
        else:
            final_lowerband.iloc[i] = final_lowerband.iloc[i-1]
            
        if i == period:
            supertrend[i] = True if curr_close > final_upperband.iloc[i] else False
        else:
            prev_st = supertrend[i-1]
            if prev_st == True and curr_close <= final_lowerband.iloc[i]:
                supertrend[i] = False
            elif prev_st == False and curr_close >= final_upperband.iloc[i]:
                supertrend[i] = True
            else:
                supertrend[i] = prev_st
                
    df['supertrend'] = supertrend
    df['atr'] = atr
    return df

@app.route('/')
def health_check():
    return "RSI, Hacim Onaylı ve Kademeli Stop Kilitli Bot Aktif", 200

@app.route('/otomatik-analiz')
def otomatik_analiz():
    print("--- Gelişmiş Filtreli Tarama ve Kademeli Stop Yönetimi Başlatıldı ---", flush=True)
    try:
        exchange = get_exchange()
        exchange.load_markets()
        
        balance_info = exchange.fetch_balance()['USDT']
        total_balance = balance_info['free'] + balance_info['used']
        
        positions = exchange.fetch_positions()
        acik_pozisyonlar = [p for p in positions if float(p['contracts']) > 0]
        print(f"Aktif Açık Pozisyon Sayısı: {len(acik_pozisyonlar)}", flush=True)
        
        # 1. Kademeli Stop Yükseltme ve Trend Yönetimi
        try:
            for p in acik_pozisyonlar:
                symbol = p['symbol']
                initial_margin = float(p['initialMargin'])
                entry_price = float(p['entryPrice'])
                side = p['side']
                contracts = float(p['contracts'])
                
                if initial_margin > 0 and entry_price > 0:
                    ticker = exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    
                    if side == 'long':
                        kar_yuzdesi = ((current_price - entry_price) / entry_price) * 100
                    else:
                        kar_yuzdesi = ((entry_price - current_price) / entry_price) * 100
                        
                    # Trend Kontrolü (1h SuperTrend ve EMA50)
                    ohlcv_1h = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
                    df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df_1h['ema50'] = df_1h['close'].ewm(span=50, adjust=False).mean()
                    df_1h = hesapla_supertrend(df_1h, period=10, multiplier=3)
                    
                    st_bullish = df_1h['supertrend'].iloc[-1]
                    ema50_val = df_1h['ema50'].iloc[-1]
                    
                    trend_tersine_dondu = False
                    if side == 'long' and (not st_bullish or current_price < ema50_val):
                        trend_tersine_dondu = True
                    elif side == 'short' and (st_bullish or current_price > ema50_val):
                        trend_tersine_dondu = True
                        
                    close_side = 'sell' if side == 'long' else 'buy'
                    
                    if trend_tersine_dondu:
                        exchange.create_order(
                            symbol=symbol, type='market', side=close_side, amount=contracts, params={'reduceOnly': True}
                        )
                        print(f"[{symbol}] Trend tersine döndü! Pozisyon kapatıldı. Kâr/Zarar: %{kar_yuzdesi:.2f}", flush=True)
                        continue
                        
                    # Kademeli Stop Güncelleme Mantığı (Her %5 artışta stopu karlı noktaya sabitle)
                    # Not: Borsa emirleri güncellenirken önce açık stop emirleri iptal edilip yeni stop fiyatı girilir.
                    try:
                        hedef_stop_fiyat = None
                        if kar_yuzdesi >= 20:
                            hedef_stop_fiyat = entry_price * (1.15 if side == 'long' else 0.85) # %15 kâr garantisi
                        elif kar_yuzdesi >= 15:
                            hedef_stop_fiyat = entry_price * (1.10 if side == 'long' else 0.90) # %10 kâr garantisi
                        elif kar_yuzdesi >= 10:
                            hedef_stop_fiyat = entry_price * (1.05 if side == 'long' else 0.95) # %5 kâr garantisi
                        elif kar_yuzdesi >= 5:
                            hedef_stop_fiyat = entry_price # Başa baş (Breakeven)
                            
                        if hedef_stop_fiyat is not None:
                            # Açık emirleri temizle ve yeni stop emri koy
                            open_orders = exchange.fetch_open_orders(symbol)
                            for o in open_orders:
                                if o['type'] == 'STOP_MARKET':
                                    exchange.cancel_order(o['id'], symbol)
                                    
                            stop_params = {
                                'stopPrice': float(exchange.price_to_precision(symbol, hedef_stop_fiyat)),
                                'reduceOnly': True
                            }
                            exchange.create_order(symbol, 'STOP_MARKET', close_side, contracts, params=stop_params)
                            print(f"[{symbol}] Kademeli Stop Güncellendi! Kâr: %{kar_yuzdesi:.2f}, Yeni Stop: {hedef_stop_fiyat:.4f}", flush=True)
                    except Exception as stop_up_err:
                        print(f"Kademeli stop güncelleme hatası: {stop_up_err}", flush=True)
                        
                    print(f"[{symbol}] Pozisyon takipte ({side.upper()}). Giriş: {entry_price}, Anlık: {current_price}, Kâr: %{kar_yuzdesi:.2f}", flush=True)
        except Exception as pos_err:
            print(f"Pozisyon yönetimi hatası: {pos_err}", flush=True)

        if len(acik_pozisyonlar) >= MAX_POSITIONS:
            print(f"Maksimum pozisyon sınırına ({MAX_POSITIONS}) ulaşıldı.", flush=True)
            return jsonify({"durum": "Beklemede", "mesaj": "Maksimum pozisyona ulaşıldı."})

        # 2. Dinamik Havuz ve Filtreleme (RSI + Gerçek Hacim Onayı)
        tickers = exchange.fetch_tickers()
        coin_listesi = []
        
        for symbol, t in tickers.items():
            if 'USDT' in symbol and not any(tradfi in symbol for tradfi in ['UP/', 'DOWN/', 'BEAR/', 'BULL/']):
                degisim_yuzdesi = 0.0
                vol = 0.0
                if 'info' in t and t['info'] is not None:
                    degisim_yuzdesi = float(t['info'].get('priceChangePercent', 0) or 0)
                    vol = float(t['info'].get('quoteVolume', 0) or 0)
                else:
                    last_p = t.get('last', 0) or 0
                    open_p = t.get('open', 0) or last_p
                    if open_p > 0 and last_p > 0:
                        degisim_yuzdesi = ((last_p - open_p) / open_p) * 100
                    vol = t.get('quoteVolume', 0) or 0

                coin_listesi.append({'symbol': symbol, 'change': degisim_yuzdesi, 'volume': vol})
        
        if len(coin_listesi) == 0:
            for s in exchange.symbols:
                if 'USDT' in s:
                    coin_listesi.append({'symbol': s, 'change': 0.0, 'volume': 1.0})

        coin_listesi.sort(key=lambda x: x['change'], reverse=True)
        top_gainers = [item['symbol'] for item in coin_listesi[:15]]
        top_losers = [item['symbol'] for item in coin_listesi[-15:]]
        target_symbols = list(set(top_gainers + top_losers))
        
        en_iyi_fırsat = None
        
        for symbol in target_symbols[:10]:
            try:
                time.sleep(0.1)
                if symbol in [p['symbol'] for p in acik_pozisyonlar]:
                    continue
                
                # Fonlama Oranı Kontrolü
                funding_info = exchange.fetch_funding_rate(symbol)
                funding_rate = funding_info.get('fundingRate', 0) or 0
                if funding_rate > 0.0015 or funding_rate < -0.0015:
                    continue
                
                # OHLCV ve Hacim / RSI Kontrolleri için 1h veri çek
                ohlcv_1h = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
                df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                
                # Gerçek Hacim Kontrolü: Son 1 saatlik hacim, son 24 saatlik ortalama saatlik hacmin 1.5 katı mı?
                ortalama_hacim = df_1h['volume'].mean()
                son_hacim = df_1h['volume'].iloc[-1]
                if son_hacim < (ortalama_hacim * 1.5):
                    continue # Yeterli hacim patlaması yoksa elenir
                
                # RSI Hesaplama ve Kontrolü
                df_1h['rsi'] = hesapla_rsi(df_1h['close'], period=14)
                current_rsi = df_1h['rsi'].iloc[-1]
                
                # 4h Trend Kontrolü
                ohlcv_4h = exchange.fetch_ohlcv(symbol, timeframe='4h', limit=60)
                df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df_4h['ema50'] = df_4h['close'].ewm(span=50, adjust=False).mean()
                trend_4h_up = df_4h['close'].iloc[-1] > df_4h['ema50'].iloc[-1]
                
                df_1h['ema50'] = df_1h['close'].ewm(span=50, adjust=False).mean()
                df_1h = hesapla_supertrend(df_1h, period=10, multiplier=3)
                
                current_price = df_1h['close'].iloc[-1]
                ema50_1h = df_1h['ema50'].iloc[-1]
                st_bullish_1h = df_1h['supertrend'].iloc[-1]
                
                if pd.isna(ema50_1h) or pd.isna(current_rsi):
                    continue
                
                # Long Sinyal ve RSI Bandı (50 - 70 arası)
                if trend_4h_up and current_price > ema50_1h and st_bullish_1h and (50 <= current_rsi <= 70):
                    en_iyi_fırsat = {'symbol': symbol, 'side': 'buy', 'price': current_price}
                    break
                # Short Sinyal ve RSI Bandı (30 - 50 arası)
                elif not trend_4h_up and current_price < ema50_1h and not st_bullish_1h and (30 <= current_rsi <= 50):
                    en_iyi_fırsat = {'symbol': symbol, 'side': 'sell', 'price': current_price}
                    break
            except Exception:
                continue

        # 3. İşlem Emri ve İlk Güvenlik Stopu Girişi
        if en_iyi_fırsat:
            symbol = en_iyi_fırsat['symbol']
            side = en_iyi_fırsat['side']
            current_price = en_iyi_fırsat['price']
            
            market_data = exchange.load_markets()
            market = market_data.get(symbol, {})
            
            raw_amount = ORDER_SIZE / current_price
            precision = int(market.get('precision', {}).get('amount', 3))
            min_amount = market.get('limits', {}).get('amount', {}).get('min', 0.001)
            min_notional = 5.5
            if (raw_amount * current_price) < min_notional:
                raw_amount = min_notional / current_price
            amount = max(round(raw_amount, precision), min_amount)
            
            toplam_kullanilan = sum([float(p['initialMargin']) for p in acik_pozisyonlar])
            max_aktif_limit = total_balance * 0.5
            
            if toplam_kullanilan + ORDER_SIZE <= max_aktif_limit:
                exchange.create_order(symbol, 'market', side, amount)
                print(f"!!! MOMENTUM İŞLEMİ AÇILDI: {symbol} - Yön: {side.upper()} - Miktar: {amount} !!!", flush=True)
                
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
                    print(f"[{symbol}] Güvenlik stop emri yerleştirildi. Stop Fiyatı: {stop_price:.4f}", flush=True)
                except Exception as borsa_stop_err:
                    print(f"Borsa stop emri hatası: {borsa_stop_err}", flush=True)

        return jsonify({"durum": "Basarili", "mesaj": "Gelişmiş RSI, hacim ve kademeli stop döngüsü tamamlandı."})
        
    except Exception as e:
        print(f"Genel analiz döngüsü hatası: {str(e)}", flush=True)
        return jsonify({"durum": "OK_Koru", "mesaj": "Hata yutuldu, sistem çalışmaya devam ediyor."})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
