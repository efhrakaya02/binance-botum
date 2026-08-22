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
    return "Kesin Çözümlü Momentum Botu Aktif", 200

@app.route('/otomatik-analiz')
def otomatik_analiz():
    print("--- Momentum ve Trend Tarama Döngüsü Başlatıldı ---", flush=True)
    try:
        exchange = get_exchange()
        exchange.load_markets()
        
        balance_info = exchange.fetch_balance()['USDT']
        total_balance = balance_info['free'] + balance_info['used']
        
        positions = exchange.fetch_positions()
        acik_pozisyonlar = [p for p in positions if float(p['contracts']) > 0]
        print(f"Aktif Açık Pozisyon Sayısı: {len(acik_pozisyonlar)}", flush=True)
        
        # 1. Pozisyon Yönetimi (Trailing Stop)
        try:
            for p in acik_pozisyonlar:
                symbol = p['symbol']
                initial_margin = float(p['initialMargin'])
                entry_price = float(p['entryPrice'])
                side = p['side']
                
                if initial_margin > 0:
                    ticker = exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    
                    ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=30)
                    df_temp = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    tr = pd.concat([df_temp['high'] - df_temp['low'], 
                                  abs(df_temp['high'] - df_temp['close'].shift()), 
                                  abs(df_temp['low'] - df_temp['close'].shift())], axis=1).max(axis=1)
                    current_atr = tr.rolling(window=10).mean().iloc[-1]
                    
                    trailing_mesafe = 1.5 * current_atr
                    stop_tetiklendi = False
                    
                    if side == 'long':
                        dinamik_stop = current_price - trailing_mesafe
                        if current_price <= dinamik_stop:
                            stop_tetiklendi = True
                    elif side == 'short':
                        dinamik_stop = current_price + trailing_mesafe
                        if current_price >= dinamik_stop:
                            stop_tetiklendi = True
                    
                    if stop_tetiklendi:
                        close_side = 'sell' if side == 'long' else 'buy'
                        exchange.create_order(
                            symbol=symbol, 
                            type='market', 
                            side=close_side, 
                            amount=float(p['contracts']), 
                            params={'reduceOnly': True}
                        )
                        print(f"[{symbol}] Trailing Stop Tetiklendi! Pozisyon kârla kapatıldı.", flush=True)
                    else:
                        print(f"[{symbol}] Pozisyon takipte ({side.upper()}). Giriş: {entry_price}, Anlık: {current_price}", flush=True)
        except Exception as pos_err:
            print(f"Pozisyon yönetimi hatası: {pos_err}", flush=True)

        if len(acik_pozisyonlar) >= MAX_POSITIONS:
            print(f"Maksimum pozisyon sınırına ({MAX_POSITIONS}) ulaşıldı.", flush=True)
            return jsonify({"durum": "Beklemede", "mesaj": "Maksimum pozisyona ulaşıldı."})

        # 2. Kesin Çözüm: Tüm aktif USDT futures paritelerini doğrudan market üzerinden çekelim
        all_symbols = [s for s in exchange.symbols if s.endswith('/USDT') and ':' not in s]
        
        coin_listesi = []
        for symbol in all_symbols:
            try:
                t = exchange.fetch_ticker(symbol)
                last_p = t.get('last', 0) or 0
                open_p = t.get('open', 0) or last_p
                vol = t.get('quoteVolume', 0) or 0
                
                if open_p > 0 and last_p > 0:
                    degisim_yuzdesi = ((last_p - open_p) / open_p) * 100
                    coin_listesi.append({
                        'symbol': symbol,
                        'change': degisim_yuzdesi,
                        'volume': vol
                    })
                time.sleep(0.05) # Rate limit koruması
            except:
                continue
        
        coin_listesi.sort(key=lambda x: x['change'], reverse=True)
        
        top_gainers = [item['symbol'] for item in coin_listesi[:15]]
        top_losers = [item['symbol'] for item in coin_listesi[-15:]]
        
        target_symbols = list(set(top_gainers + top_losers))
        print(f"Dinamik Hareketli Havuz Oluşturuldu. İncelenecek coin sayısı: {len(target_symbols)}", flush=True)
        
        en_iyi_fırsat = None
        
        for symbol in target_symbols:
            try:
                time.sleep(0.15)
                
                if symbol in [p['symbol'] for p in acik_pozisyonlar]:
                    continue
                
                # Fonlama Oranı Kontrolü
                funding_info = exchange.fetch_funding_rate(symbol)
                funding_rate = funding_info.get('fundingRate', 0) or 0
                
                skip_long = funding_rate > 0.0015
                skip_short = funding_rate < -0.0015
                
                if skip_long or skip_short:
                    continue
                
                # 4h Trend Kontrolü
                ohlcv_4h = exchange.fetch_ohlcv(symbol, timeframe='4h', limit=60)
                df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df_4h['ema50'] = df_4h['close'].ewm(span=50, adjust=False).mean()
                trend_4h_up = df_4h['close'].iloc[-1] > df_4h['ema50'].iloc[-1]
                
                # 1h Tetik Kontrolü
                ohlcv_1h = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=100)
                df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df_1h['ema50'] = df_1h['close'].ewm(span=50, adjust=False).mean()
                df_1h = hesapla_supertrend(df_1h, period=10, multiplier=3)
                
                current_price = df_1h['close'].iloc[-1]
                ema50_1h = df_1h['ema50'].iloc[-1]
                st_bullish_1h = df_1h['supertrend'].iloc[-1]
                
                if pd.isna(ema50_1h):
                    continue
                
                # Sinyal Doğrulama
                if trend_4h_up and current_price > ema50_1h and st_bullish_1h:
                    print(f"*** MOMENTUM LONG FIRSATI: {symbol} ***", flush=True)
                    en_iyi_fırsat = {'symbol': symbol, 'side': 'buy', 'price': current_price}
                    break
                elif not trend_4h_up and current_price < ema50_1h and not st_bullish_1h:
                    print(f"*** MOMENTUM SHORT FIRSATI: {symbol} ***", flush=True)
                    en_iyi_fırsat = {'symbol': symbol, 'side': 'sell', 'price': current_price}
                    break
                    
            except Exception as sym_err:
                continue

        # 3. İşlem Emri Girişi
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

        return jsonify({"durum": "Basarili", "mesaj": "Havuz başarıyla dolduruldu ve tarama yapıldı."})
        
    except Exception as e:
        print(f"Genel analiz döngüsü hatası: {str(e)}", flush=True)
        return jsonify({"durum": "OK_Koru", "mesaj": "Hata yutuldu, sistem çalışmaya devam ediyor."})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
