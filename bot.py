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
    return "Gelişmiş Kademeli Kâr Al ve Trend Kontrollü Bot Aktif", 200

@app.route('/otomatik-analiz')
def otomatik_analiz():
    print("--- Akıllı Pozisyon Yönetimi ve Tarama Döngüsü Başlatıldı ---", flush=True)
    try:
        exchange = get_exchange()
        exchange.load_markets()
        
        balance_info = exchange.fetch_balance()['USDT']
        total_balance = balance_info['free'] + balance_info['used']
        
        positions = exchange.fetch_positions()
        acik_pozisyonlar = [p for p in positions if float(p['contracts']) > 0]
        print(f"Aktif Açık Pozisyon Sayısı: {len(acik_pozisyonlar)}", flush=True)
        
        # 1. Gelişmiş Pozisyon Yönetimi (Trend Bozulması + Kademeli Kâr Al + Borsa Stopu)
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
                    
                    # Anlık ROI / Kâr yüzdesi hesapla
                    if side == 'long':
                        kar_yuzdesi = ((current_price - entry_price) / entry_price) * 100
                    else:
                        kar_yuzdesi = ((entry_price - current_price) / entry_price) * 100
                        
                    # 1h Trend ve SuperTrend Durumunu Kontrol Et
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
                    
                    # Durum A: Trend tersine döndüyse kârın/zararın tamamıyla çık
                    if trend_tersine_dondu:
                        exchange.create_order(
                            symbol=symbol, type='market', side=close_side, amount=contracts, params={'reduceOnly': True}
                        )
                        print(f"[{symbol}] Trend tersine döndü! Pozisyon tamamen kapatıldı. Kâr/Zarar Yüzdesi: %{kar_yuzdesi:.2f}", flush=True)
                        continue
                        
                    # Durum B: Kademeli Kâr Al (%10 ve sonraki her %5 artışta yarısını kapat)
                    # Not: Bu mantığı takip edebilmek için basitçe kâr eşiklerini kontrol ediyoruz.
                    # Örneğin kar %10'u geçtiyse ve henüz bu kademe alınmadıysa yarısını kapat.
                    # (Pratikte miktar kontrolüyle entegre edilebilir, şimdilik temel eşik tetikleyicisi:)
                    if kar_yuzdesi >= 10.0 and contracts > 0.001:
                        yari_miktar = round(contracts / 2, 3)
                        if yari_miktar > 0:
                            try:
                                exchange.create_order(
                                    symbol=symbol, type='market', side=close_side, amount=yari_miktar, params={'reduceOnly': True}
                                )
                                print(f"[{symbol}] Kâr hedefi yakalandı (%{kar_yuzdesi:.2f}). Miktar yarısı (%50) cebe atıldı: {yari_miktar}", flush=True)
                            except Exception as partial_err:
                                print(f"Kademeli kâr al hatası: {partial_err}", flush=True)
                                
                    print(f"[{symbol}] Pozisyon takipte ({side.upper()}). Giriş: {entry_price}, Anlık: {current_price}, Kâr: %{kar_yuzdesi:.2f}", flush=True)
                    
        except Exception as pos_err:
            print(f"Pozisyon yönetimi hatası: {pos_err}", flush=True)

        if len(acik_pozisyonlar) >= MAX_POSITIONS:
            print(f"Maksimum pozisyon sınırına ({MAX_POSITIONS}) ulaşıldı.", flush=True)
            return jsonify({"durum": "Beklemede", "mesaj": "Maksimum pozisyona ulaşıldı."})

        # 2. Dinamik Havuz Oluşturma
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
                
                funding_info = exchange.fetch_funding_rate(symbol)
                funding_rate = funding_info.get('fundingRate', 0) or 0
                if funding_rate > 0.0015 or funding_rate < -0.0015:
                    continue
                
                ohlcv_4h = exchange.fetch_ohlcv(symbol, timeframe='4h', limit=60)
                df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df_4h['ema50'] = df_4h['close'].ewm(span=50, adjust=False).mean()
                trend_4h_up = df_4h['close'].iloc[-1] > df_4h['ema50'].iloc[-1]
                
                ohlcv_1h = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=100)
                df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df_1h['ema50'] = df_1h['close'].ewm(span=50, adjust=False).mean()
                df_1h = hesapla_supertrend(df_1h, period=10, multiplier=3)
                
                current_price = df_1h['close'].iloc[-1]
                ema50_1h = df_1h['ema50'].iloc[-1]
                st_bullish_1h = df_1h['supertrend'].iloc[-1]
                
                if pd.isna(ema50_1h):
                    continue
                
                if trend_4h_up and current_price > ema50_1h and st_bullish_1h:
                    en_iyi_fırsat = {'symbol': symbol, 'side': 'buy', 'price': current_price}
                    break
                elif not trend_4h_up and current_price < ema50_1h and not st_bullish_1h:
                    en_iyi_fırsat = {'symbol': symbol, 'side': 'sell', 'price': current_price}
                    break
            except Exception:
                continue

        # 3. İşlem Emri ve Borsa Tabanlı Stop-Loss Girişi
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
                # Ana Pozisyon Emri (Market)
                order = exchange.create_order(symbol, 'market', side, amount)
                print(f"!!! MOMENTUM İŞLEMİ AÇILDI: {symbol} - Yön: {side.upper()} - Miktar: {amount} !!!", flush=True)
                
                # Borsa Tabanlı Güvence Stop-Loss Emri (Sunucu Çökmesine Karşı)
                try:
                    # ATR bazlı %3-4 mesafede borsa stopu koyalım
                    ohlcv_stop = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=20)
                    df_s = pd.DataFrame(ohlcv_stop, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    tr_s = pd.concat([df_s['high'] - df_s['low'], abs(df_s['high'] - df_s['close'].shift()), abs(df_s['low'] - df_s['close'].shift())], axis=1).max(axis=1)
                    atr_val = tr_s.rolling(window=10).mean().iloc[-1]
                    
                    stop_side = 'sell' if side == 'buy' else 'buy'
                    if side == 'buy':
                        stop_price = current_price - (2.0 * atr_val)
                    else:
                        stop_price = current_price + (2.0 * atr_val)
                        
                    # Binance stopMarket emri parametreleri
                    stop_params = {
                        'stopPrice': float(exchange.price_to_precision(symbol, stop_price)),
                        'reduceOnly': True
                    }
                    exchange.create_order(symbol, 'STOP_MARKET', stop_side, amount, params=stop_params)
                    print(f"[{symbol}] Borsa tabanlı güvenlik stop emri yerleştirildi. Stop Fiyatı: {stop_price:.4f}", flush=True)
                except Exception as borsa_stop_err:
                    print(f"Borsa stop emri oluşturulurken hata (ana işlem devam ediyor): {borsa_stop_err}", flush=True)

        return jsonify({"durum": "Basarili", "mesaj": "Akıllı döngü ve borsa stopları başarıyla işlendi."})
        
    except Exception as e:
        print(f"Genel analiz döngüsü hatası: {str(e)}", flush=True)
        return jsonify({"durum": "OK_Koru", "mesaj": "Hata yutuldu, sistem çalışmaya devam ediyor."})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
