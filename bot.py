import os
import ccxt
import pandas as pd
import numpy as np
import time
from flask import Flask, jsonify

app = Flask(__name__)

# Ayarlar
API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
ORDER_SIZE = 12.0  # İşlem başına hedef bütçe (USDT)
MAX_POSITIONS = 2  # En fazla aynı anda 2 pozisyon

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
    return "Akıllı Tarayıcı ATR Bot Aktif", 200

@app.route('/otomatik-analiz')
def otomatik_analiz():
    print("Akıllı analiz ve ATR tabanlı tarama döngüsü tetiklendi.")
    try:
        exchange = get_exchange()
        exchange.load_markets()
        
        balance_info = exchange.fetch_balance()['USDT']
        total_balance = balance_info['free'] + balance_info['used']
        
        # Açık pozisyonları al
        positions = exchange.fetch_positions()
        acik_pozisyonlar = [p for p in positions if float(p['contracts']) > 0]
        
        # 1. Açık Pozisyonları Yönet (%6 Kâr veya ATR Tabanlı Zarar Kes Kontrolü)
        try:
            for p in acik_pozisyonlar:
                symbol = p['symbol']
                initial_margin = float(p['initialMargin'])
                entry_price = float(p['entryPrice'])
                side = p['side'] # 'long' veya 'short'
                
                if initial_margin > 0:
                    current_pnl_pct = float(p['unrealizedPnl']) / initial_margin
                    
                    # Güncel fiyatı çek
                    ticker = exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    
                    # ATR hesaplamak için son 1h verisini çek
                    ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=30)
                    df_temp = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    tr = pd.concat([df_temp['high'] - df_temp['low'], 
                                  abs(df_temp['high'] - df_temp['close'].shift()), 
                                  abs(df_temp['low'] - df_temp['close'].shift())], axis=1).max(axis=1)
                    current_atr = tr.rolling(window=10).mean().iloc[-1]
                    
                    # ATR Tabanlı Dinamik Stop Seviyesi (Örn: 1.5 * ATR mesafe)
                    stop_mesafe = 1.5 * current_atr
                    
                    stop_tetiklendi = False
                    if side == 'long':
                        stop_fiyat = entry_price - stop_mesafe
                        if current_price <= stop_fiyat:
                            stop_tetiklendi = True
                    elif side == 'short':
                        stop_fiyat = entry_price + stop_mesafe
                        if current_price >= stop_fiyat:
                            stop_tetiklendi = True
                    
                    # Kural: %6 Kâr veya ATR Stop noktası gerçekleştiyse kapat
                    if current_pnl_pct >= 0.06 or stop_tetiklendi:
                        close_side = 'sell' if side == 'long' else 'buy'
                        exchange.create_order(
                            symbol=symbol, 
                            type='market', 
                            side=close_side, 
                            amount=float(p['contracts']), 
                            params={'reduceOnly': True}
                        )
                        print(f"Pozisyon Kapatıldı ({symbol}): {'Kâr Hedefi' if current_pnl_pct >= 0.06 else 'ATR Stop Loss'}")
        except Exception as pos_err:
            print(f"Pozisyon yönetimi hatası (devam ediliyor): {pos_err}")

        # Eğer maksimum pozisyon sınırına ulaşıldıysa yeni tarama yapma
        if len(acik_pozisyonlar) >= MAX_POSITIONS:
            return jsonify({"durum": "Beklemede", "mesaj": f"Maksimum pozisyon sınırına ({MAX_POSITIONS}) ulaşıldı."})

        # 2. Tüm Borsayı Tara ve Hacme Göre En İyi 20 Coin'i Seç
        tickers = exchange.fetch_tickers()
        usdt_tickers = {s: t for s, t in tickers.items() if s.endswith('/USDT') and ':' not in s}
        
        sorted_tickers = sorted(usdt_tickers.items(), key=lambda x: x[1].get('quoteVolume', 0) or 0, reverse=True)
        top_symbols = [item[0] for item in sorted_tickers[:20]]
        
        en_iyi_fırsat = None
        
        for symbol in top_symbols:
            try:
                time.sleep(0.2)
                
                if symbol in [p['symbol'] for p in acik_pozisyonlar]:
                    continue
                
                # Fonlama Oranı Kontrolü
                funding_info = exchange.fetch_funding_rate(symbol)
                funding_rate = funding_info.get('fundingRate', 0) or 0
                
                skip_long = funding_rate > 0.0015
                skip_short = funding_rate < -0.0015
                
                # Çoklu Zaman Dilimi Teyidi (4h Ana Trend)
                ohlcv_4h = exchange.fetch_ohlcv(symbol, timeframe='4h', limit=60)
                df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df_4h['ema50'] = df_4h['close'].ewm(span=50, adjust=False).mean()
                trend_4h_up = df_4h['close'].iloc[-1] > df_4h['ema50'].iloc[-1]
                
                # 1h Tetik Zamanı
                ohlcv_1h = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=100)
                df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df_1h['ema50'] = df_1h['close'].ewm(span=50, adjust=False).mean()
                df_1h = hesapla_supertrend(df_1h, period=10, multiplier=3)
                
                current_price = df_1h['close'].iloc[-1]
                ema50_1h = df_1h['ema50'].iloc[-1]
                st_bullish_1h = df_1h['supertrend'].iloc[-1]
                
                if pd.isna(ema50_1h):
                    continue
                
                # Sinyal Kontrolleri
                if trend_4h_up and current_price > ema50_1h and st_bullish_1h and not skip_long:
                    en_iyi_fırsat = {'symbol': symbol, 'side': 'buy', 'price': current_price}
                    break
                elif not trend_4h_up and current_price < ema50_1h and not st_bullish_1h and not skip_short:
                    en_iyi_fırsat = {'symbol': symbol, 'side': 'sell', 'price': current_price}
                    break
                    
            except Exception as sym_err:
                continue

        # 3. Bulunan Fırsatı Değerlendir ve Emri Gir
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
                print(f"AKILLI ATR İŞLEMİ AÇILDI: {symbol} - Yön: {side.upper()} - Miktar: {amount}")

        return jsonify({"durum": "Basarili", "mesaj": "ATR tabanlı akıllı analiz döngüsü tamamlandı."})
        
    except Exception as e:
        print(f"Genel analiz döngüsü hatası: {str(e)}")
        return jsonify({"durum": "OK_Koru", "mesaj": "Hata yutuldu, sistem çalışmaya devam ediyor."})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
