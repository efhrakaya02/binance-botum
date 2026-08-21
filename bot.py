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
ORDER_SIZE = 12.0  # Hedef işlem büyüklüğü (10-15 USDT arası)

def get_exchange():
    return ccxt.binance({
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })

def hesapla_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=int(period)).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=int(period)).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def hesapla_supertrend(df, period=10, multiplier=3):
    # ATR Hesaplama
    high = df['high']
    low = df['low']
    close = df['close']
    
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    
    # HL2 (Orta Nokta)
    hl2 = (high + low) / 2
    
    # Temel Bantlar
    final_upperband = hl2 + (multiplier * atr)
    final_lowerband = hl2 - (multiplier * atr)
    
    supertrend = [True] * len(df)
    
    for i in range(period, len(df)):
        curr_close = close.iloc[i]
        prev_close = close.iloc[i-1]
        
        # Üst Bant Mantığı
        if final_upperband.iloc[i] < final_upperband.iloc[i-1] or prev_close > final_upperband.iloc[i-1]:
            pass
        else:
            final_upperband.iloc[i] = final_upperband.iloc[i-1]
            
        # Alt Bant Mantığı
        if final_lowerband.iloc[i] > final_lowerband.iloc[i-1] or prev_close < final_lowerband.iloc[i-1]:
            pass
        else:
            final_lowerband.iloc[i] = final_lowerband.iloc[i-1]
            
        # SuperTrend Yönü (True = Bullish/Long, False = Bearish/Short)
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
    return df

@app.route('/')
def health_check():
    return "Bot Aktif ve Çalışıyor", 200

@app.route('/otomatik-analiz')
def otomatik_analiz():
    print("Analiz döngüsü tetiklendi.")
    try:
        exchange = get_exchange()
        
        # Bakiyeyi al
        balance_info = exchange.fetch_balance()['USDT']
        total_balance = balance_info['free'] + balance_info['used']
        
        # 1. Açık Pozisyonları Yönet (ROI Kontrolü: %6 Kar, %3 Zarar)
        try:
            positions = exchange.fetch_positions()
            acik_pozisyonlar = [p for p in positions if float(p['contracts']) > 0]
            
            for p in acik_pozisyonlar:
                initial_margin = float(p['initialMargin'])
                if initial_margin > 0:
                    roi = float(p['unrealizedPnl']) / initial_margin
                    
                    if roi >= 0.06 or roi <= -0.03:
                        side = 'sell' if p['side'] == 'long' else 'buy'
                        exchange.create_order(
                            symbol=p['symbol'], 
                            type='market', 
                            side=side, 
                            amount=float(p['contracts']), 
                            params={'reduceOnly': True}
                        )
        except Exception as pos_err:
            print(f"Pozisyon yönetimi sırasında hata (devam ediliyor): {pos_err}")

        # 2. Yeni Pozisyon Açma Kontrolü (EMA50 + SuperTrend + RSI Stratejisi)
        symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ZEC/USDT', 'LINK/USDT', 'BNB/USDT', 'ADA/USDT', 'ONG/USDT', 'XAU/USDT', 'SKHYNIX/USDT', 'HYPE/USDT', 'SPCX/USDT', 'SOXL/USDT', 'KORU/USDT', 'MU/USDT']
        
        for symbol in symbols:
            try:
                time.sleep(0.3)
                
                positions = exchange.fetch_positions()
                acik_pozisyonlar = [p for p in positions if float(p['contracts']) > 0]
                acik_semboller = [p['symbol'] for p in acik_pozisyonlar]
                
                if symbol in acik_semboller:
                    continue
                    
                toplam_kullanilan = sum([float(p['initialMargin']) for p in acik_pozisyonlar])
                max_aktif_limit = total_balance * 0.5
                
                if toplam_kullanilan + ORDER_SIZE > max_aktif_limit:
                    break
                
                # Yeterli veri alabilmek için limit 100 yapıldı (EMA50 için gerekli)
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=100)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                
                # İndikatörler
                df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
                df['rsi'] = hesapla_rsi(df['close'], period=14)
                df = hesapla_supertrend(df, period=10, multiplier=3)
                
                current_price = df['close'].iloc[-1]
                ema50 = df['ema50'].iloc[-1]
                rsi = df['rsi'].iloc[-1]
                st_bullish = df['supertrend'].iloc[-1]  # True ise Long, False ise Short
                
                if pd.isna(ema50) or pd.isna(rsi):
                    continue
                
                # Binance Min Notional (En az 5.5 USDT) Koruması
                market_data = exchange.load_markets()
                market = market_data.get(symbol, {})
                
                raw_amount = ORDER_SIZE / current_price
                precision = int(market.get('precision', {}).get('amount', 3))
                min_amount = market.get('limits', {}).get('amount', {}).get('min', 0.001)
                
                min_notional = 5.5
                if (raw_amount * current_price) < min_notional:
                    raw_amount = min_notional / current_price
                
                amount = max(round(raw_amount, precision), min_amount)
                
                # LONG SİNYALİ: Fiyat EMA50 üstünde + SuperTrend Bullish (True) + RSI < 30 (Örtüşme)
                if current_price > ema50 and st_bullish and rsi < 30:
                    exchange.create_order(symbol, 'market', 'buy', amount)
                
                # SHORT SİNYALİ: Fiyat EMA50 altında + SuperTrend Bearish (False) + RSI > 70 (Örtüşme)
                elif current_price < ema50 and not st_bullish and rsi > 70:
                    exchange.create_order(symbol, 'market', 'sell', amount)

            except Exception as sym_err:
                print(f"{symbol} taranırken hata oluştu (atlandı): {sym_err}")
                continue
        
        return jsonify({"durum": "Basarili", "mesaj": "EMA50 + SuperTrend + RSI analiz döngüsü tamamlandı."})
        
    except Exception as e:
        print(f"Genel analiz döngüsü hatası yakalandı: {str(e)}")
        return jsonify({"durum": "OK_Koru", "mesaj": "Anlık bir hata yutuldu, sistem çalışmaya devam ediyor."})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
