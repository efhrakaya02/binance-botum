import os
import ccxt
import pandas as pd
import numpy as np
from flask import Flask, jsonify

app = Flask(__name__)

# Ayarlar
API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
ORDER_SIZE = 10.0  # Kesin olarak 10 USDT taban işlem büyüklüğü

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

@app.route('/otomatik-analiz')
def otomatik_analiz():
    try:
        exchange = get_exchange()
        
        # Bakiyeyi al
        balance_info = exchange.fetch_balance()['USDT']
        total_balance = balance_info['free'] + balance_info['used']
        
        # 1. Açık Pozisyonları Yönet (ROI Kontrolü: %5 Kar, %2 Zarar)
        positions = exchange.fetch_positions()
        acik_pozisyonlar = [p for p in positions if float(p['contracts']) > 0]
        
        for p in acik_pozisyonlar:
            initial_margin = float(p['initialMargin'])
            if initial_margin > 0:
                roi = float(p['unrealizedPnl']) / initial_margin
                
                # %5 Kar veya %2 Zarar durumunda pozisyonu kapat
                if roi >= 0.05 or roi <= -0.02:
                    side = 'sell' if p['side'] == 'long' else 'buy'
                    exchange.create_order(
                        symbol=p['symbol'], 
                        type='market', 
                        side=side, 
                        amount=float(p['contracts']), 
                        params={'reduceOnly': True}
                    )
        
        # 2. Yeni Pozisyon Açma Kontrolü (Bakiyenin %50'si aktif, %50'si boşta kalacak)
        symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ZEC/USDT', 'RE/USDT', 'TUT/USDT', 'RED/USDT', 'LINK/USDT', 'BNB/USDT']
        
        for symbol in symbols:
            # Her adımda güncel pozisyonları ve kullanılan marjini yeniden hesapla
            positions = exchange.fetch_positions()
            acik_pozisyonlar = [p for p in positions if float(p['contracts']) > 0]
            acik_semboller = [p['symbol'] for p in acik_pozisyonlar]
            
            # Eğer bu coinde zaten açık pozisyon varsa atla (Her coin için max 1 pozisyon)
            if symbol in acik_semboller:
                continue
                
            toplam_kullanilan = sum([float(p['initialMargin']) for p in acik_pozisyonlar])
            max_aktif_limit = total_balance * 0.5  # Bakiyenin %50'si kullanılabilir max sınır
            
            # Eğer yeni emir eklemek %50 sınırı aşıyorsa döngüyü bitir
            if toplam_kullanilan + ORDER_SIZE > max_aktif_limit:
                break
            
            try:
                # OHLCV Veri Çek ve İndikatörleri Hesapla
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                
                df['ma20'] = df['close'].rolling(window=20).mean()
                df['rsi'] = hesapla_rsi(df['close'], period=14)
                
                current_price = df['close'].iloc[-1]
                ma20 = df['ma20'].iloc[-1]
                rsi = df['rsi'].iloc[-1]
                
                if pd.isna(ma20) or pd.isna(rsi):
                    continue
                
                # Miktar hesaplama ve Binance lot/min-notional kısıtlarına uygunluk kontrolü
                # 10 ile 15 USDT arasında olması garanti edilir (ORDER_SIZE = 10.0)
                raw_amount = ORDER_SIZE / current_price
                market_data = exchange.load_markets()
                market = market_data.get(symbol, {})
                precision = market.get('precision', {}).get('amount', 3)
                
                # Coine özel minimum işlem miktarı kontrolü
                min_amount = market.get('limits', {}).get('amount', {}).get('min', 0.001)
                amount = max(round(raw_amount, precision), min_amount)
                
                # LONG Sinyali: Fiyat MA20'nin üstünde ve RSI < 65
                if current_price > ma20 and rsi < 65:
                    exchange.create_order(symbol, 'market', 'buy', amount)
                
                # SHORT Sinyali: Fiyat MA20'nin altında ve RSI > 35
                elif current_price < ma20 and rsi > 35:
                    exchange.create_order(symbol, 'market', 'sell', amount)

            except Exception:
                # Hatalı veya borsada bulunmayan coinlerde takılmadan devam et
                continue
        
        return jsonify({"durum": "Basarili", "mesaj": "Analiz ve 10-15 USDT sınırlandırılmış emir döngüsü tamamlandı."})
        
    except Exception as e:
        return jsonify({"durum": "Hata", "hata_mesaji": str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
