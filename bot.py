import os
import ccxt
import pandas as pd
import numpy as np
from flask import Flask, jsonify

app = Flask(__name__)

# Railway Variables'dan anahtarları çek
API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')

def get_exchange():
    return ccxt.binance({
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })

def hesapla_rsi(closes, period=14):
    deltas = np.diff(closes)
    seed = deltas[:period+1]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi = np.zeros_like(closes)
    rsi[:period] = 100. - 100. / (1. + rs)
    for i in range(period, len(closes)):
        delta = deltas[i - 1]
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = up / down if down != 0 else 0
        rsi[i] = 100. - 100. / (1. + rs)
    return rsi[-1]

@app.route('/otomatik-analiz')
def otomatik_analiz():
    try:
        exchange = get_exchange()
        symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ZEC/USDT']
        butce_usdt = 15.0  
        leverage = 5

        # Cüzdan ve Açık Pozisyon Kontrolü
        balance = exchange.fetch_balance()
        usdt_free = balance.get('USDT', {}).get('free', 0)
        positions = exchange.fetch_positions()
        acik_semboller = [p['symbol'] for p in positions if float(p['contracts']) > 0]

        sonuclar = []

        for symbol in symbols:
            try:
                # 1. Aynı coinde açık pozisyon varsa işlem açma, geç
                if symbol in acik_semboller:
                    sonuclar.append(f"{symbol}: Açık pozisyon var, pas geçildi.")
                    continue
                
                # 2. Bakiye kontrolü
                if usdt_free < butce_usdt:
                    sonuclar.append("Yetersiz bakiye, işlem durduruldu.")
                    break

                # 3. Analiz için veri çek
                exchange.set_leverage(leverage, symbol)
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                closes = df['close'].values
                current_price = closes[-1]
                ma20 = df['close'].rolling(window=20).mean().iloc[-1]
                rsi = hesapla_rsi(closes, period=14)

                # 4. Strateji Kararı
                signal = None
                if current_price > ma20 and rsi < 65: signal = 'buy'
                elif current_price < ma20 and rsi > 35: signal = 'sell'

                if signal:
                    # Miktar hesapla ve minimum lot sınırlarını koru
                    amount = round((butce_usdt * leverage) / current_price, 4)
                    if symbol == 'BTC/USDT' and amount < 0.001: amount = 0.001
                    elif symbol == 'ETH/USDT' and amount < 0.001: amount = 0.001
                    elif symbol == 'SOL/USDT' and amount < 0.01: amount = 0.01
                    elif symbol == 'XRP/USDT' and amount < 1.0: amount = 1.0
                    elif symbol == 'ZEC/USDT' and amount < 0.01: amount = 0.01

                    # TP (%4) ve SL (%2) Fiyat Hesaplama
                    if signal == 'buy':
                        tp_price = round(current_price * 1.04, 4)
                        sl_price = round(current_price * 0.98, 4)
                        tp_sl_side = 'sell'
                    else:
                        tp_price = round(current_price * 0.96, 4)
                        sl_price = round(current_price * 1.02, 4)
                        tp_sl_side = 'buy'

                    # 1. Ana Giriş Emri (Market)
                    exchange.create_order(symbol, 'market', signal, amount)
                    
                    # 2. Kar Al (TP) - LİMİT Emri (Binance'de en kararlı çalışan yöntem)
                    exchange.create_order(symbol, 'limit', tp_sl_side, amount, tp_price, {'reduceOnly': True})
                    
                    # 3. Zarar Durdur (SL) - STOP_MARKET Emri
                    sl_params = {'stopPrice': sl_price, 'reduceOnly': True}
                    exchange.create_order(symbol, 'STOP_MARKET', tp_sl_side, amount, None, sl_params)

                    sonuclar.append(f"{symbol}: {signal.upper()} açıldı (TP: {tp_price}, SL: {sl_price}).")
                else:
                    sonuclar.append(f"{symbol}: Nötr.")

            except Exception as ex:
                sonuclar.append(f"{symbol} hata: {str(ex)}")

        return jsonify({"durum": "basarili", "detaylar": sonuclar}), 200

    except Exception as e:
        return jsonify({"durum": "hata", "mesaj": str(e)}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
