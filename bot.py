import os
import ccxt
import pandas as pd
import numpy as np
from flask import Flask, jsonify

app = Flask(__name__)

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
        if delta > 0: upval = delta; downval = 0.
        else: upval = 0.; downval = -delta
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = up / down if down != 0 else 0
        rsi[i] = 100. - 100. / (1. + rs)
    return rsi[-1]

@app.route('/otomatik-analiz')
def otomatik_analiz():
    try:
        exchange = get_exchange()
        # İsterseniz burada ZEC/USDT'yi çıkarabilirsiniz
        symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT'] 
        butce_usdt = 15.0
        leverage = 5

        positions = exchange.fetch_positions()
        acik_semboller = [p['symbol'] for p in positions if float(p['contracts']) > 0]
        
        results = []

        for symbol in symbols:
            if symbol in acik_semboller: continue

            exchange.set_leverage(leverage, symbol)
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            current_price = df['close'].iloc[-1]
            ma20 = df['close'].rolling(window=20).mean().iloc[-1]
            rsi = hesapla_rsi(df['close'].values, period=14)

            signal = 'buy' if current_price > ma20 and rsi < 65 else ('sell' if current_price < ma20 and rsi > 35 else None)

            if signal:
                amount = round((butce_usdt * leverage) / current_price, 4)
                
                # 1. Ana Giriş (Market)
                exchange.create_order(symbol, 'market', signal, amount)
                
                # 2. TP ve SL Fiyatlarını Hesapla
                # %5 Kar, %2 Zarar
                if signal == 'buy':
                    tp_price = round(current_price * 1.05, 4)
                    sl_price = round(current_price * 0.98, 4)
                    close_side = 'sell'
                else:
                    tp_price = round(current_price * 0.95, 4)
                    sl_price = round(current_price * 1.02, 4)
                    close_side = 'buy'

                # 3. TP (Limit Emri)
                exchange.create_order(symbol, 'limit', close_side, amount, tp_price, {'reduceOnly': True})
                
                # 4. SL (Stop-Market Emri)
                exchange.create_order(symbol, 'STOP_MARKET', close_side, amount, None, {'stopPrice': sl_price, 'reduceOnly': True})
                
                results.append(f"{symbol} işlem açıldı: TP@{tp_price}, SL@{sl_price}")

        return jsonify({"durum": "basarili", "detaylar": results})
    except Exception as e:
        return jsonify({"hata": str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
