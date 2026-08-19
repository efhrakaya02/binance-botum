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

        positions = exchange.fetch_positions()
        # 1. Önce Açık Pozisyonları Denetle (ROI Yönetimi)
        for p in positions:
            if float(p['contracts']) > 0:
                roi = float(p['percentage']) * leverage # Kaldıraçlı ROI
                symbol = p['symbol']
                
                # Hedef: ROI %5 kâr veya -%2 zarar
                if roi >= 5.0 or roi <= -2.0:
                    print(f"POZİSYON KAPATILIYOR: {symbol} | ROI: %{roi:.2f}")
                    side = 'sell' if p['side'] == 'long' else 'buy'
                    exchange.create_order(symbol, 'market', side, float(p['contracts']), None, {'reduceOnly': True})

        # 2. Yeni İşlem Fırsatları
        balance = exchange.fetch_balance()
        usdt_free = balance.get('USDT', {}).get('free', 0)
        acik_semboller = [p['symbol'] for p in positions if float(p['contracts']) > 0]

        for symbol in symbols:
            if symbol in acik_semboller: continue
            if usdt_free < butce_usdt: break

            exchange.set_leverage(leverage, symbol)
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            current_price = df['close'].iloc[-1]
            ma20 = df['close'].rolling(window=20).mean().iloc[-1]
            rsi = hesapla_rsi(df['close'].values, period=14)

            signal = 'buy' if current_price > ma20 and rsi < 65 else ('sell' if current_price < ma20 and rsi > 35 else None)

            if signal:
                amount = round((butce_usdt * leverage) / current_price, 4)
                exchange.create_order(symbol, 'market', signal, amount)
                print(f"YENİ İŞLEM: {symbol} {signal.upper()}")

        return jsonify({"durum": "basarili"}), 200
    except Exception as e:
        return jsonify({"durum": "hata", "mesaj": str(e)}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
