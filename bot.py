from flask import Flask, jsonify
import ccxt
import os
import pandas as pd

app = Flask(__name__)

# Binance Vadeli İşlemler bağlantısı
exchange = ccxt.binance({
    'apiKey': os.getenv('BINANCE_API_KEY'),
    'secret': os.getenv('BINANCE_SECRET_KEY'),
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

# ==========================================
# KULLANICI AYARLARI (BURAYI İSTEDİĞİNİZ GİBİ DEĞİŞTİREBİLİRSİNİZ)
# ==========================================
ISLEM_YAPILACAK_COINLER = ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']
MAX_KALDIRAC = 10                  # Maksimum kaldıraç oranı (10x)
ISLEM_BUTCESI_USDT = 15            # Her işlemde kullanılacak teminat (Dolar)
HEDEF_KAR_YUZDESI = 0.6            # Fiyatta hedeflenen yüzde (Örn: %0.6 fiyat değişimi, 10x ile %6 kar demektir)
TIMEFRAME = '15m'                  # Hızlı işlemler için 15 dakikalık grafikler


@app.route('/')
def ana_sayfa():
    return "Scalping Vadeli İşlem Botu Aktif ve Çalışıyor! 🚀"


@app.route('/otomatik-analiz', methods=['GET', 'POST'])
def scalping_motoru():
    islem_raporu = []
    try:
        exchange.load_markets()
        
        # 1. AŞAMA: Açık pozisyonları kontrol et ve kar hedefine ulaşıldıysa kapat
        aktif_pozisyonlar = exchange.fetch_positions()
        for pos in aktif_pozisyonlar:
            symbol = pos['symbol']
            if symbol in ISLEM_YAPILACAK_COINLER and float(pos['contracts']) > 0:
                giris_fiyati = float(pos['entryPrice'])
                anlik_fiyat = exchange.fetch_ticker(symbol)['last']
                yon = pos['side'] # 'long' veya 'short'
                
                # Kar yüzdesini hesapla
                if yon == 'long':
                    fiyat_degisim_yuzdesi = ((anlik_fiyat - giris_fiyati) / giris_fiyati) * 100
                else: # short
                    fiyat_degisim_yuzdesi = ((giris_fiyati - anlik_fiyat) / giris_fiyati) * 100
                
                # Kaldıraç etkisi dahil net kar yüzdesi hedefe ulaştıysa pozisyonu kapat
                net_kar_orani = fiyat_degisim_yuzdesi * MAX_KALDIRAC
                
                if fiyat_degisim_yuzdesi >= HEDEF_KAR_YUZDESI:
                    if yon == 'long':
                        exchange.create_market_sell_order(symbol, float(pos['contracts']), params={'reduceOnly': True})
                    else:
                        exchange.create_market_buy_order(symbol, float(pos['contracts']), params={'reduceOnly': True})
                    
                    islem_raporu.append(f"Kâr Alındı! {symbol} pozisyonu kapatıldı. Tahmini Getiri: %{net_kar_orani:.2f}")

        # 2. AŞAMA: Açık pozisyon olmayan coinler için yeni analiz yap
        for coin in ISLEM_YAPILACAK_COINLER:
            # Bu coin için zaten açık pozisyon var mı kontrol et
            acik_mi = any(p['symbol'] == coin and float(p['contracts']) > 0 for p in exchange.fetch_positions())
            if acik_mi:
                continue # Zaten açık pozisyon varsa bu coini atla

            # Kaldıraç ayarla
            try:
                exchange.set_leverage(MAX_KALDIRAC, coin)
            except:
                pass

            # Kısa vadeli hızlı analiz için mum verilerini çek
            ohlcv = exchange.fetch_ohlcv(coin, timeframe=TIMEFRAME, limit=30)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            # Hızlı RSI Hesaplama (14 periyot)
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            guncel_rsi = df['rsi'].iloc[-1]
            guncel_fiyat = df['close'].iloc[-1]

            # Scalping Stratejisi:
            # RSI 38 altındaysa aşırı satım (Tepki Long fırsatı)
            if guncel_rsi < 38:
                adet = (ISLEM_BUTCESI_USDT * MAX_KALDIRAC) / guncel_fiyat
                exchange.create_market_buy_order(coin, adet)
                islem_raporu.append(f"LONG Açıldı: {coin} (RSI: {guncel_rsi:.1f})")
            
            # RSI 62 üzerindeyse aşırı alım (Tepki Short fırsatı)
            elif guncel_rsi > 62:
                adet = (ISLEM_BUTCESI_USDT * MAX_KALDIRAC) / guncel_fiyat
                exchange.create_market_sell_order(coin, adet)
                islem_raporu.append(f"SHORT Açıldı: {coin} (RSI: {guncel_rsi:.1f})")

        return jsonify({'durum': 'basarili', 'islem_raporu': islem_raporu}), 200

    except Exception as hata:
        return jsonify({'durum': 'hata', 'mesaj': str(hata)}), 500


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
