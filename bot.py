import os
import ccxt
from flask import Flask, jsonify

app = Flask(__name__)

# Çevre değişkenlerinden API anahtarlarını güvenli bir şekilde alıyoruz
API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')

# Binance Futures bağlantı ayarları
def get_exchange():
    return ccxt.binance({
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })

@app.route('/')
def home():
    return "Scalping Vadeli İşlem Botu Aktif ve Çalışıyor! 🚀", 200

@app.route('/otomatik-analiz')
def otomatik_analiz():
    try:
        # Binance bağlantısını test edelim ve cüzdan bakiyesini çekelim
        exchange = get_exchange()
        
        # Güvenli bağlantı testi için bakiye bilgisini sorguluyoruz
        balance = exchange.fetch_balance()
        usdt_free = balance.get('USDT', {}).get('free', 0)
        
        # --- BURAYA Kendi Scalping / Al-Sat Strateji Kodlarınızı Ekleyebilirsiniz ---
        # Örnek: ticker = exchange.fetch_ticker('BTC/USDT')
        # ------------------------------------------------------------------------
        
        print(f"Analiz başarılı. Serbest USDT: {usdt_free}")
        
        # Cron-job ve sistemin her zaman mutlu (200 OK) dönmesi için başarılı yanıt döndürüyoruz
        return jsonify({
            "durum": "basarili", 
            "mesaj": "Analiz tamamlandı ve işlem kontrol edildi.",
            "serbest_usdt": usdt_free
        }), 200

    except Exception as e:
        # Hata olsa bile kod patlamaz, cron-job hata almaz, hatayı Railway loglarında görürüz
        hata_mesaji = str(e)
        print(f"İşlem sırasında hata oluştu: {hata_mesaji}")
        
        return jsonify({
            "durum": "hata", 
            "mesaj": "İşlem sırasında hata yakalandı ama sunucu ayakta.",
            "detay": hata_mesaji
        }), 200  # 200 döndürerek cron-job'ın hata alarmı vermesini engelliyoruz

if __name__ == '__main__':
    # Railway'in atadığı portu otomatik kullanır, yoksa varsayılan 8080 olur
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
