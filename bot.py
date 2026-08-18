from flask import Flask, jsonify, request
import ccxt
import os

app = Flask(__name__)

exchange = ccxt.binance(
    {
        'apiKey': os.getenv('BINANCE_API_KEY'),
        'secret': os.getenv('BINANCE_SECRET_KEY'),
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'},
    }
)


@app.route('/webhook', methods=['POST'])
def sinyali_isle():
  try:
    veri = request.json
    print(f'Gelen Sinyal Bilgisi: {veri}')

    coin_adi = veri.get('symbol')
    islem_turu = veri.get('side')
    butce_usdt = float(veri.get('amount_usdt', 10))

    if not coin_adi or not islem_turu:
      return jsonify({'durum': 'hata', 'mesaj': 'Eksik bilgi gönderildi'}), 400

    piyasa_bilgisi = exchange.fetch_ticker(coin_adi)
    guncel_fiyat = piyasa_bilgisi['last']
    alinacak_miktar = butce_usdt / guncel_fiyat

    if islem_turu == 'buy':
      emir = exchange.create_market_buy_order(coin_adi, alinacak_miktar)
      print(
          f'BAŞARILI: {coin_adi} coini için {butce_usdt} dolarlık alım yapıldı.'
      )
    elif islem_turu == 'sell':
      cüzdan = exchange.fetch_balance()
      ana_para_birimi = coin_adi.split('/')[0]
      mevcut_coin_miktari = cüzdan['free'].get(ana_para_birimi, 0)

      if mevcut_coin_miktari > 0:
        emir = exchange.create_market_sell_order(coin_adi, mevcut_coin_miktari)
        print(f'BAŞARILI: Elindeki tüm {coin_adi} satıldı.')
      else:
        return jsonify(
            {'durum': 'hata', 'mesaj': 'Satılacak yeterli coin yok'}
        ), 400

    return jsonify({'durum': 'basarili', 'islem_id': emir['id']}), 200

  except Exception as hata:
    print(f'Bir hata oluştu: {str(hata)}')
    return jsonify({'durum': 'hata', 'mesaj': str(hata)}), 500


if __name__ == '__main__':
port = int(os.environ.get("PORT", 5000))
app.run(host='0.0.0.0', port=port)
