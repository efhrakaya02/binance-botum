import os

class Config:
    # --- API VE ORTAM AYARLARI ---
    BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
    BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
    PAPER_TRADING = True        # Cüzdanı riske atmadan test modu devrede

    # --- PORTFÖY VE RİSK LİMİTLERİ ---
    MAX_OPEN_POSITIONS = 3      # Aynı anda en fazla 3 işlem açılabilir
    MARGIN_PER_TRADE_USDT = 10  # İşlem başına riske edilecek bakiye (10$)
    LEVERAGE = 5                # Tüm işlemlerde sabit 5x Kaldıraç

    # --- KAR/ZARAR SINIRLARI (Risk_Manager ve Main Tarafından Okunur) ---
    HARD_TP_PCT = 3.0           # %3 Kâr Barajı (Aşıldığında "Peak TP / Zirveden Dönüş" aranır)
    HARD_SL_PCT = 1.5           # %1.5 Kırmızı Çizgi (Dinamik stop bile olsa zarar bunu aşamaz)

    # --- ZAMANLAYICILAR (Asenkron Döngü Hızları) ---
    SCAN_INTERVAL = 300         # Tarayıcı her 5 dakikada bir (300 sn) piyasayı tarar
    MONITOR_INTERVAL = 1        # Orkestra şefi saniyede 1 kez fiyatı kontrol edip risk kilitlerini çalıştırır
