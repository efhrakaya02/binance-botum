import os

class Config:
    BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
    BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

    MAX_OPEN_POSITIONS = 3
    MARGIN_PER_TRADE_USDT = 10
    LEVERAGE = 5
    PAPER_TRADING = True

    # Kâr/Zarar Sınırları
    HARD_TP_PCT = 3.0      # %3 Hedef
    HARD_SL_PCT = 1.5      # %1.5 Kırmızı Çizgi

    # Zamanlayıcılar (Saniye)
    SCAN_INTERVAL = 300    # 5 Dakikada bir tarama
    MONITOR_INTERVAL = 1   # Saniyede bir fiyat ve stop kontrolü
