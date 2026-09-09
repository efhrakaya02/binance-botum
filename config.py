import os

class Config:
    # --- API VE ORTAM AYARLARI ---
    BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
    BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
    PAPER_TRADING = True        # Sanal Cüzdan Modu

    # --- PORTFÖY LİMİTLERİ ---
    MAX_OPEN_POSITIONS = 3      # Aynı anda en fazla 3 av
    MARGIN_PER_TRADE_USDT = 10  # İşlem başı bakiye
    LEVERAGE = 5                # Sabit Kaldıraç

    # --- ZAMANLAYICILAR (Saniye) ---
    SCAN_INTERVAL = 300         # 5 Dakikada bir yeni tarama
    MONITOR_INTERVAL = 1        # Fiyatı saniyede 1 kez izle
