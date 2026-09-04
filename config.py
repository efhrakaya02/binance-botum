import os

# API ve Güvenlik (Railway Variables üzerinden çekilecek)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET = os.getenv("BINANCE_SECRET", "")

# İşlem Parametreleri
MAX_OPEN_POSITIONS = 3
MARGIN_PER_TRADE_USDT = 10
LEVERAGE = 5

# 🛠️ TEST MODU: True olduğunda gerçek bakiye harcanmaz, işlemler sanal yürütülür.
PAPER_TRADING = True

# Risk & Kar Yönetimi Parametreleri
BREAKEVEN_PCT = 1.0  
TRAILING_ACTIVATION_PCT = 1.5
