import os

# API ve Güvenlik (Railway Variables üzerinden çekilecek)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# İşlem Parametreleri
MAX_OPEN_POSITIONS = 3
MARGIN_PER_TRADE_USDT = 10
LEVERAGE = 5

# Risk & Kar Yönetimi Parametreleri
BREAKEVEN_PCT = 1.0  # %1 ham kârda stop maliyete çekilir
TRAILING_ACTIVATION_PCT = 1.5  # %1.5 ham kârda iz süren stop başlar
