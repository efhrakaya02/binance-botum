class RiskManager:
    def __init__(self, config):
        self.config = config

    def calculate_stop_loss(self, entry_price, max_reached_price, is_long=True, initial_sl=None):
        if is_long:
            profit_pct = ((max_reached_price - entry_price) / entry_price) * 100
            
            if profit_pct < 1.5:
                # Kâr %1.5'e ulaşana kadar, Dinamik Stop (Swing Low) veya sabit -%1 koruması devrededir
                return initial_sl if initial_sl else entry_price * 0.99
            else:
                # 🚀 YENİ: Kâr %1.5'i geçtiği andan itibaren, görülen ZİRVENİN %75'ini kilitler!
                # (Örn: Zirve %2.0 -> Kilit %1.5'te | Zirve %2.75 -> Kilit %2.06'da)
                locked_profit_pct = profit_pct * 0.75
                return entry_price * (1 + (locked_profit_pct / 100))
        else:
            profit_pct = ((entry_price - max_reached_price) / entry_price) * 100
            
            if profit_pct < 1.5:
                return initial_sl if initial_sl else entry_price * 1.01
            else:
                locked_profit_pct = profit_pct * 0.75
                return entry_price * (1 - (locked_profit_pct / 100))
