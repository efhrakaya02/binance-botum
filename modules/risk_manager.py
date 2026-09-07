class RiskManager:
    def __init__(self, config):
        self.config = config

    def calculate_stop_loss(self, entry_price, max_reached_price, is_long=True, initial_sl=None):
        if is_long:
            profit_pct = ((max_reached_price - entry_price) / entry_price) * 100
            
            # 🚀 GÜNCELLEME: Sabit -%1 yerine, PA temelli Swing Low (Dinamik) Stop noktası kullanılır
            if profit_pct < 1.5:
                return initial_sl if initial_sl else entry_price * 0.99
            elif 1.5 <= profit_pct < 3.0:
                return entry_price * 1.01 
            elif 3.0 <= profit_pct < 3.5:
                locked_profit_pct = profit_pct * 0.70  
                return entry_price * (1 + (locked_profit_pct / 100))
            else:
                locked_profit_pct = profit_pct * 0.75 
                return entry_price * (1 + (locked_profit_pct / 100))
        else:
            profit_pct = ((entry_price - max_reached_price) / entry_price) * 100
            
            if profit_pct < 1.5:
                return initial_sl if initial_sl else entry_price * 1.01
            elif 1.5 <= profit_pct < 3.0:
                return entry_price * 0.99  
            elif 3.0 <= profit_pct < 3.5:
                locked_profit_pct = profit_pct * 0.70
                return entry_price * (1 - (locked_profit_pct / 100))
            else:
                locked_profit_pct = profit_pct * 0.75
                return entry_price * (1 - (locked_profit_pct / 100))
