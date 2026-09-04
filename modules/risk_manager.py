class RiskManager:
    def __init__(self, config):
        self.config = config

    def calculate_stop_loss(self, entry_price, max_reached_price, is_long=True):
        if is_long:
            profit_pct = ((max_reached_price - entry_price) / entry_price) * 100
            
            if profit_pct < self.config.BREAKEVEN_PCT:
                return entry_price * 0.99
            elif self.config.BREAKEVEN_PCT <= profit_pct < self.config.TRAILING_ACTIVATION_PCT:
                return entry_price
            else:
                locked_profit_pct = profit_pct / 2.0
                return entry_price * (1 + (locked_profit_pct / 100))
        else:
            profit_pct = ((entry_price - max_reached_price) / entry_price) * 100
            
            if profit_pct < self.config.BREAKEVEN_PCT:
                return entry_price * 1.01
            elif self.config.BREAKEVEN_PCT <= profit_pct < self.config.TRAILING_ACTIVATION_PCT:
                return entry_price
            else:
                locked_profit_pct = profit_pct / 2.0
                return entry_price * (1 - (locked_profit_pct / 100))
