class RiskManager:
    def __init__(self, config):
        self.config = config

    def calculate_stop_loss(self, entry_price, max_reached_price, is_long=True, initial_sl=None):
        if is_long:
            profit_pct = ((max_reached_price - entry_price) / entry_price) * 100
            
            # KURAL 1: Kesin Zarar Sınırı (Maksimum %1.5)
            # ATR bazlı stop gelse bile, zarar asla %1.5'i geçemez.
            max_allowed_sl = entry_price * 0.985
            current_sl = max(initial_sl, max_allowed_sl) if initial_sl else max_allowed_sl

            # KURAL 4: %3 Kâr Barajı ve Kilit
            if profit_pct >= 3.0:
                # Kâr %3'ü gördüğünde, EN AZ %3'lük kâr kesin kilitlenir. 
                # (Eğer %75 Trailing Stop daha yüksek bir rakam veriyorsa, onu kullanır)
                locked_profit_pct = max(3.0, profit_pct * 0.75)
                return entry_price * (1 + (locked_profit_pct / 100))
                
            # KURAL 3: %1.5 Barajı ve İzleyen Stop (Trailing)
            elif profit_pct >= 1.5:
                # Kârın %75'ini kilitlerek takip et
                locked_profit_pct = profit_pct * 0.75
                return entry_price * (1 + (locked_profit_pct / 100))
                
            # KURAL 2: %1 Barajı ve Başa Baş (Break-even)
            elif profit_pct >= 1.0:
                # Kâr %1'i geçtiği an, işlemi sıfır risk bölgesine al
                return entry_price
                
            # Henüz hiçbir kâr barajı aşılmadıysa, korumalı ilk stopu kullan
            else:
                return current_sl

        else:
            # SHORT işlemler için max_reached_price, ulaşılan EN DÜŞÜK fiyattır
            profit_pct = ((entry_price - max_reached_price) / entry_price) * 100
            
            # KURAL 1: Kesin Zarar Sınırı (Maksimum %1.5)
            max_allowed_sl = entry_price * 1.015
            current_sl = min(initial_sl, max_allowed_sl) if initial_sl else max_allowed_sl

            # KURAL 4: %3 Kâr Barajı ve Kilit
            if profit_pct >= 3.0:
                locked_profit_pct = max(3.0, profit_pct * 0.75)
                return entry_price * (1 - (locked_profit_pct / 100))
                
            # KURAL 3: %1.5 Barajı ve İzleyen Stop (Trailing)
            elif profit_pct >= 1.5:
                locked_profit_pct = profit_pct * 0.75
                return entry_price * (1 - (locked_profit_pct / 100))
                
            # KURAL 2: %1 Barajı ve Başa Baş (Break-even)
            elif profit_pct >= 1.0:
                return entry_price
                
            # Başlangıç stop noktası
            else:
                return current_sl
