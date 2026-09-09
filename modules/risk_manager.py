class RiskManager:
    def __init__(self, config):
        self.config = config

    def check_exit_conditions(self, current_price, entry_price, side, sl_price, tp_price):
        """
        Timsah Modu: Sadece Dinamik ATR hedeflerine (SL/TP) sadık kalınır. 
        Sabit yüzdelik veya izleyen stop (trailing) kullanılmaz.
        """
        if side == 'buy':
            if current_price <= sl_price:
                return {"exit": True, "reason": f"🛑 Genişletilmiş ATR Stop-Loss (SL) Vuruldu. Fiyat: {current_price}"}
            if current_price >= tp_price:
                return {"exit": True, "reason": f"🐊 Timsah Avı Başarılı! 4H Hedefe (TP) Ulaşıldı. Fiyat: {current_price}"}
        
        elif side == 'sell':
            if current_price >= sl_price:
                return {"exit": True, "reason": f"🛑 Genişletilmiş ATR Stop-Loss (SL) Vuruldu. Fiyat: {current_price}"}
            if current_price <= tp_price:
                return {"exit": True, "reason": f"🐊 Timsah Avı Başarılı! 4H Hedefe (TP) Ulaşıldı. Fiyat: {current_price}"}
                
        return {"exit": False, "reason": ""}
