class StateManager:
    def __init__(self):
        self.hourly_trades = []

    def record_closed_trade(self, symbol, pnl, reason):
        """Kapanan işlemi hafızaya alır."""
        self.hourly_trades.append({
            'symbol': symbol,
            'pnl': pnl,
            'reason': reason
        })

    def generate_hourly_report(self):
        """Saatlik performans özetini hesaplar ve sıfırlar."""
        if not self.hourly_trades:
            return "Geçtiğimiz saat içinde kapanan işlem olmadı."

        total_trades = len(self.hourly_trades)
        wins = sum(1 for t in self.hourly_trades if t['pnl'] > 0)
        losses = sum(1 for t in self.hourly_trades if t['pnl'] <= 0)
        net_pnl = sum(t['pnl'] for t in self.hourly_trades)

        report = (
            f"🕒 SAATLİK KAPANIŞ RAPORU 🕒\n"
            f"Toplam Kapanan İşlem: {total_trades}\n"
            f"Başarılı (Kâr): {wins} | Başarısız (Zarar): {losses}\n"
            f"Net PnL Durumu: %{net_pnl:.2f}\n"
            f"-----------------------------------"
        )
        self.hourly_trades = [] # Rapor sonrası sıfırla
        return report
