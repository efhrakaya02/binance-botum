import json
import os
import time

STATE_FILE = "data/state.json"

class StateManager:
    def __init__(self):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        if not os.path.exists(STATE_FILE):
            self._create_empty_state()
        self.state = self.load_state()

    def _create_empty_state(self):
        initial_state = {
            "active_trades": {}, 
            "reserved_slots": [],
            "cooldowns": {} # 🛡️ YENİ: Bekleme süreleri listesi
        }
        with open(STATE_FILE, "w") as f:
            json.dump(initial_state, f, indent=4)

    def load_state(self):
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
            # Eski state dosyasında cooldowns yoksa çökmemesi için ekleme
            if "cooldowns" not in data:
                data["cooldowns"] = {}
            return data

    def save_state(self):
        with open(STATE_FILE, "w") as f:
            json.dump(self.state, f, indent=4)
            
    def get_used_slots(self):
        return len(self.state["active_trades"]) + len(self.state["reserved_slots"])

    # 🛡️ COOLDOWN (SOĞUMA) KONTROLLERİ
    def set_cooldown(self, symbol, minutes):
        self.state["cooldowns"][symbol] = time.time() + (minutes * 60)
        self.save_state()

    def is_in_cooldown(self, symbol):
        if symbol in self.state["cooldowns"]:
            if time.time() < self.state["cooldowns"][symbol]:
                return True
            else:
                del self.state["cooldowns"][symbol] # Süresi dolduysa listeden sil
                self.save_state()
                return False
        return False
