import json
import os

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
            "reserved_slots": [] 
        }
        with open(STATE_FILE, "w") as f:
            json.dump(initial_state, f, indent=4)

    def load_state(self):
        with open(STATE_FILE, "r") as f:
            return json.load(f)

    def save_state(self):
        with open(STATE_FILE, "w") as f:
            json.dump(self.state, f, indent=4)
            
    def get_used_slots(self):
        return len(self.state["active_trades"]) + len(self.state["reserved_slots"])
