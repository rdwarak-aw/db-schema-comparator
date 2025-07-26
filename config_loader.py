import json
import threading

_config = None
_config_lock = threading.Lock()

def load_config(path="config.json"):
    global _config
    with _config_lock:
        if _config is None:
            try:
                with open(path, "r") as f:
                    _config = json.load(f)
            except Exception as e:
                raise RuntimeError(f"Failed to load config from {path}: {e}")
        return _config
