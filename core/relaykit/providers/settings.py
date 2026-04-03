import json
from pathlib import Path
from typing import Optional


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def gemini_model_from_settings() -> Optional[str]:
    path = Path.home() / ".gemini" / "settings.json"
    data = _read_json(path)
    if not data:
        return None
    model = None
    if isinstance(data.get("model"), dict):
        model = data["model"].get("name")
    if isinstance(model, str) and model:
        return model
    return None


def claude_model_from_settings() -> Optional[str]:
    path = Path.home() / ".claude" / "settings.json"
    data = _read_json(path)
    if not data:
        return None
    model = data.get("model")
    if isinstance(model, str) and model:
        return model
    if isinstance(model, dict):
        for key in ("id", "name", "model"):
            val = model.get(key)
            if isinstance(val, str) and val:
                return val
    return None
