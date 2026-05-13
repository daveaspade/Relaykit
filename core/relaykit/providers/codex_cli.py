import json
import os
import shutil
import subprocess
from typing import Generator, Iterable, List


def _flatten_value(val: object, depth: int = 0) -> str:
    if depth > 6:
        return ""
    if isinstance(val, str):
        return val
    if isinstance(val, list):
        parts: List[str] = []
        for item in val:
            text = _flatten_value(item, depth + 1)
            if text:
                parts.append(text)
        return "\n".join(parts)
    if isinstance(val, dict):
        for key in ("text", "content", "message", "output", "result", "response"):
            if key in val:
                text = _flatten_value(val.get(key), depth + 1)
                if text:
                    return text
        skip_keys = {"type", "id", "role", "name", "model", "session_id", "thread_id", "turn_id", "status", "uuid"}
        for k, item in val.items():
            if k in skip_keys:
                continue
            text = _flatten_value(item, depth + 1)
            if text:
                return text
    return ""


def _extract_text(obj: dict) -> str:
    if isinstance(obj.get("item"), dict):
        return _flatten_value(obj.get("item")).strip()
    return _flatten_value(obj).strip()


class CodexCLIBackend:
    def __init__(self) -> None:
        self.available = shutil.which("codex") is not None

    def list_models(self) -> List[str]:
        env = os.environ.get("RELAYKIT_CODEX_MODELS", "")
        models = [m.strip() for m in env.split(",") if m.strip()]
        if self.available and "auto" not in models:
            models.insert(0, "auto")
        return models

    def chat(self, model: str, prompt: str, stream: bool) -> Iterable[str]:
        if not self.available:
            return []
        use_model = bool(model) and model.lower() not in {"auto", "default"}
        cmd = ["codex", "exec", "--json", "--skip-git-repo-check"]
        if use_model:
            cmd += ["-m", model]
        cmd.append(prompt)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if proc.stdout is None:
            return []
        try:
            out, _ = proc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            return []
        # Extract the last non-empty text from all JSON lines
        last = ""
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                text = _extract_text(obj)
                if text:
                    last = text
            except json.JSONDecodeError:
                continue
        return [last] if last else []
