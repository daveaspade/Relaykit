import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Generator, Iterable, List, Optional


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


class OpenClawCLIBackend:
    def __init__(self) -> None:
        self.command = self._resolve_command()
        self.available = self.command is not None

    def list_models(self) -> List[str]:
        env = os.environ.get("RELAYKIT_OPENCLAW_MODELS", "")
        models = [m.strip() for m in env.split(",") if m.strip()]
        if self.available and "auto" not in models:
            models.insert(0, "auto")
        return models

    def chat(self, model: str, prompt: str, stream: bool) -> Iterable[str]:
        if not self.available:
            return []
        use_model = bool(model) and model.lower() not in {"auto", "default"}
        cmd = [self.command or "openclaw", "exec", "--json", "--skip-git-repo-check"]
        if use_model:
            cmd += ["-m", model]
        cmd.append(prompt)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if proc.stdout is None:
            return []

        def gen() -> Generator[str, None, None]:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    text = _extract_text(obj)
                    if text:
                        yield text
                except json.JSONDecodeError:
                    continue

        if stream:
            return gen()

        last = ""
        for chunk in gen():
            last = chunk
        return [last]

    def _resolve_command(self) -> Optional[str]:
        explicit = os.environ.get("RELAYKIT_OPENCLAW_COMMAND", "").strip()
        if explicit:
            expanded = str(Path(explicit).expanduser())
            if Path(expanded).exists():
                return expanded
            resolved = shutil.which(explicit)
            if resolved:
                return resolved

        resolved = shutil.which("openclaw")
        if resolved:
            return resolved

        candidates = [
            "~/.npm-global/bin/openclaw",
            "~/.local/bin/openclaw",
            "~/bin/openclaw",
            "/opt/homebrew/bin/openclaw",
            "/usr/local/bin/openclaw",
        ]
        for candidate in candidates:
            expanded = Path(candidate).expanduser()
            if expanded.exists() and os.access(expanded, os.X_OK):
                return str(expanded)
        return None
