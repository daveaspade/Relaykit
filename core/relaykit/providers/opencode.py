import os
import shutil
import subprocess
from typing import Iterable, List, Tuple


class OpenCodeBackend:
    def __init__(self) -> None:
        self.available = shutil.which("opencode") is not None

    def list_models(self, refresh: bool = False) -> List[str]:
        if not self.available:
            return []
        # Use cached list (fast). Users can refresh by setting RELAYKIT_REFRESH=1
        refresh = refresh or bool(int(os.environ.get("RELAYKIT_REFRESH", "0")))
        cmd = ["opencode", "models"]
        if refresh:
            cmd.append("--refresh")
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            return []
        models: List[str] = []
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line or line.lower().startswith("models cache refreshed"):
                continue
            if "/" in line:
                models.append(line)
        return models

    def run_chat(self, model: str, prompt: str, stream: bool = False) -> Iterable[str]:
        # model format: provider/model (opencode)
        cmd = ["opencode", "run", "--format", "json", "-m", model, prompt]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if proc.stdout is None:
            return []
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            yield line
        proc.wait()


def map_prefix_to_provider(prefix: str) -> str:
    prefix = prefix.lower()
    if prefix in {"claude", "anthropic"}:
        return "anthropic"
    if prefix in {"gemini", "google"}:
        return "google"
    if prefix in {"codex", "openai"}:
        return "openai"
    if prefix in {"hermes"}:
        return "hermes"
    if prefix in {"ollama"}:
        return "ollama"
    return "openai"


def to_model_prefix(model: str) -> Tuple[str, str]:
    if "/" in model:
        prefix, rest = model.split("/", 1)
        return prefix, rest
    if ":" in model:
        prefix, rest = model.split(":", 1)
        return prefix, rest
    return "openai", model


def opencode_model_id(prefix: str, model: str) -> str:
    provider = map_prefix_to_provider(prefix)
    return f"{provider}/{model}"
