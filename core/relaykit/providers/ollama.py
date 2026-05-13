import json
import os
import urllib.error
import urllib.request
from typing import Generator, Iterable, List, Optional


def _get(url: str, timeout: int = 2) -> Optional[dict]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
        return None


def _post(url: str, payload: dict, timeout: int = 30) -> Optional[urllib.request.urlopen]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        return urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            body = ""
        raise RuntimeError(f"Ollama HTTP {exc.code}: {body or exc.reason}") from exc
    except urllib.error.URLError:
        return None


class OllamaBackend:
    def __init__(self, base_url: str = "http://localhost:11434", default_options: Optional[dict] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.default_options = default_options or {}

    def list_models(self) -> List[str]:
        data = _get(f"{self.base_url}/api/tags")
        if not data:
            return []
        models = []
        for m in data.get("models", []):
            name = m.get("name")
            if isinstance(name, str) and name:
                models.append(name)
        return models

    def chat(self, model: str, messages: List[dict], stream: bool, options: Optional[dict] = None) -> Iterable[str]:
        payload = {"model": model, "messages": messages, "stream": stream}
        merged_options = dict(self.default_options)
        if isinstance(options, dict):
            merged_options.update(options)
        if merged_options:
            payload["options"] = merged_options
        if os.environ.get("RELAYKIT_DEBUG_OLLAMA_PAYLOAD", "0") == "1":
            print(f"[relaykit][ollama] payload model={model} options={payload.get('options', {})}")
        resp = _post(f"{self.base_url}/api/chat", payload)
        if resp is None:
            return []
        if not stream:
            body = json.loads(resp.read().decode("utf-8"))
            if isinstance(body, dict) and isinstance(body.get("error"), str) and body.get("error"):
                raise RuntimeError(f"Ollama error: {body['error']}")
            msg = body.get("message", {})
            content = msg.get("content", "")
            return [content]

        def gen() -> Generator[str, None, None]:
            for line in resp:
                try:
                    obj = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    continue
                msg = obj.get("message")
                if isinstance(msg, dict):
                    text = msg.get("content", "")
                    if isinstance(text, str) and text:
                        yield text
        return gen()

    def embeddings(self, model: str, prompt: str) -> Optional[List[float]]:
        payload = {"model": model, "prompt": prompt}
        resp = _post(f"{self.base_url}/api/embeddings", payload)
        if resp is None:
            return None
        try:
            body = json.loads(resp.read().decode("utf-8"))
        except json.JSONDecodeError:
            return None
        emb = body.get("embedding")
        if isinstance(emb, list):
            return emb
        return None
