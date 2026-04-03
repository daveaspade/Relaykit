import json
import os
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator, Iterable, List, Optional, Tuple


def _get(url: str, timeout: int = 2) -> Optional[dict]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
        return None


def _post(url: str, payload: dict, headers: dict, timeout: int = 60) -> Optional[urllib.request.urlopen]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        return urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.URLError:
        return None


def _join(base: str, path: str) -> str:
    if base.endswith("/"):
        base = base[:-1]
    if path.startswith("/"):
        return base + path
    return base + "/" + path


def _probe_one(name: str, url: str) -> Optional["OpenAICompatBackend"]:
    backend = OpenAICompatBackend(name=name, base_url=url)
    models = backend.list_models()
    if models:
        return backend
    return None


class OpenAICompatBackend:
    def __init__(self, name: str, base_url: str, api_key: Optional[str] = None) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def list_models(self) -> List[str]:
        data = _get(_join(self.base_url, "/models"))
        if not data:
            return []
        models = []
        for item in data.get("data", []):
            mid = item.get("id")
            if isinstance(mid, str) and mid:
                models.append(mid)
        return models

    def chat(self, model: str, messages: List[dict], stream: bool) -> Iterable[str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {"model": model, "messages": messages, "stream": stream}
        resp = _post(_join(self.base_url, "/chat/completions"), payload, headers=headers)
        if resp is None:
            return []
        if not stream:
            body = json.loads(resp.read().decode("utf-8"))
            choices = body.get("choices", [])
            if choices:
                msg = choices[0].get("message", {})
                content = msg.get("content", "")
                return [content]
            return [""]

        def gen() -> Generator[str, None, None]:
            for line in resp:
                if not line:
                    continue
                try:
                    raw = line.decode("utf-8").strip()
                except Exception:
                    continue
                if not raw:
                    continue
                if raw.startswith("data:"):
                    raw = raw[len("data:"):].strip()
                if raw == "[DONE]":
                    break
                try:
                    obj = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                choices = obj.get("choices", [])
                if choices:
                    delta = choices[0].get("delta", {})
                    text = delta.get("content", "")
                    if isinstance(text, str) and text:
                        yield text
        return gen()

    def embeddings(self, model: str, inputs: List[str]) -> Optional[List[List[float]]]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {"model": model, "input": inputs}
        resp = _post(_join(self.base_url, "/embeddings"), payload, headers=headers)
        if resp is None:
            return None
        try:
            body = json.loads(resp.read().decode("utf-8"))
        except json.JSONDecodeError:
            return None
        data = body.get("data", [])
        out: List[List[float]] = []
        for item in data:
            emb = item.get("embedding")
            if isinstance(emb, list):
                out.append(emb)
        return out


def detect_local_endpoints() -> List[Tuple[str, str]]:
    # Known local OpenAI-compatible endpoints.
    endpoints: List[Tuple[str, str]] = [
        ("lmstudio", "http://127.0.0.1:1234/v1"),
        ("lmstudio", "http://localhost:1234/v1"),
    ]

    # Optional bounded scan of common local ports. This is intentionally short
    # and loopback-only so we improve discovery without turning startup into a
    # network sweep.
    scan_ports = os.environ.get("RELAYKIT_COMPAT_SCAN_PORTS", "")
    ports: List[str] = [p.strip() for p in scan_ports.split(",") if p.strip()]
    if not ports:
        ports = ["3210", "5000", "8000", "8080"]
    seen = {(name, url) for name, url in endpoints}
    for port in ports:
        for host in ("127.0.0.1", "localhost"):
            url = f"http://{host}:{port}/v1"
            item = ("compat", url)
            if item not in seen:
                endpoints.append(item)
                seen.add(item)
    return endpoints


def probe_endpoints(endpoints: List[Tuple[str, str]]) -> List[OpenAICompatBackend]:
    backends: List[OpenAICompatBackend] = []
    if not endpoints:
        return backends
    max_workers = min(8, len(endpoints))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_probe_one, name, url): (name, url) for name, url in endpoints}
        for fut in as_completed(future_map):
            try:
                backend = fut.result()
            except Exception:
                continue
            if backend is not None:
                backends.append(backend)
    return backends
