import argparse
import asyncio
import json
import os
import shutil
import time
import threading
from datetime import datetime
from typing import Any, Dict, Generator, Iterable, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse, Response

from relaykit.config import SETTINGS
from relaykit.providers.opencode import OpenCodeBackend, opencode_model_id, to_model_prefix
from relaykit.providers.ollama import OllamaBackend
from relaykit.providers.settings import claude_model_from_settings, gemini_model_from_settings
from relaykit.providers.openai_compat import detect_local_endpoints, probe_endpoints
from relaykit.providers.claude_cli import ClaudeCLIBackend
from relaykit.providers.gemini_cli import GeminiCLIBackend
from relaykit.providers.codex_cli import CodexCLIBackend
from relaykit.providers.hermes_cli import HermesCLIBackend
from relaykit.config_store import load_config, save_config, create_key, verify_key, list_keys


app = FastAPI()
backend = OpenCodeBackend()
claude_cli = ClaudeCLIBackend()
gemini_cli = GeminiCLIBackend()
codex_cli = CodexCLIBackend()
hermes_cli = HermesCLIBackend()

_MODEL_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "refresh_started": 0.0,
    "refresh_finished": 0.0,
    "refreshing": False,
    "error": None,
    "models": [],
    "all_models": [],
    "map": {},
    "aliases": {},
    "disabled_ids": [],
    "disabled_providers": [],
    "providers": [],
}

_STATE_LOCK = threading.RLock()
_DISCOVERY_TTL_SECONDS = int(os.environ.get("RELAYKIT_CATALOG_TTL_SECONDS", "20"))
_ROUTING_LOCK = threading.RLock()
_ROUTING_RUNTIME: Dict[str, Any] = {
    "models": {},
    "probe": {
        "thread_started": False,
        "last_started": 0.0,
        "last_finished": 0.0,
        "running": False,
        "cycles": 0,
        "last_error": "",
        "last_results": {},
    },
}


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _routing_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    routing = cfg.get("routing", {})
    if not isinstance(routing, dict):
        routing = {}
    weights = routing.get("weights", {})
    if not isinstance(weights, dict):
        weights = {}
    reliability_w = _clamp(_to_float(weights.get("reliability"), 0.55), 0.0, 1.0)
    latency_w = _clamp(_to_float(weights.get("latency"), 0.30), 0.0, 1.0)
    cost_w = _clamp(_to_float(weights.get("cost"), 0.15), 0.0, 1.0)
    total = reliability_w + latency_w + cost_w
    if total <= 0:
        reliability_w, latency_w, cost_w = 0.55, 0.30, 0.15
        total = 1.0
    return {
        "weights": {
            "reliability": reliability_w / total,
            "latency": latency_w / total,
            "cost": cost_w / total,
        },
        "max_attempts_per_candidate": max(1, _to_int(routing.get("max_attempts_per_candidate"), 2)),
        "retry_backoff_ms": max(10, _to_int(routing.get("retry_backoff_ms"), 220)),
        "breaker_failure_threshold": max(1, _to_int(routing.get("breaker_failure_threshold"), 3)),
        "breaker_cooldown_seconds": max(5, _to_int(routing.get("breaker_cooldown_seconds"), 45)),
        "latency_ewma_alpha": _clamp(_to_float(routing.get("latency_ewma_alpha"), 0.35), 0.05, 0.95),
        "probe_enabled": bool(routing.get("probe_enabled", True)),
        "probe_interval_seconds": max(20, _to_int(routing.get("probe_interval_seconds"), 90)),
        "probe_model_timeout_seconds": max(1, _to_int(routing.get("probe_model_timeout_seconds"), 6)),
        "probe_mode": str(routing.get("probe_mode", "light") or "light").strip().lower(),
    }


def _runtime_entry(model: str) -> Dict[str, Any]:
    with _ROUTING_LOCK:
        models = _ROUTING_RUNTIME.setdefault("models", {})
        entry = models.get(model)
        if not isinstance(entry, dict):
            entry = {}
            models[model] = entry
        entry.setdefault("requests", 0)
        entry.setdefault("successes", 0)
        entry.setdefault("failures", 0)
        entry.setdefault("consecutive_failures", 0)
        entry.setdefault("last_error", "")
        entry.setdefault("last_failure_at", "")
        entry.setdefault("last_success_at", "")
        entry.setdefault("last_latency_ms", 0.0)
        entry.setdefault("ewma_latency_ms", 0.0)
        entry.setdefault("breaker_open_until", 0.0)
        entry.setdefault("probe_failures", 0)
        entry.setdefault("probe_successes", 0)
        entry.setdefault("last_probe_at", "")
        entry.setdefault("health_status", "unknown")
        return entry


def _circuit_open_until(model: str) -> float:
    entry = _runtime_entry(model)
    return _to_float(entry.get("breaker_open_until"), 0.0)


def _is_circuit_open(model: str) -> bool:
    return _circuit_open_until(model) > time.time()


def _register_route_success(model: str, latency_ms: float, cfg: Dict[str, Any]) -> None:
    routing = _routing_config(cfg)
    alpha = float(routing["latency_ewma_alpha"])
    entry = _runtime_entry(model)
    now_iso = _now_iso()
    with _ROUTING_LOCK:
        prev_ewma = _to_float(entry.get("ewma_latency_ms"), 0.0)
        if prev_ewma <= 0:
            ewma = latency_ms
        else:
            ewma = (alpha * latency_ms) + ((1.0 - alpha) * prev_ewma)
        entry["requests"] = int(entry.get("requests", 0) or 0) + 1
        entry["successes"] = int(entry.get("successes", 0) or 0) + 1
        entry["consecutive_failures"] = 0
        entry["last_success_at"] = now_iso
        entry["last_error"] = ""
        entry["last_latency_ms"] = round(max(0.0, latency_ms), 2)
        entry["ewma_latency_ms"] = round(max(0.0, ewma), 2)
        entry["breaker_open_until"] = 0.0
        entry["health_status"] = "healthy"


def _register_route_failure(model: str, error: str, latency_ms: float, cfg: Dict[str, Any]) -> None:
    routing = _routing_config(cfg)
    threshold = int(routing["breaker_failure_threshold"])
    cooldown = int(routing["breaker_cooldown_seconds"])
    entry = _runtime_entry(model)
    now = time.time()
    now_iso = _now_iso()
    with _ROUTING_LOCK:
        entry["requests"] = int(entry.get("requests", 0) or 0) + 1
        entry["failures"] = int(entry.get("failures", 0) or 0) + 1
        entry["consecutive_failures"] = int(entry.get("consecutive_failures", 0) or 0) + 1
        entry["last_failure_at"] = now_iso
        entry["last_error"] = (error or "")[:500]
        entry["last_latency_ms"] = round(max(0.0, latency_ms), 2)
        if int(entry["consecutive_failures"]) >= threshold:
            entry["breaker_open_until"] = now + cooldown
            entry["health_status"] = "unhealthy"
        elif int(entry["consecutive_failures"]) >= max(1, threshold // 2):
            entry["health_status"] = "degraded"


def _register_probe_result(model: str, success: bool, error: str = "") -> None:
    entry = _runtime_entry(model)
    now_iso = _now_iso()
    with _ROUTING_LOCK:
        entry["last_probe_at"] = now_iso
        if success:
            entry["probe_successes"] = int(entry.get("probe_successes", 0) or 0) + 1
            entry["probe_failures"] = 0
            if not _is_circuit_open(model):
                entry["health_status"] = "healthy"
        else:
            entry["probe_failures"] = int(entry.get("probe_failures", 0) or 0) + 1
            if error:
                entry["last_error"] = error[:500]
            if int(entry["probe_failures"]) >= 3:
                entry["health_status"] = "unhealthy"
            elif int(entry["probe_failures"]) >= 1:
                entry["health_status"] = "degraded"


def _latency_penalty_ms(value: float) -> float:
    # 0ms -> 0 penalty; >=8s -> max penalty.
    return _clamp(max(0.0, value) / 8000.0, 0.0, 1.0)


def _estimate_model_cost_penalty(model_id: str) -> float:
    lower = (model_id or "").lower()
    penalty = 0.42
    cheap_tokens = ("mini", "haiku", "flash", "lite", "nano", "small", "auto", "embedding")
    expensive_tokens = ("opus", "pro", "max", "ultra", "70b", "35b", "32b", "30b", "gpt-5")
    very_expensive_tokens = ("opus-4-6", "gpt-5.4", "gpt-5-codex")
    if any(tok in lower for tok in cheap_tokens):
        penalty -= 0.20
    if any(tok in lower for tok in expensive_tokens):
        penalty += 0.22
    if any(tok in lower for tok in very_expensive_tokens):
        penalty += 0.15
    if lower.startswith("hermes:") or lower == "hermes":
        penalty += 0.08
    if lower.startswith("relaykit:"):
        penalty += 0.04
    return _clamp(penalty, 0.0, 1.0)


def _strict_mode_enabled(cfg: Dict[str, Any]) -> bool:
    ui = cfg.get("ui", {})
    if not isinstance(ui, dict):
        return False
    return bool(ui.get("strict_mode", False))


def _is_strict_excluded_model(item: Dict[str, Any]) -> bool:
    if not isinstance(item, dict):
        return False
    model_id = str(item.get("id") or "").lower()
    provider = str(item.get("provider") or item.get("source") or item.get("owned_by") or "").lower()
    kind = str(item.get("kind") or "").lower()
    target = f"{provider} {kind} {model_id}"
    blocked_tokens = (
        "preview",
        "experimental",
        "beta",
        "alpha",
        "canary",
        "nightly",
        "dev",
        "test",
    )
    return any(token in target for token in blocked_tokens)


def _is_retryable_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPException):
        code = int(exc.status_code)
        if code in {408, 409, 425, 429}:
            return True
        if code >= 500:
            return True
        return False
    msg = str(exc).lower()
    retry_signals = ("timeout", "timed out", "temporar", "connection", "reset by peer", "unavailable", "try again")
    return any(token in msg for token in retry_signals)


def _retry_delay_seconds(attempt_idx: int, cfg: Dict[str, Any]) -> float:
    routing = _routing_config(cfg)
    base_ms = int(routing["retry_backoff_ms"])
    delay_ms = base_ms * (2 ** max(0, attempt_idx))
    return _clamp(delay_ms / 1000.0, 0.05, 3.0)


def _choose_probe_targets(catalog: Dict[str, Any]) -> List[str]:
    targets: Dict[str, str] = {}
    preferred = {"hermes", "claude", "gemini", "codex", "ollama"}
    for item in catalog.get("models", []):
        mid = item.get("id")
        provider = item.get("provider")
        if not isinstance(mid, str) or not isinstance(provider, str):
            continue
        if not item.get("enabled", True):
            continue
        if provider not in preferred:
            continue
        if provider not in targets:
            targets[provider] = mid
        if mid.endswith(":auto") or mid.endswith("/auto") or mid == provider:
            targets[provider] = mid
    return list(targets.values())


def _light_probe_model(model: str, cfg: Dict[str, Any], catalog: Dict[str, Any]) -> tuple[bool, str]:
    prefix, model_name = to_model_prefix(model)
    if prefix == "relaykit":
        prefix, model_name = _route_relaykit_namespace(model_name, catalog)
    if prefix in {"claude", "anthropic"}:
        if not claude_cli.available:
            return False, "Claude CLI unavailable"
        return True, ""
    if prefix in {"gemini", "google"}:
        if not gemini_cli.available:
            return False, "Gemini CLI unavailable"
        return True, ""
    if prefix in {"codex", "openai"}:
        if not codex_cli.available:
            return False, "Codex CLI unavailable"
        return True, ""
    if prefix == "hermes":
        if not hermes_cli.available:
            return False, "Hermes CLI unavailable"
        return True, ""
    if prefix == "ollama":
        try:
            ollama_cfg = cfg.get("providers", {}).get("ollama", {})
            base_url = ollama_cfg.get("base_url", "http://localhost:11434")
            models = OllamaBackend(base_url=base_url).list_models()
            return (len(models) > 0), ("No Ollama models detected" if not models else "")
        except Exception as exc:
            return False, str(exc)
    return True, ""


def _run_health_probe_cycle(cfg: Dict[str, Any]) -> None:
    routing = _routing_config(cfg)
    if not routing["probe_enabled"]:
        return
    catalog = _get_catalog(cfg)
    targets = _choose_probe_targets(catalog)
    probe = _ROUTING_RUNTIME.setdefault("probe", {})
    with _ROUTING_LOCK:
        probe["running"] = True
        probe["last_started"] = time.time()
        probe["last_error"] = ""
    results: Dict[str, str] = {}
    try:
        for candidate in targets:
            ok, error = _light_probe_model(candidate, cfg, catalog)
            _register_probe_result(candidate, ok, error=error)
            results[candidate] = "ok" if ok else (error or "probe failed")
    except Exception as exc:
        with _ROUTING_LOCK:
            probe["last_error"] = str(exc)
    finally:
        with _ROUTING_LOCK:
            probe["running"] = False
            probe["cycles"] = int(probe.get("cycles", 0) or 0) + 1
            probe["last_finished"] = time.time()
            probe["last_results"] = results


def _health_probe_loop() -> None:
    while True:
        try:
            cfg = load_config()
            routing = _routing_config(cfg)
            interval = int(routing["probe_interval_seconds"])
            _run_health_probe_cycle(cfg)
            time.sleep(max(10, interval))
        except Exception:
            time.sleep(15)


def _ensure_probe_thread_started() -> None:
    probe = _ROUTING_RUNTIME.setdefault("probe", {})
    with _ROUTING_LOCK:
        if probe.get("thread_started"):
            return
        probe["thread_started"] = True
    thread = threading.Thread(target=_health_probe_loop, daemon=True, name="relaykit-health-probe")
    thread.start()


def _routing_runtime_snapshot(limit: int = 120) -> Dict[str, Any]:
    with _ROUTING_LOCK:
        models = dict(_ROUTING_RUNTIME.get("models", {}))
        probe = dict(_ROUTING_RUNTIME.get("probe", {}))
    items = []
    for model, raw in models.items():
        if not isinstance(raw, dict):
            continue
        breaker_open_until = _to_float(raw.get("breaker_open_until"), 0.0)
        items.append(
            {
                "model": model,
                "health_status": str(raw.get("health_status", "unknown") or "unknown"),
                "consecutive_failures": int(raw.get("consecutive_failures", 0) or 0),
                "breaker_open": breaker_open_until > time.time(),
                "breaker_open_until": breaker_open_until,
                "ewma_latency_ms": _to_float(raw.get("ewma_latency_ms"), 0.0),
                "last_latency_ms": _to_float(raw.get("last_latency_ms"), 0.0),
                "last_error": str(raw.get("last_error", "") or ""),
                "last_success_at": str(raw.get("last_success_at", "") or ""),
                "last_failure_at": str(raw.get("last_failure_at", "") or ""),
                "probe_failures": int(raw.get("probe_failures", 0) or 0),
                "probe_successes": int(raw.get("probe_successes", 0) or 0),
            }
        )
    items.sort(key=lambda x: (0 if x["breaker_open"] else 1, x["health_status"] != "healthy", -x["consecutive_failures"], x["model"]))
    unhealthy = [item for item in items if item["health_status"] in {"degraded", "unhealthy"}]
    open_circuits = [item for item in items if item["breaker_open"]]
    return {
        "probe": probe,
        "open_circuit_count": len(open_circuits),
        "unhealthy_count": len(unhealthy),
        "open_circuits": open_circuits[: min(50, max(1, limit // 2))],
        "models": items[:limit],
    }


def _audit_log(entry: Dict[str, Any]) -> None:
    try:
        from relaykit.config_store import CONFIG_DIR
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        path = CONFIG_DIR / "audit.log"
        entry["ts"] = int(time.time())
        with path.open("a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.url.path.startswith("/v1") and request.url.path != "/v1/models":
        cfg = load_config()
        keys = list_keys(cfg)
        if keys:
            auth = request.headers.get("Authorization", "")
            token = ""
            if auth.startswith("Bearer "):
                token = auth[len("Bearer "):].strip()
            label = verify_key(cfg, token) if token else None
            if not label:
                return JSONResponse({"error": "unauthorized"}, status_code=401)
            request.state.key_label = label
    response = await call_next(request)
    return response


@app.on_event("startup")
async def startup_event() -> None:
    _ensure_probe_thread_started()


def _compat_backends(cfg: Dict[str, Any]) -> List[dict]:
    # Allow user-specified extra endpoints and bounded loopback scans.
    urls = detect_local_endpoints()
    seen = {(name, url) for name, url in urls}
    extra = os.environ.get("RELAYKIT_COMPAT_URLS", "")
    for entry in extra.split(","):
        entry = entry.strip()
        if not entry:
            continue
        item = ("compat", entry)
        if item not in seen:
            urls.append(item)
            seen.add(item)
    scan_ports = cfg.get("ui", {}).get("scan_ports", [])
    if isinstance(scan_ports, list):
        for port in scan_ports:
            if isinstance(port, int):
                port = str(port)
            if not isinstance(port, str):
                continue
            port = port.strip()
            if not port:
                continue
            for host in ("127.0.0.1", "localhost"):
                item = ("compat", f"http://{host}:{port}/v1")
                if item not in seen:
                    urls.append(item)
                    seen.add(item)
    for item in cfg.get("providers", {}).get("openai_compat", []):
        name = item.get("name")
        url = item.get("url")
        if name and url:
            item = (name, url)
            if item not in seen:
                urls.append(item)
                seen.add(item)
    backends = probe_endpoints(urls)
    return [{"name": b.name, "url": b.base_url, "backend": b} for b in backends]


def _normalize_model_list(values: Iterable[Any], *, drop_auto: bool = False) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        if not isinstance(item, str):
            continue
        value = item.strip()
        if not value:
            continue
        lower = value.lower()
        if drop_auto and lower in {"auto", "default"}:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _detected_cli_snapshot() -> Dict[str, Any]:
    ollama_available = shutil.which("ollama") is not None
    return {
        "opencode": {"available": backend.available, "models": _normalize_model_list(backend.list_models(refresh=True))},
        "claude": {"available": claude_cli.available, "models": _normalize_model_list(claude_cli.list_models())},
        "gemini": {"available": gemini_cli.available, "models": _normalize_model_list(gemini_cli.list_models())},
        "codex": {"available": codex_cli.available, "models": _normalize_model_list(codex_cli.list_models())},
        "hermes": {"available": hermes_cli.available, "models": _normalize_model_list(hermes_cli.list_models())},
        "ollama": {"available": ollama_available, "models": []},
    }


def _seed_provider_models(cfg: Dict[str, Any], detected: Dict[str, Any], replace: bool = False) -> Dict[str, int]:
    providers_cfg = cfg.setdefault("providers", {})
    seeded_counts: Dict[str, int] = {}
    for provider in ("claude", "gemini", "codex", "hermes"):
        provider_cfg = providers_cfg.setdefault(provider, {})
        if not isinstance(provider_cfg, dict):
            provider_cfg = {}
            providers_cfg[provider] = provider_cfg
        current = provider_cfg.get("models", [])
        current_models = _normalize_model_list(current) if isinstance(current, list) else []
        detected_models = _normalize_model_list(detected.get(provider, {}).get("models", []), drop_auto=True)
        if replace or not current_models:
            provider_cfg["models"] = detected_models
            seeded_counts[provider] = len(detected_models)
        else:
            seeded_counts[provider] = len(current_models)
    ollama_cfg = providers_cfg.setdefault("ollama", {})
    if not isinstance(ollama_cfg, dict):
        ollama_cfg = {}
        providers_cfg["ollama"] = ollama_cfg
    if "enabled" not in ollama_cfg:
        ollama_cfg["enabled"] = True
    if not isinstance(ollama_cfg.get("base_url"), str) or not ollama_cfg.get("base_url"):
        ollama_cfg["base_url"] = "http://localhost:11434"
    return seeded_counts


def _model_overrides(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    ui = cfg.get("ui", {})
    overrides = ui.get("model_overrides", {})
    return overrides if isinstance(overrides, dict) else {}


def _provider_overrides(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    ui = cfg.get("ui", {})
    overrides = ui.get("provider_overrides", {})
    return overrides if isinstance(overrides, dict) else {}


def _provider_enabled(provider: str, overrides: Dict[str, Dict[str, Any]]) -> bool:
    override = overrides.get(provider)
    if not isinstance(override, dict):
        return True
    if "enabled" in override:
        return bool(override.get("enabled"))
    return True


def _model_enabled(model_id: str, overrides: Dict[str, Dict[str, Any]]) -> bool:
    override = overrides.get(model_id)
    if not isinstance(override, dict):
        return True
    if "enabled" in override:
        return bool(override.get("enabled"))
    return True


def _provider_label(provider: str, override: Optional[Dict[str, Any]]) -> str:
    if isinstance(override, dict):
        label = override.get("label")
        if isinstance(label, str) and label.strip():
            return label.strip()
    if provider == "relaykit":
        return "RelayKit gateway"
    return provider


def _display_label(model_id: str, override: Optional[Dict[str, Any]]) -> str:
    if isinstance(override, dict):
        label = override.get("label")
        if isinstance(label, str) and label.strip():
            return label.strip()
    return model_id


def _model_stats(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    ui = cfg.setdefault("ui", {})
    stats = ui.get("model_stats", {})
    return stats if isinstance(stats, dict) else {}


def _provider_priority(provider: str) -> int:
    order = {"hermes": 0, "claude": 1, "gemini": 2, "codex": 3, "ollama": 4, "openai": 5, "relaykit": 6, "alias": 7}
    return order.get(provider, 50)


def _reliability_score(stats: Optional[Dict[str, Any]]) -> float:
    if not isinstance(stats, dict):
        return 0.0
    successes = int(stats.get("successes", 0) or 0)
    failures = int(stats.get("failures", 0) or 0)
    total = successes + failures
    if total <= 0:
        return 0.0
    return successes / total


def _model_sort_key(item: Dict[str, Any]) -> tuple:
    provider = str(item.get("provider") or item.get("source") or item.get("owned_by") or "other")
    stats = item.get("stats", {})
    reliability = _reliability_score(stats)
    successes = int(stats.get("successes", 0) or 0) if isinstance(stats, dict) else 0
    last_success = str(stats.get("last_success_at", "") or stats.get("last_test_success_at", "") or "")
    last_used = str(stats.get("last_used_at", "") or "")
    kind = item.get("kind") or "model"
    relaykit_gate = 0 if provider == "hermes" and item.get("id") in {"hermes", "hermes:auto"} else 1
    gateway_penalty = 1 if kind == "gateway" else 0
    return (
        relaykit_gate,
        gateway_penalty,
        -_provider_priority(provider),
        -reliability,
        -successes,
        last_success,
        last_used,
        str(item.get("id", "")),
    )


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _catalog_snapshot(cfg: Dict[str, Any]) -> Dict[str, Any]:
    with _STATE_LOCK:
        return dict(_MODEL_CACHE)


def _update_catalog_cache(cfg: Dict[str, Any], all_models: List[Dict[str, Any]], force_refresh: bool = False) -> Dict[str, Any]:
    models = [item for item in all_models if item.get("enabled", True)]
    stats = _model_stats(cfg)
    for item in all_models:
        mid = item.get("id")
        if not isinstance(mid, str):
            continue
        item_stats = stats.get(mid, {})
        if isinstance(item_stats, dict):
            item["stats"] = item_stats
    models = sorted(models, key=_model_sort_key)
    name_map: Dict[str, List[str]] = {}
    for item in models:
        mid = item["id"]
        if ":" in mid:
            prefix, name = mid.split(":", 1)
            name_map.setdefault(name, []).append(prefix)
    aliases = cfg.get("aliases", {})
    disabled_ids = [item["id"] for item in all_models if not item.get("enabled", True)]
    provider_overrides = _provider_overrides(cfg)
    disabled_providers = sorted(
        provider
        for provider, override in provider_overrides.items()
        if isinstance(provider, str) and provider and isinstance(override, dict) and override.get("enabled") is False
    )
    providers: Dict[str, Dict[str, Any]] = {}
    for item in all_models:
        provider = item.get("provider")
        if not isinstance(provider, str) or not provider:
            continue
        summary = providers.setdefault(
            provider,
            {
                "id": provider,
                "label": _provider_label(provider, provider_overrides.get(provider)),
                "enabled": _provider_enabled(provider, provider_overrides),
                "kind": "gateway" if provider == "relaykit" else "provider",
                "visible_count": 0,
                "hidden_count": 0,
                "total_count": 0,
                "unhealthy_count": 0,
                "open_circuits": 0,
                "requests": 0,
                "successes": 0,
                "failures": 0,
                "reliability": 0.0,
                "last_success_at": "",
                "last_failure_at": "",
            },
        )
        summary["total_count"] += 1
        if item.get("enabled", True):
            summary["visible_count"] += 1
        else:
            summary["hidden_count"] += 1
        if str(item.get("health", "")) in {"degraded", "unhealthy"}:
            summary["unhealthy_count"] += 1
        if bool(item.get("circuit_open", False)):
            summary["open_circuits"] += 1
        item_stats = item.get("stats", {})
        if isinstance(item_stats, dict):
            summary["requests"] += int(item_stats.get("requests", 0) or 0)
            summary["successes"] += int(item_stats.get("successes", 0) or 0)
            summary["failures"] += int(item_stats.get("failures", 0) or 0)
            if item_stats.get("last_success_at"):
                summary["last_success_at"] = max(summary["last_success_at"], str(item_stats.get("last_success_at")))
            if item_stats.get("last_failure_at"):
                summary["last_failure_at"] = max(summary["last_failure_at"], str(item_stats.get("last_failure_at")))
    for summary in providers.values():
        requests = summary["requests"]
        successes = summary["successes"]
        failures = summary["failures"]
        summary["reliability"] = round((successes / (successes + failures)) if (successes + failures) else 0.0, 3)
        summary["requests"] = int(requests)
        summary["successes"] = int(successes)
        summary["failures"] = int(failures)
    provider_list = sorted(providers.values(), key=lambda x: (x["id"] != "hermes", -x["reliability"], x["id"]))
    now = time.time()
    with _STATE_LOCK:
        _MODEL_CACHE.update(
            {
                "ts": now,
                "refresh_started": _MODEL_CACHE.get("refresh_started", now) if force_refresh else _MODEL_CACHE.get("refresh_started", now),
                "refresh_finished": now,
                "refreshing": False,
                "error": None,
                "models": models,
                "all_models": all_models,
                "map": name_map,
                "aliases": aliases,
                "disabled_ids": disabled_ids,
                "disabled_providers": disabled_providers,
                "providers": provider_list,
            }
        )
        return dict(_MODEL_CACHE)


def _mark_catalog_refresh_start() -> bool:
    with _STATE_LOCK:
        if _MODEL_CACHE.get("refreshing"):
            return False
        _MODEL_CACHE["refreshing"] = True
        _MODEL_CACHE["refresh_started"] = time.time()
        return True


def _refresh_catalog_thread(cfg: Dict[str, Any], force_refresh: bool = False) -> None:
    try:
        all_models = _get_model_lists(cfg, refresh=force_refresh)
        _update_catalog_cache(cfg, all_models, force_refresh=force_refresh)
    except Exception as exc:
        with _STATE_LOCK:
            _MODEL_CACHE["error"] = str(exc)
            _MODEL_CACHE["refreshing"] = False
            _MODEL_CACHE["refresh_finished"] = time.time()


def _refresh_catalog_async(cfg: Dict[str, Any], force_refresh: bool = False) -> None:
    if not _mark_catalog_refresh_start():
        return
    thread = threading.Thread(target=_refresh_catalog_thread, args=(cfg, force_refresh), daemon=True)
    thread.start()


def _get_model_lists(cfg: Dict[str, Any], refresh: bool = False) -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    seen = set()
    provider_overrides = _provider_overrides(cfg)
    overrides = _model_overrides(cfg)

    def canonicalize_model_id(model_id: str, provider: Optional[str], source: str) -> str:
        mid = str(model_id or "").strip()
        provider_name = str(provider or source or "").strip().lower()
        lower = mid.lower()
        hermes_aliases = {
            "hermes",
            "hermes:auto",
            "hermes/hermes",
            "hermes/hermes:auto",
            "hermes:hermes",
            "hermes:hermes:auto",
        }
        if provider_name == "hermes" or lower in hermes_aliases:
            return "hermes:auto"
        return mid

    def add_model(model_id: str, owner: str, source: str, provider: Optional[str] = None, kind: str = "model") -> None:
        model_id = canonicalize_model_id(model_id, provider, source)
        if model_id in seen:
            return
        seen.add(model_id)
        provider_name = provider or source
        provider_enabled = _provider_enabled(provider_name, provider_overrides)
        override = overrides.get(model_id)
        enabled = provider_enabled and _model_enabled(model_id, overrides)
        runtime = _runtime_entry(model_id)
        circuit_open = _is_circuit_open(model_id)
        health_status = str(runtime.get("health_status", "unknown") or "unknown")
        record: Dict[str, Any] = {
            "id": model_id,
            "object": "model",
            "created": int(time.time()),
            "owned_by": owner,
            "source": source,
            "provider": provider_name,
            "kind": kind,
            "enabled": enabled,
            "label": _display_label(model_id, override),
            "health": health_status,
            "circuit_open": circuit_open,
        }
        if isinstance(override, dict) and isinstance(override.get("notes"), str):
            record["notes"] = override["notes"]
        data.append(record)

    def add_relaykit_aliases() -> None:
        # Expose gateway-scoped aliases so downstream tools can target RelayKit
        # without colliding with their own built-in provider names.
        for item in list(data):
            mid = item["id"]
            if mid.startswith("relaykit:"):
                continue
            add_model(f"relaykit:{mid}", "relaykit", "relaykit", provider="relaykit", kind="gateway")

    # Put Hermes first so clients that pick the first available model
    # naturally land on the orchestrator path.
    add_model("hermes", "hermes", "hermes", provider="hermes")
    add_model("hermes:auto", "hermes", "hermes", provider="hermes")
    add_model("hermes/hermes", "hermes", "hermes", provider="hermes")
    add_model("hermes/hermes:auto", "hermes", "hermes", provider="hermes")

    # OpenCode (if available)
    for m in backend.list_models(refresh=refresh):
        if "/" not in m:
            continue
        provider, model = m.split("/", 1)
        if provider == "anthropic":
            model_id = f"claude:{model}"
        elif provider == "google":
            model_id = f"gemini:{model}"
        elif provider == "openai":
            model_id = f"codex:{model}"
        elif provider == "ollama":
            model_id = f"ollama:{model}"
        else:
            model_id = f"{provider}:{model}"
        add_model(model_id, provider, provider, provider=provider)

    # CLI model hints (config + env)
    cfg_models = cfg.get("providers", {})
    for model in cfg_models.get("claude", {}).get("models", []):
        add_model(f"claude:{model}", "claude", "claude", provider="claude")
    for model in cfg_models.get("gemini", {}).get("models", []):
        add_model(f"gemini:{model}", "gemini", "gemini", provider="gemini")
    for model in cfg_models.get("codex", {}).get("models", []):
        add_model(f"codex:{model}", "openai", "codex", provider="codex")
    for model in cfg_models.get("hermes", {}).get("models", []):
        add_model(f"hermes:{model}", "hermes", "hermes", provider="hermes")

    for model in claude_cli.list_models():
        add_model(f"claude:{model}", "claude", "claude", provider="claude")
    for model in gemini_cli.list_models():
        add_model(f"gemini:{model}", "gemini", "gemini", provider="gemini")
    for model in codex_cli.list_models():
        add_model(f"codex:{model}", "openai", "codex", provider="codex")
    for model in hermes_cli.list_models():
        add_model(f"hermes:{model}", "hermes", "hermes", provider="hermes")

    # Add auto models for installed CLIs so apps can work without a model list.
    if claude_cli.available:
        add_model("claude:auto", "claude", "claude", provider="claude")
    if gemini_cli.available:
        add_model("gemini:auto", "gemini", "gemini", provider="gemini")
    if codex_cli.available:
        add_model("codex:auto", "openai", "codex", provider="codex")
    if hermes_cli.available:
        add_model("hermes:auto", "hermes", "hermes", provider="hermes")
    # Ollama local models
    ollama_cfg = cfg.get("providers", {}).get("ollama", {})
    if ollama_cfg.get("enabled", True):
        base_url = ollama_cfg.get("base_url", "http://localhost:11434")
        ollama_backend = OllamaBackend(base_url=base_url)
        for model in ollama_backend.list_models():
            add_model(f"ollama:{model}", "ollama", "ollama", provider="ollama")

    # OpenAI-compatible local endpoints (LM Studio, etc.)
    for item in _compat_backends(cfg):
        for model in item["backend"].list_models():
            add_model(f"{item['name']}:{model}", item["name"], item["name"], provider=item["name"])

    use_settings = os.environ.get("RELAYKIT_USE_SETTINGS", "0") == "1" or cfg.get("settings", {}).get("use_settings", False)
    if use_settings:
        gemini_model = gemini_model_from_settings()
        if gemini_model:
            add_model(f"gemini:{gemini_model}", "gemini", "gemini", provider="gemini")
        claude_model = claude_model_from_settings()
        if claude_model:
            add_model(f"claude:{claude_model}", "claude", "claude", provider="claude")

    # Aliases
    aliases = cfg.get("aliases", {})
    for alias, target in aliases.items():
        if isinstance(alias, str) and isinstance(target, str):
            add_model(alias, "alias", "alias", provider="alias")

    add_relaykit_aliases()

    return data


def _get_catalog(cfg: Dict[str, Any], force_refresh: bool = False) -> Dict[str, Any]:
    now = time.time()
    with _STATE_LOCK:
        cached = dict(_MODEL_CACHE)
        age = now - float(_MODEL_CACHE.get("ts", 0.0) or 0.0)
        has_models = bool(_MODEL_CACHE.get("models"))
        is_fresh = has_models and age < _DISCOVERY_TTL_SECONDS
        refreshing = bool(_MODEL_CACHE.get("refreshing"))

    if force_refresh:
        _refresh_catalog_thread(cfg, force_refresh=True)
        with _STATE_LOCK:
            return dict(_MODEL_CACHE)

    if is_fresh:
        return cached

    if has_models:
        if not refreshing:
            _refresh_catalog_async(cfg, force_refresh=False)
        return cached

    _refresh_catalog_thread(cfg, force_refresh=False)
    with _STATE_LOCK:
        return dict(_MODEL_CACHE)


def _messages_to_prompt(messages: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if isinstance(content, list):
            # OpenAI multi-part content
            parts = []
            for item in content:
                if item.get("type") == "text":
                    parts.append(item.get("text", ""))
            content = "\n".join(parts)
        lines.append(f"{role.upper()}: {content}")
    lines.append("ASSISTANT:")
    return "\n\n".join(lines)


def _normalize_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if isinstance(content, list):
            parts = []
            for item in content:
                if item.get("type") == "text":
                    parts.append(item.get("text", ""))
            content = "\n".join(parts)
        if not isinstance(content, str):
            content = str(content)
        normalized.append({"role": role, "content": content})
    return normalized


def _build_hermes_system_prompt(catalog: Dict[str, Any]) -> str:
    """
    Build a short, explicit catalog prompt for Hermes.

    Hermes needs to know about RelayKit's current model inventory so it does
    not answer from its own default provider state. We keep relaykit:* aliases
    out of the summary because they mirror the same underlying entries.
    """
    groups: Dict[str, List[str]] = {}
    seen = set()
    for item in catalog.get("models", []):
        mid = item.get("id", "")
        if not isinstance(mid, str) or not mid.strip():
            continue
        if mid.startswith("relaykit:"):
            continue
        if mid in seen:
            continue
        seen.add(mid)
        if ":" in mid:
            prefix, name = mid.split(":", 1)
        else:
            prefix, name = "other", mid
        groups.setdefault(prefix, []).append(name)

    lines = [
        "You are Hermes running behind RelayKit.",
        "Use the RelayKit catalog below when answering questions about what models you can access.",
        "Do not answer based only on your default provider configuration.",
        "When the user asks a direct question about access or model count, answer in one short sentence only.",
        "Do not repeat yourself, do not use bullet points, and do not invent extra explanation unless the user asks for it.",
        "RelayKit model catalog:",
    ]
    total = 0
    for prefix in sorted(groups):
        names = sorted(set(groups[prefix]))
        total += len(names)
        lines.append(f"- {prefix}: {', '.join(names)}")
    lines.append(f"Total visible model entries: {total}")
    lines.append("When the user asks how many models you have access to, answer using this RelayKit catalog.")
    return "\n".join(lines)


def _route_relaykit_namespace(model_name: str, catalog: Dict[str, Any]) -> tuple[str, str]:
    """
    Route a RelayKit-scoped model name to the underlying provider.

    RelayKit advertises models using a `relaykit:` namespace so external
    clients can target the gateway explicitly without colliding with their own
    built-in provider routing. When the model name has an explicit inner
    provider prefix, unwrap it. Otherwise, map well-known gateway names like
    `hermes` back to the matching backend with its own auto/default model.
    """
    value = (model_name or "").strip()
    if not value:
        return "openai", "auto"

    if ":" in value or "/" in value:
        inner_prefix, inner_model = to_model_prefix(value)
        return inner_prefix, inner_model

    lowered = value.lower()
    if lowered in {"claude", "anthropic"}:
        return "claude", "auto"
    if lowered in {"gemini", "google"}:
        return "gemini", "auto"
    if lowered in {"codex", "openai"}:
        return "codex", "auto"
    if lowered == "hermes":
        return "hermes", "auto"
    if lowered == "ollama":
        return "ollama", "auto"

    candidates = catalog.get("map", {}).get(value, [])
    if len(candidates) == 1:
        return candidates[0], value

    return "openai", value


def _canonical_route_model(model: str, catalog: Dict[str, Any]) -> str:
    """
    Normalize a requested model id to the underlying provider model id.

    Example:
      relaykit:codex:auto -> codex:auto
      relaykit:hermes     -> hermes:auto
    """
    value = (model or "").strip()
    if not value:
        return value
    prefix, model_name = to_model_prefix(value)
    if prefix == "relaykit":
        inner_prefix, inner_model = _route_relaykit_namespace(model_name, catalog)
        return f"{inner_prefix}:{inner_model}"
    return value


def _extract_text_from_events(events: List[Dict[str, Any]]) -> str:
    # Heuristic: take last event with a text field
    for event in reversed(events):
        for key in ("text", "content", "message", "output"):
            val = event.get(key)
            if isinstance(val, str) and val.strip():
                return val
        if isinstance(event.get("data"), dict):
            data = event["data"]
            for key in ("text", "content", "message", "output"):
                val = data.get(key)
                if isinstance(val, str) and val.strip():
                    return val
    return ""


def _wrap_stream(model: str, chunks: Iterable[str]) -> Generator[str, None, None]:
    for chunk in chunks:
        payload = {
            "id": "relaykit-stream",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [{"delta": {"content": chunk}, "index": 0, "finish_reason": None}],
        }
        yield f"data: {json.dumps(payload)}\n\n"
    payload = {
        "id": "relaykit-stream",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}],
    }
    yield f"data: {json.dumps(payload)}\n\n"
    yield "data: [DONE]\n\n"


def _simple_response(model: str, text: str) -> JSONResponse:
    resp = {
        "id": "relaykit",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
    }
    return JSONResponse(resp)


def _log_request(request: Request, endpoint: str, model: str) -> None:
    _audit_log({"endpoint": endpoint, "model": model, "key": getattr(request.state, "key_label", None)})


def _extract_bearer_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[len("Bearer "):].strip()
    return ""


def _is_disabled_catalog_entry(model: str, catalog: Dict[str, Any]) -> bool:
    model = _canonical_route_model(model, catalog)
    if model in set(catalog.get("disabled_ids", [])):
        return True
    prefix, _ = to_model_prefix(model)
    if prefix in set(catalog.get("disabled_providers", [])):
        return True
    return False


def _record_model_activity(
    model: str,
    *,
    success: bool,
    is_test: bool = False,
    output: str = "",
    error: str = "",
    latency_ms: Optional[float] = None,
) -> None:
    cfg = load_config()
    ui = cfg.setdefault("ui", {})
    stats = ui.setdefault("model_stats", {})
    if not isinstance(stats, dict):
        stats = {}
        ui["model_stats"] = stats
    entry = stats.setdefault(
        model,
        {
            "requests": 0,
            "successes": 0,
            "failures": 0,
            "last_used_at": "",
            "last_success_at": "",
            "last_failure_at": "",
            "last_test_at": "",
            "last_test_success_at": "",
            "last_test_failure_at": "",
            "last_output": "",
            "last_error": "",
            "last_latency_ms": 0.0,
            "avg_latency_ms": 0.0,
            "latency_samples": 0,
        },
    )
    if not isinstance(entry, dict):
        entry = {}
        stats[model] = entry
    entry["requests"] = int(entry.get("requests", 0) or 0) + 1
    entry["last_used_at"] = _now_iso()
    if success:
        entry["successes"] = int(entry.get("successes", 0) or 0) + 1
        entry["last_success_at"] = _now_iso()
        if output:
            entry["last_output"] = output[:500]
        if is_test:
            entry["last_test_at"] = _now_iso()
            entry["last_test_success_at"] = _now_iso()
            entry["last_test_failure_at"] = ""
    else:
        entry["failures"] = int(entry.get("failures", 0) or 0) + 1
        entry["last_failure_at"] = _now_iso()
        if error:
            entry["last_error"] = error[:500]
        if is_test:
            entry["last_test_at"] = _now_iso()
            entry["last_test_failure_at"] = _now_iso()
            entry["last_test_success_at"] = ""
    if latency_ms is not None:
        latency = max(0.0, float(latency_ms))
        prev_avg = _to_float(entry.get("avg_latency_ms"), 0.0)
        prev_n = max(0, _to_int(entry.get("latency_samples"), 0))
        new_n = prev_n + 1
        new_avg = latency if prev_n <= 0 else ((prev_avg * prev_n) + latency) / new_n
        entry["last_latency_ms"] = round(latency, 2)
        entry["avg_latency_ms"] = round(new_avg, 2)
        entry["latency_samples"] = new_n
    save_config(cfg)
    with _STATE_LOCK:
        _MODEL_CACHE["ts"] = 0.0


def _provider_summary_by_id(catalog: Dict[str, Any], provider: str) -> Dict[str, Any]:
    for item in catalog.get("providers", []):
        if isinstance(item, dict) and item.get("id") == provider:
            return item
    return {}


def _candidate_breakdown(candidate: str, catalog: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    routing = _routing_config(cfg)
    stats = _model_stats(cfg).get(candidate, {})
    if not isinstance(stats, dict):
        stats = {}
    prefix, _ = to_model_prefix(candidate)
    provider_summary = _provider_summary_by_id(catalog, prefix)
    model_reliability = _reliability_score(stats)
    provider_reliability = _clamp(_to_float(provider_summary.get("reliability"), 0.0), 0.0, 1.0)
    reliability = _clamp((0.72 * model_reliability) + (0.28 * provider_reliability), 0.0, 1.0)

    runtime = _runtime_entry(candidate)
    runtime_ewma = _to_float(runtime.get("ewma_latency_ms"), 0.0)
    recorded_avg = _to_float(stats.get("avg_latency_ms"), 0.0)
    observed_latency_ms = runtime_ewma if runtime_ewma > 0 else recorded_avg
    if observed_latency_ms <= 0:
        observed_latency_ms = 1200.0
    latency_penalty = _latency_penalty_ms(observed_latency_ms)
    cost_penalty = _estimate_model_cost_penalty(candidate)
    circuit_open = _is_circuit_open(candidate)

    health_status = str(runtime.get("health_status", "unknown") or "unknown")
    health_penalty = 0.0
    if health_status == "degraded":
        health_penalty = 0.10
    elif health_status == "unhealthy":
        health_penalty = 0.30
    if circuit_open:
        health_penalty += 0.55

    weights = routing["weights"]
    route_score = (
        (weights["reliability"] * (1.0 - reliability))
        + (weights["latency"] * latency_penalty)
        + (weights["cost"] * cost_penalty)
        + health_penalty
    )
    route_score = round(route_score, 6)
    return {
        "route_score": route_score,
        "reliability": round(reliability, 6),
        "model_reliability": round(model_reliability, 6),
        "provider_reliability": round(provider_reliability, 6),
        "latency_ms": round(observed_latency_ms, 2),
        "latency_penalty": round(latency_penalty, 6),
        "cost_penalty": round(cost_penalty, 6),
        "health_penalty": round(health_penalty, 6),
        "circuit_open": circuit_open,
        "health_status": health_status,
        "consecutive_failures": int(runtime.get("consecutive_failures", 0) or 0),
        "breaker_open_until": _circuit_open_until(candidate),
        "provider_priority": _provider_priority(prefix),
        "provider_requests": int(provider_summary.get("requests", 0) or 0),
        "model_successes": int(stats.get("successes", 0) or 0),
    }


def _candidate_score(candidate: str, catalog: Dict[str, Any], cfg: Dict[str, Any]) -> tuple:
    breakdown = _candidate_breakdown(candidate, catalog, cfg)
    return (
        1 if breakdown["circuit_open"] else 0,
        float(breakdown["route_score"]),
        -float(breakdown["reliability"]),
        int(breakdown["provider_requests"]),
        int(breakdown["provider_priority"]),
        candidate,
    )


def _failover_candidates(model: str, catalog: Dict[str, Any], cfg: Dict[str, Any]) -> List[str]:
    candidates: List[str] = []
    seen = set()

    def add(candidate: str) -> None:
        candidate = (candidate or "").strip()
        if not candidate or candidate in seen:
            return
        seen.add(candidate)
        candidates.append(candidate)

    add(model)
    prefix, model_name = to_model_prefix(model)
    def prioritize_requested(items: List[str]) -> List[str]:
        if model in items:
            return [model] + [c for c in items if c != model]
        return items

    if model_name == "auto":
        # Avoid recursive orchestration loops: when the requested auto model
        # is not Hermes, do not inject Hermes as a fallback candidate.
        providers = ("hermes", "claude", "gemini", "codex", "ollama")
        for provider in providers:
            if provider == "hermes" and prefix != "hermes":
                continue
            if provider != prefix:
                add(f"{provider}:auto")
        ordered = sorted(candidates, key=lambda candidate: _candidate_score(candidate, catalog, cfg))
        ordered = prioritize_requested(ordered)
        closed = [c for c in ordered if not _is_circuit_open(c)]
        if closed:
            return prioritize_requested(closed)
        return ordered

    for alt_prefix in catalog.get("map", {}).get(model_name, []):
        if alt_prefix in {prefix, "relaykit", "alias"}:
            continue
        add(f"{alt_prefix}:{model_name}")
    ordered = sorted(candidates, key=lambda candidate: _candidate_score(candidate, catalog, cfg))
    ordered = prioritize_requested(ordered)
    closed = [c for c in ordered if not _is_circuit_open(c)]
    if closed:
        return prioritize_requested(closed)
    return ordered


def _route_trace(model: str, catalog: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    model_overrides = _model_overrides(cfg)
    provider_overrides = _provider_overrides(cfg)
    candidates = _failover_candidates(model, catalog, cfg)
    trace = []
    for candidate in candidates:
        prefix, model_name = to_model_prefix(candidate)
        provider_summary = _provider_summary_by_id(catalog, prefix)
        stats = _model_stats(cfg).get(candidate, {})
        score = _candidate_score(candidate, catalog, cfg)
        breakdown = _candidate_breakdown(candidate, catalog, cfg)
        trace.append(
            {
                "model": candidate,
                "provider": prefix,
                "model_name": model_name,
                "enabled": _model_enabled(candidate, model_overrides),
                "provider_enabled": _provider_enabled(prefix, provider_overrides),
                "model_reliability": round(_reliability_score(stats), 3),
                "provider_reliability": round(float(provider_summary.get("reliability", 0.0) or 0.0), 3),
                "model_successes": int(stats.get("successes", 0) or 0),
                "model_failures": int(stats.get("failures", 0) or 0),
                "provider_requests": int(provider_summary.get("requests", 0) or 0),
                "provider_successes": int(provider_summary.get("successes", 0) or 0),
                "priority": _provider_priority(prefix),
                "route_score": breakdown["route_score"],
                "latency_ms": breakdown["latency_ms"],
                "latency_penalty": breakdown["latency_penalty"],
                "cost_penalty": breakdown["cost_penalty"],
                "health_penalty": breakdown["health_penalty"],
                "health_status": breakdown["health_status"],
                "circuit_open": breakdown["circuit_open"],
                "consecutive_failures": breakdown["consecutive_failures"],
                "breaker_open_until": breakdown["breaker_open_until"],
                "sort_key": [str(part) for part in score],
            }
        )
    return {"model": model, "candidates": trace}


async def _execute_chat_completion_attempt(
    request: Request,
    cfg: Dict[str, Any],
    catalog: Dict[str, Any],
    model: str,
    messages: List[Dict[str, Any]],
    stream: bool,
    prompt: str,
) -> Any:
    resp_model = model
    aliases = catalog.get("aliases", {})
    if model in aliases:
        model = aliases[model]
    if _is_disabled_catalog_entry(model, catalog):
        raise HTTPException(status_code=404, detail="Model is disabled")
    prefix, model_name = to_model_prefix(model)
    if prefix == "relaykit":
        prefix, model_name = _route_relaykit_namespace(model_name, catalog)
    if prefix == "hermes":
        lower_model = model_name.lower()
        if lower_model.startswith("hermes:"):
            model_name = model_name.split(":", 1)[1].strip() or "auto"
        elif lower_model.startswith("hermes/"):
            model_name = model_name.split("/", 1)[1].strip() or "auto"
    if ":" not in model:
        candidates = catalog.get("map", {}).get(model_name, [])
        if len(candidates) == 1:
            prefix = candidates[0]
    if prefix == "ollama":
        ollama_cfg = cfg.get("providers", {}).get("ollama", {})
        base_url = ollama_cfg.get("base_url", "http://localhost:11434")
        ollama_backend = OllamaBackend(base_url=base_url)
        ollama_messages = _normalize_messages(messages)
        if stream:
            return StreamingResponse(_wrap_stream(resp_model, ollama_backend.chat(model_name, ollama_messages, stream=True)), media_type="text/event-stream")
        parts = list(ollama_backend.chat(model_name, ollama_messages, stream=False))
        text = parts[0] if parts else ""
        return _simple_response(resp_model, text)

    if prefix in {"claude", "anthropic"} and claude_cli.available:
        available_models = set(claude_cli.list_models())
        if model_name not in available_models and "auto" in available_models:
            model_name = "auto"
            resp_model = "claude:auto"
        prompt = _messages_to_prompt(messages)
        if stream:
            return StreamingResponse(_wrap_stream(resp_model, claude_cli.chat(model_name, prompt, stream=True)), media_type="text/event-stream")
        parts = list(claude_cli.chat(model_name, prompt, stream=False))
        text = parts[0] if parts else ""
        return _simple_response(resp_model, text)

    if prefix in {"gemini", "google"} and gemini_cli.available:
        prompt = _messages_to_prompt(messages)
        if stream:
            return StreamingResponse(_wrap_stream(resp_model, gemini_cli.chat(model_name, prompt, stream=True)), media_type="text/event-stream")
        parts = list(gemini_cli.chat(model_name, prompt, stream=False))
        text = parts[0] if parts else ""
        return _simple_response(resp_model, text)

    if prefix in {"codex", "openai"} and codex_cli.available:
        prompt = _messages_to_prompt(messages)
        if stream:
            return StreamingResponse(_wrap_stream(resp_model, codex_cli.chat(model_name, prompt, stream=True)), media_type="text/event-stream")
        parts = list(codex_cli.chat(model_name, prompt, stream=False))
        text = parts[0] if parts else ""
        return _simple_response(resp_model, text)

    if prefix == "hermes":
        if not hermes_cli.available:
            raise HTTPException(status_code=503, detail="Hermes CLI is not installed or RELAYKIT_HERMES_COMMAND is not configured")
        prompt = _messages_to_prompt(messages)
        hermes_system_prompt = _build_hermes_system_prompt(catalog)
        relaykit_api_key = _extract_bearer_token(request)

        def _run_hermes_chat() -> List[str]:
            return list(
                hermes_cli.chat(
                    model_name,
                    prompt,
                    stream=False,
                    system_prompt=hermes_system_prompt,
                    api_key=relaykit_api_key,
                    base_url="http://127.0.0.1:11436/v1",
                )
            )

        parts = await asyncio.to_thread(_run_hermes_chat)
        text = parts[0] if parts else ""
        if stream:
            return StreamingResponse(_wrap_stream(resp_model, [text] if text else []), media_type="text/event-stream")
        return _simple_response(resp_model, text)

    compat_backends = _compat_backends(cfg)
    for item in compat_backends:
        if prefix == item["name"]:
            compat_messages = _normalize_messages(messages)
            if stream:
                return StreamingResponse(_wrap_stream(resp_model, item["backend"].chat(model_name, compat_messages, stream=True)), media_type="text/event-stream")
            parts = list(item["backend"].chat(model_name, compat_messages, stream=False))
            text = parts[0] if parts else ""
            return _simple_response(resp_model, text)

    if not backend.available:
        raise HTTPException(status_code=503, detail="No compatible backend available for this model")

    oc_model = opencode_model_id(prefix, model_name)

    def event_stream() -> Generator[str, None, None]:
        for line in backend.run_chat(oc_model, prompt, stream=True):
            try:
                event = json.loads(line)
                text = _extract_text_from_events([event])
                if text:
                    payload = {
                        "id": "relaykit-stream",
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": resp_model,
                        "choices": [{"delta": {"content": text}, "index": 0, "finish_reason": None}],
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
            except Exception:
                continue
        payload = {
            "id": "relaykit-stream",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": resp_model,
            "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}],
        }
        yield f"data: {json.dumps(payload)}\n\n"
        yield "data: [DONE]\n\n"

    if stream:
        return StreamingResponse(event_stream(), media_type="text/event-stream")

    events: List[Dict[str, Any]] = []
    for line in backend.run_chat(oc_model, prompt, stream=False):
        try:
            events.append(json.loads(line))
        except Exception:
            continue
    text = _extract_text_from_events(events)
    resp = {
        "id": "relaykit",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": resp_model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
    }
    return JSONResponse(resp)


async def _execute_embeddings_attempt(
    request: Request,
    cfg: Dict[str, Any],
    catalog: Dict[str, Any],
    model: str,
    inputs: List[str],
) -> Any:
    resp_model = model
    aliases = catalog.get("aliases", {})
    if model in aliases:
        model = aliases[model]
    if _is_disabled_catalog_entry(model, catalog):
        raise HTTPException(status_code=404, detail="Model is disabled")
    prefix, model_name = to_model_prefix(model)
    if prefix == "relaykit":
        prefix, model_name = _route_relaykit_namespace(model_name, catalog)
    if ":" not in model:
        candidates = catalog.get("map", {}).get(model_name, [])
        if len(candidates) == 1:
            prefix = candidates[0]

    if prefix == "ollama":
        ollama_cfg = cfg.get("providers", {}).get("ollama", {})
        base_url = ollama_cfg.get("base_url", "http://localhost:11434")
        ollama_backend = OllamaBackend(base_url=base_url)
        data = []
        for i, text in enumerate(inputs):
            emb = ollama_backend.embeddings(model_name, text)
            if emb is None:
                raise HTTPException(status_code=502, detail="Ollama embeddings unavailable")
            data.append({"object": "embedding", "index": i, "embedding": emb})
        return JSONResponse({"object": "list", "data": data, "model": resp_model})

    compat_backends = _compat_backends(cfg)
    for item in compat_backends:
        if prefix == item["name"]:
            emb = item["backend"].embeddings(model_name, inputs)
            if emb is None:
                raise HTTPException(status_code=502, detail="Embedding backend unavailable")
            data = [{"object": "embedding", "index": i, "embedding": e} for i, e in enumerate(emb)]
            return JSONResponse({"object": "list", "data": data, "model": resp_model})

    raise HTTPException(status_code=501, detail="Embeddings not supported for this provider")


@app.get("/health")
async def health() -> Dict[str, Any]:
    cfg = load_config()
    cli_checks = {
        "opencode": shutil.which("opencode") is not None,
        "claude": shutil.which("claude") is not None,
        "codex": shutil.which("codex") is not None,
        "gemini": shutil.which("gemini") is not None,
        "hermes": hermes_cli.available,
        "ollama": shutil.which("ollama") is not None,
    }
    compat = _compat_backends(cfg)
    use_settings = os.environ.get("RELAYKIT_USE_SETTINGS", "0") == "1" or cfg.get("settings", {}).get("use_settings", False)
    gemini_model = gemini_model_from_settings() if use_settings else None
    claude_model = claude_model_from_settings() if use_settings else None
    routing = _routing_config(cfg)
    runtime = _routing_runtime_snapshot(limit=30)
    return {
        "ok": True,
        "backend": "opencode",
        "available": backend.available,
        "cli_detected": cli_checks,
        "config_models": {
            "gemini": gemini_model,
            "claude": claude_model,
            "hermes": cfg.get("providers", {}).get("hermes", {}).get("models", []),
        },
        "compat_endpoints": [c["url"] for c in compat],
        "routing": {
            "weights": routing["weights"],
            "max_attempts_per_candidate": routing["max_attempts_per_candidate"],
            "breaker_failure_threshold": routing["breaker_failure_threshold"],
            "breaker_cooldown_seconds": routing["breaker_cooldown_seconds"],
            "probe_enabled": routing["probe_enabled"],
            "probe_interval_seconds": routing["probe_interval_seconds"],
        },
        "runtime": {
            "open_circuit_count": runtime["open_circuit_count"],
            "unhealthy_count": runtime["unhealthy_count"],
        },
    }


@app.get("/v1/models")
async def list_models() -> Dict[str, Any]:
    cfg = load_config()
    catalog = _get_catalog(cfg)
    strict_mode = _strict_mode_enabled(cfg)
    visible_models = []
    for item in catalog.get("models", []):
        if not isinstance(item, dict):
            continue
        model_id = str(item.get("id") or "")
        provider = str(item.get("provider") or item.get("source") or item.get("owned_by") or "").lower()
        kind = str(item.get("kind") or "").lower()
        if model_id.startswith("relaykit:") or provider == "relaykit" or kind == "gateway":
            continue
        if strict_mode and _is_strict_excluded_model(item):
            continue
        visible_models.append(item)
    return {"object": "list", "data": visible_models}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request) -> Any:
    body = await request.json()
    model = body.get("model")
    if not model:
        model = "hermes:auto"
    if isinstance(model, str):
        model = model.strip()
    if not model:
        model = "hermes:auto"
    if model in {"claude", "anthropic"}:
        model = "claude:auto"
    elif model in {"gemini", "google"}:
        model = "gemini:auto"
    elif model in {"codex", "openai"}:
        model = "codex:auto"
    elif model in {"hermes"}:
        model = "hermes:auto"
    messages = body.get("messages", [])
    stream = bool(body.get("stream"))
    prompt = _messages_to_prompt(messages)

    cfg = load_config()
    catalog = _get_catalog(cfg)
    candidates = _failover_candidates(_canonical_route_model(model, catalog), catalog, cfg)
    routing = _routing_config(cfg)
    attempts_per_candidate = int(routing["max_attempts_per_candidate"])
    last_error = ""
    for candidate in candidates:
        if _is_circuit_open(candidate):
            open_until = _circuit_open_until(candidate)
            last_error = f"Circuit open for {candidate} until {datetime.fromtimestamp(open_until).isoformat()}"
            continue

        for attempt_idx in range(attempts_per_candidate):
            started = time.perf_counter()
            try:
                response = await _execute_chat_completion_attempt(request, cfg, catalog, candidate, messages, stream, prompt)
                latency_ms = (time.perf_counter() - started) * 1000.0
                _register_route_success(candidate, latency_ms, cfg)
                _record_model_activity(candidate, success=True, is_test=False, latency_ms=latency_ms)
                try:
                    response.headers["X-RelayKit-Route"] = candidate
                    response.headers["X-RelayKit-Model"] = candidate
                    response.headers["X-RelayKit-Attempt"] = str(attempt_idx + 1)
                    response.headers["X-RelayKit-Latency-Ms"] = str(int(latency_ms))
                except Exception:
                    pass
                _log_request(request, "/v1/chat/completions", candidate)
                return response
            except Exception as exc:
                latency_ms = (time.perf_counter() - started) * 1000.0
                retryable = _is_retryable_error(exc)
                if isinstance(exc, HTTPException):
                    last_error = str(exc.detail)
                else:
                    last_error = str(exc)
                _register_route_failure(candidate, last_error, latency_ms, cfg)
                _record_model_activity(candidate, success=False, is_test=False, error=last_error, latency_ms=latency_ms)
                if _is_circuit_open(candidate):
                    break
                if retryable and (attempt_idx + 1) < attempts_per_candidate:
                    await asyncio.sleep(_retry_delay_seconds(attempt_idx, cfg))
                    continue
                break
    raise HTTPException(status_code=503, detail=f"No model route succeeded: {last_error or 'unknown error'}")


@app.post("/v1/embeddings")
async def embeddings(request: Request) -> Any:
    body = await request.json()
    model = body.get("model")
    if not model:
        raise HTTPException(status_code=400, detail="model is required")
    if isinstance(model, str):
        model = model.strip()
    if model in {"claude", "anthropic"}:
        model = "claude:auto"
    elif model in {"gemini", "google"}:
        model = "gemini:auto"
    elif model in {"codex", "openai"}:
        model = "codex:auto"
    raw_input = body.get("input", "")
    if isinstance(raw_input, list):
        inputs = [str(x) for x in raw_input]
    else:
        inputs = [str(raw_input)]

    cfg = load_config()
    catalog = _get_catalog(cfg)
    candidates = _failover_candidates(_canonical_route_model(model, catalog), catalog, cfg)
    routing = _routing_config(cfg)
    attempts_per_candidate = int(routing["max_attempts_per_candidate"])
    last_error = ""
    for candidate in candidates:
        if _is_circuit_open(candidate):
            open_until = _circuit_open_until(candidate)
            last_error = f"Circuit open for {candidate} until {datetime.fromtimestamp(open_until).isoformat()}"
            continue
        for attempt_idx in range(attempts_per_candidate):
            started = time.perf_counter()
            try:
                response = await _execute_embeddings_attempt(request, cfg, catalog, candidate, inputs)
                latency_ms = (time.perf_counter() - started) * 1000.0
                _register_route_success(candidate, latency_ms, cfg)
                _record_model_activity(candidate, success=True, is_test=False, latency_ms=latency_ms)
                try:
                    response.headers["X-RelayKit-Route"] = candidate
                    response.headers["X-RelayKit-Model"] = candidate
                    response.headers["X-RelayKit-Attempt"] = str(attempt_idx + 1)
                    response.headers["X-RelayKit-Latency-Ms"] = str(int(latency_ms))
                except Exception:
                    pass
                _audit_log({"endpoint": "/v1/embeddings", "model": candidate, "key": getattr(request.state, "key_label", None)})
                return response
            except Exception as exc:
                latency_ms = (time.perf_counter() - started) * 1000.0
                retryable = _is_retryable_error(exc)
                if isinstance(exc, HTTPException):
                    last_error = str(exc.detail)
                else:
                    last_error = str(exc)
                _register_route_failure(candidate, last_error, latency_ms, cfg)
                _record_model_activity(candidate, success=False, is_test=False, error=last_error, latency_ms=latency_ms)
                if _is_circuit_open(candidate):
                    break
                if retryable and (attempt_idx + 1) < attempts_per_candidate:
                    await asyncio.sleep(_retry_delay_seconds(attempt_idx, cfg))
                    continue
                break
    raise HTTPException(status_code=503, detail=f"No embedding route succeeded: {last_error or 'unknown error'}")


@app.get("/admin/config")
async def admin_get_config() -> Dict[str, Any]:
    return load_config()


@app.post("/admin/config")
async def admin_save_config(request: Request) -> Dict[str, Any]:
    body = await request.json()
    save_config(body)
    _MODEL_CACHE["ts"] = 0.0
    return {"ok": True}


@app.get("/admin/config/export")
async def admin_export_config() -> Response:
    cfg = load_config()
    payload = json.dumps(cfg, indent=2)
    return Response(
        content=payload,
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="relaykit-config.json"'},
    )


@app.post("/admin/config/import")
async def admin_import_config(request: Request) -> Dict[str, Any]:
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid config payload")
    save_config(body)
    _MODEL_CACHE["ts"] = 0.0
    return {"ok": True}


@app.get("/admin/setup")
async def admin_setup_state() -> Dict[str, Any]:
    cfg = load_config()
    setup = cfg.get("setup", {})
    if not isinstance(setup, dict):
        setup = {}
    detected = _detected_cli_snapshot()
    catalog = _get_catalog(cfg)
    providers_cfg = cfg.get("providers", {})
    if not isinstance(providers_cfg, dict):
        providers_cfg = {}

    def _count_models(provider_name: str) -> int:
        node = providers_cfg.get(provider_name, {})
        if not isinstance(node, dict):
            return 0
        models = node.get("models", [])
        if not isinstance(models, list):
            return 0
        return len(models)
    return {
        "ok": True,
        "setup": {
            "initialized": bool(setup.get("initialized", False)),
            "last_bootstrap_at": setup.get("last_bootstrap_at", ""),
            "last_bootstrap_version": setup.get("last_bootstrap_version", ""),
        },
        "detected": detected,
        "providers_configured": {
            "claude": _count_models("claude"),
            "gemini": _count_models("gemini"),
            "codex": _count_models("codex"),
            "hermes": _count_models("hermes"),
        },
        "catalog": {
            "visible_models": len(catalog.get("models", [])),
            "hidden_models": len(catalog.get("disabled_ids", [])),
            "providers": len(catalog.get("providers", [])),
        },
    }


@app.post("/admin/setup/bootstrap")
async def admin_setup_bootstrap(request: Request) -> Dict[str, Any]:
    body = await request.json()
    if not isinstance(body, dict):
        body = {}

    replace_provider_models = bool(body.get("replace_provider_models", False))
    reset_model_overrides = bool(body.get("reset_model_overrides", False))
    create_api_key_flag = bool(body.get("create_api_key", True))
    force_new_api_key = bool(body.get("force_new_api_key", False))
    run_health_probe = bool(body.get("run_health_probe", True))
    key_label = body.get("key_label", "bootstrap")
    if not isinstance(key_label, str) or not key_label.strip():
        key_label = "bootstrap"

    cfg = load_config()
    ui = cfg.setdefault("ui", {})
    if not isinstance(ui, dict):
        ui = {}
        cfg["ui"] = ui
    if not isinstance(ui.get("scan_ports"), list) or not ui.get("scan_ports"):
        ui["scan_ports"] = [1234, 3000, 5000, 8000, 8080, 3210]
    if reset_model_overrides:
        ui["model_overrides"] = {}
        ui["provider_overrides"] = {}

    detected = _detected_cli_snapshot()
    seeded_counts = _seed_provider_models(cfg, detected, replace=replace_provider_models)

    setup = cfg.setdefault("setup", {})
    if not isinstance(setup, dict):
        setup = {}
        cfg["setup"] = setup
    setup["initialized"] = True
    setup["last_bootstrap_at"] = _now_iso()
    setup["last_bootstrap_version"] = "v1"
    setup["seeded_counts"] = seeded_counts
    setup["detected_available"] = {
        name: bool((entry or {}).get("available", False))
        for name, entry in detected.items()
    }

    save_config(cfg)

    created_key = ""
    if create_api_key_flag:
        existing_keys = list_keys(cfg)
        if force_new_api_key or not existing_keys:
            created_key = create_key(cfg, key_label)

    _MODEL_CACHE["ts"] = 0.0
    catalog = _get_catalog(cfg, force_refresh=True)

    if run_health_probe:
        try:
            _run_health_probe_cycle(cfg)
        except Exception:
            pass

    return {
        "ok": True,
        "bootstrap": {
            "initialized": True,
            "last_bootstrap_at": setup.get("last_bootstrap_at", ""),
            "created_api_key": bool(created_key),
            "api_key": created_key,
            "seeded_counts": seeded_counts,
        },
        "detected": detected,
        "catalog": {
            "visible_models": len(catalog.get("models", [])),
            "hidden_models": len(catalog.get("disabled_ids", [])),
            "providers": len(catalog.get("providers", [])),
        },
        "routing_runtime": _routing_runtime_snapshot(limit=80),
    }


@app.get("/admin/catalog")
async def admin_get_catalog() -> Dict[str, Any]:
    cfg = load_config()
    catalog = _get_catalog(cfg)
    return {
        "models": catalog.get("models", []),
        "all_models": catalog.get("all_models", []),
        "providers": catalog.get("providers", []),
        "strict_mode": _strict_mode_enabled(cfg),
        "overrides": _model_overrides(cfg),
        "provider_overrides": _provider_overrides(cfg),
        "model_stats": _model_stats(cfg),
        "scan_ports": cfg.get("ui", {}).get("scan_ports", []),
        "cli_detected": {
            "opencode": backend.available,
            "claude": claude_cli.available,
            "gemini": gemini_cli.available,
            "codex": codex_cli.available,
            "hermes": hermes_cli.available,
            "ollama": shutil.which("ollama") is not None,
        },
    }


@app.post("/admin/catalog")
async def admin_save_catalog(request: Request) -> Dict[str, Any]:
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid catalog payload")
    cfg = load_config()
    ui = cfg.setdefault("ui", {})
    if body.get("reset") is True:
        ui["model_overrides"] = {}
        ui["provider_overrides"] = {}
        if "scan_ports" not in body:
            ui["scan_ports"] = [1234, 3000, 5000, 8000, 8080, 3210]
    if "model_overrides" in body:
        overrides = body.get("model_overrides")
        ui["model_overrides"] = overrides if isinstance(overrides, dict) else {}
    if "provider_overrides" in body:
        overrides = body.get("provider_overrides")
        ui["provider_overrides"] = overrides if isinstance(overrides, dict) else {}
    if "scan_ports" in body:
        scan_ports = body.get("scan_ports")
        ui["scan_ports"] = scan_ports if isinstance(scan_ports, list) else ui.get("scan_ports", [])
    if "strict_mode" in body:
        ui["strict_mode"] = bool(body.get("strict_mode"))
    save_config(cfg)
    _MODEL_CACHE["ts"] = 0.0
    return {"ok": True, "catalog": await admin_get_catalog()}


@app.post("/admin/catalog/refresh")
async def admin_refresh_catalog() -> Dict[str, Any]:
    cfg = load_config()
    _MODEL_CACHE["ts"] = 0.0
    catalog = _get_catalog(cfg, force_refresh=True)
    return {
        "ok": True,
        "models": catalog.get("models", []),
        "all_models": catalog.get("all_models", []),
        "providers": catalog.get("providers", []),
        "strict_mode": _strict_mode_enabled(cfg),
        "overrides": _model_overrides(cfg),
        "provider_overrides": _provider_overrides(cfg),
        "model_stats": _model_stats(cfg),
        "scan_ports": cfg.get("ui", {}).get("scan_ports", []),
        "cli_detected": {
            "opencode": backend.available,
            "claude": claude_cli.available,
            "gemini": gemini_cli.available,
            "codex": codex_cli.available,
            "hermes": hermes_cli.available,
            "ollama": shutil.which("ollama") is not None,
        },
    }


@app.get("/admin/diagnostics")
async def admin_diagnostics() -> Dict[str, Any]:
    cfg = load_config()
    catalog = _get_catalog(cfg)
    providers = catalog.get("providers", [])
    routing = _routing_config(cfg)
    runtime = _routing_runtime_snapshot(limit=200)
    return {
        "ok": True,
        "relaykit": {
            "base_url": "http://127.0.0.1:11436/v1",
            "ui_url": "http://127.0.0.1:11436/ui",
            "catalog_age_seconds": max(0, int(time.time() - catalog.get("ts", 0.0))),
            "catalog_ttl_seconds": _DISCOVERY_TTL_SECONDS,
            "refreshing": bool(catalog.get("refreshing", False)),
            "refresh_error": catalog.get("error"),
            "visible_models": len(catalog.get("models", [])),
            "hidden_models": len(catalog.get("disabled_ids", [])),
            "providers": len(providers),
            "relaykit_gateway_entries": len([m for m in catalog.get("all_models", []) if m.get("kind") == "gateway"]),
        },
        "providers": providers,
        "cli_detected": {
            "opencode": backend.available,
            "claude": claude_cli.available,
            "gemini": gemini_cli.available,
            "codex": codex_cli.available,
            "hermes": hermes_cli.available,
            "ollama": shutil.which("ollama") is not None,
        },
        "scan_ports": cfg.get("ui", {}).get("scan_ports", []),
        "provider_overrides": _provider_overrides(cfg),
        "model_overrides": _model_overrides(cfg),
        "model_stats": _model_stats(cfg),
        "routing": routing,
        "routing_runtime": runtime,
    }


@app.get("/admin/routing")
async def admin_get_routing() -> Dict[str, Any]:
    cfg = load_config()
    return {
        "ok": True,
        "routing": _routing_config(cfg),
        "routing_runtime": _routing_runtime_snapshot(limit=200),
    }


@app.post("/admin/routing")
async def admin_save_routing(request: Request) -> Dict[str, Any]:
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid routing payload")

    cfg = load_config()
    routing = cfg.get("routing", {})
    if not isinstance(routing, dict):
        routing = {}
    cfg["routing"] = routing

    if "weights" in body:
        weights_in = body.get("weights")
        if not isinstance(weights_in, dict):
            raise HTTPException(status_code=400, detail="weights must be an object")
        weights = routing.get("weights", {})
        if not isinstance(weights, dict):
            weights = {}
        for key in ("reliability", "latency", "cost"):
            if key in weights_in:
                weights[key] = _clamp(_to_float(weights_in.get(key), weights.get(key, 0.0)), 0.0, 1.0)
        routing["weights"] = weights

    if "breaker_failure_threshold" in body:
        routing["breaker_failure_threshold"] = _clamp(
            _to_int(body.get("breaker_failure_threshold"), 3), 1, 20
        )
    if "max_attempts_per_candidate" in body:
        routing["max_attempts_per_candidate"] = _clamp(
            _to_int(body.get("max_attempts_per_candidate"), 2), 1, 8
        )
    if "probe_interval_seconds" in body:
        routing["probe_interval_seconds"] = _clamp(
            _to_int(body.get("probe_interval_seconds"), 90), 20, 3600
        )

    resolved = _routing_config(cfg)
    cfg["routing"] = {
        "weights": dict(resolved["weights"]),
        "max_attempts_per_candidate": int(resolved["max_attempts_per_candidate"]),
        "retry_backoff_ms": int(resolved["retry_backoff_ms"]),
        "breaker_failure_threshold": int(resolved["breaker_failure_threshold"]),
        "breaker_cooldown_seconds": int(resolved["breaker_cooldown_seconds"]),
        "latency_ewma_alpha": float(resolved["latency_ewma_alpha"]),
        "probe_enabled": bool(resolved["probe_enabled"]),
        "probe_interval_seconds": int(resolved["probe_interval_seconds"]),
        "probe_model_timeout_seconds": int(resolved["probe_model_timeout_seconds"]),
        "probe_mode": str(resolved["probe_mode"]),
    }
    save_config(cfg)

    return {
        "ok": True,
        "routing": resolved,
        "routing_runtime": _routing_runtime_snapshot(limit=200),
    }


@app.post("/admin/test")
async def admin_test_model(request: Request) -> Dict[str, Any]:
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid test payload")
    model = body.get("model")
    prompt = body.get("prompt") or "Say ok"
    if not isinstance(model, str) or not model.strip():
        raise HTTPException(status_code=400, detail="model is required")
    if not isinstance(prompt, str) or not prompt.strip():
        prompt = "Say ok"
    cfg = load_config()
    catalog = _get_catalog(cfg)
    if _is_disabled_catalog_entry(model, catalog):
        raise HTTPException(status_code=404, detail="Model is disabled")
    started = time.perf_counter()
    try:
        response = await _execute_chat_completion_attempt(
            request,
            cfg,
            catalog,
            model,
            [{"role": "user", "content": prompt}],
            False,
            _messages_to_prompt([{"role": "user", "content": prompt}]),
        )
        text = ""
        if isinstance(response, JSONResponse):
            payload = json.loads(response.body.decode("utf-8"))
            text = payload.get("choices", [{}])[0].get("message", {}).get("content", "") or ""
        latency_ms = (time.perf_counter() - started) * 1000.0
        _register_route_success(model, latency_ms, cfg)
        _record_model_activity(model, success=True, is_test=True, output=text, latency_ms=latency_ms)
        return {"ok": True, "model": model, "text": text}
    except Exception as exc:
        latency_ms = (time.perf_counter() - started) * 1000.0
        _register_route_failure(model, str(exc), latency_ms, cfg)
        _record_model_activity(model, success=False, is_test=True, error=str(exc), latency_ms=latency_ms)
        raise


@app.get("/admin/route")
async def admin_route_trace(model: str) -> Dict[str, Any]:
    cfg = load_config()
    catalog = _get_catalog(cfg)
    if not isinstance(model, str) or not model.strip():
        raise HTTPException(status_code=400, detail="model is required")
    requested_model = model.strip()
    canonical_model = _canonical_route_model(requested_model, catalog)
    trace = _route_trace(canonical_model, catalog, cfg)
    return {"ok": True, "requested_model": requested_model, "canonical_model": canonical_model, **trace}


@app.get("/admin/keys")
async def admin_list_keys() -> Dict[str, Any]:
    cfg = load_config()
    keys = [{"label": k.get("label"), "created": k.get("created")} for k in list_keys(cfg)]
    return {"keys": keys}


@app.post("/admin/keys")
async def admin_create_key(request: Request) -> Dict[str, Any]:
    cfg = load_config()
    body = await request.json()
    label = body.get("label") or "app"
    key = create_key(cfg, label)
    return {"key": key}


@app.get("/ui")
async def ui() -> HTMLResponse:
    html = """<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>RelayKit</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #141414;
      --panel: #1b1b1b;
      --surface: #242424;
      --line: rgba(255,255,255,0.12);
      --ink: #f2f2f2;
      --muted: #b8b8b8;
      --chip: #2a2a2a;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 18px;
      font-family: \"SF Pro Text\", \"Segoe UI\", -apple-system, BlinkMacSystemFont, sans-serif;
      font-size: 16px;
      line-height: 1.45;
      color: var(--ink);
      background: linear-gradient(180deg, #161616 0%, var(--bg) 100%);
    }
    .shell { max-width: 1200px; margin: 0 auto; }
    h1 { margin: 0; font-size: clamp(30px, 3vw, 38px); line-height: 1.1; letter-spacing: -0.02em; }
    h2 { margin: 0; font-size: 22px; line-height: 1.2; letter-spacing: -0.01em; }
    h3 { margin: 0; font-size: 16px; line-height: 1.2; }
    p { margin: 0; }

    .hero {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 14px;
      align-items: end;
      margin-bottom: 14px;
    }
    .subtitle {
      margin-top: 6px;
      font-size: 15px;
      color: var(--muted);
      max-width: 760px;
      line-height: 1.4;
    }
    .status-pill {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 9px 13px;
      background: #2a2a2a;
      color: var(--ink);
      font-size: 13px;
      white-space: nowrap;
    }

    .card {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: var(--panel);
    }
    .tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 16px;
    }
    .tab-btn {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 9px 13px;
      background: var(--surface);
      color: var(--ink);
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
    }
    .tab-btn.active {
      background: #ededed;
      border-color: #ededed;
      color: #111;
    }
    .panel { display: none; }
    .panel.active { display: block; }

    .section-note {
      margin-top: 6px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.4;
    }

    .chip-row {
      margin-top: 10px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .chip {
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--chip);
      padding: 7px 11px;
      font-size: 13px;
      color: var(--muted);
    }
    .chip strong { color: var(--ink); }

    .controls {
      margin-top: 10px;
      display: grid;
      grid-template-columns: minmax(220px, 2fr) minmax(160px, 1fr) auto auto auto auto;
      gap: 8px;
      align-items: center;
    }

    input[type=\"text\"], select, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: var(--surface);
      color: var(--ink);
      padding: 10px 12px;
      font-size: 14px;
      line-height: 1.35;
    }
    textarea {
      min-height: 250px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 13px;
    }

    .btn {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #f0f0f0;
      color: #101010;
      padding: 9px 12px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
    }
    .btn.secondary { background: #2b2b2b; color: var(--ink); }
    .btn.ghost { background: transparent; color: var(--ink); }
    .btn-row {
      margin-top: 10px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .model-list-head { display: none; }
    .models-grid {
      margin-top: 8px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .model-card {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: var(--surface);
      padding: 10px 12px;
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
      align-items: start;
    }
    .model-card.disabled { opacity: 0.55; }
    .model-top {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 10px;
    }
    .model-main {
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 4px;
    }
    .model-title {
      font-size: 15px;
      line-height: 1.24;
      font-weight: 700;
      word-break: break-word;
    }
    .model-meta {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      align-items: center;
    }
    .provider-badge {
      display: inline-flex;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #262626;
      padding: 2px 7px;
      font-size: 12px;
      color: var(--ink);
      width: fit-content;
    }
    .model-settings {
      display: none;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      border-top: 1px solid var(--line);
      padding-top: 8px;
      margin-top: 2px;
    }
    .model-card.settings-open .model-settings {
      display: flex;
    }
    .model-toggle {
      display: inline-flex;
      gap: 6px;
      align-items: center;
      font-size: 13px;
      color: #d2d2d2;
      white-space: nowrap;
    }
    .model-label {
      padding: 7px 9px;
      font-size: 14px;
      line-height: 1.25;
      border-radius: 10px;
      min-width: 260px;
      flex: 1 1 320px;
    }
    .model-stats {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
    }
    .model-health-line {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .model-actions {
      display: flex;
      gap: 6px;
      align-items: center;
      justify-content: flex-end;
    }
    .icon-btn {
      border: 1px solid var(--line);
      border-radius: 9px;
      min-width: 30px;
      height: 30px;
      padding: 0 8px;
      background: #2b2b2b;
      color: var(--ink);
      font-size: 13px;
      line-height: 1;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      cursor: pointer;
    }
    .icon-btn.secondary { background: #333; }
    .icon-btn.ghost { background: transparent; }
    .icon-btn.active { background: #f0f0f0; color: #111; }
    .icon-btn-label {
      margin-left: 4px;
      font-size: 11px;
      color: var(--muted);
    }
    .test-status {
      margin-top: 0;
      color: var(--muted);
      font-size: 12px;
      min-height: 1.2em;
      padding-left: 2px;
    }

    .provider-grid {
      margin-top: 10px;
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    .provider-card {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: var(--surface);
      padding: 10px 12px;
    }
    .provider-card.disabled { opacity: 0.55; }
    .provider-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin-bottom: 8px;
      font-size: 16px;
      font-weight: 700;
    }
    .provider-meta {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }

    .layout-two {
      display: grid;
      grid-template-columns: 1.15fr 1fr;
      gap: 10px;
    }
    .note {
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.4;
    }
    .routing-box {
      margin-top: 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--surface);
      padding: 12px;
    }
    .routing-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(170px, 1fr));
      gap: 10px;
    }
    .routing-field {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .routing-field span {
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      font-weight: 700;
    }
    .routing-field input {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #202020;
      color: var(--ink);
      padding: 10px 12px;
      font-size: 14px;
      line-height: 1.2;
    }
    .routing-side {
      display: grid;
      gap: 12px;
    }
    pre {
      margin: 0;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: var(--surface);
      color: #ddd;
      font-size: 13px;
      overflow: auto;
      max-height: 360px;
      white-space: pre-wrap;
    }
    code {
      background: #2a2a2a;
      border-radius: 6px;
      padding: 2px 6px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
    }
    .empty-state {
      margin-top: 10px;
      border: 1px dashed var(--line);
      border-radius: 10px;
      padding: 12px;
      color: var(--muted);
      font-size: 13px;
    }
    .setup-grid {
      margin-top: 10px;
      display: grid;
      grid-template-columns: repeat(2, minmax(240px, 1fr));
      gap: 10px;
    }
    .setup-card {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: var(--surface);
      padding: 12px;
      display: grid;
      gap: 8px;
    }
    .setup-card h3 {
      margin: 0;
      font-size: 15px;
      color: #d4d4d4;
    }
    .setup-list {
      margin: 0;
      padding: 0;
      display: grid;
      gap: 6px;
      list-style: none;
      font-size: 14px;
      line-height: 1.4;
      color: var(--muted);
    }
    .setup-row {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      align-items: center;
    }
    .setup-row code {
      flex: 1 1 auto;
      min-width: 180px;
    }
    .copy-btn {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #2b2b2b;
      color: var(--ink);
      padding: 6px 9px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
    }
    .setup-status {
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
      min-height: 1.2em;
    }
    .modal-backdrop {
      position: fixed;
      inset: 0;
      display: none;
      align-items: center;
      justify-content: center;
      background: rgba(0, 0, 0, 0.6);
      z-index: 40;
      padding: 16px;
    }
    .modal-backdrop.open {
      display: flex;
    }
    .modal {
      width: min(880px, 96vw);
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #1f1f1f;
      padding: 16px;
      display: grid;
      gap: 10px;
      box-shadow: 0 14px 40px rgba(0, 0, 0, 0.45);
    }
    .modal-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }
    .modal-head h3 {
      font-size: 16px;
      letter-spacing: 0;
      color: #e0e0e0;
    }
    .modal-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
    }
    .modal-field {
      display: grid;
      gap: 6px;
      align-content: start;
    }
    .modal-toggle {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      color: var(--ink);
      font-size: 14px;
    }
    .modal-sub {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
    }
    .modal-status {
      color: var(--muted);
      font-size: 13px;
      min-height: 1.2em;
    }

    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 13px;
    }

    @media (max-width: 1240px) {
      .controls { grid-template-columns: 1fr 1fr; }
      .controls .btn { grid-column: span 1; }
      .routing-grid { grid-template-columns: repeat(2, minmax(160px, 1fr)); }
      .setup-grid { grid-template-columns: 1fr 1fr; }
      .layout-two { grid-template-columns: 1fr; }
      .subtitle { font-size: 14px; }
    }

    @media (max-width: 840px) {
      body { padding: 14px; }
      .hero { grid-template-columns: 1fr; }
      .status-pill { width: fit-content; }
      .subtitle { font-size: 13px; }
      .controls { grid-template-columns: 1fr; }
      .controls .btn { width: 100%; }
      .routing-grid { grid-template-columns: 1fr; }
      .setup-grid { grid-template-columns: 1fr; }
      .modal-grid { grid-template-columns: 1fr; }
      .tab-btn { font-size: 12px; padding: 8px 10px; }
      .model-title { font-size: 14px; }
      .chip { font-size: 14px; }
      h2 { font-size: 20px; }
    }
  </style>
</head>
<body>
  <div class=\"shell\">
    <div class=\"hero\">
      <div>
        <h1>RelayKit</h1>
        <p class=\"subtitle\">One local endpoint for your AI apps. Configure models, providers, and failover in one place.</p>
      </div>
      <div class=\"status-pill\" id=\"backend-state\">Checking status...</div>
    </div>

    <section class=\"card\">
      <div class=\"tabs\">
        <button class=\"tab-btn active\" data-tab=\"setup\" type=\"button\">Setup</button>
        <button class=\"tab-btn\" data-tab=\"models\" type=\"button\">Models</button>
        <button class=\"tab-btn\" data-tab=\"providers\" type=\"button\">Settings</button>
        <button class=\"tab-btn\" data-tab=\"diagnostics\" type=\"button\">Runtime</button>
      </div>

      <section class=\"panel active\" id=\"panel-setup\">
        <h2>Setup</h2>
        <p class=\"section-note\">One-screen onboarding for install, endpoint copy, and provider detection status.</p>
        <div class=\"setup-grid\">
          <article class=\"setup-card\">
            <h3>1. Install</h3>
            <ul class=\"setup-list\">
              <li>From repo root run:</li>
            </ul>
            <div class=\"setup-row\">
              <code id=\"setup-install-command\">./scripts/install.sh</code>
              <button class=\"copy-btn\" type=\"button\" data-copy-target=\"setup-install-command\">Copy</button>
            </div>
            <div class=\"setup-row\">
              <code id=\"setup-run-command\">./scripts/run.sh</code>
              <button class=\"copy-btn\" type=\"button\" data-copy-target=\"setup-run-command\">Copy</button>
            </div>
            <div class=\"setup-row\">
              <code id=\"setup-service-command\">./scripts/install-service.sh</code>
              <button class=\"copy-btn\" type=\"button\" data-copy-target=\"setup-service-command\">Copy</button>
            </div>
          </article>
          <article class=\"setup-card\">
            <h3>2. Connect Apps</h3>
            <ul class=\"setup-list\">
              <li>OpenAI-compatible Base URL</li>
            </ul>
            <div class=\"setup-row\">
              <code id=\"setup-endpoint\">http://127.0.0.1:11436/v1</code>
              <button class=\"copy-btn\" type=\"button\" data-copy-target=\"setup-endpoint\">Copy</button>
            </div>
            <ul class=\"setup-list\">
              <li>Docker app Base URL</li>
            </ul>
            <div class=\"setup-row\">
              <code id=\"setup-docker-endpoint\">http://host.docker.internal:11436/v1</code>
              <button class=\"copy-btn\" type=\"button\" data-copy-target=\"setup-docker-endpoint\">Copy</button>
            </div>
          </article>
          <article class=\"setup-card\">
            <h3>3. Health Snapshot</h3>
            <ul class=\"setup-list\">
              <li>Setup initialized: <strong id=\"setup-initialized\">no</strong></li>
              <li>Last bootstrap: <strong id=\"setup-last-bootstrap\">never</strong></li>
              <li>Detected CLIs: <strong id=\"setup-detected\">checking...</strong></li>
              <li>Visible models: <strong id=\"setup-visible\">0</strong></li>
              <li>Hidden models: <strong id=\"setup-hidden\">0</strong></li>
            </ul>
            <div class=\"btn-row\">
              <button id=\"setup-bootstrap\" class=\"btn\" type=\"button\">Run first-run wizard</button>
              <button id=\"setup-refresh\" class=\"btn secondary\" type=\"button\">Refresh snapshot</button>
              <button id=\"setup-open-models\" class=\"btn\" type=\"button\">Open models tab</button>
            </div>
            <p id=\"setup-status\" class=\"setup-status\"></p>
            <pre id=\"setup-bootstrap-output\">Wizard output will appear here.</pre>
          </article>
        </div>
      </section>

      <section class=\"panel\" id=\"panel-models\">
        <h2>Models</h2>
        <p class=\"section-note\">Simple list view. Internal RelayKit alias models are hidden by default.</p>
        <div class=\"chip-row\">
          <span class=\"chip\"><strong>Detected CLIs</strong> <span id=\"clis\">checking...</span></span>
          <span class=\"chip\"><strong>Showing</strong> <span id=\"visible-count\">0</span> models</span>
          <span class=\"chip\"><strong>Hidden</strong> <span id=\"hidden-count\">0</span></span>
        </div>

        <div class=\"controls\">
          <input id=\"model-filter\" class=\"mono\" type=\"text\" placeholder=\"Search models\" />
          <select id=\"provider-filter\" class=\"mono\">
            <option value=\"all\">All providers</option>
          </select>
          <button id=\"show-disabled\" class=\"btn secondary\" type=\"button\">Hide disabled</button>
          <button id=\"show-outdated\" class=\"btn secondary\" type=\"button\">Hide outdated</button>
          <button id=\"strict-mode-toggle\" class=\"btn secondary\" type=\"button\">Strict mode: Off</button>
          <button id=\"show-gateway\" class=\"btn secondary\" type=\"button\">Show internal aliases</button>
        </div>

        <div class=\"btn-row\">
          <button id=\"refresh-catalog\" class=\"btn\" type=\"button\">Refresh discovery</button>
          <button id=\"save-models\" class=\"btn\" type=\"button\">Save model settings</button>
          <button id=\"reset-models\" class=\"btn ghost\" type=\"button\">Reset overrides</button>
        </div>
        <p id=\"models-status\" class=\"note\"></p>

        <div class=\"model-list-head\">
          <div>Model</div>
          <div>Settings</div>
          <div>Health</div>
          <div>Actions</div>
        </div>
        <div id=\"model-cards\" class=\"models-grid\"></div>
        <div id=\"models-empty\" class=\"empty-state\" style=\"display:none;\">No models match the current filters.</div>
      </section>

      <section class=\"panel\" id=\"panel-providers\">
        <h2>Settings</h2>
        <p class=\"section-note\">Provider toggles and endpoint scan settings. API key and routing live in Setup wizard.</p>
        <div class=\"layout-two\">
          <div>
            <label class=\"section-note\" for=\"scan-ports\">Compat scan ports</label>
            <input id=\"scan-ports\" class=\"mono\" type=\"text\" placeholder=\"1234, 3000, 5000, 8000, 8080, 3210\" />
            <div class=\"btn-row\">
              <button id=\"refresh-catalog-providers\" class=\"btn\" type=\"button\">Refresh discovery</button>
              <button id=\"save-models-providers\" class=\"btn\" type=\"button\">Save provider settings</button>
            </div>
            <p class=\"note\">Changes persist in <code>~/.relaykit/config.json</code>.</p>
          </div>
          <div>
            <pre id=\"provider-summary\">Loading providers...</pre>
          </div>
        </div>
        <div id=\"provider-grid\" class=\"provider-grid\"></div>
      </section>

      <section class=\"panel\" id=\"panel-connection\">
        <h2>Connection</h2>
        <p class=\"section-note\">Use these endpoints in your client apps and store your local RelayKit API key.</p>
        <div class=\"layout-two\">
          <div>
            <p><strong>Base URL:</strong> <code>http://127.0.0.1:11436/v1</code></p>
            <p class=\"note\">For Docker apps use <code>http://host.docker.internal:11436/v1</code>.</p>
            <label class=\"section-note\" for=\"api-key\">RelayKit API key</label>
            <input id=\"api-key\" type=\"text\" placeholder=\"Paste RelayKit key\" />
            <div class=\"btn-row\">
              <button id=\"save-key\" class=\"btn\" type=\"button\">Save key</button>
              <button id=\"clear-key\" class=\"btn secondary\" type=\"button\">Clear key</button>
            </div>
            <p id=\"key-status\" class=\"note\"></p>
          </div>
          <div>
            <pre id=\"connection-help\">Auth header:
Authorization: Bearer &lt;your-relaykit-key&gt;

Example:
curl http://127.0.0.1:11436/v1/models \\
  -H \"Authorization: Bearer &lt;key&gt;\"</pre>
          </div>
        </div>
      </section>

      <section class=\"panel\" id=\"panel-diagnostics\">
        <h2>Runtime</h2>
        <p class=\"section-note\">View health, open circuits, and route traces. Routing controls are now in Setup wizard options.</p>
        <div class=\"btn-row\">
          <button id=\"refresh-diagnostics\" class=\"btn\" type=\"button\">Refresh diagnostics</button>
        </div>
        <div class=\"layout-two\">
          <pre id=\"diagnostics\">Loading diagnostics...</pre>
          <div class=\"routing-side\">
            <pre id=\"routing-runtime\">Loading routing runtime...</pre>
            <pre id=\"route-trace\">Select a model and click Trace to inspect routing.</pre>
          </div>
        </div>
      </section>

      <section class=\"panel\" id=\"panel-advanced\">
        <h2>Advanced</h2>
        <p class=\"section-note\">Raw config and API key management.</p>
        <div class=\"layout-two\">
          <div>
            <textarea id=\"config\"></textarea>
            <div class=\"btn-row\">
              <button id=\"save-config\" class=\"btn\" type=\"button\">Save config</button>
              <button id=\"export-config\" class=\"btn secondary\" type=\"button\">Export config</button>
              <button id=\"import-config\" class=\"btn secondary\" type=\"button\">Import config</button>
            </div>
            <p id=\"save-status\" class=\"note\"></p>
          </div>
          <div>
            <label class=\"section-note\" for=\"key-label\">Create API key</label>
            <input id=\"key-label\" type=\"text\" placeholder=\"app label\" />
            <div class=\"btn-row\">
              <button id=\"create-key\" class=\"btn\" type=\"button\">Create key</button>
            </div>
            <pre id=\"key-output\"></pre>
            <pre id=\"keys\">Loading keys...</pre>
          </div>
        </div>
      </section>
    </section>
  </div>

  <div id=\"wizard-modal-backdrop\" class=\"modal-backdrop\" aria-hidden=\"true\">
    <div class=\"modal\" role=\"dialog\" aria-modal=\"true\" aria-labelledby=\"wizard-modal-title\">
      <div class=\"modal-head\">
        <h3 id=\"wizard-modal-title\">First-Run Wizard Options</h3>
        <button id=\"wizard-modal-close\" class=\"btn ghost\" type=\"button\">Close</button>
      </div>
      <div class=\"modal-grid\">
        <label class=\"modal-field\">
          <span class=\"modal-toggle\">
            <input id=\"wizard-option-replace-models\" type=\"checkbox\" />
            <span>Replace provider model lists</span>
          </span>
          <span class=\"modal-sub\">Overwrite configured model lists with current CLI-detected models.</span>
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-toggle\">
            <input id=\"wizard-option-reset-overrides\" type=\"checkbox\" />
            <span>Reset model/provider overrides</span>
          </span>
          <span class=\"modal-sub\">Clear hidden/label overrides from UI catalog settings.</span>
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-toggle\">
            <input id=\"wizard-option-create-key\" type=\"checkbox\" />
            <span>Create API key</span>
          </span>
          <span class=\"modal-sub\">Generate and return a RelayKit API key for app connections.</span>
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-toggle\">
            <input id=\"wizard-option-force-key\" type=\"checkbox\" />
            <span>Force new API key</span>
          </span>
          <span class=\"modal-sub\">Create a new key even if keys already exist.</span>
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-toggle\">
            <input id=\"wizard-option-run-probe\" type=\"checkbox\" checked />
            <span>Run health probe after bootstrap</span>
          </span>
          <span class=\"modal-sub\">Quickly mark provider auto routes as healthy/unhealthy.</span>
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-sub\">Auto-configure by goal</span>
          <select id=\"wizard-goal-profile\">
            <option value=\"balanced\">Balanced</option>
            <option value=\"coding\">Coding</option>
            <option value=\"fastest\">Fastest</option>
            <option value=\"cheapest\">Cheapest</option>
            <option value=\"reliable\">Most reliable</option>
          </select>
          <span class=\"modal-sub\">Applies routing and model visibility defaults.</span>
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-sub\">Routing preset</span>
          <select id=\"wizard-routing-preset\">
            <option value=\"balanced\">Balanced (recommended)</option>
            <option value=\"stable\">Stable (fewer provider switches)</option>
            <option value=\"fast\">Fast recovery (more aggressive failover)</option>
          </select>
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-sub\">Breaker threshold</span>
          <input id=\"wizard-routing-breaker-threshold\" type=\"number\" min=\"1\" max=\"20\" step=\"1\" />
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-sub\">Retry count per candidate</span>
          <input id=\"wizard-routing-retry-count\" type=\"number\" min=\"1\" max=\"8\" step=\"1\" />
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-sub\">Probe interval (seconds)</span>
          <input id=\"wizard-routing-probe-interval\" type=\"number\" min=\"20\" max=\"3600\" step=\"1\" />
        </label>
        <label class=\"modal-field\">
          <span class=\"modal-sub\">API key label</span>
          <input id=\"wizard-option-key-label\" type=\"text\" value=\"bootstrap\" placeholder=\"bootstrap\" />
        </label>
      </div>
      <div class=\"btn-row\">
        <button id=\"wizard-goal-apply\" class=\"btn secondary\" type=\"button\">Apply goal now</button>
        <button id=\"wizard-modal-run\" class=\"btn\" type=\"button\">Run wizard</button>
      </div>
      <p id=\"routing-status\" class=\"modal-status\"></p>
      <p id=\"wizard-modal-status\" class=\"modal-status\"></p>
    </div>
  </div>

<script>
let catalogState = { models: [], all_models: [], providers: [], strict_mode: false, overrides: {}, provider_overrides: {}, scan_ports: [] };
let showDisabled = true;
let hideOutdated = false;
let showGatewayAliases = false;
let strictModeEnabled = false;
let modelFilterText = '';
let providerFilterValue = 'all';
let activeTab = 'setup';

function setSetupStatus(message) {
  const el = document.getElementById('setup-status');
  if (el) el.textContent = message || '';
}

function setWizardModalStatus(message) {
  const el = document.getElementById('wizard-modal-status');
  if (el) el.textContent = message || '';
}

function openWizardModal() {
  const modal = document.getElementById('wizard-modal-backdrop');
  if (!modal) return;
  modal.classList.add('open');
  modal.setAttribute('aria-hidden', 'false');
  setWizardModalStatus('');
  setRoutingStatus('');
  loadRoutingControls({ silent: true }).catch(() => {});
}

function closeWizardModal() {
  const modal = document.getElementById('wizard-modal-backdrop');
  if (!modal) return;
  modal.classList.remove('open');
  modal.setAttribute('aria-hidden', 'true');
  setWizardModalStatus('');
  setRoutingStatus('');
}

function syncWizardOptionState() {
  const createKey = document.getElementById('wizard-option-create-key');
  const forceKey = document.getElementById('wizard-option-force-key');
  const keyLabel = document.getElementById('wizard-option-key-label');
  const createEnabled = Boolean(createKey && createKey.checked);
  if (forceKey) {
    forceKey.disabled = !createEnabled;
    if (!createEnabled) forceKey.checked = false;
  }
  if (keyLabel) keyLabel.disabled = !createEnabled;
}

function getWizardPayloadFromModal() {
  return {
    replace_provider_models: Boolean(document.getElementById('wizard-option-replace-models')?.checked),
    reset_model_overrides: Boolean(document.getElementById('wizard-option-reset-overrides')?.checked),
    create_api_key: Boolean(document.getElementById('wizard-option-create-key')?.checked),
    force_new_api_key: Boolean(document.getElementById('wizard-option-force-key')?.checked),
    run_health_probe: Boolean(document.getElementById('wizard-option-run-probe')?.checked),
    key_label: (document.getElementById('wizard-option-key-label')?.value || 'bootstrap').trim() || 'bootstrap',
  };
}

function setSetupBootstrapOutput(payload) {
  const el = document.getElementById('setup-bootstrap-output');
  if (!el) return;
  if (!payload) {
    el.textContent = 'Wizard output will appear here.';
    return;
  }
  el.textContent = JSON.stringify(payload, null, 2);
}

function applySetupState(json) {
  if (!json || json.ok !== true) return;
  const setup = json.setup || {};
  const initializedEl = document.getElementById('setup-initialized');
  if (initializedEl) initializedEl.textContent = setup.initialized ? 'yes' : 'no';
  const lastBootstrapEl = document.getElementById('setup-last-bootstrap');
  if (lastBootstrapEl) lastBootstrapEl.textContent = setup.last_bootstrap_at || 'never';

  const detected = json.detected || {};
  const detectedNames = Object.entries(detected)
    .filter(([_, v]) => v && v.available)
    .map(([k]) => k);
  const detectedEl = document.getElementById('setup-detected');
  if (detectedEl) detectedEl.textContent = detectedNames.length ? detectedNames.join(', ') : 'none';
}

async function loadSetupState(opts = {}) {
  const silent = opts.silent === true;
  const res = await fetch('/admin/setup', { headers: getAuthHeaders() });
  const json = await res.json();
  if (!res.ok || !json.ok) {
    if (!silent) setSetupStatus(`Setup state load failed (${res.status}).`);
    return;
  }
  applySetupState(json);
  if (!silent) setSetupStatus('Setup snapshot refreshed.');
}

async function runSetupBootstrap(payload = null) {
  const runButton = document.getElementById('wizard-modal-run');
  if (runButton) runButton.disabled = true;
  setSetupStatus('Running first-run wizard...');
  setWizardModalStatus('Running...');
  const body = payload || {
    replace_provider_models: false,
    reset_model_overrides: false,
    create_api_key: false,
    run_health_probe: true,
    key_label: 'bootstrap',
  };
  try {
    const goal = (document.getElementById('wizard-goal-profile')?.value || 'balanced').trim().toLowerCase();
    if (goal !== 'balanced') {
      setWizardModalStatus(`Applying goal "${goal}"...`);
      await applyGoalProfile(goal, { persist: true, silent: true });
    } else {
      applyGoalRoutingDefaults('balanced');
    }
    setRoutingStatus('Applying routing settings...');
    await saveRoutingControls({ silent: true });
    setRoutingStatus('Routing settings applied.');
    const res = await fetch('/admin/setup/bootstrap', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
      body: JSON.stringify(body),
    });
    const json = await res.json();
    if (!res.ok || !json.ok) {
      setSetupStatus((json && json.detail) ? String(json.detail) : `Wizard failed (${res.status}).`);
      setWizardModalStatus((json && json.detail) ? String(json.detail) : `Wizard failed (${res.status}).`);
      return;
    }
    setSetupBootstrapOutput(json);
    const newKey = json?.bootstrap?.api_key || '';
    if (newKey) {
      const keyInput = document.getElementById('api-key');
      if (keyInput) keyInput.value = newKey;
      localStorage.setItem('relaykit_api_key', newKey);
      document.getElementById('key-status').textContent = 'Wizard created and saved API key.';
    }
    setSetupStatus('First-run wizard completed.');
    setWizardModalStatus('Completed.');
    closeWizardModal();
    await loadSetupState({ silent: true });
    await loadCatalog(true);
    await loadDiagnostics();
    await loadRoutingControls({ silent: true });
  } catch (e) {
    const message = (e && e.message) ? String(e.message) : 'Wizard request failed.';
    setSetupStatus(message);
    setWizardModalStatus(message);
  } finally {
    if (runButton) runButton.disabled = false;
  }
}

function copyText(value) {
  if (!value) return Promise.resolve(false);
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(value).then(() => true).catch(() => false);
  }
  const ta = document.createElement('textarea');
  ta.value = value;
  ta.style.position = 'fixed';
  ta.style.opacity = '0';
  document.body.appendChild(ta);
  ta.focus();
  ta.select();
  let ok = false;
  try {
    ok = document.execCommand('copy');
  } catch (_) {
    ok = false;
  }
  document.body.removeChild(ta);
  return Promise.resolve(ok);
}

function getAuthHeaders() {
  const token = localStorage.getItem('relaykit_api_key') || '';
  const headers = {};
  if (token.trim()) headers['Authorization'] = `Bearer ${token.trim()}`;
  return headers;
}

function setCatalogStatus(msg) {
  const top = document.getElementById('models-status');
  const bottom = document.getElementById('save-status');
  if (top) top.textContent = msg || '';
  if (bottom) bottom.textContent = msg || '';
}

let autoSaveTimer = null;
function scheduleAutoSave(reason = 'Changes detected') {
  if (autoSaveTimer) clearTimeout(autoSaveTimer);
  setCatalogStatus(`${reason}. Autosaving...`);
  autoSaveTimer = setTimeout(() => {
    document.getElementById('save-models').click();
  }, 700);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function normalizeScanPorts(value) {
  return value
    .split(',')
    .map(v => v.trim())
    .filter(Boolean)
    .map(v => Number(v))
    .filter(v => Number.isFinite(v) && v > 0);
}

function parseModelName(modelId, providerName) {
  const id = String(modelId || '');
  const provider = String(providerName || '');
  if (provider && id.startsWith(`${provider}:`)) return id.slice(provider.length + 1);
  if (id.includes(':')) return id.split(':').slice(1).join(':');
  return id;
}

function modelDateCode(modelName) {
  const name = String(modelName || '');
  const iso = name.match(/(20\d{2})-(\d{2})-(\d{2})/);
  if (iso) return Number(`${iso[1]}${iso[2]}${iso[3]}`);
  const compact = name.match(/(20\d{2})(\d{2})(\d{2})/);
  if (compact) return Number(`${compact[1]}${compact[2]}${compact[3]}`);
  return null;
}

function modelFamilyKey(modelName) {
  return String(modelName || '')
    .toLowerCase()
    .replace(/20\d{2}-\d{2}-\d{2}/g, '')
    .replace(/20\d{6}/g, '')
    .replace(/[-_:]+$/g, '');
}

function isStrictExcludedModel(model, providerName) {
  const provider = String(providerName || '').toLowerCase();
  const kind = String(model?.kind || '').toLowerCase();
  const id = String(model?.id || '').toLowerCase();
  const target = `${provider} ${kind} ${id}`;
  const blocked = ['preview', 'experimental', 'beta', 'alpha', 'canary', 'nightly', 'dev', 'test'];
  return blocked.some(token => target.includes(token));
}

function renderTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === activeTab);
  });
  document.querySelectorAll('.panel').forEach(panel => {
    panel.classList.toggle('active', panel.id === `panel-${activeTab}`);
  });
}

function renderProviders() {
  const grid = document.getElementById('provider-grid');
  const providers = Array.isArray(catalogState.providers) ? catalogState.providers : [];
  grid.innerHTML = '';
  providers.forEach(provider => {
    const card = document.createElement('article');
    card.className = 'provider-card';
    if (provider.enabled === false) card.classList.add('disabled');
    card.dataset.providerId = provider.id;
    card.innerHTML = `
      <div class="provider-head">
        <strong>${escapeHtml(provider.label || provider.id)}</strong>
        <label style="display:flex; gap:8px; align-items:center; font-size:16px;">
          <input type="checkbox" class="provider-enabled" ${provider.enabled === false ? '' : 'checked'} />
          <span>On</span>
        </label>
      </div>
      <input type="text" class="provider-label" value="${escapeHtml(provider.label || provider.id)}" placeholder="Display label" />
      <div class="provider-meta">${provider.kind === 'gateway' ? 'Gateway alias layer' : 'Provider family'} · ${provider.visible_count || 0} visible / ${provider.hidden_count || 0} hidden / ${provider.total_count || 0} total</div>
    `;
    grid.appendChild(card);
  });
  const summary = providers.map(p => `${p.id}: ${p.visible_count || 0} visible / ${p.hidden_count || 0} hidden`).join(String.fromCharCode(10));
  document.getElementById('provider-summary').textContent = summary || 'No providers found';
}

function renderCatalog(data) {
  catalogState = { ...(catalogState || {}), ...(data || {}) };
  const rows = Array.isArray(catalogState.all_models) ? catalogState.all_models : (catalogState.models || []);
  strictModeEnabled = Boolean(catalogState.strict_mode);

  document.getElementById('visible-count').textContent = '0';
  document.getElementById('hidden-count').textContent = String(rows.filter(m => m.enabled === false).length);
  document.getElementById('scan-ports').value = Array.isArray(catalogState.scan_ports) ? catalogState.scan_ports.join(', ') : '';
  const strictBtn = document.getElementById('strict-mode-toggle');
  if (strictBtn) strictBtn.textContent = strictModeEnabled ? 'Strict mode: On' : 'Strict mode: Off';

  const clis = Object.entries((catalogState.cli_detected || {})).filter(([_, v]) => v).map(([k]) => k);
  document.getElementById('clis').textContent = clis.length ? clis.join(', ') : 'none';
  const setupDetected = document.getElementById('setup-detected');
  if (setupDetected) setupDetected.textContent = clis.length ? clis.join(', ') : 'none';
  const setupVisible = document.getElementById('setup-visible');
  if (setupVisible) setupVisible.textContent = String(rows.filter(m => m.enabled !== false).length);
  const setupHidden = document.getElementById('setup-hidden');
  if (setupHidden) setupHidden.textContent = String(rows.filter(m => m.enabled === false).length);
  document.getElementById('backend-state').textContent = 'RelayKit ready';

  const providerFilter = document.getElementById('provider-filter');
  if (providerFilter) {
    const providerSet = new Set(rows.map(r => r.provider || r.source || r.owned_by || 'other'));
    const options = ['<option value="all">All providers</option>']
      .concat(Array.from(providerSet).sort().map(p => `<option value="${escapeHtml(p)}">${escapeHtml(p)}</option>`))
      .join('');
    providerFilter.innerHTML = options;
    if (providerSet.has(providerFilterValue)) {
      providerFilter.value = providerFilterValue;
    } else {
      providerFilter.value = 'all';
      providerFilterValue = 'all';
    }
  }

  renderProviders();

  const cards = document.getElementById('model-cards');
  cards.innerHTML = '';
  let rendered = 0;
  const latestByFamily = {};
  rows.forEach(model => {
    const providerName = model.provider || model.source || model.owned_by || 'other';
    const modelName = parseModelName(model.id, providerName);
    const dateCode = modelDateCode(modelName);
    if (dateCode === null) return;
    const key = `${providerName}|${modelFamilyKey(modelName)}`;
    latestByFamily[key] = Math.max(latestByFamily[key] || 0, dateCode);
  });

  rows
    .sort((a, b) => {
      const pa = (a.provider || a.source || a.owned_by || '').localeCompare(b.provider || b.source || b.owned_by || '');
      if (pa !== 0) return pa;
      return String(a.id).localeCompare(String(b.id));
    })
    .forEach(model => {
      const providerName = model.provider || model.source || model.owned_by || 'other';
      const isGatewayAlias = providerName === 'relaykit' || String(model.kind || '').toLowerCase() === 'gateway' || String(model.id || '').startsWith('relaykit:');
      if (!showGatewayAliases && isGatewayAlias) return;
      if (strictModeEnabled && isStrictExcludedModel(model, providerName)) return;
      if (!showDisabled && model.enabled === false) return;
      if (providerFilterValue !== 'all' && providerName !== providerFilterValue) return;
      if (hideOutdated) {
        const modelName = parseModelName(model.id, providerName);
        const dateCode = modelDateCode(modelName);
        if (dateCode !== null) {
          const key = `${providerName}|${modelFamilyKey(modelName)}`;
          if ((latestByFamily[key] || 0) > dateCode) return;
        }
      }
      if (modelFilterText) {
        const haystack = `${model.id} ${model.label || ''} ${providerName}`.toLowerCase();
        if (!haystack.includes(modelFilterText)) return;
      }

      const stats = model.stats || {};
      const lastSuccess = stats.last_success_at || stats.last_test_success_at || '';
      const lastFailure = stats.last_failure_at || stats.last_test_failure_at || '';
      const reliability = (Number(stats.successes || 0) + Number(stats.failures || 0)) > 0
        ? Number(stats.successes || 0) / (Number(stats.successes || 0) + Number(stats.failures || 0))
        : null;

      const card = document.createElement('article');
      card.className = 'model-card';
      if (model.enabled === false) card.classList.add('disabled');
      card.dataset.modelId = model.id;
      card.innerHTML = `
        <div class="model-top">
          <div class="model-main">
            <div class="model-title">${escapeHtml(model.id)}</div>
            <div class="model-meta">
              <span class="provider-badge">${escapeHtml(providerName)}</span>
              <span class="model-stats">Owned by ${escapeHtml(model.owned_by || 'n/a')}</span>
            </div>
            <div class="model-health-line">
              <span>${reliability !== null ? `Reliability ${(reliability * 100).toFixed(0)}%` : 'Reliability n/a'}</span>
              <span>${stats.successes || 0} success / ${stats.failures || 0} fail</span>
              ${lastSuccess ? `<span>Last success ${escapeHtml(lastSuccess)}</span>` : ''}
              ${lastFailure ? `<span>Last failure ${escapeHtml(lastFailure)}</span>` : ''}
            </div>
          </div>
          <div class="model-actions">
            <button type="button" class="icon-btn settings-toggle" title="Model settings" aria-expanded="false">⚙</button>
            <button type="button" class="icon-btn secondary test-model" data-model-id="${escapeHtml(model.id)}" title="Test model">▶</button>
            <button type="button" class="icon-btn ghost trace-route" data-model-id="${escapeHtml(model.id)}" title="Trace route">↗</button>
          </div>
        </div>
        <div class="test-status" data-test-status="${escapeHtml(model.id)}">${stats.last_test_at ? `Last test ${escapeHtml(stats.last_test_at)}` : ''}</div>
        <div class="model-settings">
          <label class="model-toggle">
            <input type="checkbox" class="model-enabled" ${model.enabled === false ? '' : 'checked'} />
            <span>Enabled</span>
          </label>
          <input type="text" class="model-label" value="${escapeHtml(model.label || model.id)}" placeholder="Display label" />
        </div>
      `;
      cards.appendChild(card);
      rendered += 1;
    });

  document.getElementById('models-empty').style.display = rendered === 0 ? 'block' : 'none';
  document.getElementById('visible-count').textContent = String(rendered);
}

async function loadCatalog(force = false) {
  const isRefresh = force === true;
  const res = await fetch(isRefresh ? '/admin/catalog/refresh' : '/admin/catalog', {
    method: isRefresh ? 'POST' : 'GET',
    headers: {
      ...(isRefresh ? { 'Content-Type': 'application/json' } : {}),
      ...getAuthHeaders(),
    },
    ...(isRefresh ? { body: '{}' } : {}),
  });

  let json = {};
  try {
    json = await res.json();
  } catch (_) {
    json = {};
  }

  if (!res.ok || !Array.isArray(json.models)) {
    setCatalogStatus(`Catalog request failed (${res.status}).`);
    return catalogState;
  }

  renderCatalog(json);
  return json;
}

async function loadDiagnostics() {
  const res = await fetch('/admin/diagnostics', { headers: getAuthHeaders() });
  const json = await res.json();
  document.getElementById('diagnostics').textContent = JSON.stringify(json, null, 2);
}

function _setRoutingInputs(routing) {
  const weights = (routing && routing.weights) || {};
  const setValue = (id, value, precision = null) => {
    const el = document.getElementById(id);
    if (!el) return;
    if (typeof value === 'number' && Number.isFinite(value)) {
      el.value = precision == null ? String(value) : value.toFixed(precision);
    } else {
      el.value = '';
    }
  };
  setValue('wizard-routing-breaker-threshold', Number(routing?.breaker_failure_threshold));
  setValue('wizard-routing-retry-count', Number(routing?.max_attempts_per_candidate));
  setValue('wizard-routing-probe-interval', Number(routing?.probe_interval_seconds));
  const preset = identifyRoutingPreset(weights);
  const presetEl = document.getElementById('wizard-routing-preset');
  if (presetEl) presetEl.value = preset;
}

function routingPresetWeights(preset) {
  if (preset === 'stable') return { reliability: 0.70, latency: 0.20, cost: 0.10 };
  if (preset === 'fast') return { reliability: 0.45, latency: 0.45, cost: 0.10 };
  return { reliability: 0.55, latency: 0.30, cost: 0.15 };
}

function identifyRoutingPreset(weights) {
  const r = Number(weights?.reliability);
  const l = Number(weights?.latency);
  const c = Number(weights?.cost);
  const close = (a, b) => Number.isFinite(a) && Math.abs(a - b) < 0.02;
  if (close(r, 0.70) && close(l, 0.20) && close(c, 0.10)) return 'stable';
  if (close(r, 0.45) && close(l, 0.45) && close(c, 0.10)) return 'fast';
  return 'balanced';
}

function applyRoutingPresetDefaults(preset) {
  const defaults = {
    balanced: { breaker: 3, retry: 2, probe: 90 },
    stable: { breaker: 5, retry: 2, probe: 180 },
    fast: { breaker: 2, retry: 3, probe: 45 },
  };
  const selected = defaults[preset] || defaults.balanced;
  const set = (id, value) => {
    const el = document.getElementById(id);
    if (el) el.value = String(value);
  };
  set('wizard-routing-breaker-threshold', selected.breaker);
  set('wizard-routing-retry-count', selected.retry);
  set('wizard-routing-probe-interval', selected.probe);
}

function goalRoutingDefaults(goal) {
  if (goal === 'coding') return { preset: 'balanced', breaker: 3, retry: 2, probe: 90 };
  if (goal === 'fastest') return { preset: 'fast', breaker: 2, retry: 3, probe: 45 };
  if (goal === 'cheapest') return { preset: 'fast', breaker: 3, retry: 2, probe: 120 };
  if (goal === 'reliable') return { preset: 'stable', breaker: 5, retry: 2, probe: 180 };
  return { preset: 'balanced', breaker: 3, retry: 2, probe: 90 };
}

function applyGoalRoutingDefaults(goal) {
  const d = goalRoutingDefaults(goal);
  const presetEl = document.getElementById('wizard-routing-preset');
  if (presetEl) presetEl.value = d.preset;
  const set = (id, value) => {
    const el = document.getElementById(id);
    if (el) el.value = String(value);
  };
  set('wizard-routing-breaker-threshold', d.breaker);
  set('wizard-routing-retry-count', d.retry);
  set('wizard-routing-probe-interval', d.probe);
}

function goalModelDecision(goal, model, providerName) {
  const provider = String(providerName || '').toLowerCase();
  const id = String(model?.id || '').toLowerCase();
  const name = parseModelName(id, provider).toLowerCase();
  if (id.startsWith('relaykit:') || provider === 'relaykit' || String(model?.kind || '').toLowerCase() === 'gateway') return null;

  const has = (re) => re.test(name) || re.test(id);
  if (goal === 'coding') {
    if (has(/embedding|image|audio|tts|live|native-audio|preview-tts/)) return false;
    if (['codex', 'claude', 'gemini', 'hermes', 'ollama'].includes(provider)) return true;
    return null;
  }
  if (goal === 'fastest') {
    if (has(/flash|haiku|lite|mini|auto|8b|9b|small|3b/)) return true;
    if (has(/opus|max|pro|30b|35b|70b|preview-customtools/)) return false;
    return null;
  }
  if (goal === 'cheapest') {
    if (provider === 'ollama') return true;
    if (has(/lite|mini|haiku|flash|auto|8b|9b|3b/)) return true;
    if (has(/opus|max|pro|30b|35b|70b/)) return false;
    return null;
  }
  if (goal === 'reliable') {
    if (has(/preview|experimental|beta/)) return false;
    if (has(/auto|sonnet|haiku|flash|gpt-5\.2|gpt-5\.3|gpt-5\.4|codex-mini-latest/)) return true;
    return null;
  }
  return null;
}

async function applyGoalProfile(goal, opts = {}) {
  const persist = opts.persist === true;
  const silent = opts.silent === true;
  const normalizedGoal = String(goal || 'balanced').trim().toLowerCase();
  applyGoalRoutingDefaults(normalizedGoal);

  if (!persist) return { changed: 0 };

  const baseOverrides = JSON.parse(JSON.stringify(catalogState.overrides || {}));
  const providerOverrides = JSON.parse(JSON.stringify(catalogState.provider_overrides || {}));
  const rows = Array.isArray(catalogState.all_models) ? catalogState.all_models : (catalogState.models || []);
  let changed = 0;

  for (const model of rows) {
    const modelId = String(model?.id || '');
    if (!modelId) continue;
    const provider = model.provider || model.source || model.owned_by || 'other';
    const decision = goalModelDecision(normalizedGoal, model, provider);
    if (decision === null) continue;
    const prev = (baseOverrides[modelId] && typeof baseOverrides[modelId] === 'object') ? { ...baseOverrides[modelId] } : {};
    if (decision) delete prev.enabled;
    else prev.enabled = false;
    if (Object.keys(prev).length) baseOverrides[modelId] = prev;
    else delete baseOverrides[modelId];
    changed += 1;
  }

  const payload = {
    model_overrides: baseOverrides,
    provider_overrides: providerOverrides,
    scan_ports: normalizeScanPorts(document.getElementById('scan-ports')?.value || ''),
  };
  const res = await fetch('/admin/catalog', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(payload),
  });
  const json = await res.json();
  if (!res.ok || !json.ok) {
    const message = (json && json.detail) ? String(json.detail) : `Goal apply failed (${res.status}).`;
    if (!silent) setWizardModalStatus(message);
    throw new Error(message);
  }
  renderCatalog((json && json.catalog) ? json.catalog : json);
  if (!silent) {
    setCatalogStatus(`Applied goal "${normalizedGoal}" to ${changed} models.`);
    setWizardModalStatus(`Goal "${normalizedGoal}" applied.`);
  }
  return { changed };
}

function setRoutingStatus(message) {
  const el = document.getElementById('routing-status');
  if (el) el.textContent = message || '';
}

async function loadRoutingControls(opts = {}) {
  const silent = opts.silent === true;
  const res = await fetch('/admin/routing', { headers: getAuthHeaders() });
  const json = await res.json();
  if (!res.ok || !json.ok) {
    if (!silent) setRoutingStatus(`Routing load failed (${res.status}).`);
    return;
  }
  _setRoutingInputs(json.routing || {});
  const runtimeEl = document.getElementById('routing-runtime');
  if (runtimeEl) runtimeEl.textContent = JSON.stringify(json.routing_runtime || {}, null, 2);
  if (!silent) setRoutingStatus('Routing settings loaded.');
}

async function saveRoutingControls(opts = {}) {
  const silent = opts.silent === true;
  const num = (id, fallback) => {
    const raw = document.getElementById(id)?.value ?? '';
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : fallback;
  };
  const preset = (document.getElementById('wizard-routing-preset')?.value || 'balanced').trim().toLowerCase();
  const weights = routingPresetWeights(preset);
  const payload = {
    weights,
    breaker_failure_threshold: Math.round(num('wizard-routing-breaker-threshold', 3)),
    max_attempts_per_candidate: Math.round(num('wizard-routing-retry-count', 2)),
    probe_interval_seconds: Math.round(num('wizard-routing-probe-interval', 90)),
  };

  if (!silent) setRoutingStatus('Saving routing...');
  const res = await fetch('/admin/routing', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(payload),
  });
  const json = await res.json();
  if (!res.ok || !json.ok) {
    const message = (json && json.detail) ? String(json.detail) : `Routing save failed (${res.status}).`;
    setRoutingStatus(message);
    throw new Error(message);
  }
  _setRoutingInputs(json.routing || {});
  const runtimeEl = document.getElementById('routing-runtime');
  if (runtimeEl) runtimeEl.textContent = JSON.stringify(json.routing_runtime || {}, null, 2);
  if (!silent) setRoutingStatus('Routing settings saved.');
  return json;
}

async function load() {
  const health = await fetch('/health').then(r => r.json());
  document.getElementById('backend-state').textContent = health.available ? 'RelayKit ready' : 'RelayKit backend unavailable';

  await loadCatalog(false);
  await loadSetupState({ silent: true });
  await loadDiagnostics();
  await loadRoutingControls({ silent: true });

  const config = await fetch('/admin/config', { headers: getAuthHeaders() }).then(r => r.json());
  document.getElementById('config').value = JSON.stringify(config, null, 2);

  const keys = await fetch('/admin/keys', { headers: getAuthHeaders() }).then(r => r.json());
  document.getElementById('keys').textContent = (keys.keys || []).map(k => `${k.label} (${k.created})`).join(String.fromCharCode(10)) || 'No API keys yet.';
}

load();
renderTabs();
syncWizardOptionState();
setSetupBootstrapOutput(null);

// tabs

document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    activeTab = btn.dataset.tab || 'models';
    renderTabs();
  });
});

// filters

document.getElementById('show-disabled').addEventListener('click', (ev) => {
  ev.preventDefault();
  showDisabled = !showDisabled;
  ev.target.textContent = showDisabled ? 'Hide disabled' : 'Show disabled';
  renderCatalog(catalogState);
});
document.getElementById('show-disabled').textContent = showDisabled ? 'Hide disabled' : 'Show disabled';

document.getElementById('show-outdated').addEventListener('click', (ev) => {
  ev.preventDefault();
  hideOutdated = !hideOutdated;
  ev.target.textContent = hideOutdated ? 'Show outdated' : 'Hide outdated';
  renderCatalog(catalogState);
});
document.getElementById('show-outdated').textContent = hideOutdated ? 'Show outdated' : 'Hide outdated';

document.getElementById('strict-mode-toggle').addEventListener('click', async (ev) => {
  ev.preventDefault();
  const next = !strictModeEnabled;
  setCatalogStatus('Saving strict mode...');
  const res = await fetch('/admin/catalog', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ strict_mode: next }),
  });
  const json = await res.json();
  if (!res.ok || !json.ok) {
    setCatalogStatus((json && json.detail) ? String(json.detail) : `Strict mode save failed (${res.status}).`);
    return;
  }
  strictModeEnabled = next;
  setCatalogStatus(`Strict mode ${strictModeEnabled ? 'enabled' : 'disabled'}.`);
  await loadCatalog(true);
});

document.getElementById('show-gateway').addEventListener('click', (ev) => {
  ev.preventDefault();
  showGatewayAliases = !showGatewayAliases;
  ev.target.textContent = showGatewayAliases ? 'Hide internal aliases' : 'Show internal aliases';
  renderCatalog(catalogState);
});
document.getElementById('show-gateway').textContent = showGatewayAliases ? 'Hide internal aliases' : 'Show internal aliases';

document.getElementById('model-filter').addEventListener('input', (ev) => {
  modelFilterText = (ev.target.value || '').trim().toLowerCase();
  renderCatalog(catalogState);
});

document.getElementById('provider-filter').addEventListener('change', (ev) => {
  providerFilterValue = ev.target.value || 'all';
  renderCatalog(catalogState);
});

// autosave interactions
document.getElementById('model-cards').addEventListener('change', (ev) => {
  if (ev.target.closest('.model-enabled')) scheduleAutoSave('Model enabled updated');
});
document.getElementById('model-cards').addEventListener('input', (ev) => {
  if (ev.target.closest('.model-label')) scheduleAutoSave('Model label updated');
});
document.getElementById('provider-grid').addEventListener('change', (ev) => {
  if (ev.target.closest('.provider-enabled')) scheduleAutoSave('Provider enabled updated');
});
document.getElementById('provider-grid').addEventListener('input', (ev) => {
  if (ev.target.closest('.provider-label')) scheduleAutoSave('Provider label updated');
});
document.getElementById('scan-ports').addEventListener('input', () => {
  scheduleAutoSave('Scan ports updated');
});
document.getElementById('model-cards').addEventListener('click', (ev) => {
  const toggle = ev.target.closest('.settings-toggle');
  if (!toggle) return;
  const card = ev.target.closest('.model-card');
  if (!card) return;
  const open = card.classList.toggle('settings-open');
  toggle.classList.toggle('active', open);
  toggle.setAttribute('aria-expanded', open ? 'true' : 'false');
});

// key storage

document.getElementById('api-key').value = localStorage.getItem('relaykit_api_key') || '';
document.getElementById('save-key').addEventListener('click', async () => {
  const token = document.getElementById('api-key').value.trim();
  localStorage.setItem('relaykit_api_key', token);
  document.getElementById('key-status').textContent = token ? 'Saved.' : 'Key cleared.';
  await load();
});

document.getElementById('clear-key').addEventListener('click', async () => {
  localStorage.removeItem('relaykit_api_key');
  document.getElementById('api-key').value = '';
  document.getElementById('key-status').textContent = 'Cleared.';
  await load();
});

// catalog operations

document.getElementById('refresh-catalog').addEventListener('click', async () => {
  setCatalogStatus('Refreshing...');
  await loadCatalog(true);
  setCatalogStatus('Discovery refreshed.');
});

document.getElementById('setup-refresh').addEventListener('click', async () => {
  setSetupStatus('Refreshing...');
  await loadSetupState({ silent: true });
  await loadCatalog(true);
  await loadDiagnostics();
  setSetupStatus('Snapshot refreshed.');
});

document.getElementById('setup-bootstrap').addEventListener('click', async () => {
  openWizardModal();
});

document.getElementById('setup-open-models').addEventListener('click', () => {
  activeTab = 'models';
  renderTabs();
});

document.getElementById('wizard-modal-close').addEventListener('click', () => {
  closeWizardModal();
});

document.getElementById('wizard-option-create-key').addEventListener('change', () => {
  syncWizardOptionState();
});

document.getElementById('wizard-goal-profile').addEventListener('change', (ev) => {
  const goal = (ev.target?.value || 'balanced').trim().toLowerCase();
  applyGoalRoutingDefaults(goal);
  setWizardModalStatus(`Goal "${goal}" selected.`);
});

document.getElementById('wizard-goal-apply').addEventListener('click', async () => {
  const goal = (document.getElementById('wizard-goal-profile')?.value || 'balanced').trim().toLowerCase();
  setWizardModalStatus(`Applying goal "${goal}"...`);
  try {
    await applyGoalProfile(goal, { persist: true });
    await loadDiagnostics();
  } catch (e) {
    setWizardModalStatus((e && e.message) ? String(e.message) : 'Goal apply failed.');
  }
});

document.getElementById('wizard-routing-preset').addEventListener('change', (ev) => {
  const preset = (ev.target?.value || 'balanced').trim().toLowerCase();
  applyRoutingPresetDefaults(preset);
  setRoutingStatus('Preset applied. You can still edit values before running.');
});

document.getElementById('wizard-modal-run').addEventListener('click', async () => {
  const payload = getWizardPayloadFromModal();
  await runSetupBootstrap(payload);
});

document.getElementById('wizard-modal-backdrop').addEventListener('click', (ev) => {
  if (ev.target && ev.target.id === 'wizard-modal-backdrop') {
    closeWizardModal();
  }
});

document.addEventListener('keydown', (ev) => {
  if (ev.key === 'Escape') closeWizardModal();
});

document.getElementById('refresh-catalog-providers').addEventListener('click', async () => {
  await loadCatalog(true);
});

document.getElementById('save-models-providers').addEventListener('click', () => {
  document.getElementById('save-models').click();
});

document.getElementById('reset-models').addEventListener('click', async () => {
  setCatalogStatus('Resetting...');
  const res = await fetch('/admin/catalog', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ reset: true }),
  });
  const json = await res.json();
  if (res.ok && json.ok) {
    setCatalogStatus('Overrides reset.');
    await loadCatalog(true);
  } else {
    setCatalogStatus('Reset failed.');
  }
});

document.getElementById('save-models').addEventListener('click', async () => {
  const overrides = JSON.parse(JSON.stringify(catalogState.overrides || {}));
  const providerOverrides = JSON.parse(JSON.stringify(catalogState.provider_overrides || {}));
  const scanPorts = normalizeScanPorts(document.getElementById('scan-ports').value);

  document.querySelectorAll('#provider-grid .provider-card[data-provider-id]').forEach(card => {
    const providerId = card.dataset.providerId;
    const enabled = card.querySelector('.provider-enabled')?.checked ?? true;
    const label = card.querySelector('.provider-label')?.value?.trim() || '';
    const entry = (providerOverrides[providerId] && typeof providerOverrides[providerId] === 'object')
      ? { ...providerOverrides[providerId] }
      : {};
    if (!enabled) entry.enabled = false;
    else delete entry.enabled;
    if (label && label !== providerId) entry.label = label;
    else delete entry.label;
    if (Object.keys(entry).length) providerOverrides[providerId] = entry;
    else delete providerOverrides[providerId];
  });

  document.querySelectorAll('#model-cards .model-card[data-model-id]').forEach(card => {
    const modelId = card.dataset.modelId;
    const enabled = card.querySelector('.model-enabled')?.checked ?? true;
    const label = card.querySelector('.model-label')?.value?.trim() || '';
    const entry = (overrides[modelId] && typeof overrides[modelId] === 'object')
      ? { ...overrides[modelId] }
      : {};
    if (!enabled) entry.enabled = false;
    else delete entry.enabled;
    if (label && label !== modelId) entry.label = label;
    else delete entry.label;
    if (Object.keys(entry).length) overrides[modelId] = entry;
    else delete overrides[modelId];
  });

  const payload = { provider_overrides: providerOverrides, model_overrides: overrides, scan_ports: scanPorts };
  setCatalogStatus('Saving...');
  const res = await fetch('/admin/catalog', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(payload),
  });
  const json = await res.json();
  if (res.ok && json.ok) {
    setCatalogStatus(`Catalog saved. ${Object.keys(overrides).length} model overrides.`);
    await loadCatalog(true);
    await loadDiagnostics();
  } else {
    setCatalogStatus('Catalog save failed.');
  }
});

// advanced config

document.getElementById('save-config').addEventListener('click', async () => {
  const txt = document.getElementById('config').value;
  try {
    const json = JSON.parse(txt);
    await fetch('/admin/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
      body: JSON.stringify(json),
    });
    document.getElementById('save-status').textContent = 'Saved.';
  } catch (e) {
    document.getElementById('save-status').textContent = 'Invalid JSON.';
  }
});

document.getElementById('export-config').addEventListener('click', async () => {
  const txt = await fetch('/admin/config/export', { headers: getAuthHeaders() }).then(r => r.text());
  document.getElementById('config').value = txt;
  document.getElementById('save-status').textContent = 'Export loaded into editor.';
});

document.getElementById('import-config').addEventListener('click', async () => {
  const txt = document.getElementById('config').value;
  try {
    const json = JSON.parse(txt);
    const res = await fetch('/admin/config/import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
      body: JSON.stringify(json),
    });
    if (res.ok) {
      document.getElementById('save-status').textContent = 'Config imported.';
      await load();
    } else {
      document.getElementById('save-status').textContent = 'Import failed.';
    }
  } catch (e) {
    document.getElementById('save-status').textContent = 'Invalid JSON.';
  }
});

// diagnostics + route trace

document.getElementById('refresh-diagnostics').addEventListener('click', async () => {
  await loadDiagnostics();
  await loadRoutingControls({ silent: true });
});

const loadRoutingBtn = document.getElementById('load-routing');
if (loadRoutingBtn) {
  loadRoutingBtn.addEventListener('click', async () => {
    await loadRoutingControls();
  });
}

const saveRoutingBtn = document.getElementById('save-routing');
if (saveRoutingBtn) {
  saveRoutingBtn.addEventListener('click', async () => {
    await saveRoutingControls();
    await loadDiagnostics();
  });
}

document.getElementById('model-cards').addEventListener('click', async (ev) => {
  const testButton = ev.target.closest('.test-model');
  const traceButton = ev.target.closest('.trace-route');

  if (testButton) {
    const modelId = testButton.dataset.modelId;
    const status = document.querySelector(`[data-test-status="${CSS.escape(modelId)}"]`);
    if (status) status.textContent = 'Testing...';
    try {
      const res = await fetch('/admin/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
        body: JSON.stringify({ model: modelId, prompt: 'Say ok' }),
      });
      const json = await res.json();
      if (res.ok && json.ok) {
        if (status) status.textContent = `OK: ${json.text || 'ok'}`;
        await loadCatalog(true);
      } else {
        if (status) status.textContent = json.detail || 'Test failed';
      }
    } catch (e) {
      if (status) status.textContent = 'Test failed';
    }
    return;
  }

  if (!traceButton) return;
  const modelId = traceButton.dataset.modelId;
  const tracePanel = document.getElementById('route-trace');
  if (tracePanel) tracePanel.textContent = 'Tracing route...';
  try {
    const res = await fetch(`/admin/route?model=${encodeURIComponent(modelId)}`, { headers: getAuthHeaders() });
    const json = await res.json();
    if (res.ok && json.ok) {
      tracePanel.textContent = JSON.stringify(json, null, 2);
      activeTab = 'diagnostics';
      renderTabs();
    } else {
      tracePanel.textContent = json.detail || 'Route trace failed';
    }
  } catch (e) {
    if (tracePanel) tracePanel.textContent = 'Route trace failed';
  }
});

// API keys

document.getElementById('create-key').addEventListener('click', async () => {
  const label = document.getElementById('key-label').value || 'app';
  const res = await fetch('/admin/keys', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ label }),
  });
  const json = await res.json();
  document.getElementById('key-output').textContent = `New key: ${json.key}`;
  await load();
});

document.addEventListener('click', async (ev) => {
  const btn = ev.target.closest('.copy-btn');
  if (!btn) return;
  const targetId = btn.dataset.copyTarget;
  const literal = btn.dataset.copy;
  const text = literal || (targetId ? document.getElementById(targetId)?.textContent : '');
  const ok = await copyText((text || '').trim());
  setSetupStatus(ok ? 'Copied to clipboard.' : 'Copy failed. You can copy manually.');
});
</script>
</body>
</html>"""
    return HTMLResponse(html)
@app.get("/favicon.ico")
async def favicon() -> Response:
    return Response(status_code=204)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=SETTINGS.host)
    parser.add_argument("--port", type=int, default=SETTINGS.port)
    args = parser.parse_args()

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
