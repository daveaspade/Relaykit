# RelayKit

RelayKit is a local OpenAI-compatible gateway that routes requests to OAuth-authenticated CLI providers (OpenCode, Claude, Gemini, Codex, Ollama, and more) and exposes a single base URL for any app that supports OpenAI-compatible endpoints.

## Quick start (one-command)

```bash
cd /path/to/RelayKit
./scripts/install.sh
./scripts/run.sh
```

Open the local UI at:
- http://127.0.0.1:11436/ui

OpenAI-compatible base URL:
- http://127.0.0.1:11436/v1

If your app runs inside Docker, use:
- http://host.docker.internal:11436/v1

## Zero-to-ready checks

```bash
./scripts/doctor.sh
```

`doctor.sh` checks host tools and installed provider CLIs (OpenCode, Claude, Gemini, Codex, Hermes, Ollama).

## Run as a background service (auto-start on boot)

macOS (launchd):

```bash
./scripts/install-service.sh
```

Linux (systemd):

```bash
sudo ./scripts/install-service.sh
```

Default host/port:
- `127.0.0.1:11436`

Override before install/run:

```bash
export RELAYKIT_HOST=127.0.0.1
export RELAYKIT_PORT=11436
```

## What works today

- OpenAI-compatible `/v1/models`
- OpenAI-compatible `/v1/chat/completions`
- Streaming responses
- Auto-detect OpenCode CLI and list models
- Auto-detect local Ollama models (via `http://localhost:11434/api/tags`)
- Optional: read Gemini/Claude settings if enabled
- Native CLI adapters (no OpenCode required): Claude CLI, Gemini CLI, Codex CLI
- Hermes CLI agent adapter
- Auto models for installed CLIs (`claude:auto`, `gemini:auto`, `codex:auto`)
- Hermes model (`hermes` or `hermes:auto`) that lets Hermes choose the underlying provider/model
- OpenAI-compatible local servers (LM Studio) via `http://localhost:1234/v1`
- API keys (local) + audit log
- UI controls for enabling/disabling detected models and tweaking loopback scan ports
- UI routing controls (weights, breaker threshold, retry count, probe interval)
- Embeddings endpoint for Ollama and OpenAI-compatible servers
- Route by model prefix:
  - `claude:...` -> Claude CLI (or OpenCode if available)
  - `gemini:...` -> Gemini CLI (or OpenCode if available)
  - `codex:...` -> Codex CLI (or OpenCode if available)
  - `hermes:...` -> Hermes CLI agent
  - `ollama:...` -> Ollama local API
  - `lmstudio:...` -> LM Studio (OpenAI-compatible)

## Provider detection

RelayKit detects installed CLIs and lists them in the UI. It uses multiple sources so detection works even when OpenCode is not installed:

- OpenCode CLI model list (if installed)
- Ollama local models API
- Optional settings file support (disabled by default):
  - `RELAYKIT_USE_SETTINGS=1`
- Claude/Gemini/Codex CLI adapters can be forced to expose models with env vars:
  - `RELAYKIT_CLAUDE_MODELS=claude-sonnet-4-6,claude-opus-4-6`
  - `RELAYKIT_GEMINI_MODELS=gemini-2.5-pro,gemini-2.5-flash`
  - `RELAYKIT_CODEX_MODELS=gpt-5.4,gpt-5.2`
- Hermes can optionally expose named models too:
  - `RELAYKIT_HERMES_MODELS=hermes:auto,hermes:custom`
- Additional OpenAI-compatible endpoints:
  - `RELAYKIT_COMPAT_URLS=http://127.0.0.1:8080/v1,http://host:port/v1`
- Optional bounded scan ports for local OpenAI-compatible servers:
  - `RELAYKIT_COMPAT_SCAN_PORTS=1234,3000,5000,8000,8080`

## RelayKit v1 execution priorities

This is the practical release order for "one-click on any machine":

1. Packaging + install
- Keep `scripts/install.sh` + `scripts/install-service.sh` stable.
- Add Homebrew + winget + Linux packages (deb/rpm) in CI.
- Publish versioned release artifacts for macOS/Linux/Windows.

2. First-run automation
- Auto-detect local CLI providers and test each in setup.
- Create a default profile for local apps (`/v1`, API key, recommended model aliases).
- Save a known-good starter config in `~/.relaykit/config.json`.

3. Production-grade gateway controls
- Per-app virtual keys, quotas, and audit filtering.
- Routing policy presets (latency-first, reliability-first, cost-first).
- Health probes + provider failover profiles.

4. UX/operations polish
- Setup-first UI tab with copy-safe endpoint snippets.
- Better app templates (OpenClaw, CoPaw, Paperclip, Zo, AionUI).
- Upgrade-safe migration path for config and model overrides.

## Local config + API keys

RelayKit stores config in `~/.relaykit/config.json` (editable from the UI). You can add model lists, aliases, OpenAI-compatible endpoints, and per-model visibility overrides there. The UI writes model toggles under `ui.model_overrides` and scan ports under `ui.scan_ports`.

If you create local API keys, RelayKit requires `Authorization: Bearer <key>` for `/v1/*` requests and writes an audit log to `~/.relaykit/audit.log`.

If you want to force a specific backend later, set:

```bash
export RELAYKIT_BACKEND=opencode
```

## Desktop UI (Tauri)

A minimal desktop UI is included in `apps/desktop`.

```bash
cd apps/desktop
npm install
npm run tauri dev
```

## License

MIT
