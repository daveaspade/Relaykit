#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

check_cmd() {
  local name="$1"
  if command -v "$name" >/dev/null 2>&1; then
    echo "  - ${name}: $(command -v "$name")"
  else
    echo "  - ${name}: not found"
  fi
}

echo "RelayKit Doctor"
echo "==============="
echo
echo "System"
echo "  - OS: $(uname -s)"
echo "  - Arch: $(uname -m)"
echo "  - User: $(id -un)"
echo
echo "Core tools"
check_cmd python3
check_cmd python
check_cmd pipx
check_cmd docker
echo
echo "Provider CLIs"
check_cmd opencode
check_cmd claude
check_cmd gemini
check_cmd codex
check_cmd hermes
check_cmd ollama
echo

if [[ -f "${HOME}/.relaykit/config.json" ]]; then
  echo "Config"
  echo "  - ${HOME}/.relaykit/config.json found"
else
  echo "Config"
  echo "  - ${HOME}/.relaykit/config.json not found yet (will be created on first run)"
fi

echo
echo "Next"
echo "  1) ./scripts/install.sh"
echo "  2) ./scripts/run.sh"
echo "  3) open http://127.0.0.1:11436/ui"
