#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -d ".venv" ]]; then
  echo "ERROR: .venv not found. Run ./scripts/install.sh first." >&2
  exit 1
fi

source .venv/bin/activate

HOST="${RELAYKIT_HOST:-127.0.0.1}"
PORT="${RELAYKIT_PORT:-11436}"

echo "Starting RelayKit on http://${HOST}:${PORT}"
python -m relaykit.server --host "$HOST" --port "$PORT"
