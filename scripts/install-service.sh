#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

HOST="${RELAYKIT_HOST:-127.0.0.1}"
PORT="${RELAYKIT_PORT:-11436}"
PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "ERROR: ${PYTHON_BIN} not found. Run ./scripts/install.sh first." >&2
  exit 1
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  PLIST_PATH="${HOME}/Library/LaunchAgents/com.relaykit.server.plist"
  mkdir -p "${HOME}/Library/LaunchAgents"
  cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.relaykit.server</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PYTHON_BIN}</string>
    <string>-m</string>
    <string>relaykit.server</string>
    <string>--host</string><string>${HOST}</string>
    <string>--port</string><string>${PORT}</string>
  </array>
  <key>WorkingDirectory</key><string>${ROOT_DIR}</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/tmp/relaykit.log</string>
  <key>StandardErrorPath</key><string>/tmp/relaykit.log</string>
</dict>
</plist>
EOF
  launchctl unload "$PLIST_PATH" >/dev/null 2>&1 || true
  launchctl load "$PLIST_PATH"
  launchctl kickstart -k "gui/$(id -u)/com.relaykit.server"
  echo "Installed launchd service: com.relaykit.server"
  echo "UI: http://${HOST}:${PORT}/ui"
  exit 0
fi

if [[ "$(uname -s)" == "Linux" ]]; then
  if [[ "${EUID}" -ne 0 ]]; then
    echo "ERROR: Linux systemd install requires root." >&2
    echo "Run with sudo: sudo ./scripts/install-service.sh" >&2
    exit 1
  fi
  SERVICE_PATH="/etc/systemd/system/relaykit.service"
  cat > "$SERVICE_PATH" <<EOF
[Unit]
Description=RelayKit OpenAI-compatible local gateway
After=network.target

[Service]
Type=simple
User=${SUDO_USER:-root}
WorkingDirectory=${ROOT_DIR}
ExecStart=${PYTHON_BIN} -m relaykit.server --host ${HOST} --port ${PORT}
Restart=always
RestartSec=2
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable --now relaykit
  echo "Installed systemd service: relaykit"
  echo "UI: http://${HOST}:${PORT}/ui"
  exit 0
fi

echo "ERROR: Unsupported platform $(uname -s)." >&2
exit 1
