#!/usr/bin/env bash
set -euo pipefail

GOOSE_PROVIDER_FILE="${HOME}/.config/goose/custom_providers/custom_relaykit.json"
RELAYKIT_MODELS_URL="${RELAYKIT_MODELS_URL:-http://127.0.0.1:11436/v1/models}"

if [[ ! -f "${GOOSE_PROVIDER_FILE}" ]]; then
  echo "Missing Goose provider file: ${GOOSE_PROVIDER_FILE}" >&2
  exit 1
fi

python3 - <<'PY'
import datetime
import json
import shutil
import urllib.request
from pathlib import Path

cfg = Path.home() / ".config" / "goose" / "custom_providers" / "custom_relaykit.json"
backup = cfg.with_name(cfg.name + ".bak." + datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
shutil.copy2(cfg, backup)

provider = json.loads(cfg.read_text())
url = "http://127.0.0.1:11436/v1/models"
models_json = json.load(urllib.request.urlopen(url))

seen = set()
model_ids = []
for item in models_json.get("data", []):
    mid = str(item.get("id", "")).strip()
    if not mid or mid in seen:
        continue
    seen.add(mid)
    model_ids.append(mid)

provider["name"] = "custom_relaykit"
provider["engine"] = "openai"
provider["display_name"] = "RelayKit"
provider["description"] = "RelayKit dynamic provider mirror"
provider["base_url"] = "http://127.0.0.1:11436/v1"
provider["api_key_env"] = ""
provider["requires_auth"] = False
provider["supports_streaming"] = True
provider["models"] = [
    {
        "name": mid,
        "context_limit": 128000,
        "input_token_cost": None,
        "output_token_cost": None,
        "currency": None,
        "supports_cache_control": None,
    }
    for mid in model_ids
]

cfg.write_text(json.dumps(provider, indent=2))
print(f"Backup: {backup}")
print(f"Models synced: {len(model_ids)}")
print("First 10:", ", ".join(model_ids[:10]))
PY
