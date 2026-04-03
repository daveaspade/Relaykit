import os
from dataclasses import dataclass


@dataclass
class Settings:
    host: str = os.environ.get("RELAYKIT_HOST", "127.0.0.1")
    port: int = int(os.environ.get("RELAYKIT_PORT", "11435"))
    backend: str = os.environ.get("RELAYKIT_BACKEND", "opencode")


SETTINGS = Settings()
