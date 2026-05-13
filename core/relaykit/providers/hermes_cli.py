import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Generator, Iterable, List, Optional


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_SESSION_FOOTER_RE = re.compile(r"(?:\r?\n)?session_id:\s*[A-Za-z0-9._:-]+\s*$", re.IGNORECASE)


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def _clean_lines(text: str) -> List[str]:
    lines: List[str] = []
    for raw in _strip_ansi(text).splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.lower().startswith("session_id:"):
            continue
        if line.startswith(("INFO:", "WARN:", "WARNING:", "DEBUG:", "ERROR:")):
            continue
        if line.startswith(("╭", "╰", "│", "─")):
            continue
        lines.append(line)
    return lines


def _dedupe_consecutive_lines(lines: List[str]) -> List[str]:
    deduped: List[str] = []
    prev = None
    for line in lines:
        if line != prev:
            deduped.append(line)
        prev = line
    return deduped


class HermesCLIBackend:
    def __init__(self) -> None:
        self.command = os.environ.get("RELAYKIT_HERMES_COMMAND", "").strip()
        if self.command:
            self.available = shutil.which(self.command) is not None or os.path.exists(self.command)
        else:
            self.command = "hermes"
            self.available = shutil.which("hermes") is not None
        self._repo_root = self._detect_repo_root()
        self._library_agent_class = None

    def list_models(self) -> List[str]:
        env = os.environ.get("RELAYKIT_HERMES_MODELS", "")
        models = [m.strip() for m in env.split(",") if m.strip()]
        if "auto" not in models:
            models.insert(0, "auto")
        return models

    def _resolve_command_path(self) -> Optional[Path]:
        command = (self.command or "").strip()
        if not command:
            return None

        candidate = Path(command).expanduser()
        if candidate.exists():
            return candidate

        resolved = shutil.which(command)
        if resolved:
            return Path(resolved).expanduser()
        return None

    def _detect_repo_root(self) -> Optional[Path]:
        env_repo = os.environ.get("RELAYKIT_HERMES_REPO", "").strip()
        if env_repo:
            candidate = Path(env_repo).expanduser()
            if (candidate / "run_agent.py").exists():
                return candidate

        command_path = self._resolve_command_path()
        if not command_path:
            return None

        try:
            command_path = command_path.resolve()
        except Exception:
            pass

        for parent in command_path.parents:
            if (parent / "run_agent.py").exists():
                return parent
        return None

    def _load_library_agent_class(self):
        if self._library_agent_class is not None:
            return self._library_agent_class
        if not self._repo_root:
            self._library_agent_class = False
            return None

        repo_root = str(self._repo_root)
        inserted = False
        try:
            if repo_root not in sys.path:
                sys.path.insert(0, repo_root)
                inserted = True
            from run_agent import AIAgent  # type: ignore

            self._library_agent_class = AIAgent
            return AIAgent
        except Exception:
            self._library_agent_class = False
            return None
        finally:
            if inserted and sys.path and sys.path[0] == repo_root:
                sys.path.pop(0)

    def _normalize_model(self, model: str) -> str:
        value = (model or "").strip()
        if not value:
            return "auto"
        lower = value.lower()
        if lower in {"auto", "default"}:
            return "auto"
        if lower.startswith("hermes:"):
            return value.split(":", 1)[1].strip() or "auto"
        if lower.startswith("hermes/"):
            return value.split("/", 1)[1].strip() or "auto"
        return value

    def chat(
        self,
        model: str,
        prompt: str,
        stream: bool,
        system_prompt: str = "",
        api_key: str = "",
        base_url: str = "",
    ) -> Iterable[str]:
        if not self.available:
            raise RuntimeError(
                "Hermes CLI is not installed or RELAYKIT_HERMES_COMMAND is not set correctly",
            )

        model = self._normalize_model(model)
        use_model = model.lower() not in {"auto", "default"}
        default_model = os.environ.get("RELAYKIT_HERMES_DEFAULT_MODEL", "codex:auto")
        chosen_model = model if use_model else default_model
        system_prompt = (system_prompt or os.environ.get("RELAYKIT_HERMES_EPHEMERAL_SYSTEM_PROMPT", "")).strip()
        api_key = (api_key or os.environ.get("RELAYKIT_HERMES_API_KEY", "")).strip()
        base_url = (base_url or os.environ.get("RELAYKIT_HERMES_BASE_URL", "") or "http://127.0.0.1:11436/v1").strip()

        library_agent_class = self._load_library_agent_class()
        if library_agent_class is not None:
            try:
                # Avoid recursive routing through RelayKit-prefixed aliases by default.
                agent = library_agent_class(
                    model=chosen_model,
                    provider="custom",
                    base_url=base_url,
                    api_key=api_key or "no-key-required",
                    quiet_mode=True,
                    skip_context_files=True,
                    skip_memory=True,
                    enabled_toolsets=[],
                    max_iterations=int(os.environ.get("RELAYKIT_HERMES_MAX_ITERATIONS", "1")),
                    ephemeral_system_prompt=system_prompt or None,
                )

                if stream:
                    def gen() -> Generator[str, None, None]:
                        result = agent.chat(prompt)
                        if result:
                            cleaned = _SESSION_FOOTER_RE.sub("", _strip_ansi(result))
                            for line in _dedupe_consecutive_lines(_clean_lines(cleaned)):
                                yield line

                    return gen()

                result = agent.chat(prompt)
                cleaned = _SESSION_FOOTER_RE.sub("", _strip_ansi(result or ""))
                lines = _dedupe_consecutive_lines(_clean_lines(cleaned))
                if lines:
                    return ["\n".join(lines)]
                return []
            except Exception:
                pass

        cmd = [self.command, "chat", "-Q", "--source", "tool"]
        if system_prompt or api_key or base_url:
            cmd_env = os.environ.copy()
            if system_prompt:
                cmd_env["HERMES_EPHEMERAL_SYSTEM_PROMPT"] = system_prompt
            if api_key:
                cmd_env["RELAYKIT_HERMES_API_KEY"] = api_key
            if base_url:
                cmd_env["RELAYKIT_HERMES_BASE_URL"] = base_url
        else:
            cmd_env = None
        # Always pass a concrete model to avoid inheriting stale Hermes config defaults.
        cmd += ["--model", chosen_model]
        cmd += ["-q", prompt]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=cmd_env,
            text=True,
        )
        if proc.stdout is None:
            return []

        if stream:
            def gen() -> Generator[str, None, None]:
                for line in proc.stdout:
                    line = _SESSION_FOOTER_RE.sub("", _strip_ansi(line))
                    for cleaned in _clean_lines(line):
                        yield cleaned
            return gen()

        try:
            out, _ = proc.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
            if not out:
                return []
        out = _SESSION_FOOTER_RE.sub("", _strip_ansi(out))
        lines = _dedupe_consecutive_lines(_clean_lines(out))
        if lines:
            return ["\n".join(lines)]
        return []
