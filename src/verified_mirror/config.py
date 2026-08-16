from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import dataclass
from pathlib import Path

from .errors import ConfigurationError


@dataclass(frozen=True)
class SourceConfig:
    path: Path
    marker: Path | None = None
    lock_file: Path | None = None
    symlinks: str = "reject"


@dataclass(frozen=True)
class DestinationConfig:
    provider: str
    root: str
    executable: Path | None = None


@dataclass(frozen=True)
class SafetyConfig:
    minimum_files: int = 1
    minimum_bytes: int = 0
    max_delete_files: int = 500
    max_delete_fraction: float = 0.05
    full_audit_interval_days: int = 30


@dataclass(frozen=True)
class PerformanceConfig:
    verify_workers: int = 4
    upload_batch_size: int = 20
    maximum_queued_parents: int = 8


@dataclass(frozen=True)
class AppConfig:
    source: SourceConfig
    destination: DestinationConfig
    state_dir: Path
    safety: SafetyConfig
    performance: PerformanceConfig

    def binding(
        self, *, destination_identity: str, provider_id: str, path_semantics: str
    ) -> dict[str, str]:
        source_identity = str(self.source.path.resolve())
        values = {
            "source": source_identity,
            "provider": provider_id,
            "destination": destination_identity,
            "path_semantics": path_semantics,
        }
        canonical = json.dumps(values, sort_keys=True, separators=(",", ":"))
        values["fingerprint"] = hashlib.sha256(canonical.encode()).hexdigest()
        return values


def _table(payload: dict, key: str) -> dict:
    value = payload.get(key, {})
    if not isinstance(value, dict):
        raise ConfigurationError(f"Configuration section [{key}] must be a table")
    return value


def _optional_path(value) -> Path | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise ConfigurationError("Configured paths must be strings")
    return Path(value).expanduser()


def load_config(path: Path) -> AppConfig:
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ConfigurationError(f"Cannot load configuration {path}: {error}") from error

    source = _table(payload, "source")
    destination = _table(payload, "destination")
    state = _table(payload, "state")
    safety = _table(payload, "safety")
    performance = _table(payload, "performance")

    try:
        source_path = Path(source["path"]).expanduser()
        provider = str(destination["provider"])
        destination_root = str(destination["root"])
        state_dir = Path(state["directory"]).expanduser()
    except KeyError as error:
        raise ConfigurationError(f"Missing required configuration key: {error.args[0]}") from error

    symlinks = str(source.get("symlinks", "reject"))
    if symlinks not in {"reject", "ignore"}:
        raise ConfigurationError("source.symlinks must be 'reject' or 'ignore'")
    if provider not in {"proton-drive", "local-filesystem"}:
        raise ConfigurationError(
            "destination.provider must be 'proton-drive' or 'local-filesystem'"
        )

    safety_config = SafetyConfig(
        minimum_files=int(safety.get("minimum_files", 1)),
        minimum_bytes=int(safety.get("minimum_bytes", 0)),
        max_delete_files=int(safety.get("max_delete_files", 500)),
        max_delete_fraction=float(safety.get("max_delete_fraction", 0.05)),
        full_audit_interval_days=int(safety.get("full_audit_interval_days", 30)),
    )
    performance_config = PerformanceConfig(
        verify_workers=int(performance.get("verify_workers", 4)),
        upload_batch_size=int(performance.get("upload_batch_size", 20)),
        maximum_queued_parents=int(performance.get("maximum_queued_parents", 8)),
    )
    if safety_config.minimum_files < 0 or safety_config.minimum_bytes < 0:
        raise ConfigurationError("Minimum source thresholds cannot be negative")
    if safety_config.max_delete_files < 1:
        raise ConfigurationError("safety.max_delete_files must be at least 1")
    if not 0 < safety_config.max_delete_fraction <= 1:
        raise ConfigurationError("safety.max_delete_fraction must be greater than 0 and at most 1")
    if not 1 <= performance_config.verify_workers <= 64:
        raise ConfigurationError("performance.verify_workers must be between 1 and 64")
    if not 1 <= performance_config.upload_batch_size <= 1000:
        raise ConfigurationError("performance.upload_batch_size must be between 1 and 1000")
    if performance_config.maximum_queued_parents < performance_config.verify_workers:
        raise ConfigurationError(
            "performance.maximum_queued_parents cannot be smaller than verify_workers"
        )

    return AppConfig(
        source=SourceConfig(
            path=source_path,
            marker=_optional_path(source.get("marker")),
            lock_file=_optional_path(source.get("lock_file")),
            symlinks=symlinks,
        ),
        destination=DestinationConfig(
            provider=provider,
            root=destination_root,
            executable=_optional_path(destination.get("executable")),
        ),
        state_dir=state_dir,
        safety=safety_config,
        performance=performance_config,
    )
