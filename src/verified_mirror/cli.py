from __future__ import annotations

import argparse
import contextlib
import dataclasses
import fcntl
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from collections.abc import Iterator, Sequence
from pathlib import Path

from . import __version__
from .config import AppConfig, load_config
from .engine import Synchronizer
from .errors import ConfigurationError, MirrorError, PrerequisiteError
from .providers import LocalFilesystemProvider, ProtonDriveProvider, Provider
from .state import StateDatabase


@contextlib.contextmanager
def source_lock(path: Path | None) -> Iterator[None]:
    if path is None:
        yield
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def make_provider(config: AppConfig) -> Provider:
    destination = config.destination
    if destination.provider == "local-filesystem":
        return LocalFilesystemProvider(Path(destination.root))
    executable = destination.executable
    if executable is None:
        discovered = shutil.which("proton-drive")
        if not discovered:
            raise PrerequisiteError(
                "Official Proton Drive CLI was not found; configure destination.executable"
            )
        executable = Path(discovered)
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise PrerequisiteError(
            f"Official Proton Drive CLI is unavailable or not executable: {executable}"
        )
    return ProtonDriveProvider(executable, destination.root)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="verified-mirror",
        description="Resumable, safety-first, one-way directory mirroring",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(os.environ.get("VERIFIED_MIRROR_CONFIG", "verified-mirror.toml")),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Write a commented starter configuration")
    commands.add_parser("config-validate", help="Validate configuration without remote access")
    commands.add_parser("doctor", help="Check source, state, and provider access")
    commands.add_parser("sync", help="Run an incremental mirror")
    commands.add_parser("bootstrap", help="Build the first trusted generation with a full audit")
    commands.add_parser("full-audit", help="Re-hash and remotely reconcile every file")
    commands.add_parser("dry-run", help="Plan without changing remote or persistent state")
    commands.add_parser("status", help="Print machine-readable state summary")
    backup = commands.add_parser(
        "state-backup", help="Create a consistent SQLite backup without migrating state"
    )
    backup.add_argument("output", type=Path)
    deletions = commands.add_parser("deletions", help="Manage the remote-trash feature gate")
    deletions.add_argument("action", choices=("enable", "disable", "status"))
    approval = commands.add_parser("approve-deletions", help="Approve one exact deletion set")
    approval.add_argument("run_id")
    state = commands.add_parser("state-bind", help="Bind imported state to this configuration")
    state.add_argument("--expect-tracked-files", required=True, type=int)
    return parser


STARTER_CONFIG = """# Verified Mirror configuration
[source]
path = "/path/to/source"
# marker = "/path/to/producer-success.json"
# lock_file = "/run/lock/verified-mirror/source.lock"
symlinks = "reject"

[destination]
provider = "proton-drive"
root = "/my-files/Server Backup"
# executable = "/usr/local/bin/proton-drive"

[state]
directory = "/var/lib/verified-mirror"

[safety]
minimum_files = 1
minimum_bytes = 0
max_delete_files = 500
max_delete_fraction = 0.05
full_audit_interval_days = 30

[performance]
verify_workers = 4
upload_batch_size = 20
maximum_queued_parents = 8
"""


def _init(path: Path) -> int:
    if path.exists():
        raise ConfigurationError(f"Refusing to overwrite existing configuration: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(STARTER_CONFIG, encoding="utf-8")
    os.chmod(path, 0o600)
    print(path)
    return 0


def _open_database(config: AppConfig) -> StateDatabase:
    config.state_dir.mkdir(parents=True, exist_ok=True)
    return StateDatabase(config.state_dir / "index.sqlite3")


def _backup_state(config: AppConfig, output: Path) -> int:
    source_path = config.state_dir / "index.sqlite3"
    if not source_path.is_file():
        raise PrerequisiteError(f"State database is unavailable: {source_path}")
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as error:
        raise ConfigurationError(f"Refusing to overwrite existing backup: {output}") from error
    os.close(descriptor)
    source: sqlite3.Connection | None = None
    destination: sqlite3.Connection | None = None
    backup_succeeded = False
    try:
        source = sqlite3.connect(f"{source_path.resolve().as_uri()}?mode=ro", uri=True)
        destination = sqlite3.connect(output)
        source.backup(destination)
        integrity = str(destination.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise PrerequisiteError(f"Backup integrity check failed: {integrity}")
        backup_succeeded = True
    finally:
        if destination is not None:
            destination.close()
        if source is not None:
            source.close()
        if not backup_succeeded:
            output.unlink(missing_ok=True)
    print(json.dumps({"backup": str(output), "integrity": "ok"}, sort_keys=True))
    return 0


def _run_dry(config: AppConfig, provider: Provider, source_database: StateDatabase) -> dict:
    with tempfile.TemporaryDirectory(prefix="verified-mirror-dry-run-") as temporary:
        temporary_state = Path(temporary)
        dry_config = dataclasses.replace(config, state_dir=temporary_state)
        dry_database = StateDatabase(temporary_state / "index.sqlite3")
        try:
            source_database.connection.backup(dry_database.connection)
            synchronizer = Synchronizer(
                dry_database,
                provider,
                dry_config,
                dry_run=True,
                full_audit=source_database.meta_int("trusted_generation") == 0,
            )
            return synchronizer.run("dry-run")
        finally:
            dry_database.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "init":
            return _init(args.config)
        config = load_config(args.config)
        if args.command == "config-validate":
            print(json.dumps({"valid": True, "config": str(args.config)}))
            return 0
        if args.command == "state-backup":
            return _backup_state(config, args.output)

        database = _open_database(config)
        try:
            if args.command == "status":
                print(json.dumps(database.summary(), sort_keys=True))
                return 0
            provider = make_provider(config)
            binding = config.binding(
                destination_identity=provider.destination_identity,
                provider_id=provider.capabilities.provider_id,
                path_semantics=provider.capabilities.path_semantics,
            )
            if args.command == "state-bind":
                database.bind(binding, expected_tracked_files=args.expect_tracked_files)
                print(json.dumps({"bound": True, "binding": binding}, sort_keys=True))
                return 0
            if args.command == "deletions":
                database.ensure_binding(binding)
                if args.action == "enable":
                    database.set_meta("deletions_enabled", 1)
                elif args.action == "disable":
                    database.set_meta("deletions_enabled", 0)
                print(
                    json.dumps({"deletionsEnabled": bool(database.meta_int("deletions_enabled"))})
                )
                return 0
            if args.command == "approve-deletions":
                database.ensure_binding(binding)
                digest, count = database.approve_deletions(args.run_id)
                print(
                    json.dumps(
                        {"runId": args.run_id, "approvedCount": count, "digest": digest},
                        sort_keys=True,
                    )
                )
                return 0
            if args.command == "doctor":
                database.ensure_binding(binding)
                provider.validate()
                if not config.source.path.is_dir():
                    raise PrerequisiteError(f"Source is unavailable: {config.source.path}")
                print(
                    json.dumps(
                        {
                            "healthy": True,
                            "provider": provider.capabilities.provider_id,
                            "stateIntegrity": database.integrity_check(),
                            "binding": binding,
                        },
                        sort_keys=True,
                    )
                )
                return 0

            with source_lock(config.source.lock_file):
                if args.command == "dry-run":
                    result = _run_dry(config, provider, database)
                else:
                    synchronizer = Synchronizer(
                        database,
                        provider,
                        config,
                        full_audit=args.command in ("bootstrap", "full-audit")
                        or database.meta_int("trusted_generation") == 0,
                    )
                    result = synchronizer.run(args.command)
            print(json.dumps(result, sort_keys=True))
            return 0
        finally:
            database.close()
    except MirrorError as error:
        print(f"{error.category}: {error}", file=sys.stderr)
        return 4 if error.category in ("remote", "source-changing") else 3


if __name__ == "__main__":
    raise SystemExit(main())
