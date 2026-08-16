from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

from ..errors import PrerequisiteError, RemoteError, SafetyError
from ..models import ProviderCapabilities, RemoteItem


class LocalFilesystemProvider:
    """Reference provider for tests and mirrors to another mounted filesystem."""

    capabilities = ProviderCapabilities(
        provider_id="local-filesystem",
        checksum_algorithm="sha256",
        path_semantics="linux-posix-v1",
        supports_trash=True,
        max_read_workers=16,
        max_upload_batch=100,
        reserved_root_names=(
            ".proton-drive-sync-wrapper-trash",
            ".proton-drive-relay-trash",
            ".verified-mirror-trash",
        ),
    )

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.trash_root = self.root / ".proton-drive-sync-wrapper-trash"
        self.relay_trash_root = self.root / ".proton-drive-relay-trash"
        self.legacy_trash_root = self.root / ".verified-mirror-trash"

    @property
    def destination_identity(self) -> str:
        return str(self.root)

    def validate(self) -> None:
        if not self.root.is_dir():
            raise PrerequisiteError(f"Local destination directory is unavailable: {self.root}")

    def _path(self, relative: str) -> Path:
        relative_path = PurePosixPath(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise SafetyError(f"Unsafe destination-relative path: {relative}")
        candidate = self.root.joinpath(*relative_path.parts)
        if not candidate.resolve(strict=False).is_relative_to(self.root):
            raise SafetyError(f"Destination path escapes configured root: {relative}")
        return candidate

    @staticmethod
    def _digest(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while block := handle.read(4 * 1024 * 1024):
                digest.update(block)
        return digest.hexdigest()

    def list_dir(self, relative_dir: str, allow_missing: bool = False) -> list[RemoteItem]:
        directory = self._path(relative_dir)
        if not directory.exists() and allow_missing:
            return []
        if not directory.is_dir():
            raise RemoteError(f"Local destination directory is unavailable: {directory}")
        items: list[RemoteItem] = []
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if path in (self.trash_root, self.relay_trash_root, self.legacy_trash_root):
                continue
            stat_result = path.stat(follow_symlinks=False)
            if path.is_symlink():
                raise SafetyError(f"Destination symbolic link is not supported: {path}")
            items.append(
                RemoteItem(
                    name=path.name,
                    item_type="folder" if path.is_dir() else "file",
                    uid=f"{stat_result.st_dev}:{stat_result.st_ino}",
                    revision_uid=str(stat_result.st_mtime_ns),
                    claimed_size=stat_result.st_size if path.is_file() else None,
                    digest_algorithm="sha256" if path.is_file() else None,
                    digest=self._digest(path) if path.is_file() else None,
                )
            )
        return items

    def create_folder(self, relative_parent: str, name: str) -> None:
        parent = self._path(relative_parent)
        target = parent / name
        target.mkdir(exist_ok=True)

    def upload(self, local_paths: Sequence[Path], relative_parent: str) -> None:
        parent = self._path(relative_parent)
        parent.mkdir(parents=True, exist_ok=True)
        for source in local_paths:
            temporary = parent / f".{source.name}.{uuid.uuid4().hex}.tmp"
            try:
                with source.open("rb") as source_handle, temporary.open("xb") as destination:
                    shutil.copyfileobj(source_handle, destination, 4 * 1024 * 1024)
                    destination.flush()
                    os.fsync(destination.fileno())
                os.replace(temporary, parent / source.name)
            finally:
                temporary.unlink(missing_ok=True)

    def download(self, relative_path: str, local_parent: Path) -> Path:
        source = self._path(relative_path)
        if not source.is_file():
            raise RemoteError(f"Local provider file is unavailable: {source}")
        local_parent.mkdir(parents=True, exist_ok=True)
        destination = local_parent / source.name
        with source.open("rb") as source_handle, destination.open("xb") as target_handle:
            shutil.copyfileobj(source_handle, target_handle, 4 * 1024 * 1024)
            target_handle.flush()
            os.fsync(target_handle.fileno())
        return destination

    def trash(self, relative_paths: Sequence[str]) -> None:
        for relative in relative_paths:
            source = self._path(relative)
            if not source.exists():
                continue
            destination = self.trash_root.joinpath(*PurePosixPath(relative).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                destination = destination.with_name(f"{destination.name}.{uuid.uuid4().hex}")
            os.replace(source, destination)
