from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import TypeVar

from .errors import ChangedDuringRead, PrerequisiteError, SafetyError
from .models import LocalFile

T = TypeVar("T")


def now_epoch() -> int:
    import time

    return int(time.time())


def chunks(items: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    for offset in range(0, len(items), size):
        yield items[offset : offset + size]


def atomic_json(path: Path, value: dict, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, mode)
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def inventory(source: Path, symlinks: str = "reject") -> Iterator[LocalFile]:
    if not source.is_dir():
        raise PrerequisiteError(f"Source directory is unavailable: {source}")
    for root, directory_names, file_names in os.walk(source, followlinks=False):
        directory_names.sort()
        file_names.sort()
        root_path = Path(root)
        for directory_name in list(directory_names):
            directory = root_path / directory_name
            if directory.is_symlink():
                if symlinks == "ignore":
                    directory_names.remove(directory_name)
                    continue
                raise SafetyError(f"Symbolic-link directory is not supported: {directory}")
        for name in file_names:
            absolute = root_path / name
            stat_result = absolute.stat(follow_symlinks=False)
            if absolute.is_symlink():
                if symlinks == "ignore":
                    continue
                raise SafetyError(f"Symbolic link is not supported: {absolute}")
            if not absolute.is_file():
                raise SafetyError(f"Non-regular source entry is not supported: {absolute}")
            yield LocalFile(
                path=absolute.relative_to(source).as_posix(),
                absolute_path=absolute,
                size=stat_result.st_size,
                inode=stat_result.st_ino,
                device=stat_result.st_dev,
                mtime_ns=stat_result.st_mtime_ns,
                ctime_ns=stat_result.st_ctime_ns,
            )


def stable_digests(
    local: LocalFile,
    algorithms: Sequence[str],
    block_size: int = 4 * 1024 * 1024,
) -> dict[str, str]:
    unique_algorithms = tuple(dict.fromkeys(algorithm.lower() for algorithm in algorithms))
    digests = {
        algorithm: hashlib.new(algorithm, usedforsecurity=False) for algorithm in unique_algorithms
    }
    with local.absolute_path.open("rb", buffering=0) as handle:
        before = os.fstat(handle.fileno())
        while block := handle.read(block_size):
            for digest in digests.values():
                digest.update(block)
        after = os.fstat(handle.fileno())
    before_key = (
        before.st_size,
        before.st_ino,
        before.st_dev,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    after_key = (
        after.st_size,
        after.st_ino,
        after.st_dev,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    expected_key = (
        local.size,
        local.inode,
        local.device,
        local.mtime_ns,
        local.ctime_ns,
    )
    if before_key != after_key or before_key != expected_key:
        raise ChangedDuringRead(f"File changed while being read: {local.path}")
    return {algorithm: digest.hexdigest() for algorithm, digest in digests.items()}
