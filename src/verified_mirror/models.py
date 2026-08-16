from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LocalFile:
    path: str
    absolute_path: Path
    size: int
    inode: int
    device: int
    mtime_ns: int
    ctime_ns: int


@dataclass(frozen=True)
class RemoteItem:
    name: str
    item_type: str
    uid: str | None
    revision_uid: str | None
    claimed_size: int | None
    digest_algorithm: str | None
    digest: str | None


@dataclass(frozen=True)
class ProviderCapabilities:
    provider_id: str
    checksum_algorithm: str
    path_semantics: str
    supports_trash: bool
    max_read_workers: int = 8
    max_upload_batch: int = 20
    reserved_root_names: tuple[str, ...] = ()
