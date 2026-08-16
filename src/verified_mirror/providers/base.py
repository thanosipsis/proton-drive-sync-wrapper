from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

from ..models import ProviderCapabilities, RemoteItem


class Provider(Protocol):
    @property
    def capabilities(self) -> ProviderCapabilities: ...

    @property
    def destination_identity(self) -> str: ...

    def validate(self) -> None: ...

    def list_dir(self, relative_dir: str, allow_missing: bool = False) -> list[RemoteItem]: ...

    def create_folder(self, relative_parent: str, name: str) -> None: ...

    def upload(self, local_paths: Sequence[Path], relative_parent: str) -> None: ...

    def download(self, relative_path: str, local_parent: Path) -> Path: ...

    def trash(self, relative_paths: Sequence[str]) -> None: ...
