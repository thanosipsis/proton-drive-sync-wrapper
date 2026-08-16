from __future__ import annotations

import json
import secrets

# The provider must invoke the official executable; all calls use argv lists.
import subprocess  # nosec B404
import time
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

from ..errors import AuthenticationError, PrerequisiteError, RemoteError
from ..models import ProviderCapabilities, RemoteItem


def _unwrap(value):
    if isinstance(value, dict) and value.get("ok") is True and "value" in value:
        return value["value"]
    return value


def parse_remote_items(payload) -> list[RemoteItem]:
    if isinstance(payload, dict) and "value" in payload:
        payload = _unwrap(payload)
    if not isinstance(payload, list):
        raise RemoteError("Proton Drive CLI returned an unexpected list payload")
    items: list[RemoteItem] = []
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        name = _unwrap(raw.get("name"))
        if not isinstance(name, str):
            continue
        revision = _unwrap(raw.get("activeRevision"))
        if not isinstance(revision, dict):
            revision = {}
        digests = _unwrap(revision.get("claimedDigests"))
        if not isinstance(digests, dict):
            digests = {}
        claimed_size = _unwrap(revision.get("claimedSize"))
        items.append(
            RemoteItem(
                name=name,
                item_type=str(raw.get("type") or "unknown"),
                uid=str(raw.get("uid")) if raw.get("uid") else None,
                revision_uid=str(revision.get("uid")) if revision.get("uid") else None,
                claimed_size=int(claimed_size) if isinstance(claimed_size, (int, float)) else None,
                digest_algorithm="sha1" if digests.get("sha1") else None,
                digest=str(digests.get("sha1")) if digests.get("sha1") else None,
            )
        )
    return items


def _escape_segment(segment: str) -> str:
    return segment.replace("\\", "\\\\").replace("/", "\\/")


def _join(root: str, relative: str = "") -> str:
    if not relative or relative == ".":
        return root.rstrip("/")
    escaped = "/".join(_escape_segment(part) for part in PurePosixPath(relative).parts)
    return f"{root.rstrip('/')}/{escaped}"


class ProtonDriveProvider:
    """Provider backed exclusively by Proton's official Linux CLI."""

    capabilities = ProviderCapabilities(
        provider_id="proton-drive-cli",
        checksum_algorithm="sha1",
        path_semantics="proton-posix-v1",
        supports_trash=True,
        max_read_workers=8,
        max_upload_batch=20,
    )

    def __init__(self, executable: Path, root: str, retries: int = 5):
        self.executable = executable
        self.root = root.rstrip("/")
        self.retries = retries

    @property
    def destination_identity(self) -> str:
        return self.root

    def validate(self) -> None:
        if not self.executable.is_file() or not self.executable.stat().st_mode & 0o111:
            raise PrerequisiteError(
                f"Official Proton Drive CLI is unavailable or not executable: {self.executable}"
            )
        self.list_dir("", allow_missing=False)

    def _run(
        self,
        args: Sequence[str],
        *,
        json_output: bool = False,
        allow_not_found: bool = False,
    ):
        command = [str(self.executable), *args]
        if json_output:
            command.insert(len(command) - 1, "-j")
        last_message = ""
        for attempt in range(self.retries):
            # No shell is involved, so provider paths and filenames cannot become shell syntax.
            result = subprocess.run(  # nosec B603
                command, capture_output=True, text=True, check=False
            )
            if result.returncode == 0:
                if not json_output:
                    return result.stdout
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError as error:
                    raise RemoteError(f"Invalid JSON from Proton Drive CLI: {error}") from error
            last_message = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
            lowered = last_message.lower()
            if allow_not_found and ("not found" in lowered or "does not exist" in lowered):
                return None
            authentication_words = ("session", "authentication", "login", "credentials")
            if any(word in lowered for word in authentication_words):
                raise AuthenticationError(last_message[:500])
            if attempt + 1 < self.retries:
                jitter = secrets.randbelow(1000) / 1000
                time.sleep(min(60.0, (2**attempt) + jitter))
        raise RemoteError(
            f"Proton Drive CLI failed after {self.retries} attempts: {last_message[:500]}"
        )

    def list_dir(self, relative_dir: str, allow_missing: bool = False) -> list[RemoteItem]:
        payload = self._run(
            ["filesystem", "list", _join(self.root, relative_dir)],
            json_output=True,
            allow_not_found=allow_missing,
        )
        return [] if payload is None else parse_remote_items(payload)

    def create_folder(self, relative_parent: str, name: str) -> None:
        self._run(["filesystem", "create-folder", _join(self.root, relative_parent), name])

    def upload(self, local_paths: Sequence[Path], relative_parent: str) -> None:
        self._run(
            [
                "filesystem",
                "upload",
                "--file-conflict-strategy",
                "merge",
                "--folder-conflict-strategy",
                "merge",
                "--skip-thumbnails",
                *(str(path) for path in local_paths),
                _join(self.root, relative_parent),
            ]
        )

    def download(self, relative_path: str, local_parent: Path) -> Path:
        name = PurePosixPath(relative_path).name
        local_parent.mkdir(parents=True, exist_ok=True)
        self._run(
            [
                "filesystem",
                "download",
                "--file-conflict-strategy",
                "replace",
                _join(self.root, relative_path),
                str(local_parent),
            ]
        )
        downloaded = local_parent / name
        if not downloaded.is_file():
            raise RemoteError(f"Downloaded file did not appear locally: {relative_path}")
        return downloaded

    def trash(self, relative_paths: Sequence[str]) -> None:
        if relative_paths:
            self._run(
                ["filesystem", "trash", *(_join(self.root, path) for path in relative_paths)],
                allow_not_found=True,
            )
