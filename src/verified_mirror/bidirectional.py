from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .engine import Synchronizer
from .errors import ApprovalRequired, ConflictError, RemoteError, SafetyError
from .models import LocalFile, RemoteItem
from .util import chunks, inventory, stable_digests


@dataclass(frozen=True)
class SyncPlan:
    uploads: tuple[str, ...]
    downloads: tuple[str, ...]
    remote_trash: tuple[str, ...]
    local_trash: tuple[str, ...]
    removed: tuple[str, ...]


def _remote_signature(item: RemoteItem | None) -> tuple | None:
    if item is None:
        return None
    return (
        item.item_type,
        item.uid,
        item.revision_uid,
        item.claimed_size,
        item.digest_algorithm,
        item.digest,
    )


class BidirectionalSynchronizer(Synchronizer):
    """Conservative two-way reconciliation against the last trusted generation."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stats.update(
            {
                "filesDownloaded": 0,
                "filesWouldDownload": 0,
                "bytesDownloaded": 0,
                "bytesWouldDownload": 0,
                "localFilesTrashed": 0,
                "localFilesWouldTrash": 0,
                "conflictCount": 0,
            }
        )

    def run(self, mode: str) -> dict:
        # Keep bidirectional runs separate from legacy upload-only resumable runs.
        return super().run(f"two-way-{mode}")

    @staticmethod
    def _safe_name(name: str) -> bool:
        return bool(name) and name not in {".", ".."} and "/" not in name and "\0" not in name

    def _remote_inventory(self) -> tuple[dict[str, RemoteItem], set[str]]:
        files: dict[str, RemoteItem] = {}
        folders = {""}
        pending = [""]
        while pending:
            parent = pending.pop()
            children = self._index_remote(self.provider.list_dir(parent), parent or ".")
            for name, item in children.items():
                if not self._safe_name(name):
                    raise SafetyError("Remote contains a name that cannot map safely to POSIX")
                relative = name if not parent else f"{parent}/{name}"
                if item.item_type == "folder":
                    if relative in files:
                        raise SafetyError(f"Remote file/folder collision: {relative}")
                    folders.add(relative)
                    pending.append(relative)
                elif item.item_type == "file":
                    if relative in folders:
                        raise SafetyError(f"Remote file/folder collision: {relative}")
                    if not item.digest_algorithm or not item.digest or item.claimed_size is None:
                        raise SafetyError(
                            f"Remote file lacks content evidence required for sync: {relative}"
                        )
                    files[relative] = item
                else:
                    raise SafetyError(
                        f"Unsupported remote item type at {relative}: {item.item_type}"
                    )
        return files, folders

    @staticmethod
    def _metadata_matches(row, local: LocalFile) -> bool:
        return all(
            int(row[key]) == getattr(local, key)
            for key in ("size", "inode", "device", "mtime_ns", "ctime_ns")
        )

    def _digests(
        self, local: LocalFile, trusted, cache: dict[str, dict[str, str]]
    ) -> dict[str, str]:
        cached = cache.get(local.path)
        if cached is not None:
            return cached
        algorithm = self.provider.capabilities.checksum_algorithm
        if trusted is not None and self._metadata_matches(trusted, local):
            provider_digest = trusted["remote_digest"]
            if trusted["remote_digest_algorithm"] == algorithm and provider_digest:
                cached = {"sha256": str(trusted["local_digest"]), algorithm: str(provider_digest)}
                cache[local.path] = cached
                return cached
        cached = stable_digests(local, ("sha256", algorithm))
        cache[local.path] = cached
        self.stats["filesHashed"] += 1
        self.stats["bytesHashed"] += local.size
        return cached

    def _local_matches_trusted(self, local: LocalFile, trusted, digests: dict[str, str]) -> bool:
        return local.size == int(trusted["size"]) and digests["sha256"] == str(
            trusted["local_digest"]
        )

    @staticmethod
    def _remote_matches_trusted(remote: RemoteItem, trusted) -> bool:
        return (
            remote.claimed_size == trusted["remote_claimed_size"]
            and remote.digest_algorithm == trusted["remote_digest_algorithm"]
            and remote.digest == trusted["remote_digest"]
        )

    def _plan(
        self,
        local_files: dict[str, LocalFile],
        remote_files: dict[str, RemoteItem],
        trusted_files: dict,
        digest_cache: dict[str, dict[str, str]],
    ) -> SyncPlan:
        uploads: list[str] = []
        downloads: list[str] = []
        remote_trash: list[str] = []
        local_trash: list[str] = []
        removed: list[str] = []
        conflicts: list[str] = []
        algorithm = self.provider.capabilities.checksum_algorithm

        for path in sorted(set(local_files) | set(remote_files) | set(trusted_files)):
            local = local_files.get(path)
            remote = remote_files.get(path)
            trusted = trusted_files.get(path)
            if local and remote:
                digests = self._digests(local, trusted, digest_cache)
                equivalent = (
                    local.size == remote.claimed_size
                    and remote.digest_algorithm == algorithm
                    and digests[algorithm] == remote.digest
                )
                if trusted is None:
                    if not equivalent:
                        conflicts.append(path)
                    continue
                local_changed = not self._local_matches_trusted(local, trusted, digests)
                remote_changed = not self._remote_matches_trusted(remote, trusted)
                if local_changed and remote_changed and not equivalent:
                    conflicts.append(path)
                elif local_changed and not equivalent:
                    uploads.append(path)
                elif remote_changed and not equivalent:
                    downloads.append(path)
            elif local:
                if trusted is None:
                    uploads.append(path)
                else:
                    digests = self._digests(local, trusted, digest_cache)
                    if self._local_matches_trusted(local, trusted, digests):
                        local_trash.append(path)
                        removed.append(path)
                    else:
                        conflicts.append(path)
            elif remote:
                if trusted is None:
                    downloads.append(path)
                elif self._remote_matches_trusted(remote, trusted):
                    remote_trash.append(path)
                    removed.append(path)
                else:
                    conflicts.append(path)
            else:
                removed.append(path)

        self.stats["conflictCount"] = len(conflicts)
        if conflicts:
            preview = ", ".join(conflicts[:3])
            suffix = "" if len(conflicts) <= 3 else f" and {len(conflicts) - 3} more"
            raise ConflictError(
                f"{len(conflicts)} divergent path(s) require manual resolution: {preview}{suffix}"
            )
        return SyncPlan(
            tuple(uploads),
            tuple(downloads),
            tuple(remote_trash),
            tuple(local_trash),
            tuple(removed),
        )

    def _assert_remote_unchanged(self, path: str, expected: RemoteItem | None) -> None:
        parent, _, name = path.rpartition("/")
        current = self._index_remote(self.provider.list_dir(parent), parent or ".").get(name)
        if _remote_signature(current) != _remote_signature(expected):
            raise ConflictError(f"Remote changed while the sync was running: {path}")

    def _download(self, path: str, remote: RemoteItem, original: LocalFile | None) -> None:
        destination = self.source.joinpath(*PurePosixPath(path).parts)
        if not destination.resolve(strict=False).is_relative_to(self.source):
            raise SafetyError(f"Download path escapes the local root: {path}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._assert_remote_unchanged(path, remote)
        with tempfile.TemporaryDirectory(
            prefix=".proton-drive-sync-wrapper-download-", dir=destination.parent
        ) as temporary:
            downloaded = self.provider.download(path, Path(temporary))
            stat_result = downloaded.stat(follow_symlinks=False)
            candidate = LocalFile(
                path=path,
                absolute_path=downloaded,
                size=stat_result.st_size,
                inode=stat_result.st_ino,
                device=stat_result.st_dev,
                mtime_ns=stat_result.st_mtime_ns,
                ctime_ns=stat_result.st_ctime_ns,
            )
            remote_algorithm = remote.digest_algorithm or "sha1"
            digest = stable_digests(candidate, (remote_algorithm,))
            if candidate.size != remote.claimed_size or digest[remote_algorithm] != remote.digest:
                raise RemoteError(f"Downloaded content does not match remote evidence: {path}")
            if original is not None:
                try:
                    current_stat = destination.stat(follow_symlinks=False)
                except FileNotFoundError as error:
                    raise ConflictError(
                        f"Local changed while the sync was running: {path}"
                    ) from error
                current_key = (
                    current_stat.st_size,
                    current_stat.st_ino,
                    current_stat.st_dev,
                    current_stat.st_mtime_ns,
                    current_stat.st_ctime_ns,
                )
                original_key = (
                    original.size,
                    original.inode,
                    original.device,
                    original.mtime_ns,
                    original.ctime_ns,
                )
                if current_key != original_key:
                    raise ConflictError(f"Local changed while the sync was running: {path}")
            elif destination.exists():
                raise ConflictError(f"Local appeared while the sync was running: {path}")
            os.replace(downloaded, destination)

    def _trash_local(self, path: str, original: LocalFile) -> None:
        source = self.source.joinpath(*PurePosixPath(path).parts)
        current = source.stat(follow_symlinks=False)
        if (
            current.st_size,
            current.st_ino,
            current.st_dev,
            current.st_mtime_ns,
            current.st_ctime_ns,
        ) != (
            original.size,
            original.inode,
            original.device,
            original.mtime_ns,
            original.ctime_ns,
        ):
            raise ConflictError(f"Local changed while the sync was running: {path}")
        destination = self.config.state_dir / "local-trash" / self.run_id
        destination = destination.joinpath(*PurePosixPath(path).parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            raise SafetyError(f"Local trash destination already exists: {path}")
        shutil.move(str(source), str(destination))

    def _run(self) -> dict:
        self.set_phase("scanning")
        local_files = {
            item.path: item for item in inventory(self.source, self.config.source.symlinks)
        }
        remote_files, remote_folders = self._remote_inventory()
        for path in local_files:
            if path in remote_folders:
                raise ConflictError(f"Local file conflicts with a remote folder: {path}")
        for path in remote_files:
            local_path = self.source.joinpath(*PurePosixPath(path).parts)
            if local_path.is_dir():
                raise ConflictError(f"Remote file conflicts with a local folder: {path}")

        self.stats["filesScanned"] = len(local_files) + len(remote_files)
        self.stats["totalBytes"] = sum(item.size for item in local_files.values())
        if len(set(local_files) | set(remote_files)) < self.config.safety.minimum_files:
            raise SafetyError("Combined local and remote inventory is below safety.minimum_files")
        if (
            max(
                self.stats["totalBytes"],
                sum(item.claimed_size or 0 for item in remote_files.values()),
            )
            < self.config.safety.minimum_bytes
        ):
            raise SafetyError("Combined local and remote inventory is below safety.minimum_bytes")

        self.database.connection.execute("DELETE FROM scan_entries WHERE run_id=?", (self.run_id,))
        self.database.connection.commit()
        trusted_files = self.database.trusted_files()
        digest_cache: dict[str, dict[str, str]] = {}
        self.set_phase("planning")
        plan = self._plan(local_files, remote_files, trusted_files, digest_cache)

        deletion_keys = tuple(
            sorted(
                [
                    *(f"local:{path}" for path in plan.local_trash),
                    *(f"remote:{path}" for path in plan.remote_trash),
                ]
            )
        )
        self.database.retain_deletion_set(self.run_id, deletion_keys)
        self.stats["pendingDeletionCount"] = len(deletion_keys)
        self.stats["filesWouldUpload"] = len(plan.uploads)
        self.stats["bytesWouldUpload"] = sum(local_files[path].size for path in plan.uploads)
        self.stats["filesWouldDownload"] = len(plan.downloads)
        self.stats["bytesWouldDownload"] = sum(
            remote_files[path].claimed_size or 0 for path in plan.downloads
        )
        self.stats["filesWouldTrash"] = len(plan.remote_trash)
        self.stats["localFilesWouldTrash"] = len(plan.local_trash)

        tracked = max(1, len(trusted_files))
        threshold = min(
            self.config.safety.max_delete_files,
            max(1, int(tracked * self.config.safety.max_delete_fraction)),
        )
        deletions_enabled = bool(self.database.meta_int("deletions_enabled"))
        if deletion_keys and not deletions_enabled and not self.dry_run:
            raise SafetyError(
                "Deletion propagation is disabled; review the plan and run 'deletions enable'"
            )
        approval_required = len(deletion_keys) > threshold and not self.database.deletion_approved(
            self.run_id, deletion_keys
        )
        self.stats["deletionApprovalRequired"] = approval_required
        if approval_required and not self.dry_run:
            raise ApprovalRequired(
                f"Deletion approval required for {len(deletion_keys)} paths "
                f"(threshold {threshold}); run ID {self.run_id}"
            )

        if self.dry_run:
            return {
                "runId": self.run_id,
                "generation": self.generation,
                "direction": "two-way",
                "dryRun": True,
                "deletionsEnabled": deletions_enabled,
                **self.stats,
            }

        self.set_phase("uploading")
        for parent in sorted({path.rpartition("/")[0] for path in plan.uploads}):
            paths = [path for path in plan.uploads if path.rpartition("/")[0] == parent]
            self._ensure_remote_dir(parent)
            for batch in chunks(paths, self.upload_batch_size):
                for path in batch:
                    self._assert_remote_unchanged(path, remote_files.get(path))
                self.provider.upload([local_files[path].absolute_path for path in batch], parent)
                self.stats["filesUploaded"] += len(batch)
                self.stats["bytesUploaded"] += sum(local_files[path].size for path in batch)

        self.set_phase("downloading")
        for path in plan.downloads:
            self._download(path, remote_files[path], local_files.get(path))
            self.stats["filesDownloaded"] += 1
            self.stats["bytesDownloaded"] += remote_files[path].claimed_size or 0

        self.set_phase("deleting")
        for path in plan.local_trash:
            self._trash_local(path, local_files[path])
            self.stats["localFilesTrashed"] += 1
        for batch in chunks(plan.remote_trash, self.upload_batch_size):
            for path in batch:
                self._assert_remote_unchanged(path, remote_files[path])
            self.provider.trash(batch)
            self.stats["filesTrashed"] += len(batch)

        self.set_phase("verifying")
        final_local = {
            item.path: item for item in inventory(self.source, self.config.source.symlinks)
        }
        final_remote, _ = self._remote_inventory()
        if set(final_local) != set(final_remote):
            raise RemoteError("Local and remote inventories did not converge")
        algorithm = self.provider.capabilities.checksum_algorithm
        for path in sorted(final_local):
            local = final_local[path]
            remote = final_remote[path]
            digests = stable_digests(local, ("sha256", algorithm))
            if local.size != remote.claimed_size or digests[algorithm] != remote.digest:
                raise RemoteError(f"Post-sync content verification failed: {path}")
            self.database.stage(
                self.run_id,
                local,
                digests["sha256"],
                digests[algorithm],
                algorithm,
                remote,
            )
            self.stats["filesVerified"] += 1
        self.database.connection.commit()
        self.database.commit_generation(
            self.run_id,
            self.generation,
            plan.removed,
            deletions_processed=True,
            full_audit=True,
        )
        return {
            "runId": self.run_id,
            "generation": self.generation,
            "direction": "two-way",
            "dryRun": False,
            "deletionsEnabled": deletions_enabled,
            **self.stats,
        }
