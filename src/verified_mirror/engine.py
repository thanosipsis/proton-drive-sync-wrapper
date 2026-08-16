from __future__ import annotations

import concurrent.futures
import json
import time
import uuid
from collections.abc import Iterator, Sequence
from pathlib import Path, PurePosixPath

from .config import AppConfig
from .errors import ApprovalRequired, MirrorError, PrerequisiteError, RemoteError, SafetyError
from .models import LocalFile, RemoteItem
from .providers.base import Provider
from .state import StateDatabase
from .util import atomic_json, chunks, inventory, now_epoch, stable_digests

STATUS_SCHEMA_VERSION = 2


class Synchronizer:
    def __init__(
        self,
        database: StateDatabase,
        provider: Provider,
        config: AppConfig,
        *,
        dry_run: bool = False,
        full_audit: bool = False,
    ):
        self.database = database
        self.provider = provider
        self.config = config
        self.source = config.source.path.resolve()
        self.status_path = config.state_dir / "status.json"
        self.dry_run = dry_run
        self.full_audit = full_audit
        capabilities = provider.capabilities
        if config.performance.verify_workers > capabilities.max_read_workers:
            raise SafetyError(
                f"Provider supports at most {capabilities.max_read_workers} read workers"
            )
        self.verify_workers = config.performance.verify_workers
        self.maximum_queued_parents = config.performance.maximum_queued_parents
        self.upload_batch_size = min(
            config.performance.upload_batch_size, capabilities.max_upload_batch
        )
        self.run_id = ""
        self.generation = 0
        self.started_at = 0
        self.phase = "waiting"
        self.last_status_write = 0.0
        self.scan_token = uuid.uuid4().hex
        self.remote_cache: dict[str, dict[str, RemoteItem]] = {}
        self.stats = {
            "filesScanned": 0,
            "filesHashed": 0,
            "filesUploaded": 0,
            "filesWouldUpload": 0,
            "filesSkipped": 0,
            "filesVerified": 0,
            "filesTrashed": 0,
            "filesWouldTrash": 0,
            "bytesHashed": 0,
            "bytesUploaded": 0,
            "bytesWouldUpload": 0,
            "totalBytes": 0,
            "pendingDeletionCount": 0,
            "deletionApprovalRequired": False,
        }
        self._validate_layout()

    def _validate_layout(self) -> None:
        state = self.config.state_dir.resolve()
        if state.is_relative_to(self.source) or self.source.is_relative_to(state):
            raise SafetyError("Source and state directories must not overlap")
        if self.provider.capabilities.provider_id == "local-filesystem":
            destination = Path(self.provider.destination_identity).resolve()
            if destination.is_relative_to(self.source) or self.source.is_relative_to(destination):
                raise SafetyError("Source and local destination directories must not overlap")

    def source_marker(self) -> int:
        marker = self.config.source.marker
        if marker is None:
            return 0
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
            completed = int(payload.get("completedAtEpoch") or 0)
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
            raise PrerequisiteError(f"No valid producer success marker at {marker}") from error
        if completed <= 0:
            raise PrerequisiteError("Producer success marker has no positive completedAtEpoch")
        return completed

    def _binding(self) -> dict[str, str]:
        capabilities = self.provider.capabilities
        return self.config.binding(
            destination_identity=self.provider.destination_identity,
            provider_id=capabilities.provider_id,
            path_semantics=capabilities.path_semantics,
        )

    def write_status(
        self,
        state: str,
        failure: MirrorError | None = None,
        *,
        force: bool = False,
    ) -> None:
        monotonic = time.monotonic()
        if not force and monotonic - self.last_status_write < 1.0:
            return
        summary = self.database.summary()
        atomic_json(
            self.status_path,
            {
                "schemaVersion": STATUS_SCHEMA_VERSION,
                "stateVersion": 3,
                "state": state,
                "phase": self.phase,
                "active": state == "running",
                "protected": summary["trustedGeneration"] > 0,
                "runId": self.run_id or None,
                "generatedAtEpoch": now_epoch(),
                "currentStartedAtEpoch": self.started_at,
                "lastTrustedAtEpoch": summary["lastTrustedAtEpoch"],
                "lastCompletedAtEpoch": summary["lastTrustedAtEpoch"],
                "lastResult": (
                    "failed"
                    if state in ("failed", "approval-required")
                    else "success"
                    if summary["trustedGeneration"] > 0
                    else "never"
                ),
                **self.stats,
                "failureCategory": failure.category if failure else None,
                "failureMessage": self._public_failure_message(failure),
            },
            mode=0o644,
        )
        self.last_status_write = monotonic

    @staticmethod
    def _public_failure_message(failure: MirrorError | None) -> str | None:
        if failure is None:
            return None
        messages = {
            "authentication": "Provider authentication failed; inspect private service logs",
            "remote": "Remote operation failed; inspect private service logs",
            "source-changing": "Source changed during a protected read",
            "approval-required": "Deletion approval is required",
            "prerequisite": "A configured prerequisite is unavailable",
            "configuration": "Configuration is invalid",
            "safety": "A safety check blocked the run",
            "internal": "The run failed; inspect private service logs",
        }
        return messages.get(failure.category, messages["internal"])

    def set_phase(self, phase: str) -> None:
        self.phase = phase
        if self.run_id:
            self.database.update_run(self.run_id, "running", phase, self.stats)
        self.write_status("running", force=True)

    def _absolute(self, local: LocalFile) -> LocalFile:
        return LocalFile(
            path=local.path,
            absolute_path=self.source.joinpath(*PurePosixPath(local.path).parts),
            size=local.size,
            inode=local.inode,
            device=local.device,
            mtime_ns=local.mtime_ns,
            ctime_ns=local.ctime_ns,
        )

    def _remote_children(
        self, relative_dir: str, *, refresh: bool = False
    ) -> dict[str, RemoteItem]:
        normalized = "" if relative_dir in ("", ".") else relative_dir
        if refresh or normalized not in self.remote_cache:
            self.remote_cache[normalized] = self._index_remote(
                self.provider.list_dir(normalized), normalized
            )
        return self.remote_cache[normalized]

    @staticmethod
    def _index_remote(items: Sequence[RemoteItem], relative_dir: str) -> dict[str, RemoteItem]:
        indexed: dict[str, RemoteItem] = {}
        for item in items:
            if item.name in indexed:
                raise SafetyError(
                    f"Provider returned duplicate names in directory: {relative_dir or '.'}"
                )
            indexed[item.name] = item
        return indexed

    def _ensure_remote_dir(self, relative_dir: str) -> None:
        if relative_dir in ("", "."):
            return
        relative = PurePosixPath(relative_dir)
        parent = "" if str(relative.parent) == "." else relative.parent.as_posix()
        self._ensure_remote_dir(parent)
        children = self._remote_children(parent)
        existing = children.get(relative.name)
        if existing and existing.item_type == "folder":
            return
        if existing:
            raise SafetyError(f"Remote object blocks required folder: {relative_dir}")
        if self.dry_run:
            self.remote_cache.setdefault(relative_dir, {})
            return
        self.provider.create_folder(parent, relative.name)
        children = self._remote_children(parent, refresh=True)
        if not children.get(relative.name) or children[relative.name].item_type != "folder":
            raise RemoteError(f"Created remote folder did not appear: {relative_dir}")

    def _reconcile_parent(
        self, relative_parent: str, local_files: Sequence[LocalFile]
    ) -> tuple[str, list[tuple[LocalFile, dict[str, str], RemoteItem | None]]]:
        remote_children = self._index_remote(
            self.provider.list_dir(
                "" if relative_parent == "." else relative_parent,
                allow_missing=True,
            ),
            relative_parent,
        )
        algorithm = self.provider.capabilities.checksum_algorithm
        reconciled = []
        for stored in local_files:
            local = self._absolute(stored)
            digests = stable_digests(local, ("sha256", algorithm))
            remote_item = remote_children.get(PurePosixPath(local.path).name)
            if remote_item and remote_item.item_type != "file":
                raise SafetyError(f"Remote object blocks required file: {local.path}")
            reconciled.append((local, digests, remote_item))
        return relative_parent, reconciled

    def _bounded_reconciliation(self, parents: Iterator[str]) -> None:
        algorithm = self.provider.capabilities.checksum_algorithm
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.verify_workers,
            thread_name_prefix="verified-mirror-read",
        ) as executor:
            pending: dict[concurrent.futures.Future, str] = {}

            def fill() -> None:
                while len(pending) < self.maximum_queued_parents:
                    try:
                        parent = next(parents)
                    except StopIteration:
                        return
                    files = self.database.inventory_files(self.run_id, parent, "candidate")
                    pending[executor.submit(self._reconcile_parent, parent, files)] = parent

            fill()
            try:
                while pending:
                    future = next(concurrent.futures.as_completed(tuple(pending)))
                    pending.pop(future)
                    _, reconciled = future.result()
                    for local, digests, remote_item in reconciled:
                        local_digest = digests["sha256"]
                        provider_digest = digests[algorithm]
                        self.stats["filesHashed"] += 1
                        self.stats["bytesHashed"] += local.size
                        if (
                            remote_item
                            and remote_item.item_type == "file"
                            and remote_item.claimed_size == local.size
                            and remote_item.digest_algorithm == algorithm
                            and remote_item.digest == provider_digest
                        ):
                            self.database.stage(
                                self.run_id,
                                local,
                                local_digest,
                                provider_digest,
                                algorithm,
                                remote_item,
                            )
                            self.stats["filesSkipped"] += 1
                            self.stats["filesVerified"] += 1
                        else:
                            self.database.set_inventory_status(
                                self.run_id,
                                local.path,
                                "upload",
                                local_digest,
                                provider_digest,
                            )
                        self.write_status("running")
                    self.database.connection.commit()
                    fill()
            except Exception:
                for future in pending:
                    future.cancel()
                raise

    def run(self, mode: str) -> dict:
        self.database.ensure_binding(self._binding())
        marker = self.source_marker()
        summary = self.database.summary()
        if (
            mode == "sync"
            and marker
            and summary["lastTrustedAtEpoch"]
            and marker <= summary["lastTrustedAtEpoch"]
        ):
            self.phase = "waiting"
            self.write_status("success", force=True)
            return {"state": "waiting", "reason": "No newer producer success marker"}
        if mode == "sync" and self.config.safety.full_audit_interval_days > 0:
            maximum_age = self.config.safety.full_audit_interval_days * 86400
            last_audit = summary["lastFullAuditAtEpoch"]
            if not last_audit or now_epoch() - last_audit >= maximum_age:
                self.full_audit = True

        self.run_id, self.generation, self.started_at, _ = self.database.start_or_resume_run(
            mode, marker
        )
        self.write_status("running", force=True)
        try:
            result = self._run()
            self.database.update_run(self.run_id, "success", "success", self.stats)
            self.phase = "success"
            self.write_status("success", force=True)
            return result
        except MirrorError as error:
            status = "approval-required" if error.category == "approval-required" else "failed"
            self.database.update_run(self.run_id, status, self.phase, self.stats, error)
            self.write_status(status, error, force=True)
            raise
        except Exception as error:
            wrapped = MirrorError(str(error))
            self.database.update_run(self.run_id, "failed", self.phase, self.stats, wrapped)
            self.write_status("failed", wrapped, force=True)
            raise

    def _run(self) -> dict:
        self.set_phase("scanning")
        scanned_since_commit = 0
        for local in inventory(self.source, self.config.source.symlinks):
            first_segment = PurePosixPath(local.path).parts[0]
            if first_segment in self.provider.capabilities.reserved_root_names:
                raise SafetyError(f"Source uses a provider-reserved root name: {first_segment}")
            self.stats["filesScanned"] += 1
            self.stats["totalBytes"] += local.size
            previous_inventory = self.database.record_inventory(self.run_id, local, self.scan_token)
            staged = self.database.staged_file(self.run_id, local.path)
            if (
                staged
                and self.database.metadata_equal(staged, local)
                and staged["local_digest"]
                and staged["remote_digest"]
            ):
                resumed_remote = RemoteItem(
                    name=PurePosixPath(local.path).name,
                    item_type="file",
                    uid=staged["remote_uid"],
                    revision_uid=staged["remote_revision_uid"],
                    claimed_size=staged["remote_claimed_size"],
                    digest_algorithm=staged["remote_digest_algorithm"],
                    digest=staged["remote_digest"],
                )
                self.database.stage(
                    self.run_id,
                    local,
                    str(staged["local_digest"]),
                    str(staged["remote_digest"]),
                    str(staged["remote_digest_algorithm"] or "sha1"),
                    resumed_remote,
                    local_algorithm=str(staged["local_digest_algorithm"]),
                )
                self.stats["filesSkipped"] += 1
                self.stats["filesVerified"] += 1
                continue
            if previous_inventory and previous_inventory["status"] == "upload":
                self.stats["filesHashed"] += 1
                self.stats["bytesHashed"] += local.size
                continue
            trusted = self.database.trusted_file(local.path)
            if (
                trusted
                and self.database.metadata_equal(trusted, local)
                and trusted["local_digest"]
                and not self.full_audit
            ):
                remote = RemoteItem(
                    name=PurePosixPath(local.path).name,
                    item_type="file",
                    uid=trusted["remote_uid"],
                    revision_uid=trusted["remote_revision_uid"],
                    claimed_size=trusted["remote_claimed_size"],
                    digest_algorithm=trusted["remote_digest_algorithm"],
                    digest=trusted["remote_digest"],
                )
                provider_digest = str(trusted["remote_digest"] or trusted["sha1"])
                self.database.stage(
                    self.run_id,
                    local,
                    str(trusted["local_digest"]),
                    provider_digest,
                    str(trusted["remote_digest_algorithm"] or "sha1"),
                    remote,
                    local_algorithm=str(trusted["local_digest_algorithm"]),
                )
                self.stats["filesSkipped"] += 1
                self.stats["filesVerified"] += 1
            scanned_since_commit += 1
            if scanned_since_commit >= 1000:
                self.database.connection.commit()
                scanned_since_commit = 0
            self.write_status("running")
        self.database.connection.commit()
        self.database.finish_inventory(self.run_id, self.scan_token)

        if self.stats["filesScanned"] < self.config.safety.minimum_files:
            raise SafetyError(
                f"Source contains {self.stats['filesScanned']} files; configured minimum is "
                f"{self.config.safety.minimum_files}"
            )
        if self.stats["totalBytes"] < self.config.safety.minimum_bytes:
            raise SafetyError(
                f"Source contains {self.stats['totalBytes']} bytes; configured minimum is "
                f"{self.config.safety.minimum_bytes}"
            )

        self.set_phase("verifying")
        candidate_parents = self.database.inventory_parents(self.run_id, "candidate")
        self._bounded_reconciliation(iter(candidate_parents))

        algorithm = self.provider.capabilities.checksum_algorithm
        upload_parents = self.database.inventory_parents(self.run_id, "upload")
        for parent in upload_parents:
            uploads = self.database.inventory_files(self.run_id, parent, "upload")
            self._ensure_remote_dir(parent)
            for stored_batch in chunks(uploads, self.upload_batch_size):
                batch = [self._absolute(item) for item in stored_batch]
                self.stats["filesWouldUpload"] += len(batch)
                self.stats["bytesWouldUpload"] += sum(item.size for item in batch)
                expected = {
                    item.path: self.database.inventory_digest(self.run_id, item.path)
                    for item in batch
                }
                if not self.dry_run:
                    self.set_phase("uploading")
                    local_paths = [item.absolute_path for item in batch]
                    remote_parent = "" if parent == "." else parent
                    self.provider.upload(local_paths, remote_parent)
                    remote_children = self._index_remote(
                        self.provider.list_dir("" if parent == "." else parent), parent
                    )
                self.set_phase("verifying")
                for local in batch:
                    local_digest, provider_digest = expected[local.path]
                    if self.dry_run:
                        remote_item = RemoteItem(
                            PurePosixPath(local.path).name,
                            "file",
                            None,
                            None,
                            local.size,
                            algorithm,
                            provider_digest,
                        )
                    else:
                        uploaded_item = remote_children.get(PurePosixPath(local.path).name)
                        if not uploaded_item or uploaded_item.item_type != "file":
                            raise RemoteError(f"Uploaded file is absent remotely: {local.path}")
                        if (
                            uploaded_item.claimed_size != local.size
                            or uploaded_item.digest_algorithm != algorithm
                            or uploaded_item.digest != provider_digest
                        ):
                            raise RemoteError(
                                f"Uploaded file metadata does not match local content: {local.path}"
                            )
                        remote_item = uploaded_item
                    self.database.stage(
                        self.run_id,
                        local,
                        local_digest,
                        provider_digest,
                        algorithm,
                        remote_item,
                    )
                    if not self.dry_run:
                        self.stats["filesUploaded"] += 1
                        self.stats["bytesUploaded"] += local.size
                    self.stats["filesVerified"] += 1
                self.database.connection.commit()
                self.write_status("running", force=True)

        self.set_phase("deleting")
        deletions = self.database.prepare_deletions(self.run_id)
        self.stats["pendingDeletionCount"] = len(deletions)
        tracked = max(1, self.database.summary()["trackedFiles"])
        threshold = min(
            self.config.safety.max_delete_files,
            max(1, int(tracked * self.config.safety.max_delete_fraction)),
        )
        deletions_enabled = bool(self.database.meta_int("deletions_enabled"))
        if deletions_enabled and deletions:
            if not self.provider.capabilities.supports_trash:
                raise SafetyError("Configured provider does not support recoverable trash")
            approval_required = len(deletions) > threshold and not self.database.deletion_approved(
                self.run_id, deletions
            )
            self.stats["deletionApprovalRequired"] = approval_required
            if approval_required and not self.dry_run:
                raise ApprovalRequired(
                    f"Deletion approval required for {len(deletions)} paths "
                    f"(threshold {threshold}); run ID {self.run_id}"
                )
            self.stats["filesWouldTrash"] = len(deletions)
            if not self.dry_run:
                for deletion_batch in chunks(deletions, self.upload_batch_size):
                    self.provider.trash(deletion_batch)
                    self.stats["filesTrashed"] += len(deletion_batch)
                    self.write_status("running", force=True)

        if not self.dry_run:
            self.database.commit_generation(
                self.run_id,
                self.generation,
                deletions,
                deletions_processed=deletions_enabled,
                full_audit=self.full_audit,
            )
        return {
            "runId": self.run_id,
            "generation": self.generation,
            "dryRun": self.dry_run,
            "fullAudit": self.full_audit,
            "deletionsEnabled": deletions_enabled,
            **self.stats,
        }
