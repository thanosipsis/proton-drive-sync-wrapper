from __future__ import annotations

import dataclasses
import hashlib
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from verified_mirror.bidirectional import BidirectionalSynchronizer
from verified_mirror.config import (
    AppConfig,
    DestinationConfig,
    PerformanceConfig,
    SafetyConfig,
    SourceConfig,
    SyncConfig,
)
from verified_mirror.engine import Synchronizer
from verified_mirror.errors import ApprovalRequired, ConflictError, PrerequisiteError, SafetyError
from verified_mirror.models import RemoteItem
from verified_mirror.providers.local import LocalFilesystemProvider
from verified_mirror.providers.proton import parse_remote_items
from verified_mirror.state import StateDatabase
from verified_mirror.util import inventory, stable_digests


class TrackingLocalProvider(LocalFilesystemProvider):
    def __init__(self, root: Path, delay: float = 0):
        super().__init__(root)
        self.delay = delay
        self.active_lists = 0
        self.max_active_lists = 0
        self.lock = threading.Lock()

    def list_dir(self, relative_dir: str, allow_missing: bool = False):
        with self.lock:
            self.active_lists += 1
            self.max_active_lists = max(self.max_active_lists, self.active_lists)
        try:
            if self.delay:
                time.sleep(self.delay)
            return super().list_dir(relative_dir, allow_missing)
        finally:
            with self.lock:
                self.active_lists -= 1


class FailingUploadProvider(LocalFilesystemProvider):
    def upload(self, local_paths, relative_parent):
        raise RuntimeError("injected upload interruption")


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.destination = self.root / "destination"
        self.state = self.root / "state"
        self.source.mkdir()
        self.destination.mkdir()
        self.config = AppConfig(
            source=SourceConfig(self.source),
            destination=DestinationConfig("local-filesystem", str(self.destination)),
            state_dir=self.state,
            safety=SafetyConfig(full_audit_interval_days=0),
            performance=PerformanceConfig(
                verify_workers=4, upload_batch_size=20, maximum_queued_parents=4
            ),
        )
        self.database = StateDatabase(self.state / "index.sqlite3")
        self.provider = LocalFilesystemProvider(self.destination)

    def tearDown(self):
        self.database.close()
        self.temporary.cleanup()

    def synchronizer(self, **kwargs):
        return Synchronizer(self.database, self.provider, self.config, **kwargs)

    def test_bootstrap_and_metadata_fast_path(self):
        (self.source / "a").write_bytes(b"alpha")
        nested = self.source / "nested"
        nested.mkdir()
        (nested / "b").write_bytes(b"beta")

        first = self.synchronizer(full_audit=True).run("bootstrap")
        second = self.synchronizer().run("sync")

        self.assertEqual(first["filesUploaded"], 2)
        self.assertEqual(second["filesHashed"], 0)
        self.assertEqual(second["filesSkipped"], 2)
        self.assertEqual((self.destination / "a").read_bytes(), b"alpha")

    def test_state_binding_rejects_destination_change(self):
        (self.source / "a").write_bytes(b"alpha")
        self.synchronizer(full_audit=True).run("bootstrap")
        other = self.root / "other"
        other.mkdir()
        changed = dataclasses.replace(
            self.config,
            destination=DestinationConfig("local-filesystem", str(other)),
        )
        with self.assertRaises(SafetyError):
            Synchronizer(self.database, LocalFilesystemProvider(other), changed).run("sync")

    def test_state_binding_rejects_direction_change(self):
        (self.source / "a").write_bytes(b"alpha")
        self.synchronizer(full_audit=True).run("bootstrap")
        changed = dataclasses.replace(self.config, sync=SyncConfig(direction="two-way"))
        with self.assertRaises(SafetyError):
            BidirectionalSynchronizer(self.database, self.provider, changed).run("sync")

    def test_source_state_and_local_destination_must_not_overlap(self):
        nested_state = dataclasses.replace(self.config, state_dir=self.source / "state")
        with self.assertRaises(SafetyError):
            Synchronizer(self.database, self.provider, nested_state)

        nested_destination = self.source / "destination"
        nested_destination.mkdir()
        overlapping = dataclasses.replace(
            self.config,
            destination=DestinationConfig("local-filesystem", str(nested_destination)),
        )
        with self.assertRaises(SafetyError):
            Synchronizer(
                self.database,
                LocalFilesystemProvider(nested_destination),
                overlapping,
            )

    def test_disabled_deletion_tombstones_then_trashes(self):
        keep = self.source / "keep"
        remove = self.source / "remove"
        keep.write_bytes(b"keep")
        remove.write_bytes(b"remove")
        self.synchronizer(full_audit=True).run("bootstrap")

        remove.unlink()
        disabled = self.synchronizer().run("sync")
        self.assertEqual(disabled["pendingDeletionCount"], 1)
        self.assertTrue((self.destination / "remove").exists())

        self.database.set_meta("deletions_enabled", 1)
        enabled = self.synchronizer().run("sync")
        self.assertEqual(enabled["filesTrashed"], 1)
        self.assertFalse((self.destination / "remove").exists())
        self.assertTrue((self.destination / ".proton-drive-sync-wrapper-trash" / "remove").exists())

    def test_mass_deletion_requires_exact_approval(self):
        for index in range(25):
            (self.source / f"file-{index}").write_text(str(index), encoding="utf-8")
        self.synchronizer(full_audit=True).run("bootstrap")
        self.database.set_meta("deletions_enabled", 1)
        for index in range(2):
            (self.source / f"file-{index}").unlink()

        with self.assertRaises(ApprovalRequired):
            self.synchronizer().run("sync")
        run_id = self.database.summary()["activeRunId"]
        if run_id is None:
            run_id = str(
                self.database.connection.execute(
                    "SELECT run_id FROM runs WHERE status='approval-required' "
                    "ORDER BY started_at DESC"
                ).fetchone()[0]
            )
        _, count = self.database.approve_deletions(run_id)
        self.assertEqual(count, 2)
        result = self.synchronizer().run("sync")
        self.assertEqual(result["filesTrashed"], 2)

        for index in range(2):
            (self.source / f"file-{index}").write_text(f"again-{index}", encoding="utf-8")
        self.synchronizer().run("sync")
        for index in range(2):
            (self.source / f"file-{index}").unlink()
        with self.assertRaises(ApprovalRequired):
            self.synchronizer().run("sync")

    def test_bounded_workers_reconcile_concurrently(self):
        self.provider = TrackingLocalProvider(self.destination, delay=0.03)
        for index in range(8):
            directory = self.source / f"parent-{index}"
            directory.mkdir()
            (directory / "object").write_text(f"payload-{index}", encoding="utf-8")
        result = self.synchronizer(full_audit=True).run("bootstrap")
        self.assertEqual(result["filesUploaded"], 8)
        self.assertEqual(self.provider.max_active_lists, 4)

    def test_source_minimum_blocks_empty_mount(self):
        with self.assertRaises(SafetyError):
            self.synchronizer(full_audit=True).run("bootstrap")
        self.assertEqual(self.database.summary()["trustedGeneration"], 0)

    def test_provider_reserved_root_name_is_rejected(self):
        for name in (
            ".proton-drive-sync-wrapper-trash",
            ".proton-drive-relay-trash",
            ".verified-mirror-trash",
        ):
            path = self.source / name
            path.write_text("payload", encoding="utf-8")
            with self.assertRaises(SafetyError):
                self.synchronizer(full_audit=True).run("bootstrap")
            path.unlink()

    def test_remote_folder_cannot_be_replaced_by_source_file(self):
        (self.source / "collision").write_text("payload", encoding="utf-8")
        (self.destination / "collision").mkdir()
        with self.assertRaises(SafetyError):
            self.synchronizer(full_audit=True).run("bootstrap")

    def test_removed_file_is_discarded_when_failed_run_resumes(self):
        keep = self.source / "keep"
        removed = self.source / "removed"
        keep.write_text("keep", encoding="utf-8")
        removed.write_text("removed", encoding="utf-8")
        failing = FailingUploadProvider(self.destination)
        with self.assertRaises(RuntimeError):
            Synchronizer(self.database, failing, self.config, full_audit=True).run("bootstrap")

        removed.unlink()
        result = self.synchronizer(full_audit=True).run("bootstrap")
        self.assertEqual(result["filesUploaded"], 1)
        self.assertTrue((self.destination / "keep").exists())
        self.assertFalse((self.destination / "removed").exists())

    def test_public_status_redacts_path_bearing_failure(self):
        missing = self.root / "private-source-name"
        config = dataclasses.replace(
            self.config, source=dataclasses.replace(self.config.source, path=missing)
        )
        with self.assertRaises(PrerequisiteError):
            Synchronizer(self.database, self.provider, config, full_audit=True).run("bootstrap")
        status = json.loads((self.state / "status.json").read_text(encoding="utf-8"))
        self.assertNotIn("private-source-name", status["failureMessage"])

    def test_symlink_is_rejected(self):
        target = self.source / "target"
        target.write_text("payload", encoding="utf-8")
        (self.source / "link").symlink_to(target)
        with self.assertRaises(SafetyError):
            list(inventory(self.source))

    def test_stable_digest_computes_strong_and_provider_hashes(self):
        path = self.source / "object"
        path.write_bytes(b"payload")
        local = next(inventory(self.source))
        digests = stable_digests(local, ("sha256", "sha1"))
        self.assertEqual(digests["sha256"], hashlib.sha256(b"payload").hexdigest())
        self.assertEqual(digests["sha1"], hashlib.sha1(b"payload").hexdigest())

    def test_proton_json_fixture(self):
        payload = [
            {
                "uid": "node",
                "name": {"ok": True, "value": "example"},
                "type": "file",
                "activeRevision": {
                    "uid": "revision",
                    "claimedSize": 12,
                    "claimedDigests": {"sha1": "abc", "sha1Verified": False},
                },
            }
        ]
        self.assertEqual(
            parse_remote_items(payload),
            [RemoteItem("example", "file", "node", "revision", 12, "sha1", "abc")],
        )


class MigrationTests(unittest.TestCase):
    def test_version_one_state_migrates_without_losing_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "legacy.sqlite3"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE runs(run_id TEXT PRIMARY KEY,mode TEXT,status TEXT,phase TEXT,
                    generation INTEGER,started_at INTEGER,finished_at INTEGER,source_marker INTEGER,
                    stats_json TEXT,failure_category TEXT,failure_message TEXT);
                CREATE TABLE files(path TEXT PRIMARY KEY,size INTEGER,inode INTEGER,
                    mtime_ns INTEGER,
                    ctime_ns INTEGER,sha1 TEXT,remote_uid TEXT,remote_revision_uid TEXT,
                    remote_claimed_size INTEGER,remote_sha1 TEXT,sequence INTEGER,
                    trusted_generation INTEGER,deleted_at INTEGER,operation_state TEXT);
                CREATE TABLE scan_entries(run_id TEXT,path TEXT,size INTEGER,inode INTEGER,
                    mtime_ns INTEGER,ctime_ns INTEGER,sha1 TEXT,remote_uid TEXT,
                    remote_revision_uid TEXT,remote_claimed_size INTEGER,remote_sha1 TEXT,
                    operation_state TEXT,PRIMARY KEY(run_id,path));
                CREATE TABLE run_deletions(run_id TEXT,path TEXT,PRIMARY KEY(run_id,path));
                CREATE TABLE deletion_approvals(digest TEXT PRIMARY KEY,
                    source_run_id TEXT,approved_at INTEGER);
                INSERT INTO files VALUES('object',7,1,2,3,'legacy-sha1',NULL,NULL,7,
                    'legacy-sha1',1,1,NULL,'verified');
                INSERT INTO meta VALUES('trusted_generation','1');
                PRAGMA user_version=1;
                """
            )
            connection.commit()
            connection.close()

            database = StateDatabase(path)
            try:
                row = database.trusted_file("object")
                self.assertEqual(row["local_digest"], "legacy-sha1")
                self.assertEqual(row["remote_digest"], "legacy-sha1")
                self.assertEqual(database.integrity_check(), "ok")
            finally:
                database.close()


class BidirectionalTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.local = self.root / "local"
        self.remote = self.root / "remote"
        self.state = self.root / "state"
        self.local.mkdir()
        self.remote.mkdir()
        self.config = AppConfig(
            source=SourceConfig(self.local),
            destination=DestinationConfig("local-filesystem", str(self.remote)),
            state_dir=self.state,
            safety=SafetyConfig(full_audit_interval_days=0),
            performance=PerformanceConfig(
                verify_workers=2, upload_batch_size=20, maximum_queued_parents=2
            ),
            sync=SyncConfig(direction="two-way"),
        )
        self.database = StateDatabase(self.state / "index.sqlite3")
        self.provider = LocalFilesystemProvider(self.remote)

    def tearDown(self):
        self.database.close()
        self.temporary.cleanup()

    def sync(self):
        return BidirectionalSynchronizer(self.database, self.provider, self.config).run("sync")

    def test_initial_sync_merges_non_overlapping_files(self):
        (self.local / "from-local").write_text("local", encoding="utf-8")
        (self.remote / "from-remote").write_text("remote", encoding="utf-8")

        result = self.sync()

        self.assertEqual(result["filesUploaded"], 1)
        self.assertEqual(result["filesDownloaded"], 1)
        self.assertEqual((self.local / "from-remote").read_text(encoding="utf-8"), "remote")
        self.assertEqual((self.remote / "from-local").read_text(encoding="utf-8"), "local")

    def test_one_sided_edits_flow_in_both_directions(self):
        (self.local / "local-edit").write_text("base", encoding="utf-8")
        (self.local / "remote-edit").write_text("base", encoding="utf-8")
        self.sync()

        (self.local / "local-edit").write_text("changed locally", encoding="utf-8")
        (self.remote / "remote-edit").write_text("changed remotely", encoding="utf-8")
        result = self.sync()

        self.assertEqual(result["filesUploaded"], 1)
        self.assertEqual(result["filesDownloaded"], 1)
        self.assertEqual(
            (self.remote / "local-edit").read_text(encoding="utf-8"), "changed locally"
        )
        self.assertEqual(
            (self.local / "remote-edit").read_text(encoding="utf-8"), "changed remotely"
        )

    def test_divergent_edit_stops_without_overwriting_either_side(self):
        (self.local / "conflict").write_text("base", encoding="utf-8")
        self.sync()
        (self.local / "conflict").write_text("local", encoding="utf-8")
        (self.remote / "conflict").write_text("remote", encoding="utf-8")

        with self.assertRaises(ConflictError):
            self.sync()

        self.assertEqual((self.local / "conflict").read_text(encoding="utf-8"), "local")
        self.assertEqual((self.remote / "conflict").read_text(encoding="utf-8"), "remote")

    def test_deletions_propagate_to_recoverable_trash_on_both_sides(self):
        (self.local / "delete-remotely").write_text("a", encoding="utf-8")
        (self.local / "delete-locally").write_text("b", encoding="utf-8")
        self.sync()
        self.database.set_meta("deletions_enabled", 1)
        (self.local / "delete-remotely").unlink()
        (self.remote / "delete-locally").unlink()

        with self.assertRaises(ApprovalRequired):
            self.sync()
        run_id = str(
            self.database.connection.execute(
                "SELECT run_id FROM runs WHERE status='approval-required' "
                "ORDER BY started_at DESC LIMIT 1"
            ).fetchone()[0]
        )
        self.database.approve_deletions(run_id)
        result = self.sync()

        self.assertEqual(result["filesTrashed"], 1)
        self.assertEqual(result["localFilesTrashed"], 1)
        self.assertTrue(
            (self.remote / ".proton-drive-sync-wrapper-trash" / "delete-remotely").exists()
        )
        local_trash = self.state / "local-trash" / result["runId"] / "delete-locally"
        self.assertTrue(local_trash.exists())


if __name__ == "__main__":
    unittest.main()
