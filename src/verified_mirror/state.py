from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from collections.abc import Iterable, Sequence
from pathlib import Path

from .errors import MirrorError, SafetyError
from .models import LocalFile, RemoteItem
from .util import now_epoch

SCHEMA_VERSION = 2


class StateDatabase:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def close(self) -> None:
        self.connection.close()

    def _columns(self, table: str) -> set[str]:
        return {str(row[1]) for row in self.connection.execute(f"PRAGMA table_info({table})")}

    def _add_column(self, table: str, definition: str) -> None:
        name = definition.split()[0]
        if name not in self._columns(table):
            self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")

    def _migrate(self) -> None:
        current = int(self.connection.execute("PRAGMA user_version").fetchone()[0])
        if current > SCHEMA_VERSION:
            raise SafetyError(
                f"State schema {current} is newer than supported schema {SCHEMA_VERSION}"
            )
        if current == 0:
            self.connection.executescript(
                """
                BEGIN;
                CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE runs (
                    run_id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    started_at INTEGER NOT NULL,
                    finished_at INTEGER,
                    source_marker INTEGER NOT NULL DEFAULT 0,
                    stats_json TEXT NOT NULL DEFAULT '{}',
                    failure_category TEXT,
                    failure_message TEXT
                );
                CREATE TABLE files (
                    path TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    inode INTEGER NOT NULL,
                    device INTEGER NOT NULL DEFAULT 0,
                    mtime_ns INTEGER NOT NULL,
                    ctime_ns INTEGER NOT NULL,
                    sha1 TEXT NOT NULL,
                    local_digest_algorithm TEXT NOT NULL,
                    local_digest TEXT NOT NULL,
                    remote_uid TEXT,
                    remote_revision_uid TEXT,
                    remote_claimed_size INTEGER,
                    remote_sha1 TEXT,
                    remote_digest_algorithm TEXT,
                    remote_digest TEXT,
                    sequence INTEGER NOT NULL,
                    trusted_generation INTEGER NOT NULL,
                    deleted_at INTEGER,
                    operation_state TEXT NOT NULL
                );
                CREATE TABLE scan_entries (
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
                    path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    inode INTEGER NOT NULL,
                    device INTEGER NOT NULL DEFAULT 0,
                    mtime_ns INTEGER NOT NULL,
                    ctime_ns INTEGER NOT NULL,
                    sha1 TEXT NOT NULL,
                    local_digest_algorithm TEXT NOT NULL,
                    local_digest TEXT NOT NULL,
                    remote_uid TEXT,
                    remote_revision_uid TEXT,
                    remote_claimed_size INTEGER,
                    remote_sha1 TEXT,
                    remote_digest_algorithm TEXT,
                    remote_digest TEXT,
                    operation_state TEXT NOT NULL,
                    PRIMARY KEY (run_id, path)
                );
                CREATE TABLE run_inventory (
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
                    path TEXT NOT NULL,
                    parent TEXT NOT NULL,
                    seen_token TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    inode INTEGER NOT NULL,
                    device INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    ctime_ns INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    local_digest TEXT,
                    provider_digest TEXT,
                    PRIMARY KEY (run_id, path)
                );
                CREATE TABLE run_deletions (
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
                    path TEXT NOT NULL,
                    PRIMARY KEY (run_id, path)
                );
                CREATE TABLE deletion_approvals (
                    digest TEXT PRIMARY KEY,
                    source_run_id TEXT NOT NULL,
                    approved_at INTEGER NOT NULL
                );
                CREATE INDEX files_deleted_idx ON files(deleted_at);
                CREATE INDEX scan_entries_run_idx ON scan_entries(run_id);
                CREATE INDEX run_inventory_status_idx ON run_inventory(run_id,status,path);
                CREATE INDEX run_inventory_parent_idx ON run_inventory(run_id,status,parent);
                PRAGMA user_version=2;
                COMMIT;
                """
            )
            current = 2
        if current == 1:
            with self.connection:
                for table in ("files", "scan_entries"):
                    self._add_column(table, "device INTEGER NOT NULL DEFAULT 0")
                    self._add_column(table, "local_digest_algorithm TEXT NOT NULL DEFAULT 'sha1'")
                    self._add_column(table, "local_digest TEXT")
                    self._add_column(table, "remote_digest_algorithm TEXT")
                    self._add_column(table, "remote_digest TEXT")
                migration_updates = (
                    "UPDATE files SET local_digest=sha1 WHERE local_digest IS NULL",
                    "UPDATE scan_entries SET local_digest=sha1 WHERE local_digest IS NULL",
                    "UPDATE files SET remote_digest_algorithm='sha1', "
                    "remote_digest=remote_sha1 WHERE remote_digest IS NULL "
                    "AND remote_sha1 IS NOT NULL",
                    "UPDATE scan_entries SET remote_digest_algorithm='sha1', "
                    "remote_digest=remote_sha1 WHERE remote_digest IS NULL "
                    "AND remote_sha1 IS NOT NULL",
                )
                for query in migration_updates:
                    self.connection.execute(query)
                self.connection.execute(
                    """CREATE TABLE IF NOT EXISTS run_inventory (
                       run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
                       path TEXT NOT NULL,size INTEGER NOT NULL,inode INTEGER NOT NULL,
                       device INTEGER NOT NULL,mtime_ns INTEGER NOT NULL,ctime_ns INTEGER NOT NULL,
                       status TEXT NOT NULL,local_digest TEXT,provider_digest TEXT,
                       PRIMARY KEY(run_id,path))"""
                )
                self._add_column("run_inventory", "parent TEXT NOT NULL DEFAULT '.'")
                self._add_column("run_inventory", "seen_token TEXT NOT NULL DEFAULT ''")
                self.connection.execute(
                    "CREATE INDEX IF NOT EXISTS run_inventory_status_idx "
                    "ON run_inventory(run_id,status,path)"
                )
                self.connection.execute(
                    "CREATE INDEX IF NOT EXISTS run_inventory_parent_idx "
                    "ON run_inventory(run_id,status,parent)"
                )
                self.connection.execute("PRAGMA user_version=2")

        # Version 2 was not released before these queue-safety columns were
        # added. Keep this idempotent so development databases remain usable.
        if "run_inventory" in {
            str(row[0])
            for row in self.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }:
            with self.connection:
                self._add_column("run_inventory", "parent TEXT NOT NULL DEFAULT '.'")
                self._add_column("run_inventory", "seen_token TEXT NOT NULL DEFAULT ''")
                self.connection.execute(
                    "CREATE INDEX IF NOT EXISTS run_inventory_parent_idx "
                    "ON run_inventory(run_id,status,parent)"
                )

        defaults = {
            "deletions_enabled": "0",
            "trusted_generation": "0",
            "last_sequence": "0",
            "last_full_audit_at": "0",
        }
        with self.connection:
            for key, value in defaults.items():
                self.connection.execute(
                    "INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)", (key, value)
                )

    def integrity_check(self) -> str:
        return str(self.connection.execute("PRAGMA integrity_check").fetchone()[0])

    def meta(self, key: str) -> str | None:
        row = self.connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else None

    def meta_int(self, key: str) -> int:
        return int(self.meta(key) or 0)

    def set_meta(self, key: str, value: str | int) -> None:
        self.connection.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        self.connection.commit()

    def binding(self) -> dict[str, str]:
        keys = ("source", "provider", "destination", "path_semantics", "fingerprint")
        return {key: value for key in keys if (value := self.meta(f"binding.{key}")) is not None}

    def bind(self, expected: dict[str, str], *, expected_tracked_files: int | None = None) -> None:
        tracked = int(
            self.connection.execute(
                "SELECT count(*) FROM files WHERE deleted_at IS NULL"
            ).fetchone()[0]
        )
        existing = self.binding()
        if existing and existing != expected:
            raise SafetyError(
                "State is already bound to a different source or destination; "
                "use a new state directory"
            )
        if tracked and expected_tracked_files != tracked:
            raise SafetyError(f"Binding existing state requires --expect-tracked-files {tracked}")
        with self.connection:
            for key, value in expected.items():
                self.connection.execute(
                    "INSERT INTO meta(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (f"binding.{key}", value),
                )

    def ensure_binding(self, expected: dict[str, str]) -> None:
        current = self.binding()
        tracked = self.summary()["trackedFiles"]
        if not current and tracked == 0:
            self.bind(expected)
            return
        if not current:
            raise SafetyError(
                "Existing state has no source/destination binding. Run 'state bind' with the "
                "current tracked-file count after verifying the configuration."
            )
        if current != expected:
            differences = ", ".join(
                key for key in sorted(expected) if current.get(key) != expected.get(key)
            )
            raise SafetyError(
                f"Configuration does not match this state database ({differences}); "
                "use the original configuration or a new state directory"
            )

    def start_or_resume_run(self, mode: str, source_marker: int) -> tuple[str, int, int, bool]:
        generation = self.meta_int("trusted_generation") + 1
        resumable = self.connection.execute(
            """SELECT run_id,generation,started_at FROM runs
               WHERE mode=? AND source_marker=? AND generation=?
                 AND status IN ('running','failed','approval-required')
               ORDER BY started_at DESC LIMIT 1""",
            (mode, source_marker, generation),
        ).fetchone()
        if resumable:
            self.connection.execute(
                """UPDATE runs SET status='running',phase='scanning',finished_at=NULL,
                   failure_category=NULL,failure_message=NULL WHERE run_id=?""",
                (resumable["run_id"],),
            )
            self.connection.commit()
            return (
                str(resumable["run_id"]),
                int(resumable["generation"]),
                int(resumable["started_at"]),
                True,
            )
        run_id = f"{time.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        started_at = now_epoch()
        self.connection.execute(
            "INSERT INTO runs(run_id,mode,status,phase,generation,started_at,source_marker) "
            "VALUES(?,?,?,?,?,?,?)",
            (run_id, mode, "running", "scanning", generation, started_at, source_marker),
        )
        self.connection.commit()
        return run_id, generation, started_at, False

    def update_run(
        self,
        run_id: str,
        status: str,
        phase: str,
        stats: dict,
        error: MirrorError | None = None,
    ) -> None:
        finished = now_epoch() if status != "running" else None
        self.connection.execute(
            """UPDATE runs SET status=?,phase=?,stats_json=?,finished_at=?,
               failure_category=?,failure_message=? WHERE run_id=?""",
            (
                status,
                phase,
                json.dumps(stats, sort_keys=True),
                finished,
                error.category if error else None,
                str(error)[:500] if error else None,
                run_id,
            ),
        )
        self.connection.commit()

    @staticmethod
    def metadata_equal(row: sqlite3.Row, local: LocalFile) -> bool:
        ordinary_metadata_equal = all(
            int(row[key]) == getattr(local, key)
            for key in ("size", "inode", "mtime_ns", "ctime_ns")
        )
        # Schema 1 did not store st_dev. Zero is its explicit unknown sentinel.
        device_equal = int(row["device"]) in (0, local.device)
        return ordinary_metadata_equal and device_equal

    def trusted_file(self, path: str):
        return self.connection.execute(
            "SELECT * FROM files WHERE path=? AND deleted_at IS NULL", (path,)
        ).fetchone()

    def staged_file(self, run_id: str, path: str):
        return self.connection.execute(
            "SELECT * FROM scan_entries WHERE run_id=? AND path=? AND operation_state='verified'",
            (run_id, path),
        ).fetchone()

    def record_inventory(
        self, run_id: str, local: LocalFile, seen_token: str
    ) -> sqlite3.Row | None:
        previous = self.connection.execute(
            "SELECT * FROM run_inventory WHERE run_id=? AND path=?", (run_id, local.path)
        ).fetchone()
        preserve = previous is not None and self.metadata_equal(previous, local)
        self.connection.execute(
            """INSERT INTO run_inventory(run_id,path,parent,seen_token,size,inode,device,
               mtime_ns,ctime_ns,status,local_digest,provider_digest)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id,path) DO UPDATE SET size=excluded.size,inode=excluded.inode,
               parent=excluded.parent,device=excluded.device,mtime_ns=excluded.mtime_ns,ctime_ns=excluded.ctime_ns,
               seen_token=excluded.seen_token,
               status=excluded.status,local_digest=excluded.local_digest,
               provider_digest=excluded.provider_digest""",
            (
                run_id,
                local.path,
                local.path.rpartition("/")[0] or ".",
                seen_token,
                local.size,
                local.inode,
                local.device,
                local.mtime_ns,
                local.ctime_ns,
                previous["status"] if preserve else "candidate",
                previous["local_digest"] if preserve else None,
                previous["provider_digest"] if preserve else None,
            ),
        )
        return previous if preserve else None

    def set_inventory_status(
        self,
        run_id: str,
        path: str,
        status: str,
        local_digest: str | None = None,
        provider_digest: str | None = None,
    ) -> None:
        self.connection.execute(
            "UPDATE run_inventory SET status=?,local_digest=coalesce(?,local_digest),"
            "provider_digest=coalesce(?,provider_digest) WHERE run_id=? AND path=?",
            (status, local_digest, provider_digest, run_id, path),
        )

    def inventory_parents(self, run_id: str, status: str) -> Iterable[str]:
        previous = ""
        while True:
            row = self.connection.execute(
                """SELECT min(parent) FROM run_inventory
                   WHERE run_id=? AND status=? AND parent>?""",
                (run_id, status, previous),
            ).fetchone()
            if not row or row[0] is None:
                return
            previous = str(row[0])
            yield previous

    def inventory_files(self, run_id: str, parent: str, status: str) -> list[LocalFile]:
        rows = self.connection.execute(
            "SELECT * FROM run_inventory WHERE run_id=? AND status=? AND parent=? ORDER BY path",
            (run_id, status, parent),
        )
        files: list[LocalFile] = []
        for row in rows:
            relative = str(row["path"])
            files.append(
                LocalFile(
                    path=relative,
                    absolute_path=Path(relative),
                    size=int(row["size"]),
                    inode=int(row["inode"]),
                    device=int(row["device"]),
                    mtime_ns=int(row["mtime_ns"]),
                    ctime_ns=int(row["ctime_ns"]),
                )
            )
        return files

    def inventory_digest(self, run_id: str, path: str) -> tuple[str, str]:
        row = self.connection.execute(
            "SELECT local_digest,provider_digest FROM run_inventory WHERE run_id=? AND path=?",
            (run_id, path),
        ).fetchone()
        if not row or not row[0] or not row[1]:
            raise SafetyError(f"Pending upload has no verified local digest: {path}")
        return str(row[0]), str(row[1])

    def finish_inventory(self, run_id: str, seen_token: str) -> None:
        self.connection.execute(
            "DELETE FROM run_inventory WHERE run_id=? AND seen_token!=?", (run_id, seen_token)
        )
        self.connection.execute(
            "DELETE FROM scan_entries WHERE run_id=? AND path NOT IN "
            "(SELECT path FROM run_inventory WHERE run_id=?)",
            (run_id, run_id),
        )
        self.connection.commit()

    def stage(
        self,
        run_id: str,
        local: LocalFile,
        local_digest: str,
        provider_digest: str,
        provider_algorithm: str,
        remote: RemoteItem | None,
        local_algorithm: str = "sha256",
    ) -> None:
        self.connection.execute(
            """INSERT OR REPLACE INTO scan_entries(
               run_id,path,size,inode,device,mtime_ns,ctime_ns,sha1,
               local_digest_algorithm,local_digest,remote_uid,remote_revision_uid,
               remote_claimed_size,remote_sha1,remote_digest_algorithm,remote_digest,
               operation_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_id,
                local.path,
                local.size,
                local.inode,
                local.device,
                local.mtime_ns,
                local.ctime_ns,
                provider_digest,
                local_algorithm,
                local_digest,
                remote.uid if remote else None,
                remote.revision_uid if remote else None,
                remote.claimed_size if remote else None,
                remote.digest if remote else None,
                provider_algorithm,
                remote.digest if remote else provider_digest,
                "verified",
            ),
        )
        self.set_inventory_status(run_id, local.path, "verified", local_digest, provider_digest)

    def prepare_deletions(self, run_id: str) -> list[str]:
        self.connection.execute("DELETE FROM run_deletions WHERE run_id=?", (run_id,))
        self.connection.execute(
            """INSERT INTO run_deletions(run_id,path)
               SELECT ?,path FROM files
               WHERE ((deleted_at IS NOT NULL AND operation_state='tombstone')
                 OR (deleted_at IS NULL AND NOT EXISTS(
                   SELECT 1 FROM scan_entries s WHERE s.run_id=? AND s.path=files.path)))
               AND NOT EXISTS(
                 SELECT 1 FROM scan_entries s WHERE s.run_id=? AND s.path=files.path
               )""",
            (run_id, run_id, run_id),
        )
        self.connection.commit()
        return [
            str(row[0])
            for row in self.connection.execute(
                "SELECT path FROM run_deletions WHERE run_id=? ORDER BY path", (run_id,)
            )
        ]

    @staticmethod
    def deletion_digest(paths: Sequence[str]) -> str:
        digest = hashlib.sha256()
        for path in paths:
            digest.update(path.encode("utf-8", "surrogateescape"))
            digest.update(b"\0")
        return digest.hexdigest()

    def deletion_approved(self, run_id: str, paths: Sequence[str]) -> bool:
        if not paths:
            return True
        return (
            self.connection.execute(
                "SELECT 1 FROM deletion_approvals WHERE digest=? AND source_run_id=?",
                (self.deletion_digest(paths), run_id),
            ).fetchone()
            is not None
        )

    def approve_deletions(self, run_id: str) -> tuple[str, int]:
        paths = [
            str(row[0])
            for row in self.connection.execute(
                "SELECT path FROM run_deletions WHERE run_id=? ORDER BY path", (run_id,)
            )
        ]
        if not paths:
            raise SafetyError(f"Run has no retained deletion set: {run_id}")
        digest = self.deletion_digest(paths)
        self.connection.execute(
            "INSERT OR REPLACE INTO deletion_approvals(digest,source_run_id,approved_at) "
            "VALUES(?,?,?)",
            (digest, run_id, now_epoch()),
        )
        self.connection.commit()
        return digest, len(paths)

    def commit_generation(
        self,
        run_id: str,
        generation: int,
        deletion_paths: Sequence[str],
        deletions_processed: bool,
        full_audit: bool,
    ) -> None:
        sequence = self.meta_int("last_sequence")
        with self.connection:
            for row in self.connection.execute(
                "SELECT * FROM scan_entries WHERE run_id=? ORDER BY path", (run_id,)
            ):
                previous = self.connection.execute(
                    "SELECT * FROM files WHERE path=?", (row["path"],)
                ).fetchone()
                unchanged = previous and all(
                    previous[key] == row[key]
                    for key in (
                        "size",
                        "inode",
                        "device",
                        "mtime_ns",
                        "ctime_ns",
                        "local_digest",
                    )
                )
                if not unchanged:
                    sequence += 1
                self.connection.execute(
                    """INSERT INTO files(path,size,inode,device,mtime_ns,ctime_ns,sha1,
                       local_digest_algorithm,local_digest,remote_uid,remote_revision_uid,
                       remote_claimed_size,remote_sha1,remote_digest_algorithm,remote_digest,
                       sequence,trusted_generation,deleted_at,operation_state)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?)
                       ON CONFLICT(path) DO UPDATE SET size=excluded.size,inode=excluded.inode,
                       device=excluded.device,mtime_ns=excluded.mtime_ns,ctime_ns=excluded.ctime_ns,
                       sha1=excluded.sha1,local_digest_algorithm=excluded.local_digest_algorithm,
                       local_digest=excluded.local_digest,remote_uid=excluded.remote_uid,
                       remote_revision_uid=excluded.remote_revision_uid,
                       remote_claimed_size=excluded.remote_claimed_size,
                       remote_sha1=excluded.remote_sha1,
                       remote_digest_algorithm=excluded.remote_digest_algorithm,
                       remote_digest=excluded.remote_digest,sequence=excluded.sequence,
                       trusted_generation=excluded.trusted_generation,deleted_at=NULL,
                       operation_state=excluded.operation_state""",
                    (
                        row["path"],
                        row["size"],
                        row["inode"],
                        row["device"],
                        row["mtime_ns"],
                        row["ctime_ns"],
                        row["sha1"],
                        row["local_digest_algorithm"],
                        row["local_digest"],
                        row["remote_uid"],
                        row["remote_revision_uid"],
                        row["remote_claimed_size"],
                        row["remote_sha1"],
                        row["remote_digest_algorithm"],
                        row["remote_digest"],
                        sequence if not unchanged else previous["sequence"],
                        generation,
                        row["operation_state"],
                    ),
                )
            for path in deletion_paths:
                sequence += 1
                self.connection.execute(
                    "UPDATE files SET deleted_at=coalesce(deleted_at,?),sequence=?,"
                    "trusted_generation=?,operation_state=? WHERE path=?",
                    (
                        now_epoch(),
                        sequence,
                        generation,
                        "trashed" if deletions_processed else "tombstone",
                        path,
                    ),
                )
            self.connection.execute(
                "UPDATE meta SET value=? WHERE key='trusted_generation'", (str(generation),)
            )
            self.connection.execute(
                "UPDATE meta SET value=? WHERE key='last_sequence'", (str(sequence),)
            )
            if full_audit:
                self.connection.execute(
                    "UPDATE meta SET value=? WHERE key='last_full_audit_at'",
                    (str(now_epoch()),),
                )
            self.connection.execute("DELETE FROM run_inventory WHERE run_id=?", (run_id,))

    def summary(self) -> dict:
        last = self.connection.execute(
            """SELECT * FROM runs WHERE status='success' AND mode!='dry-run'
               ORDER BY finished_at DESC LIMIT 1"""
        ).fetchone()
        active = self.connection.execute(
            "SELECT * FROM runs WHERE status='running' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return {
            "schemaVersion": SCHEMA_VERSION,
            "trustedGeneration": self.meta_int("trusted_generation"),
            "deletionsEnabled": bool(self.meta_int("deletions_enabled")),
            "trackedFiles": int(
                self.connection.execute(
                    "SELECT count(*) FROM files WHERE deleted_at IS NULL"
                ).fetchone()[0]
            ),
            "tombstones": int(
                self.connection.execute(
                    "SELECT count(*) FROM files WHERE deleted_at IS NOT NULL"
                ).fetchone()[0]
            ),
            "lastTrustedAtEpoch": int(last["finished_at"]) if last and last["finished_at"] else 0,
            "lastFullAuditAtEpoch": self.meta_int("last_full_audit_at"),
            "activeRunId": str(active["run_id"]) if active else None,
            "binding": self.binding(),
        }
