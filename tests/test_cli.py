from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from verified_mirror.cli import main
from verified_mirror.config import load_config


class CLITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.destination = self.root / "destination"
        self.state = self.root / "state"
        self.source.mkdir()
        self.destination.mkdir()
        self.config = self.root / "config.toml"
        self.config.write_text(
            f"""[source]
path = {json.dumps(str(self.source))}

[destination]
provider = "local-filesystem"
root = {json.dumps(str(self.destination))}

[state]
directory = {json.dumps(str(self.state))}

[safety]
full_audit_interval_days = 0

[performance]
verify_workers = 2
maximum_queued_parents = 2
""",
            encoding="utf-8",
        )

    def tearDown(self):
        self.temporary.cleanup()

    def invoke(self, *arguments: str) -> tuple[int, str, str]:
        stdout = StringIO()
        stderr = StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["--config", str(self.config), *arguments])
        return code, stdout.getvalue(), stderr.getvalue()

    def test_dry_run_does_not_mutate_remote_or_persistent_generation(self):
        (self.source / "a").write_text("alpha", encoding="utf-8")
        code, output, _ = self.invoke("dry-run")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["filesWouldUpload"], 1)
        self.assertFalse((self.destination / "a").exists())

        code, output, _ = self.invoke("status")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["trustedGeneration"], 0)
        self.assertEqual(json.loads(output)["binding"], {})

    def test_config_validate_and_doctor(self):
        code, _, _ = self.invoke("config-validate")
        self.assertEqual(code, 0)
        code, output, _ = self.invoke("doctor")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["stateIntegrity"], "ok")

    def test_direction_switch_accepts_one_way_and_two_way(self):
        self.assertEqual(load_config(self.config).sync.direction, "upload-only")
        with self.config.open("a", encoding="utf-8") as handle:
            handle.write('\n[sync]\ndirection = "two-way"\n')
        self.assertEqual(load_config(self.config).sync.direction, "two-way")

    def test_state_backup_does_not_migrate_source(self):
        self.state.mkdir()
        source_database = self.state / "index.sqlite3"
        connection = sqlite3.connect(source_database)
        connection.execute("PRAGMA user_version=1")
        connection.close()
        backup = self.root / "state-backup.sqlite3"

        code, output, _ = self.invoke("state-backup", str(backup))

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["integrity"], "ok")
        source = sqlite3.connect(source_database)
        copied = sqlite3.connect(backup)
        try:
            self.assertEqual(source.execute("PRAGMA user_version").fetchone()[0], 1)
            self.assertEqual(copied.execute("PRAGMA user_version").fetchone()[0], 1)
        finally:
            source.close()
            copied.close()


if __name__ == "__main__":
    unittest.main()
