# Proton Drive Sync Wrapper

Proton Drive Sync Wrapper adds resumable, safety-first synchronization to Proton's
official command-line client. It can run as either a local-to-Proton relay or a
conservative two-way sync engine, and its provider boundary can support other
storage services without putting cloud-specific behavior in the sync engine.

> [!WARNING]
> Sync is not a backup. A deletion that passes the configured safety gates can
> propagate. Keep independent snapshots, retention, and tested restore procedures.

Proton Drive Sync Wrapper is independent software. It is not affiliated with or
endorsed by Proton AG.

## What it adds

- `one-way` and `two-way` direction switches;
- durable SQLite generations and crash-safe upload-only resume;
- a three-way baseline for detecting local and remote changes;
- safe handling of one-sided additions and edits in either direction;
- conflict stops when both sides changed differently;
- local SHA-256 plus provider-checksum verification;
- remote deletion disabled by default;
- recoverable local and remote trash, mass-deletion limits, and exact-set approvals;
- state binding that prevents an index from being reused with another root or direction;
- machine-readable status without filenames, credentials, or object IDs;
- dry runs that modify neither side nor persistent state.

The official Proton Drive CLI continues to handle authentication, encryption,
uploads, downloads, and service rate limits. The wrapper invokes it with argument
arrays and uses only its public command-line interface.

## Choose a direction

```toml
[sync]
direction = "two-way"
```

The available values are:

| Direction | Behavior | Best for |
| --- | --- | --- |
| `one-way` | Local files are authoritative; changes only travel to the provider | Backups and publishing |
| `two-way` | Additions and one-sided edits travel in both directions | A synced working directory |

Configurations created before version 0.2 remain one-way when `[sync]` is
absent. Changing an existing state directory from one direction to the other is
blocked; create a separate state directory and review a dry run first.
`upload-only` is accepted as a descriptive compatibility alias for `one-way`.

## Two-way conflict rules

The wrapper compares the current local file, current remote item, and last trusted
generation:

- a change on only one side is copied to the other;
- matching changes on both sides are accepted as already converged;
- different changes on both sides stop the run without overwriting either copy;
- a deletion opposed by an edit is a conflict;
- a one-sided deletion propagates only after deletion safety is enabled;
- files unique to either side during the first run are merged;
- different files at the same path during the first run are a conflict.

Conflicts are deliberately resolved by the person who owns the data. Edit or
rename one of the copies, run `dry-run`, and then sync again.

## Supported providers

| Provider | Status | Verification | Recoverable deletion |
| --- | --- | --- | --- |
| Proton Drive official CLI | Primary | Remote size and claimed SHA-1 | Proton Trash |
| Local filesystem | Reference/testing | Independently read SHA-256 | `.proton-drive-sync-wrapper-trash` |

The provider contract supports listing, folder creation, upload, download, and
trash. See [provider development](docs/providers.md) to add another service.

## Requirements

- Linux
- Python 3.11 or newer
- For Proton Drive: an authenticated official `proton-drive` CLI

The wrapper itself has no runtime Python dependencies. Install the official CLI from
[Proton's download page](https://proton.me/download/drive/cli) and follow
[Proton's authentication instructions](https://proton.me/support/drive-cli).

## Installation

```bash
python3 -m venv .venv
.venv/bin/pip install .
.venv/bin/proton-drive-sync-wrapper --version
```

Create and edit a starter configuration:

```bash
proton-drive-sync-wrapper --config ./proton-drive-sync-wrapper.toml init
chmod 600 ./proton-drive-sync-wrapper.toml
proton-drive-sync-wrapper --config ./proton-drive-sync-wrapper.toml config-validate
proton-drive-sync-wrapper --config ./proton-drive-sync-wrapper.toml doctor
```

`doctor` lists the destination but does not upload, download, or delete.

## First run

Always inspect a non-mutating plan first:

```bash
proton-drive-sync-wrapper --config /etc/proton-drive-sync-wrapper/config.toml dry-run
```

Then establish the first trusted generation and run incremental syncs:

```bash
proton-drive-sync-wrapper --config /etc/proton-drive-sync-wrapper/config.toml bootstrap
proton-drive-sync-wrapper --config /etc/proton-drive-sync-wrapper/config.toml sync
proton-drive-sync-wrapper --config /etc/proton-drive-sync-wrapper/config.toml status
```

In one-way mode an optional producer marker can defer `sync` until another
program atomically writes a newer successful completion time. An optional
advisory lock can serialize the wrapper with a cooperating local writer.

## Deletion safety

Deletion propagation begins disabled. Review the dry run before enabling it:

```bash
proton-drive-sync-wrapper --config /etc/proton-drive-sync-wrapper/config.toml deletions enable
```

In two-way mode, a remote deletion moves the local copy under the state
directory's `local-trash/RUN_ID` tree. A local deletion moves the provider copy
to its trash. Large deletion sets stop before changing either side:

```bash
proton-drive-sync-wrapper --config /etc/proton-drive-sync-wrapper/config.toml status
proton-drive-sync-wrapper --config /etc/proton-drive-sync-wrapper/config.toml \
  approve-deletions RUN_ID
proton-drive-sync-wrapper --config /etc/proton-drive-sync-wrapper/config.toml sync
```

Approval is bound to the run and a SHA-256 digest of the exact sorted operation
set, including which side will be trashed.

## Recovery and audits

- A trusted generation advances only after the chosen sides converge and verify.
- Two-way sync inventories both trees and verifies every converged file.
- `full-audit` re-reads and reconciles every file in one-way mode.
- Back up SQLite through the built-in online backup command, not by copying its
  main file while WAL mode may be active:

  ```bash
  proton-drive-sync-wrapper --config /etc/proton-drive-sync-wrapper/config.toml \
    state-backup /secure/path/index.sqlite3.backup
  ```

- If state is lost, disable deletions, use a new state directory, and inspect a
  dry run before establishing a new baseline.

## Compatibility after the rename

Version 0.2 renamed Verified Mirror to Proton Drive Sync Wrapper. The
`verified-mirror` and `proton-drive-relay` executables remain as compatibility
aliases, the internal Python import namespace remains `verified_mirror`, and
existing upload-only state bindings remain valid. New documentation and
installations should use `proton-drive-sync-wrapper`.

## Limitations

- Sync is periodic, not a continuously watching filesystem mount.
- Empty directories are not represented.
- Symlinks are rejected by default and may only be ignored, never followed.
- Two-way mode requires remote files to expose stable size and digest evidence.
- Proton Docs and Sheets cannot be synchronized as ordinary files.
- Remote names that cannot map safely to POSIX paths stop the run.
- Proton verification relies on metadata returned by the official CLI; it is
  not an independent remote download and re-hash unless a file is downloaded.

See [architecture](docs/architecture.md), [operations](docs/operations.md), and
[provider development](docs/providers.md) for detailed contracts.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/ruff check .
.venv/bin/mypy src/verified_mirror --ignore-missing-imports
.venv/bin/bandit -q -r src
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m build
```

Proton Drive Sync Wrapper is licensed under the [MIT License](LICENSE).
