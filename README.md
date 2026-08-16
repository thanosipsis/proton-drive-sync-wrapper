# Verified Mirror

Verified Mirror is a resumable, safety-first, one-way directory mirror. It was
created to copy an immutable backup repository to Proton Drive through Proton's
official CLI, but it works with ordinary directories and has a provider
interface that keeps cloud-specific behavior out of the synchronization engine.

> [!WARNING]
> A mirror is not a complete backup strategy. Source corruption or an approved
> deletion can propagate. Keep independent snapshots, retention, and tested
> restore procedures.

## Why it exists

The official Proton Drive CLI handles authentication, encryption, uploads, and
service-specific rate limits. Verified Mirror adds orchestration needed for a
large unattended tree:

- durable SQLite generations and crash-safe resume;
- metadata-only skips for unchanged files;
- local SHA-256 plus provider-checksum reconciliation;
- bounded parallel reads and sequential verified writes;
- a source-change check around every content read;
- remote deletion disabled by default;
- recoverable trash, mass-deletion limits, and exact-set approvals;
- state binding that prevents reusing an index with another source or target;
- machine-readable status without filenames, credentials, or object IDs;
- dry runs that modify neither the remote nor persistent state.

The source is always read-only. Verified Mirror is intentionally not a
bidirectional sync engine, mounted filesystem, or permanent-delete tool.

## Supported providers

| Provider | Status | Verification | Deletion behavior |
| --- | --- | --- | --- |
| Proton Drive official CLI | Primary | Claimed remote size and SHA-1 | Moves to Proton Trash |
| Local filesystem | Reference | Independently read SHA-256 | Moves under `.verified-mirror-trash` |

The Proton adapter does not call private APIs and does not bundle or modify
Proton software. Install the official CLI from [Proton's download page](https://proton.me/download/drive/cli)
and follow [Proton's authentication instructions](https://proton.me/support/drive-cli).

## Requirements

- Linux
- Python 3.11 or newer
- For Proton Drive: an authenticated official `proton-drive` CLI

Verified Mirror itself has no runtime Python dependencies.

## Installation

From a release checkout:

```bash
python3 -m venv .venv
.venv/bin/pip install .
.venv/bin/verified-mirror --version
```

Create a starter configuration:

```bash
.venv/bin/verified-mirror --config ./verified-mirror.toml init
chmod 600 ./verified-mirror.toml
```

Edit the source, destination, and state paths, then validate them:

```bash
.venv/bin/verified-mirror --config ./verified-mirror.toml config-validate
.venv/bin/verified-mirror --config ./verified-mirror.toml doctor
```

`doctor` performs a read-only destination listing. It does not upload or delete.

## First run

Start with a non-mutating plan:

```bash
verified-mirror --config /etc/verified-mirror/config.toml dry-run
```

Then bootstrap the first trusted generation:

```bash
verified-mirror --config /etc/verified-mirror/config.toml bootstrap
verified-mirror --config /etc/verified-mirror/config.toml status
```

Subsequent runs are incremental:

```bash
verified-mirror --config /etc/verified-mirror/config.toml sync
```

An optional producer marker prevents `sync` from running until another program
has atomically written JSON such as:

```json
{"completedAtEpoch": 1786831200}
```

An optional advisory lock serializes Verified Mirror with a cooperating source
writer. For changing data, filesystem or storage snapshots are stronger than an
advisory lock that other processes may ignore.

## Deletion safety

Local disappearance initially creates only a tombstone. Remote trash is a
separate state-database feature gate:

```bash
verified-mirror --config /etc/verified-mirror/config.toml deletions enable
```

If a deletion set exceeds the lower of the configured absolute or percentage
limits, the run stops without trashing anything. Review the source and exact run
before approving:

```bash
verified-mirror --config /etc/verified-mirror/config.toml status
verified-mirror --config /etc/verified-mirror/config.toml approve-deletions RUN_ID
verified-mirror --config /etc/verified-mirror/config.toml sync
```

Approval is bound to the run and a SHA-256 digest of its sorted path set. Any
change to the run or set requires another approval. A dry run reports whether
approval would be required without requiring or recording one.

## Recovery and audits

- Interrupted work resumes when mode, marker, and next generation match.
- A generation becomes trusted only after every file and deletion policy passes.
- `full-audit` re-reads and remotely reconciles every file.
- Normal `sync` automatically performs a full audit after the configured age.
- Back up SQLite with its online backup API or while the mirror is stopped; do
  not copy only the main file while WAL mode is active. The built-in command
  does not migrate the source database:

  ```bash
  verified-mirror --config /etc/verified-mirror/config.toml \
    state-backup /secure/path/index.sqlite3.backup
  ```
- If state is lost, keep deletion disabled and bootstrap into a new state
  directory. Existing matching remote objects will be verified and skipped.

See [architecture](docs/architecture.md), [operations](docs/operations.md), and
[provider development](docs/providers.md) for the detailed contracts.

## Important limitations

- Empty directories are not represented; the project mirrors regular files and
  their containing directories.
- Symlinks are rejected by default and may only be ignored, never followed.
- Metadata skips assume source metadata cannot be forged while the cooperating
  lock or snapshot policy is in effect.
- Proton verification compares locally computed content with size and digest
  metadata returned by the official CLI. It is not an independent download and
  re-hash of every remote object.
- Changes made directly at the destination are discovered during a full audit,
  not necessarily during every metadata-fast incremental run.

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

Verified Mirror is licensed under the [MIT License](LICENSE). Proton Drive is a
third-party service and trademark; this project is independent and is not
affiliated with or endorsed by Proton AG.
