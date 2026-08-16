# Operations

## Scheduling

Schedule Proton Drive Sync Wrapper at the interval appropriate for the data. In
one-way mode, run it after the source producer; a marker and shared advisory
lock are useful when the producer can cooperate. Prefer a read-only filesystem
snapshot when consistent snapshots are available.

Two-way mode is periodic rather than continuously watching. Do not run two wrapper
processes against the same roots or state directory concurrently. Review
conflicts manually; the wrapper will not choose a winner for divergent edits.

The example systemd unit is intentionally a template: replace source mount,
user/group, configuration path, and writable CLI state for the target host.
Validate the result with `systemd-analyze verify`.

## Monitoring

`status` prints the durable database summary. `status.json` contains active-run
progress and retains status schema version 2 for existing consumers. Fields
whose names begin with `filesWould` or `bytesWould` describe planned mutations,
including dry runs.

## State backup

SQLite uses WAL mode and `synchronous=FULL`. `state-backup OUTPUT` uses SQLite's
online backup API, refuses to overwrite its target, verifies the result, and
does not migrate the source database. Alternatively, stop scheduled runs and
copy the database together with its WAL state. Test restoration into an
isolated path.

## Upgrades

1. Stop the schedule and confirm no run is active.
2. Back up the state database correctly.
3. Install the reviewed release in a versioned virtual environment.
4. Run `config-validate`, `status`, and `doctor`.
5. Run `dry-run` and review planned uploads, downloads, and trash operations.
6. Re-enable the schedule.

Downgrades are not supported after a state schema migration. Restore the
pre-upgrade state backup with the older package instead.

State binding identifies the configured provider and destination path. If a
provider session is reauthenticated to another account that has an identical
destination path, that external account change may not be detectable from
configuration alone. Run `doctor` and `full-audit` after credential or account
changes, and prefer a new state directory when changing accounts.

## Lost state

Do not enable deletion. Create a new state directory, run `dry-run`, then
`bootstrap`. Matching remote content will be skipped when provider evidence
matches. Run representative restore tests before treating the new generation as
protected.

## Local trash

In two-way mode, provider-side deletions move the corresponding local file to
`STATE_DIRECTORY/local-trash/RUN_ID/PATH`. Retention is operator-managed. Do not
remove this directory until the deletion has been reviewed and ordinary backups
cover the recovery window.
