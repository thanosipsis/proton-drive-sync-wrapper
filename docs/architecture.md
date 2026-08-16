# Architecture

Proton Drive Sync Wrapper separates local and remote inventory, durable generations,
safety policy, and provider-specific operations.

```text
local directory <-> reconciliation and conflict plan <-> provider
                         |                      |
                         +---- SQLite state ---+
                                      |
                               status.json
```

## Trusted generations

`files` contains only the last fully committed generation. A run inventories the
source into `run_inventory` and writes verified objects into `scan_entries`.
Only after reconciliation, uploads, post-upload verification, and deletion
policy finish does one transaction merge staging and advance the trusted
generation.

## Direction modes

One-way mode preserves the original resumable mirror pipeline and treats the
local tree as authoritative. Two-way mode inventories both trees and compares
them with the last trusted generation. That three-way comparison distinguishes a
one-sided edit from simultaneous divergent edits without relying on timestamps
from different systems.

Two-way planning completes, conflict checks pass, and deletion approval is
validated before any mutation begins. Each download is verified in a temporary
directory on the destination filesystem and atomically installed. Provider and
local objects are checked again before mutation to detect changes during a run.
The generation commits only after a complete post-sync inventory and content
verification show that both sides converged.

A failed or interrupted generation therefore never partially replaces the
trusted view. Verified staging and pending-upload digests survive a retry of the
same run.

## State binding

State records the canonical local path, provider ID, destination identity, path
semantics, optional two-way direction, and a configuration fingerprint. Every
operational command checks that binding. Changing a bound value requires a new
state directory. Legacy upload-only bindings deliberately remain compatible.

Schema-1 state from the original Proton/Kopia prototype migrates to schema 2,
but must be explicitly bound with `state-bind --expect-tracked-files N`. This
prevents a silent migration from converting a configuration mistake into a mass
deletion candidate.

Schema-1 SHA-1 evidence remains labeled as SHA-1 and can continue through the
metadata fast path. Changed files and full audits replace it with local SHA-256,
avoiding a mandatory whole-tree read at migration time.

## Content evidence

Each changed file is read once while calculating SHA-256 and the provider's
comparison digest. File size, inode, device, modification time, and change time
must match before and after the read.

The database stores local and remote algorithms separately. Provider metadata
is evidence at the strength supplied by that provider; the status must not imply
an independently downloaded remote hash when none occurred.

## Bounded concurrency

Inventory and SQLite writes stay on the main thread. At most the configured
number of parents are queued, and at most `verify_workers` read-only provider
listings and local hashes run concurrently. Uploads, folder creation,
post-upload verification, and trash operations are sequential.

This prevents parent fan-out from creating an unbounded future list or process
count. Pending uploads are stored in SQLite rather than accumulated in memory.

## Failure boundary

Authentication, prerequisites, source mutation, remote errors, approval gates,
and safety errors have stable categories. The status document bounds error text
and excludes filenames, object IDs, account details, and credentials.
