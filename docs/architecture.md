# Architecture

Verified Mirror separates source inventory, durable generations, safety policy,
and destination-specific operations.

```text
source directory -> stable inventory -> reconciliation queue -> provider
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

A failed or interrupted generation therefore never partially replaces the
trusted view. Verified staging and pending-upload digests survive a retry of the
same run.

## State binding

State records the canonical source path, provider ID, destination identity, path
semantics, and a configuration fingerprint. Every operational command checks
that binding. Changing any bound value requires a new state directory.

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
