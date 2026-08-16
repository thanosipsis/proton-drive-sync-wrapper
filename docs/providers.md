# Provider contract

A provider implements `verified_mirror.providers.base.Provider`:

- immutable capability metadata;
- a stable destination identity;
- read-only validation;
- directory listing;
- folder creation;
- batch upload;
- single-file download to an engine-owned temporary directory;
- recoverable trash.

The engine passes provider-relative POSIX paths. The adapter owns root joining,
escaping, authentication, pagination, retry behavior, rate-limit handling, and
translation into `RemoteItem` metadata.

## Required safety properties

A provider must:

1. Reject relative paths that escape its configured root.
2. Return every child from `list_dir`, handling pagination internally.
3. Never report an upload complete before the provider accepts all bytes.
4. Supply stable size and digest evidence or refuse verified synchronization.
5. Implement recoverable trash; permanent delete is outside this contract.
6. Classify authentication and transient remote failures.
7. Document case sensitivity, Unicode normalization, reserved names, and
   consistency delays in `path_semantics` and adapter tests.

## Contract tests

New adapters should reuse the engine integration scenarios for:

- bootstrap and unchanged fast path;
- nested folder creation;
- changed content replacement;
- remote-only download and post-download verification;
- one-sided edits flowing in each direction;
- divergent edit conflict detection;
- interruption and retry;
- missing destination objects;
- recoverable trash;
- unusual filenames supported by the provider;
- throttling, pagination, and delayed visibility.

Do not expose an adapter as supported solely because its method signatures
compile. A disposable canary integration suite is required before promotion.
