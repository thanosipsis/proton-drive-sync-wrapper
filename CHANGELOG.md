# Changelog

All notable changes will be documented here.

## 0.2.0 - 2026-08-16

- Renamed the project, distribution, primary command, and repository to Proton Drive Sync Wrapper.
- Added `sync.direction` with backward-compatible `one-way` and new `two-way` modes.
- Added verified provider downloads and remote-to-local synchronization.
- Added three-way edit detection and non-destructive conflict stops.
- Added gated deletion propagation with recoverable trash on both sides.
- Retained `proton-drive-relay`, `verified-mirror`, and `verified_mirror` as compatibility aliases.

## 0.1.1 - 2026-08-15

- Added a pre-migration-safe online state backup command.

## 0.1.0 - 2026-08-15

- Extracted the production Proton Drive mirror into an installable package.
- Added generic source configuration and provider contract.
- Added official Proton Drive CLI and local-filesystem providers.
- Added state/destination binding and schema-1 migration support.
- Added bounded reconciliation queues and persisted pending uploads.
- Added local SHA-256 evidence alongside provider checksums.
- Made dry runs non-mutating and separated actual from planned counters.
- Added configurable source-size guards and periodic full audits.
- Added tests, CI, systemd examples, threat model, and operations documentation.
