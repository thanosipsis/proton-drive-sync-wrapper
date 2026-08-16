# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's security-advisory
feature for this repository. Do not open a public issue containing credentials,
private paths, account identifiers, or exploit details.

## Security model

Proton Drive Sync Wrapper never needs a cloud password in its configuration. The provider
CLI or platform credential store owns authentication. Configuration and state
directories should be readable only by the service account.

Deletion propagation is disabled by default and only recoverable trash is supported.
State binding, minimum source thresholds, mass-deletion approval, and atomic
generation commits are defense-in-depth controls; none replace independent
backups and restore testing.

The project does not defend against a malicious root user, a compromised
provider CLI, a compromised local filesystem, or forged provider metadata.
