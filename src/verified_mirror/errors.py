class MirrorError(RuntimeError):
    """Base error with a stable machine-readable category."""

    category = "internal"


class PrerequisiteError(MirrorError):
    category = "prerequisite"


class AuthenticationError(MirrorError):
    category = "authentication"


class RemoteError(MirrorError):
    category = "remote"


class SafetyError(MirrorError):
    category = "safety"


class ConflictError(SafetyError):
    category = "conflict"


class ApprovalRequired(SafetyError):
    category = "approval-required"


class ChangedDuringRead(MirrorError):
    category = "source-changing"


class ConfigurationError(MirrorError):
    category = "configuration"
