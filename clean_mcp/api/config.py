"""Explicit immutable API platform configuration."""

from dataclasses import dataclass, field

try:
    from persistence.config import ControlPlaneConfig
except ModuleNotFoundError:
    from ..persistence.config import ControlPlaneConfig


@dataclass(frozen=True, slots=True)
class ApiSettings:
    max_request_body_bytes: int = 1_048_576
    control_plane: ControlPlaneConfig = field(default_factory=ControlPlaneConfig.from_environment)

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_request_body_bytes, bool)
            or not isinstance(self.max_request_body_bytes, int)
            or self.max_request_body_bytes <= 0
        ):
            raise ValueError("max_request_body_bytes must be a positive integer.")
        if not isinstance(self.control_plane, ControlPlaneConfig):
            raise TypeError("control_plane must be a ControlPlaneConfig value.")
