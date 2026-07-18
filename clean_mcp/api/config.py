"""Explicit immutable API platform configuration."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ApiSettings:
    max_request_body_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_request_body_bytes, bool)
            or not isinstance(self.max_request_body_bytes, int)
            or self.max_request_body_bytes <= 0
        ):
            raise ValueError("max_request_body_bytes must be a positive integer.")
