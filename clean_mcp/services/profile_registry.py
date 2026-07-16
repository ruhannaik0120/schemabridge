"""Immutable registry for explicitly named connection profiles."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from models.connection_profile import ConnectionProfile, ConnectionProfileError


_PROFILE_FIELDS = frozenset(
    {
        "db_type",
        "host",
        "database",
        "username",
        "password",
        "connection_options",
        "timeout_seconds",
        "max_rows",
    }
)


class ProfileRegistryError(ValueError):
    """Raised when profile registry input is malformed."""


class UnknownProfileError(ProfileRegistryError):
    """Raised when a requested profile ID is not registered."""


@dataclass(frozen=True, slots=True, init=False)
class ProfileRegistry:
    """An immutable, non-global collection of connection profiles."""

    _profiles: Mapping[str, ConnectionProfile] = field(repr=False)

    def __init__(self, profiles: Mapping[str, Mapping[str, Any] | ConnectionProfile]) -> None:
        if not isinstance(profiles, Mapping):
            raise ProfileRegistryError("Connection profiles must be a mapping keyed by profile ID.")

        built: dict[str, ConnectionProfile] = {}
        for raw_profile_id, raw_profile in profiles.items():
            if not isinstance(raw_profile_id, str) or not raw_profile_id.strip():
                raise ProfileRegistryError("Every connection profile must have a non-empty string ID.")
            display_id = raw_profile_id.strip()
            normalized_id = display_id.casefold()
            if normalized_id in built:
                raise ProfileRegistryError("Connection profile IDs must be unique ignoring case.")

            try:
                if isinstance(raw_profile, ConnectionProfile):
                    if raw_profile.normalized_profile_id != normalized_id:
                        raise ProfileRegistryError("A profile mapping key must match its profile ID.")
                    profile = raw_profile
                elif isinstance(raw_profile, Mapping):
                    values = dict(raw_profile)
                    if any(not isinstance(key, str) or key not in _PROFILE_FIELDS for key in values):
                        raise ProfileRegistryError("A connection profile contains an unsupported field.")
                    profile = ConnectionProfile(
                        profile_id=display_id,
                        db_type=values.get("db_type"),  # type: ignore[arg-type]
                        host=values.get("host", ""),  # type: ignore[arg-type]
                        database=values.get("database", ""),  # type: ignore[arg-type]
                        username=values.get("username", ""),  # type: ignore[arg-type]
                        password=values.get("password", ""),  # type: ignore[arg-type]
                        connection_options=values.get("connection_options", {}),  # type: ignore[arg-type]
                        timeout_seconds=values.get("timeout_seconds", 30),  # type: ignore[arg-type]
                        max_rows=values.get("max_rows", 500),  # type: ignore[arg-type]
                    )
                else:
                    raise ProfileRegistryError("Every connection profile value must be a mapping.")
            except ConnectionProfileError as exc:
                raise ProfileRegistryError(f"Connection profile {display_id!r} is invalid: {exc}") from None

            built[normalized_id] = profile

        object.__setattr__(self, "_profiles", MappingProxyType(built))

    @classmethod
    def from_json(cls, raw_json: str) -> "ProfileRegistry":
        """Build a registry from the existing DB_PROFILES_JSON document shape."""

        if not isinstance(raw_json, str):
            raise ProfileRegistryError("DB_PROFILES_JSON must be a string containing a JSON object.")
        if not raw_json.strip():
            return cls({})
        try:
            parsed = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError, ValueError):
            raise ProfileRegistryError("DB_PROFILES_JSON must be valid JSON.") from None
        if not isinstance(parsed, dict):
            raise ProfileRegistryError("DB_PROFILES_JSON must be a JSON object keyed by profile ID.")
        return cls(parsed)

    def resolve(self, profile_id: str) -> ConnectionProfile:
        """Resolve a profile ID without regard to casing."""

        if not isinstance(profile_id, str) or not profile_id.strip():
            raise UnknownProfileError("A non-empty profile ID is required.")
        profile = self._profiles.get(profile_id.strip().casefold())
        if profile is None:
            raise UnknownProfileError("The requested connection profile is not configured.")
        return profile

    def safe_profiles(self) -> list[dict[str, object]]:
        """Return deterministic, credential-free profile metadata."""

        return [self._profiles[key].to_safe_dict() for key in sorted(self._profiles)]

    def __len__(self) -> int:
        return len(self._profiles)
