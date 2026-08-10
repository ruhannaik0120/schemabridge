"""Load secret-bearing configuration for the independent control plane.

The DSN is read lazily during application construction and excluded from object
representations so logs and test failures cannot expose it.
"""
from __future__ import annotations
import os
from dataclasses import dataclass,field

@dataclass(frozen=True,slots=True,repr=False)
class ControlPlaneConfig:
    """Represent an enabled DSN or an explicitly disabled persistence boundary."""

    dsn:str=field(repr=False); enabled:bool=True
    def __post_init__(self):
        if not isinstance(self.enabled,bool): raise TypeError("enabled must be boolean.")
        if not isinstance(self.dsn,str): raise TypeError("dsn must be text.")
        if self.enabled and not self.dsn.strip(): raise ValueError("A control-plane DSN is required when persistence is enabled.")
        if not self.enabled and self.dsn: raise ValueError("A disabled control plane must not retain a DSN.")
    def __repr__(self): return f"ControlPlaneConfig(enabled={self.enabled}, dsn='[REDACTED]')"
    @classmethod
    def from_environment(cls):
        """Enable persistence only when the control-plane DSN is non-empty."""

        value=os.getenv("SCHEMABRIDGE_CONTROL_PLANE_DSN","")
        return cls(dsn=value,enabled=bool(value.strip()))
