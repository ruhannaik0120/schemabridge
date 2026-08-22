"""Public contracts for extending SchemaBridge with target database systems."""

from importlib import import_module

from .base import (
    PreparedMigrationTarget,
    TargetExecutionAdapter,
    TargetExecutionCapabilities,
    TargetExecutionDisposition,
    TargetExecutionResult,
    TargetTransformationCompiler,
    UnsupportedTargetOperationError,
)
from .registry import (
    DuplicateTargetAdapterError,
    InvalidTargetAdapterError,
    TargetExecutionRegistry,
    TargetExecutionRegistryError,
    TargetExecutionCapabilitySummary,
    UnsupportedTargetSystemError,
)
_LAZY_ADAPTERS = {
    "MySqlTargetExecutionAdapter": ".mysql",
    "MySqlTargetTransformationCompiler": ".mysql",
    "PostgreSqlTargetExecutionAdapter": ".postgresql",
    "PostgreSqlTargetTransformationCompiler": ".postgresql",
    "SnowflakeTargetExecutionAdapter": ".snowflake",
    "SnowflakeTargetTransformationCompiler": ".snowflake",
}


def __getattr__(name: str):
    """Import a concrete adapter only when code actually requests it."""

    module_name = _LAZY_ADAPTERS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value

__all__ = [
    "DuplicateTargetAdapterError",
    "InvalidTargetAdapterError",
    "MySqlTargetExecutionAdapter",
    "MySqlTargetTransformationCompiler",
    "PreparedMigrationTarget",
    "PostgreSqlTargetExecutionAdapter",
    "PostgreSqlTargetTransformationCompiler",
    "SnowflakeTargetExecutionAdapter",
    "SnowflakeTargetTransformationCompiler",
    "TargetExecutionAdapter",
    "TargetExecutionCapabilities",
    "TargetExecutionDisposition",
    "TargetExecutionRegistry",
    "TargetExecutionRegistryError",
    "TargetExecutionCapabilitySummary",
    "TargetExecutionResult",
    "TargetTransformationCompiler",
    "UnsupportedTargetSystemError",
    "UnsupportedTargetOperationError",
]
