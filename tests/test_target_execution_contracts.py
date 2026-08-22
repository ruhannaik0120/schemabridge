"""Verify the extension skeleton used by current and future target systems."""

import pytest

from schemabridge.models.mapping import SqlDialect
from schemabridge.target_execution import (
    DuplicateTargetAdapterError,
    InvalidTargetAdapterError,
    SnowflakeTargetExecutionAdapter,
    TargetExecutionAdapter,
    TargetExecutionCapabilities,
    TargetExecutionCapabilitySummary,
    TargetExecutionRegistry,
    TargetTransformationCompiler,
    UnsupportedTargetSystemError,
)


class ExampleCompiler:
    def compile_select(self, plan, *, staging_relation, source_alias="src"):
        return plan, staging_relation, source_alias

    def compile_insert_select(self, plan, *, staging_relation, source_alias="src"):
        return plan, staging_relation, source_alias


class ExampleAdapter:
    database_type = "exampledb"
    dialect = SqlDialect.POSTGRESQL
    capabilities = TargetExecutionCapabilities(
        supports_select_preview=True,
        supports_insert_select_preview=True,
        supports_insert_select_execution=True,
    )

    def __init__(self):
        self.compiler = ExampleCompiler()

    def validate_preview(self, preview):
        return None

    def execute(self, target, preview):
        return target, preview


def test_structural_contracts_require_no_framework_inheritance() -> None:
    adapter = ExampleAdapter()

    assert isinstance(adapter.compiler, TargetTransformationCompiler)
    assert isinstance(adapter, TargetExecutionAdapter)
    assert adapter.capabilities.supports_insert_select_execution is True


def test_registry_resolves_normalized_database_type_and_lists_capabilities() -> None:
    adapter = ExampleAdapter()
    registry = TargetExecutionRegistry((adapter,))

    assert registry.resolve(" ExampleDB ") is adapter
    assert registry.supported_database_types == ("exampledb",)
    assert registry.capability_summaries == (
        TargetExecutionCapabilitySummary(
            database_type="exampledb",
            dialect=SqlDialect.POSTGRESQL,
            capabilities=adapter.capabilities,
        ),
    )


def test_capability_summaries_are_sorted_and_describe_each_registered_adapter() -> None:
    registry = TargetExecutionRegistry(
        (SnowflakeTargetExecutionAdapter(), ExampleAdapter())
    )

    assert [item.database_type for item in registry.capability_summaries] == [
        "exampledb",
        "snowflake",
    ]
    assert registry.capability_summaries[1].capabilities.supports_insert_select_execution


def test_registry_rejects_duplicate_or_incomplete_adapters() -> None:
    registry = TargetExecutionRegistry((ExampleAdapter(),))

    with pytest.raises(DuplicateTargetAdapterError):
        registry.register(ExampleAdapter())
    with pytest.raises(InvalidTargetAdapterError):
        TargetExecutionRegistry((object(),))


def test_registry_rejects_adapter_that_omits_its_capability_card() -> None:
    class IncompleteAdapter(ExampleAdapter):
        capabilities = None

    with pytest.raises(InvalidTargetAdapterError):
        TargetExecutionRegistry((IncompleteAdapter(),))


@pytest.mark.parametrize("database_type", ["unknown", "", "bad-name", None])
def test_registry_uses_one_safe_error_for_unsupported_targets(database_type) -> None:
    registry = TargetExecutionRegistry((ExampleAdapter(),))

    with pytest.raises(UnsupportedTargetSystemError, match="unsupported"):
        registry.resolve(database_type)
