"""Contract tests for vendor-neutral column metadata normalization."""

import json
from dataclasses import FrozenInstanceError, fields

import pytest

from models.metadata import CanonicalType, ColumnMetadata
from normalizers.postgresql import normalize_postgresql_column
from normalizers.snowflake import normalize_snowflake_column


def test_canonical_type_contains_the_complete_foundation_set():
    assert {item.value for item in CanonicalType} == {
        "STRING", "INTEGER", "DECIMAL", "FLOAT", "BOOLEAN", "DATE", "TIME",
        "TIMESTAMP", "TIMESTAMP_TZ", "BINARY", "SEMI_STRUCTURED", "UNKNOWN",
    }


def test_postgresql_row_normalizes_lowercase_keys_and_preserves_identifiers():
    metadata = normalize_postgresql_column(
        {
            "column_name": "OrderAmount",
            "data_type": "NUMERIC(20, 0)",
            "is_nullable": "NO",
            "character_maximum_length": None,
            "numeric_precision": "20",
            "numeric_scale": "0",
            "datetime_precision": "",
            "ordinal_position": "3",
        },
        catalog_name="SalesDb",
        schema_name="Accounting",
        table_name="Invoices",
    )

    assert metadata.catalog_name == "SalesDb"
    assert metadata.schema_name == "Accounting"
    assert metadata.table_name == "Invoices"
    assert metadata.column_name == "OrderAmount"
    assert metadata.native_type == "NUMERIC(20, 0)"
    assert metadata.canonical_type is CanonicalType.INTEGER
    assert metadata.nullable is False
    assert metadata.ordinal_position == 3
    assert metadata.numeric_precision == 20
    assert metadata.numeric_scale == 0
    assert metadata.datetime_precision is None
    assert metadata.is_primary_key is None
    assert metadata.is_foreign_key is None


def test_snowflake_row_normalizes_uppercase_keys_to_the_same_model_contract():
    metadata = normalize_snowflake_column(
        {
            "COLUMN_NAME": "OrderAmount",
            "DATA_TYPE": "NUMBER(20, 0)",
            "IS_NULLABLE": "NO",
            "NUMERIC_PRECISION": "20",
            "NUMERIC_SCALE": "0",
            "ORDINAL_POSITION": "3",
        },
        catalog_name="SalesDb",
        schema_name="Accounting",
        table_name="Invoices",
    )

    assert isinstance(metadata, ColumnMetadata)
    assert [field.name for field in fields(metadata)] == list(metadata.to_dict())
    assert metadata.column_name == "OrderAmount"
    assert metadata.canonical_type is CanonicalType.INTEGER
    assert metadata.numeric_precision == 20
    assert metadata.numeric_scale == 0


@pytest.mark.parametrize("normalizer", [normalize_postgresql_column, normalize_snowflake_column])
@pytest.mark.parametrize("native_type", ["NUMERIC(12, 2)", "DECIMAL"])
def test_fixed_point_with_nonzero_or_unknown_scale_maps_to_decimal(normalizer, native_type):
    row = {"COLUMN_NAME": "amount", "DATA_TYPE": native_type, "NUMERIC_SCALE": "2"}
    metadata = normalizer(row, catalog_name=None, schema_name=None, table_name="ledger")
    assert metadata.canonical_type is CanonicalType.DECIMAL


@pytest.mark.parametrize(
    ("normalizer", "native_type"),
    [
        (normalize_postgresql_column, "real"),
        (normalize_postgresql_column, "double precision"),
        (normalize_postgresql_column, "float4"),
        (normalize_postgresql_column, "float8"),
        (normalize_snowflake_column, "FLOAT"),
        (normalize_snowflake_column, "DOUBLE"),
        (normalize_snowflake_column, "REAL"),
    ],
)
def test_floating_point_types_map_to_float(normalizer, native_type):
    metadata = normalizer(
        {"COLUMN_NAME": "ratio", "DATA_TYPE": native_type},
        catalog_name=None,
        schema_name=None,
        table_name="metrics",
    )
    assert metadata.canonical_type is CanonicalType.FLOAT


@pytest.mark.parametrize(
    ("normalizer", "native_type"),
    [(normalize_postgresql_column, "integer[]"), (normalize_postgresql_column, "ARRAY"), (normalize_snowflake_column, "ARRAY")],
)
def test_array_like_types_map_to_semi_structured_and_preserve_native_type(normalizer, native_type):
    metadata = normalizer(
        {"COLUMN_NAME": "items", "DATA_TYPE": native_type},
        catalog_name="catalog",
        schema_name="schema",
        table_name="orders",
    )
    assert metadata.canonical_type is CanonicalType.SEMI_STRUCTURED
    assert metadata.native_type == native_type


@pytest.mark.parametrize("normalizer", [normalize_postgresql_column, normalize_snowflake_column])
def test_missing_and_invalid_optional_values_become_none(normalizer):
    metadata = normalizer(
        {
            "CoLuMn_NaMe": "value",
            "DaTa_TyPe": "vendor_special",
            "IS_NULLABLE": "UNKNOWN",
            "ORDINAL_POSITION": "12.5",
            "CHARACTER_MAXIMUM_LENGTH": "invalid",
            "NUMERIC_PRECISION": "",
        },
        catalog_name="",
        schema_name=None,
        table_name="ExactTable",
    )
    assert metadata.catalog_name is None
    assert metadata.schema_name is None
    assert metadata.ordinal_position is None
    assert metadata.character_length is None
    assert metadata.numeric_precision is None
    assert metadata.nullable is None
    assert metadata.canonical_type is CanonicalType.UNKNOWN


@pytest.mark.parametrize("normalizer", [normalize_postgresql_column, normalize_snowflake_column])
def test_normalizers_reject_empty_object_identifiers(normalizer):
    with pytest.raises(ValueError, match="table_name"):
        normalizer({"COLUMN_NAME": "id"}, catalog_name=None, schema_name=None, table_name=" ")
    with pytest.raises(ValueError, match="column_name"):
        normalizer({"COLUMN_NAME": ""}, catalog_name=None, schema_name=None, table_name="items")


def test_column_metadata_is_immutable_and_converts_to_json():
    source_row = {"COLUMN_NAME": "payload", "DATA_TYPE": "VARIANT", "EXTRA": {"tags": ["a", "b"]}}
    metadata = normalize_snowflake_column(
        source_row,
        catalog_name="Warehouse",
        schema_name="PUBLIC",
        table_name="Events",
    )
    source_row["EXTRA"]["tags"].append("changed")

    with pytest.raises(FrozenInstanceError):
        metadata.column_name = "other"
    with pytest.raises(TypeError):
        metadata.vendor_metadata["new"] = "value"

    payload = metadata.to_dict()
    assert payload["canonical_type"] == "SEMI_STRUCTURED"
    assert payload["vendor_metadata"]["EXTRA"]["tags"] == ["a", "b"]
    assert json.loads(json.dumps(payload)) == payload
