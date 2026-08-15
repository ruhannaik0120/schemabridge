"""Verify the vendor-neutral contracts before adding live implementations."""

from dataclasses import FrozenInstanceError

import pytest

from schemabridge.models.metadata import CanonicalType
from schemabridge.models.transport import (
    BatchWriteResult,
    DataBatch,
    StagingColumn,
    StagingTableDefinition,
    TransportRelation,
)
from schemabridge.transport.base import BatchSourceReader, StagingTableWriter


RELATION = TransportRelation(
    catalog_name="warehouse",
    schema_name="landing",
    object_name="customers_stage",
)


def test_data_batch_is_bounded_ordered_immutable_and_redacted() -> None:
    batch = DataBatch(
        batch_number=1,
        column_names=("customer_id", "email"),
        rows=((1, "private@example.com"), (2, None)),
    )

    assert batch.row_count == 2
    assert "private@example.com" not in repr(batch)
    with pytest.raises(FrozenInstanceError):
        batch.batch_number = 2  # type: ignore[misc]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch_number": 0, "column_names": ("id",), "rows": ((1,),)},
        {"batch_number": 1, "column_names": ("id", "id"), "rows": ((1, 2),)},
        {"batch_number": 1, "column_names": ("id",), "rows": ()},
        {"batch_number": 1, "column_names": ("id", "name"), "rows": ((1,),)},
    ],
)
def test_data_batch_rejects_ambiguous_or_malformed_shapes(kwargs: object) -> None:
    with pytest.raises(ValueError):
        DataBatch(**kwargs)  # type: ignore[arg-type]


def test_staging_definition_uses_canonical_types_and_unique_columns() -> None:
    identifier = StagingColumn(
        name="customer_id",
        canonical_type=CanonicalType.INTEGER,
        nullable=False,
        numeric_precision=19,
        numeric_scale=0,
    )
    definition = StagingTableDefinition(relation=RELATION, columns=(identifier,))

    assert definition.columns == (identifier,)
    with pytest.raises(ValueError, match="unique"):
        StagingTableDefinition(relation=RELATION, columns=(identifier, identifier))


def test_staging_column_keeps_vendor_neutral_numeric_scale_rules() -> None:
    column = StagingColumn(
        name="rounded_amount",
        canonical_type=CanonicalType.DECIMAL,
        nullable=True,
        numeric_precision=8,
        numeric_scale=-2,
    )

    assert column.numeric_scale == -2
    with pytest.raises(ValueError, match="numeric_scale"):
        StagingColumn(
            name="invalid_amount",
            canonical_type=CanonicalType.DECIMAL,
            nullable=True,
            numeric_scale=True,  # type: ignore[arg-type]
        )


def test_batch_write_result_cannot_claim_more_rows_than_received() -> None:
    assert BatchWriteResult(
        batch_number=1,
        rows_received=5,
        rows_written=5,
    ).rows_written == 5
    with pytest.raises(ValueError, match="exceed"):
        BatchWriteResult(batch_number=1, rows_received=5, rows_written=6)


def test_structural_contracts_do_not_require_connector_inheritance() -> None:
    class Reader:
        def read_batches(self, **kwargs):
            yield DataBatch(batch_number=1, column_names=("id",), rows=((1,),))

    class Writer:
        def prepare_staging_table(self, **kwargs):
            return None

        def write_batch(self, **kwargs):
            return BatchWriteResult(batch_number=1, rows_received=1, rows_written=1)

        def drop_staging_table(self, **kwargs):
            return None

    assert isinstance(Reader(), BatchSourceReader)
    assert isinstance(Writer(), StagingTableWriter)
