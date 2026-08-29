"""Tests for bounded exact primary-key reconciliation primitives."""

from dataclasses import dataclass

import pytest

from schemabridge.models.mapping import SqlDialect
from schemabridge.validation.key_batches import (
    compile_primary_key_batch_sql,
    read_primary_key_batches,
    reconcile_primary_key_batches,
)


@dataclass
class _Result:
    columns: tuple[str, ...]
    rows: tuple[object, ...]


def test_compiler_quotes_relation_keys_and_paginates_per_dialect():
    postgres = compile_primary_key_batch_sql(
        dialect=SqlDialect.POSTGRESQL,
        relation=("public", "orders"),
        key_columns=("tenant_id", "order_id"),
        limit=50,
        offset=100,
    )
    mysql = compile_primary_key_batch_sql(
        dialect=SqlDialect.MYSQL,
        relation=("warehouse", "orders"),
        key_columns=("id",),
        limit=10,
        offset=0,
    )
    snowflake = compile_primary_key_batch_sql(
        dialect=SqlDialect.SNOWFLAKE,
        relation=("db", "landing", "orders"),
        key_columns=("id",),
        limit=10,
        offset=0,
    )

    assert '"tenant_id" AS "key_000"' in postgres.sql
    assert 'ORDER BY "tenant_id" ASC, "order_id" ASC LIMIT 50 OFFSET 100' in postgres.sql
    assert "FROM `warehouse`.`orders`" in mysql.sql
    assert 'FROM "db"."landing"."orders"' in snowflake.sql


def test_reader_advances_by_returned_rows_and_uses_bounded_reads():
    responses = iter(
        (
            _Result(("key_000",), ((1,), (2,))),
            _Result(("key_000",), ((3,),)),
            _Result(("key_000",), ()),
        )
    )
    calls = []

    def execute(**kwargs):
        calls.append(kwargs)
        return next(responses)

    batches = tuple(
        read_primary_key_batches(
            execute,
            dialect=SqlDialect.POSTGRESQL,
            relation=("public", "orders"),
            key_columns=("id",),
            batch_size=2,
            timeout_seconds=9,
        )
    )

    assert batches == (((1,), (2,)), ((3,),))
    assert ["OFFSET 0" in call["sql"] for call in calls] == [True, False, False]
    assert "OFFSET 2" in calls[1]["sql"] and "OFFSET 3" in calls[2]["sql"]
    assert all(call["max_rows"] == 2 for call in calls)


def test_reconciliation_reports_exact_missing_extra_and_duplicate_keys():
    report = reconcile_primary_key_batches(
        iter((((1,), (2,)), ((2,), (4,)))),
        iter((((2,), (3,)), ((3,), (4,)))),
    )

    assert report.source_key_count == 4 and report.target_key_count == 4
    assert report.missing_key_count == 2 and report.extra_key_count == 2
    assert report.source_duplicate_key_count == 1
    assert report.target_duplicate_key_count == 1
    assert report.matches is False


def test_reader_rejects_wrong_result_columns():
    with pytest.raises(ValueError, match="Malformed primary-key batch result"):
        tuple(
            read_primary_key_batches(
                lambda **_kwargs: _Result(("unexpected",), ((1,),)),
                dialect=SqlDialect.POSTGRESQL,
                relation=("public", "orders"),
                key_columns=("id",),
                batch_size=2,
                timeout_seconds=None,
            )
        )
