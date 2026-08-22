"""Focused tests for MySQL's safe batch-staging writer."""

from __future__ import annotations

import pytest

from schemabridge.config import ConfigError
from schemabridge.connectors.mysql.connector import MySQLConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.metadata import CanonicalType
from schemabridge.models.transport import DataBatch, StagingColumn, StagingTableDefinition, TransportRelation
from schemabridge.transport.base import BatchTransportError, StagingTableWriter


def _profile(*, write_enabled: bool = True) -> ConnectionProfile:
    return ConnectionProfile(profile_id="mysql-target", db_type="mysql", host="db", database="bridge", username="loader", password="secret", max_rows=10, write_enabled=write_enabled)


RELATION = TransportRelation(catalog_name="bridge", schema_name="bridge", object_name="SB_STAGE_1")
DEFINITION = StagingTableDefinition(relation=RELATION, columns=(StagingColumn(name="id", canonical_type=CanonicalType.INTEGER, nullable=False), StagingColumn(name="payload", canonical_type=CanonicalType.SEMI_STRUCTURED, nullable=True)))


class Cursor:
    def __init__(self, connection, rowcount=0): self.connection, self.rowcount, self.closed = connection, rowcount, False
    def execute(self, query, parameters=None): self.connection.executions.append((query, parameters))
    def close(self): self.closed = True


class Connection:
    def __init__(self, rowcount=0): self.cursor_instance, self.executions, self.commits, self.rollbacks, self.closed = Cursor(self, rowcount), [], 0, 0, False
    def cursor(self, **_kwargs): return self.cursor_instance
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1
    def close(self): self.closed = True


class Connector(MySQLConnector):
    def __init__(self, connection, *, profile=None): super().__init__(profile=profile or _profile()); self.connection = connection; self.calls = []
    def _driver(self):
        parent = self
        class Driver:
            def connect(self, **kwargs): parent.calls.append(kwargs); return parent.connection
        return Driver()


def test_mysql_writer_prepares_writes_and_drops_exact_staging_table() -> None:
    connection = Connection(rowcount=2)
    connector = Connector(connection)
    connector.prepare_staging_table(definition=DEFINITION, timeout_seconds=7)
    result = connector.write_batch(definition=DEFINITION, batch=DataBatch(batch_number=1, column_names=("id", "payload"), rows=((1, {"b": 2, "a": 1}), (2, None))), timeout_seconds=7)
    connector.drop_staging_table(relation=RELATION, timeout_seconds=7)

    assert isinstance(connector, StagingTableWriter)
    assert connection.executions == [
        ('CREATE TABLE `bridge`.`SB_STAGE_1` (`id` DECIMAL(38,0) NOT NULL, `payload` JSON)', None),
        ('INSERT INTO `bridge`.`SB_STAGE_1` (`id`, `payload`) VALUES (%s, %s), (%s, %s)', (1, '{"a":1,"b":2}', 2, None)),
        ('DROP TABLE IF EXISTS `bridge`.`SB_STAGE_1`', None),
    ]
    assert result.rows_written == 2 and connection.commits == 3 and connection.closed
    assert connector.calls[0]["write_timeout"] == 7


def test_mysql_writer_rejects_disabled_profiles_and_partial_writes() -> None:
    disabled = Connector(Connection(), profile=_profile(write_enabled=False))
    with pytest.raises(ConfigError, match="write-enabled"):
        disabled.prepare_staging_table(definition=DEFINITION, timeout_seconds=1)

    partial = Connector(Connection(rowcount=1))
    with pytest.raises(BatchTransportError, match="complete"):
        partial.write_batch(definition=DEFINITION, batch=DataBatch(batch_number=1, column_names=("id", "payload"), rows=((1, None), (2, None))), timeout_seconds=1)
    assert partial.connection.rollbacks == 1
