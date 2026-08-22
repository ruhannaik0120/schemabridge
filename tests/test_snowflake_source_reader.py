"""Verify Snowflake participates in the shared bounded source-reader contract."""

from schemabridge.connectors.snowflake.connector import SnowflakeConnector
from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.models.transport import TransportRelation


class Cursor:
    def __init__(self):
        self.query = None
        self.timeout = None
        self.rows = [(1, "Asha"), (2, "Rahul"), (3, "Neha")]
        self.closed = False

    def execute(self, query, timeout=None):
        self.query = query
        self.timeout = timeout

    def fetchmany(self, size):
        batch, self.rows = self.rows[:size], self.rows[size:]
        return batch

    def close(self):
        self.closed = True


class Connection:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


class Connector(SnowflakeConnector):
    def __init__(self, connection):
        super().__init__(profile=ConnectionProfile(
            profile_id="snowflake-source", db_type="snowflake", host="account",
            database="WAREHOUSE", username="reader", password="secret", max_rows=10,
        ))
        self.connection = connection

    def _driver(self):
        return type("Driver", (), {"connect": lambda _self, **_kwargs: self.connection})()


def test_snowflake_reader_generates_a_bounded_identifier_quoted_select() -> None:
    cursor = Cursor()
    connection = Connection(cursor)
    batches = tuple(Connector(connection).read_batches(
        relation=TransportRelation(catalog_name="WAREHOUSE", schema_name="PUBLIC", object_name="PEOPLE"),
        column_names=("ID", "FULL_NAME"), batch_size=2, timeout_seconds=11,
    ))

    assert [batch.row_count for batch in batches] == [2, 1]
    assert cursor.query == 'SELECT "ID", "FULL_NAME" FROM "WAREHOUSE"."PUBLIC"."PEOPLE"'
    assert cursor.timeout == 11
    assert cursor.closed is True and connection.closed is True
