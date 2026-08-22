# Adding a connector

A new generic backend implements `DatabaseConnector`, exports `Connector`, and receives one factory registration. That makes it available to `DatabaseService`; it does not automatically make it a supported durable migration source or target. The current durable workflow contains explicit PostgreSQL-source and Snowflake-target policy.

## Contract

```python
class ExampleConnector(DatabaseConnector):
    def connect(self, database=None, timeout_seconds=None): ...
    def test_connection(self, database=None, timeout_seconds=None): ...
    def health_check(self, database=None, timeout_seconds=None): ...
    def list_databases(self, timeout_seconds=None): ...
    def list_tables(self, database=None, schema=None, timeout_seconds=None): ...
    def describe_table(self, database=None, table=None, schema=None, timeout_seconds=None): ...
    def execute_query(self, query, *, parameters=None, database=None, timeout_seconds=None, max_rows=None): ...
    def close(self): ...

Connector = ExampleConnector
```

Register the module path in `SUPPORTED_CONNECTORS` inside `schemabridge/connectors/factory.py`.

## Rules

1. Import the vendor driver lazily and identify the required package in missing-driver errors.
2. Support the immutable `ConnectionProfile` injected by `ConnectorFactory.create_for_profile`; retain `Config.connection_config()` only for the existing generic/default path.
3. Never return credentials, tokens, private keys, or connection strings.
4. Parameterize metadata filters such as schema and table names.
5. Apply timeouts and returned-row limits with native driver features where possible.
6. Commit successful data-changing statements for transactional drivers.
7. Return stable dictionaries expected by `DatabaseService`.
8. Close cursors and connections in `finally` blocks or context managers.
9. Never import vendor drivers outside `connectors/` or the control-plane persistence boundary.
10. Add fake-driver unit tests and perform opt-in live verification separately.

Supporting a new backend in durable orchestration is separate feature work. It requires explicit discovery, execution, validation, safety, and recovery policy rather than only a factory entry.

## Bounded transport extensions

Connectors can also implement one or both optional transport capabilities:

```python
class ExampleSourceReader:
    def read_batches(self, *, relation, column_names, batch_size, timeout_seconds): ...

class ExampleStagingWriter:
    def prepare_staging_table(self, *, definition, timeout_seconds): ...
    def write_batch(self, *, definition, batch, timeout_seconds): ...
    def drop_staging_table(self, *, relation, timeout_seconds): ...
```

The contracts are `BatchSourceReader` and `StagingTableWriter` in
`schemabridge.transport.base`. `BatchTransportService` is deliberately
vendor-neutral: it streams one `DataBatch` at a time and accepts a batch only
when `BatchWriteResult` confirms the exact received row count.

### Reader requirements

1. Build the `SELECT` statement from separately quoted relation and column identifiers; do not accept arbitrary SQL.
2. Fetch at most `batch_size` rows per batch, enforce the profile `max_rows` limit, and close resources if a consumer stops early.
3. Use the supplied timeout and translate driver failures to sanitized transport exceptions.
4. A source profile need not be write-enabled.

### Writer requirements

1. Require a named `ConnectionProfile` with `write_enabled=true`; generic environment configuration is never sufficient for staging writes.
2. Require the staging relation database to exactly match the target profile. Quote every identifier component; never splice untrusted text as SQL.
3. Create a non-overwriting, SchemaBridge-managed landing table. Map canonical values losslessly; fall back to text when target numeric or temporal limits could lose information, and reject `UNKNOWN` rather than guessing.
4. Bind every row value. Serialize semi-structured values deterministically before using the target's JSON representation.
5. Commit only after the driver confirms that it wrote the full batch. Any partial or absent row-count confirmation must roll back and return no success evidence.
6. Implement `DROP TABLE IF EXISTS` for the exact relation so recovery can prove cleanup before a retry.

Snowflake, PostgreSQL, and MySQL currently implement both optional transport
roles. The focused fake-driver tests cover identifier quoting, profile gating,
timeouts/error sanitization, exact row-count checks, and a Snowflake-to-
PostgreSQL bounded-transfer proof. Add a comparable proof whenever a new
source/writer combination is introduced.
