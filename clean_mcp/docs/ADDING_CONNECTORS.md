# Adding A Connector

A new backend implements `DatabaseConnector`, exports `Connector`, and receives one factory registration. MCP tools and services remain unchanged.

## Contract

```python
class ExampleConnector(DatabaseConnector):
    def connect(self, database=None, timeout_seconds=None): ...
    def test_connection(self, database=None, timeout_seconds=None): ...
    def health_check(self, database=None, timeout_seconds=None): ...
    def list_databases(self, timeout_seconds=None): ...
    def list_tables(self, database=None, schema=None, timeout_seconds=None): ...
    def describe_table(self, database=None, table=None, schema=None, timeout_seconds=None): ...
    def execute_query(self, query, *, database=None, timeout_seconds=None, max_rows=None): ...
    def close(self): ...

Connector = ExampleConnector
```

Register the module path in `SUPPORTED_CONNECTORS` inside `connectors/factory.py`.

## Rules

1. Import the vendor driver lazily and identify the required package in missing-driver errors.
2. Read settings only through `Config.connection_config()`.
3. Never return credentials, tokens, private keys, or connection strings.
4. Parameterize metadata filters such as schema and table names.
5. Apply timeouts and returned-row limits with native driver features where possible.
6. Commit successful data-changing statements for transactional drivers.
7. Return stable dictionaries expected by `QueryService`.
8. Close cursors and connections in `finally` blocks or context managers.
9. Never import vendor drivers from `server.py`, `tools/`, or `services/`.
10. Add fake-driver unit tests and perform opt-in live verification separately.
