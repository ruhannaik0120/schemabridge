# Automatic Spark transport

SchemaBridge automatically chooses Spark for eligible large PostgreSQL, MySQL,
and Snowflake tables. This is a transport implementation detail: a user approves the
migration mapping and target write as usual, but does not choose Spark for an
individual table.

## Routing rule

SchemaBridge uses Spark when all of these are true:

1. the discovered source row estimate is at least
   `SCHEMABRIDGE_SPARK_MINIMUM_SOURCE_ROWS` (default `1000000`);
2. the source and write-enabled staging target are PostgreSQL, MySQL, or
   Snowflake;
3. PostgreSQL/MySQL sources have one discovered integer primary-key column,
   which provides a safe JDBC partition key;
4. the optional PySpark runtime is installed; and
5. when either endpoint is Snowflake, the configured Spark packages include the
   Snowflake Spark Connector.

Otherwise it uses the existing bounded connector-batch transport. The routing
decision has a safe reason code such as `SOURCE_ROW_ESTIMATE_BELOW_THRESHOLD`,
`SOURCE_ROW_ESTIMATE_UNAVAILABLE`, `SPARK_RUNTIME_UNAVAILABLE`, or
`SPARK_JDBC_REQUIREMENTS_NOT_MET`.

Snowflake uses its dedicated Spark connector rather than the JDBC reader. If
that connector package is not configured, SchemaBridge falls back to normal
batches with `SNOWFLAKE_SPARK_CONNECTOR_UNAVAILABLE`.

## What Spark changes—and what it does not

Spark reads PostgreSQL/MySQL through an internally generated JDBC query and
splits it using integer primary-key bounds. Snowflake reads use a generated
query through the dedicated connector. Every path appends the resulting DataFrame
only to SchemaBridge's pre-created `SB_STAGE_<UUID>` table. It does not write
directly to a business target table and it never accepts caller-provided SQL.

SchemaBridge counts the DataFrame rows, counts the staging table rows, requires
an exact match, and stops the Spark session even when the load fails. Existing
workflow approval, staging cleanup, generated final SQL, idempotency, and
recovery rules remain unchanged.

## Local setup

Spark support is optional so ordinary installations remain small:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-spark.txt
```

Java is also required. For local execution, set `SCHEMABRIDGE_SPARK_MASTER` to
`local[2]` if you want two local worker threads. Relevant optional settings:

| Variable | Default | Purpose |
|---|---:|---|
| `SCHEMABRIDGE_SPARK_MINIMUM_SOURCE_ROWS` | `1000000` | Automatic Spark-routing threshold. |
| `SCHEMABRIDGE_SPARK_NUM_PARTITIONS` | `4` | JDBC partitions for an eligible source table. |
| `SCHEMABRIDGE_SPARK_MASTER` | empty | Spark master, for example `local[2]`. |
| `SCHEMABRIDGE_SPARK_JARS_PACKAGES` | empty | Maven connector packages needed by Spark. |
| `SCHEMABRIDGE_SPARK_APPLICATION_NAME` | `SchemaBridge` | Spark application name. |

For a local PostgreSQL proof, use
`org.postgresql:postgresql:42.7.5`. For MySQL, use
`com.mysql:mysql-connector-j:8.4.0`. In a managed deployment, make the required
JDBC driver available to the Spark cluster instead of relying on package
download at job start.

For Snowflake, configure both a compatible `spark-snowflake` package and its
compatible `snowflake-jdbc` package. The connector version must match the
Spark/Scala runtime. This is a cluster prerequisite, not a per-migration user
choice. See Snowflake's [connector installation guide](https://docs.snowflake.com/en/user-guide/spark-connector-install)
and [usage guide](https://docs.snowflake.com/en/user-guide/spark-connector-use).
Use the `SCHEMABRIDGE_SPARK_SNOWFLAKE_*` variables from `.env.example` only to
opt in to the non-production live proof.

## Verification evidence

The repository includes credential-free unit tests for settings, routing,
partition planning, JDBC plans, reader/writer behavior, and cleanup semantics.
It also contains opt-in live Docker tests:

- `tests/test_spark_postgresql_integration.py`: PostgreSQL → Spark → PostgreSQL;
- `tests/test_spark_mysql_integration.py`: MySQL → Spark → MySQL.
- `tests/test_spark_snowflake_integration.py`: Snowflake → Spark → Snowflake.

Each proof creates three source rows, checks equal source/staging counts, and
removes only generated proof objects. The PostgreSQL and MySQL proofs use two
Spark JDBC partitions. The Snowflake proof is disabled unless
`SCHEMABRIDGE_SPARK_SNOWFLAKE_INTEGRATION=1` and non-production account
variables are configured; it creates a unique transient source table and the
generated staging table is removed after verification.
