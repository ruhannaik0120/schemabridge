# Automatic Spark transport

SchemaBridge automatically chooses Spark for eligible large PostgreSQL and
MySQL tables. This is a transport implementation detail: a user approves the
migration mapping and target write as usual, but does not choose Spark for an
individual table.

## Routing rule

SchemaBridge uses Spark when all of these are true:

1. the discovered source row estimate is at least
   `SCHEMABRIDGE_SPARK_MINIMUM_SOURCE_ROWS` (default `1000000`);
2. the source is PostgreSQL or MySQL;
3. the table has one discovered integer primary-key column, which provides a
   safe JDBC partition key;
4. the write-enabled staging target is PostgreSQL or MySQL; and
5. the optional PySpark runtime is installed.

Otherwise it uses the existing bounded connector-batch transport. The routing
decision has a safe reason code such as `SOURCE_ROW_ESTIMATE_BELOW_THRESHOLD`,
`SOURCE_ROW_ESTIMATE_UNAVAILABLE`, `SPARK_RUNTIME_UNAVAILABLE`, or
`SPARK_JDBC_REQUIREMENTS_NOT_MET`.

Snowflake remains on the normal connector-batch path. Its production Spark
integration needs the dedicated Snowflake Spark Connector and its warehouse,
role, and temporary-stage controls; it is not implemented by this JDBC path.

## What Spark changes—and what it does not

Spark reads the source through an internally generated JDBC query, splits the
read using the integer primary-key bounds, and appends the resulting DataFrame
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
| `SCHEMABRIDGE_SPARK_JARS_PACKAGES` | empty | Maven JDBC package coordinate needed by Spark. |
| `SCHEMABRIDGE_SPARK_APPLICATION_NAME` | `SchemaBridge` | Spark application name. |

For a local PostgreSQL proof, use
`org.postgresql:postgresql:42.7.5`. For MySQL, use
`com.mysql:mysql-connector-j:8.4.0`. In a managed deployment, make the required
JDBC driver available to the Spark cluster instead of relying on package
download at job start.

## Verification evidence

The repository includes credential-free unit tests for settings, routing,
partition planning, JDBC plans, reader/writer behavior, and cleanup semantics.
It also contains opt-in live Docker tests:

- `tests/test_spark_postgresql_integration.py`: PostgreSQL → Spark → PostgreSQL;
- `tests/test_spark_mysql_integration.py`: MySQL → Spark → MySQL.

Both create three source rows, use two Spark JDBC partitions, assert equal
source/staging counts, and remove only generated proof objects.
