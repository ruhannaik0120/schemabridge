"""Fixed read-only Snowflake Information Schema queries for discovery."""

_CURRENT_DATABASE_QUERY = """
SELECT CURRENT_DATABASE() AS "current_database"
"""

_SCHEMAS_QUERY = """
SELECT
    CATALOG_NAME AS "catalog_name",
    SCHEMA_NAME AS "schema_name",
    SCHEMA_OWNER AS "owner",
    COMMENT AS "comment",
    (SCHEMA_NAME = 'INFORMATION_SCHEMA') AS "is_system_managed",
    CASE
        WHEN SCHEMA_NAME = 'INFORMATION_SCHEMA' THEN 'INFORMATION_SCHEMA'
        ELSE 'USER'
    END AS "schema_classification",
    (IS_TRANSIENT = 'YES') AS "is_transient",
    (IS_MANAGED_ACCESS = 'YES') AS "is_managed_access"
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE CATALOG_NAME = %s
ORDER BY SCHEMA_NAME
"""

_OBJECTS_QUERY = """
SELECT
    TABLE_CATALOG AS "catalog_name",
    TABLE_SCHEMA AS "schema_name",
    TABLE_NAME AS "object_name",
    TABLE_OWNER AS "owner",
    COMMENT AS "comment",
    TABLE_TYPE AS "table_type",
    (IS_TRANSIENT = 'YES') AS "is_transient",
    (IS_TEMPORARY = 'YES') AS "is_temporary",
    (IS_DYNAMIC = 'YES') AS "is_dynamic",
    (IS_ICEBERG = 'YES') AS "is_iceberg",
    (IS_HYBRID = 'YES') AS "is_hybrid",
    AUTO_CLUSTERING_ON AS "auto_clustering_on",
    (CLUSTERING_KEY IS NOT NULL) AS "has_clustering_key",
    ROW_COUNT AS "estimated_row_count"
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = %s
  AND TABLE_SCHEMA = %s
ORDER BY TABLE_TYPE, TABLE_NAME
"""
