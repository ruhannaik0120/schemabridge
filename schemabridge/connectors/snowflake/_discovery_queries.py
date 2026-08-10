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

_TABLE_METADATA_QUERY = """
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
    (IS_IMMUTABLE = 'YES') AS "is_immutable",
    CLUSTERING_KEY AS "clustering_expression",
    AUTO_CLUSTERING_ON AS "auto_clustering_on",
    (CLUSTERING_KEY IS NOT NULL) AS "has_clustering_key",
    ROW_COUNT AS "estimated_row_count"
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = %s
  AND TABLE_SCHEMA = %s
  AND TABLE_NAME = %s
"""

_COLUMNS_QUERY = """
SELECT
    COLUMN_NAME AS "column_name",
    ORDINAL_POSITION AS "ordinal_position",
    DATA_TYPE AS "data_type",
    DATA_TYPE_ALIAS AS "data_type_alias",
    IS_NULLABLE AS "is_nullable",
    CHARACTER_MAXIMUM_LENGTH AS "character_maximum_length",
    NUMERIC_PRECISION AS "numeric_precision",
    NUMERIC_SCALE AS "numeric_scale",
    DATETIME_PRECISION AS "datetime_precision",
    COLUMN_DEFAULT AS "column_default",
    COMMENT AS "column_comment",
    COLLATION_NAME AS "collation_name",
    (IS_IDENTITY = 'YES') AS "is_identity",
    IDENTITY_GENERATION AS "identity_generation",
    IDENTITY_START AS "identity_start",
    IDENTITY_INCREMENT AS "identity_increment",
    (IDENTITY_CYCLE = 'YES') AS "identity_cycle",
    (IDENTITY_ORDERED = 'YES') AS "identity_ordered",
    EXPRESSION AS "generation_expression",
    KIND AS "kind",
    DTD_IDENTIFIER AS "dtd_identifier"
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = %s
  AND TABLE_SCHEMA = %s
  AND TABLE_NAME = %s
ORDER BY ORDINAL_POSITION, COLUMN_NAME
"""

_ELEMENT_TYPES_QUERY = """
SELECT
    COLLECTION_TYPE_IDENTIFIER AS "collection_type_identifier",
    DATA_TYPE AS "data_type",
    DTD_IDENTIFIER AS "dtd_identifier"
FROM INFORMATION_SCHEMA.ELEMENT_TYPES
WHERE OBJECT_CATALOG = %s
  AND OBJECT_SCHEMA = %s
  AND OBJECT_NAME = %s
ORDER BY COLLECTION_TYPE_IDENTIFIER, DTD_IDENTIFIER
"""

_KEY_CONSTRAINTS_QUERY = """
SELECT
    CONSTRAINT_NAME AS "constraint_name",
    CONSTRAINT_TYPE AS "constraint_type",
    (ENFORCED = 'YES') AS "is_enforced",
    (RELY = 'YES') AS "is_rely",
    (IS_DEFERRABLE = 'YES') AS "is_deferrable",
    (INITIALLY_DEFERRED = 'YES') AS "initially_deferred",
    COMMENT AS "comment"
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_CATALOG = %s
  AND TABLE_SCHEMA = %s
  AND TABLE_NAME = %s
  AND CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY CONSTRAINT_TYPE, CONSTRAINT_NAME
"""

_FOREIGN_KEYS_QUERY = """
SELECT
    local_constraint.CONSTRAINT_NAME AS "constraint_name",
    referenced_constraint.TABLE_CATALOG AS "referenced_catalog",
    referenced_constraint.TABLE_SCHEMA AS "referenced_schema",
    referenced_constraint.TABLE_NAME AS "referenced_table",
    reference.MATCH_OPTION AS "match_option",
    reference.UPDATE_RULE AS "update_rule",
    reference.DELETE_RULE AS "delete_rule",
    (local_constraint.ENFORCED = 'YES') AS "is_enforced",
    (local_constraint.RELY = 'YES') AS "is_rely",
    (local_constraint.IS_DEFERRABLE = 'YES') AS "is_deferrable",
    (local_constraint.INITIALLY_DEFERRED = 'YES') AS "initially_deferred",
    local_constraint.COMMENT AS "comment"
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS local_constraint
JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS reference
  ON reference.CONSTRAINT_CATALOG = local_constraint.CONSTRAINT_CATALOG
 AND reference.CONSTRAINT_SCHEMA = local_constraint.CONSTRAINT_SCHEMA
 AND reference.CONSTRAINT_NAME = local_constraint.CONSTRAINT_NAME
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS referenced_constraint
  ON referenced_constraint.CONSTRAINT_CATALOG = reference.UNIQUE_CONSTRAINT_CATALOG
 AND referenced_constraint.CONSTRAINT_SCHEMA = reference.UNIQUE_CONSTRAINT_SCHEMA
 AND referenced_constraint.CONSTRAINT_NAME = reference.UNIQUE_CONSTRAINT_NAME
WHERE local_constraint.TABLE_CATALOG = %s
  AND local_constraint.TABLE_SCHEMA = %s
  AND local_constraint.TABLE_NAME = %s
  AND local_constraint.CONSTRAINT_TYPE = 'FOREIGN KEY'
ORDER BY local_constraint.CONSTRAINT_NAME
"""

_CHECK_CONSTRAINTS_QUERY = """
SELECT
    CONSTRAINT_CATALOG AS "constraint_catalog",
    CONSTRAINT_SCHEMA AS "constraint_schema",
    CONSTRAINT_TABLE AS "constraint_table",
    CONSTRAINT_NAME AS "constraint_name",
    CHECK_CLAUSE AS "expression"
FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS
WHERE CONSTRAINT_CATALOG = %s
  AND CONSTRAINT_SCHEMA = %s
  AND CONSTRAINT_TABLE = %s
ORDER BY CONSTRAINT_NAME
"""

_VIEW_DEFINITION_QUERY = """
SELECT
    VIEW_DEFINITION AS "view_definition",
    (IS_SECURE = 'YES') AS "is_secure"
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_CATALOG = %s
  AND TABLE_SCHEMA = %s
  AND TABLE_NAME = %s
"""
