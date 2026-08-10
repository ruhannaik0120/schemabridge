"""Fixed read-only PostgreSQL catalog queries used by canonical discovery."""

_CAPABILITIES_QUERY = """
SELECT
    current_database()::text AS current_database,
    current_setting('server_version_num')::integer AS server_version_num,
    current_setting('max_identifier_length')::integer AS max_identifier_length,
    to_regprocedure('pg_catalog.pg_get_partkeydef(oid)') IS NOT NULL AS has_partition_key_helper,
    to_regprocedure('pg_catalog.pg_get_partition_constraintdef(oid)') IS NOT NULL AS has_partition_constraint_helper
"""

_IDENTIFIER_LENGTH_QUERY = """
SELECT octet_length(convert_to(%s::text, current_setting('server_encoding')))::integer AS byte_length
"""

_SCHEMAS_QUERY = """
SELECT
    current_database()::text AS catalog_name,
    n.nspname::text AS schema_name,
    r.rolname::text AS owner,
    obj_description(n.oid, 'pg_namespace')::text AS comment,
    CASE
        WHEN n.nspname = 'pg_catalog' THEN 'POSTGRESQL_CATALOG'
        WHEN n.nspname = 'information_schema' THEN 'INFORMATION_SCHEMA'
        WHEN left(n.nspname, 14) = 'pg_toast_temp_' THEN 'POSTGRESQL_TOAST_TEMPORARY'
        WHEN n.nspname = 'pg_toast' OR left(n.nspname, 9) = 'pg_toast_' THEN 'POSTGRESQL_TOAST'
        WHEN left(n.nspname, 8) = 'pg_temp_' THEN 'POSTGRESQL_TEMPORARY'
        ELSE 'USER'
    END::text AS schema_classification,
    (n.nspname = 'pg_catalog'
        OR n.nspname = 'information_schema'
        OR n.nspname = 'pg_toast'
        OR left(n.nspname, 9) = 'pg_toast_'
        OR left(n.nspname, 8) = 'pg_temp_') AS is_system_managed
FROM pg_catalog.pg_namespace AS n
JOIN pg_catalog.pg_roles AS r ON r.oid = n.nspowner
WHERE current_database() = %s
ORDER BY n.nspname
"""

_OBJECTS_QUERY = """
SELECT
    current_database()::text AS catalog_name,
    n.nspname::text AS schema_name,
    c.relname::text AS object_name,
    c.relkind::text AS relkind,
    c.relpersistence::text AS relpersistence,
    r.rolname::text AS owner,
    obj_description(c.oid, 'pg_class')::text AS comment,
    CASE
        WHEN c.relkind::text = ANY(ARRAY['r', 'p', 'm', 'f']::text[]) AND c.reltuples >= 0
            THEN c.reltuples::bigint
        ELSE NULL
    END AS estimated_row_count,
    c.relispartition AS is_partition_child,
    (n.nspname = 'pg_catalog'
        OR n.nspname = 'information_schema'
        OR n.nspname = 'pg_toast'
        OR left(n.nspname, 9) = 'pg_toast_'
        OR left(n.nspname, 8) = 'pg_temp_') AS is_system_managed,
    CASE
        WHEN n.nspname = 'pg_catalog' THEN 'POSTGRESQL_CATALOG'
        WHEN n.nspname = 'information_schema' THEN 'INFORMATION_SCHEMA'
        WHEN left(n.nspname, 14) = 'pg_toast_temp_' THEN 'POSTGRESQL_TOAST_TEMPORARY'
        WHEN n.nspname = 'pg_toast' OR left(n.nspname, 9) = 'pg_toast_' THEN 'POSTGRESQL_TOAST'
        WHEN left(n.nspname, 8) = 'pg_temp_' THEN 'POSTGRESQL_TEMPORARY'
        ELSE 'USER'
    END::text AS schema_classification
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_roles AS r ON r.oid = c.relowner
WHERE current_database() = %s
  AND n.nspname = %s
  AND c.relkind::text = ANY(%s::text[])
ORDER BY c.relkind::text, c.relname
"""

_BASE_OBJECT_QUERY = """
SELECT
    c.oid AS relation_oid,
    current_database()::text AS catalog_name,
    n.nspname::text AS schema_name,
    c.relname::text AS object_name,
    c.relkind::text AS relkind,
    c.relpersistence::text AS relpersistence,
    r.rolname::text AS owner,
    obj_description(c.oid, 'pg_class')::text AS comment,
    CASE
        WHEN c.relkind::text = ANY(ARRAY['r', 'p', 'm', 'f']::text[]) AND c.reltuples >= 0
            THEN c.reltuples::bigint
        ELSE NULL
    END AS estimated_row_count,
    c.relispartition AS is_partition_child,
    (n.nspname = 'pg_catalog'
        OR n.nspname = 'information_schema'
        OR n.nspname = 'pg_toast'
        OR left(n.nspname, 9) = 'pg_toast_'
        OR left(n.nspname, 8) = 'pg_temp_') AS is_system_managed,
    CASE
        WHEN n.nspname = 'pg_catalog' THEN 'POSTGRESQL_CATALOG'
        WHEN n.nspname = 'information_schema' THEN 'INFORMATION_SCHEMA'
        WHEN left(n.nspname, 14) = 'pg_toast_temp_' THEN 'POSTGRESQL_TOAST_TEMPORARY'
        WHEN n.nspname = 'pg_toast' OR left(n.nspname, 9) = 'pg_toast_' THEN 'POSTGRESQL_TOAST'
        WHEN left(n.nspname, 8) = 'pg_temp_' THEN 'POSTGRESQL_TEMPORARY'
        ELSE 'USER'
    END::text AS schema_classification
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_roles AS r ON r.oid = c.relowner
WHERE current_database() = %s
  AND n.nspname = %s
  AND c.relname = %s
  AND c.relkind::text = ANY(ARRAY['r', 'p', 'v', 'm', 'f']::text[])
"""

_COLUMNS_QUERY = """
SELECT
    a.attname::text AS column_name,
    a.attnum::integer AS ordinal_position,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS data_type,
    (NOT a.attnotnull) AS is_nullable,
    ic.character_maximum_length::integer AS character_maximum_length,
    ic.numeric_precision::integer AS numeric_precision,
    ic.numeric_scale::integer AS numeric_scale,
    ic.datetime_precision::integer AS datetime_precision,
    CASE WHEN a.attgenerated = '' THEN pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) ELSE NULL END::text AS column_default,
    pg_catalog.col_description(a.attrelid, a.attnum)::text AS column_comment,
    coll.collname::text AS collation_name,
    (a.attidentity <> '') AS is_identity,
    CASE a.attidentity WHEN 'a' THEN 'ALWAYS' WHEN 'd' THEN 'BY DEFAULT' ELSE NULL END::text AS identity_generation,
    (a.attidentity <> '' OR (ad.adbin IS NOT NULL AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) LIKE 'nextval(%')) AS is_auto_increment,
    (a.attgenerated <> '') AS is_generated,
    CASE WHEN a.attgenerated <> '' THEN pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) ELSE NULL END::text AS generation_expression,
    a.attndims::integer AS array_dimensions,
    CASE WHEN t.typelem <> 0 THEN pg_catalog.format_type(t.typelem, NULL) ELSE NULL END::text AS element_native_type,
    CASE a.attgenerated WHEN 's' THEN 'STORED' WHEN 'v' THEN 'VIRTUAL' ELSE 'NONE' END::text AS generation_kind
FROM pg_catalog.pg_attribute AS a
JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_type AS t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_attrdef AS ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
LEFT JOIN pg_catalog.pg_collation AS coll ON coll.oid = a.attcollation
LEFT JOIN information_schema.columns AS ic
    ON ic.table_catalog = current_database()
   AND ic.table_schema = n.nspname
   AND ic.table_name = c.relname
   AND ic.column_name = a.attname
WHERE a.attrelid = %s::oid
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
"""

_KEY_CONSTRAINTS_QUERY_V14 = """
SELECT
    con.oid::text AS constraint_oid,
    con.conname::text AS constraint_name,
    con.contype::text AS constraint_type,
    a.attname::text AS column_name,
    key_columns.ordinal_position::integer AS key_sequence,
    TRUE AS is_enforced,
    con.convalidated AS is_validated,
    con.condeferrable AS is_deferrable,
    con.condeferred AS initially_deferred,
    obj_description(con.oid, 'pg_constraint')::text AS comment
FROM pg_catalog.pg_constraint AS con
JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key_columns(attnum, ordinal_position) ON TRUE
LEFT JOIN pg_catalog.pg_attribute AS a ON a.attrelid = con.conrelid AND a.attnum = key_columns.attnum
WHERE con.conrelid = %s::oid
  AND con.contype::text = ANY(ARRAY['p', 'u']::text[])
ORDER BY con.oid, key_columns.ordinal_position
"""

_KEY_CONSTRAINTS_QUERY_V18 = """
SELECT
    con.oid::text AS constraint_oid,
    con.conname::text AS constraint_name,
    con.contype::text AS constraint_type,
    a.attname::text AS column_name,
    key_columns.ordinal_position::integer AS key_sequence,
    con.conenforced AS is_enforced,
    con.convalidated AS is_validated,
    con.condeferrable AS is_deferrable,
    con.condeferred AS initially_deferred,
    obj_description(con.oid, 'pg_constraint')::text AS comment
FROM pg_catalog.pg_constraint AS con
JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key_columns(attnum, ordinal_position) ON TRUE
LEFT JOIN pg_catalog.pg_attribute AS a ON a.attrelid = con.conrelid AND a.attnum = key_columns.attnum
WHERE con.conrelid = %s::oid
  AND con.contype::text = ANY(ARRAY['p', 'u']::text[])
ORDER BY con.oid, key_columns.ordinal_position
"""

_FOREIGN_KEYS_QUERY_V14 = """
SELECT
    con.oid::text AS constraint_oid,
    con.conname::text AS constraint_name,
    local_attribute.attname::text AS local_column_name,
    referenced_attribute.attname::text AS referenced_column_name,
    current_database()::text AS referenced_catalog,
    referenced_namespace.nspname::text AS referenced_schema,
    referenced_class.relname::text AS referenced_table,
    local_keys.ordinal_position::integer AS key_sequence,
    array_length(con.conkey, 1)::integer AS expected_column_count,
    CASE con.confmatchtype WHEN 'f' THEN 'FULL' WHEN 'p' THEN 'PARTIAL' WHEN 's' THEN 'SIMPLE' ELSE NULL END::text AS match_option,
    CASE con.confupdtype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' ELSE NULL END::text AS update_rule,
    CASE con.confdeltype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' ELSE NULL END::text AS delete_rule,
    TRUE AS is_enforced,
    con.convalidated AS is_validated,
    con.condeferrable AS is_deferrable,
    con.condeferred AS initially_deferred,
    obj_description(con.oid, 'pg_constraint')::text AS comment
FROM pg_catalog.pg_constraint AS con
JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS local_keys(attnum, ordinal_position) ON TRUE
LEFT JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS referenced_keys(attnum, ordinal_position)
    ON referenced_keys.ordinal_position = local_keys.ordinal_position
LEFT JOIN pg_catalog.pg_attribute AS local_attribute
    ON local_attribute.attrelid = con.conrelid AND local_attribute.attnum = local_keys.attnum
LEFT JOIN pg_catalog.pg_attribute AS referenced_attribute
    ON referenced_attribute.attrelid = con.confrelid AND referenced_attribute.attnum = referenced_keys.attnum
JOIN pg_catalog.pg_class AS referenced_class ON referenced_class.oid = con.confrelid
JOIN pg_catalog.pg_namespace AS referenced_namespace ON referenced_namespace.oid = referenced_class.relnamespace
WHERE con.conrelid = %s::oid
  AND con.contype = 'f'
ORDER BY con.oid, local_keys.ordinal_position
"""

_FOREIGN_KEYS_QUERY_V18 = """
SELECT
    con.oid::text AS constraint_oid,
    con.conname::text AS constraint_name,
    local_attribute.attname::text AS local_column_name,
    referenced_attribute.attname::text AS referenced_column_name,
    current_database()::text AS referenced_catalog,
    referenced_namespace.nspname::text AS referenced_schema,
    referenced_class.relname::text AS referenced_table,
    local_keys.ordinal_position::integer AS key_sequence,
    array_length(con.conkey, 1)::integer AS expected_column_count,
    CASE con.confmatchtype WHEN 'f' THEN 'FULL' WHEN 'p' THEN 'PARTIAL' WHEN 's' THEN 'SIMPLE' ELSE NULL END::text AS match_option,
    CASE con.confupdtype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' ELSE NULL END::text AS update_rule,
    CASE con.confdeltype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' ELSE NULL END::text AS delete_rule,
    con.conenforced AS is_enforced,
    con.convalidated AS is_validated,
    con.condeferrable AS is_deferrable,
    con.condeferred AS initially_deferred,
    obj_description(con.oid, 'pg_constraint')::text AS comment
FROM pg_catalog.pg_constraint AS con
JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS local_keys(attnum, ordinal_position) ON TRUE
LEFT JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS referenced_keys(attnum, ordinal_position)
    ON referenced_keys.ordinal_position = local_keys.ordinal_position
LEFT JOIN pg_catalog.pg_attribute AS local_attribute
    ON local_attribute.attrelid = con.conrelid AND local_attribute.attnum = local_keys.attnum
LEFT JOIN pg_catalog.pg_attribute AS referenced_attribute
    ON referenced_attribute.attrelid = con.confrelid AND referenced_attribute.attnum = referenced_keys.attnum
JOIN pg_catalog.pg_class AS referenced_class ON referenced_class.oid = con.confrelid
JOIN pg_catalog.pg_namespace AS referenced_namespace ON referenced_namespace.oid = referenced_class.relnamespace
WHERE con.conrelid = %s::oid
  AND con.contype = 'f'
ORDER BY con.oid, local_keys.ordinal_position
"""

_CHECK_CONSTRAINTS_QUERY_V14 = """
SELECT
    con.oid::text AS constraint_oid,
    con.conname::text AS constraint_name,
    pg_catalog.pg_get_expr(con.conbin, con.conrelid)::text AS expression,
    pg_catalog.pg_get_constraintdef(con.oid, FALSE)::text AS constraint_definition,
    TRUE AS is_enforced,
    con.convalidated AS is_validated,
    obj_description(con.oid, 'pg_constraint')::text AS comment
FROM pg_catalog.pg_constraint AS con
WHERE con.conrelid = %s::oid
  AND con.contype = 'c'
ORDER BY con.oid
"""

_CHECK_CONSTRAINTS_QUERY_V18 = """
SELECT
    con.oid::text AS constraint_oid,
    con.conname::text AS constraint_name,
    pg_catalog.pg_get_expr(con.conbin, con.conrelid)::text AS expression,
    pg_catalog.pg_get_constraintdef(con.oid, FALSE)::text AS constraint_definition,
    con.conenforced AS is_enforced,
    con.convalidated AS is_validated,
    obj_description(con.oid, 'pg_constraint')::text AS comment
FROM pg_catalog.pg_constraint AS con
WHERE con.conrelid = %s::oid
  AND con.contype = 'c'
ORDER BY con.oid
"""

_VIEW_DEFINITION_QUERY = """
SELECT pg_catalog.pg_get_viewdef(%s::oid, FALSE)::text AS view_definition
"""

_PARTITION_QUERY = """
SELECT
    (c.relkind = 'p') AS is_partitioned,
    c.relispartition AS is_partition_child,
    parent_namespace.nspname::text AS parent_schema,
    parent_class.relname::text AS parent_table,
    CASE partitioned_table.partstrat WHEN 'h' THEN 'HASH' WHEN 'l' THEN 'LIST' WHEN 'r' THEN 'RANGE' ELSE NULL END::text AS partition_strategy,
    CASE
        WHEN c.relkind = 'p' THEN pg_catalog.pg_get_partkeydef(c.oid)
        WHEN c.relispartition THEN pg_catalog.pg_get_partkeydef(parent_class.oid)
        ELSE NULL
    END::text AS partitioning_expression,
    CASE WHEN c.relispartition THEN pg_catalog.pg_get_expr(c.relpartbound, c.oid) ELSE NULL END::text AS partition_bound,
    CASE WHEN c.relispartition THEN pg_catalog.pg_get_partition_constraintdef(c.oid) ELSE NULL END::text AS partition_constraint
FROM pg_catalog.pg_class AS c
LEFT JOIN pg_catalog.pg_inherits AS inheritance ON inheritance.inhrelid = c.oid AND c.relispartition
LEFT JOIN pg_catalog.pg_class AS parent_class ON parent_class.oid = inheritance.inhparent
LEFT JOIN pg_catalog.pg_namespace AS parent_namespace ON parent_namespace.oid = parent_class.relnamespace
LEFT JOIN pg_catalog.pg_partitioned_table AS partitioned_table
    ON partitioned_table.partrelid = CASE WHEN c.relkind = 'p' THEN c.oid ELSE parent_class.oid END
WHERE c.oid = %s::oid
"""

_PARTITION_QUERY_WITHOUT_HELPERS = """
SELECT
    (c.relkind = 'p') AS is_partitioned,
    c.relispartition AS is_partition_child,
    parent_namespace.nspname::text AS parent_schema,
    parent_class.relname::text AS parent_table,
    CASE partitioned_table.partstrat WHEN 'h' THEN 'HASH' WHEN 'l' THEN 'LIST' WHEN 'r' THEN 'RANGE' ELSE NULL END::text AS partition_strategy,
    NULL::text AS partitioning_expression,
    CASE WHEN c.relispartition THEN pg_catalog.pg_get_expr(c.relpartbound, c.oid) ELSE NULL END::text AS partition_bound,
    NULL::text AS partition_constraint
FROM pg_catalog.pg_class AS c
LEFT JOIN pg_catalog.pg_inherits AS inheritance ON inheritance.inhrelid = c.oid AND c.relispartition
LEFT JOIN pg_catalog.pg_class AS parent_class ON parent_class.oid = inheritance.inhparent
LEFT JOIN pg_catalog.pg_namespace AS parent_namespace ON parent_namespace.oid = parent_class.relnamespace
LEFT JOIN pg_catalog.pg_partitioned_table AS partitioned_table
    ON partitioned_table.partrelid = CASE WHEN c.relkind = 'p' THEN c.oid ELSE parent_class.oid END
WHERE c.oid = %s::oid
"""
