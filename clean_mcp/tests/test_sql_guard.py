"""SQL validation tests for the MCP execution framework."""

from validation.sql_guard import validate_query


def test_validate_query_rejects_sql_comments():
    ok, reason = validate_query("SELECT 1 -- comment")
    assert ok is False
    assert "comments" in reason.lower()


def test_validate_query_allows_select():
    ok, reason = validate_query("SELECT name FROM sys.databases")
    assert ok is True
    assert reason == ""


def test_validator_accepts_approved_write_statement():
    ok, reason = validate_query("UPDATE users SET is_active = 1")
    assert ok is True
    assert reason == ""


def test_validate_query_rejects_multiple_statements():
    ok, reason = validate_query("UPDATE users SET is_active = 1; DELETE FROM users")
    assert ok is False
    assert "multiple statements" in reason.lower()


def test_validate_query_allows_cte():
    ok, reason = validate_query("WITH cte AS (SELECT 1 AS value) SELECT * FROM cte")
    assert ok is True
    assert reason == ""


def test_validate_query_allows_comment_tokens_inside_string_literals():
    ok, reason = validate_query("SELECT * FROM logs WHERE message = 'value--part'")
    assert ok is True
    assert reason == ""


def test_validate_query_allows_semicolon_inside_string_literal():
    ok, reason = validate_query("SELECT 'alpha;beta' AS value")
    assert ok is True
    assert reason == ""


def test_sqlserver_blocks_write_after_select_without_semicolon():
    ok, reason = validate_query("SELECT 1\nDELETE FROM audit_log", "sqlserver")

    assert ok is False
    assert "multiple statements" in reason.lower()


def test_sqlserver_quoted_write_word_does_not_trigger_batch_guard():
    ok, reason = validate_query("SELECT [update] FROM audit_log", "sqlserver")

    assert ok is True
    assert reason == ""


def test_mysql_hash_comment_is_blocked():
    ok, reason = validate_query("SELECT 1 # hidden comment", "mysql")

    assert ok is False
    assert "comments" in reason.lower()


def test_sqlserver_blocks_two_write_statements_without_semicolon():
    ok, reason = validate_query("UPDATE items SET active = 1\nDELETE FROM audit_log", "sqlserver")

    assert ok is False
    assert "multiple statements" in reason.lower()


def test_sqlserver_allows_cte_followed_by_its_main_update():
    ok, reason = validate_query(
        "WITH target AS (SELECT id FROM items)\nUPDATE items SET active = 1 WHERE id IN (SELECT id FROM target)",
        "sqlserver",
    )

    assert ok is True
    assert reason == ""


def test_sqlserver_allows_union_select_on_new_line():
    ok, reason = validate_query("SELECT 1\nUNION ALL\nSELECT 2", "sqlserver")

    assert ok is True
    assert reason == ""


def test_sqlserver_blocks_same_line_statement_after_write():
    for query in (
        "UPDATE items SET active = 1 DELETE FROM audit_log",
        "UPDATE items SET active = 1 SELECT 1",
    ):
        ok, reason = validate_query(query, "sqlserver")
        assert ok is False
        assert "multiple statements" in reason.lower()


def test_sqlserver_allows_multiline_merge_actions():
    query = """
        MERGE target AS t
        USING source AS s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.value = s.value
        WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """

    ok, reason = validate_query(query, "sqlserver")

    assert ok is True
    assert reason == ""


def test_sqlserver_allows_table_hint_with_keyword():
    ok, reason = validate_query("SELECT * FROM items WITH (NOLOCK)", "sqlserver")

    assert ok is True
    assert reason == ""


def test_sqlserver_blocks_additional_waitfor_or_set_statement():
    for query in (
        "SELECT 1 WAITFOR DELAY '00:00:01'",
        "SELECT 1 SET NOCOUNT ON",
    ):
        ok, reason = validate_query(query, "sqlserver")
        assert ok is False
        assert "multiple statements" in reason.lower()


def test_sqlserver_allows_update_set_clause():
    ok, reason = validate_query("UPDATE items SET active = 1", "sqlserver")

    assert ok is True
    assert reason == ""
