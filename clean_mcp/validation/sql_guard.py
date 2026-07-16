"""SQL validation guardrails for the MCP execution framework.

This module keeps the validation lightweight and readable. It should not parse
SQL fully or take on connector responsibilities.
"""

from __future__ import annotations

import re

from logger import logger

_STATEMENT_STARTS = frozenset(
    {
        "SELECT",
        "WITH",
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "EXEC",
        "EXECUTE",
        "USE",
        "SET",
        "WAITFOR",
        "DECLARE",
        "PRINT",
        "RAISERROR",
        "THROW",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVE",
        "BACKUP",
        "RESTORE",
        "DBCC",
        "CHECKPOINT",
        "KILL",
        "SHUTDOWN",
        "DENY",
    }
)

def _strip_quoted_content(sql: str) -> str:
    """Replace literals and quoted identifiers before structural scanning."""

    patterns = (
        r"N?'(?:''|[^'])*'",
        r'"(?:""|[^"])*"',
        r"\[(?:\]\]|[^\]])*\]",
        r"`(?:``|[^`])*`",
    )
    stripped = sql
    for pattern in patterns:
        stripped = re.sub(pattern, "''", stripped)
    return stripped


def normalize_query(sql: str) -> str:
    """Normalize whitespace and trailing delimiters for validation."""

    return sql.strip().rstrip(";").strip()


def _top_level_words(sql: str) -> list[tuple[str, int, int]]:
    """Tokenize words outside parentheses after quoted content is removed."""

    words: list[tuple[str, int, int]] = []
    depth = 0
    for match in re.finditer(r"[A-Za-z_][A-Za-z0-9_$#]*|[()]", sql):
        token = match.group(0)
        if token == "(":
            depth += 1
        elif token == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            words.append((token.upper(), match.start(), match.end()))
    return words


def _has_adjacent_tsql_statement(sql: str) -> bool:
    """Detect a second top-level T-SQL command without misreading common clauses."""

    words = _top_level_words(sql)
    if not words:
        return False

    main_index = 0
    if words[0][0] == "WITH":
        for index, (word, _, _) in enumerate(words[1:], start=1):
            if word in {"SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"}:
                main_index = index
                break

    main_command = words[main_index][0]
    insert_source_seen = False
    update_set_seen = False
    for index in range(main_index + 1, len(words)):
        word, _, end = words[index]
        previous = words[index - 1][0]
        if word not in _STATEMENT_STARTS:
            continue
        if word == "WITH" and sql[end:].lstrip().startswith("("):
            # SQL Server table hint, for example FROM items WITH (NOLOCK).
            continue
        if word == "SET":
            if main_command == "UPDATE" and not update_set_seen:
                update_set_seen = True
                continue
            if main_command == "MERGE" and previous == "UPDATE":
                continue
            return True
        if word == "SELECT":
            if previous in {"UNION", "ALL", "INTERSECT", "EXCEPT"}:
                continue
            if main_command == "INSERT" and not insert_source_seen:
                insert_source_seen = True
                continue
            if main_command in {"CREATE", "ALTER"} and previous == "AS":
                continue
            return True
        if word in {"INSERT", "UPDATE", "DELETE"} and main_command == "MERGE" and previous == "THEN":
            continue
        if word in {"EXEC", "EXECUTE"} and main_command == "INSERT" and not insert_source_seen:
            insert_source_seen = True
            continue
        return True
    return False


def validate_query(sql: str, db_type: str = "") -> tuple[bool, str]:
    """Validate that an approved request contains one unambiguous statement."""

    if not sql or not sql.strip():
        return False, "Empty query is not allowed."

    normalized = normalize_query(sql)
    if not normalized:
        return False, "Empty query is not allowed."

    # Strip quoted text before scanning so values such as 'value--part' do not
    # look like SQL comments or forbidden commands.
    stripped_for_comments = _strip_quoted_content(normalized)
    if re.search(r"--|/\*|\*/", stripped_for_comments):
        logger.warning("Blocked query containing SQL comments.")
        return False, "Query blocked - SQL comments are not permitted."
    if db_type.strip().lower() == "mysql" and "#" in stripped_for_comments:
        logger.warning("Blocked query containing a MySQL hash comment.")
        return False, "Query blocked - SQL comments are not permitted."

    # One tool invocation maps to one auditable statement. Multiple statements
    # could hide a write operation behind an otherwise valid SELECT.
    statement_count = len([statement for statement in stripped_for_comments.split(";") if statement.strip()])
    if statement_count > 1:
        logger.warning("Blocked query containing multiple statements.")
        return False, "Query blocked - multiple statements are not permitted."

    if db_type.strip().lower() == "sqlserver":
        if _has_adjacent_tsql_statement(stripped_for_comments):
            logger.warning("Blocked SQL Server request containing adjacent statements.")
            return False, "Query blocked - multiple statements are not permitted."

    return True, ""
