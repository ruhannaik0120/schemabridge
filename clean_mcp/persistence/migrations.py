"""Deterministic checksum-verified PostgreSQL control-plane migration runner."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Callable

from persistence.errors import WorkflowMigrationError


_MIGRATIONS = Path(__file__).with_name("migrations")
_NAME = re.compile(r"(\d{4})_[a-z0-9_]+\.sql\Z")


class ControlPlaneMigrationRunner:
    def __init__(
        self,
        connection_factory: Callable[[], object],
        migration_directory: Path = _MIGRATIONS,
    ) -> None:
        self.connection_factory = connection_factory
        self.migration_directory = migration_directory

    def discover(self) -> tuple[tuple[int, str, bytes, str], ...]:
        items: list[tuple[int, str, bytes, str]] = []
        for path in self.migration_directory.glob("*.sql"):
            match = _NAME.fullmatch(path.name)
            if not match:
                raise WorkflowMigrationError()
            version = int(match.group(1))
            if version < 1:
                raise WorkflowMigrationError()
            data = path.read_bytes()
            items.append((version, path.name, data, hashlib.sha256(data).hexdigest()))
        items.sort(key=lambda item: item[0])
        if len({item[0] for item in items}) != len(items):
            raise WorkflowMigrationError()
        return tuple(items)

    def run(self) -> tuple[int, ...]:
        connection = None
        try:
            migrations = self.discover()
            connection = self.connection_factory()
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_xact_lock(%s)", (748392615,))
                    cursor.execute(
                        "CREATE TABLE IF NOT EXISTS schemabridge_control_plane_migrations "
                        "(version INTEGER PRIMARY KEY CHECK (version > 0), "
                        "filename TEXT NOT NULL UNIQUE, checksum CHAR(64) NOT NULL, "
                        "applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)"
                    )
                    cursor.execute(
                        "SELECT version, checksum "
                        "FROM schemabridge_control_plane_migrations ORDER BY version"
                    )
                    applied = dict(cursor.fetchall())
                    for version, filename, data, checksum in migrations:
                        if version in applied:
                            if applied[version] != checksum:
                                raise WorkflowMigrationError()
                            continue
                        cursor.execute(data.decode("utf-8"))
                        cursor.execute(
                            "INSERT INTO schemabridge_control_plane_migrations"
                            "(version, filename, checksum) VALUES (%s, %s, %s)",
                            (version, filename, checksum),
                        )
            return tuple(version for version, _, _, _ in migrations)
        except WorkflowMigrationError:
            raise
        except Exception:
            raise WorkflowMigrationError() from None
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
