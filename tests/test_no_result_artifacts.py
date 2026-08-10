"""Verify that controlled database execution creates no result artifacts."""

from schemabridge.models.connection_profile import ConnectionProfile
from schemabridge.services.database_service import DatabaseService


class _Connector:
    """Return one deterministic query result without external infrastructure."""

    def execute_query(self, query, *, database=None, timeout_seconds=None, max_rows=None):
        return {"columns": ["value"], "rows": [{"value": 1}], "rows_affected": 1}


def test_execution_does_not_write_result_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    profile = ConnectionProfile(
        profile_id="demo-local",
        db_type="demo",
        database="schemabridge_demo",
    )

    result = DatabaseService(profile, _Connector()).execute_validation_query(
        sql="SELECT 1"
    )

    assert result.rows == ({"value": 1},)
    assert list(tmp_path.rglob("*.json")) == []
