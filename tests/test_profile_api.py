"""HTTP coverage for safe configured connection-profile metadata."""

from fastapi.testclient import TestClient

from schemabridge.api.app import create_app


def test_list_profiles_returns_selection_metadata_without_credentials(monkeypatch) -> None:
    monkeypatch.setenv(
        "DB_PROFILES_JSON",
        '{"source-pg":{"db_type":"postgresql","host":"db.internal","database":"sales","username":"reader","password":"secret","write_enabled":false},"target-sf":{"db_type":"snowflake","host":"account","database":"warehouse","username":"writer","password":"other-secret","write_enabled":true}}',
    )
    with TestClient(create_app()) as client:
        response = client.get("/api/v1/profiles")

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {"name": "source-pg", "db_type": "postgresql", "database": "sales", "database_present": True, "write_enabled": False},
            {"name": "target-sf", "db_type": "snowflake", "database": "warehouse", "database_present": True, "write_enabled": True},
        ]
    }
    serialized = response.text
    assert "secret" not in serialized
    assert "db.internal" not in serialized
