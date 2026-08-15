"""Static and executable checks for the local placement-ready package."""

from pathlib import Path
import subprocess
import sys


WORKSPACE = Path(__file__).resolve().parents[1]


def test_docker_package_is_non_root_health_checked_and_migration_gated() -> None:
    dockerfile = (WORKSPACE / "Dockerfile").read_text(encoding="utf-8")
    compose = (WORKSPACE / "compose.yaml").read_text(encoding="utf-8")
    ignore = (WORKSPACE / ".dockerignore").read_text(encoding="utf-8")

    assert "USER schemabridge" in dockerfile
    assert "HEALTHCHECK" in dockerfile
    assert "requirements-api.lock" in dockerfile
    assert "schemabridge.api.app:create_app" in dockerfile
    for service in ("control-plane:", "migrate:", "api:"):
        assert service in compose
    assert "service_healthy" in compose
    assert "service_completed_successfully" in compose
    assert "schemabridge-control-plane:" in compose
    assert "scripts.migrate_control_plane" in compose
    assert "/health/ready" in compose
    assert "tests" in ignore and "**/.env" in ignore
    combined = (dockerfile + compose).casefold()
    assert all(secret not in combined for secret in ("private_key", "access_token", "snowflake_password"))


def test_environment_examples_separate_control_source_target_and_have_no_demo_passwords() -> None:
    root = (WORKSPACE / ".env.example").read_text(encoding="utf-8")
    combined = root
    for name in (
        "SCHEMABRIDGE_CONTROL_PLANE_DSN",
        "postgres-source",
        "snowflake-target",
        "write_enabled",
        "DB_TIMEOUT_SECONDS",
        "SCHEMABRIDGE_RUN_CONTROL_PLANE_INTEGRATION",
    ):
        assert name in combined
    assert "demo_password" not in combined
    assert "<SNOWFLAKE_PASSWORD>" in combined and "<POSTGRES_PASSWORD>" in combined


def test_migration_cli_discovers_all_versions_without_a_database() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "scripts.migrate_control_plane", "--check"],
        cwd=WORKSPACE,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().endswith("1, 2, 3, 4")
    assert "postgresql://" not in completed.stdout + completed.stderr


def test_recruiter_and_demo_documentation_match_implemented_scope() -> None:
    readme = (WORKSPACE / "README.md").read_text(encoding="utf-8")
    interview = (WORKSPACE / "docs/INTERVIEW_DEMO.md").read_text(encoding="utf-8")
    workflow = (WORKSPACE / "docs/LOCAL_WORKFLOW_DEMO.md").read_text(encoding="utf-8")
    for term in (
        "SchemaBridge",
        "```mermaid",
        "Idempotency-Key",
        "VALIDATION_REVIEW_REQUIRED",
        "Docker Compose",
        "Swagger",
        "Batch transport currently supports PostgreSQL sources",
    ):
        assert term in readme
    assert "Five-to-seven minute sequence" in interview
    assert "five-row PostgreSQL-to-Snowflake migration" in interview
    assert "load-staging" in workflow
    assert "never claims that data moved" in workflow
