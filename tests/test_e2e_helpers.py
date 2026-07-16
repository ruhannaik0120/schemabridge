"""Regression tests for the external E2E run and reporting helpers."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

from scripts import export_results, init_poc_run


def test_ticket_folder_names_are_stable_and_collision_resistant():
    assert init_poc_run.sanitize_ticket_key("abc-123") == "ABC-123"
    first = init_poc_run.sanitize_ticket_key("ABC/123")
    second = init_poc_run.sanitize_ticket_key("ABC?123")

    assert first != second
    assert first == export_results.sanitize_ticket_key("ABC/123")
    assert "/" not in first
    assert init_poc_run.sanitize_ticket_key("CON.txt").startswith("RUN-")


def test_initialize_run_is_idempotent(tmp_path: Path):
    run_folder, created = init_poc_run.initialize_run("ABC-123", tmp_path)
    marker = run_folder / "qa_plan.md"
    marker.write_text("keep this", encoding="utf-8")

    _, created_again = init_poc_run.initialize_run("ABC-123", tmp_path)

    assert len(created) == 6
    assert created_again == []
    assert marker.read_text(encoding="utf-8") == "keep this"
    payload = json.loads((run_folder / "execution_result.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "1.0"


def test_raw_execution_success_is_not_a_qa_pass():
    result = {"success": True, "request_id": "req-1", "rows": [{"defect_count": 2}]}

    assert export_results.query_status(result) == "executed_not_evaluated"
    summary = export_results.build_summary({}, [result], [])
    assert summary["status"] == "awaiting_evaluation"
    assert summary["passed"] == 0
    assert summary["not_evaluated"] == 1


def test_summary_recomputes_stale_placeholder_counts():
    payload = {
        "summary": {"status": "not_started", "total_queries": 0, "passed": 0},
        "query_results": [
            {"validation_status": "passed"},
            {"validation_status": "failed"},
        ],
    }

    summary = export_results.build_summary(payload, payload["query_results"], [])

    assert summary["status"] == "validation_failed"
    assert summary["total_queries"] == 2
    assert summary["passed"] == 1
    assert summary["failed"] == 1


def test_empty_or_wrong_ticket_report_is_rejected():
    empty = {"ticket_id": "ABC-123", "query_results": [], "errors": []}
    with pytest.raises(ValueError, match="No execution results"):
        export_results.validate_report_payload("ABC-123", empty, require_results=True)

    wrong_ticket = {"ticket_id": "XYZ-999", "query_results": [{"success": True}]}
    with pytest.raises(ValueError, match="belongs to"):
        export_results.validate_report_payload("ABC-123", wrong_ticket, require_results=True)


def test_top_level_response_list_is_normalized(tmp_path: Path):
    path = tmp_path / "execution_result.json"
    path.write_text(json.dumps([{"success": True}, {"success": False}]), encoding="utf-8")

    payload = export_results.load_execution_result(path)

    assert len(payload["query_results"]) == 2


def test_sensitive_values_are_redacted_recursively():
    payload = {
        "auth": {"clientSecret": "hidden"},
        "connectionString": "Server=x;Password=hidden",
        "error": "Authorization: Bearer abc.def",
    }

    redacted = export_results.redact_sensitive(payload)

    assert redacted["auth"]["clientSecret"] == "[REDACTED]"
    assert redacted["connectionString"] == "[REDACTED]"
    assert "abc.def" not in redacted["error"]


def test_excel_cells_block_formulas_and_invalid_control_characters():
    assert export_results._excel_cell("=HYPERLINK(\"https://example.test\")").startswith("'=")
    assert export_results._excel_cell("unsafe\x00value") == "unsafevalue"
    assert len(export_results._excel_cell("x" * 40_000)) <= 32_767


def test_html_escapes_values_and_reports_profile(tmp_path: Path):
    output = tmp_path / "report.html"
    payload = {
        "query_results": [
            {
                "check_id": "CHECK-1<script>",
                "validation_status": "passed",
                "metadata": {"profile": "qa-profile"},
                "row_count": 1,
                "rows_affected": 4,
                "rows": [{"value": "<unsafe>"}],
            }
        ]
    }

    export_results.export_html("ABC-123", payload, output)
    report = output.read_text(encoding="utf-8")

    assert "CHECK-1&lt;script&gt;" in report
    assert "&lt;unsafe&gt;" in report
    assert "qa-profile" in report
    assert "Rows Affected" in report


def test_excel_export_creates_safe_expected_sheets(tmp_path: Path):
    openpyxl = pytest.importorskip("openpyxl")
    workbook_class, font_class = export_results.load_openpyxl()
    output = tmp_path / "report.xlsx"
    payload = {
        "query_results": [
            {
                "check_id": "=UNSAFE()",
                "validation_status": "failed",
                "expected": "zero defects",
                "actual": "2 defects\x00",
                "rows": [{"defect_count": 2}],
            }
        ],
        "errors": [{"message": "+unsafe formula"}],
    }

    export_results.export_excel("ABC-123", payload, output, workbook_class, font_class)

    workbook = openpyxl.load_workbook(output, read_only=True, data_only=False)
    try:
        assert workbook.sheetnames == ["Summary", "Query Results", "Errors"]
        result_row = next(workbook["Query Results"].iter_rows(min_row=2, values_only=True))
        error_row = next(workbook["Errors"].iter_rows(min_row=2, values_only=True))
        assert result_row[1].startswith("'=")
        assert "\x00" not in result_row[5]
        assert error_row[3].startswith("'+")
    finally:
        workbook.close()
    assert not list(tmp_path.glob("*.tmp.xlsx"))


def test_cli_initialization_and_both_exports_work_end_to_end(tmp_path: Path, monkeypatch, capsys):
    pytest.importorskip("openpyxl")
    artifact_root = tmp_path / "poc_runs"
    monkeypatch.setattr(init_poc_run, "ARTIFACT_ROOT", artifact_root)
    monkeypatch.setattr(sys, "argv", ["init_poc_run.py", "ABC-123"])

    assert init_poc_run.main() == 0

    result_path = artifact_root / "ABC-123" / "execution_result.json"
    result_path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "ticket_id": "ABC-123",
                "query_results": [
                    {
                        "check_id": "CHECK-1",
                        "execution_success": True,
                        "validation_status": "passed",
                        "expected": "one row",
                        "actual": "one row",
                        "rows": [{"value": 1}],
                    }
                ],
                "errors": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(export_results, "ARTIFACT_ROOT", artifact_root)
    monkeypatch.setattr(sys, "argv", ["export_results.py", "ABC-123", "--format", "both"])

    assert export_results.main() == 0
    assert (artifact_root / "ABC-123" / "output" / "report.html").is_file()
    assert (artifact_root / "ABC-123" / "output" / "report.xlsx").is_file()
    assert "report.xlsx" in capsys.readouterr().out
