"""Export saved E2E PoC execution results to HTML or Excel."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_ROOT = REPOSITORY_ROOT / "poc_runs"
SENSITIVE_KEY_PARTS = (
    "password",
    "passwd",
    "pwd",
    "token",
    "secret",
    "credential",
    "authorization",
    "connectionstring",
    "privatekey",
    "apikey",
    "passphrase",
)
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{number}" for number in range(1, 10)),
    *(f"LPT{number}" for number in range(1, 10)),
}
_EXCEL_ILLEGAL_CHARACTERS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
_QA_PASS_STATUSES = {"passed", "pass"}
_QA_FAIL_STATUSES = {"failed", "fail"}


def sanitize_ticket_key(ticket_key: str) -> str:
    """Return the same safe folder name used by init_poc_run.py."""

    original = ticket_key.strip()
    normalized = original.upper()
    sanitized = re.sub(r"[^A-Z0-9._-]+", "_", normalized).strip("._-")
    if not sanitized:
        raise ValueError("The Jira ticket key must contain letters or numbers.")
    changed = sanitized != normalized or len(sanitized) > 100
    if changed:
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:8]
        sanitized = sanitized[:90].rstrip("._-")
        sanitized = f"{sanitized}-{digest}"
    if sanitized.split(".", 1)[0] in _WINDOWS_RESERVED_NAMES:
        sanitized = f"RUN-{sanitized[:95]}"
    return sanitized


def _safe_child(parent: Path, child_name: str) -> Path:
    """Return a child path only when its resolved destination stays under parent."""

    resolved_parent = parent.resolve()
    child = resolved_parent / child_name
    if not child.resolve(strict=False).is_relative_to(resolved_parent):
        raise ValueError(f"Unsafe path outside {resolved_parent}: {child_name}")
    return child


def redact_sensitive(value: Any, key: str = "") -> Any:
    """Redact credential-like fields before placing saved data in a report."""

    normalized_key = re.sub(r"[^a-z0-9]", "", key.lower())
    if any(part in normalized_key for part in SENSITIVE_KEY_PARTS):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(item_key): redact_sensitive(item_value, str(item_key)) for item_key, item_value in value.items()}
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    if isinstance(value, tuple):
        return [redact_sensitive(item) for item in value]
    if isinstance(value, str):
        redacted = re.sub(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+", "[REDACTED]", value)
        redacted = re.sub(
            r"(?i)\b(password|passwd|pwd|token|secret|api[_-]?key|authorization)\s*[:=]\s*([^\s,;]+)",
            r"\1=[REDACTED]",
            redacted,
        )
        redacted = re.sub(r"(?i)([a-z][a-z0-9+.-]*://)[^/@\s:]+:[^/@\s]+@", r"\1[REDACTED]@", redacted)
        redacted = re.sub(
            r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
            "[REDACTED PRIVATE KEY]",
            redacted,
            flags=re.DOTALL,
        )
        return redacted
    return value


def load_execution_result(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Execution result file not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Execution result is not valid JSON: {path} ({exc})") from exc
    if isinstance(payload, list):
        if any(not isinstance(item, dict) for item in payload):
            raise ValueError("Every entry in a top-level execution result list must be a JSON object.")
        payload = {"schema_version": "1.0", "query_results": payload, "errors": []}
    if not isinstance(payload, dict):
        raise ValueError("execution_result.json must contain a JSON object or a list of response objects.")
    return redact_sensitive(payload)


def _nested_value(item: dict[str, Any], key: str) -> Any:
    if key in item:
        return item[key]
    data = item.get("data")
    if isinstance(data, dict) and key in data:
        return data[key]
    result = item.get("result")
    if isinstance(result, dict) and key in result:
        return result[key]
    return None


def query_results(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("query_results", "results", "queries", "executions"):
        candidate = payload.get(key)
        if isinstance(candidate, list):
            if any(not isinstance(item, dict) for item in candidate):
                raise ValueError(f"{key} must contain only JSON objects.")
            return candidate
    if any(key in payload for key in ("success", "rows", "row_count", "rows_affected", "error")):
        return [payload]
    return []


def query_status(item: dict[str, Any]) -> str:
    validation_status = item.get("validation_status", item.get("qa_status"))
    if validation_status not in (None, ""):
        return str(validation_status)
    status = item.get("status")
    if str(status).lower() in _QA_PASS_STATUSES | _QA_FAIL_STATUSES:
        return str(status)
    success = item.get("execution_success", item.get("success"))
    if success is True:
        return "executed_not_evaluated"
    if success is False:
        return "execution_failed"
    return "not_evaluated"


def returned_row_count(item: dict[str, Any]) -> int | str:
    count = _nested_value(item, "row_count")
    if count is not None:
        return count
    rows = _nested_value(item, "rows")
    return len(rows) if isinstance(rows, list) else ""


def affected_row_count(item: dict[str, Any]) -> int | str:
    count = _nested_value(item, "rows_affected")
    return count if count is not None else ""


def result_preview(item: dict[str, Any], limit: int = 10) -> Any:
    rows = _nested_value(item, "rows")
    if isinstance(rows, list):
        return rows[:limit]
    preview = _nested_value(item, "preview")
    return preview if preview is not None else []


def query_identifier(item: dict[str, Any], index: int) -> str:
    for key in ("check_id", "query_id", "id", "name", "check", "request_id", "tool"):
        value = item.get(key)
        if value not in (None, ""):
            return str(value)
    return f"Query {index}"


def query_profile(item: dict[str, Any]) -> str:
    metadata = item.get("metadata")
    if isinstance(metadata, dict) and metadata.get("profile") not in (None, ""):
        return str(metadata["profile"])
    return str(item.get("profile", ""))


def expected_result(item: dict[str, Any]) -> str:
    value = item.get("expected", item.get("expected_result", ""))
    return str(value) if value not in (None, "") else ""


def actual_result(item: dict[str, Any]) -> str:
    value = item.get("actual", item.get("actual_result", ""))
    return str(value) if value not in (None, "") else ""


def collect_errors(payload: dict[str, Any], results: list[dict[str, Any]]) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []

    def add_error(error: Any, query: str = "Run") -> None:
        if not error:
            return
        if isinstance(error, dict):
            errors.append(
                {
                    "query": query,
                    "code": str(error.get("code", "")),
                    "message": str(error.get("message", error.get("error", ""))),
                    "detail": str(error.get("detail", "")),
                }
            )
        else:
            errors.append({"query": query, "code": "", "message": str(error), "detail": ""})

    top_level_errors = payload.get("errors", [])
    if isinstance(top_level_errors, list):
        for error in top_level_errors:
            add_error(error)
    else:
        add_error(top_level_errors)

    if not results or results[0] is not payload:
        add_error(payload.get("error"))
    for index, item in enumerate(results, start=1):
        add_error(item.get("error"), query_identifier(item, index))
    return errors


def build_summary(payload: dict[str, Any], results: list[dict[str, Any]], errors: list[dict[str, str]]) -> dict[str, Any]:
    summary = payload.get("summary")
    safe_summary = dict(summary) if isinstance(summary, dict) else {}
    statuses = [query_status(item).strip().lower() for item in results]
    passed = sum(status in _QA_PASS_STATUSES for status in statuses)
    failed = sum(status in _QA_FAIL_STATUSES for status in statuses)
    execution_failed = sum(status == "execution_failed" for status in statuses)
    not_evaluated = len(results) - passed - failed - execution_failed
    if errors or execution_failed:
        overall_status = "execution_failed"
    elif failed:
        overall_status = "validation_failed"
    elif results and not_evaluated:
        overall_status = "awaiting_evaluation"
    elif results:
        overall_status = "validation_passed"
    else:
        overall_status = "not_started"
    safe_summary.update(
        {
            "status": overall_status,
            "total_queries": len(results),
            "passed": passed,
            "failed": failed,
            "not_evaluated": not_evaluated,
            "execution_failed": execution_failed,
            "error_count": len(errors),
        }
    )
    return safe_summary


def validate_report_payload(ticket_id: str, payload: dict[str, Any], *, require_results: bool) -> None:
    """Reject mismatched or unfinished evidence before creating a final report."""

    payload_ticket = payload.get("ticket_id")
    if payload_ticket not in (None, "") and sanitize_ticket_key(str(payload_ticket)) != ticket_id:
        raise ValueError(
            f"execution_result.json belongs to {payload_ticket!r}, not the requested run {ticket_id!r}."
        )
    results = query_results(payload)
    errors = collect_errors(payload, results)
    if require_results and not results and not errors:
        raise ValueError("No execution results are available. Complete the approved query run before exporting a report.")


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, default=str)


def _excel_cell(value: Any) -> Any:
    """Convert untrusted report values into safe, valid Excel cell content."""

    if value is None:
        return ""
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, (dict, list, tuple)):
        value = _json_text(value)
    text = _EXCEL_ILLEGAL_CHARACTERS.sub("", str(value))
    if len(text) > 32_767:
        text = text[:32_764] + "..."
    if text.lstrip().startswith(("=", "+", "-", "@")):
        text = "'" + text
    return text


def export_html(ticket_id: str, payload: dict[str, Any], output_path: Path) -> None:
    payload = redact_sensitive(payload)
    results = query_results(payload)
    errors = collect_errors(payload, results)
    summary = build_summary(payload, results, errors)

    summary_rows = "".join(
        f"<tr><th>{html.escape(str(key).replace('_', ' ').title())}</th>"
        f"<td>{html.escape(str(value))}</td></tr>"
        for key, value in summary.items()
    )
    result_rows = "".join(
        "<tr>"
        f"<td>{index}</td>"
        f"<td>{html.escape(query_identifier(item, index))}</td>"
        f"<td>{html.escape(query_profile(item))}</td>"
        f"<td>{html.escape(query_status(item))}</td>"
        f"<td>{html.escape(expected_result(item))}</td>"
        f"<td>{html.escape(actual_result(item))}</td>"
        f"<td>{html.escape(str(returned_row_count(item)))}</td>"
        f"<td>{html.escape(str(affected_row_count(item)))}</td>"
        f"<td><pre>{html.escape(_json_text(result_preview(item)))}</pre></td>"
        "</tr>"
        for index, item in enumerate(results, start=1)
    ) or '<tr><td colspan="9">No query results are available.</td></tr>'
    error_rows = "".join(
        "<tr>"
        f"<td>{html.escape(error['query'])}</td>"
        f"<td>{html.escape(error['code'])}</td>"
        f"<td>{html.escape(error['message'])}</td>"
        f"<td>{html.escape(error['detail'])}</td>"
        "</tr>"
        for error in errors
    ) or '<tr><td colspan="4">No errors were recorded.</td></tr>'

    document = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QA Execution Report - {html.escape(ticket_id)}</title>
  <style>
    body {{ color: #17202a; font-family: Arial, sans-serif; margin: 32px auto; max-width: 1200px; padding: 0 20px; }}
    h1 {{ color: #163a5f; }}
    h2 {{ border-bottom: 2px solid #d8e1ea; margin-top: 32px; padding-bottom: 6px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #c8d2dc; padding: 9px; text-align: left; vertical-align: top; }}
    th {{ background: #eaf0f5; }}
    pre {{ margin: 0; max-height: 260px; overflow: auto; white-space: pre-wrap; }}
    .run-id {{ color: #465866; }}
  </style>
</head>
<body>
  <h1>QA Execution Report</h1>
  <p class="run-id"><strong>Ticket / run:</strong> {html.escape(ticket_id)}</p>
  <h2>Execution Summary</h2>
  <table><tbody>{summary_rows}</tbody></table>
  <h2>Query Results</h2>
  <table><thead><tr><th>#</th><th>Check / Query</th><th>Profile</th><th>Validation Status</th><th>Expected</th><th>Actual</th><th>Returned Rows</th><th>Rows Affected</th><th>Result Preview</th></tr></thead><tbody>{result_rows}</tbody></table>
  <h2>Errors</h2>
  <table><thead><tr><th>Query</th><th>Code</th><th>Message</th><th>Detail</th></tr></thead><tbody>{error_rows}</tbody></table>
</body>
</html>
"""
    temporary_path = output_path.with_name(f".{output_path.stem}.tmp{output_path.suffix}")
    try:
        temporary_path.write_text(document, encoding="utf-8")
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def load_openpyxl() -> tuple[Any, Any]:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except ImportError as exc:
        raise RuntimeError(
            "Excel export requires openpyxl. Install it with: "
            ".\\.venv\\Scripts\\python.exe -m pip install -r requirements-e2e.txt"
        ) from exc
    return Workbook, Font


def export_excel(
    ticket_id: str,
    payload: dict[str, Any],
    output_path: Path,
    workbook_class: Any,
    font_class: Any,
) -> None:
    payload = redact_sensitive(payload)
    results = query_results(payload)
    errors = collect_errors(payload, results)
    summary = build_summary(payload, results, errors)

    workbook = workbook_class()
    summary_sheet = workbook.active
    summary_sheet.title = "Summary"
    summary_sheet.append(["Ticket / Run", _excel_cell(ticket_id)])
    for key, value in summary.items():
        summary_sheet.append([_excel_cell(str(key).replace("_", " ").title()), _excel_cell(value)])
    summary_sheet["A1"].font = font_class(bold=True)
    summary_sheet.column_dimensions["A"].width = 24
    summary_sheet.column_dimensions["B"].width = 40

    results_sheet = workbook.create_sheet("Query Results")
    results_sheet.append(
        [
            "#",
            "Check / Query",
            "Profile",
            "Validation Status",
            "Expected",
            "Actual",
            "Returned Rows",
            "Rows Affected",
            "Request ID",
            "Environment",
            "Execution Time (ms)",
            "Result Preview",
        ]
    )
    for cell in results_sheet[1]:
        cell.font = font_class(bold=True)
    for index, item in enumerate(results, start=1):
        results_sheet.append(
            [
                index,
                _excel_cell(query_identifier(item, index)),
                _excel_cell(query_profile(item)),
                _excel_cell(query_status(item)),
                _excel_cell(expected_result(item)),
                _excel_cell(actual_result(item)),
                _excel_cell(returned_row_count(item)),
                _excel_cell(affected_row_count(item)),
                _excel_cell(item.get("request_id", "")),
                _excel_cell(item.get("environment", "")),
                _excel_cell(item.get("execution_time_ms", "")),
                _excel_cell(_json_text(result_preview(item))),
            ]
        )
    for column, width in {
        "A": 6,
        "B": 28,
        "C": 20,
        "D": 24,
        "E": 36,
        "F": 36,
        "G": 14,
        "H": 14,
        "I": 38,
        "J": 18,
        "K": 20,
        "L": 70,
    }.items():
        results_sheet.column_dimensions[column].width = width

    errors_sheet = workbook.create_sheet("Errors")
    errors_sheet.append(["#", "Query", "Code", "Message", "Detail"])
    for cell in errors_sheet[1]:
        cell.font = font_class(bold=True)
    for index, error in enumerate(errors, start=1):
        errors_sheet.append(
            [
                index,
                _excel_cell(error["query"]),
                _excel_cell(error["code"]),
                _excel_cell(error["message"]),
                _excel_cell(error["detail"]),
            ]
        )
    for column, width in {"A": 6, "B": 28, "C": 22, "D": 45, "E": 70}.items():
        errors_sheet.column_dimensions[column].width = width

    temporary_path = output_path.with_name(f".{output_path.stem}.tmp{output_path.suffix}")
    try:
        workbook.save(temporary_path)
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export saved E2E PoC execution results.")
    parser.add_argument("ticket_key", help="Jira ticket key, for example ABC-123")
    parser.add_argument(
        "--format",
        required=True,
        choices=("json_only", "html", "excel", "both"),
        dest="export_format",
        help="Final report format",
    )
    args = parser.parse_args()

    try:
        ticket_id = sanitize_ticket_key(args.ticket_key)
        run_folder = _safe_child(ARTIFACT_ROOT, ticket_id)
        result_path = _safe_child(run_folder, "execution_result.json")
        payload = load_execution_result(result_path)
        validate_report_payload(ticket_id, payload, require_results=args.export_format != "json_only")
    except (FileNotFoundError, OSError, ValueError) as exc:
        parser.exit(1, f"Unable to export results: {exc}\n")

    if args.export_format == "json_only":
        print(result_path)
        return 0

    workbook_class = font_class = None
    if args.export_format in {"excel", "both"}:
        try:
            workbook_class, font_class = load_openpyxl()
        except RuntimeError as exc:
            parser.exit(2, f"Unable to export results: {exc}\n")

    output_folder = _safe_child(run_folder, "output")
    output_folder.mkdir(parents=True, exist_ok=True)
    try:
        if args.export_format in {"html", "both"}:
            html_path = output_folder / "report.html"
            export_html(ticket_id, payload, html_path)
            print(html_path)
        if args.export_format in {"excel", "both"}:
            excel_path = output_folder / "report.xlsx"
            export_excel(ticket_id, payload, excel_path, workbook_class, font_class)
            print(excel_path)
    except Exception as exc:
        parser.exit(1, f"Unable to write report: {redact_sensitive(str(exc))}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
