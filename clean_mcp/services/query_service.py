"""Business and orchestration logic for MCP tools."""

from __future__ import annotations
from datetime import datetime, timezone
from difflib import SequenceMatcher
import os
from time import perf_counter
from uuid import uuid4

from config import Config, ConfigError
from connectors.base import DatabaseConnector
from connectors.factory import ConnectorFactory
from logger import logger, reset_environment, reset_request_id, set_environment, set_request_id
from models.connection_profile import ConnectionProfile
from models.errors import ErrorCode, StructuredError
from models.responses import ToolResponse
from services.runtime_state import runtime_lock, runtime_metadata
from validation.sql_guard import validate_query


class QueryService:
    """Service layer that orchestrates tool requests and formats responses."""

    def __init__(self, sql_connector=None, *, profile: ConnectionProfile | None = None):
        """Create the service with a selected connector or an injected test double."""

        if profile is not None and not isinstance(profile, ConnectionProfile):
            raise TypeError("profile must be a ConnectionProfile.")
        if (
            profile is not None
            and isinstance(sql_connector, DatabaseConnector)
            and not sql_connector.matches_profile(profile)
        ):
            raise ConfigError("Connector is not bound to the supplied connection profile")
        self._connection_profile = profile
        if sql_connector is None:
            if profile is None:
                Config.load()
                self.connector = ConnectorFactory.create()
            else:
                self.connector = ConnectorFactory.create_for_profile(profile)
        else:
            self.connector = sql_connector

    def _db_type(self) -> str:
        """Return the profile dialect or the current legacy Config dialect."""

        if self._connection_profile is not None:
            return self._connection_profile.db_type
        return Config.DB_TYPE

    def _database(self) -> str:
        """Return the profile database or the current legacy Config database."""

        if self._connection_profile is not None:
            return self._connection_profile.database
        return Config.DATABASE

    def _profile_id(self) -> str:
        """Return the bound profile ID or the current legacy active profile."""

        if self._connection_profile is not None:
            return self._connection_profile.profile_id
        return os.getenv("DB_ACTIVE_PROFILE", "default").strip() or "default"

    def _timeout_ceiling(self) -> int:
        """Return the request timeout ceiling for this service context."""

        if self._connection_profile is not None:
            return self._connection_profile.timeout_seconds
        return Config.GLOBAL_TIMEOUT_SECONDS

    def _row_limit_ceiling(self) -> int:
        """Return the result-row ceiling for this service context."""

        if self._connection_profile is not None:
            return self._connection_profile.max_rows
        return Config.GLOBAL_MAX_ROWS

    def _environment_name(self) -> str:
        """Return the safe display name for this service's database type."""

        return (self._db_type() or "database").strip().upper() or "DATABASE"

    def _safe_connection_details(self) -> dict[str, object]:
        """Return credential-free connection context for health responses."""

        if self._connection_profile is not None:
            return self._connection_profile.to_safe_dict()
        return Config.connection_config().safe_dict()

    def _safe_diagnostics(self) -> dict[str, object]:
        """Return credential-free diagnostics for this service context."""

        if self._connection_profile is not None:
            return self._connection_profile.to_safe_dict()
        return Config.diagnostics()

    def _safe_error_detail(self, error: Exception) -> str:
        """Prevent raw profile-bound connector errors from leaving the service."""

        if self._connection_profile is not None:
            return "Database operation failed for the selected connection profile."
        return Config.redact_text(error)

    def _request_id(self) -> str:
        """Generate the short correlation ID shared by responses and logs."""

        return uuid4().hex[:12]

    def _effective_timeout(self, timeout_seconds: int | None) -> int:
        """Return a positive timeout that cannot exceed the configured ceiling."""

        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ConfigError("timeout_seconds must be greater than zero.")
        ceiling = self._timeout_ceiling()
        return min(timeout_seconds or ceiling, ceiling)

    def _execution_database(self, database: str | None) -> str:
        """Keep query execution bound to the database in the approved profile."""

        requested = (database or "").strip()
        configured = (self._database() or "").strip()
        if requested and configured and requested.casefold() != configured.casefold():
            raise ConfigError(
                "Query execution database must match the active profile. "
                "Configure and approve a profile switch before targeting another database."
            )
        return requested or configured

    def _begin_request(self, tool: str) -> tuple[str, object, object, float, str]:
        """Initialize correlation context, timing, and the request-received log."""

        # Context variables attach the same request ID and active backend to
        # every log emitted while this tool call is being processed.
        request_id = self._request_id()
        request_token = set_request_id(request_id)
        environment_name = self._environment_name()
        environment_token = set_environment(environment_name)
        start_time = perf_counter()
        logger.info(
            f"Starting {tool}.",
            extra={
                "tool": tool,
                "environment": environment_name,
                "db_type": self._db_type(),
                "success": None,
                "execution_time_ms": 0,
                "event": "request_received",
            },
        )
        return request_id, request_token, environment_token, start_time, environment_name

    def _response(
        self,
        *,
        tool: str,
        environment: str,
        success: bool,
        request_id: str,
        start_time: float,
        data: dict | None = None,
        metadata: dict | None = None,
        error: StructuredError | None = None,
    ) -> ToolResponse:
        """Build the common success/error response envelope with elapsed time."""

        # Timing and envelope construction are centralized so every MCP tool
        # returns the same contract regardless of the selected connector.
        execution_time_ms = int((perf_counter() - start_time) * 1000)
        response_metadata = {
            "profile": self._profile_id(),
            "db_type": self._db_type(),
            **runtime_metadata(),
            **(metadata or {}),
        }
        return ToolResponse(
            success=success,
            tool=tool,
            request_id=request_id,
            environment=environment,
            execution_time_ms=execution_time_ms,
            data=data or {},
            metadata=response_metadata,
            error=error,
        )

    def _error(
        self,
        *,
        tool: str,
        environment: str,
        code: str,
        message: str,
        request_id: str,
        start_time: float,
        detail: str | None = None,
        hint: str | None = None,
        retryable: bool = False,
        context: dict | None = None,
        data: dict | None = None,
        metadata: dict | None = None,
    ) -> ToolResponse:
        """Build a failed response using the framework's structured error model."""

        return self._response(
            tool=tool,
            environment=environment,
            success=False,
            request_id=request_id,
            start_time=start_time,
            data=data,
            metadata=metadata,
            error=StructuredError(
                code=code,
                message=message,
                request_id=request_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                detail=detail,
                hint=hint,
                retryable=retryable,
                context=context or {},
            ),
        )

    def _end_request(self, tool: str, environment: str, request_id: str, response: ToolResponse) -> None:
        """Write the final correlated success or failure log entry."""

        success = response.success
        log_message = f"{tool} {'succeeded' if success else 'failed'}."
        log_method = logger.info if success else logger.error
        log_method(
            log_message,
            extra={
                "tool": tool,
                "environment": environment,
                "db_type": self._db_type(),
                "success": success,
                "execution_time_ms": response.execution_time_ms,
                "event": "request_completed",
                "error_code": getattr(response.error, "code", None) if response.error else None,
            },
        )

    def _finalize_request(
        self,
        response: ToolResponse,
        tool: str,
        environment: str,
        request_id: str,
        database: str | None = None,
        schema: str | None = None,
        query: str | None = None,
    ) -> ToolResponse:
        """Log an outcome before returning it to the MCP wrapper."""

        self._end_request(tool, environment, request_id, response)
        return response

    def _handle_connector_error(
        self,
        *,
        tool: str,
        requested_environment: str,
        request_id: str,
        start_time: float,
        error: Exception,
        data: dict | None = None,
        retryable: bool = True,
        code: str = ErrorCode.DATABASE_ERROR,
        message: str,
        hint: str | None = None,
    ) -> ToolResponse:
        """Translate connector/configuration exceptions into structured errors."""

        # Configuration mistakes are non-retryable; network/database errors may
        # be retried by an upstream orchestrator after user intervention.
        if isinstance(error, ConfigError):
            code = ErrorCode.CONFIG_INVALID
            retryable = False
        return self._error(
            tool=tool,
            environment=requested_environment,
            code=code,
            message=message,
            request_id=request_id,
            start_time=start_time,
            detail=self._safe_error_detail(error),
            hint=hint,
            retryable=retryable,
            data=data,
        )

    def test_connection(
        self,
        environment: str | None = None,
        database: str | None = None,
        timeout_seconds: int | None = None,
    ) -> ToolResponse:
        """Test the selected connector and return a normalized server snapshot."""

        request_id, request_token, environment_token, start_time, requested_environment = self._begin_request("test_connection")
        try:
            snapshot = self.connector.test_connection(
                database=database or self._database(),
                timeout_seconds=self._effective_timeout(timeout_seconds),
            )
            response = self._response(
                tool="test_connection",
                environment=self._environment_name(),
                success=True,
                request_id=request_id,
                start_time=start_time,
                data={
                    "current_environment": self._environment_name(),
                    "database": database or self._database(),
                    "connection_status": snapshot.get("connection_status", "connected"),
                    "server_information": snapshot.get("server_information", snapshot),
                },
                metadata={
                    "db_type": self._db_type(),
                    "connector_type": snapshot.get("connector_type", self.connector.__class__.__name__),
                },
            )
            return self._finalize_request(
                response,
                tool="test_connection",
                environment=self._environment_name(),
                request_id=request_id,
                database=database or self._database(),
                schema="",
                query="",
            )
        except Exception as exc:
            response = self._handle_connector_error(
                tool="test_connection",
                requested_environment=requested_environment,
                request_id=request_id,
                start_time=start_time,
                error=exc,
                data={"current_environment": requested_environment, "connection_status": "failed"},
                message="Unable to establish a database connection.",
                hint="Check DB_TYPE and the generic connection settings for the selected connector.",
            )
            return self._finalize_request(
                response,
                tool="test_connection",
                environment=requested_environment,
                request_id=request_id,
                database=database or self._database(),
                schema="",
                query="",
            )
        finally:
            reset_request_id(request_token)
            reset_environment(environment_token)

    def health(
        self,
        environment: str | None = None,
        timeout_seconds: int | None = None,
    ) -> ToolResponse:
        """Report liveness plus redacted effective environment configuration."""

        request_id, request_token, environment_token, start_time, requested_environment = self._begin_request("health")
        try:
            snapshot = self.connector.health_check(
                database=self._database(),
                timeout_seconds=self._effective_timeout(timeout_seconds),
            )
            response = self._response(
                tool="health",
                environment=self._environment_name(),
                success=True,
                request_id=request_id,
                start_time=start_time,
                data={
                    "current_environment": self._environment_name(),
                    "connection_status": snapshot.get("connection_status", "connected"),
                    "server_information": snapshot.get("server_information", snapshot),
                    "environment_details": self._safe_connection_details(),
                },
                metadata={
                    "db_type": self._db_type(),
                    "connector_type": snapshot.get("connector_type", self.connector.__class__.__name__),
                },
            )
            return self._finalize_request(
                response,
                tool="health",
                environment=self._environment_name(),
                request_id=request_id,
                database=self._database(),
                schema="",
                query="",
            )
        except Exception as exc:
            response = self._handle_connector_error(
                tool="health",
                requested_environment=requested_environment,
                request_id=request_id,
                start_time=start_time,
                error=exc,
                data={
                    "current_environment": requested_environment,
                    "connection_status": "failed",
                    "server_information": {},
                },
                message="Health check failed.",
                hint="Check DB_TYPE and the generic connection settings for the selected connector.",
            )
            return self._finalize_request(
                response,
                tool="health",
                environment=requested_environment,
                request_id=request_id,
                database=self._database(),
                schema="",
                query="",
            )
        finally:
            reset_request_id(request_token)
            reset_environment(environment_token)

    def list_databases(
        self,
        environment: str | None = None,
        timeout_seconds: int | None = None,
    ) -> ToolResponse:
        """List databases through the selected connector and common response schema."""

        request_id, request_token, environment_token, start_time, requested_environment = self._begin_request("list_databases")
        try:
            payload = self.connector.list_databases(timeout_seconds=self._effective_timeout(timeout_seconds))
            response = self._response(
                tool="list_databases",
                environment=self._environment_name(),
                success=True,
                request_id=request_id,
                start_time=start_time,
                data={
                    "current_environment": self._environment_name(),
                    "count": payload.get("count", len(payload.get("databases", []))),
                    "databases": payload.get("databases", []),
                },
                metadata={"db_type": self._db_type()},
            )
            return self._finalize_request(
                response,
                tool="list_databases",
                environment=self._environment_name(),
                request_id=request_id,
                database=self._database(),
                schema="",
                query="",
            )
        except Exception as exc:
            response = self._handle_connector_error(
                tool="list_databases",
                requested_environment=requested_environment,
                request_id=request_id,
                start_time=start_time,
                error=exc,
                message="Failed to list databases.",
            )
            return self._finalize_request(
                response,
                tool="list_databases",
                environment=requested_environment,
                request_id=request_id,
                database=self._database(),
                schema="",
                query="",
            )
        finally:
            reset_request_id(request_token)
            reset_environment(environment_token)

    def list_tables(
        self,
        database: str | None = None,
        schema: str | None = None,
        environment: str | None = None,
        timeout_seconds: int | None = None,
    ) -> ToolResponse:
        """List tables/views for a database and optional schema."""

        request_id, request_token, environment_token, start_time, requested_environment = self._begin_request("list_tables")
        try:
            target_database = database or self._database()
            payload = self.connector.list_tables(
                database=target_database,
                schema=schema,
                timeout_seconds=self._effective_timeout(timeout_seconds),
            )
            response = self._response(
                tool="list_tables",
                environment=self._environment_name(),
                success=True,
                request_id=request_id,
                start_time=start_time,
                data={
                    "current_environment": self._environment_name(),
                    "database": target_database,
                    "schema": schema or "",
                    "count": payload.get("count", len(payload.get("tables", []))),
                    "tables": payload.get("tables", []),
                },
                metadata={"db_type": self._db_type()},
            )
            return self._finalize_request(
                response,
                tool="list_tables",
                environment=self._environment_name(),
                request_id=request_id,
                database=target_database,
                schema=schema or "",
                query="",
            )
        except Exception as exc:
            response = self._handle_connector_error(
                tool="list_tables",
                requested_environment=requested_environment,
                request_id=request_id,
                start_time=start_time,
                error=exc,
                message="Failed to list tables.",
            )
            return self._finalize_request(
                response,
                tool="list_tables",
                environment=requested_environment,
                request_id=request_id,
                database=database or self._database(),
                schema=schema or "",
                query="",
            )
        finally:
            reset_request_id(request_token)
            reset_environment(environment_token)

    def describe_table(
        self,
        database: str | None = None,
        table: str | None = None,
        schema: str | None = None,
        environment: str | None = None,
        timeout_seconds: int | None = None,
    ) -> ToolResponse:
        """Return normalized column metadata or a structured not-found result."""

        request_id, request_token, environment_token, start_time, requested_environment = self._begin_request("describe_table")
        try:
            payload = self.connector.describe_table(
                database=database or self._database(),
                table=table,
                schema=schema,
                timeout_seconds=self._effective_timeout(timeout_seconds),
            )

            if not payload.get("columns"):
                response = self._error(
                    tool="describe_table",
                    environment=self._environment_name(),
                    code=ErrorCode.VALIDATION_FAILED,
                    message=f"Table {table!r} was not found in {payload.get('database', database or self._database())!r}.",
                    request_id=request_id,
                    start_time=start_time,
                    retryable=False,
                    data={
                        "current_environment": self._environment_name(),
                        "database": payload.get("database", database or self._database()),
                        "schema": payload.get("schema", schema or ""),
                        "table": table or "",
                        "column_count": 0,
                        "columns": [],
                    },
                )
                return self._finalize_request(
                    response,
                    tool="describe_table",
                    environment=self._environment_name(),
                    request_id=request_id,
                    database=payload.get("database", database or self._database()),
                    schema=payload.get("schema", schema or ""),
                    query="",
                )

            response = self._response(
                tool="describe_table",
                environment=self._environment_name(),
                success=True,
                request_id=request_id,
                start_time=start_time,
                data={
                    "current_environment": self._environment_name(),
                    "database": payload.get("database", database or self._database()),
                    "schema": payload.get("schema", schema or ""),
                    "table": payload.get("table", table or ""),
                    "column_count": payload.get("column_count", len(payload.get("columns", []))),
                    "columns": payload.get("columns", []),
                },
                metadata={"db_type": self._db_type()},
            )
            return self._finalize_request(
                response,
                tool="describe_table",
                environment=self._environment_name(),
                request_id=request_id,
                database=payload.get("database", database or self._database()),
                schema=payload.get("schema", schema or ""),
                query="",
            )
        except Exception as exc:
            response = self._handle_connector_error(
                tool="describe_table",
                requested_environment=requested_environment,
                request_id=request_id,
                start_time=start_time,
                error=exc,
                message="Failed to describe table.",
            )
            return self._finalize_request(
                response,
                tool="describe_table",
                environment=requested_environment,
                request_id=request_id,
                database=database or self._database(),
                schema=schema or "",
                query="",
            )
        finally:
            reset_request_id(request_token)
            reset_environment(environment_token)

    @staticmethod
    def _metadata_value(column: dict, *names: str) -> object:
        wanted = {name.casefold() for name in names}
        for key, value in column.items():
            if str(key).casefold() in wanted:
                return value
        return ""

    def suggest_columns(
        self,
        *,
        table: str,
        missing_column: str,
        database: str | None = None,
        schema: str | None = None,
        environment: str | None = None,
        timeout_seconds: int | None = None,
        limit: int = 5,
    ) -> ToolResponse:
        """Rank similar real columns without modifying or executing SQL."""

        request_id, request_token, environment_token, start_time, requested_environment = self._begin_request(
            "suggest_columns"
        )
        try:
            normalized_table = table.strip()
            normalized_missing = missing_column.strip()
            if not normalized_table or not normalized_missing:
                raise ConfigError("table and missing_column are required.")
            if limit <= 0 or limit > 20:
                raise ConfigError("limit must be between 1 and 20.")

            target_database = self._execution_database(database)
            payload = self.connector.describe_table(
                database=target_database,
                table=normalized_table,
                schema=schema,
                timeout_seconds=self._effective_timeout(timeout_seconds),
            )
            columns = payload.get("columns", [])
            if not columns:
                response = self._error(
                    tool="suggest_columns",
                    environment=self._environment_name(),
                    code=ErrorCode.VALIDATION_FAILED,
                    message=f"Table {normalized_table!r} was not found or has no visible columns.",
                    request_id=request_id,
                    start_time=start_time,
                    retryable=False,
                    data={"database": target_database, "schema": schema or "", "table": normalized_table},
                )
                return self._finalize_request(
                    response,
                    tool="suggest_columns",
                    environment=self._environment_name(),
                    request_id=request_id,
                    database=target_database,
                    schema=schema or "",
                )

            target = normalized_missing.casefold()
            ranked: list[dict[str, object]] = []
            for column in columns:
                if not isinstance(column, dict):
                    continue
                name = str(self._metadata_value(column, "column_name", "name")).strip()
                if not name:
                    continue
                candidate = name.casefold()
                score = SequenceMatcher(None, target, candidate).ratio()
                reason = "similar_name"
                if target in candidate or candidate in target:
                    score = max(score, 0.8)
                    reason = "contains_missing_name"
                ranked.append(
                    {
                        "column": name,
                        "data_type": str(self._metadata_value(column, "data_type", "type")),
                        "similarity": round(score, 3),
                        "reason": reason,
                    }
                )
            ranked.sort(key=lambda item: (-float(item["similarity"]), str(item["column"]).casefold()))
            suggestions = [item for item in ranked if float(item["similarity"]) >= 0.25][:limit]
            response = self._response(
                tool="suggest_columns",
                environment=self._environment_name(),
                success=True,
                request_id=request_id,
                start_time=start_time,
                data={
                    "database": target_database,
                    "schema": payload.get("schema", schema or ""),
                    "table": normalized_table,
                    "missing_column": normalized_missing,
                    "suggestions": suggestions,
                    "sql_modified": False,
                    "sql_executed": False,
                    "approval_required_before_revised_sql": True,
                },
                metadata={"db_type": self._db_type()},
            )
            return self._finalize_request(
                response,
                tool="suggest_columns",
                environment=self._environment_name(),
                request_id=request_id,
                database=target_database,
                schema=payload.get("schema", schema or ""),
            )
        except Exception as exc:
            response = self._handle_connector_error(
                tool="suggest_columns",
                requested_environment=requested_environment,
                request_id=request_id,
                start_time=start_time,
                error=exc,
                message="Failed to suggest similar columns.",
                retryable=False,
            )
            return self._finalize_request(
                response,
                tool="suggest_columns",
                environment=requested_environment,
                request_id=request_id,
                database=database or self._database(),
                schema=schema or "",
            )
        finally:
            reset_request_id(request_token)
            reset_environment(environment_token)

    def execute_query(
        self,
        sql: str = "",
        query: str = "",
        database: str | None = None,
        schema: str | None = None,
        environment: str | None = None,
        timeout_seconds: int | None = None,
        max_rows: int | None = None,
        _tool_name: str = "execute_query",
    ) -> ToolResponse:
        """Validate policy, execute one statement, and normalize its result."""

        request_id, request_token, environment_token, start_time, requested_environment = self._begin_request(_tool_name)
        statement = sql or query
        try:
            normalized_sql = (sql or "").strip()
            normalized_query = (query or "").strip()
            if normalized_sql and normalized_query and normalized_sql != normalized_query:
                raise ConfigError("Provide either sql or query, not two different statements.")
            if max_rows is not None and max_rows <= 0:
                raise ConfigError("max_rows must be greater than zero.")
            # Per-request limits may reduce, but never raise, the configured cap.
            row_limit = min(max_rows or self._row_limit_ceiling(), self._row_limit_ceiling())
            # The calling client owns command authorization. MCP still enforces
            # one structurally unambiguous statement per call.
            valid, reason = validate_query(statement, self._db_type())
            if not valid:
                response = self._error(
                    tool=_tool_name,
                    environment=self._environment_name(),
                    code=ErrorCode.QUERY_BLOCKED,
                    message=reason,
                    request_id=request_id,
                    start_time=start_time,
                    retryable=False,
                    data={
                        "current_environment": self._environment_name(),
                        "database": database or self._database(),
                        "schema": schema or "",
                        "query": statement,
                        "row_count": 0,
                        "rows": [],
                    },
                )
                return self._finalize_request(
                    response,
                    tool=_tool_name,
                    environment=self._environment_name(),
                    request_id=request_id,
                    database=database or self._database(),
                    schema=schema or "",
                    query=statement,
                )

            target_database = self._execution_database(database)
            payload = self.connector.execute_query(
                # QueryService owns policy and response behavior; the selected
                # connector owns dialect details and transaction semantics.
                statement,
                database=target_database,
                timeout_seconds=self._effective_timeout(timeout_seconds),
                max_rows=row_limit,
            )
            columns = payload.get("columns", [])
            rows = payload.get("rows", [])
            response = self._response(
                tool=_tool_name,
                environment=self._environment_name(),
                success=True,
                request_id=request_id,
                start_time=start_time,
                data={
                    "current_environment": self._environment_name(),
                    "database": target_database,
                    "schema": schema or "",
                    "query": statement,
                    "row_limit": row_limit,
                    "row_count": len(rows),
                    "rows_affected": payload.get("rows_affected", len(rows)),
                    "columns": columns,
                    "rows": rows,
                },
                metadata={
                    "db_type": self._db_type(),
                    "row_limit": row_limit,
                },
            )
            return self._finalize_request(
                response,
                tool=_tool_name,
                environment=self._environment_name(),
                request_id=request_id,
                database=target_database,
                schema=schema or "",
                query=statement,
            )
        except Exception as exc:
            response = self._handle_connector_error(
                tool=_tool_name,
                requested_environment=requested_environment,
                request_id=request_id,
                start_time=start_time,
                error=exc,
                data={
                    "database": database or self._database(),
                    "schema": schema or "",
                    "query": statement,
                    "row_count": 0,
                    "rows": [],
                },
                message="Query execution failed.",
            )
            return self._finalize_request(
                response,
                tool=_tool_name,
                environment=requested_environment,
                request_id=request_id,
                database=database or self._database(),
                schema=schema or "",
                query=statement,
            )
        finally:
            reset_request_id(request_token)
            reset_environment(environment_token)

    def execute_select_query(self, **kwargs) -> ToolResponse:
        """Deprecated compatibility alias for the generic execution path."""

        return self.execute_query(_tool_name="execute_select_query", **kwargs)

    def config_diagnostics(self) -> ToolResponse:
        """Return agent-safe configuration diagnostics through the standard envelope."""

        request_id, request_token, environment_token, start_time, requested_environment = self._begin_request("config_diagnostics")
        try:
            response = self._response(
                tool="config_diagnostics",
                environment=self._environment_name() if self._db_type() else "UNCONFIGURED",
                success=True,
                request_id=request_id,
                start_time=start_time,
                data={
                    "current_environment": self._environment_name() if self._db_type() else "UNCONFIGURED",
                    "configuration": self._safe_diagnostics(),
                },
                metadata={"db_type": self._db_type()},
            )
            return self._finalize_request(
                response,
                tool="config_diagnostics",
                environment=self._environment_name() if self._db_type() else "UNCONFIGURED",
                request_id=request_id,
                database=self._database(),
                schema="",
                query="",
            )
        except Exception as exc:
            response = self._handle_connector_error(
                tool="config_diagnostics",
                requested_environment=requested_environment,
                request_id=request_id,
                start_time=start_time,
                error=exc,
                message="Configuration diagnostics failed.",
            )
            return self._finalize_request(
                response,
                tool="config_diagnostics",
                environment=requested_environment,
                request_id=request_id,
                database=self._database(),
                schema="",
                query="",
            )
        finally:
            reset_request_id(request_token)
            reset_environment(environment_token)


_QUERY_SERVICE: QueryService | None = None


def get_query_service() -> QueryService:
    """Return the process-wide service used by stateless MCP tool wrappers."""

    global _QUERY_SERVICE
    with runtime_lock:
        # The service is reused during normal operation and explicitly discarded
        # by profile switching when a different connector is required.
        if _QUERY_SERVICE is None:
            _QUERY_SERVICE = QueryService()
        return _QUERY_SERVICE


def reset_query_service() -> None:
    """Discard the cached connector service after a runtime profile change."""

    global _QUERY_SERVICE
    with runtime_lock:
        if _QUERY_SERVICE is not None:
            _QUERY_SERVICE.connector.close()
        _QUERY_SERVICE = None
