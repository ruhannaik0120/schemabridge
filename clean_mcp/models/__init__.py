"""Shared response and error models for the MCP execution framework.

This package contains the structured contracts returned by every MCP tool. It
should not include transport, connector, or SQL validation logic.
"""

from models.errors import ErrorCode, StructuredError
from models.connection_profile import ConnectionProfile, ConnectionProfileError
from models.metadata import CanonicalType, ColumnMetadata
from models.responses import ToolResponse
