"""SchemaBridge domain and database metadata models."""

from schemabridge.models.connection_profile import ConnectionProfile, ConnectionProfileError
from schemabridge.models.metadata import CanonicalType, ColumnMetadata
from schemabridge.models.discovery import (
    CheckConstraintMetadata,
    ConstraintType,
    CoverageStatus,
    DatabaseObjectMetadata,
    DatabaseObjectType,
    DiscoveryCoverage,
    ForeignKeyMetadata,
    KeyConstraintMetadata,
    ObjectPersistence,
    SchemaMetadata,
    TableMetadata,
)
