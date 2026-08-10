"""SchemaBridge domain and database metadata models."""

from models.connection_profile import ConnectionProfile, ConnectionProfileError
from models.metadata import CanonicalType, ColumnMetadata
from models.discovery import (
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
