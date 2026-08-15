"""Contracts for moving bounded data batches into managed staging tables."""

from schemabridge.transport.base import (
    BatchSourceReader,
    StagingTableWriter,
    UnsupportedStagingTypeError,
)

__all__ = [
    "BatchSourceReader",
    "StagingTableWriter",
    "UnsupportedStagingTypeError",
]
