"""Contracts for moving bounded data batches into managed staging tables."""

from schemabridge.transport.base import (
    BatchSourceReader,
    StagingTableWriter,
    UnsupportedStagingTypeError,
)
from schemabridge.transport.strategy import (
    SequentialBatchTransportStrategy,
    TransportExecutionStrategy,
)

__all__ = [
    "BatchSourceReader",
    "StagingTableWriter",
    "SequentialBatchTransportStrategy",
    "TransportExecutionStrategy",
    "UnsupportedStagingTypeError",
]
