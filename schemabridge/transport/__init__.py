"""Contracts for moving bounded data batches into managed staging tables."""

from schemabridge.transport.base import BatchSourceReader, StagingTableWriter

__all__ = ["BatchSourceReader", "StagingTableWriter"]
