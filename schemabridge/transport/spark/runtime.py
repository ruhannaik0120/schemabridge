"""Lazy Spark-session lifecycle for automatically selected large-table transport."""

from __future__ import annotations

import importlib
import importlib.util
from collections.abc import Callable

from .config import SparkTransportSettings


class SparkRuntimeUnavailableError(RuntimeError):
    """Raised when a selected Spark migration cannot start its runtime."""


class SparkSessionFactory:
    """Create Spark sessions only after the large-table policy selects Spark."""

    def __init__(self, module_loader: Callable[[str], object] = importlib.import_module) -> None:
        self._module_loader = module_loader

    @staticmethod
    def runtime_available() -> bool:
        """Check for the optional PySpark package without importing it."""

        return importlib.util.find_spec("pyspark") is not None

    def create(self, settings: SparkTransportSettings) -> object:
        """Create one configured session without exposing connection details."""

        if not isinstance(settings, SparkTransportSettings):
            raise TypeError("settings must be SparkTransportSettings.")
        try:
            module = self._module_loader("pyspark.sql")
            spark_session = getattr(module, "SparkSession")
            builder = spark_session.builder.appName(settings.application_name)
            if settings.master is not None:
                builder = builder.master(settings.master)
            if settings.jars_packages is not None:
                builder = builder.config("spark.jars.packages", settings.jars_packages)
            return builder.getOrCreate()
        except (ImportError, AttributeError, TypeError, ValueError):
            raise SparkRuntimeUnavailableError(
                "Spark runtime is unavailable. Install requirements-spark.txt."
            ) from None

    @staticmethod
    def stop(session: object) -> None:
        """Stop a session created for one migration attempt when it is safe."""

        stop = getattr(session, "stop", None)
        if not callable(stop):
            raise TypeError("session must expose stop().")
        try:
            stop()
        except Exception:
            raise SparkRuntimeUnavailableError("Spark session shutdown failed.") from None
