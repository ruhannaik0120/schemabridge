"""Verify Spark remains lazy and receives only explicit runtime settings."""

from __future__ import annotations

import pytest

from schemabridge.spark import (
    SparkRuntimeUnavailableError,
    SparkSessionFactory,
    SparkTransportSettings,
)


class Builder:
    def __init__(self) -> None:
        self.application = None
        self.cluster = None
        self.settings = {}
        self.session = Session()

    def appName(self, value):
        self.application = value
        return self

    def master(self, value):
        self.cluster = value
        return self

    def config(self, key, value):
        self.settings[key] = value
        return self

    def getOrCreate(self):
        return self.session


class Session:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


class SparkModule:
    class SparkSession:
        builder = Builder()


def test_factory_imports_pyspark_only_when_creating_an_enabled_session() -> None:
    imports = []

    def loader(name):
        imports.append(name)
        return SparkModule

    factory = SparkSessionFactory(loader)
    assert imports == []

    session = factory.create(
        SparkTransportSettings(
            master="local[2]",
            jars_packages="org.postgresql:postgresql:42.7.5",
            application_name="SchemaBridge test",
        )
    )

    assert imports == ["pyspark.sql"]
    assert SparkModule.SparkSession.builder.application == "SchemaBridge test"
    assert SparkModule.SparkSession.builder.cluster == "local[2]"
    assert SparkModule.SparkSession.builder.settings == {
        "spark.jars.packages": "org.postgresql:postgresql:42.7.5"
    }
    factory.stop(session)
    assert session.stopped is True


def test_runtime_probe_checks_for_pyspark_without_starting_a_session() -> None:
    assert isinstance(SparkSessionFactory.runtime_available(), bool)


def test_missing_optional_dependency_has_a_safe_error() -> None:
    factory = SparkSessionFactory(lambda _name: (_ for _ in ()).throw(ImportError()))

    with pytest.raises(SparkRuntimeUnavailableError, match="requirements-spark"):
        factory.create(SparkTransportSettings())
