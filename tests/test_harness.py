"""Sanity checks that the test container can run Spark and import the app."""

import stream


def test_spark_session_runs(spark) -> None:
    assert spark.range(3).count() == 3


def test_stream_module_imports() -> None:
    assert stream._coerce_bool("yes", False) is True
    assert stream._coerce_bool("off", True) is False
