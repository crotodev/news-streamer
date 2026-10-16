"""Tests for the foreachBatch writer's API-readiness and failure handling."""

import pytest

import stream


def test_writer_raises_when_api_not_ready_and_wait_times_out(spark, monkeypatch) -> None:
    batch_df = spark.createDataFrame([("x",)], "value string")

    monkeypatch.setattr(stream, "is_api_ready", lambda *a, **k: False)

    def _raise_timeout(*args, **kwargs):
        raise RuntimeError("API did not become ready in time")

    monkeypatch.setattr(stream, "wait_for_api_ready", _raise_timeout)

    writer = stream.make_foreach_batch_writer([])

    with pytest.raises(RuntimeError):
        writer(batch_df, 0)


def test_writer_raises_when_processing_fails(spark, monkeypatch) -> None:
    batch_df = spark.createDataFrame([("x",)], "value string")

    monkeypatch.setattr(stream, "is_api_ready", lambda *a, **k: True)

    def _raise_value_error(*args, **kwargs):
        raise ValueError("boom")

    monkeypatch.setattr(stream, "prepare_base_dataframe", _raise_value_error)

    writer = stream.make_foreach_batch_writer([])

    with pytest.raises(ValueError):
        writer(batch_df, 0)
