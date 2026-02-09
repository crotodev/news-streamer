from unittest.mock import Mock, patch

import pytest
import requests

import stream
from sinks import KafkaSink, ParquetSink


def test_env_int_parses_valid_and_invalid_values(monkeypatch):
    monkeypatch.setenv("SOME_INT", "10")
    assert stream._env_int("SOME_INT", 3) == 10

    monkeypatch.setenv("SOME_INT", "nope")
    assert stream._env_int("SOME_INT", 7) == 7

    monkeypatch.delenv("SOME_INT", raising=False)
    assert stream._env_int("SOME_INT", 5) == 5


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        ("False", False),
        ("yes", True),
        ("0", False),
        ("unknown", True),
    ],
)
def test_coerce_bool_handles_common_inputs(value, expected):
    assert stream._coerce_bool(value, True) == expected


def test_build_sinks_from_config_respects_flags():
    sinks = stream.build_sinks_from_config(
        write_parquet=True,
        write_kafka=True,
        parquet_output_dir="/tmp/out",
        bootstrap_servers="localhost:9092",
    )
    assert any(isinstance(sink, ParquetSink) for sink in sinks)
    assert any(isinstance(sink, KafkaSink) for sink in sinks)

    sinks = stream.build_sinks_from_config(
        write_parquet=True,
        write_kafka=True,
        parquet_output_dir="/tmp/out",
        bootstrap_servers=None,
    )
    assert any(isinstance(sink, ParquetSink) for sink in sinks)
    assert not any(isinstance(sink, KafkaSink) for sink in sinks)


def test_is_api_ready_true_and_false():
    response = Mock()
    response.status_code = 200
    with patch("stream.requests.get", return_value=response):
        assert stream.is_api_ready("http://example.test") is True

    with patch(
        "stream.requests.get",
        side_effect=requests.exceptions.RequestException("boom"),
    ):
        assert stream.is_api_ready("http://example.test") is False


def test_wait_for_api_ready_returns_on_success():
    response = Mock()
    response.status_code = 204
    with patch("stream.requests.get", return_value=response), patch(
        "stream.time.sleep", return_value=None
    ):
        stream.wait_for_api_ready(
            ready_url="http://example.test",
            timeout_s=1,
            interval_s=0,
        )
