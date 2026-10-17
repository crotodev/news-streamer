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

class _CollectSink:
    def __init__(self):
        self.rows = []

    def write(self, enriched_df, batch_id):
        self.rows = enriched_df.collect()


def _article_batch(spark):
    import json

    body = "complete article " * 160 + "FINAL EVIDENCE"
    records = [
        {
            "url_hash": "h2",
            "text": "second article",
            "summary": "short two",
            "fingerprint": "f2",
        },
        {
            "url_hash": "h1",
            "text": body,
            "summary": "short one",
            "fingerprint": "f1",
        },
    ]
    return stream.parse_raw_news(
        spark.createDataFrame(
            [(json.dumps(record),) for record in records], "value string"
        )
    )


def _stub_onnx(spark, monkeypatch):
    monkeypatch.setattr(stream, "is_api_ready", lambda: True)
    monkeypatch.setattr(stream, "display_enriched_results", lambda *args: None)
    monkeypatch.setattr(
        stream,
        "call_sentiment_api",
        lambda *args: spark.createDataFrame(
            [("h1", "positive", 0.9, "s1", None), ("h2", "neutral", 0.8, "s2", None)],
            stream.sentiment_result_schema(),
        ),
    )
    monkeypatch.setattr(
        stream,
        "call_classification_api",
        lambda *args: spark.createDataFrame(
            [("h2", "science", 0.7, "c2", None), ("h1", "politics", 0.6, "c1", None)],
            stream.classification_result_schema(),
        ),
    )


def test_writer_disabled_adds_null_llm_fields_without_calling_ollama(spark, monkeypatch):
    monkeypatch.setenv("OLLAMA_ENABLED", "false")
    _stub_onnx(spark, monkeypatch)

    def forbidden(*args, **kwargs):
        raise AssertionError("disabled Ollama must not run")

    monkeypatch.setattr(stream, "call_ollama_analysis_partition", forbidden, raising=False)
    sink = _CollectSink()
    stream.make_foreach_batch_writer([sink])(_article_batch(spark), 1)
    assert len(sink.rows) == 2
    for row in sink.rows:
        value = row.asDict()
        for field in ("llm_analysis", "llm_model", "llm_inferred_at", "llm_error"):
            assert field in value
            assert value[field] is None


def test_writer_enabled_passes_full_text_and_settings_and_keeps_failed_articles(
    spark, monkeypatch, capsys
):
    import json

    from pyspark.sql import Row

    _stub_onnx(spark, monkeypatch)

    # This is the external HTTP boundary: retain real Spark projection/join/sinks.
    def analyze_partition(rows, **settings):
        for row in reversed(list(rows)):
            failed = row["url_hash"] == "h2"
            yield Row(
                url_hash=row["url_hash"],
                llm_analysis=(
                    None
                    if failed
                    else json.dumps({"text": row["text_for_llm"], "settings": settings})
                ),
                llm_model=settings["model"],
                llm_inferred_at="llm-time",
                llm_error="connection_refused" if failed else None,
            )

    monkeypatch.setattr(
        stream, "call_ollama_analysis_partition", analyze_partition, raising=False
    )
    settings = {
        "enabled": True,
        "model": "local-model",
        "base_url": "http://local:11434",
        "timeout_s": 41,
        "chunk_chars": 777,
        "num_ctx": 4096,
        "keep_alive": "2m",
    }
    first_sink, second_sink = _CollectSink(), _CollectSink()
    writer = stream.make_foreach_batch_writer(
        [first_sink, second_sink], ollama_settings=settings
    )
    writer(_article_batch(spark), 2)
    output = {row.url_hash: row for row in first_sink.rows}
    assert len(output) == len(second_sink.rows) == 2
    analysis = json.loads(output["h1"].llm_analysis)
    assert len(analysis["text"]) > stream.MAX_INFER_CHARS
    assert analysis["text"].endswith("FINAL EVIDENCE")
    assert analysis["settings"] == {
        key: value for key, value in settings.items() if key != "enabled"
    }
    assert output["h1"].text_for_inference == "short one"
    assert output["h1"].sentiment_label == "positive"
    assert output["h1"].category_label == "politics"
    assert output["h1"].llm_model == "local-model"
    assert output["h2"].llm_analysis is None
    assert output["h2"].llm_error == "connection_refused"
    assert output["h2"].sentiment_label == "neutral"
    assert output["h2"].category_label == "science"
    assert "llm_errors=1" in capsys.readouterr().out


def test_writer_rejects_enabled_ollama_without_model():
    with pytest.raises(ValueError, match="OLLAMA_MODEL"):
        stream.make_foreach_batch_writer(
            [], ollama_settings={"enabled": True, "model": "  "}
        )
