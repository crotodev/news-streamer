"""Tests for raw-news parsing and inference-text preparation."""

import json

import pytest

import stream


def _kafka_df(spark, *items):
    return spark.createDataFrame([(json.dumps(i),) for i in items], "value string")


def _item(**overrides):
    base = {
        "title": "Headline",
        "author": "A. Writer",
        "text": "Body   text\nhere",
        "summary": "Short summary",
        "url": "https://example.com/a",
        "source": "example",
        "published_at": "2026-10-16T12:30:00+00:00",
        "scraped_at": "2026-10-16T13:00:00.123456+00:00",
        "url_hash": "h1",
        "fingerprint": "f1",
    }
    base.update(overrides)
    return base


def test_parse_raw_news_converts_iso_timestamps(spark) -> None:
    [row] = stream.parse_raw_news(_kafka_df(spark, _item())).collect()

    assert row.title == "Headline"
    assert row.published_at is not None
    assert row.published_at.isoformat().startswith("2026-10-16T12:30:00")
    assert row.text_prepped == "Body text here"
    assert row.scraped_at is not None
    assert row.scraped_at.microsecond == 123456


def test_parse_raw_news_empty_published_at_becomes_null(spark) -> None:
    [row] = stream.parse_raw_news(_kafka_df(spark, _item(published_at=""))).collect()

    assert row.published_at is None
    assert row.title == "Headline"
    assert row.url_hash == "h1"


def test_parse_raw_news_null_published_at_is_null(spark) -> None:
    [row] = stream.parse_raw_news(_kafka_df(spark, _item(published_at=None))).collect()

    assert row.published_at is None


def test_text_for_inference_skips_blank_summary(spark) -> None:
    parsed = stream.parse_raw_news(_kafka_df(spark, _item(summary="  ")))
    [row] = stream.prepare_base_dataframe(parsed).collect()

    assert row.text_for_inference == "Headline"


def test_rows_with_no_usable_text_are_dropped(spark) -> None:
    parsed = stream.parse_raw_news(
        _kafka_df(spark, _item(summary="", title=" ", text=""))
    )

    assert stream.prepare_base_dataframe(parsed).count() == 0


def test_text_for_inference_is_truncated(spark) -> None:
    parsed = stream.parse_raw_news(_kafka_df(spark, _item(summary="x" * 5000)))
    [row] = stream.prepare_base_dataframe(parsed).collect()

    assert len(row.text_for_inference) == stream.MAX_INFER_CHARS


def test_default_ollama_settings_are_disabled(monkeypatch) -> None:
    for name in (
        "OLLAMA_ENABLED",
        "OLLAMA_BASE_URL",
        "OLLAMA_MODEL",
        "OLLAMA_TIMEOUT_S",
        "OLLAMA_CHUNK_CHARS",
        "OLLAMA_NUM_CTX",
        "OLLAMA_KEEP_ALIVE",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = stream.load_ollama_settings({})

    assert settings["enabled"] is False
    assert settings["base_url"] == "http://localhost:11434"
    assert settings["model"] is None
    assert settings["timeout_s"] == 180
    assert settings["chunk_chars"] == 6000
    assert settings["num_ctx"] == 8192
    assert settings["keep_alive"] == "5m"


def test_ollama_settings_use_yaml_defaults_and_environment_overrides(
    monkeypatch,
) -> None:
    for name in (
        "OLLAMA_ENABLED",
        "OLLAMA_BASE_URL",
        "OLLAMA_MODEL",
        "OLLAMA_TIMEOUT_S",
        "OLLAMA_CHUNK_CHARS",
        "OLLAMA_NUM_CTX",
        "OLLAMA_KEEP_ALIVE",
    ):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv("OLLAMA_ENABLED", "true")
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama:11434")
    monkeypatch.setenv("OLLAMA_MODEL", "test-model")

    settings = stream.load_ollama_settings(
        {
            "ollama": {
                "enabled": False,
                "base_url": "http://yaml:11434",
                "model": "yaml-model",
                "timeout_s": 90,
                "chunk_chars": 3000,
                "num_ctx": 4096,
                "keep_alive": "1m",
            }
        }
    )

    assert settings == {
        "enabled": True,
        "base_url": "http://ollama:11434",
        "model": "test-model",
        "timeout_s": 90,
        "chunk_chars": 3000,
        "num_ctx": 4096,
        "keep_alive": "1m",
    }


def test_text_for_llm_uses_full_whitespace_normalized_body(spark, monkeypatch) -> None:
    monkeypatch.setattr(stream, "MAX_INFER_CHARS", 12)
    parsed = stream.parse_raw_news(
        _kafka_df(spark, _item(text="  " + "long   body\n" * 100, summary="summary"))
    )
    [row] = stream.prepare_base_dataframe(parsed).collect()

    assert row.text_for_inference == "summary"
    assert row.text_for_llm == " ".join(("long body" for _ in range(100)))
    assert len(row.text_for_llm) > stream.MAX_INFER_CHARS


def test_missing_or_blank_body_keeps_onnx_fallback_and_null_llm_text(spark) -> None:
    parsed = stream.parse_raw_news(
        _kafka_df(
            spark,
            _item(text=None, summary="brief"),
            _item(url_hash="h2", fingerprint="f2", text=" \n  ", summary="brief"),
        )
    )
    rows = {
        row.url_hash: row for row in stream.prepare_base_dataframe(parsed).collect()
    }

    assert rows["h1"].text_for_inference == "brief"
    assert rows["h1"].text_for_llm is None
    assert rows["h2"].text_for_inference == "brief"
    assert rows["h2"].text_for_llm is None


def test_llm_result_schema_has_nullable_result_fields() -> None:
    fields = {field.name: field.nullable for field in stream.llm_result_schema().fields}

    assert fields == {
        "url_hash": False,
        "fingerprint": True,
        "llm_analysis": True,
        "llm_model": True,
        "llm_inferred_at": True,
        "llm_error": True,
    }


@pytest.mark.parametrize(
    "name", ["OLLAMA_TIMEOUT_S", "OLLAMA_CHUNK_CHARS", "OLLAMA_NUM_CTX"]
)
@pytest.mark.parametrize("value", ["invalid", "0", "-2"])
def test_enabled_ollama_rejects_invalid_numeric_settings(monkeypatch, name, value):
    monkeypatch.setenv("OLLAMA_ENABLED", "true")
    monkeypatch.setenv("OLLAMA_MODEL", "local")
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        stream.load_ollama_settings()


def test_enabled_ollama_rejects_context_too_small_for_fixed_prompt(monkeypatch):
    monkeypatch.setenv("OLLAMA_ENABLED", "true")
    monkeypatch.setenv("OLLAMA_MODEL", "local")
    monkeypatch.setenv("OLLAMA_NUM_CTX", "256")
    with pytest.raises(ValueError, match="OLLAMA_NUM_CTX"):
        stream.load_ollama_settings()


@pytest.mark.parametrize("key", ["timeout_s", "chunk_chars", "num_ctx"])
def test_enabled_ollama_rejects_malformed_yaml_numeric_settings(monkeypatch, key):
    monkeypatch.delenv("OLLAMA_" + key.upper(), raising=False)
    with pytest.raises(ValueError, match="OLLAMA_" + key.upper()):
        stream.load_ollama_settings(
            {"ollama": {"enabled": True, "model": "local", key: "bad"}}
        )
