"""Tests for raw-news parsing and inference-text preparation."""

import json

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


def test_parse_raw_news_empty_published_at_becomes_null(spark) -> None:
    [row] = stream.parse_raw_news(
        _kafka_df(spark, _item(published_at=""))
    ).collect()

    assert row.published_at is None
    assert row.title == "Headline"
    assert row.url_hash == "h1"


def test_parse_raw_news_null_published_at_is_null(spark) -> None:
    [row] = stream.parse_raw_news(
        _kafka_df(spark, _item(published_at=None))
    ).collect()

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
