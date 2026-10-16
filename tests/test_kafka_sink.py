"""KafkaSink must serialize the real enriched frame produced by stream.py."""

import json

import stream
from sinks import ENRICHED_KAFKA_COLUMNS, KafkaSink


def _enriched_df(spark):
    raw = spark.createDataFrame(
        [
            (
                json.dumps(
                    {
                        "title": "Headline",
                        "author": None,
                        "text": "Body",
                        "summary": "Summary",
                        "url": "https://example.com/a",
                        "source": "example",
                        "published_at": "",
                        "scraped_at": "2026-10-16T13:00:00+00:00",
                        "url_hash": "h1",
                        "fingerprint": "f1",
                    }
                ),
            )
        ],
        "value string",
    )
    base = stream.prepare_base_dataframe(stream.parse_raw_news(raw))
    sentiment = spark.createDataFrame(
        [("h1", "positive", 0.9, "t1", None)], stream.sentiment_result_schema()
    )
    category = spark.createDataFrame(
        [("h1", None, None, "t2", "boom")], stream.classification_result_schema()
    )
    return base.join(sentiment, "url_hash", "left").join(category, "url_hash", "left")


def test_kafka_frame_has_all_enriched_columns_with_nulls(spark) -> None:
    [row] = KafkaSink("unused:9092").to_kafka_frame(_enriched_df(spark)).collect()

    payload = json.loads(row.value)
    assert row.key == "h1"
    assert list(payload) == list(ENRICHED_KAFKA_COLUMNS)
    assert payload["sentiment_label"] == "positive"
    assert payload["category_label"] is None
    assert payload["category_error"] == "boom"
    assert payload["author"] is None
    assert payload["published_at"] is None
