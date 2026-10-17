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
    from pyspark.sql import functions as F

    enriched = base.join(sentiment, "url_hash", "left").join(category, "url_hash", "left")
    for field in ("llm_analysis", "llm_model", "llm_inferred_at", "llm_error"):
        enriched = enriched.withColumn(field, F.lit(None).cast("string"))
    return enriched


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


def test_kafka_payload_includes_llm_analysis_metadata_and_explicit_nulls(spark):
    from pyspark.sql import functions as F

    frame = _enriched_df(spark)
    sink = KafkaSink("unused:9092")
    disabled_payload = json.loads(sink.to_kafka_frame(frame).first().value)
    for field in ("llm_analysis", "llm_model", "llm_inferred_at", "llm_error"):
        assert field in disabled_payload
        assert disabled_payload[field] is None
    enabled = frame.withColumn(
        "llm_analysis", F.lit('{"summary":"Article analysis"}')
    ).withColumn("llm_model", F.lit("local-model"))
    enabled_payload = json.loads(sink.to_kafka_frame(enabled).first().value)
    assert enabled_payload["llm_analysis"] == '{"summary":"Article analysis"}'
    assert enabled_payload["llm_model"] == "local-model"
