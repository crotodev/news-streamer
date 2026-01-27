import argparse
import os
import subprocess
import sys

import yaml
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    StringType,
    TimestampType,
)

from checks import check_java_version
from sentiment_client import call_sentiment_api_partition


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


# Sentiment API Configuration
SENTIMENT_ENDPOINT = os.environ.get(
    "SENTIMENT_ENDPOINT", "http://localhost:9000/api/sentiment/batch"
)
SENTIMENT_BATCH_SIZE = _env_int("SENTIMENT_BATCH_SIZE", 25)
SENTIMENT_TIMEOUT_S = _env_int("SENTIMENT_TIMEOUT_S", 30)
MAX_SENTIMENT_CHARS = _env_int("MAX_SENTIMENT_CHARS", 2000)

# Output Configuration
CONSOLE_ROWS = 50
TRUNCATE_DISPLAY = False


def news_item_schema() -> StructType:
    """Return a StructType matching `NewsItem` fields.

    Fields:
      - title, author, text, summary, url, source, url_hash, fingerprint: String
      - published_at, scraped_at: Timestamp (nullable; expect ISO-8601 strings)
    """

    return StructType(
        [
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("text", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("url", StringType(), True),
            StructField("source", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("scraped_at", TimestampType(), True),
            StructField("url_hash", StringType(), True),
            StructField("fingerprint", StringType(), True),
        ]
    )


def sentiment_result_schema() -> StructType:
    """Schema for sentiment API responses returned from `call_sentiment_api_partition`.

    Fields:
      - url_hash: String (non-nullable, used as join key)
      - sentiment_label: String (nullable)
      - sentiment_score: Double (nullable)
      - inferred_at: String (nullable, ISO-8601)
      - error: String (nullable, error message if present)
    """
    return StructType(
        [
            StructField("url_hash", StringType(), False),
            StructField("sentiment_label", StringType(), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("inferred_at", StringType(), True),
            StructField("error", StringType(), True),
        ]
    )


def build_text_for_sentiment(df) -> F.Column:
    """Build the text_for_sentiment column with fallback logic and truncation.
    
    Prefers summary > title > text_prepped, normalizes whitespace, and truncates
    to MAX_SENTIMENT_CHARS characters.
    """
    raw_text = F.coalesce(F.col("summary"), F.col("title"), F.col("text_prepped"))
    normalized = F.trim(F.regexp_replace(raw_text, r"\s+", " "))
    return F.substring(normalized, 1, MAX_SENTIMENT_CHARS)


def prepare_base_dataframe(batch_df) -> "DataFrame":
    """Prepare base dataframe for sentiment enrichment.
    
    Selects required fields, adds text_for_sentiment column, filters nulls,
    and deduplicates on url_hash.
    """
    return (
        batch_df.select(
            "url_hash",
            "text_prepped",
            "title",
            "author",
            "text",
            "summary",
            "url",
            "source",
            "published_at",
            "scraped_at",
            "fingerprint",
        )
        .withColumn("text_for_sentiment", build_text_for_sentiment(batch_df))
        .filter(F.col("url_hash").isNotNull() & F.col("text_for_sentiment").isNotNull())
        .dropDuplicates(["url_hash"])
    )


def call_sentiment_api(spark, base_df) -> "DataFrame":
    """Call sentiment API via mapPartitions and return enriched dataframe.
    
    Handles batching, error handling, and joins results back on url_hash.
    """
    rdd = base_df.select("url_hash", "text_for_sentiment").rdd.mapPartitions(
        lambda it: call_sentiment_api_partition(
            (
                {"url_hash": r["url_hash"], "text_for_sentiment": r["text_for_sentiment"]}
                for r in it
            ),
            endpoint=SENTIMENT_ENDPOINT,
            batch_size=SENTIMENT_BATCH_SIZE,
            timeout_s=SENTIMENT_TIMEOUT_S,
        )
    )
    return spark.createDataFrame(rdd, schema=sentiment_result_schema())


def display_enriched_results(enriched_df, batch_id: int, total: int, error_count: int) -> None:
    """Display enriched sentiment results to console.
    
    Shows key columns: title, url, sentiment_label, sentiment_score, error.
    """
    print(
        f"[enrich] batch_id={batch_id} processed={total} sentiment_errors={error_count}"
    )
    enriched_df.select(
        "title",
        "url",
        "sentiment_label",
        "sentiment_score",
        "error",
    ).show(numRows=CONSOLE_ROWS, truncate=TRUNCATE_DISPLAY)


def start_news_stream(
    bootstrap_servers: str,
    checkpoint_location: str,
    starting_offsets: str = "latest",
    kafka_topic: str = "raw_news",
) -> None:
    """Start a Structured Streaming query consuming `raw_news` and parsing with `news_item_schema`.

    Args:
        bootstrap_servers: Kafka bootstrap servers, e.g. "localhost:9092" or "host1:9092,host2:9092".
        checkpoint_location: Filesystem path for Spark checkpoints.
        starting_offsets: Kafka starting offsets ("earliest" or "latest"). Defaults to "latest".
        kafka_topic: Kafka topic name to subscribe to. Defaults to "raw_news".
    """

    kafka_packages = (
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
        "org.apache.kafka:kafka-clients:3.7.1"
    )

    spark = (
        SparkSession.builder.appName("raw-news-stream")
        .config("spark.jars.packages", kafka_packages)
        .config("spark.sql.streaming.schemaInference", False)
        .getOrCreate()
    )

    # Read raw Kafka messages; values are expected to be JSON-serialized `NewsItem`.
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    parsed_df = (
        kafka_df.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json(F.col("json"), news_item_schema()).alias("news"))
        .select("news.*")
        .withColumn(
            "text_prepped", F.regexp_replace(F.trim(F.col("text")), r"\s+", " ")
        )
    )

    query = (
        parsed_df.writeStream
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .foreachBatch(enrich_sentiment_foreach_batch)
        .start()
    )

    query.awaitTermination()


def enrich_sentiment_foreach_batch(batch_df, batch_id) -> None:
    """Enrich a microbatch with sentiment results from the REST API.

    Orchestrates the enrichment pipeline:
    1. Prepares base dataframe with text_for_sentiment
    2. Calls sentiment API via mapPartitions
    3. Joins sentiment results back on url_hash
    4. Displays results to console
    """
    try:
        spark = batch_df.sparkSession

        # Prepare base dataframe
        base = prepare_base_dataframe(batch_df)

        count_after_filter = base.count()
        if count_after_filter == 0:
            print(f"[enrich] batch_id={batch_id} empty after filter; skipping")
            return

        # Call sentiment API and create dataframe
        sentiment_df = call_sentiment_api(spark, base)

        # Join sentiment results
        enriched = base.join(sentiment_df, on="url_hash", how="left")
        enriched.cache()

        try:
            # Collect stats and display
            total = enriched.count()
            error_count = enriched.filter(F.col("error").isNotNull()).count()
            display_enriched_results(enriched, batch_id, total, error_count)
        finally:
            enriched.unpersist()
    except Exception as exc:
        print(f"[enrich] batch_id={batch_id} failed: {exc}")


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file.

    Returns:
        Dictionary containing configuration parameters.
    """
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    # Check Java version before proceeding
    check_java_version()
    
    parser = argparse.ArgumentParser(
        description="Stream news from Kafka to console",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to YAML configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        help="Kafka bootstrap servers (overrides config file)",
    )
    parser.add_argument(
        "--checkpoint-location",
        type=str,
        help="Spark checkpoint location (overrides config file)",
    )
    parser.add_argument(
        "--starting-offsets",
        type=str,
        choices=["earliest", "latest"],
        help="Kafka starting offsets: earliest or latest (overrides config file)",
    )
    args = parser.parse_args()

    # Load configuration from YAML file if it exists, otherwise use defaults
    config = {}
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        pass

    # Command-line arguments override config file
    bootstrap_servers = args.bootstrap_servers or config.get("kafka", {}).get(
        "bootstrap_servers", "localhost:9092"
    )
    checkpoint_location = args.checkpoint_location or config.get("spark", {}).get(
        "checkpoint_location", "/tmp/raw_news_checkpoints"
    )
    starting_offsets = args.starting_offsets or config.get("kafka", {}).get(
        "starting_offsets", "latest"
    )
    kafka_topic = config.get("kafka", {}).get("topic", "raw_news")

    start_news_stream(
        bootstrap_servers=bootstrap_servers,
        checkpoint_location=checkpoint_location,
        starting_offsets=starting_offsets,
        kafka_topic=kafka_topic,
    )