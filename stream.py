import argparse
import os
import subprocess
import sys

import yaml
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from sentiment_client import call_sentiment_api_partition


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


SENTIMENT_ENDPOINT = os.environ.get(
    "SENTIMENT_ENDPOINT", "http://localhost:9000/api/sentiment/batch"
)
SENTIMENT_BATCH_SIZE = _env_int("SENTIMENT_BATCH_SIZE", 25)
SENTIMENT_TIMEOUT_S = _env_int("SENTIMENT_TIMEOUT_S", 30)


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


def check_java_version() -> None:
    """Check if Java 17 is installed and set as JAVA_HOME.
    
    Raises:
        SystemExit: If Java is not found or wrong version is detected.
    """
    java_home = os.environ.get("JAVA_HOME")
    
    if not java_home:
        print("ERROR: JAVA_HOME is not set.")
        print("\nPySpark 4.1.1 requires Java 17.")
        print("\nOn macOS, set JAVA_HOME with:")
        print("  export JAVA_HOME=$(/usr/libexec/java_home -v 17)")
        print("\nOr install Java 17:")
        print("  brew install --cask temurin17")
        sys.exit(1)
    
    try:
        # Check Java version
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        version_output = result.stderr  # java -version outputs to stderr
        
        # Parse version number (e.g., "17.0.1" from output)
        if "version" in version_output:
            # Extract version string, handling formats like "17.0.1", "1.8.0_xxx", etc.
            for line in version_output.split('\n'):
                if 'version' in line:
                    # Look for quoted version string
                    import re
                    match = re.search(r'"(\d+)', line)
                    if match:
                        major_version = int(match.group(1))
                        
                        if major_version != 17:
                            print(f"ERROR: Java {major_version} detected, but Java 17 is required.")
                            print(f"\nCurrent JAVA_HOME: {java_home}")
                            print("\nPySpark 4.1.1 is compatible with Java 17.")
                            print("\nOn macOS, set JAVA_HOME to Java 17:")
                            print("  export JAVA_HOME=$(/usr/libexec/java_home -v 17)")
                            sys.exit(1)
                        
                        print(f"✓ Java 17 detected (JAVA_HOME: {java_home})")
                        return
        
        print("WARNING: Could not determine Java version.")
        print(f"JAVA_HOME is set to: {java_home}")
        
    except FileNotFoundError:
        print("ERROR: Java executable not found.")
        print(f"JAVA_HOME is set to: {java_home}")
        print("\nPlease install Java 17 and ensure it's in your PATH.")
        sys.exit(1)
    except Exception as e:
        print(f"WARNING: Could not verify Java version: {e}")
        print(f"JAVA_HOME is set to: {java_home}")


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


def enrich_sentiment_foreach_batch(batch_df, batch_id) -> None:
    """Enrich a microbatch with sentiment results from the REST API.

    The enrichment runs per-partition using batched HTTP calls and joins results
    back on `url_hash`.
    """
    try:
        spark = batch_df.sparkSession

        base = (
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
            .filter(F.col("url_hash").isNotNull() & F.col("text_prepped").isNotNull())
            .dropDuplicates(["url_hash"])
        )

        count_after_filter = base.count()
        if count_after_filter == 0:
            print(f"[enrich] batch_id={batch_id} empty after filter; skipping")
            return

        rdd = base.select("url_hash", "text_prepped").rdd.mapPartitions(
            lambda it: call_sentiment_api_partition(
                (
                    {"url_hash": r["url_hash"], "text_prepped": r["text_prepped"]}
                    for r in it
                ),
                endpoint=SENTIMENT_ENDPOINT,
                batch_size=SENTIMENT_BATCH_SIZE,
                timeout_s=SENTIMENT_TIMEOUT_S,
            )
        )

        sentiment_df = spark.createDataFrame(rdd)
        enriched = base.join(sentiment_df, on="url_hash", how="left")
        enriched.cache()

        try:
            total = enriched.count()
            error_count = enriched.filter(F.col("error").isNotNull()).count()
            print(
                f"[enrich] batch_id={batch_id} processed={total} sentiment_errors={error_count}"
            )

            enriched.show(truncate=False)
        finally:
            enriched.unpersist()
    except Exception as exc:
        print(f"[enrich] batch_id={batch_id} failed: {exc}")