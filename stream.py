import argparse
import sys

import yaml
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession, functions as F

from checks import check_java_version, check_kafka_broker, check_kafka_topic


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
) -> None:
    """Start a Structured Streaming query consuming `raw_news` and parsing with `news_item_schema`.

    Args:
        bootstrap_servers: Kafka bootstrap servers, e.g. "localhost:9092" or "host1:9092,host2:9092".
        checkpoint_location: Filesystem path for Spark checkpoints.
        starting_offsets: Kafka starting offsets ("earliest" or "latest"). Defaults to "latest".
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
        .option("subscribe", "raw_news")
        .option("startingOffsets", starting_offsets)
        .load()
    )

    parsed_df = (
        kafka_df.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json(F.col("json"), news_item_schema()).alias("news"))
        .select("news.*")
    )

    prepped_df = parsed_df.withColumn(
        "text_prepped",
        F.regexp_replace(F.trim(F.col("text")), r"\s+", " ")
    )

    query = (
        prepped_df.writeStream.format("console")
        .option("truncate", False)
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()



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

    # Check Kafka broker connectivity before starting stream
    print("\n--- Kafka Connectivity Check ---")
    if not check_kafka_broker(bootstrap_servers):
        print(f"\nERROR: Cannot reach any Kafka broker at {bootstrap_servers}")
        print("Please ensure Kafka is running and accessible.")
        sys.exit(1)
    
    # Check if topic exists
    check_kafka_topic(bootstrap_servers, "raw_news")
    print("")

    start_news_stream(
        bootstrap_servers=bootstrap_servers,
        checkpoint_location=checkpoint_location,
        starting_offsets=starting_offsets,
    )