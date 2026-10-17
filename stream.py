import argparse
import os
import time
from typing import Callable, List, Optional

import requests
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from api_client import call_classify_api_partition, call_sentiment_api_partition
from checks import check_java_version, ensure_kafka_topic
from ollama_client import call_ollama_analysis_partition, validate_context_capacity
from sinks import KafkaSink, ParquetSink, Sink


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _coerce_bool(value, default: bool) -> bool:
    """Best-effort boolean coercion from config/env values."""

    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return default


def validate_ollama_settings(settings: dict) -> dict:
    """Normalize limits and reject invalid enabled configurations before startup."""
    resolved = dict(settings)
    enabled = _coerce_bool(resolved.get("enabled"), False)
    resolved["enabled"] = enabled
    if enabled and not str(resolved.get("model") or "").strip():
        raise ValueError("OLLAMA_MODEL is required when Ollama analysis is enabled")
    for key, default in (("timeout_s", 180), ("chunk_chars", 6000), ("num_ctx", 8192)):
        name = "OLLAMA_" + key.upper()
        value = resolved.get(key, default)
        try:
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                raise ValueError()
            value = int(value)
        except (TypeError, ValueError):
            if enabled:
                raise ValueError(f"{name} must be a positive integer") from None
            value = default
        if enabled and value <= 0:
            raise ValueError(f"{name} must be a positive integer")
        resolved[key] = value
    if enabled:
        validate_context_capacity(resolved["num_ctx"])
    return resolved


def load_ollama_settings(config: Optional[dict] = None) -> dict:
    """Resolve Ollama settings from YAML values and environment overrides."""
    configured = (config or {}).get("ollama", {}) or {}

    def resolve(name: str, key: str, default):
        return os.environ.get(name, configured.get(key, default))

    model = resolve("OLLAMA_MODEL", "model", None)
    if model is not None and not str(model).strip():
        model = None
    return validate_ollama_settings(
        {
            "enabled": _coerce_bool(resolve("OLLAMA_ENABLED", "enabled", False), False),
            "base_url": resolve(
                "OLLAMA_BASE_URL", "base_url", "http://localhost:11434"
            ),
            "model": model,
            "timeout_s": resolve("OLLAMA_TIMEOUT_S", "timeout_s", 180),
            "chunk_chars": resolve("OLLAMA_CHUNK_CHARS", "chunk_chars", 6000),
            "num_ctx": resolve("OLLAMA_NUM_CTX", "num_ctx", 8192),
            "keep_alive": resolve("OLLAMA_KEEP_ALIVE", "keep_alive", "5m"),
        }
    )


# API Base Configuration
API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:9000")
SENTIMENT_ENDPOINT = os.environ.get(
    "SENTIMENT_ENDPOINT", f"{API_BASE_URL}/api/sentiment/batch"
)
CLASSIFY_ENDPOINT = os.environ.get(
    "CLASSIFY_ENDPOINT", f"{API_BASE_URL}/api/topic/batch"
)
API_HEALTH_ENDPOINT = os.environ.get("API_HEALTH_ENDPOINT", f"{API_BASE_URL}/ready")

# Inference API Configuration
INFERENCE_BATCH_SIZE = _env_int("INFERENCE_BATCH_SIZE", 25)
INFERENCE_TIMEOUT_S = _env_int("INFERENCE_TIMEOUT_S", 30)
MAX_INFER_CHARS = _env_int("MAX_INFER_CHARS", 2000)

# API Readiness Configuration
API_READY_MAX_WAIT_S = _env_int("API_READY_MAX_WAIT_S", 60)
API_READY_INTERVAL_S = _env_int("API_READY_INTERVAL_S", 2)
API_READY_TIMEOUT_S = _env_int(
    "API_READY_TIMEOUT_S", 2
)  # Per-request timeout for readiness check

# Output Configuration
CONSOLE_ROWS = 50
TRUNCATE_DISPLAY = False

# Parquet sink defaults
PARQUET_OUTPUT_DIR = "./data/parquet/news_enriched"
PARQUET_CHECKPOINT_DIR = "./data/checkpoints/news_enriched_parquet"


def wait_for_api_ready(
    ready_url: str,
    timeout_s: int = API_READY_MAX_WAIT_S,
    interval_s: int = API_READY_INTERVAL_S,
) -> None:
    """Wait for the inference API to become ready.

    Polls the health/ready endpoint until a successful response is received
    or the timeout is exceeded.

    Args:
        ready_url: URL of the API health/ready endpoint.
        timeout_s: Maximum time to wait in seconds.
        interval_s: Interval between retry attempts in seconds.

    Raises:
        RuntimeError: If the API is not ready within the timeout period.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        print(f"[api] waiting for readiness: {ready_url} ...")
        try:
            response = requests.get(ready_url, timeout=API_READY_TIMEOUT_S)
            if response.status_code in (200, 204):
                print(f"[api] API is ready at {ready_url}")
                return
        except requests.exceptions.RequestException:
            pass  # Connection error, keep retrying
        time.sleep(interval_s)

    raise RuntimeError(f"Inference API not ready after {timeout_s}s. URL: {ready_url}")


def is_api_ready(ready_url: str = API_HEALTH_ENDPOINT) -> bool:
    """Check if the inference API is currently ready.

    Performs a single lightweight health check with a short timeout.

    Args:
        ready_url: URL of the API health/ready endpoint.

    Returns:
        True if the API responds with HTTP 200/204, False otherwise.
    """
    try:
        response = requests.get(ready_url, timeout=API_READY_TIMEOUT_S)
        return response.status_code in (200, 204)
    except requests.exceptions.RequestException:
        return False


def news_item_schema() -> StructType:
    """Return a StructType matching `NewsItem` fields.

    Fields:
      - title, author, text, summary, url, source, url_hash, fingerprint: String
      - published_at, scraped_at: String (ISO-8601; converted in parse_raw_news)
    """

    return StructType(
        [
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("text", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("url", StringType(), True),
            StructField("source", StringType(), True),
            StructField("published_at", StringType(), True),
            StructField("scraped_at", StringType(), True),
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
      - sentiment_inferred_at: String (nullable, ISO-8601)
      - sentiment_error: String (nullable, error message if present)
    """
    return StructType(
        [
            StructField("url_hash", StringType(), False),
            StructField("sentiment_label", StringType(), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("sentiment_inferred_at", StringType(), True),
            StructField("sentiment_error", StringType(), True),
        ]
    )


def classification_result_schema() -> StructType:
    """Schema for classification API responses returned from `call_classify_api_partition`.

    Fields:
      - url_hash: String (non-nullable, used as join key)
      - category_label: String (nullable)
      - category_score: Double (nullable)
      - category_inferred_at: String (nullable, ISO-8601)
      - category_error: String (nullable, error message if present)
    """
    return StructType(
        [
            StructField("url_hash", StringType(), False),
            StructField("category_label", StringType(), True),
            StructField("category_score", DoubleType(), True),
            StructField("category_inferred_at", StringType(), True),
            StructField("category_error", StringType(), True),
        ]
    )


def llm_result_schema() -> StructType:
    """Schema keyed by article URL and nullable content-version fingerprint."""
    return StructType(
        [
            StructField("url_hash", StringType(), False),
            StructField("fingerprint", StringType(), True),
            StructField("llm_analysis", StringType(), True),
            StructField("llm_model", StringType(), True),
            StructField("llm_inferred_at", StringType(), True),
            StructField("llm_error", StringType(), True),
        ]
    )


def _non_blank(col_name: str) -> F.Column:
    """Return the column value, or null if it is null/blank."""
    column = F.col(col_name)
    return F.when(F.trim(column) != "", column)


def build_text_for_inference() -> F.Column:
    """Build the text_for_inference column with fallback logic and truncation.

    Prefers summary > title > text_prepped (skipping blank values), normalizes
    whitespace, and truncates to MAX_INFER_CHARS characters.
    """
    raw_text = F.coalesce(
        _non_blank("summary"), _non_blank("title"), _non_blank("text_prepped")
    )
    normalized = F.trim(F.regexp_replace(raw_text, r"\s+", " "))
    return F.substring(normalized, 1, MAX_INFER_CHARS)


def prepare_base_dataframe(batch_df) -> DataFrame:
    """Prepare base dataframe for inference enrichment.

    Selects required fields, adds text_for_inference column, filters nulls,
    and deduplicates on the (url_hash, fingerprint) article version.
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
        .withColumn("text_for_inference", build_text_for_inference())
        .withColumn(
            "text_for_llm",
            F.when(
                F.trim(F.regexp_replace(F.col("text"), r"\s+", " ")) != "",
                F.trim(F.regexp_replace(F.col("text"), r"\s+", " ")),
            ),
        )
        .filter(F.col("url_hash").isNotNull() & F.col("text_for_inference").isNotNull())
        .dropDuplicates(["url_hash", "fingerprint"])
    )


def call_sentiment_api(spark, base_df) -> "DataFrame":
    """Call sentiment API via mapPartitions and return enriched dataframe.

    Handles batching, error handling, and joins results back on url_hash.
    """
    rdd = base_df.select("url_hash", "text_for_inference").rdd.mapPartitions(
        lambda it: call_sentiment_api_partition(
            (
                {
                    "url_hash": r["url_hash"],
                    "text_for_inference": r["text_for_inference"],
                }
                for r in it
            ),
            endpoint=SENTIMENT_ENDPOINT,
            batch_size=INFERENCE_BATCH_SIZE,
            timeout_s=INFERENCE_TIMEOUT_S,
        )
    )
    return spark.createDataFrame(rdd, schema=sentiment_result_schema())


def call_classification_api(spark, base_df) -> "DataFrame":
    """Call classification API via mapPartitions and return enriched dataframe.

    Handles batching, error handling, and joins results back on url_hash.
    """
    rdd = base_df.select("url_hash", "text_for_inference").rdd.mapPartitions(
        lambda it: call_classify_api_partition(
            (
                {
                    "url_hash": r["url_hash"],
                    "text_for_inference": r["text_for_inference"],
                }
                for r in it
            ),
            endpoint=CLASSIFY_ENDPOINT,
            batch_size=INFERENCE_BATCH_SIZE,
            timeout_s=INFERENCE_TIMEOUT_S,
        )
    )
    return spark.createDataFrame(rdd, schema=classification_result_schema())


def call_ollama_analysis(spark, base_df, settings: dict) -> DataFrame:
    """Analyze complete article text independently in each Spark partition."""
    partition_client = call_ollama_analysis_partition
    client_settings = {
        key: value for key, value in settings.items() if key != "enabled"
    }
    rdd = base_df.select("url_hash", "fingerprint", "text_for_llm").rdd.mapPartitions(
        lambda rows: partition_client((row.asDict() for row in rows), **client_settings)
    )
    return spark.createDataFrame(rdd, schema=llm_result_schema())


def display_enriched_results(
    enriched_df,
    batch_id: int,
    total: int,
    sentiment_error_count: int,
    category_error_count: int,
) -> None:
    """Display enriched inference results to console.

    Shows key columns: title, sentiment_label, sentiment_score, category_label, category_score, errors.
    """
    print(
        f"[enrich] batch_id={batch_id} processed={total} "
        f"sentiment_errors={sentiment_error_count} category_errors={category_error_count}"
    )
    enriched_df.select(
        "title",
        "sentiment_label",
        "sentiment_score",
        "category_label",
        "category_score",
        "sentiment_error",
        "category_error",
    ).show(n=CONSOLE_ROWS, truncate=TRUNCATE_DISPLAY)


def build_sinks_from_config(
    write_parquet: bool,
    write_kafka: bool,
    parquet_output_dir: str,
    bootstrap_servers: Optional[str] = None,
) -> List[Sink]:
    """Build a list of sinks from legacy configuration flags.

    Args:
        write_parquet: Whether to enable Parquet sink.
        write_kafka: Whether to enable Kafka sink.
        parquet_output_dir: Directory for Parquet output.
        bootstrap_servers: Kafka bootstrap servers.

    Returns:
        List of configured Sink objects.
    """
    sinks = []
    if write_parquet:
        sinks.append(ParquetSink(parquet_output_dir))
    if write_kafka and bootstrap_servers:
        sinks.append(KafkaSink(bootstrap_servers))
    return sinks


def parse_raw_news(kafka_df: DataFrame) -> DataFrame:
    """Parse Kafka `value` JSON into NewsItem columns plus `text_prepped`.

    Timestamps arrive as ISO-8601 strings (possibly empty) and are converted
    with try_to_timestamp so bad values become null instead of failing.
    """
    return (
        kafka_df.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json(F.col("json"), news_item_schema()).alias("news"))
        .select("news.*")
        .withColumn("published_at", F.try_to_timestamp(F.col("published_at")))
        .withColumn("scraped_at", F.try_to_timestamp(F.col("scraped_at")))
        .withColumn(
            "text_prepped", F.regexp_replace(F.trim(F.col("text")), r"\s+", " ")
        )
    )


def start_news_stream(
    bootstrap_servers: str,
    checkpoint_location: str,
    sinks: List[Sink],
    starting_offsets: str = "latest",
    kafka_topic: str = "raw_news",
    ollama_settings: Optional[dict] = None,
) -> None:
    """Start a Structured Streaming query consuming `raw_news` and parsing with `news_item_schema`.

    Args:
        bootstrap_servers: Kafka bootstrap servers, e.g. "localhost:9092" or "host1:9092,host2:9092".
        checkpoint_location: Filesystem path for Spark checkpoints.
        sinks: List of Sink objects to write enriched data to.
        ollama_settings: Resolved optional Ollama settings; defaults to environment values.
        starting_offsets: Kafka starting offsets ("earliest" or "latest"). Defaults to "latest".
        kafka_topic: Kafka topic name to subscribe to. Defaults to "raw_news".
    """

    batch_writer = make_foreach_batch_writer(sinks, ollama_settings=ollama_settings)

    kafka_packages = (
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
        "org.apache.kafka:kafka-clients:3.7.1"
    )

    spark = (
        SparkSession.builder.appName("raw-news-stream")
        .config("spark.jars.packages", kafka_packages)
        .config("spark.sql.streaming.schemaInference", False)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    if bootstrap_servers:
        ensure_kafka_topic(bootstrap_servers, "enriched_news")

    # Read raw Kafka messages; values are expected to be JSON-serialized `NewsItem`.
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    parsed_df = parse_raw_news(kafka_df)

    # Sentiment enrichment and writing via foreachBatch with custom sinks
    sentiment_query = (
        parsed_df.writeStream.option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .foreachBatch(batch_writer)
        .start()
    )

    sentiment_query.awaitTermination()


def make_foreach_batch_writer(
    sinks: List[Sink], ollama_settings: Optional[dict] = None
) -> Callable[..., None]:
    """Factory function that returns a foreachBatch writer function.

    Args:
        sinks: List of Sink objects to write enriched data to.
        ollama_settings: Resolved optional Ollama settings; defaults to environment values.

    Returns:
        A function (batch_df, batch_id) -> None that enriches and writes to all configured sinks.
    """

    settings = (
        validate_ollama_settings(ollama_settings)
        if ollama_settings is not None
        else load_ollama_settings()
    )

    def foreach_batch_writer(batch_df, batch_id) -> None:
        """Enrich a microbatch with sentiment and classification results and write to all configured sinks.

        Orchestrates the enrichment pipeline:
        1. Checks API readiness (skips batch if not ready)
        2. Prepares base dataframe with text_for_inference
        3. Calls sentiment API via mapPartitions
        4. Calls classification API via mapPartitions
        5. Joins ONNX results on url_hash and Ollama on (url_hash, fingerprint)
        6. Writes enriched results to all configured sinks
        7. Displays results to console
        """
        # Microbatch-level readiness guard
        if not is_api_ready():
            print(f"[enrich] batch_id={batch_id} api not ready; waiting for it")
            wait_for_api_ready(
                ready_url=API_HEALTH_ENDPOINT,
                timeout_s=API_READY_MAX_WAIT_S,
                interval_s=API_READY_INTERVAL_S,
            )

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

            # Call classification API and create dataframe
            classify_df = call_classification_api(spark, base)

            # Join both inference results
            enriched = base.join(sentiment_df, "url_hash", "left").join(
                classify_df, "url_hash", "left"
            )
            if settings.get("enabled"):
                llm_df = call_ollama_analysis(spark, base, settings)
                # A URL may have several content versions within one microbatch.
                # Null-safe equality also preserves analysis for legacy records.
                articles = enriched.alias("articles")
                analysis = llm_df.alias("analysis")
                enriched = articles.join(
                    analysis,
                    (articles["url_hash"] == analysis["url_hash"])
                    & articles["fingerprint"].eqNullSafe(analysis["fingerprint"]),
                    "left",
                ).select(
                    "articles.*",
                    *[
                        analysis[field.name]
                        for field in llm_result_schema().fields
                        if field.name not in {"url_hash", "fingerprint"}
                    ],
                )
            else:
                for field in llm_result_schema().fields:
                    if field.name not in {"url_hash", "fingerprint"}:
                        enriched = enriched.withColumn(
                            field.name, F.lit(None).cast(field.dataType)
                        )
            enriched.cache()

            try:
                # Collect stats
                total = enriched.count()
                sentiment_error_count = enriched.filter(
                    F.col("sentiment_error").isNotNull()
                ).count()
                category_error_count = enriched.filter(
                    F.col("category_error").isNotNull()
                ).count()

                llm_error_count = enriched.filter(
                    F.col("llm_error").isNotNull()
                ).count()
                print(f"[enrich] batch_id={batch_id} llm_errors={llm_error_count}")

                # Write to all configured sinks
                for sink in sinks:
                    try:
                        sink.write(enriched, batch_id)
                    except Exception as sink_exc:
                        print(
                            f"[enrich] batch_id={batch_id} sink {type(sink).__name__} failed: {sink_exc}"
                        )

                # Display results to console
                display_enriched_results(
                    enriched,
                    batch_id,
                    total,
                    sentiment_error_count,
                    category_error_count,
                )
            finally:
                enriched.unpersist()
        except Exception as exc:
            print(f"[enrich] batch_id={batch_id} failed: {exc}")
            raise

    return foreach_batch_writer


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
        "--parquet-output-dir",
        type=str,
        help="Parquet output directory (overrides config file)",
    )
    parser.add_argument(
        "--parquet-checkpoint-dir",
        type=str,
        help="Parquet checkpoint directory (overrides config file)",
    )
    parser.add_argument(
        "--write-parquet",
        dest="write_parquet",
        action="store_true",
        help="Enable Parquet sink (default: true; overrides config file)",
    )
    parser.add_argument(
        "--no-write-parquet",
        dest="write_parquet",
        action="store_false",
        help="Disable Parquet sink (overrides config file)",
    )
    parser.set_defaults(write_parquet=None)
    parser.add_argument(
        "--write-kafka",
        dest="write_kafka",
        action="store_true",
        help="Enable Kafka sink to enriched_news topic (default: false; overrides config file)",
    )
    parser.add_argument(
        "--no-write-kafka",
        dest="write_kafka",
        action="store_false",
        help="Disable Kafka sink (overrides config file)",
    )
    parser.set_defaults(write_kafka=None)
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
    parquet_config = config.get("parquet", {})
    write_parquet = (
        args.write_parquet
        if args.write_parquet is not None
        else _coerce_bool(parquet_config.get("enabled"), True)
    )
    write_kafka = (
        args.write_kafka
        if args.write_kafka is not None
        else _coerce_bool(config.get("kafka", {}).get("write_enriched"), False)
    )
    parquet_output_dir = (
        args.parquet_output_dir
        or parquet_config.get("output_dir")
        or PARQUET_OUTPUT_DIR
    )
    parquet_checkpoint_dir = (
        args.parquet_checkpoint_dir
        or parquet_config.get("checkpoint_dir")
        or PARQUET_CHECKPOINT_DIR
    )
    starting_offsets = args.starting_offsets or config.get("kafka", {}).get(
        "starting_offsets", "latest"
    )
    kafka_topic = config.get("kafka", {}).get("topic", "raw_news")

    # Build sinks from configuration
    sinks = build_sinks_from_config(
        write_parquet=write_parquet,
        write_kafka=write_kafka,
        parquet_output_dir=parquet_output_dir,
        bootstrap_servers=bootstrap_servers,
    )

    ollama_settings = load_ollama_settings(config)
    # Wait for inference API to be ready before starting stream
    wait_for_api_ready(
        ready_url=API_HEALTH_ENDPOINT,
        timeout_s=API_READY_MAX_WAIT_S,
        interval_s=API_READY_INTERVAL_S,
    )

    start_news_stream(
        bootstrap_servers=bootstrap_servers,
        checkpoint_location=checkpoint_location,
        sinks=sinks,
        starting_offsets=starting_offsets,
        kafka_topic=kafka_topic,
        ollama_settings=ollama_settings,
    )
