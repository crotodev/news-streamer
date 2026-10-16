"""Custom sinks for writing enriched streaming data.

Provides a Sink protocol and concrete implementations for various
output destinations (Parquet, Kafka, etc.).
"""

import os
from typing import Protocol

from pyspark.sql import functions as F

ENRICHED_KAFKA_COLUMNS = (
    "title",
    "author",
    "text",
    "summary",
    "url",
    "source",
    "published_at",
    "scraped_at",
    "url_hash",
    "fingerprint",
    "sentiment_label",
    "sentiment_score",
    "sentiment_inferred_at",
    "sentiment_error",
    "category_label",
    "category_score",
    "category_inferred_at",
    "category_error",
)


class Sink(Protocol):
    """Protocol for custom sinks that write enriched data.

    Each sink receives the enriched dataframe and batch_id and writes
    the data to its destination (Parquet, Kafka, database, etc.).
    """

    def write(self, enriched_df, batch_id: int) -> None:
        """Write enriched dataframe to the sink destination.

        Args:
            enriched_df: Enriched dataframe with sentiment results.
            batch_id: Batch identifier for logging/debugging.
        """
        ...


class ParquetSink:
    """Sink that writes enriched data to Parquet files partitioned by ingest_date."""

    def __init__(self, output_dir: str) -> None:
        """Initialize Parquet sink.

        Args:
            output_dir: Directory to write Parquet files to.
        """
        self.output_dir = output_dir

    def write(self, enriched_df, batch_id: int) -> None:
        """Write enriched dataframe to Parquet with ingest_date partition."""
        ingest_date = F.to_date(F.current_timestamp())
        enriched_with_date = enriched_df.withColumn("ingest_date", ingest_date)
        os.makedirs(self.output_dir, exist_ok=True)
        enriched_with_date.write.mode("append").partitionBy("ingest_date").parquet(
            self.output_dir
        )


class KafkaSink:
    """Sink that writes enriched data to a Kafka topic as JSON."""

    def __init__(self, bootstrap_servers: str, topic: str = "enriched_news"):
        """Initialize Kafka sink.

        Args:
            bootstrap_servers: Kafka bootstrap servers.
            topic: Kafka topic name to write to.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def to_kafka_frame(self, enriched_df):
        """Project the enriched dataframe to Kafka key/value columns.

        key = url_hash; value = JSON of ENRICHED_KAFKA_COLUMNS with explicit nulls.
        """
        return enriched_df.select(
            F.col("url_hash").alias("key"),
            F.to_json(
                F.struct(*ENRICHED_KAFKA_COLUMNS), {"ignoreNullFields": "false"}
            ).alias("value"),
        )

    def write(self, enriched_df, batch_id: int) -> None:
        """Write enriched dataframe to Kafka topic as JSON."""
        self.to_kafka_frame(enriched_df).write.format("kafka").option(
            "kafka.bootstrap.servers", self.bootstrap_servers
        ).option("topic", self.topic).mode("append").save()
