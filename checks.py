"""Health checks for Kafka and Java dependencies."""

import logging
import os
import re
import socket
import subprocess
import sys

logger = logging.getLogger(__name__)


def check_kafka_broker(bootstrap_servers: str, timeout: int = 5) -> bool:
    """Check if Kafka broker is reachable.

    Args:
        bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092" or "host1:9092,host2:9092").
        timeout: Connection timeout in seconds.

    Returns:
        True if at least one broker is reachable, False otherwise.
    """
    servers = bootstrap_servers.split(",")

    for server in servers:
        server = server.strip()
        host, port = server.rsplit(":", 1) if ":" in server else (server, "9092")

        try:
            logger.info("Pinging Kafka broker server=%s", server)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, int(port)))
            sock.close()

            if result == 0:
                logger.info("Kafka broker reachable server=%s", server)
                return True
            else:
                logger.warning("Kafka broker connection failed server=%s", server)
        except Exception:
            logger.exception("Kafka broker check failed server=%s", server)

    return False


def check_kafka_topic(bootstrap_servers: str, topic: str = "raw_news") -> bool:
    """Check if Kafka topic exists using kafka-python library.

    Args:
        bootstrap_servers: Kafka bootstrap servers.
        topic: Topic name to check.

    Returns:
        True if topic exists, False otherwise.
    """
    try:
        from kafka import KafkaConsumer

        logger.info("Checking Kafka topic existence topic=%s", topic)

        servers = [s.strip() for s in bootstrap_servers.split(",")]
        consumer = KafkaConsumer(
            bootstrap_servers=servers,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )

        topics = consumer.topics()
        consumer.close()

        if topic in topics:
            logger.info("Kafka topic found topic=%s", topic)
            return True
        else:
            logger.warning("Kafka topic not found topic=%s", topic)
            logger.info(
                "Kafka topics available topics=%s",
                sorted(topics) if topics else "None",
            )
            return False

    except ImportError:
        logger.warning("Kafka topic check skipped; kafka-python not installed")
        logger.info("Install with: pip install kafka-python")
        return True  # Don't fail if kafka-python not available
    except Exception:
        logger.exception("Kafka topic check skipped due to error")
        return True  # Don't fail on other errors


def ensure_kafka_topic(
    bootstrap_servers: str,
    topic: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    request_timeout_ms: int = 5000,
) -> bool:
    """Ensure a Kafka topic exists; create if missing.

    Returns True if topic exists or was created, False if create failed.
    """
    try:
        check_kafka_topic(bootstrap_servers, topic)
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError

        logger.info(
            "Creating Kafka topic topic=%s partitions=%s replication=%s",
            topic,
            num_partitions,
            replication_factor,
        )
        admin = KafkaAdminClient(
            bootstrap_servers=[s.strip() for s in bootstrap_servers.split(",")],
            request_timeout_ms=request_timeout_ms,
        )
        try:
            admin.create_topics(
                [
                    NewTopic(
                        name=topic,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                    )
                ],
                validate_only=False,
            )
            logger.info("Kafka topic created topic=%s", topic)
            return True
        except TopicAlreadyExistsError:
            logger.info("Kafka topic already exists topic=%s", topic)
            return True
        finally:
            admin.close()
    except ImportError:
        logger.warning("Kafka topic creation skipped; kafka-python not installed")
        logger.info("Install with: pip install kafka-python")
        return True
    except Exception:
        logger.exception("Kafka topic creation failed topic=%s", topic)
        return False


def check_java_version() -> None:
    """Check if Java 17 is installed and set as JAVA_HOME.

    Raises:
        SystemExit: If Java is not found or wrong version is detected.
    """
    java_home = os.environ.get("JAVA_HOME")

    if not java_home:
        logger.error("JAVA_HOME is not set.")
        logger.error("PySpark 4.1.1 requires Java 17.")
        logger.info("On macOS, set JAVA_HOME with:")
        logger.info("  export JAVA_HOME=$(/usr/libexec/java_home -v 17)")
        logger.info("Or install Java 17:")
        logger.info("  brew install --cask temurin17")
        sys.exit(1)

    try:
        # Check Java version
        result = subprocess.run(
            ["java", "-version"], capture_output=True, text=True, timeout=5
        )
        version_output = result.stderr  # java -version outputs to stderr

        # Parse version number (e.g., "17.0.1" from output)
        if "version" in version_output:
            # Extract version string, handling formats like "17.0.1", "1.8.0_xxx", etc.
            for line in version_output.split("\n"):
                if "version" in line:
                    # Look for quoted version string
                    match = re.search(r'"(\d+)', line)
                    if match:
                        major_version = int(match.group(1))

                        if major_version != 17:
                            logger.error(
                                "Java %s detected, but Java 17 is required.",
                                major_version,
                            )
                            logger.error("Current JAVA_HOME: %s", java_home)
                            logger.info("PySpark 4.1.1 is compatible with Java 17.")
                            logger.info("On macOS, set JAVA_HOME to Java 17:")
                            logger.info(
                                "  export JAVA_HOME=$(/usr/libexec/java_home -v 17)"
                            )
                            sys.exit(1)

                        logger.info("Java 17 detected JAVA_HOME=%s", java_home)
                        return

        logger.warning("Could not determine Java version.")
        logger.info("JAVA_HOME is set to: %s", java_home)

    except FileNotFoundError:
        logger.error("Java executable not found.")
        logger.error("JAVA_HOME is set to: %s", java_home)
        logger.info("Please install Java 17 and ensure it's in your PATH.")
        sys.exit(1)
    except Exception:
        logger.exception("Could not verify Java version.")
        logger.info("JAVA_HOME is set to: %s", java_home)
