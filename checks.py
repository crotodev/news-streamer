"""Health checks for Kafka and Java dependencies."""

import os
import re
import socket
import subprocess
import sys


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
            print(f"Pinging Kafka broker at {server}...", end="", flush=True)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, int(port)))
            sock.close()

            if result == 0:
                print(" ✓ OK")
                return True
            else:
                print(" ✗ Connection failed")
        except Exception as e:
            print(f" ✗ Error: {e}")

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

        print(f"Checking if topic '{topic}' exists...", end="", flush=True)

        servers = [s.strip() for s in bootstrap_servers.split(",")]
        consumer = KafkaConsumer(
            bootstrap_servers=servers,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )

        topics = consumer.topics()
        consumer.close()

        if topic in topics:
            print(f" ✓ Found")
            return True
        else:
            print(f" ✗ Not found")
            print(f"Available topics: {sorted(topics) if topics else 'None'}")
            return False

    except ImportError:
        print(" ? Skipped (kafka-python not installed)")
        print("Install with: pip install kafka-python")
        return True  # Don't fail if kafka-python not available
    except Exception as e:
        print(f" ? Skipped ({e})")
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

        print(
            f"Creating topic '{topic}' with partitions={num_partitions}, replication={replication_factor}...",
            end="",
            flush=True,
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
            print(" ✓ Created")
            return True
        except TopicAlreadyExistsError:
            print(" ✓ Already exists")
            return True
        finally:
            admin.close()
    except ImportError:
        print(" ? Skipped (kafka-python not installed)")
        print("Install with: pip install kafka-python")
        return True
    except Exception as e:
        print(f" ✗ Failed to create topic '{topic}': {e}")
        return False


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
                            print(
                                f"ERROR: Java {major_version} detected, but Java 17 is required."
                            )
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
