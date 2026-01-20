# News Streamer

A PySpark Structured Streaming application that consumes news articles from a Kafka topic, parses JSON payloads, and displays them in real-time.

## Features

- **Real-time streaming** from Kafka using PySpark Structured Streaming
- **Flexible configuration** via YAML file or command-line arguments
- **Automatic schema parsing** for news articles with comprehensive metadata
- **Console output** for monitoring incoming articles
- **Checkpointing** for fault-tolerant streaming

## Prerequisites

- **Python 3.11+**
- **Java 17** (required for PySpark 4.1.1)
- **Apache Kafka** running locally or remotely
- **Kafka topic** named `raw_news` with JSON-formatted news articles

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd news-streamer
```

2. Create and activate a virtual environment:
```bash
python3.11 -m venv .venv
source .venv/bin/activate  # On macOS/Linux
# .venv\Scripts\activate  # On Windows
```

3. Install dependencies:
```bash
pip install pyspark pyyaml
```

4. Install Java 17 (macOS with Homebrew):
```bash
brew install --cask temurin17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

## Configuration

### Using YAML Config File (Recommended)

Create or edit `config.yaml`:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "raw_news"
  starting_offsets: "latest"  # Options: "earliest" or "latest"

spark:
  checkpoint_location: "/tmp/raw_news_checkpoints"
  app_name: "raw-news-stream"
```

### Using Command-Line Arguments

All configuration options can be overridden via command-line arguments:

```bash
python stream.py --bootstrap-servers localhost:9092 \
                 --checkpoint-location /tmp/checkpoints \
                 --starting-offsets earliest
```

## Usage

### Basic Usage

Run with default config file (`config.yaml`):

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) python stream.py
```

### With Custom Config File

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) python stream.py --config my-config.yaml
```

### Override Specific Settings

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) python stream.py --bootstrap-servers localhost:9093
```

### Using Only Command-Line Arguments

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) python stream.py \
  --bootstrap-servers localhost:9092 \
  --checkpoint-location /tmp/my-checkpoints \
  --starting-offsets earliest
```

### Start from Beginning of Topic

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) python stream.py --starting-offsets earliest
```

## News Article Schema

The application expects JSON messages in the Kafka topic with the following structure:

```json
{
  "title": "Article Title",
  "author": "Author Name",
  "text": "Full article text...",
  "summary": "Article summary...",
  "url": "https://example.com/article",
  "source": "example.com",
  "published_at": "2026-01-20T10:00:00",
  "scraped_at": "2026-01-20T10:05:00",
  "url_hash": "hash_of_url",
  "fingerprint": "content_fingerprint"
}
```

### Schema Fields

| Field | Type | Description |
|-------|------|-------------|
| `title` | String | Article headline |
| `author` | String | Article author |
| `text` | String | Full article content |
| `summary` | String | Article summary/excerpt |
| `url` | String | Original article URL |
| `source` | String | Source domain/publication |
| `published_at` | Timestamp | Publication timestamp (ISO-8601) |
| `scraped_at` | Timestamp | Scraping timestamp (ISO-8601) |
| `url_hash` | String | Hash of the article URL |
| `fingerprint` | String | Content fingerprint for deduplication |

All fields are nullable.

## Command-Line Options

```
usage: stream.py [-h] [--config CONFIG] [--bootstrap-servers BOOTSTRAP_SERVERS]
                 [--checkpoint-location CHECKPOINT_LOCATION]
                 [--starting-offsets {earliest,latest}]

Stream news from Kafka to console

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG       Path to YAML configuration file (default: config.yaml)
  --bootstrap-servers BOOTSTRAP_SERVERS
                        Kafka bootstrap servers (overrides config file)
  --checkpoint-location CHECKPOINT_LOCATION
                        Spark checkpoint location (overrides config file)
  --starting-offsets {earliest,latest}
                        Kafka starting offsets: earliest or latest (overrides config file)
```

## Troubleshooting

### Java Version Issues

**Problem**: Error about `getSubject is not supported` or `incubator modules`

**Solution**: Ensure you're using Java 17:
```bash
java -version  # Should show version 17.x
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

### Kafka Connection Issues

**Problem**: `UnknownTopicOrPartitionException`

**Solution**: 
- Verify Kafka is running: `kafka-topics --bootstrap-server localhost:9092 --list`
- Check the topic exists: Ensure `raw_news` topic is created
- Verify bootstrap servers in config match your Kafka setup

### Missing Dependencies

**Problem**: `ModuleNotFoundError: No module named 'yaml'`

**Solution**: Install PyYAML:
```bash
pip install pyyaml
```

### Checkpoint Issues

**Problem**: Errors about incompatible checkpoints

**Solution**: Clear the checkpoint directory:
```bash
rm -rf /tmp/raw_news_checkpoints
```

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│    Kafka    │─────▶│   PySpark    │─────▶│   Console   │
│  raw_news   │      │  Streaming   │      │   Output    │
└─────────────┘      └──────────────┘      └─────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │ Checkpoints  │
                     └──────────────┘
```

1. **Kafka Source**: Reads JSON messages from `raw_news` topic
2. **PySpark Processing**: Parses JSON using defined schema
3. **Console Sink**: Displays formatted articles in terminal
4. **Checkpointing**: Maintains streaming state for fault tolerance

## Development

### Project Structure

```
news-streamer/
├── stream.py           # Main streaming application
├── config.yaml         # Default configuration
├── README.md          # This file
└── .venv/             # Virtual environment (created during setup)
```

### Extending the Application

To add new output formats or processing logic, modify the `start_news_stream()` function in `stream.py`. The current implementation uses a console sink, but you can replace it with:

- **CSV output**: See commented code sections for CSV sink examples
- **Parquet files**: Change `.format("console")` to `.format("parquet")`
- **Database sink**: Use `foreachBatch()` for custom database writes
- **Kafka output**: Write to another Kafka topic for downstream processing

## License

See LICENSE file for details.

## Support

For issues or questions, please open an issue in the repository.
