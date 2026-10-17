# News Streamer

A PySpark Structured Streaming application that consumes news articles from Kafka, enriches them with sentiment, and optionally writes enriched results back to Kafka and/or Parquet.

## Features

- **Real-time streaming** from Kafka using PySpark Structured Streaming
- **Flexible configuration** via YAML file or command-line arguments
- **Automatic schema parsing** for news articles with comprehensive metadata
- **Console output** for monitoring incoming articles
- **Kafka output** to `enriched_news` (auto-creates topic)
- **Parquet output** partitioned by ingest date
- **Checkpointing** for fault-tolerant streaming

## Prerequisites

- **Python 3.11+**
- **Java 17** (required for PySpark 4.1.1)
- **Apache Kafka** running locally or remotely
- **Kafka topic** named `raw_news` with JSON-formatted news articles
- **Kafka topic** named `enriched_news` (auto-created at startup if missing)

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
pip install -r requirements.txt
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
  write_enriched: true

spark:
  checkpoint_location: "/tmp/raw_news_checkpoints"
  app_name: "raw-news-stream"
```

### Using Command-Line Arguments

Kafka and Spark settings can be overridden via command-line arguments. Ollama settings use YAML and environment variables as described below:

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

### Enable Kafka Output

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) python stream.py --write-kafka
```
```

## Optional full-article analysis with Ollama

Ollama adds a structured analysis of the complete extracted article `text` after whitespace normalization. It uses a separate full-text column from the length-limited text sent to the ONNX sentiment and topic endpoints. Analysis is disabled by default, and disabled runs make no Ollama requests.

Install and start [Ollama](https://ollama.com/download) on the host or another reachable machine. Select a model that fits your available RAM/VRAM and supports instruction following and JSON output. Replace `your-model:tag` below with that model's actual name. Download it explicitly before enabling analysis:

```powershell
ollama pull your-model:tag
# If the Ollama desktop app/service is not already serving:
ollama serve
```

Run `ollama serve` in a separate terminal if needed. For a streamer running directly on the host, configure the same model and enable analysis:

```powershell
$env:OLLAMA_ENABLED = "true"
$env:OLLAMA_MODEL = "your-model:tag"
$env:OLLAMA_BASE_URL = "http://localhost:11434"
python stream.py
```

For the root Docker Compose deployment, the same environment variables are passed into the streamer, with a container default URL of `http://host.docker.internal:11434`. See the root README for the Compose command. Docker Desktop provides that hostname on Windows/macOS; other Docker environments need a host address reachable from the container or an appropriate host gateway mapping. A remote Ollama server can be selected with `OLLAMA_BASE_URL`.

YAML values belong under `ollama` in `config.yaml`. Each corresponding environment variable takes precedence. Ollama has no command-line flags in the streamer.

| Environment variable | YAML key | Host default | Purpose |
| --- | --- | --- | --- |
| `OLLAMA_ENABLED` | `enabled` | `false` | Enable analysis; set to `false` to opt out |
| `OLLAMA_BASE_URL` | `base_url` | `http://localhost:11434` | Ollama server URL; Compose defaults to `http://host.docker.internal:11434` |
| `OLLAMA_MODEL` | `model` | unset (`null`) | Exact name of a locally available model; required when enabled |
| `OLLAMA_TIMEOUT_S` | `timeout_s` | `180` | HTTP timeout in seconds for each request |
| `OLLAMA_CHUNK_CHARS` | `chunk_chars` | `6000` | Maximum normalized characters per article segment |
| `OLLAMA_NUM_CTX` | `num_ctx` | `8192` | Ollama context size per request, including prompts and output |
| `OLLAMA_KEEP_ALIVE` | `keep_alive` | `5m` | How long Ollama keeps the model loaded after a request |

A missing model name stops enabled startup with a configuration error. The pipeline does not download models or start Ollama. HTTP failures, unavailable models, empty article bodies, malformed JSON, and invalid analysis fields produce per-article `llm_error` metadata while the article continues to the configured sinks. Requests are made during batch enrichment, so an unreachable server can delay a batch until the timeout.

An article within the segment limit uses one request. Longer articles use one request for each segment, followed by a request that synthesizes all segment observations. Smaller segments reduce each request's input but increase request count and the size of the synthesis input. Larger context settings use more memory and must fit the selected model and machine. Analysis adds latency and CPU/GPU work; long articles may take substantially longer than ONNX predictions.

There is no silent text truncation. A single whitespace-delimited token larger than `OLLAMA_CHUNK_CHARS` yields `article_token_exceeds_chunk_budget`. Every request, including final synthesis, is checked against a conservative UTF-8 byte estimate with room reserved for prompts and generated JSON. If the complete prompt or all segment observations cannot fit, the article yields `context_budget_exceeded` and no partial analysis is returned. Increasing `OLLAMA_NUM_CTX`, choosing a suitable model, or adjusting the segment size may be necessary. Character limits are not token limits, and multibyte text can reach this guard sooner.

Enriched Kafka JSON and Parquet rows include:

| Field | Value |
| --- | --- |
| `llm_analysis` | JSON string with `summary` and `sentiment` strings plus `key_points`, `entities`, and `caveats` arrays of strings |
| `llm_model` | Configured model name for an attempted analysis |
| `llm_inferred_at` | UTC timestamp for the analysis attempt |
| `llm_error` | Error text on failure; null on success |

All four fields are null when disabled. On failure, `llm_analysis` is null and the attempt's model, timestamp, and error remain available. To disable analysis, set `OLLAMA_ENABLED=false` (and recreate the streamer if using Compose).

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
                 [--parquet-output-dir PARQUET_OUTPUT_DIR]
                 [--parquet-checkpoint-dir PARQUET_CHECKPOINT_DIR]
                 [--write-parquet | --no-write-parquet]
                 [--write-kafka | --no-write-kafka]
                 [--starting-offsets {earliest,latest}]

Stream news from Kafka to console

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG       Path to YAML configuration file (default: config.yaml)
  --bootstrap-servers BOOTSTRAP_SERVERS
                        Kafka bootstrap servers (overrides config file)
  --checkpoint-location CHECKPOINT_LOCATION
                        Spark checkpoint location (overrides config file)
  --parquet-output-dir PARQUET_OUTPUT_DIR
                        Parquet output directory (overrides config file)
  --parquet-checkpoint-dir PARQUET_CHECKPOINT_DIR
                        Parquet checkpoint directory (overrides config file)
  --write-parquet         Enable Parquet sink (default: true; overrides config file)
  --no-write-parquet      Disable Parquet sink (overrides config file)
  --write-kafka           Enable Kafka sink to enriched_news topic (default: false; overrides config file)
  --no-write-kafka        Disable Kafka sink (overrides config file)
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
- Check the topic exists: Ensure `raw_news` topic is created; `enriched_news` is auto-created at startup
- Verify bootstrap servers in config match your Kafka setup

### Missing Dependencies

**Problem**: `ModuleNotFoundError: No module named 'yaml'`

**Solution**: Install PyYAML:
```bash
pip install pyyaml
```

**Problem**: `No module named 'kafka'`

**Solution**: Install kafka-python:
```bash
pip install kafka-python
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
        ├──────────────▶ Kafka: enriched_news
        │
        ├──────────────▶ Parquet: data/parquet/news_enriched
        │
        ▼
      ┌──────────────┐
      │ Checkpoints  │
      └──────────────┘
```

1. **Kafka Source**: Reads JSON messages from `raw_news` topic
2. **PySpark Processing**: Parses JSON using defined schema
3. **Console Sink**: Displays formatted articles in terminal
4. **Kafka Sink**: Writes enriched JSON to `enriched_news` topic (optional)
5. **Parquet Sink**: Writes enriched rows partitioned by `ingest_date` (optional)
6. **Checkpointing**: Maintains streaming state for fault tolerance

## Development

### Project Structure

```
news-streamer/
├── stream.py              # Main streaming application
├── sentiment_client.py    # Sentiment API HTTP client
├── checks.py             # Health checks and environment validation
├── config.yaml           # Default configuration
├── README.md            # This file
├── requirements.txt     # Python dependencies
└── .venv/               # Virtual environment (created during setup)
```

### Code Organization

**stream.py** - Main streaming pipeline with modular functions:
- `build_text_for_sentiment()` - Text preparation for sentiment analysis (fallback: summary > title > body, truncate to 2000 chars)
- `prepare_base_dataframe()` - DataFrame preparation and filtering
- `call_sentiment_api()` - Sentiment API integration via mapPartitions
- `display_enriched_results()` - Console output formatting
- `enrich_sentiment_foreach_batch()` - Orchestrates the enrichment pipeline
- `start_news_stream()` - Sets up the Structured Streaming query

**sentiment_client.py** - REST API client for sentiment inference:
- Batches HTTP requests to sentiment service
- Handles empty text filtering
- Returns structured sentiment results (label, score, error)

**checks.py** - Environment validation:
- `check_java_version()` - Validates Java 17 installation
- `check_kafka_broker()` - Tests Kafka connectivity
- `check_kafka_topic()` - Verifies topic existence
- `ensure_kafka_topic()` - Creates topic if missing

### Configuration & Environment

**Environment Variables** (all optional, tunable):
```bash
SENTIMENT_ENDPOINT=http://localhost:9000/api/sentiment/batch   # Sentiment service URL
SENTIMENT_BATCH_SIZE=25                                          # Items per API batch
SENTIMENT_TIMEOUT_S=30                                           # HTTP request timeout
MAX_SENTIMENT_CHARS=2000                                         # Text truncation limit
```

### Extending the Application

To add new output formats or processing logic, modify the `start_news_stream()` function in `stream.py`. The current implementation uses a console sink and sentiment enrichment, but you can:

- **CSV output**: See commented code sections for CSV sink examples
- **Parquet files**: Change `.format("console")` to `.format("parquet")`
- **Database sink**: Use `foreachBatch()` for custom database writes
- **Kafka output**: Write to another Kafka topic for downstream processing

## License

See LICENSE file for details.

## Support

For issues or questions, please open an issue in the repository.
