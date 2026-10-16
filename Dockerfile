# News Streamer - PySpark Structured Streaming (requires Java 17)
FROM python:3.11-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    JAVA_HOME=/opt/java

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY *.py config.yaml ./

# ---------------------------------------------------------------------------
FROM base AS test
COPY requirements-dev.txt pytest.ini ./
RUN pip install -r requirements-dev.txt
COPY tests/ tests/
CMD ["python", "-m", "pytest"]

# ---------------------------------------------------------------------------
FROM base AS production
CMD ["python", "stream.py"]
