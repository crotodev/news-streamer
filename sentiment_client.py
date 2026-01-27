import datetime
from typing import Dict, Iterable, Iterator

import requests
from pyspark.sql import Row


def _utc_now_iso() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"


def call_sentiment_api_partition(
    rows_iter: Iterable[Dict[str, str]],
    endpoint: str,
    batch_size: int = 25,
    timeout_s: int = 30,
) -> Iterator[Row]:
    """Call the sentiment API in batches per partition.

    Args:
        rows_iter: Iterable of dictionaries with keys `url_hash` and `text_prepped`.
        endpoint: REST endpoint to POST the batch payload.
        batch_size: Maximum number of records per HTTP batch.
        timeout_s: Request timeout in seconds.

    Yields:
        Row objects containing url_hash, sentiment_label, sentiment_score, inferred_at, error.
    """

    session = requests.Session()
    buffer: list[Dict[str, str]] = []

    def emit_error_rows(batch, error_message: str) -> Iterator[Row]:
        inferred_at = _utc_now_iso()
        for item in batch:
            yield Row(
                url_hash=item["url_hash"],
                sentiment_label=None,
                sentiment_score=None,
                inferred_at=inferred_at,
                error=error_message,
            )

    def flush(batch) -> Iterator[Row]:
        if not batch:
            return iter(())

        payload = {
            "items": [
                {"id": item["url_hash"], "text": item["text_prepped"]} for item in batch
            ]
        }
        inferred_at = _utc_now_iso()

        try:
            response = session.post(endpoint, json=payload, timeout=timeout_s)
            response.raise_for_status()
            body = response.json()
            results = body.get("results", []) if isinstance(body, dict) else []
            results_by_id = {
                entry.get("id"): entry
                for entry in results
                if isinstance(entry, dict) and entry.get("id") is not None
            }

            for item in batch:
                result = results_by_id.get(item["url_hash"])
                if result:
                    yield Row(
                        url_hash=item["url_hash"],
                        sentiment_label=result.get("label"),
                        sentiment_score=result.get("score"),
                        inferred_at=inferred_at,
                        error=None,
                    )
                else:
                    yield Row(
                        url_hash=item["url_hash"],
                        sentiment_label=None,
                        sentiment_score=None,
                        inferred_at=inferred_at,
                        error="missing sentiment result",
                    )
        except Exception as exc:  # noqa: BLE001
            yield from emit_error_rows(batch, str(exc))

    for row in rows_iter:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield from flush(buffer)
            buffer.clear()

    if buffer:
        yield from flush(buffer)
