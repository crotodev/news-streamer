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
        rows_iter: Iterable of dictionaries with keys `url_hash` and `text_for_inference`.
        endpoint: REST endpoint to POST the batch payload.
        batch_size: Maximum number of records per HTTP batch.
        timeout_s: Request timeout in seconds.

    Yields:
        Row objects containing url_hash, sentiment_label, sentiment_score, sentiment_inferred_at, sentiment_error.
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
                sentiment_inferred_at=inferred_at,
                sentiment_error=error_message,
            )

    def flush(batch) -> Iterator[Row]:
        if not batch:
            return iter(())

        # Filter out empty text items; yield error rows for them
        valid_items = []
        for item in batch:
            text = item.get("text_for_inference", "")
            if text and isinstance(text, str) and text.strip():
                valid_items.append(item)
            else:
                # Yield error row for empty text
                yield Row(
                    url_hash=item["url_hash"],
                    sentiment_label=None,
                    sentiment_score=None,
                    sentiment_inferred_at=_utc_now_iso(),
                    sentiment_error="empty_text_for_inference",
                )

        if not valid_items:
            return

        payload = {
            "items": [
                {"id": item["url_hash"], "text": item["text_for_inference"]}
                for item in valid_items
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

            for item in valid_items:
                result = results_by_id.get(item["url_hash"])
                if result:
                    yield Row(
                        url_hash=item["url_hash"],
                        sentiment_label=result.get("label"),
                        sentiment_score=result.get("score"),
                        sentiment_inferred_at=inferred_at,
                        sentiment_error=None,
                    )
                else:
                    yield Row(
                        url_hash=item["url_hash"],
                        sentiment_label=None,
                        sentiment_score=None,
                        sentiment_inferred_at=inferred_at,
                        sentiment_error="missing sentiment result",
                    )
        except Exception as exc:  # noqa: BLE001
            yield from emit_error_rows(valid_items, str(exc))

    for row in rows_iter:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield from flush(buffer)
            buffer.clear()

    if buffer:
        yield from flush(buffer)


def call_classify_api_partition(
    rows_iter: Iterable[Dict[str, str]],
    endpoint: str,
    batch_size: int = 25,
    timeout_s: int = 30,
) -> Iterator[Row]:
    """Call the classification API in batches per partition.

    Args:
        rows_iter: Iterable of dictionaries with keys `url_hash` and `text_for_inference`.
        endpoint: REST endpoint to POST the batch payload.
        batch_size: Maximum number of records per HTTP batch.
        timeout_s: Request timeout in seconds.

    Yields:
        Row objects containing url_hash, category_label, category_score, category_inferred_at, category_error.
    """

    session = requests.Session()
    buffer: list[Dict[str, str]] = []

    def emit_error_rows(batch, error_message: str) -> Iterator[Row]:
        inferred_at = _utc_now_iso()
        for item in batch:
            yield Row(
                url_hash=item["url_hash"],
                category_label=None,
                category_score=None,
                category_inferred_at=inferred_at,
                category_error=error_message,
            )

    def flush(batch) -> Iterator[Row]:
        if not batch:
            return iter(())

        # Filter out empty text items; yield error rows for them
        valid_items = []
        for item in batch:
            text = item.get("text_for_inference", "")
            if text and isinstance(text, str) and text.strip():
                valid_items.append(item)
            else:
                # Yield error row for empty text
                yield Row(
                    url_hash=item["url_hash"],
                    category_label=None,
                    category_score=None,
                    category_inferred_at=_utc_now_iso(),
                    category_error="empty_text_for_inference",
                )

        if not valid_items:
            return

        payload = {
            "items": [
                {"id": item["url_hash"], "text": item["text_for_inference"]}
                for item in valid_items
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

            for item in valid_items:
                result = results_by_id.get(item["url_hash"])
                if result:
                    yield Row(
                        url_hash=item["url_hash"],
                        category_label=result.get("label"),
                        category_score=result.get("score"),
                        category_inferred_at=inferred_at,
                        category_error=None,
                    )
                else:
                    yield Row(
                        url_hash=item["url_hash"],
                        category_label=None,
                        category_score=None,
                        category_inferred_at=inferred_at,
                        category_error="missing classification result",
                    )
        except Exception as exc:  # noqa: BLE001
            yield from emit_error_rows(valid_items, str(exc))

    for row in rows_iter:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield from flush(buffer)
            buffer.clear()

    if buffer:
        yield from flush(buffer)
