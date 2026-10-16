import datetime
from typing import Any, Dict, Iterable, Iterator, Optional

import requests
from pyspark.sql import Row


def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _to_float(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def _call_api_partition(
    rows_iter: Iterable[Dict[str, str]],
    endpoint: str,
    batch_size: int,
    timeout_s: int,
    prefix: str,
    label_key: str,
    score_key: str,
) -> Iterator[Row]:
    """POST rows to a model-API batch endpoint and yield one Row per input.

    Request items are ``{"url_hash", "text"}``; results are matched back by
    ``url_hash``. Output Row fields are ``url_hash, {prefix}_label,
    {prefix}_score, {prefix}_inferred_at, {prefix}_error`` in that order.
    """

    session = requests.Session()

    def make_row(
        url_hash: str,
        inferred_at: str,
        label: Optional[str] = None,
        score: Optional[float] = None,
        error: Optional[str] = None,
    ) -> Row:
        return Row(
            **{
                "url_hash": url_hash,
                f"{prefix}_label": label,
                f"{prefix}_score": score,
                f"{prefix}_inferred_at": inferred_at,
                f"{prefix}_error": error,
            }
        )

    def flush(batch) -> Iterator[Row]:
        valid_items = []
        for item in batch:
            text = item.get("text_for_inference")
            if isinstance(text, str) and text.strip():
                valid_items.append(item)
            else:
                yield make_row(
                    item["url_hash"], _utc_now_iso(), error="empty_text_for_inference"
                )

        if not valid_items:
            return

        payload = {
            "items": [
                {"url_hash": item["url_hash"], "text": item["text_for_inference"]}
                for item in valid_items
            ]
        }
        inferred_at = _utc_now_iso()

        try:
            response = session.post(endpoint, json=payload, timeout=timeout_s)
            response.raise_for_status()
            body = response.json()
        except Exception as exc:  # noqa: BLE001
            for item in valid_items:
                yield make_row(item["url_hash"], inferred_at, error=str(exc))
            return

        results = body.get("results", []) if isinstance(body, dict) else []
        results_by_hash = {
            entry.get("url_hash"): entry
            for entry in results
            if isinstance(entry, dict) and entry.get("url_hash") is not None
        }

        for item in valid_items:
            result = results_by_hash.get(item["url_hash"])
            if result is None:
                yield make_row(
                    item["url_hash"], inferred_at, error=f"missing {prefix} result"
                )
            else:
                yield make_row(
                    item["url_hash"],
                    result.get("inferred_at") or inferred_at,
                    label=result.get(label_key),
                    score=_to_float(result.get(score_key)),
                    error=result.get("error"),
                )

    buffer: list[Dict[str, str]] = []
    for row in rows_iter:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield from flush(buffer)
            buffer = []

    if buffer:
        yield from flush(buffer)


def call_sentiment_api_partition(
    rows_iter: Iterable[Dict[str, str]],
    endpoint: str,
    batch_size: int = 25,
    timeout_s: int = 30,
) -> Iterator[Row]:
    """Call ``POST /api/sentiment/batch`` in batches for one Spark partition.

    Yields Rows: url_hash, sentiment_label, sentiment_score,
    sentiment_inferred_at, sentiment_error.
    """
    return _call_api_partition(
        rows_iter,
        endpoint,
        batch_size,
        timeout_s,
        prefix="sentiment",
        label_key="sentiment_label",
        score_key="sentiment_score",
    )


def call_classify_api_partition(
    rows_iter: Iterable[Dict[str, str]],
    endpoint: str,
    batch_size: int = 25,
    timeout_s: int = 30,
) -> Iterator[Row]:
    """Call ``POST /api/topic/batch`` in batches for one Spark partition.

    The API's ``topic_label``/``topic_score`` are mapped to the streamer's
    ``category_*`` columns. Yields Rows: url_hash, category_label,
    category_score, category_inferred_at, category_error.
    """
    return _call_api_partition(
        rows_iter,
        endpoint,
        batch_size,
        timeout_s,
        prefix="category",
        label_key="topic_label",
        score_key="topic_score",
    )
