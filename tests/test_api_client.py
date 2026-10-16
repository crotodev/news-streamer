"""Contract tests for the model-API partition callers."""

import importlib

import pytest
import requests

import api_client


class FakeResponse:
    def __init__(self, body, status: int = 200) -> None:
        self.body = body
        self.status = status

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise requests.HTTPError(f"{self.status} error")

    def json(self):
        return self.body


class FakeSession:
    def __init__(self, handler) -> None:
        self.handler = handler
        self.calls = []

    def post(self, url, json, timeout):
        self.calls.append((url, json))
        return self.handler(url, json)


@pytest.fixture
def fake_session(monkeypatch):
    holder = {}

    def install(handler):
        session = FakeSession(handler)
        monkeypatch.setattr(api_client.requests, "Session", lambda: session)
        holder["session"] = session
        return session

    return install


def test_sentiment_request_uses_url_hash_and_text(fake_session) -> None:
    session = fake_session(lambda url, body: FakeResponse({"results": []}))

    list(
        api_client.call_sentiment_api_partition(
            [{"url_hash": "h1", "text_for_inference": "good news"}],
            endpoint="http://api/api/sentiment/batch",
        )
    )

    assert session.calls == [
        (
            "http://api/api/sentiment/batch",
            {"items": [{"url_hash": "h1", "text": "good news"}]},
        )
    ]


def test_sentiment_response_fields_are_mapped(fake_session) -> None:
    fake_session(
        lambda url, body: FakeResponse(
            {
                "results": [
                    {
                        "url_hash": "h1",
                        "sentiment_label": "positive",
                        "sentiment_score": 0.98,
                        "inferred_at": "2026-10-16T00:00:00+00:00",
                        "error": None,
                    }
                ]
            }
        )
    )

    [row] = api_client.call_sentiment_api_partition(
        [{"url_hash": "h1", "text_for_inference": "good news"}], endpoint="x"
    )

    assert row.asDict() == {
        "url_hash": "h1",
        "sentiment_label": "positive",
        "sentiment_score": 0.98,
        "sentiment_inferred_at": "2026-10-16T00:00:00+00:00",
        "sentiment_error": None,
    }


def test_per_item_api_error_is_propagated(fake_session) -> None:
    fake_session(
        lambda url, body: FakeResponse(
            {
                "results": [
                    {
                        "url_hash": "h1",
                        "sentiment_label": None,
                        "sentiment_score": None,
                        "inferred_at": "t",
                        "error": "empty text after normalization",
                    }
                ]
            }
        )
    )

    [row] = api_client.call_sentiment_api_partition(
        [{"url_hash": "h1", "text_for_inference": "x"}], endpoint="x"
    )

    assert row.sentiment_label is None
    assert row.sentiment_error == "empty text after normalization"


def test_topic_response_maps_to_category_columns(fake_session) -> None:
    fake_session(
        lambda url, body: FakeResponse(
            {
                "results": [
                    {
                        "url_hash": "h1",
                        "topic_label": "Company",
                        "topic_score": 1,
                        "top_k": [],
                        "inferred_at": "t",
                        "error": None,
                    }
                ]
            }
        )
    )

    [row] = api_client.call_classify_api_partition(
        [{"url_hash": "h1", "text_for_inference": "Apple Inc."}], endpoint="x"
    )

    assert list(row.asDict()) == [
        "url_hash",
        "category_label",
        "category_score",
        "category_inferred_at",
        "category_error",
    ]
    assert row.category_label == "Company"
    assert row.category_score == 1.0
    assert isinstance(row.category_score, float)


def test_http_failure_marks_every_item(fake_session) -> None:
    fake_session(lambda url, body: FakeResponse({}, status=500))

    rows = list(
        api_client.call_sentiment_api_partition(
            [
                {"url_hash": "h1", "text_for_inference": "a"},
                {"url_hash": "h2", "text_for_inference": "b"},
            ],
            endpoint="x",
        )
    )

    assert [r.url_hash for r in rows] == ["h1", "h2"]
    assert all(r.sentiment_error and "500" in r.sentiment_error for r in rows)


def test_blank_text_is_not_sent(fake_session) -> None:
    session = fake_session(lambda url, body: FakeResponse({"results": []}))

    [row] = api_client.call_sentiment_api_partition(
        [{"url_hash": "h1", "text_for_inference": "   "}], endpoint="x"
    )

    assert session.calls == []
    assert row.sentiment_error == "empty_text_for_inference"


def test_batches_are_split_by_batch_size(fake_session) -> None:
    session = fake_session(lambda url, body: FakeResponse({"results": []}))

    list(
        api_client.call_sentiment_api_partition(
            [{"url_hash": f"h{i}", "text_for_inference": "t"} for i in range(5)],
            endpoint="x",
            batch_size=2,
        )
    )

    assert [len(body["items"]) for _, body in session.calls] == [2, 2, 1]


def test_default_endpoints_match_model_api(monkeypatch) -> None:
    for name in (
        "API_BASE_URL",
        "SENTIMENT_ENDPOINT",
        "CLASSIFY_ENDPOINT",
        "API_HEALTH_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    import stream

    stream = importlib.reload(stream)

    assert stream.SENTIMENT_ENDPOINT == "http://localhost:9000/api/sentiment/batch"
    assert stream.CLASSIFY_ENDPOINT == "http://localhost:9000/api/topic/batch"
    assert stream.API_HEALTH_ENDPOINT == "http://localhost:9000/ready"
