import json

import pytest
from pyspark.sql import Row

from ollama_client import (
    REQUIRED_ANALYSIS_FIELDS,
    analyze_article,
    call_ollama_analysis_partition,
    chunk_text,
)


class FakeResponse:
    def __init__(self, content=None, error=None, status_error=None):
        self.content = content
        self.error = error
        self.status_error = status_error

    def raise_for_status(self):
        if self.status_error:
            raise self.status_error

    def json(self):
        if self.error:
            return {"error": self.error}
        return {"message": {"content": json.dumps(self.content)}}


class MalformedJsonResponse(FakeResponse):
    def json(self):
        return {"message": {"content": "not json"}}


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def analysis(summary="The article reports a verified event."):
    return {
        "summary": summary,
        "key_points": ["A concrete detail"],
        "entities": ["Example Organization"],
        "sentiment": "neutral",
        "caveats": [],
    }


def test_short_article_uses_one_json_chat_request_and_returns_valid_result():
    session = FakeSession([FakeResponse(analysis())])

    result = analyze_article(
        "hash-1",
        "Full article body, not its short inference summary.",
        base_url="http://ollama:11434/",
        model="qwen-local",
        timeout_s=41,
        chunk_chars=500,
        num_ctx=4096,
        keep_alive="2m",
        session=session,
    )

    assert len(session.calls) == 1
    url, kwargs = session.calls[0]
    assert url == "http://ollama:11434/api/chat"
    assert kwargs["timeout"] == 41
    assert kwargs["json"]["model"] == "qwen-local"
    assert kwargs["json"]["format"] == "json"
    assert kwargs["json"]["options"]["temperature"] == 0
    assert kwargs["json"]["options"]["num_ctx"] == 4096
    assert kwargs["json"]["options"]["num_predict"] == 512
    assert kwargs["json"]["keep_alive"] == "2m"
    assert "Full article body" in kwargs["json"]["messages"][-1]["content"]
    assert result["url_hash"] == "hash-1"
    assert json.loads(result["llm_analysis"]) == analysis()
    assert result["llm_model"] == "qwen-local"
    assert result["llm_inferred_at"]
    assert result["llm_error"] is None


def test_long_article_analyzes_all_chunks_then_synthesizes_once():
    text = "alpha beta gamma delta epsilon zeta eta theta"
    chunks = chunk_text(text, 12)
    session = FakeSession(
        [FakeResponse(analysis(f"Observation {i}")) for i in range(len(chunks))]
        + [FakeResponse(analysis("Combined analysis"))]
    )

    result = analyze_article(
        "hash-long",
        text,
        base_url="http://localhost:11434",
        model="local",
        timeout_s=10,
        chunk_chars=12,
        num_ctx=2048,
        keep_alive="5m",
        session=session,
    )

    assert len(chunks) > 1
    assert len(session.calls) == len(chunks) + 1
    sent = [call[1]["json"]["messages"][-1]["content"] for call in session.calls]
    assert all(chunk in " ".join(sent[:-1]) for chunk in chunks)
    assert all(f"Observation {i}" in sent[-1] for i in range(len(chunks)))
    assert json.loads(result["llm_analysis"]) == analysis("Combined analysis")
    assert result["url_hash"] == "hash-long"


def test_chunking_preserves_whitespace_normalized_words_without_loss():
    text = "alpha  beta\n gamma\tdelta epsilon"
    chunks = chunk_text(text, 11)
    assert all(len(chunk) <= 11 for chunk in chunks)
    assert " ".join(chunks) == " ".join(text.split())


@pytest.mark.parametrize(
    "text,response,expected_error",
    [
        (" \n ", None, "empty_article_text"),
        ("body", FakeResponse(status_error=RuntimeError("HTTP 503")), "HTTP 503"),
        ("body", FakeResponse(error="model unavailable"), "model unavailable"),
        ("body", MalformedJsonResponse(), "invalid_json"),
    ],
)
def test_article_failures_are_returned_as_error_metadata(
    text, response, expected_error
):
    session = FakeSession([response] if response is not None else [])
    result = analyze_article(
        "hash-error",
        text,
        base_url="http://localhost:11434",
        model="local",
        timeout_s=10,
        chunk_chars=100,
        num_ctx=1024,
        keep_alive="5m",
        session=session,
    )
    assert result["url_hash"] == "hash-error"
    assert result["llm_analysis"] is None
    assert result["llm_error"]
    assert expected_error.casefold() in result["llm_error"].casefold()


@pytest.mark.parametrize(
    "payload",
    [
        {"summary": "only"},
        {
            "summary": "",
            "key_points": [],
            "entities": [],
            "sentiment": "neutral",
            "caveats": [],
        },
    ],
)
def test_missing_or_empty_required_fields_are_rejected(payload):
    session = FakeSession([FakeResponse(payload)])
    result = analyze_article(
        "hash-invalid",
        "body",
        base_url="http://localhost:11434",
        model="local",
        timeout_s=10,
        chunk_chars=100,
        num_ctx=1024,
        keep_alive="5m",
        session=session,
    )
    assert result["llm_analysis"] is None
    assert result["llm_error"]
    assert "field" in result["llm_error"].casefold()


def test_partition_adapter_keeps_each_url_hash_and_isolates_articles():
    session_responses = [
        FakeResponse(analysis()),
        RuntimeError("connection reset"),
        FakeResponse(analysis("third")),
    ]
    session = FakeSession(session_responses)

    rows = list(
        call_ollama_analysis_partition(
            iter(
                [
                    {"url_hash": "one", "text_for_llm": "first"},
                    {"url_hash": "two", "text_for_llm": "second"},
                    {"url_hash": "three", "text_for_llm": "third"},
                ]
            ),
            base_url="http://localhost:11434",
            model="local",
            timeout_s=10,
            chunk_chars=100,
            num_ctx=1024,
            keep_alive="5m",
            session=session,
        )
    )

    assert [row.url_hash for row in rows] == ["one", "two", "three"]
    assert isinstance(rows[0], Row)
    assert rows[1].llm_error
    assert rows[0].llm_error is None
    assert rows[2].llm_analysis
    assert set(REQUIRED_ANALYSIS_FIELDS) == set(analysis())



def test_chunking_rejects_an_oversized_token_without_splitting_it():
    with pytest.raises(ValueError, match="article_token_exceeds_chunk_budget"):
        chunk_text("prefix " + "x" * 25 + " suffix", 8)


def test_oversized_article_token_returns_an_explicit_error_before_http():
    session = FakeSession([FakeResponse(analysis())] * 10)
    result = analyze_article(
        "hash-token", "prefix " + "x" * 25 + " suffix",
        base_url="http://localhost:11434", model="local", timeout_s=10,
        chunk_chars=8, num_ctx=4096, keep_alive="5m", session=session,
    )
    assert result["llm_analysis"] is None
    assert "article_token_exceeds_chunk_budget" in result["llm_error"]
    assert session.calls == []


def test_oversized_synthesis_returns_error_without_sending_an_over_budget_prompt():
    text = "alpha beta gamma delta"
    chunks = chunk_text(text, 10)
    session = FakeSession(
        [FakeResponse(analysis("observation " * 600)) for _ in chunks]
        + [FakeResponse(analysis("must not be used"))]
    )
    result = analyze_article(
        "hash-synthesis", text,
        base_url="http://localhost:11434", model="local", timeout_s=10,
        chunk_chars=10, num_ctx=4096, keep_alive="5m", session=session,
    )
    assert len(session.calls) == len(chunks)
    sent = [call[1]["json"]["messages"][-1]["content"] for call in session.calls]
    assert all(chunk in " ".join(sent) for chunk in chunks)
    assert result["llm_analysis"] is None
    assert "context_budget_exceeded" in result["llm_error"]


def test_article_prompt_budget_counts_unicode_bytes_before_http():
    session = FakeSession([FakeResponse(analysis())])
    result = analyze_article(
        "hash-unicode", "\u754c" * 1000,
        base_url="http://localhost:11434", model="local", timeout_s=10,
        chunk_chars=2000, num_ctx=2048, keep_alive="5m", session=session,
    )
    assert session.calls == []
    assert result["llm_analysis"] is None
    assert "context_budget_exceeded" in result["llm_error"]
