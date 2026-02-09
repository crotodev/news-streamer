from unittest.mock import Mock, patch

import api_client


def _mock_response(results):
    response = Mock()
    response.raise_for_status = Mock()
    response.json = Mock(return_value={"results": results})
    return response


def test_call_sentiment_api_partition_handles_missing_results():
    rows = [
        {"url_hash": "a", "text_for_inference": "good"},
        {"url_hash": "b", "text_for_inference": "bad"},
    ]
    response = _mock_response(
        [
            {
                "url_hash": "a",
                "sentiment_label": "positive",
                "sentiment_score": 0.9,
            }
        ]
    )
    mock_session = Mock()
    mock_session.post.return_value = response

    with patch("api_client.requests.Session", return_value=mock_session), patch(
        "api_client._utc_now_iso", return_value="2026-02-09T00:00:00Z"
    ):
        output = list(
            api_client.call_sentiment_api_partition(
                rows,
                endpoint="http://example.test",
                batch_size=10,
                timeout_s=1,
            )
        )

    by_hash = {row.url_hash: row for row in output}
    assert by_hash["a"].sentiment_label == "positive"
    assert by_hash["a"].sentiment_error is None
    assert by_hash["b"].sentiment_error == "missing sentiment result"


def test_call_sentiment_api_partition_handles_empty_text():
    rows = [
        {"url_hash": "a", "text_for_inference": "  "},
        {"url_hash": "b", "text_for_inference": "valid"},
    ]
    response = _mock_response(
        [
            {
                "url_hash": "b",
                "sentiment_label": "neutral",
                "sentiment_score": 0.5,
            }
        ]
    )
    mock_session = Mock()
    mock_session.post.return_value = response

    with patch("api_client.requests.Session", return_value=mock_session), patch(
        "api_client._utc_now_iso", return_value="2026-02-09T00:00:00Z"
    ):
        output = list(
            api_client.call_sentiment_api_partition(
                rows,
                endpoint="http://example.test",
                batch_size=10,
                timeout_s=1,
            )
        )

    by_hash = {row.url_hash: row for row in output}
    assert by_hash["a"].sentiment_error == "empty_text_for_inference"
    assert by_hash["b"].sentiment_label == "neutral"

    payload = mock_session.post.call_args.kwargs["json"]
    assert payload["items"] == [{"url_hash": "b", "text": "valid"}]


def test_call_classify_api_partition_emits_error_on_exception():
    rows = [
        {"url_hash": "a", "text_for_inference": "alpha"},
        {"url_hash": "b", "text_for_inference": "beta"},
    ]
    mock_session = Mock()
    mock_session.post.side_effect = Exception("boom")

    with patch("api_client.requests.Session", return_value=mock_session), patch(
        "api_client._utc_now_iso", return_value="2026-02-09T00:00:00Z"
    ):
        output = list(
            api_client.call_classify_api_partition(
                rows,
                endpoint="http://example.test",
                batch_size=10,
                timeout_s=1,
            )
        )

    by_hash = {row.url_hash: row for row in output}
    assert by_hash["a"].category_error == "boom"
    assert by_hash["b"].category_error == "boom"
