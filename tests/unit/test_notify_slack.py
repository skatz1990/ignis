import json
from unittest.mock import MagicMock, patch

import pytest

from ignis.notify.slack import _build_payload, post

FINDINGS_DATA = {
    "app_id": "application_test_001",
    "app_name": "ignis-test-app",
    "finding_count": 1,
    "findings": [
        {
            "rule": "data-skew",
            "severity": "warning",
            "stage_id": 0,
            "stage_name": "groupBy at job.py:42",
            "message": "Stage 0: max 800ms vs median 105ms (7.6x)",
            "recommendation": "Repartition before the shuffle.",
        }
    ],
}

CLEAN_DATA = {
    "app_id": "application_test_002",
    "app_name": "clean-app",
    "finding_count": 0,
    "findings": [],
}


def test_payload_header_contains_app_name():
    payload = _build_payload(FINDINGS_DATA)
    header = payload["blocks"][0]
    assert header["type"] == "header"
    assert "ignis-test-app" in header["text"]["text"]
    assert "1 issue" in header["text"]["text"]


def test_payload_contains_finding_section():
    payload = _build_payload(FINDINGS_DATA)
    sections = [b for b in payload["blocks"] if b["type"] == "section"]
    assert len(sections) == 1
    text = sections[0]["text"]["text"]
    assert "data-skew" in text
    assert "WARNING" in text
    assert "Repartition before the shuffle." in text


def test_payload_no_trailing_divider():
    payload = _build_payload(FINDINGS_DATA)
    assert payload["blocks"][-1]["type"] != "divider"


def test_payload_clean_run_has_no_sections():
    payload = _build_payload(CLEAN_DATA)
    sections = [b for b in payload["blocks"] if b["type"] == "section"]
    assert len(sections) == 0


def test_post_calls_webhook():
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
        post("https://hooks.slack.com/test", FINDINGS_DATA)
        mock_open.assert_called_once()
        req = mock_open.call_args[0][0]
        body = json.loads(req.data)
        assert "blocks" in body


def test_post_raises_on_http_error():
    import urllib.error

    with patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.HTTPError(None, 400, "Bad Request", {}, None),
    ):
        with pytest.raises(RuntimeError, match="400"):
            post("https://hooks.slack.com/test", FINDINGS_DATA)
