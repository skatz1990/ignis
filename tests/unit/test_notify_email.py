import smtplib
from unittest.mock import MagicMock, patch

import pytest

from ignis.notify.email import _build_body, _build_subject, send

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


def test_subject_with_findings():
    assert "1 issue" in _build_subject(FINDINGS_DATA)
    assert "ignis-test-app" in _build_subject(FINDINGS_DATA)


def test_subject_clean_run():
    assert "no issues" in _build_subject(CLEAN_DATA)
    assert "clean-app" in _build_subject(CLEAN_DATA)


def test_plain_body_contains_finding():
    plain, _ = _build_body(FINDINGS_DATA)
    assert "data-skew" in plain
    assert "Repartition before the shuffle." in plain


def test_html_body_contains_finding():
    _, html = _build_body(FINDINGS_DATA)
    assert "data-skew" in html
    assert "Repartition before the shuffle." in html


def test_html_clean_run():
    _, html = _build_body(CLEAN_DATA)
    assert "No issues found" in html


def test_send_calls_smtp():
    mock_smtp = MagicMock()
    mock_smtp.__enter__ = lambda s: s
    mock_smtp.__exit__ = MagicMock(return_value=False)

    with patch("smtplib.SMTP", return_value=mock_smtp):
        send(
            FINDINGS_DATA,
            to="to@example.com",
            sender="from@example.com",
            smtp_host="smtp.example.com",
            smtp_port=587,
            username="user",
            password="pass",
        )
        mock_smtp.sendmail.assert_called_once()
        args = mock_smtp.sendmail.call_args[0]
        assert args[0] == "from@example.com"
        assert args[1] == "to@example.com"


def test_send_raises_on_smtp_error():
    with patch("smtplib.SMTP", side_effect=smtplib.SMTPException("connection refused")):
        with pytest.raises(smtplib.SMTPException):
            send(
                FINDINGS_DATA,
                to="to@example.com",
                sender="from@example.com",
                smtp_host="bad-host",
                smtp_port=587,
                username=None,
                password=None,
            )
