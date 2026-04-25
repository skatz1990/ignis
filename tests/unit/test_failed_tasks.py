import pathlib

import pytest

from ignis.parser.event_log import parse_event_log
from ignis.rules.base import Severity
from ignis.rules.failed_tasks import FailedTasksRule

_FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures"
FIXTURE = str(_FIXTURES / "failed_tasks_example.ndjson")


@pytest.fixture
def app():
    return parse_event_log(FIXTURE)


def test_failure_rate_fires(app):
    findings = FailedTasksRule().analyze(app)
    warnings = [f for f in findings if f.severity == Severity.WARNING]
    assert len(warnings) == 1


def test_speculation_rate_fires(app):
    findings = FailedTasksRule().analyze(app)
    infos = [f for f in findings if f.severity == Severity.INFO]
    assert len(infos) == 1


def test_failure_message_contains_counts(app):
    findings = FailedTasksRule().analyze(app)
    warning = next(f for f in findings if f.severity == Severity.WARNING)
    assert "2" in warning.message
    assert "10" in warning.message


def test_high_failure_threshold_suppresses(app):
    findings = FailedTasksRule(failure_rate=0.5).analyze(app)
    warnings = [f for f in findings if f.severity == Severity.WARNING]
    assert len(warnings) == 0


def test_high_speculation_threshold_suppresses(app):
    findings = FailedTasksRule(speculation_rate=0.5).analyze(app)
    infos = [f for f in findings if f.severity == Severity.INFO]
    assert len(infos) == 0


def test_describe_reflects_custom_thresholds():
    rule = FailedTasksRule(failure_rate=0.05, speculation_rate=0.50)
    desc = rule.describe().threshold
    assert "5%" in desc
    assert "50%" in desc
