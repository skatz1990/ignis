import pathlib

import pytest

from ignis.parser.event_log import parse_event_log
from ignis.rules.gc_pressure import GCPressureRule

_FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures"
FIXTURE = str(_FIXTURES / "gc_pressure_example.ndjson")


@pytest.fixture
def app():
    return parse_event_log(FIXTURE)


def test_gc_pressure_fires(app):
    assert len(GCPressureRule().analyze(app)) == 1


def test_gc_pressure_message_contains_ratio(app):
    finding = GCPressureRule().analyze(app)[0]
    assert "25%" in finding.message


def test_high_threshold_suppresses(app):
    assert len(GCPressureRule(gc_ratio=0.50).analyze(app)) == 0


def test_low_threshold_still_fires(app):
    assert len(GCPressureRule(gc_ratio=0.05).analyze(app)) >= 1


def test_describe_reflects_custom_threshold():
    rule = GCPressureRule(gc_ratio=0.20)
    assert "20%" in rule.describe().threshold
