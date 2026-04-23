import pathlib
import statistics

import pytest

from ignis.parser.event_log import parse_event_log
from ignis.rules.skew import SKEW_RATIO_THRESHOLD, DataSkewRule

FIXTURE = str(pathlib.Path(__file__).parent.parent / "fixtures" / "skew_example.ndjson")


@pytest.fixture(scope="module")
def app():
    return parse_event_log(FIXTURE)


def test_parser_loads_app_metadata(app):
    assert app.app_name == "ignis-test-app"
    assert app.app_id == "application_test_001"


def test_parser_loads_stage(app):
    assert (0, 0) in app.stages
    assert app.stages[(0, 0)].name == "groupBy at job.py:42"


def test_parser_loads_four_successful_tasks(app):
    assert len(app.stages[(0, 0)].successful_tasks) == 4


def test_parser_skips_unknown_events(app):
    # Fixture contains SparkListenerUnknownCustomEvent; parsing must not raise.
    assert app.app_name == "ignis-test-app"


def test_skew_rule_detects_one_finding(app):
    findings = DataSkewRule().analyze(app)
    assert len(findings) == 1


def test_skew_finding_targets_correct_stage(app):
    finding = DataSkewRule().analyze(app)[0]
    assert finding.stage_id == 0
    assert finding.rule == "data-skew"


def test_skew_ratio_exceeds_threshold(app):
    # Durations: [90, 100, 110, 800] → median=105, max=800, ratio≈7.6x
    durations = [t.metrics.duration_ms for t in app.stages[(0, 0)].successful_tasks]
    ratio = max(durations) / statistics.median(durations)
    assert ratio >= SKEW_RATIO_THRESHOLD
