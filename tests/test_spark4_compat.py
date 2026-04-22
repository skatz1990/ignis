"""Parser and skew-rule compatibility against a real Spark 4.0.2 event log."""

import pathlib

from ignis.parser.event_log import parse_event_log
from ignis.rules.skew import SKEW_RATIO_THRESHOLD, DataSkewRule

FIXTURE = str(pathlib.Path(__file__).parent / "fixtures" / "spark4_skew_example.ndjson")


def test_spark4_parser_loads_app_metadata():
    app = parse_event_log(FIXTURE)
    assert app.app_name == "ignis-skew-test"
    assert app.app_id.startswith("local-")


def test_spark4_parser_loads_stages():
    app = parse_event_log(FIXTURE)
    assert len(app.stages) > 0


def test_spark4_skew_rule_detects_finding():
    app = parse_event_log(FIXTURE)
    findings = DataSkewRule().analyze(app)
    assert len(findings) >= 1


def test_spark4_skew_ratio_exceeds_threshold():
    import statistics

    app = parse_event_log(FIXTURE)
    findings = DataSkewRule().analyze(app)
    assert len(findings) >= 1
    skewed_stage = app.stages[(findings[0].stage_id, 0)]
    durations = [t.metrics.duration_ms for t in skewed_stage.successful_tasks]
    ratio = max(durations) / statistics.median(durations)
    assert ratio >= SKEW_RATIO_THRESHOLD
