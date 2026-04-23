import pathlib

from ignis.parser.event_log import parse_event_log
from ignis.rules.shuffle import SHUFFLE_WRITE_THRESHOLD_BYTES, ShuffleSizeRule

FIXTURE = str(pathlib.Path(__file__).parent.parent / "fixtures" / "shuffle_example.ndjson")

# Stage 0 shuffle write: 429_496_730 + 429_496_730 + 322_122_568 = 1_181_116_028 bytes (~1.1 GB)
STAGE0_TOTAL_BYTES = 429_496_730 + 429_496_730 + 322_122_568


def test_shuffle_rule_detects_one_finding():
    app = parse_event_log(FIXTURE)
    findings = ShuffleSizeRule().analyze(app)
    assert len(findings) == 1


def test_shuffle_finding_targets_correct_stage():
    app = parse_event_log(FIXTURE)
    finding = ShuffleSizeRule().analyze(app)[0]
    assert finding.stage_id == 0
    assert finding.rule == "shuffle-size"


def test_shuffle_stage0_total_exceeds_threshold():
    assert STAGE0_TOTAL_BYTES > SHUFFLE_WRITE_THRESHOLD_BYTES


def test_shuffle_stage1_below_threshold_produces_no_finding():
    app = parse_event_log(FIXTURE)
    findings = ShuffleSizeRule().analyze(app)
    flagged_ids = {f.stage_id for f in findings}
    assert 1 not in flagged_ids


def test_shuffle_finding_message_contains_gb():
    app = parse_event_log(FIXTURE)
    finding = ShuffleSizeRule().analyze(app)[0]
    assert "GB" in finding.message
