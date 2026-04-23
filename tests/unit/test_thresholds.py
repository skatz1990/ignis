import pathlib

from ignis.parser.event_log import parse_event_log
from ignis.rules.partition import PartitionCountRule
from ignis.rules.shuffle import ShuffleSizeRule
from ignis.rules.skew import DataSkewRule
from ignis.rules.spill import SpillRule

_FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures"
SKEW_FIXTURE = str(_FIXTURES / "skew_example.ndjson")
SHUFFLE_FIXTURE = str(_FIXTURES / "shuffle_example.ndjson")
SPILL_FIXTURE = str(_FIXTURES / "spill_example.ndjson")
PARTITION_FIXTURE = str(_FIXTURES / "partition_example.ndjson")


# ── DataSkewRule ─────────────────────────────────────────────────────────────


def test_skew_default_threshold_fires():
    app = parse_event_log(SKEW_FIXTURE)
    assert len(DataSkewRule().analyze(app)) == 1


def test_skew_raised_threshold_suppresses_finding():
    app = parse_event_log(SKEW_FIXTURE)
    # Set ratio very high so no stage qualifies.
    assert len(DataSkewRule(skew_ratio=1000.0).analyze(app)) == 0


def test_skew_lowered_threshold_still_fires():
    app = parse_event_log(SKEW_FIXTURE)
    # Any ratio ≥ 1 will fire; default fixture has a large ratio.
    assert len(DataSkewRule(skew_ratio=1.1).analyze(app)) >= 1


def test_skew_describe_reflects_custom_threshold():
    rule = DataSkewRule(skew_ratio=3.0)
    assert "3.0" in rule.describe().threshold


# ── ShuffleSizeRule ──────────────────────────────────────────────────────────


def test_shuffle_default_threshold_fires():
    app = parse_event_log(SHUFFLE_FIXTURE)
    assert len(ShuffleSizeRule().analyze(app)) == 1


def test_shuffle_raised_threshold_suppresses_finding():
    app = parse_event_log(SHUFFLE_FIXTURE)
    # 1 TB threshold — no stage will reach it.
    assert len(ShuffleSizeRule(threshold_bytes=1_099_511_627_776).analyze(app)) == 0


def test_shuffle_lowered_threshold_fires_on_small_shuffle():
    app = parse_event_log(SHUFFLE_FIXTURE)
    # 1-byte threshold — everything fires.
    assert len(ShuffleSizeRule(threshold_bytes=1).analyze(app)) >= 1


def test_shuffle_describe_reflects_custom_threshold():
    rule = ShuffleSizeRule(threshold_bytes=2_147_483_648)  # 2 GB
    assert "2" in rule.describe().threshold


# ── SpillRule ────────────────────────────────────────────────────────────────


def test_spill_default_threshold_fires():
    app = parse_event_log(SPILL_FIXTURE)
    from ignis.rules.base import Severity

    findings = SpillRule().analyze(app)
    assert any(f.severity == Severity.INFO for f in findings)


def test_spill_raised_memory_threshold_suppresses_info():
    app = parse_event_log(SPILL_FIXTURE)
    from ignis.rules.base import Severity

    # 100 GB threshold — memory spill INFO won't fire.
    findings = SpillRule(memory_threshold_bytes=100 * 1_073_741_824).analyze(app)
    assert not any(f.severity == Severity.INFO for f in findings)


def test_spill_describe_reflects_custom_threshold():
    rule = SpillRule(memory_threshold_bytes=1_073_741_824)  # 1 GB
    assert "1024" in rule.describe().threshold  # 1024 MB


# ── PartitionCountRule ───────────────────────────────────────────────────────


def test_partition_default_threshold_fires():
    app = parse_event_log(PARTITION_FIXTURE)
    assert len(PartitionCountRule().analyze(app)) == 2


def test_partition_raised_max_suppresses_over_finding():
    app = parse_event_log(PARTITION_FIXTURE)
    # Raise the ceiling so 15,000 partitions no longer fires.
    findings = PartitionCountRule(max_partitions=20_000).analyze(app)
    flagged = {f.stage_id for f in findings}
    assert 2 not in flagged


def test_partition_lowered_min_suppresses_under_finding():
    app = parse_event_log(PARTITION_FIXTURE)
    # Require only 1 task per core — stage 1 with 4 partitions / 4 cores won't fire.
    findings = PartitionCountRule(min_tasks_per_core=1).analyze(app)
    flagged = {f.stage_id for f in findings}
    assert 1 not in flagged


def test_partition_describe_reflects_custom_thresholds():
    rule = PartitionCountRule(min_tasks_per_core=4, max_partitions=5_000)
    desc = rule.describe().threshold
    assert "4" in desc
    assert "5,000" in desc
