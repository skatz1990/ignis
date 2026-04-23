import pathlib

from ignis.parser.event_log import parse_event_log
from ignis.rules.base import Severity
from ignis.rules.partition import MAX_PARTITION_COUNT, MIN_TASKS_PER_CORE, PartitionCountRule

FIXTURE = str(pathlib.Path(__file__).parent.parent / "fixtures" / "partition_example.ndjson")

# Fixture layout:
#   Stage 0 (map, no shuffle read)        → skipped
#   Stage 1 (4 partitions, 4 cores)       → WARNING: under-partitioned (4 < 2×4=8)
#   Stage 2 (15000 partitions, 4 cores)   → WARNING: over-partitioned (15000 > 10_000)
#   Stage 3 (16 partitions, 4 cores)      → no finding (16 is within [8, 10000])


def test_executor_cores_parsed():
    app = parse_event_log(FIXTURE)
    assert app.executor_cores == 4


def test_finds_two_findings():
    app = parse_event_log(FIXTURE)
    findings = PartitionCountRule().analyze(app)
    assert len(findings) == 2


def test_under_partitioned_is_warning():
    app = parse_event_log(FIXTURE)
    findings = PartitionCountRule().analyze(app)
    under = [f for f in findings if f.stage_id == 1]
    assert len(under) == 1
    assert under[0].severity == Severity.WARNING
    assert under[0].rule == "partition-count"


def test_over_partitioned_is_warning():
    app = parse_event_log(FIXTURE)
    findings = PartitionCountRule().analyze(app)
    over = [f for f in findings if f.stage_id == 2]
    assert len(over) == 1
    assert over[0].severity == Severity.WARNING
    assert over[0].rule == "partition-count"


def test_map_stage_skipped():
    app = parse_event_log(FIXTURE)
    findings = PartitionCountRule().analyze(app)
    flagged_ids = {f.stage_id for f in findings}
    assert 0 not in flagged_ids


def test_well_partitioned_stage_no_finding():
    app = parse_event_log(FIXTURE)
    findings = PartitionCountRule().analyze(app)
    flagged_ids = {f.stage_id for f in findings}
    assert 3 not in flagged_ids


def test_under_partitioned_recommendation_contains_core_count():
    app = parse_event_log(FIXTURE)
    findings = PartitionCountRule().analyze(app)
    under = next(f for f in findings if f.stage_id == 1)
    expected_min = str(MIN_TASKS_PER_CORE * app.executor_cores)
    assert expected_min in under.recommendation


def test_over_partitioned_message_contains_partition_count():
    app = parse_event_log(FIXTURE)
    findings = PartitionCountRule().analyze(app)
    over = next(f for f in findings if f.stage_id == 2)
    assert f"{MAX_PARTITION_COUNT:,}" in over.message or "15,000" in over.message


def test_constants():
    assert MIN_TASKS_PER_CORE == 2
    assert MAX_PARTITION_COUNT == 10_000
