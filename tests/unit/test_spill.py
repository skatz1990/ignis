import pathlib

from ignis.parser.event_log import parse_event_log
from ignis.rules.base import Severity
from ignis.rules.spill import MEMORY_SPILL_THRESHOLD_BYTES, SpillRule

FIXTURE = str(pathlib.Path(__file__).parent.parent / "fixtures" / "spill_example.ndjson")

# Stage 0 disk spill: 209_715_200 + 157_286_400 + 52_428_800 = 419_430_400 bytes (400 MB)
# Stage 0 worst task: Task 0 at 209_715_200 bytes (200 MB)
# Stage 1 memory spill: 314_572_800 + 293_601_280 = 608_174_080 bytes (~580 MB, above threshold)
# Stage 2: no spill


def test_spill_rule_finds_two_findings():
    app = parse_event_log(FIXTURE)
    findings = SpillRule().analyze(app)
    assert len(findings) == 2


def test_disk_spill_finding_is_warning():
    app = parse_event_log(FIXTURE)
    findings = SpillRule().analyze(app)
    disk_findings = [f for f in findings if f.severity == Severity.WARNING]
    assert len(disk_findings) == 1
    assert disk_findings[0].stage_id == 0
    assert disk_findings[0].rule == "spill"


def test_memory_spill_finding_is_info():
    app = parse_event_log(FIXTURE)
    findings = SpillRule().analyze(app)
    info_findings = [f for f in findings if f.severity == Severity.INFO]
    assert len(info_findings) == 1
    assert info_findings[0].stage_id == 1
    assert info_findings[0].rule == "spill"


def test_disk_spill_message_contains_worst_task():
    app = parse_event_log(FIXTURE)
    findings = SpillRule().analyze(app)
    disk_finding = next(f for f in findings if f.severity == Severity.WARNING)
    # Task 0 is the worst disk spiller (200 MB)
    assert "task 0" in disk_finding.message.lower()


def test_disk_spill_message_contains_task_count():
    app = parse_event_log(FIXTURE)
    findings = SpillRule().analyze(app)
    disk_finding = next(f for f in findings if f.severity == Severity.WARNING)
    assert "3 task" in disk_finding.message


def test_clean_stage_produces_no_finding():
    app = parse_event_log(FIXTURE)
    findings = SpillRule().analyze(app)
    flagged_ids = {f.stage_id for f in findings}
    assert 2 not in flagged_ids


def test_memory_spill_threshold():
    # Stage 1 total memory spill (~580 MB) must exceed the threshold.
    assert 608_174_080 >= MEMORY_SPILL_THRESHOLD_BYTES
