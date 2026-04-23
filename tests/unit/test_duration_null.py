import pathlib

from ignis.parser.event_log import parse_event_log

FIXTURE = str(pathlib.Path(__file__).parent.parent / "fixtures" / "duration_null.ndjson")


# Task 0: Launch=1700000001000, Finish=1700000001200 → 200ms
# Task 1: Launch=1700000001000, Finish=1700000001150 → 150ms
# Task 2: Launch=1700000001000, Finish=1700000001180 → 180ms
EXPECTED_DURATIONS = {0: 200, 1: 150, 2: 180}


def test_parser_loads_three_successful_tasks():
    app = parse_event_log(FIXTURE)
    assert len(app.stages[(0, 0)].successful_tasks) == 3


def test_duration_null_falls_back_to_wall_clock():
    app = parse_event_log(FIXTURE)
    tasks = {t.task_id: t for t in app.stages[(0, 0)].successful_tasks}
    for task_id, expected_ms in EXPECTED_DURATIONS.items():
        assert tasks[task_id].metrics.duration_ms == expected_ms, (
            f"Task {task_id}: expected {expected_ms}ms, got {tasks[task_id].metrics.duration_ms}ms"
        )


def test_duration_null_does_not_produce_zero_duration():
    app = parse_event_log(FIXTURE)
    for task in app.stages[(0, 0)].successful_tasks:
        assert task.metrics.duration_ms > 0
