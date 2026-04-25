"""Parser compatibility against a real Spark 3.5.0 event log (SparkPi job)."""

import pathlib

from ignis.parser.event_log import parse_event_log

FIXTURE = str(pathlib.Path(__file__).parent.parent / "fixtures" / "spark35_compat.ndjson")


def test_spark35_parser_loads_app_metadata():
    app = parse_event_log(FIXTURE)
    assert app.app_name == "Spark Pi"
    assert app.app_id.startswith("local-")


def test_spark35_parser_loads_stages():
    app = parse_event_log(FIXTURE)
    assert len(app.stages) > 0


def test_spark35_parser_loads_executor_cores():
    app = parse_event_log(FIXTURE)
    assert app.executor_cores > 0


def test_spark35_parser_loads_task_metrics():
    app = parse_event_log(FIXTURE)
    for stage in app.stages.values():
        successful = stage.successful_tasks
        assert len(successful) > 0
        assert all(t.metrics is not None for t in successful)
