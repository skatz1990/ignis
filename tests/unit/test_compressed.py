import pathlib

from ignis.parser.event_log import parse_event_log
from ignis.rules.skew import DataSkewRule

_FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures"


def test_gz_parses_app_metadata():
    app = parse_event_log(str(_FIXTURES / "skew_example.ndjson.gz"))
    assert app.app_name == "ignis-test-app"
    assert app.app_id == "application_test_001"


def test_gz_produces_same_findings_as_uncompressed():
    compressed = parse_event_log(str(_FIXTURES / "skew_example.ndjson.gz"))
    uncompressed = parse_event_log(str(_FIXTURES / "skew_example.ndjson"))
    assert len(DataSkewRule().analyze(compressed)) == len(DataSkewRule().analyze(uncompressed))
    assert len(compressed.stages) == len(uncompressed.stages)
