import json
import pathlib

from ignis.parser.event_log import parse_event_log
from ignis.reporter.json_reporter import render_findings
from ignis.rules.partition import PartitionCountRule
from ignis.rules.skew import DataSkewRule

_FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures"
SKEW_FIXTURE = str(_FIXTURES / "skew_example.ndjson")
PARTITION_FIXTURE = str(_FIXTURES / "partition_example.ndjson")


def test_json_output_is_valid_json(capsys):
    app = parse_event_log(SKEW_FIXTURE)
    findings = DataSkewRule().analyze(app)
    render_findings(findings, app.app_id, app.app_name)
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert isinstance(parsed, dict)


def test_json_output_top_level_keys(capsys):
    app = parse_event_log(SKEW_FIXTURE)
    findings = DataSkewRule().analyze(app)
    render_findings(findings, app.app_id, app.app_name)
    data = json.loads(capsys.readouterr().out)
    assert "app_id" in data
    assert "app_name" in data
    assert "finding_count" in data
    assert "findings" in data


def test_json_output_app_metadata(capsys):
    app = parse_event_log(SKEW_FIXTURE)
    findings = DataSkewRule().analyze(app)
    render_findings(findings, app.app_id, app.app_name)
    data = json.loads(capsys.readouterr().out)
    assert data["app_id"] == app.app_id
    assert data["app_name"] == app.app_name


def test_json_finding_count_matches_findings_array(capsys):
    app = parse_event_log(PARTITION_FIXTURE)
    findings = PartitionCountRule().analyze(app)
    render_findings(findings, app.app_id, app.app_name)
    data = json.loads(capsys.readouterr().out)
    assert data["finding_count"] == len(data["findings"])
    assert data["finding_count"] == 2


def test_json_finding_fields(capsys):
    app = parse_event_log(SKEW_FIXTURE)
    findings = DataSkewRule().analyze(app)
    render_findings(findings, app.app_id, app.app_name)
    data = json.loads(capsys.readouterr().out)
    f = data["findings"][0]
    assert "rule" in f
    assert "severity" in f
    assert "stage_id" in f
    assert "stage_name" in f
    assert "message" in f
    assert "recommendation" in f


def test_json_severity_is_lowercase_string(capsys):
    app = parse_event_log(SKEW_FIXTURE)
    findings = DataSkewRule().analyze(app)
    render_findings(findings, app.app_id, app.app_name)
    data = json.loads(capsys.readouterr().out)
    for f in data["findings"]:
        assert f["severity"] in {"info", "warning", "error"}


def test_json_no_findings_empty_array(capsys):
    app = parse_event_log(SKEW_FIXTURE)
    render_findings([], app.app_id, app.app_name)
    data = json.loads(capsys.readouterr().out)
    assert data["findings"] == []
    assert data["finding_count"] == 0
