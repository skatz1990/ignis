import pathlib

from ignis.parser.event_log import parse_event_log
from ignis.rules.partition import PartitionCountRule
from ignis.rules.skew import DataSkewRule
from tests.cloud.helpers import make_cloud_fs_fixture

SKEW_FIXTURE = pathlib.Path(__file__).parent.parent / "fixtures" / "skew_example.ndjson"

mock_s3 = make_cloud_fs_fixture(("s3", "s3a"), "s3fs.S3FileSystem")

BASE = "s3://test-container/logs"


def test_parse_event_log_from_s3(mock_s3):
    app = parse_event_log(f"{BASE}/skew_example.ndjson")
    assert app.app_name != "unknown"
    assert len(app.stages) > 0


def test_s3_app_metadata_parsed(mock_s3):
    app = parse_event_log(f"{BASE}/skew_example.ndjson")
    assert app.app_id != "unknown"
    assert app.app_name != "unknown"


def test_s3_rules_produce_same_findings_as_local(mock_s3):
    local = parse_event_log(str(SKEW_FIXTURE))
    remote = parse_event_log(f"{BASE}/skew_example.ndjson")

    local_findings = DataSkewRule().analyze(local)
    remote_findings = DataSkewRule().analyze(remote)

    assert len(local_findings) == len(remote_findings)
    assert local_findings[0].stage_id == remote_findings[0].stage_id
    assert local_findings[0].rule == remote_findings[0].rule


def test_s3_partition_fixture(mock_s3):
    app = parse_event_log(f"{BASE}/partition_example.ndjson")
    findings = PartitionCountRule().analyze(app)
    assert len(findings) == 2


def test_s3_executor_cores_parsed(mock_s3):
    app = parse_event_log(f"{BASE}/partition_example.ndjson")
    assert app.executor_cores == 4
