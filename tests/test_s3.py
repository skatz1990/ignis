"""
S3 path support tests.

Rather than running against real AWS or fighting moto/aiobotocore version
conflicts, we register a MemoryFileSystem under the "s3" and "s3a" protocols
for the duration of each test. This exercises the full parse_event_log("s3://...")
code path — fsspec.open, line iteration, JSON parsing, rule analysis — with
zero external dependencies.
"""

import pathlib

import fsspec
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from ignis.parser.event_log import parse_event_log
from ignis.rules.partition import PartitionCountRule
from ignis.rules.skew import DataSkewRule

SKEW_FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "skew_example.ndjson"
PARTITION_FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "partition_example.ndjson"


class _MemoryS3(MemoryFileSystem):
    """MemoryFileSystem that responds to s3:// and s3a:// URLs."""

    protocol = ("s3", "s3a")

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        for proto in ("s3a://", "s3://"):
            if path.startswith(proto):
                # Mirror memory:// behavior: strip the scheme and add a leading slash.
                return "/" + path[len(proto) :]
        return path


@pytest.fixture(autouse=False)
def mock_s3():
    """Register an in-memory filesystem as the s3 handler for the test."""
    fs = _MemoryS3()
    fs.store.clear()

    # Use fs.pipe so the store contains proper MemoryFile objects.
    for name, fixture_path in [
        ("skew_example.ndjson", SKEW_FIXTURE),
        ("partition_example.ndjson", PARTITION_FIXTURE),
    ]:
        fs.pipe(f"/test-bucket/logs/{name}", fixture_path.read_bytes())

    fsspec.register_implementation("s3", _MemoryS3, clobber=True)
    fsspec.register_implementation("s3a", _MemoryS3, clobber=True)
    yield fs
    # Restore the real s3fs implementation after the test.
    fsspec.register_implementation("s3", "s3fs.S3FileSystem", clobber=True)
    fsspec.register_implementation("s3a", "s3fs.S3FileSystem", clobber=True)


def test_parse_event_log_from_s3(mock_s3):
    app = parse_event_log("s3://test-bucket/logs/skew_example.ndjson")
    assert app.app_name != "unknown"
    assert len(app.stages) > 0


def test_s3_app_metadata_parsed(mock_s3):
    app = parse_event_log("s3://test-bucket/logs/skew_example.ndjson")
    assert app.app_id != "unknown"
    assert app.app_name != "unknown"


def test_s3_rules_produce_same_findings_as_local(mock_s3):
    local = parse_event_log(str(SKEW_FIXTURE))
    remote = parse_event_log("s3://test-bucket/logs/skew_example.ndjson")

    local_findings = DataSkewRule().analyze(local)
    remote_findings = DataSkewRule().analyze(remote)

    assert len(local_findings) == len(remote_findings)
    assert local_findings[0].stage_id == remote_findings[0].stage_id
    assert local_findings[0].rule == remote_findings[0].rule


def test_s3_partition_fixture(mock_s3):
    app = parse_event_log("s3://test-bucket/logs/partition_example.ndjson")
    findings = PartitionCountRule().analyze(app)
    assert len(findings) == 2


def test_s3_executor_cores_parsed(mock_s3):
    app = parse_event_log("s3://test-bucket/logs/partition_example.ndjson")
    assert app.executor_cores == 4
