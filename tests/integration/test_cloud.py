"""
Docker-based integration tests for cloud storage backends.

Each test spins up a local emulator via testcontainers, uploads the skew
fixture, parses it through the full ignis pipeline, and asserts specific
findings — so any regression in cloud I/O or rule logic is caught.

Emulators used:
  S3    — MinIO          (minio/minio)
  GCS   — fake-gcs-server (fsouza/fake-gcs-server)
  Azure — Azurite        (mcr.microsoft.com/azure-storage/azurite)

Run only these tests:
    pytest -m integration

Requires Docker and the integration extra:
    pip install "spark-ignis[integration]"
"""

import pytest

from ignis.parser.event_log import parse_event_log
from ignis.rules.skew import DataSkewRule

pytestmark = pytest.mark.integration

# The skew fixture has app_name="ignis-test-app", 1 stage, and 1 skew finding.
_EXPECTED_APP_NAME = "ignis-test-app"
_EXPECTED_STAGES = 1
_EXPECTED_SKEW_FINDINGS = 1


# ── S3 / MinIO ───────────────────────────────────────────────────────────────


def test_s3_parse_and_analyze(s3_fixture):
    path, opts = s3_fixture
    app = parse_event_log(path, **opts)
    assert app.app_name == _EXPECTED_APP_NAME
    assert len(app.stages) == _EXPECTED_STAGES
    assert len(DataSkewRule().analyze(app)) == _EXPECTED_SKEW_FINDINGS


# ── GCS / fake-gcs-server ────────────────────────────────────────────────────


def test_gcs_parse_and_analyze(gcs_fixture):
    path, opts = gcs_fixture
    app = parse_event_log(path, **opts)
    assert app.app_name == _EXPECTED_APP_NAME
    assert len(app.stages) == _EXPECTED_STAGES
    assert len(DataSkewRule().analyze(app)) == _EXPECTED_SKEW_FINDINGS


# ── Azure / Azurite ──────────────────────────────────────────────────────────


def test_azure_parse_and_analyze(azure_fixture):
    path, opts = azure_fixture
    app = parse_event_log(path, **opts)
    assert app.app_name == _EXPECTED_APP_NAME
    assert len(app.stages) == _EXPECTED_STAGES
    assert len(DataSkewRule().analyze(app)) == _EXPECTED_SKEW_FINDINGS
