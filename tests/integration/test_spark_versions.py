"""
Spark version compatibility integration tests.

Spins up a real Spark cluster via Docker for each supported version, runs a
SparkPi job with event logging enabled, then asserts ignis can parse the
resulting log and extract expected structure.

Versions tested: 3.5.8, 4.0.2, 4.1.1

Run only these tests:
    pytest -m integration -k spark_version

Requires Docker. No extra Python packages needed beyond the core install.
"""

import subprocess

import pytest

from ignis.parser.event_log import parse_event_log

pytestmark = pytest.mark.integration

_SPARK_VERSIONS = ["3.5.8", "4.0.2", "4.1.1"]


def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True, timeout=10)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


@pytest.fixture(scope="session", params=_SPARK_VERSIONS)
def spark_event_log(request, tmp_path_factory):
    """Run a SparkPi job and return (version, log_path)."""
    version = request.param

    if not _docker_available():
        pytest.skip("Docker not available")

    log_dir = tmp_path_factory.mktemp(f"spark-{version.replace('.', '-')}")

    cmd = [
        "docker",
        "run",
        "--rm",
        "--user",
        "root",
        "-v",
        f"{log_dir}:/tmp/spark-logs",
        f"apache/spark:{version}",
        "/bin/bash",
        "-c",
        # Find the examples jar regardless of Scala version (2.12 vs 2.13)
        "JAR=$(find /opt/spark/examples/jars -name 'spark-examples_*.jar' | head -1) && "
        "/opt/spark/bin/spark-submit "
        "--conf spark.eventLog.enabled=true "
        "--conf spark.eventLog.dir=/tmp/spark-logs "
        # Disable rolling format (Spark 4.x default) so we always get a single file
        "--conf spark.eventLog.rolling.enabled=false "
        "--class org.apache.spark.examples.SparkPi "
        # Make logs world-readable before container exits; running as root inside
        # the container means files land as root:root on the host mount.
        "$JAR 10 && chmod -R a+r /tmp/spark-logs",
    ]

    result = subprocess.run(cmd, capture_output=True, timeout=180)
    if result.returncode != 0:
        pytest.skip(f"Spark {version} job failed:\n{result.stderr.decode()[-2000:]}")

    logs = [p for p in log_dir.iterdir() if not p.name.endswith(".inprogress")]
    if not logs:
        pytest.skip(f"No event log found for Spark {version} in {log_dir}")

    yield version, str(logs[0])


def test_parser_reads_log(spark_event_log):
    version, path = spark_event_log
    app = parse_event_log(path)
    assert app.app_id != "unknown", f"Spark {version}: app_id not parsed"
    assert app.app_name != "unknown", f"Spark {version}: app_name not parsed"


def test_stages_parsed(spark_event_log):
    version, path = spark_event_log
    app = parse_event_log(path)
    assert len(app.stages) > 0, f"Spark {version}: no stages parsed"


def test_tasks_parsed(spark_event_log):
    version, path = spark_event_log
    app = parse_event_log(path)
    total_tasks = sum(len(s.tasks) for s in app.stages.values())
    assert total_tasks > 0, f"Spark {version}: no tasks parsed"


def test_task_metrics_present(spark_event_log):
    version, path = spark_event_log
    app = parse_event_log(path)
    successful = [t for s in app.stages.values() for t in s.successful_tasks]
    assert len(successful) > 0, f"Spark {version}: no successful tasks with metrics"
    for task in successful:
        assert task.metrics is not None
        assert task.metrics.executor_run_time_ms >= 0


def test_parent_ids_parsed(spark_event_log):
    version, path = spark_event_log
    app = parse_event_log(path)
    # All stages should have parent_ids as a list (empty for root stages)
    for stage in app.stages.values():
        assert isinstance(stage.parent_ids, list)
