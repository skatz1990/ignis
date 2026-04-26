"""
Example Airflow DAG: run a Spark job, then analyze its event log with ignis.

The DAG has three tasks:
  1. spark_job        — submits the Spark job via spark-submit
  2. ignis_analyze    — runs ignis against the event log written to S3
  3. notify_findings  — placeholder: send findings somewhere (Slack, PagerDuty, etc.)

ignis exits 0 (no issues) or 1 (issues found). The notify task only runs when
ignis exits 1, leaving the DAG green on clean runs.

Requirements
------------
- spark-ignis installed in the Airflow worker environment:
    pip install "spark-ignis[s3]"
- spark-submit on PATH (or adjust SPARK_SUBMIT to the full path)
- AWS credentials available to the worker (instance role, env vars, etc.)
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

# ---------------------------------------------------------------------------
# Configuration — adjust these for your environment
# ---------------------------------------------------------------------------

S3_LOG_BUCKET = "my-spark-logs"
APP_JAR = "s3://my-artifacts/jobs/my-job.jar"
APP_CLASS = "com.example.MyJob"
SPARK_SUBMIT = "spark-submit"

# Spark is configured to write event logs to S3. The log path is deterministic:
# spark.eventLog.dir = s3://my-spark-logs/events/
# spark.app.name     = my-spark-job   →  app ID becomes part of the filename
SPARK_CONF = (
    f"--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=s3://{S3_LOG_BUCKET}/events/"
)

# Always exit 0 — branching is driven by finding_count in the JSON, not
# by ignis's exit code (which is 1 when findings exist).
IGNIS_CMD = (
    "ignis analyze "
    f"s3://{S3_LOG_BUCKET}/events/{{{{ ti.xcom_pull(task_ids='spark_job') }}}} "
    "--output json > /tmp/ignis_findings.json; "
    "python3 -c \"import json; print(json.dumps(json.load(open('/tmp/ignis_findings.json'))))\""
)

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="spark_with_ignis",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["spark", "ignis"],
) as dag:
    # 1. Submit the Spark job.
    #    Push the event log app ID to XCom so ignis_analyze can build the path.
    spark_job = BashOperator(
        task_id="spark_job",
        bash_command=(
            f"{SPARK_SUBMIT} {SPARK_CONF} "
            f"--class {APP_CLASS} "
            f"{APP_JAR} "
            # Capture the application ID from the spark-submit output and push
            # it to XCom so the next task can build the full log path.
            "2>&1 | tee /tmp/spark_submit.log; "
            "grep -o 'application_[0-9_]*' /tmp/spark_submit.log | tail -1"
        ),
        do_xcom_push=True,
    )

    # 2. Run ignis against the event log.
    #    Exit code drives the branch: 0 → done, 1 → notify.
    ignis_analyze = BashOperator(
        task_id="ignis_analyze",
        bash_command=IGNIS_CMD,
        # Allow the DAG to continue even when ignis exits 1 so the notify
        # branch can run. Without this, Airflow marks the task failed and
        # skips all downstream tasks.
        append_env=True,
    )

    def _branch(**context) -> str:
        ti = context["ti"]
        findings_raw = ti.xcom_pull(task_ids="ignis_analyze")
        try:
            findings = json.loads(findings_raw or "{}")
            if findings.get("finding_count", 0) > 0:
                return "notify_findings"
        except (json.JSONDecodeError, TypeError):
            pass
        return "no_findings"

    branch = BranchPythonOperator(
        task_id="branch_on_findings",
        python_callable=_branch,
    )

    # 3a. Findings detected — send a notification.
    #     Replace this with a SlackWebhookOperator, PagerDuty call, etc.
    notify_findings = BashOperator(
        task_id="notify_findings",
        bash_command=(
            "echo 'ignis found issues — wire up your notification here'; "
            "cat /tmp/ignis_findings.json"
        ),
    )

    # 3b. No findings — no action needed.
    no_findings = BashOperator(
        task_id="no_findings",
        bash_command="echo 'ignis: no issues found'",
    )

    spark_job >> ignis_analyze >> branch >> [notify_findings, no_findings]
