"""
Local test version of spark_with_ignis.py.

Replaces the real spark-submit with a no-op that returns a known fixture path,
and points ignis at a local NDJSON fixture so the DAG can be exercised without
a Spark cluster or cloud storage.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

FIXTURE_PATH = "/Users/shaharkatz/Repos/ignis/tests/fixtures/skew_example.ndjson"

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="spark_with_ignis_test",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "ignis", "test"],
) as dag:

    # Mock spark-submit: just return the fixture path as if it were an app ID
    spark_job = BashOperator(
        task_id="spark_job",
        bash_command=f"echo '{FIXTURE_PATH}'",
        do_xcom_push=True,
    )

    ignis_analyze = BashOperator(
        task_id="ignis_analyze",
        # Always exit 0 — branching is driven by finding_count in the JSON, not
        # by ignis's exit code (which is 1 when findings exist).
        bash_command=(
            "ignis analyze {{ ti.xcom_pull(task_ids='spark_job') }} --output json "
            "> /tmp/ignis_findings.json; "
            "python3 -c \"import json,sys; print(json.dumps(json.load(open('/tmp/ignis_findings.json'))))\""
        ),
        do_xcom_push=True,
    )

    def _branch(**context) -> str:
        ti = context["ti"]
        raw = ti.xcom_pull(task_ids="ignis_analyze")
        try:
            findings = json.loads(raw or "{}")
            if findings.get("finding_count", 0) > 0:
                return "notify_findings"
        except (json.JSONDecodeError, TypeError):
            pass
        return "no_findings"

    branch = BranchPythonOperator(
        task_id="branch_on_findings",
        python_callable=_branch,
    )

    notify_findings = BashOperator(
        task_id="notify_findings",
        bash_command="echo '--- ignis findings ---' && python3 -m json.tool /tmp/ignis_findings.json",
    )

    no_findings = BashOperator(
        task_id="no_findings",
        bash_command="echo 'ignis: no issues found'",
    )

    spark_job >> ignis_analyze >> branch >> [notify_findings, no_findings]
