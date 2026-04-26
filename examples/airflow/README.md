# Airflow + ignis example

This example shows how to run ignis automatically after a Spark job completes,
using Apache Airflow to wire the two steps together.

## How it works

The DAG has three stages:

```
spark_job  →  ignis_analyze  →  branch_on_findings
                                  ├── notify_findings   (issues found)
                                  └── no_findings       (clean run)
```

1. **spark_job** — submits your Spark application via `spark-submit`. Spark is
   configured to write its event log to S3.
2. **ignis_analyze** — runs `ignis analyze <s3-log-path> --output json` against
   the event log as soon as the job finishes. ignis exits `0` if no issues are
   found, `1` if any are.
3. **branch_on_findings** — reads the JSON output and routes to either
   `notify_findings` (issues detected) or `no_findings` (clean run).

## Setup

Install ignis in your Airflow worker environment:

```bash
pip install "spark-ignis[s3]"   # or [gcs] / [azure] depending on your storage
```

Configure Spark to write event logs. Add these to your `spark-defaults.conf` or
pass them as `--conf` flags in `spark_submit`:

```
spark.eventLog.enabled  true
spark.eventLog.dir      s3://my-spark-logs/events/
```

## Sending notifications

The `notify_findings` task is a placeholder — replace the `BashOperator` with
whatever makes sense for your team:

**Slack** (via `apache-airflow-providers-slack`):
```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

notify_findings = SlackWebhookOperator(
    task_id="notify_findings",
    slack_webhook_conn_id="slack_default",
    message="ignis found performance issues in {{ ti.xcom_pull(task_ids='spark_job') }}",
)
```

**Email** (built-in):
```python
from airflow.operators.email import EmailOperator

notify_findings = EmailOperator(
    task_id="notify_findings",
    to="data-engineering@example.com",
    subject="ignis findings — {{ ds }}",
    html_content="<pre>{{ ti.xcom_pull(task_ids='ignis_analyze') }}</pre>",
)
```

## GCS and Azure

The DAG uses S3 by default. For other storage backends, change the log path
prefix and install the matching extra:

| Cloud | Log path prefix | Install |
|---|---|---|
| AWS S3 | `s3://` | `spark-ignis[s3]` |
| Google Cloud Storage | `gs://` | `spark-ignis[gcs]` |
| Azure ADLS Gen2 | `abfs://` | `spark-ignis[azure]` |
