# Ignis

[![CI](https://github.com/skatz1990/ignis/actions/workflows/ci.yml/badge.svg)](https://github.com/skatz1990/ignis/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/spark-ignis)](https://pypi.org/project/spark-ignis/)
[![Python](https://img.shields.io/pypi/pyversions/spark-ignis)](https://pypi.org/project/spark-ignis/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

ESLint for Apache Spark jobs. Point it at an event log and get actionable diagnostics for data skew, shuffle size, spill, and bad partitioning.

```
$ ignis analyze /path/to/spark-event-log

──────────────────── ignis  my-spark-app ────────────────────

2 issue(s) found

  Severity   Rule              Stage  Message
 ────────────────────────────────────────────────────────────
  WARNING    data-skew             2  Stage 2 ('groupBy at job.py:42'):
                                      max task 42,300ms vs median 1,800ms (23.5x ratio)
  WARNING    partition-count       3  Stage 3 ('join at job.py:71'):
                                      2 shuffle partition(s) across 8 executor core(s)
                                      — cluster is under-utilized

╭───────────────────── data-skew — Stage 2 ──────────────────╮
│ Repartition before the shuffle with a higher partition     │
│ count, or salt the join/groupBy key to spread work across  │
│ more tasks.                                                │
╰────────────────────────────────────────────────────────────╯
╭─────────────────── partition-count — Stage 3 ──────────────╮
│ Raise spark.sql.shuffle.partitions to at least 16          │
│ (2× your 8 executor cores).                                │
╰────────────────────────────────────────────────────────────╯
```

## Installation

```bash
pip install spark-ignis            # core only
pip install "spark-ignis[s3]"      # + AWS S3
pip install "spark-ignis[gcs]"     # + Google Cloud Storage
pip install "spark-ignis[azure]"   # + Azure Data Lake Storage
```

Or install from source:

```bash
git clone https://github.com/skatz1990/ignis
cd ignis
python3 -m venv .venv && source .venv/bin/activate
pip install -e .               # local files only
pip install -e ".[s3]"         # + AWS S3
pip install -e ".[gcs]"        # + Google Cloud Storage
pip install -e ".[azure]"      # + Azure Data Lake Storage
```

## Usage

```bash
# Analyze a local event log (terminal output, exits 1 if issues found)
ignis analyze /path/to/spark-event-log

# Analyze directly from cloud storage
ignis analyze s3://my-bucket/spark-logs/application_1234_0001
ignis analyze gs://my-bucket/spark-logs/application_1234_0001
ignis analyze abfs://my-container/spark-logs/application_1234_0001

# Machine-readable JSON output — pipe to jq, store in CI artifacts
ignis analyze s3://my-bucket/spark-logs/application_1234_0001 --output json

# Pipe findings directly to a Slack channel (webhook URL from env var)
export IGNIS_SLACK_WEBHOOK="https://hooks.slack.com/services/..."
ignis analyze s3://my-bucket/spark-logs/application_1234_0001 --output json \
  | ignis notify slack

# Send findings by email
export IGNIS_SMTP_PASSWORD="pass"
ignis analyze s3://my-bucket/spark-logs/application_1234_0001 --output json \
  | ignis notify email ops@example.com \
      --from ignis@example.com --smtp-host smtp.example.com

# List all rules with their thresholds
ignis rules
```

Exits `0` if no issues are found, `1` if any are — in both terminal and JSON modes.

Spark event logs are standard NDJSON files (Spark 3.x) or zstd-compressed directories (Spark 4.0+). Databricks writes them to DBFS, S3, GCS, or ADLS after each job.

## Notifications

`ignis notify` reads findings JSON from stdin and routes them to a notification channel. It is silent (exit 0, no message) when there are no findings. Pass `--always` to send a clean-run confirmation.

### Slack

```bash
export IGNIS_SLACK_WEBHOOK="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
ignis analyze /path/to/spark-event-log --output json | ignis notify slack
```

The webhook URL can also be passed as a positional argument, but using the environment variable keeps it out of shell history and CI logs. Create an incoming webhook at **api.slack.com/apps** → your app → Incoming Webhooks.

### Email

```bash
export IGNIS_SMTP_USERNAME="user"
export IGNIS_SMTP_PASSWORD="pass"
ignis analyze /path/to/spark-event-log --output json \
  | ignis notify email ops@example.com \
      --from ignis@example.com \
      --smtp-host smtp.example.com
```

Sends a plain-text + HTML multipart email via SMTP with STARTTLS. Credentials are read from `IGNIS_SMTP_USERNAME` / `IGNIS_SMTP_PASSWORD` environment variables (recommended) or passed via `--username` / `--password` flags. `--smtp-port` defaults to 587.

## Cloud storage

### AWS S3

```bash
pip install -e ".[s3]"
ignis analyze s3://my-bucket/spark-logs/application_1234_0001
```

Credentials from the standard AWS chain:

| Source | How |
|---|---|
| Environment variables | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` |
| Named profile | `AWS_PROFILE=my-profile ignis analyze s3://...` |
| Instance role (EC2/ECS) | No configuration needed |
| SSO | `aws sso login` then run ignis normally |

### Google Cloud Storage

```bash
pip install -e ".[gcs]"
ignis analyze gs://my-bucket/spark-logs/application_1234_0001
```

Credentials from the standard GCP chain:

| Source | How |
|---|---|
| User credentials | `gcloud auth application-default login` |
| Service account key | `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json` |
| Workload Identity (GKE) | No configuration needed |

### Azure Data Lake Storage (ADLS Gen2)

```bash
pip install -e ".[azure]"
ignis analyze abfs://my-container/spark-logs/application_1234_0001
```

Credentials from the standard Azure chain:

| Source | How |
|---|---|
| Service principal | `AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` |
| Azure CLI | `az login` then run ignis normally |
| Managed identity | No configuration needed |

## Rules

| Rule | What it detects | Default threshold |
|---|---|---|
| `data-skew` | One task takes far longer than its peers in a shuffle stage | max ≥ 5× median task duration |
| `shuffle-size` | A stage writes an excessive amount of data to shuffle files | total shuffle write ≥ 1 GB |
| `spill` | Tasks spill execution data to disk or show significant memory pressure | any disk spill (WARNING); memory spill ≥ 500 MB (INFO) |
| `partition-count` | Shuffle partition count leaves the cluster idle or overwhelms the driver | < 2× executor cores or > 10,000 partitions |
| `failed-tasks` | High rate of task failures or speculative task launches in a stage | failure rate ≥ 10% (WARNING); speculation rate ≥ 25% (INFO) |
| `gc-pressure` | JVM garbage collection consumes a large fraction of executor run time | GC time ≥ 10% of executor run time (WARNING) |

Run `ignis rules` for a live summary with thresholds.

## JSON output

`--output json` emits a structured document to stdout:

```json
{
  "app_id": "application_1234_0001",
  "app_name": "my-spark-app",
  "finding_count": 1,
  "findings": [
    {
      "rule": "data-skew",
      "severity": "warning",
      "stage_id": 2,
      "stage_name": "groupBy at job.py:42",
      "message": "Stage 2 ('groupBy at job.py:42'): max task 42,300ms vs median 1,800ms (23.5x ratio)",
      "recommendation": "Repartition before the shuffle with a higher partition count, or salt the join/groupBy key to spread work across more tasks."
    }
  ]
}
```

## Development

```bash
git clone https://github.com/skatz1990/ignis
cd ignis
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## Project layout

```
ignis/
  parser/     NDJSON event log parsing → Application/Stage/Task models
  rules/      Diagnostic rules (one module per rule)
  reporter/   Terminal (rich) and JSON output
  cli.py      Entry point — ignis analyze <path>, ignis rules
tests/
  fixtures/   Hand-crafted NDJSON snippets that trigger each rule
docs/
  rules.md    Detailed explanation of each rule and its detection logic
```
