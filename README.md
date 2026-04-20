# Ignis

ESLint for Apache Spark jobs. Point it at an event log and get actionable diagnostics for data skew, shuffle size, spill, and bad partitioning.

```
$ ignis analyze /path/to/spark-event-log

──────────────────── ignis  my-spark-app ────────────────────

1 issue(s) found

  Severity   Rule        Stage  Message
 ────────────────────────────────────────────────────────────
  WARNING    data-skew       2  Stage 2 ('groupBy at job.py:42'):
                                max task 42,300ms vs median 1,800ms (23.5x ratio)

╭───────────────────── data-skew — Stage 2 ──────────────────╮
│ Repartition before the shuffle with a higher partition     │
│ count, or salt the join/groupBy key to spread work across  │
│ more tasks.                                                │
╰────────────────────────────────────────────────────────────╯
```

## Installation

```bash
pip install spark-ignis
```

## Usage

```bash
ignis analyze <path-to-event-log>
```

Exits `0` if no issues are found, `1` if any are.

Spark event logs are standard NDJSON files. Databricks writes them to DBFS or cloud storage after each job; download one locally or point directly at the path once S3 support lands.

## Rules

| Rule | Trigger | Default threshold |
|---|---|---|
| `data-skew` | Max task duration / median task duration within a stage | ≥ 5× |

More rules coming: shuffle size, spill, partition count.

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
  reporter/   Rich terminal output
  cli.py      Entry point — ignis analyze <path>
tests/
  fixtures/   Hand-crafted NDJSON snippets that trigger each rule
```
