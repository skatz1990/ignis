# Ignis — TODO

Living task list. Items within each section are roughly priority-ordered.

---

## Rules

All four initial rules are implemented and on main:
- [x] **Data skew** — `DataSkewRule`
- [x] **Shuffle size** — `ShuffleSizeRule` (threshold: 1 GB)
- [x] **Spill** — `SpillRule` (disk: any spill → WARNING; memory: ≥500 MB → INFO)
- [x] **Partition count** — `PartitionCountRule` (under: <2× cores; over: >10k partitions)

### Future rules

- [x] **Failed tasks / speculation** — `FailedTasksRule`: failure rate ≥ 10% (WARNING), speculation rate ≥ 25% (INFO).
- [x] **Executor memory pressure** — `GCPressureRule`: GC time ≥ 10% of executor run time (WARNING).

---

## Parser

- [ ] Parse stage parent IDs (`Parent IDs` in Stage Info) to reconstruct the stage DAG — enables rules that reason about multi-stage pipelines.

---

## CLI / Output

- [x] `--output json` flag — emits `{app_id, app_name, finding_count, findings[]}` to stdout.
- [x] `ignis rules` command — lists all rules with severity and threshold.
- [x] `--threshold` overrides — `--skew-ratio`, `--shuffle-gb`, `--spill-memory-mb`, `--min-tasks-per-core`, `--max-partitions`.
- [ ] `--output html` flag — self-contained HTML report with charts.

---

## Distribution

- [x] Publish to PyPI as `spark-ignis` — versioned via git tags (`hatch-vcs`), published via GitHub Actions trusted publishing on every `v*` tag push.
- [x] Set up a dedicated GitHub Actions `pypi` environment with required reviewers to gate publishes.

---

## Storage / Input

- [x] S3 support — `pip install spark-ignis[s3]` installs `s3fs`; credentials via standard AWS chain.
- [x] GCS support — `pip install spark-ignis[gcs]` installs `gcsfs`; credentials via standard GCP chain.
- [x] Azure ADLS Gen2 support — `pip install spark-ignis[azure]` installs `adlfs`; credentials via standard Azure chain.
- [x] Cloud integration tests — Docker-based suite using MinIO, fake-gcs-server, and Azurite via testcontainers. Runs in CI on every PR.
- [x] Compressed logs — `compression="infer"` in `fsspec.open` handles `.gz` (and `.bz2`, `.zst`) transparently; verified with a gzipped fixture.

---

## Automation / Integrations

ignis is a reactive tool — it analyzes event logs after a Spark job completes. The natural next step is making it run automatically as part of a larger workflow.

- [ ] **Orchestrator integration** — document (and potentially provide helpers for) running `ignis analyze` as a post-Spark task in Airflow, Dagster, or Prefect, using exit code and JSON output to route findings.
- [ ] **Cloud event trigger** — support triggering ignis via S3/GCS/Azure Blob event notifications (e.g. Lambda, Cloud Function, Azure Function) so analysis runs automatically when a new event log appears in a bucket.
- [ ] **Notification / routing layer** — a way to send findings somewhere actionable: Slack webhook, PagerDuty, a database. Required for any automated workflow to be useful beyond logging.

---

## Dev / Testing infrastructure

- [x] Add real Spark 3.x event log fixture and compat tests — `spark35_compat.ndjson` generated from a real Spark 3.5.0 SparkPi job via Docker; 4 parser compat tests added.
- [ ] Integration test that runs the K8s job and asserts ignis finds the skew finding — currently `make run` is manual.
- [ ] Consider `pytest-snapshot` for reporter output to catch formatting regressions.