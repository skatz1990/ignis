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

- [ ] **Failed tasks / speculation** — flag stages with high task failure or speculation rates.
- [ ] **Executor memory pressure** — flag high JVM GC time as a fraction of executor run time.

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
- [ ] Compressed logs — Spark can gzip event logs (`spark.eventLog.compress=true`). `fsspec` handles `.gz` transparently; verify and add a test fixture.

---

## Dev / Testing infrastructure

- [ ] Add real Spark 3.x event log fixture and compat tests — currently only Spark 4.0.2 has a real event log; all Spark 3.x tests use synthetic hand-crafted fixtures. Needed before confidently promoting past 0.1.0.
- [ ] Integration test that runs the K8s job and asserts ignis finds the skew finding — currently `make run` is manual.
- [ ] Consider `pytest-snapshot` for reporter output to catch formatting regressions.