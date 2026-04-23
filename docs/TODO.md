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
- [ ] `--output html` flag — self-contained HTML report with charts.
- [ ] `--threshold` overrides — e.g. `--skew-ratio 3.0` to tighten the default.

---

## Distribution

- [ ] Publish to PyPI as `spark-ignis`. Requires `__version__` bump, changelog, and `python -m build` + `twine upload` workflow.

---

## Storage / Input

- [x] S3 support — `pip install spark-ignis[s3]` installs `s3fs`; credentials via standard AWS chain. Tested with in-memory fsspec mock.
- [x] GCS support — `pip install spark-ignis[gcs]` installs `gcsfs`; credentials via standard GCP chain.
- [x] Azure ADLS Gen2 support — `pip install spark-ignis[azure]` installs `adlfs`; credentials via standard Azure chain.
- [ ] Cloud integration tests — optional `@pytest.mark.integration` suite that runs against real buckets when credentials are present. Currently all cloud tests use an in-memory fsspec mock.
- [ ] Compressed logs — Spark can gzip event logs (`spark.eventLog.compress=true`). `fsspec` handles `.gz` transparently; verify and add a test fixture.

---

## Dev / Testing infrastructure

- [ ] Integration test that runs the K8s job and asserts ignis finds the skew finding — currently `make run` is manual.
- [ ] Consider `pytest-snapshot` for reporter output to catch formatting regressions.
