# Ignis — TODO

Living task list. Items within each section are roughly priority-ordered.

---

## Bugs / Regression gaps

- [ ] Add a `tests/fixtures/duration_null.ndjson` fixture where `Duration` is explicitly `null` in Task Info, and a corresponding test that verifies the fallback to `Finish Time - Launch Time`. Caught by the real Spark 3.5 run — not covered by the hand-crafted fixture today.

---

## Rules (in priority order)

- [ ] **Shuffle size** — flag stages where total shuffle write bytes across tasks exceeds a threshold (default: 1 GB). Data is already in `TaskMetrics.shuffle_write_bytes`. Named constant: `SHUFFLE_WRITE_THRESHOLD_BYTES = 1_073_741_824`.

- [ ] **Spill** — flag stages where tasks spill to disk (`disk_spill_bytes > 0`). Report total spill per stage and the worst offending task. Threshold: `DISK_SPILL_THRESHOLD_BYTES = 0` (any spill is worth flagging). Memory spill (`memory_spill_bytes`) is lower severity — warn only if large.

- [ ] **Partition count** — flag jobs with too few partitions (under-parallelism) or too many (scheduling overhead). Heuristic: fewer than 2× executor cores is too few; more than 10,000 is too many. Requires reading executor count from `SparkListenerExecutorAdded` events.

---

## Parser

- [ ] Parse `SparkListenerExecutorAdded` to capture executor count and core count — needed by the partition count rule and useful for future rules.
- [ ] Parse stage parent IDs (`Parent IDs` in Stage Info) to reconstruct the stage DAG — enables rules that reason about multi-stage pipelines.

---

## CLI / Output

- [ ] `--output json` flag — emit findings as JSON for scripting and CI integration.
- [ ] `--output html` flag — self-contained HTML report with charts.
- [ ] `--rules` flag — list available rules with descriptions and thresholds.
- [ ] `--threshold` overrides — e.g. `--skew-ratio 3.0` to tighten the default.

---

## Distribution

- [ ] Publish to PyPI as `spark-ignis`. Requires `__version__` bump, changelog, and `python -m build` + `twine upload` workflow.
- [ ] GitHub Actions CI — run `pytest` on push, gate PRs.

---

## Storage / Input

- [ ] S3 support — already architected via `fsspec`. Needs a real-world test against an S3 path and docs on credential setup (`AWS_PROFILE`, instance role, etc.).
- [ ] Compressed logs — Spark can gzip event logs (`spark.eventLog.compress=true`). `fsspec` handles `.gz` transparently; verify and add a test fixture.

---

## Dev / Testing infrastructure

- [ ] Add `Duration=null` fixture (see Bugs section above).
- [ ] Integration test that runs the K8s job and asserts ignis finds the skew finding — currently `make run` is manual.
- [ ] Consider `pytest-snapshot` for reporter output to catch formatting regressions.