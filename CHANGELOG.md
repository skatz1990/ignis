# Changelog

All notable changes to this project will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.2.0] — 2026-04-25

### Added

- **Failed tasks / speculation rule** — flags stages where failed tasks exceed 10% (WARNING) or speculative tasks exceed 25% (INFO); `--failure-rate` and `--speculation-rate` CLI overrides
- **GC pressure rule** — flags stages where JVM GC time exceeds 10% of executor run time (WARNING); `--gc-ratio` CLI override
- Compressed log support — `.gz`, `.bz2`, and `.zst` event logs are decompressed transparently via `compression="infer"`
- Spark 3.5.0 compatibility — verified against a real event log generated from a Spark 3.5.0 job

### Changed

- PyPI publish workflow now requires manual approval via a dedicated GitHub Actions `pypi` environment before uploading

---

## [0.1.0] — 2026-04-22

Initial release.

### Added

- `ignis analyze <path>` — parse a Spark event log and report performance findings
- `ignis rules` — list all available rules with default severity and threshold
- **Data skew rule** — flags stages where the slowest task is ≥ 5× the median
- **Shuffle size rule** — flags stages that write ≥ 1 GB to shuffle files
- **Spill rule** — flags disk spill (WARNING) and memory spill ≥ 500 MB (INFO)
- **Partition count rule** — flags shuffle stages with too few (< 2× cores) or too many (> 10 000) partitions
- `--output json` for machine-readable output
- Per-rule threshold overrides via CLI flags (`--skew-ratio`, `--shuffle-gb`, `--spill-memory-mb`, `--min-tasks-per-core`, `--max-partitions`)
- Cloud support for S3 (`spark-ignis[s3]`), GCS (`spark-ignis[gcs]`), and Azure ADLS Gen2 (`spark-ignis[azure]`)
- Spark 4.0 rolling log format support