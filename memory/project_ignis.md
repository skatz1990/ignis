---
name: Ignis project overview
description: Architecture, design constraints, rule roadmap, venv path, and current state of the Ignis Spark log analyzer
type: project
---

Ignis is a Python CLI tool (`ignis analyze <path>`) that parses Apache Spark NDJSON event logs and diagnoses performance issues.

**Venv:** `/Users/shaharkatz/Repos/ignis/.venv`

**Entry point:** `ignis/cli.py` — typer app with two subcommands:
- `ignis analyze <path> [--output terminal|json]` — runs all rules, exits 1 if findings
- `ignis rules` — prints a table of all registered rules with thresholds

**Current state (as of 2026-04-21):** All four initial rules are implemented, tested, and on main. README, docs/rules.md, and docs/TODO.md are all up to date.

**Rules (all complete):**
- `data-skew` — max/median task duration ≥ 5× (WARNING)
- `shuffle-size` — total shuffle write ≥ 1 GB (WARNING)
- `spill` — any disk spill (WARNING); memory spill ≥ 500 MB (INFO)
- `partition-count` — < 2× executor cores or > 10,000 shuffle partitions (WARNING)

**Output formats:**
- Terminal: rich table + recommendation panels (default)
- JSON: `--output json` emits `{app_id, app_name, finding_count, findings[]}` to stdout

**Key design decisions:**
- Only shuffle-read stages checked by partition-count rule (map stages skipped)
- `num_tasks` from StageSubmitted used for partition count (not task event count)
- Spark 4.0 v2 log format (zstd-compressed directories) handled in dev/Makefile
- fsspec for path abstraction (S3 support architected but not yet tested)

**Next steps (priority order):**
1. PyPI publishing — needs versioning strategy, CHANGELOG, `__version__` bump
2. S3 support — test against real S3 path, document credential setup
3. Future rules: failed task rates, JVM GC pressure

**Test suite:** 42 tests across 6 test files. All pass. CI runs pytest on Python 3.11 + 3.12 and ruff on every PR.

**K8s dev setup:** `dev/Makefile` with `run`, `run-shuffle`, `run-spill`, `run-partition` targets. Uses `apache/spark:4.0.2-python3` image, `ignis-dev` namespace.
