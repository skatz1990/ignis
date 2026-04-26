import json
import sys
from enum import Enum

import typer
from rich import box
from rich.console import Console
from rich.table import Table

from ignis.parser.event_log import parse_event_log
from ignis.reporter import json_reporter
from ignis.reporter import terminal as terminal_reporter
from ignis.rules.failed_tasks import (
    TASK_FAILURE_RATE_THRESHOLD,
    TASK_SPECULATION_RATE_THRESHOLD,
    FailedTasksRule,
)
from ignis.rules.gc_pressure import GC_RATIO_THRESHOLD, GCPressureRule
from ignis.rules.partition import MAX_PARTITION_COUNT, MIN_TASKS_PER_CORE, PartitionCountRule
from ignis.rules.shuffle import SHUFFLE_WRITE_THRESHOLD_BYTES, ShuffleSizeRule
from ignis.rules.skew import SKEW_RATIO_THRESHOLD, DataSkewRule
from ignis.rules.spill import MEMORY_SPILL_THRESHOLD_BYTES, SpillRule

app = typer.Typer(
    name="ignis",
    help="ESLint for Apache Spark jobs — analyze event logs, diagnose performance issues.",
    no_args_is_help=True,
)

notify_app = typer.Typer(help="Send findings to notification channels.", no_args_is_help=True)
app.add_typer(notify_app, name="notify")

_err = Console(stderr=True)

# Default rule instances — used by `ignis rules` to show default thresholds.
_DEFAULT_RULES = [
    DataSkewRule(),
    ShuffleSizeRule(),
    SpillRule(),
    PartitionCountRule(),
    FailedTasksRule(),
    GCPressureRule(),
]


class OutputFormat(str, Enum):
    terminal = "terminal"
    json = "json"


@app.callback()
def _root() -> None:
    """ESLint for Apache Spark jobs — analyze event logs, diagnose performance issues."""


@app.command()
def analyze(
    path: str = typer.Argument(..., help="Path to a Spark event log (local path or s3://)."),
    output: OutputFormat = typer.Option(
        OutputFormat.terminal, "--output", "-o", help="Output format."
    ),
    skew_ratio: float = typer.Option(
        SKEW_RATIO_THRESHOLD, "--skew-ratio", help="Max/median task duration ratio to flag as skew."
    ),
    shuffle_gb: float = typer.Option(
        SHUFFLE_WRITE_THRESHOLD_BYTES / 1_073_741_824,
        "--shuffle-gb",
        help="Shuffle write threshold in GB.",
    ),
    spill_memory_mb: int = typer.Option(
        MEMORY_SPILL_THRESHOLD_BYTES // 1_048_576,
        "--spill-memory-mb",
        help="Memory spill threshold in MB before firing an INFO finding.",
    ),
    min_tasks_per_core: int = typer.Option(
        MIN_TASKS_PER_CORE,
        "--min-tasks-per-core",
        help="Minimum shuffle partitions per executor core before flagging under-parallelism.",
    ),
    max_partitions: int = typer.Option(
        MAX_PARTITION_COUNT,
        "--max-partitions",
        help="Maximum shuffle partitions before flagging scheduling overhead.",
    ),
    failure_rate: float = typer.Option(
        TASK_FAILURE_RATE_THRESHOLD,
        "--failure-rate",
        help="Fraction of failed tasks per stage to flag (0.0–1.0).",
    ),
    speculation_rate: float = typer.Option(
        TASK_SPECULATION_RATE_THRESHOLD,
        "--speculation-rate",
        help="Fraction of speculative tasks per stage to flag (0.0–1.0).",
    ),
    gc_ratio: float = typer.Option(
        GC_RATIO_THRESHOLD,
        "--gc-ratio",
        help="GC time as a fraction of executor run time to flag (0.0–1.0).",
    ),
) -> None:
    """Analyze a Spark event log and report performance issues."""
    try:
        application = parse_event_log(path)
    except Exception as exc:
        _err.print(f"[red]Error reading event log:[/red] {exc}")
        raise typer.Exit(1)

    active_rules = [
        DataSkewRule(skew_ratio=skew_ratio),
        ShuffleSizeRule(threshold_bytes=int(shuffle_gb * 1_073_741_824)),
        SpillRule(memory_threshold_bytes=spill_memory_mb * 1_048_576),
        PartitionCountRule(min_tasks_per_core=min_tasks_per_core, max_partitions=max_partitions),
        FailedTasksRule(failure_rate=failure_rate, speculation_rate=speculation_rate),
        GCPressureRule(gc_ratio=gc_ratio),
    ]
    findings = [f for rule in active_rules for f in rule.analyze(application)]

    if output == OutputFormat.json:
        json_reporter.render_findings(findings, application.app_id, application.app_name)
    else:
        terminal_reporter.render_findings(findings, application.app_name)

    if findings:
        raise typer.Exit(1)


@app.command()
def rules() -> None:
    """List all available rules with their default severity and trigger threshold."""
    console = Console()
    table = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold", expand=False)
    table.add_column("Rule", style="bold")
    table.add_column("Severity", width=14)
    table.add_column("Threshold")
    table.add_column("Description")

    for rule in _DEFAULT_RULES:
        info = rule.describe()
        table.add_row(info.id, info.severity, info.threshold, info.description)

    console.print(table)


@notify_app.command("slack")
def notify_slack(
    webhook_url: str = typer.Argument(..., help="Slack incoming webhook URL."),
    always: bool = typer.Option(
        False, "--always", help="Post even when there are no findings (clean-run confirmation)."
    ),
) -> None:
    """Read findings JSON from stdin and post a summary to a Slack webhook."""
    from ignis.notify.slack import post

    try:
        data = json.loads(sys.stdin.read())
    except json.JSONDecodeError as exc:
        _err.print(f"[red]Invalid JSON on stdin:[/red] {exc}")
        raise typer.Exit(1)

    finding_count = data.get("finding_count", 0)

    if finding_count == 0 and not always:
        return

    try:
        post(webhook_url, data)
    except RuntimeError as exc:
        _err.print(f"[red]Slack notification failed:[/red] {exc}")
        raise typer.Exit(1)


def main() -> None:
    app()
