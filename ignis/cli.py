from enum import Enum

import typer
from rich import box
from rich.console import Console
from rich.table import Table

from ignis.parser.event_log import parse_event_log
from ignis.reporter import json_reporter
from ignis.reporter import terminal as terminal_reporter
from ignis.rules.partition import PartitionCountRule
from ignis.rules.shuffle import ShuffleSizeRule
from ignis.rules.skew import DataSkewRule
from ignis.rules.spill import SpillRule

app = typer.Typer(
    name="ignis",
    help="ESLint for Apache Spark jobs — analyze event logs, diagnose performance issues.",
    no_args_is_help=True,
)

_err = Console(stderr=True)

_RULES = [DataSkewRule(), ShuffleSizeRule(), SpillRule(), PartitionCountRule()]


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
) -> None:
    """Analyze a Spark event log and report performance issues."""
    try:
        application = parse_event_log(path)
    except Exception as exc:
        _err.print(f"[red]Error reading event log:[/red] {exc}")
        raise typer.Exit(1)

    findings = [f for rule in _RULES for f in rule.analyze(application)]

    if output == OutputFormat.json:
        json_reporter.render_findings(findings, application.app_id, application.app_name)
    else:
        terminal_reporter.render_findings(findings, application.app_name)

    if findings:
        raise typer.Exit(1)


@app.command()
def rules() -> None:
    """List all available rules with their severity and trigger threshold."""
    console = Console()
    table = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold", expand=False)
    table.add_column("Rule", style="bold")
    table.add_column("Severity", width=14)
    table.add_column("Threshold")
    table.add_column("Description")

    for rule in _RULES:
        info = rule.describe()
        table.add_row(info.id, info.severity, info.threshold, info.description)

    console.print(table)


def main() -> None:
    app()
