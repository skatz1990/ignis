import typer
from rich.console import Console

from ignis.parser.event_log import parse_event_log
from ignis.reporter.terminal import render_findings
from ignis.rules.shuffle import ShuffleSizeRule
from ignis.rules.skew import DataSkewRule

app = typer.Typer(
    name="ignis",
    help="ESLint for Apache Spark jobs — analyze event logs, diagnose performance issues.",
    no_args_is_help=True,
)

_err = Console(stderr=True)

_RULES = [DataSkewRule(), ShuffleSizeRule()]


@app.callback()
def _root() -> None:
    """ESLint for Apache Spark jobs — analyze event logs, diagnose performance issues."""


@app.command()
def analyze(
    path: str = typer.Argument(..., help="Path to a Spark event log (local path or s3://)."),
) -> None:
    """Analyze a Spark event log and report performance issues."""
    try:
        application = parse_event_log(path)
    except Exception as exc:
        _err.print(f"[red]Error reading event log:[/red] {exc}")
        raise typer.Exit(1)

    findings = [f for rule in _RULES for f in rule.analyze(application)]
    render_findings(findings, application.app_name)

    if findings:
        raise typer.Exit(1)


def main() -> None:
    app()
