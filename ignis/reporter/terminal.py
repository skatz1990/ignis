from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ignis.rules.base import Finding, Severity

_SEVERITY_COLOR = {
    Severity.INFO: "cyan",
    Severity.WARNING: "yellow",
    Severity.ERROR: "red",
}

console = Console()


def render_findings(findings: list[Finding], app_name: str) -> None:
    console.rule(f"[bold]ignis[/bold]  {app_name}")

    if not findings:
        console.print(Panel("[green]No issues found.[/green]", border_style="green"))
        return

    console.print(f"\n[bold]{len(findings)} issue(s) found[/bold]\n")

    table = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold", expand=False)
    table.add_column("Severity", width=10)
    table.add_column("Rule", width=15)
    table.add_column("Stage", width=6, justify="right")
    table.add_column("Message")

    for f in findings:
        color = _SEVERITY_COLOR[f.severity]
        table.add_row(
            f"[{color}]{f.severity.value.upper()}[/{color}]",
            f.rule,
            str(f.stage_id),
            f.message,
        )

    console.print(table)

    for f in findings:
        color = _SEVERITY_COLOR[f.severity]
        console.print(
            Panel(
                f.recommendation,
                title=f"[{color}]{f.rule}[/{color}] — Stage {f.stage_id}",
                border_style=color,
                padding=(0, 1),
            )
        )
