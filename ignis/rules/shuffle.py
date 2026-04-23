from ignis.parser.models import Application

from .base import Finding, Rule, RuleInfo, Severity

SHUFFLE_WRITE_THRESHOLD_BYTES = 1_073_741_824  # 1 GB


class ShuffleSizeRule(Rule):
    def __init__(self, threshold_bytes: int = SHUFFLE_WRITE_THRESHOLD_BYTES) -> None:
        self.threshold_bytes = threshold_bytes

    def analyze(self, app: Application) -> list[Finding]:
        findings = []
        for stage in app.stages.values():
            successful = stage.successful_tasks
            if not successful:
                continue

            total_bytes = sum(t.metrics.shuffle_write_bytes for t in successful)
            if total_bytes < self.threshold_bytes:
                continue

            total_gb = total_bytes / 1_073_741_824
            findings.append(
                Finding(
                    rule="shuffle-size",
                    severity=Severity.WARNING,
                    stage_id=stage.stage_id,
                    stage_name=stage.name,
                    message=(
                        f"Stage {stage.stage_id} ({stage.name!r}): "
                        f"total shuffle write is {total_gb:.2f} GB "
                        f"across {len(successful)} task(s)"
                    ),
                    recommendation=(
                        "Reduce shuffle size by filtering data earlier, broadening aggregations, "
                        "or using broadcast joins for smaller tables to avoid the shuffle entirely."
                    ),
                )
            )
        return findings

    def describe(self) -> RuleInfo:
        threshold_gb = self.threshold_bytes / 1_073_741_824
        return RuleInfo(
            id="shuffle-size",
            description="A stage writes an excessive amount of data to shuffle files",
            severity="WARNING",
            threshold=f"total shuffle write ≥ {threshold_gb:.0f} GB",
        )
