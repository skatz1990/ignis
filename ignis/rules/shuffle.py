from ignis.parser.models import Application

from .base import Finding, Rule, Severity

SHUFFLE_WRITE_THRESHOLD_BYTES = 1_073_741_824  # 1 GB


class ShuffleSizeRule(Rule):
    def analyze(self, app: Application) -> list[Finding]:
        findings = []
        for stage in app.stages.values():
            successful = stage.successful_tasks
            if not successful:
                continue

            total_bytes = sum(t.metrics.shuffle_write_bytes for t in successful)
            if total_bytes < SHUFFLE_WRITE_THRESHOLD_BYTES:
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
