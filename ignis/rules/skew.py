import statistics

from ignis.parser.models import Application

from .base import Finding, Rule, Severity

SKEW_RATIO_THRESHOLD = 5.0
MIN_TASKS_FOR_ANALYSIS = 3


class DataSkewRule(Rule):
    def analyze(self, app: Application) -> list[Finding]:
        findings = []
        for stage in app.stages.values():
            successful = stage.successful_tasks
            if len(successful) < MIN_TASKS_FOR_ANALYSIS:
                continue

            durations = [t.metrics.duration_ms for t in successful]
            median_ms = statistics.median(durations)
            max_ms = max(durations)

            if median_ms == 0:
                continue

            ratio = max_ms / median_ms
            if ratio >= SKEW_RATIO_THRESHOLD:
                findings.append(
                    Finding(
                        rule="data-skew",
                        severity=Severity.WARNING,
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        message=(
                            f"Stage {stage.stage_id} ({stage.name!r}): "
                            f"max task {max_ms:,}ms vs median {median_ms:.0f}ms "
                            f"({ratio:.1f}x ratio)"
                        ),
                        recommendation=(
                            "Repartition before the shuffle with a higher partition count, "
                            "or salt the join/groupBy key to spread work across more tasks."
                        ),
                    )
                )
        return findings
