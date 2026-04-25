from ignis.parser.models import Application

from .base import Finding, Rule, RuleInfo, Severity

TASK_FAILURE_RATE_THRESHOLD = 0.10  # 10 % of tasks failed
TASK_SPECULATION_RATE_THRESHOLD = 0.25  # 25 % of tasks speculative
MIN_TASKS_FOR_ANALYSIS = 3


class FailedTasksRule(Rule):
    def __init__(
        self,
        failure_rate: float = TASK_FAILURE_RATE_THRESHOLD,
        speculation_rate: float = TASK_SPECULATION_RATE_THRESHOLD,
    ) -> None:
        self.failure_rate = failure_rate
        self.speculation_rate = speculation_rate

    def analyze(self, app: Application) -> list[Finding]:
        findings = []
        for stage in app.stages.values():
            tasks = stage.tasks
            if len(tasks) < MIN_TASKS_FOR_ANALYSIS:
                continue

            total = len(tasks)
            failed = sum(1 for t in tasks if not t.successful)
            speculative = sum(1 for t in tasks if t.speculative)

            if failed / total >= self.failure_rate:
                findings.append(
                    Finding(
                        rule="failed-tasks",
                        severity=Severity.WARNING,
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        message=(
                            f"Stage {stage.stage_id} ({stage.name!r}): "
                            f"{failed} of {total} tasks failed "
                            f"({failed / total:.0%})"
                        ),
                        recommendation=(
                            "Check driver logs for the root cause. Common causes: OOM, "
                            "network timeouts, or corrupt input data. Consider increasing "
                            "executor memory or adding input validation."
                        ),
                    )
                )

            if speculative / total >= self.speculation_rate:
                findings.append(
                    Finding(
                        rule="failed-tasks",
                        severity=Severity.INFO,
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        message=(
                            f"Stage {stage.stage_id} ({stage.name!r}): "
                            f"{speculative} of {total} tasks were speculative "
                            f"({speculative / total:.0%})"
                        ),
                        recommendation=(
                            "High speculation rates indicate persistent stragglers. "
                            "Investigate data skew or node health, or enable "
                            "spark.speculation.multiplier tuning."
                        ),
                    )
                )

        return findings

    def describe(self) -> RuleInfo:
        return RuleInfo(
            id="failed-tasks",
            description="A stage has a high proportion of failed or speculative tasks",
            severity="WARNING / INFO",
            threshold=(
                f"failed tasks ≥ {self.failure_rate:.0%} (WARNING); "
                f"speculative tasks ≥ {self.speculation_rate:.0%} (INFO)"
            ),
        )
