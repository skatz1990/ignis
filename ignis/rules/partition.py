from ignis.parser.models import Application

from .base import Finding, Rule, RuleInfo, Severity

# Fewer than 2x executor cores leaves half the cluster idle on average.
MIN_TASKS_PER_CORE = 2
# Beyond 10k partitions, scheduling overhead starts to dominate.
MAX_PARTITION_COUNT = 10_000


class PartitionCountRule(Rule):
    def __init__(
        self,
        min_tasks_per_core: int = MIN_TASKS_PER_CORE,
        max_partitions: int = MAX_PARTITION_COUNT,
    ) -> None:
        self.min_tasks_per_core = min_tasks_per_core
        self.max_partitions = max_partitions

    def analyze(self, app: Application) -> list[Finding]:
        findings = []
        for stage in app.stages.values():
            successful = stage.successful_tasks
            if not successful:
                continue

            # Only check shuffle-read stages — their partition count is
            # controlled by spark.sql.shuffle.partitions and is actionable.
            if not any(t.metrics.shuffle_read_bytes > 0 for t in successful):
                continue

            # num_tasks from Stage Info is the configured partition count.
            # Fall back to len(tasks) for stubs created without StageSubmitted.
            task_count = stage.num_tasks or len(stage.tasks)

            if task_count > self.max_partitions:
                findings.append(
                    Finding(
                        rule="partition-count",
                        severity=Severity.WARNING,
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        message=(
                            f"Stage {stage.stage_id} ({stage.name!r}): "
                            f"{task_count:,} shuffle partitions is likely too many "
                            f"— scheduling overhead will dominate task compute time"
                        ),
                        recommendation=(
                            "Lower spark.sql.shuffle.partitions. "
                            "A good starting point is 2–4× your total executor core count."
                        ),
                    )
                )
            elif (
                app.executor_cores > 0 and task_count < self.min_tasks_per_core * app.executor_cores
            ):
                findings.append(
                    Finding(
                        rule="partition-count",
                        severity=Severity.WARNING,
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        message=(
                            f"Stage {stage.stage_id} ({stage.name!r}): "
                            f"{task_count} shuffle partition(s) across {app.executor_cores} "
                            f"executor core(s) — cluster is under-utilized"
                        ),
                        recommendation=(
                            f"Raise spark.sql.shuffle.partitions to at least "
                            f"{self.min_tasks_per_core * app.executor_cores} "
                            f"({self.min_tasks_per_core}× your "
                            f"{app.executor_cores} executor cores)."
                        ),
                    )
                )
        return findings

    def describe(self) -> RuleInfo:
        return RuleInfo(
            id="partition-count",
            description="Shuffle partition count leaves the cluster idle or overwhelms the driver",
            severity="WARNING",
            threshold=(
                f"< {self.min_tasks_per_core}× executor cores or "
                f"> {self.max_partitions:,} partitions"
            ),
        )
