from ignis.parser.models import Application

from .base import Finding, Rule, RuleInfo, Severity

GC_RATIO_THRESHOLD = 0.10  # GC time ≥ 10 % of executor run time


class GCPressureRule(Rule):
    def __init__(self, gc_ratio: float = GC_RATIO_THRESHOLD) -> None:
        self.gc_ratio = gc_ratio

    def analyze(self, app: Application) -> list[Finding]:
        findings = []
        for stage in app.stages.values():
            successful = stage.successful_tasks
            if not successful:
                continue

            total_gc = sum(t.metrics.gc_time_ms for t in successful)
            total_run = sum(t.metrics.executor_run_time_ms for t in successful)

            if total_run == 0:
                continue

            ratio = total_gc / total_run
            if ratio >= self.gc_ratio:
                findings.append(
                    Finding(
                        rule="gc-pressure",
                        severity=Severity.WARNING,
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        message=(
                            f"Stage {stage.stage_id} ({stage.name!r}): "
                            f"GC consumed {ratio:.0%} of executor run time "
                            f"({total_gc:,} ms GC / {total_run:,} ms run)"
                        ),
                        recommendation=(
                            "Reduce object churn by using primitive types or Datasets instead "
                            "of RDDs, increase executor memory, or tune GC settings via "
                            "spark.executor.extraJavaOptions=-XX:+UseG1GC."
                        ),
                    )
                )

        return findings

    def describe(self) -> RuleInfo:
        return RuleInfo(
            id="gc-pressure",
            description="JVM garbage collection consumes a large fraction of executor run time",
            severity="WARNING",
            threshold=f"GC time ≥ {self.gc_ratio:.0%} of total executor run time",
        )
