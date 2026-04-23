from ignis.parser.models import Application

from .base import Finding, Rule, RuleInfo, Severity

# Any disk spill is worth flagging — disk I/O is 10-100x slower than memory.
DISK_SPILL_THRESHOLD_BYTES = 0
# Memory spill at this scale indicates serious memory pressure.
MEMORY_SPILL_THRESHOLD_BYTES = 524_288_000  # 500 MB


def _fmt_bytes(n: int) -> str:
    if n >= 1_073_741_824:
        return f"{n / 1_073_741_824:.2f} GB"
    if n >= 1_048_576:
        return f"{n / 1_048_576:.1f} MB"
    return f"{n:,} B"


class SpillRule(Rule):
    def __init__(self, memory_threshold_bytes: int = MEMORY_SPILL_THRESHOLD_BYTES) -> None:
        self.memory_threshold_bytes = memory_threshold_bytes

    def analyze(self, app: Application) -> list[Finding]:
        findings = []
        for stage in app.stages.values():
            successful = stage.successful_tasks
            if not successful:
                continue

            # Disk spill — flag any non-zero amount.
            spilling = [
                t for t in successful if t.metrics.disk_spill_bytes > DISK_SPILL_THRESHOLD_BYTES
            ]
            if spilling:
                total_disk = sum(t.metrics.disk_spill_bytes for t in successful)
                worst = max(spilling, key=lambda t: t.metrics.disk_spill_bytes)
                findings.append(
                    Finding(
                        rule="spill",
                        severity=Severity.WARNING,
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        message=(
                            f"Stage {stage.stage_id} ({stage.name!r}): "
                            f"{len(spilling)} task(s) spilled to disk; "
                            f"total {_fmt_bytes(total_disk)}, "
                            f"worst task {worst.task_id} spilled "
                            f"{_fmt_bytes(worst.metrics.disk_spill_bytes)}"
                        ),
                        recommendation=(
                            "Increase executor memory or raise spark.sql.shuffle.partitions "
                            "to reduce per-task data volume and eliminate spill."
                        ),
                    )
                )

            # Memory spill — flag only when the total is significant.
            total_memory = sum(t.metrics.memory_spill_bytes for t in successful)
            if total_memory >= self.memory_threshold_bytes:
                findings.append(
                    Finding(
                        rule="spill",
                        severity=Severity.INFO,
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        message=(
                            f"Stage {stage.stage_id} ({stage.name!r}): "
                            f"tasks spilled {_fmt_bytes(total_memory)} to memory "
                            f"— executor memory is under pressure"
                        ),
                        recommendation=(
                            "Memory spill is a precursor to disk spill. "
                            "Consider increasing executor memory or reducing partition size."
                        ),
                    )
                )

        return findings

    def describe(self) -> RuleInfo:
        mem_mb = self.memory_threshold_bytes // 1_048_576
        return RuleInfo(
            id="spill",
            description="Tasks spill execution data to disk or show significant memory pressure",
            severity="WARNING / INFO",
            threshold=f"any disk spill (WARNING); memory spill ≥ {mem_mb} MB (INFO)",
        )
