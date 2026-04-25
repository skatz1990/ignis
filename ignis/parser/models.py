from dataclasses import dataclass, field


@dataclass
class TaskMetrics:
    duration_ms: int
    executor_run_time_ms: int = 0
    gc_time_ms: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spill_bytes: int = 0
    disk_spill_bytes: int = 0


@dataclass
class Task:
    task_id: int
    stage_id: int
    stage_attempt_id: int
    successful: bool
    speculative: bool = False
    metrics: TaskMetrics | None = None


@dataclass
class Stage:
    stage_id: int
    stage_attempt_id: int
    name: str
    num_tasks: int = 0  # Configured partition count from Stage Info
    tasks: list[Task] = field(default_factory=list)

    @property
    def successful_tasks(self) -> list[Task]:
        return [t for t in self.tasks if t.successful and t.metrics is not None]


@dataclass
class Application:
    app_id: str
    app_name: str
    stages: dict[tuple[int, int], Stage] = field(default_factory=dict)
    executor_cores: int = 0  # Total cores across all executors
