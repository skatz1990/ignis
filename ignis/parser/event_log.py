import json

import fsspec

from .models import Application, Stage, Task, TaskMetrics


def parse_event_log(path: str) -> Application:
    app = Application(app_id="unknown", app_name="unknown")

    with fsspec.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            _dispatch(event, app)

    return app


def _dispatch(event: dict, app: Application) -> None:
    event_type = event.get("Event", "")

    if event_type == "SparkListenerApplicationStart":
        app.app_id = event.get("App ID", "unknown")
        app.app_name = event.get("App Name", "unknown")

    elif event_type == "SparkListenerStageSubmitted":
        info = event.get("Stage Info", {})
        key = (info.get("Stage ID", 0), info.get("Stage Attempt ID", 0))
        app.stages[key] = Stage(
            stage_id=key[0],
            stage_attempt_id=key[1],
            name=info.get("Stage Name", ""),
        )

    elif event_type == "SparkListenerTaskEnd":
        _handle_task_end(event, app)

    # All other event types are silently ignored.


def _handle_task_end(event: dict, app: Application) -> None:
    stage_id = event.get("Stage ID", 0)
    attempt_id = event.get("Stage Attempt ID", 0)
    key = (stage_id, attempt_id)

    task_info = event.get("Task Info", {})
    launch_time = task_info.get("Launch Time", 0)
    finish_time = task_info.get("Finish Time", 0)
    # Duration can be null in some Spark versions; fall back to wall-clock diff.
    duration_ms = task_info.get("Duration") or (finish_time - launch_time)

    raw_metrics = event.get("Task Metrics")
    metrics = None
    if raw_metrics is not None:
        shuffle_read = raw_metrics.get("Shuffle Read Metrics", {})
        shuffle_write = raw_metrics.get("Shuffle Write Metrics", {})
        metrics = TaskMetrics(
            duration_ms=duration_ms,
            executor_run_time_ms=raw_metrics.get("Executor Run Time", 0),
            shuffle_read_bytes=(
                shuffle_read.get("Remote Bytes Read", 0) + shuffle_read.get("Local Bytes Read", 0)
            ),
            shuffle_write_bytes=shuffle_write.get("Shuffle Bytes Written", 0),
            memory_spill_bytes=raw_metrics.get("Memory Bytes Spilled", 0),
            disk_spill_bytes=raw_metrics.get("Disk Bytes Spilled", 0),
        )

    task = Task(
        task_id=task_info.get("Task ID", 0),
        stage_id=stage_id,
        stage_attempt_id=attempt_id,
        successful=not task_info.get("Failed", False) and not task_info.get("Killed", False),
        metrics=metrics,
    )

    if key not in app.stages:
        # StageSubmitted may have been missing; create a stub so tasks aren't lost.
        app.stages[key] = Stage(stage_id=stage_id, stage_attempt_id=attempt_id, name="unknown")

    app.stages[key].tasks.append(task)
