# Ignis Rules Reference

How ignis detects each problem, why the detection works the way it does, and what the findings mean.

---

## Spark Fundamentals

Before the rules, a short primer on the Spark concepts they operate on.

### Jobs, Stages, and Tasks

When you call an action (`.count()`, `.write()`, etc.) on a Spark DataFrame, Spark compiles it into a **job**. Each job is divided into **stages** — groups of operations that can be pipelined together without moving data between executors. Stage boundaries occur wherever a **shuffle** is required.

Within a stage, Spark creates one **task** per input partition. All tasks in a stage run in parallel across the cluster. A stage completes only when its last task finishes — meaning a single slow task holds up everything downstream.

### Shuffles

A shuffle is a full redistribution of data across the cluster. When Spark needs to regroup rows by a new key (for `groupBy`, `join`, `repartition`, `sort`), it must:

1. **Shuffle write** — each map task partitions its output rows by the target key and writes them to local disk, one file per downstream partition.
2. **Network transfer** — each reduce task fetches its partition from every map task across the network.
3. **Shuffle read** — reduce tasks read the fetched data and proceed with computation.

Shuffles are expensive. They introduce disk I/O, network traffic, and memory pressure. Most performance problems in Spark trace back to a shuffle in some way.

### Event Logs

Spark can write a structured log of everything that happened during a job to a file (or directory in Spark 4.0+). Each line is a JSON object describing one event: application start, stage submission, task completion, etc. Task completion events (`SparkListenerTaskEnd`) include detailed metrics — duration, shuffle bytes read/written, memory and disk spill, and more.

Ignis parses these logs and applies rules to the collected metrics.

---

## Rule: Data Skew

**ID:** `data-skew` | **Implemented in** `ignis/rules/skew.py`

### What it is

Data skew occurs when rows are distributed unevenly across tasks in a shuffle stage. In a `groupBy` or `join`, all rows with the same key are routed to the same reduce task. If one key accounts for a disproportionate share of rows — a classic example is `null` keys, or a single customer ID representing 80% of a dataset — that task processes far more data than its peers.

### Why it matters

A stage finishes when its last task finishes. If one task handles 10× more data than the others, the entire stage takes 10× longer than it should — even if 99 other tasks completed in one second. Everything downstream waits.

### How ignis detects it

For each stage with at least 3 successful tasks, ignis computes:

```
median_duration = median(task.duration_ms for task in successful_tasks)
max_duration    = max(task.duration_ms for task in successful_tasks)
ratio           = max_duration / median_duration
```

If `ratio ≥ SKEW_RATIO_THRESHOLD` (default: **5.0×**), a WARNING is reported.

**Why median, not mean?**
The outlier itself inflates the mean, which understates the ratio. Consider durations of `[1s, 1s, 1s, 50s]`:
- Mean = 13.25s → ratio = 50 / 13.25 = **3.8×** (looks mild)
- Median = 1s → ratio = 50 / 1 = **50×** (reflects reality)

The median represents what a "normal" task looks like, independently of the outlier.

**Why task duration, not record count?**
Record counts aren't reliably present across Spark versions and log formats. Duration directly measures the impact on wall-clock job time, which is what actually matters.

**Why ≥ 3 tasks?**
With only 1 or 2 tasks, the ratio is meaningless — there's no meaningful "normal" baseline to compare against.

---

## Rule: Shuffle Size

**ID:** `shuffle-size` | **Implemented in** `ignis/rules/shuffle.py`

### What it is

Shuffle size is the total volume of data written to shuffle files by a stage. Every time Spark performs a shuffle, map tasks write their partitioned output to local disk. The more data written, the more disk I/O, network bandwidth, and memory the shuffle consumes.

### Why it matters

Large shuffles are a leading cause of slow jobs:

- **Disk I/O**: writing and reading gigabytes on every executor adds direct latency.
- **Network pressure**: all that data crosses the network between map and reduce tasks.
- **Memory pressure**: Spark buffers shuffle data in memory before spilling to disk. Large shuffles push executors toward spill (see the Spill rule).
- **Long tail**: a large shuffle amplifies any skew — if one reduce partition receives disproportionately more data, the gap in bytes is larger, and so is the slowdown.

As a rule of thumb, a stage writing more than 1 GB to shuffle is doing more data movement than is typical for a well-tuned job.

### How ignis detects it

For each stage with at least one successful task:

```
total_bytes = sum(task.metrics.shuffle_write_bytes for task in successful_tasks)
```

If `total_bytes ≥ SHUFFLE_WRITE_THRESHOLD_BYTES` (default: **1 GB**), a WARNING is reported.

The metric used is `Shuffle Bytes Written` from Spark's `ShuffleWriteMetrics`. It reflects bytes actually written to shuffle files after Spark's internal serialization and compression. It is consistent across Spark 3.x and 4.x.

**Implementation note — column pruning**
Spark's query optimizer can project away columns that aren't needed by downstream operations before the shuffle write. For example, `df.repartition("key").count()` causes Spark to drop all non-key columns before writing shuffle output, because `count()` only needs rows to exist. This makes `Shuffle Bytes Written` appear small even when the DataFrame has large payloads. The metric correctly reflects what Spark actually shuffled — if the number looks low, it's worth checking whether columns are being pruned upstream.

---

## Rule: Spill

**ID:** `spill` | **Implemented in** `ignis/rules/spill.py`

### What it is

Memory spill occurs when Spark's execution memory is exhausted during a shuffle or sort. Rather than fail, Spark writes intermediate data to local disk as a temporary measure. When the operation resumes, it reads that data back from disk.

There are two types:
- **Disk spill** (`Disk Bytes Spilled`): data physically written to disk. This is the expensive one.
- **Memory spill** (`Memory Bytes Spilled`): data serialized in memory before being written to disk. A leading indicator of memory pressure.

### Why it matters

Local disk I/O is typically 10–100× slower than in-memory operations. Even a modest amount of disk spill can dramatically extend stage runtime — and the cause isn't always obvious from wall-clock time alone. A stage that "should" take 30 seconds can take 10 minutes if executors are repeatedly spilling and re-reading hundreds of MB per task.

### How ignis detects it

For each stage with at least one successful task:

- **Any non-zero disk spill** across tasks → WARNING. There is no minimum threshold because disk spill is always a sign that executor memory is undersized relative to the data being processed. The finding reports the worst offending task and the total spill across the stage.
- **Total memory spill ≥ `MEMORY_SPILL_THRESHOLD_BYTES`** (default: **500 MB**) → INFO. Memory spill alone is a leading indicator — data hasn't hit disk yet but the executor is under pressure.

**Recommendation**
Increase `--executor-memory` or `--driver-memory`, or reduce the shuffle partition size so each task handles less data at once.

---

## Rule: Partition Count

**ID:** `partition-count` | **Implemented in** `ignis/rules/partition.py`

### What it is

The number of partitions in a shuffle stage determines how many parallel reduce tasks Spark creates. This is controlled by `spark.sql.shuffle.partitions` (default: 200). Too few and the cluster is under-utilized; too many and the overhead of scheduling, serializing, and coordinating thousands of tiny tasks slows the job down.

### Why it matters

**Too few partitions**
If you have 32 executor cores and a shuffle produces 10 partitions, only 10 cores are active at once. The other 22 sit idle. Runtime is at least 3× longer than it needs to be.

**Too many partitions**
Each task carries overhead: scheduling on the driver, JVM thread startup, result serialization, metadata tracking. With 50,000 partitions processing a 1 GB dataset, each task handles 20 KB — the overhead per task exceeds the compute time. The driver becomes a bottleneck.

A healthy partition count for a shuffle stage is typically **2–4× the total number of executor cores** available to the job.

### How ignis detects it

Ignis parses `SparkListenerExecutorAdded` events to determine total available executor cores, then checks each shuffle-read stage (stages that consume shuffle output — reduce and join stages, not map stages):

- **`num_tasks < 2 × total_cores`** → WARNING (under-parallelism). Recommendation: raise `spark.sql.shuffle.partitions` to at least `2 × total_cores`.
- **`num_tasks > 10,000`** → WARNING (scheduling overhead). Recommendation: lower `spark.sql.shuffle.partitions`; a good starting point is 2–4× your executor core count.

`num_tasks` is taken from the `Number of Tasks` field in `SparkListenerStageSubmitted`, which reflects the configured partition count — not the number of task events in the log.
