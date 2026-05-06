# NeoTask API Reference

## TaskPool API

### Constructor

```python
TaskPool(executor=None, config=None)
```

| Param | Type | Default | Description |
|---|---|---|---|
| `executor` | `Callable \| TaskExecutor` | `None` | Async or sync callable; auto-wrapped if sync |
| `config` | `TaskPoolConfig` | `TaskPoolConfig()` | Pool configuration |

### Submission

| Method | Returns | Description |
|---|---|---|
| `submit(data, task_id=None, priority=NORMAL, delay=0, ttl=3600)` | `str` | Submit task, returns task_id |
| `submit_async(data, ...)` | `str` | Async submit |
| `submit_batch(tasks, priority=NORMAL)` | `List[str]` | Batch submit |
| `submit_batch_async(tasks, ...)` | `List[str]` | Async batch submit |

### Waiting & Results

| Method | Returns | Description |
|---|---|---|
| `wait_for_result(task_id, timeout=300)` | `Any` | Block until task completes |
| `wait_for_result_async(task_id, ...)` | `Any` | Async wait |
| `wait_all(task_ids, timeout=300)` | `Dict[str, Any]` | Wait for all tasks |
| `wait_all_async(task_ids, ...)` | `Dict[str, Any]` | Async wait all |

### Querying

| Method | Returns | Description |
|---|---|---|
| `get_status(task_id)` | `str \| None` | Status: pending/running/completed/failed/cancelled |
| `get_result(task_id)` | `Dict \| None` | Full result dict |
| `get_task(task_id)` | `Dict \| None` | Full task info |
| `task_exists(task_id)` | `bool` | Check existence |

### Management

| Method | Returns | Description |
|---|---|---|
| `cancel(task_id)` | `bool` | Cancel pending task |
| `delete(task_id)` | `bool` | Delete task record |
| `retry(task_id, delay=0)` | `bool` | Retry failed task |
| `pause()` | `None` | Pause queue processing |
| `resume()` | `None` | Resume queue processing |
| `clear_queue()` | `None` | Clear all pending tasks |

### Monitoring

| Method | Returns | Description |
|---|---|---|
| `get_stats()` | `Dict` | Queue + task stats |
| `get_queue_size()` | `int` | Current queue length |
| `get_worker_stats()` | `Dict[int, Any]` | Per-worker stats |
| `get_health_status()` | `Dict` | System health |

### Lock Management

| Method | Returns | Description |
|---|---|---|
| `acquire_lock(task_id, ttl=30)` | `bool` | Acquire distributed lock |
| `release_lock(task_id)` | `bool` | Release lock |

### Lifecycle

| Method | Description |
|---|---|
| `start()` | Start pool (auto-called on submit) |
| `shutdown(graceful=True, timeout=30)` | Graceful shutdown |

---

## TaskScheduler API

### Constructor

```python
TaskScheduler(executor=None, config=None)
```

### Delayed Tasks

| Method | Returns | Description |
|---|---|---|
| `submit_delayed(data, delay_seconds, ...)` | `str` | Execute after N seconds |
| `submit_at(data, execute_at, ...)` | `str` | Execute at specific datetime |

### Periodic Tasks

| Method | Returns | Description |
|---|---|---|
| `submit_interval(data, interval_seconds, ...)` | `str` | Repeat every N seconds |
| `submit_cron(data, cron_expr, ...)` | `str` | Cron-based scheduling |

### Periodic Management

| Method | Returns | Description |
|---|---|---|
| `cancel_periodic(task_id)` | `bool` | Stop periodic task |
| `pause_periodic(task_id)` | `bool` | Pause periodic task |
| `resume_periodic(task_id)` | `bool` | Resume periodic task |
| `get_periodic_tasks()` | `List[Dict]` | List all periodic tasks |
| `get_periodic_task(task_id)` | `Dict \| None` | Get single periodic task |

### Delegated Methods (to TaskPool)

`submit`, `wait_for_result`, `get_status`, `get_result`, `get_task`, `cancel`, `delete`, `retry`, `get_stats`, `pause`, `resume`, `clear_queue`, `on_created`, `on_started`, `on_completed`, `on_failed`, `on_cancelled`

---

## Cron Expression Format

Standard 5-field format: `minute hour day month weekday`

| Field | Range | Special chars |
|---|---|---|
| minute | 0-59 | `* , - /` |
| hour | 0-23 | `* , - /` |
| day | 1-31 | `* , - /` |
| month | 1-12 | `* , - /` |
| weekday | 0-6 (Sun=0) | `* , - /` |

Predefined: `@yearly`, `@monthly`, `@weekly`, `@daily`, `@hourly`

Examples:
- `0 9 * * *` — 9:00 AM daily
- `*/5 * * * *` — Every 5 minutes
- `0 2 * * 1` — 2:00 AM every Monday
- `0 0 1 * *` — Midnight on 1st of month
