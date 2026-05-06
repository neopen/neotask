# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

NeoTask is a pure-Python async task queue (v0.4.0). Zero mandatory dependencies — runs on stdlib `asyncio`. Storage backends: memory, SQLite, Redis. Optional Web UI via FastAPI.

**Package layout**: `src/neotask/` (install with `pip install -e .`).

## Commands

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/

# Run a single test file/module
pytest tests/unit/test_task.py -v
pytest tests/unit/test_cron_parser.py -v

# Run only unit tests
pytest tests/unit/ -v

# Run only integration tests
pytest tests/integration/ -v

# Run with coverage
pytest --cov=src/neotask tests/

# Lint & format
ruff check src/ tests/
ruff format src/ tests/

# Type check
mypy src/neotask/

# Run an example
python examples/01_simple.py
```

Pre-commit hooks: `ruff`, `ruff-format`, `mypy`, `mdformat`, `trailing-whitespace`, `end-of-file-fixer`.

## Architecture

The internal pipeline flows through these layers:

```
User code → TaskPool / TaskScheduler  (public API, Facade over internals)
          → TaskDispatcher            (creates task via LifecycleManager, pushes to queue)
          → QueueScheduler            (PriorityQueue + DelayedQueue, wraps both in one interface)
          → WorkerPool                (N asyncio worker coroutines, optional TaskPrefetcher)
          → TaskExecutor              (async / thread / process execution)
          → EventBus                  (pub-sub lifecycle events: created→started→completed/failed)
          → TaskRepository            (abstracts memory / sqlite / redis persistence)
```

- **`TaskPool`** — immediate tasks. Submit → queue → workers → result. Sync+async API. Runs its own event loop in a daemon thread; sync methods use `run_coroutine_threadsafe`.
- **`TaskScheduler`** — wraps `TaskPool` and adds `submit_delayed`, `submit_interval`, `submit_cron`. Has its own scheduler loop thread that scans periodic tasks.
- **`TaskEngine`** — alternate async-only entry point (no implicit event loop thread, no sync API). Lower-level, more explicit control.
- **`QueueScheduler`** — facade over `PriorityQueue` (heap-based, priority 0=highest→3=lowest) and `DelayedQueue` (time-heap, auto-promotes to priority queue on expiry).
- **`WorkerPool`** — two modes: prefetch (TaskPrefetcher batches from shared queue into local queue, HYBRID strategy) or direct pop. Exponential backoff on empty queues.
- **`TaskLifecycleManager`** — CRUD + state machine. Validates transitions (PENDING→RUNNING→SUCCESS/FAILED/CANCELLED). In-memory cache + `FutureManager` for result awaiting.
- **`EventBus`** — async pub-sub. `subscribe(event_type, handler)` or `subscribe_global`. Events: `task.created`, `task.started`, `task.completed`, `task.failed`, `task.cancelled`, `task.retry`.
- **Storage** — Repository pattern: `TaskRepository` (task CRUD) + `QueueRepository` (queue push/pop). Three impls: `Memory*`, `SQLite*`, `Redis*`. `StorageFactory.create(config)` returns both.
- **Retry** — `WorkerPool._handle_task_failure` re-queues with delay = `retry_delay * retry_count`. After `max_retries` exceeded, marks task FAILED.

## Key patterns

- **Event loop ownership**: `TaskPool.start()` spawns a daemon `threading.Thread` running `asyncio.new_event_loop()`. All internal components share this loop. `TaskScheduler` borrows the pool's loop.
- **Sync/async dual API**: Every public method has `method()` (sync, calls `_run_coroutine`) and `method_async()` (async). Sync calls use `asyncio.run_coroutine_threadsafe` targeting the pool's loop.
- **Context manager**: `with TaskPool(...) as pool:` auto-calls `start()` and `shutdown()`.
- **Task ID format**: `TSK{YYMMDDHHmmssffff}{random6}` for TaskPool, `PRD_{timestamp}_{random6}` for periodic tasks.
- **Config factories**: Config classes use `@classmethod` factories — e.g. `StorageConfig.sqlite(path)`, `SchedulerConfig.high_performance()`.
