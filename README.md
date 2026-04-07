# AgentWorkflow memory leak reproduction

`AgentWorkflow.run()` permanently pins per-request objects (~30-60 MB) in memory when combined with periodic asyncio timers (like aiohttp's `TCPConnector._cleanup_closed`).

Root cause: CPython's `Handle._run()` executes the callback inside the handle's own `_context`. When a periodic timer re-registers via `call_at()`, the new `copy_context()` copies the running context — which contains `_current_run` → `RunContext` → `AgentWorkflow`. The value is inherited forever.

## Run

```
uv run python repro.py
```

## Output

```
Part 1: one-shot 3s timer — released after timer fires
-------------------------------------------------------
  Immediately: 10 AgentWorkflow pinned
  After 5s:    0 AgentWorkflow (released)

Part 2: periodic timer — PERMANENT leak
-------------------------------------------------------
  (simulates aiohttp TCPConnector._cleanup_closed)

  Immediately:  10 AgentWorkflow pinned
  After 5s:     10 AgentWorkflow STILL pinned
  After 10s:    10 AgentWorkflow STILL pinned (permanent)

  Why permanent: CPython Handle._run() executes callback in the
  handle's own Context. Periodic re-register copies that Context,
  inheriting RunContext → AgentWorkflow forever.

  After cancel: 1 AgentWorkflow (freed)
```

## Issue

https://github.com/run-llama/workflows-py/issues/485
