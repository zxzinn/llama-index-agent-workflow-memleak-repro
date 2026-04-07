# AgentWorkflow memory leak reproduction

`AgentWorkflow.run()` pins per-request objects (~30-60 MB) in memory for the duration of the workflow timeout (default 600s) via `ContextVar` + asyncio `TimerHandle` context snapshots.

Not a permanent leak — objects are released when the timer fires. But under sustained load (1 req/s), steady state = 600 × 30-60 MB = 18-36 GB → OOM.

## Run

```
uv run python repro.py
```

## Output

```
Part 1: 600s timer — objects pinned until timer fires
-------------------------------------------------------
  After 10 requests: 10 AgentWorkflow pinned
  After 20 requests: 20 AgentWorkflow pinned
  After 30 requests: 30 AgentWorkflow pinned
  After 40 requests: 40 AgentWorkflow pinned
  After 50 requests: 50 AgentWorkflow pinned

  At 1 req/s steady state: 600 × ~30-60 MB = 18-36 GB → OOM

Part 2: 2s timer — proving objects are released after timer fires
-------------------------------------------------------
  Immediately after: 10 AgentWorkflow pinned
  After 3s (timer fired): 0 AgentWorkflow pinned

  Confirmed: not a permanent leak, but 600s retention under load = OOM.
```

## Issue

https://github.com/run-llama/workflows-py/issues/485
