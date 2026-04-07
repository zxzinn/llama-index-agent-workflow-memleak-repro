# AgentWorkflow memory leak reproduction

`AgentWorkflow.run()` pins per-request objects (~30-60 MB) in memory via `ContextVar` + asyncio `TimerHandle` context snapshots.

## Run

```
uv run python repro.py
```

## Output

```
After 10 requests: 10 AgentWorkflow objects leaked
After 20 requests: 20 AgentWorkflow objects leaked
After 30 requests: 30 AgentWorkflow objects leaked
After 40 requests: 40 AgentWorkflow objects leaked
After 50 requests: 50 AgentWorkflow objects leaked

Expected: 0 (all workflows completed and out of scope)
Actual:   50 pinned by TimerHandle → Context → RunContext
```

## What's happening

`RunContext` (holding the entire `AgentWorkflow`) is stored in a `ContextVar`. `asyncio.create_task()` inside the workflow control loop snapshots the context. Any `call_later`/`call_at` during the run (e.g. aiohttp `TCPConnector` cleanup timer) also captures the context. Objects stay pinned until the timer fires or the session is closed.
