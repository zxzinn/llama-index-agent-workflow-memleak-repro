# AgentWorkflow memory leak reproduction

`AgentWorkflow.run()` pins per-request objects in memory via `ContextVar` + asyncio `TimerHandle` context snapshots.

## Run

```
uv run python repro.py
```

## Output

```
Part 1: one-shot 600s timer
----------------------------------------
  After 10 requests: 10 AgentWorkflow leaked
  After 20 requests: 20 AgentWorkflow leaked
  After 30 requests: 30 AgentWorkflow leaked
  After 40 requests: 40 AgentWorkflow leaked
  After 50 requests: 50 AgentWorkflow leaked

  Objects pinned for 600s then released.
  At 1 req/s steady state: 600 × ~30-60 MB = 18-36 GB

Part 2: periodic timer (simulates aiohttp TCPConnector)
----------------------------------------
  After 10 requests: 10 AgentWorkflow leaked
  After 20 requests: 20 AgentWorkflow leaked
  After 30 requests: 30 AgentWorkflow leaked

  Waiting 3s (periodic timers keep re-registering)...
  After waiting:  30 AgentWorkflow still leaked (permanent)

  Cancelling all periodic timers...
  After cancel:   1 AgentWorkflow (freed only after explicit cancel)
```

Part 1 shows delayed release (objects freed after 600s timer fires).
Part 2 shows permanent leak — aiohttp's `TCPConnector` registers periodic cleanup timers via `call_at()` that re-register on every fire. Without `session.close()`, these timers (and the pinned objects) live forever.
