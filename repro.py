"""
AgentWorkflow pins per-request objects via ContextVar + asyncio TimerHandle.

Any call_later/call_at during a workflow run (e.g. aiohttp TCPConnector cleanup)
captures contextvars.Context containing _current_run → RunContext → AgentWorkflow
→ agents, LLM clients, memory blocks. Pinned until timer fires or session closes.

Part 1: One-shot timer (600s) — delayed release, objects freed after timer fires.
Part 2: Periodic timer (like aiohttp) — permanent leak, objects never freed.

    pip install llama-index-core llama-index-workflows
    python repro.py
"""

import asyncio
import gc
import logging
from collections.abc import Sequence
from typing import Any

from llama_index.core.agent.workflow import AgentWorkflow, FunctionAgent
from llama_index.core.base.llms.types import (
    ChatMessage,
    ChatResponse,
    ChatResponseAsyncGen,
    ChatResponseGen,
    CompletionResponse,
    CompletionResponseAsyncGen,
    CompletionResponseGen,
    LLMMetadata,
    MessageRole,
)
from llama_index.core.llms.function_calling import FunctionCallingLLM
from llama_index.core.memory import ChatMemoryBuffer

logging.disable(logging.CRITICAL)


# Collect periodic timer handles so we can stop them at the end
_periodic_handles: list[asyncio.TimerHandle] = []


class FakeLLM(FunctionCallingLLM):
    """Stub LLM with 1 MB ballast. Registers a timer during achat like aiohttp would."""

    _ballast: bytes
    _use_periodic_timer: bool

    def __init__(self, use_periodic_timer: bool = False, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._ballast = b'\x00' * 1024 * 1024
        self._use_periodic_timer = use_periodic_timer

    @property
    def metadata(self) -> LLMMetadata:
        return LLMMetadata(model_name='fake', is_function_calling_model=True)

    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        return ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content='Hi'))

    async def achat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        loop = asyncio.get_running_loop()
        if self._use_periodic_timer:
            # Simulates aiohttp TCPConnector._cleanup_closed: a periodic timer
            # that re-registers itself on every fire. Never stops unless explicitly
            # cancelled. Each fire captures a fresh Context, but the *first* one
            # (created during workflow run) pins RunContext → AgentWorkflow forever.
            def _periodic():
                handle = loop.call_later(0.5, _periodic)
                _periodic_handles.append(handle)

            handle = loop.call_later(0.5, _periodic)
            _periodic_handles.append(handle)
        else:
            # One-shot 600s timer. Objects pinned for 600 seconds then released.
            loop.call_later(600, lambda: None)
        return self.chat(messages)

    def complete(self, p: str, formatted: bool = False, **kw: Any) -> CompletionResponse:
        return CompletionResponse(text='Hi')

    async def acomplete(self, p: str, formatted: bool = False, **kw: Any) -> CompletionResponse:
        return self.complete(p)

    def stream_chat(self, m: Sequence[ChatMessage], **kw: Any) -> ChatResponseGen:
        yield self.chat(m)

    async def astream_chat(self, m: Sequence[ChatMessage], **kw: Any) -> ChatResponseAsyncGen:
        yield self.chat(m)

    def stream_complete(self, p: str, formatted: bool = False, **kw: Any) -> CompletionResponseGen:
        yield self.complete(p)

    async def astream_complete(self, p: str, formatted: bool = False, **kw: Any) -> CompletionResponseAsyncGen:
        yield self.complete(p)

    def _prepare_chat_with_tools(self, tools, user_msg=None, chat_history=None, **kw):
        return {'messages': chat_history or []}

    def get_tool_calls_from_response(self, response, **kw):
        return []

    async def achat_with_tools(self, tools, user_msg=None, chat_history=None, **kw):
        return await self.achat(chat_history or [])

    async def astream_chat_with_tools(self, tools, user_msg=None, chat_history=None, **kw):
        async def _gen():
            yield await self.achat(chat_history or [])

        return _gen()


def count(name: str) -> int:
    gc.collect()
    return sum(1 for o in gc.get_objects() if type(o).__name__ == name)


async def run_one(use_periodic_timer: bool = False):
    llm = FakeLLM(use_periodic_timer=use_periodic_timer)
    wf = AgentWorkflow(
        agents=[FunctionAgent(name='t', description='t', llm=llm, system_prompt='Hi.')],
        timeout=600,
    )
    h = wf.run(user_msg='hi', memory=ChatMemoryBuffer.from_defaults())
    async for _ in h.stream_events():
        pass
    await h


async def main():
    # --- Part 1: one-shot 600s timer (delayed release) ---
    print('Part 1: one-shot 600s timer')
    print('-' * 40)

    baseline = count('AgentWorkflow')
    for batch in range(1, 6):
        for _ in range(10):
            await run_one(use_periodic_timer=False)
        n = count('AgentWorkflow') - baseline
        print(f'  After {batch * 10:>2d} requests: {n} AgentWorkflow leaked')

    print()
    print(f'  Objects pinned for 600s then released.')
    print(f'  At 1 req/s steady state: 600 × ~30-60 MB = 18-36 GB')

    # --- Part 2: periodic timer (permanent leak) ---
    print()
    print('Part 2: periodic timer (simulates aiohttp TCPConnector)')
    print('-' * 40)

    baseline2 = count('AgentWorkflow')
    for batch in range(1, 4):
        for _ in range(10):
            await run_one(use_periodic_timer=True)
        n = count('AgentWorkflow') - baseline2
        print(f'  After {batch * 10:>2d} requests: {n} AgentWorkflow leaked')

    print()
    print('  Waiting 3s (periodic timers keep re-registering)...')
    await asyncio.sleep(3)
    n = count('AgentWorkflow') - baseline2
    print(f'  After waiting:  {n} AgentWorkflow still leaked (permanent)')

    print()
    print('  Cancelling all periodic timers...')
    for h in _periodic_handles:
        h.cancel()
    _periodic_handles.clear()
    # Let the event loop process cancellations
    await asyncio.sleep(0.1)
    gc.collect()
    n = count('AgentWorkflow') - baseline2
    print(f'  After cancel:   {n} AgentWorkflow (freed only after explicit cancel)')


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
    loop.close()
