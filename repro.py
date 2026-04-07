"""
AgentWorkflow PERMANENT memory leak via ContextVar + periodic timer context propagation.

Root cause: CPython's Handle._run() executes the callback inside the handle's
own _context (via self._context.run(self._callback, *self._args)). When a
periodic timer (like aiohttp's TCPConnector._cleanup_closed) re-registers
itself via call_at(), the new handle's copy_context() copies the CURRENT
running context — which is the old handle's context. So the ContextVar value
(_current_run → RunContext → AgentWorkflow) is inherited forever.

Part 1: one-shot timer — objects pinned until timer fires (not permanent).
Part 2: periodic timer — objects pinned FOREVER (permanent leak).

    pip install llama-index-core==0.14.20 llama-index-workflows==2.17.1
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

_periodic_handles: list[asyncio.TimerHandle] = []


class FakeLLM(FunctionCallingLLM):
    """Stub LLM with 1 MB ballast. Registers a timer during achat."""

    _ballast: bytes
    _timer_mode: str  # 'oneshot', 'periodic'
    _timer_delay: float

    def __init__(self, timer_mode: str = 'oneshot', timer_delay: float = 3.0, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._ballast = b'\x00' * 1024 * 1024
        self._timer_mode = timer_mode
        self._timer_delay = timer_delay

    @property
    def metadata(self) -> LLMMetadata:
        return LLMMetadata(model_name='fake', is_function_calling_model=True)

    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        return ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content='Hi'))

    async def achat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        loop = asyncio.get_running_loop()
        if self._timer_mode == 'periodic':
            # Simulates aiohttp TCPConnector._cleanup_closed: a periodic timer
            # that re-registers itself inside its own callback.
            #
            # CPython's Handle._run() calls self._context.run(callback, *args),
            # so the callback runs in the handle's Context. When the callback
            # does call_later() → copy_context(), it copies the handle's Context
            # (which contains _current_run → RunContext → AgentWorkflow).
            # The new handle inherits RunContext. This repeats forever.
            def _periodic():
                h = loop.call_later(self._timer_delay, _periodic)
                _periodic_handles.append(h)

            h = loop.call_later(self._timer_delay, _periodic)
            _periodic_handles.append(h)
        else:
            loop.call_later(self._timer_delay, lambda: None)
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


async def run_one(timer_mode: str = 'oneshot', timer_delay: float = 3.0):
    llm = FakeLLM(timer_mode=timer_mode, timer_delay=timer_delay)
    wf = AgentWorkflow(
        agents=[FunctionAgent(name='t', description='t', llm=llm, system_prompt='Hi.')],
        timeout=600,
    )
    h = wf.run(user_msg='hi', memory=ChatMemoryBuffer.from_defaults())
    async for _ in h.stream_events():
        pass
    await h


async def part1():
    """One-shot timer: objects released after timer fires."""
    print('Part 1: one-shot 3s timer — released after timer fires')
    print('-' * 55)

    for _ in range(10):
        await run_one(timer_mode='oneshot', timer_delay=3.0)
    print(f'  Immediately: {count("AgentWorkflow")} AgentWorkflow pinned')

    await asyncio.sleep(5)
    print(f'  After 5s:    {count("AgentWorkflow")} AgentWorkflow (released)')


async def part2():
    """Periodic timer: objects NEVER released (permanent leak)."""
    print()
    print('Part 2: periodic timer — PERMANENT leak')
    print('-' * 55)
    print('  (simulates aiohttp TCPConnector._cleanup_closed)')
    print()

    _periodic_handles.clear()

    for _ in range(10):
        await run_one(timer_mode='periodic', timer_delay=0.5)
    print(f'  Immediately:  {count("AgentWorkflow")} AgentWorkflow pinned')

    # Let periodic timers re-register many times
    await asyncio.sleep(5)
    print(f'  After 5s:     {count("AgentWorkflow")} AgentWorkflow STILL pinned')

    await asyncio.sleep(5)
    print(f'  After 10s:    {count("AgentWorkflow")} AgentWorkflow STILL pinned (permanent)')

    print()
    print('  Why permanent: CPython Handle._run() executes callback in the')
    print("  handle's own Context. Periodic re-register copies that Context,")
    print('  inheriting RunContext → AgentWorkflow forever.')

    # Prove: cancelling the periodic timers frees everything
    print()
    for h in _periodic_handles:
        h.cancel()
    _periodic_handles.clear()
    await asyncio.sleep(1)
    gc.collect()
    print(f'  After cancel: {count("AgentWorkflow")} AgentWorkflow (freed)')


if __name__ == '__main__':
    loop1 = asyncio.new_event_loop()
    loop1.run_until_complete(part1())
    loop1.close()

    loop2 = asyncio.new_event_loop()
    loop2.run_until_complete(part2())
    loop2.close()
