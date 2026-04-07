"""
AgentWorkflow pins per-request objects via ContextVar + asyncio TimerHandle.

Any call_later/call_at during a workflow run captures contextvars.Context
containing _current_run → RunContext → AgentWorkflow → agents, LLM clients,
memory blocks. Pinned until the timer fires (default workflow timeout: 600s).

Not a permanent leak — objects ARE released when the timer fires. But under
sustained load (1 req/s), steady state = 600 × 30-60 MB = 18-36 GB → OOM.

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


class FakeLLM(FunctionCallingLLM):
    """Stub LLM with 1 MB ballast. Registers a timer during achat."""

    _ballast: bytes
    _timer_delay: float

    def __init__(self, timer_delay: float = 600.0, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._ballast = b'\x00' * 1024 * 1024
        self._timer_delay = timer_delay

    @property
    def metadata(self) -> LLMMetadata:
        return LLMMetadata(model_name='fake', is_function_calling_model=True)

    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        return ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content='Hi'))

    async def achat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        # Simulates what happens in practice: any async library (aiohttp, aioboto3)
        # that calls loop.call_later() or loop.call_at() during a workflow run.
        # The TimerHandle captures the current contextvars.Context, which contains
        # _current_run → RunContext → AgentWorkflow → this LLM → _ballast.
        asyncio.get_running_loop().call_later(self._timer_delay, lambda: None)
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


async def run_one(timer_delay: float = 600.0):
    llm = FakeLLM(timer_delay=timer_delay)
    wf = AgentWorkflow(
        agents=[FunctionAgent(name='t', description='t', llm=llm, system_prompt='Hi.')],
        timeout=600,
    )
    h = wf.run(user_msg='hi', memory=ChatMemoryBuffer.from_defaults())
    async for _ in h.stream_events():
        pass
    await h


async def main():
    # --- Part 1: objects pinned for timeout duration ---
    print('Part 1: 600s timer — objects pinned until timer fires')
    print('-' * 55)

    baseline = count('AgentWorkflow')
    for batch in range(1, 6):
        for _ in range(10):
            await run_one(timer_delay=600.0)
        n = count('AgentWorkflow') - baseline
        print(f'  After {batch * 10:>2d} requests: {n} AgentWorkflow pinned')

    print()
    print('  At 1 req/s steady state: 600 × ~30-60 MB = 18-36 GB → OOM')

async def part2():
    """Run in a separate event loop to avoid Part 1 timer contamination."""
    print()
    print('Part 2: 2s timer — proving objects are released after timer fires')
    print('-' * 55)

    for _ in range(10):
        await run_one(timer_delay=2.0)
    pinned = count('AgentWorkflow')
    print(f'  Immediately after: {pinned} AgentWorkflow pinned')

    await asyncio.sleep(3)
    remaining = count('AgentWorkflow')
    print(f'  After 3s (timer fired): {remaining} AgentWorkflow pinned')
    print()
    print('  Confirmed: not a permanent leak, but 600s retention under load = OOM.')


if __name__ == '__main__':
    loop1 = asyncio.new_event_loop()
    loop1.run_until_complete(main())
    loop1.close()

    loop2 = asyncio.new_event_loop()
    loop2.run_until_complete(part2())
    loop2.close()
