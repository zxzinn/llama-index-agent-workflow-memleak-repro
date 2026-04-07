"""
AgentWorkflow pins per-request objects via ContextVar + asyncio TimerHandle.

Any call_later/call_at during a workflow run (e.g. aiohttp TCPConnector cleanup)
captures contextvars.Context containing _current_run → RunContext → AgentWorkflow
→ agents, LLM clients, memory blocks. Pinned until timer fires or session closes.

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


class FakeLLM(FunctionCallingLLM):
    """Stub LLM with 1 MB ballast. Registers a timer during achat like aiohttp would."""

    _ballast: bytes

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._ballast = b'\x00' * 1024 * 1024

    @property
    def metadata(self) -> LLMMetadata:
        return LLMMetadata(model_name='fake', is_function_calling_model=True)

    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        return ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content='Hi'))

    async def achat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        # Simulates aiohttp TCPConnector._cleanup_closed registering a periodic timer.
        # The timer captures the current contextvars.Context, which at this point
        # contains _current_run → RunContext → AgentWorkflow → this LLM → _ballast.
        asyncio.get_running_loop().call_later(600, lambda: None)
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


async def run_one():
    llm = FakeLLM()
    wf = AgentWorkflow(
        agents=[FunctionAgent(name='t', description='t', llm=llm, system_prompt='Hi.')],
        timeout=600,
    )
    h = wf.run(user_msg='hi', memory=ChatMemoryBuffer.from_defaults())
    async for _ in h.stream_events():
        pass
    await h
    # wf, h, llm all go out of scope — but AgentWorkflow stays alive


async def main():
    baseline = count('AgentWorkflow')
    for batch in range(1, 6):
        for _ in range(10):
            await run_one()
        n = count('AgentWorkflow') - baseline
        print(f'After {batch * 10:>2d} requests: {n} AgentWorkflow objects leaked')

    print()
    print(f'Expected: 0 (all workflows completed and out of scope)')
    print(f'Actual:   {count("AgentWorkflow") - baseline} pinned by TimerHandle → Context → RunContext')


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
    loop.close()
