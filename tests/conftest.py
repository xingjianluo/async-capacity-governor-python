"""pytest 公共配置。

- 打开 governor 日志：单测可看到请求标识、准入判定与耗时分解；
- 自驱动 async 测试：每个 async 测试用独立事件循环运行，
  结束后取消并回收全部遗留任务（无需 pytest-asyncio 插件）。
"""

from __future__ import annotations

import asyncio
import inspect
import logging

import pytest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)


def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:  # noqa: ANN001
    func = pyfuncitem.obj
    if not inspect.iscoroutinefunction(func):
        return None
    sig = inspect.signature(func)
    kwargs = {
        name: pyfuncitem.funcargs[name]
        for name in sig.parameters
        if name in pyfuncitem.funcargs
    }
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(func(**kwargs))
        pending = asyncio.all_tasks(loop)
        for t in pending:
            t.cancel()
        if pending:
            loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )
        loop.run_until_complete(loop.shutdown_asyncgens())
    finally:
        asyncio.set_event_loop(None)
        loop.close()
    return True
