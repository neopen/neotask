"""
@FileName: redis_compat.py
@Description: Redis 建连兼容助手 - 统一 RESP 协议选择，并把异步客户端绑定到专用事件循环，
             避免 TaskPool 内部 loop 与调用方 loop 并发使用同一连接池导致跨 loop 报错。
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/03 00:00
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Awaitable, Callable, List, Optional, Tuple

from neotask.utils.dedicated_loop import get_shared_loop, run_on_loop

# 默认使用 RESP2（protocol=2）。
# redis-py 高版本可能默认以 RESP3 握手（连接时发送 HELLO 命令），而 Redis < 6.0
# 不支持 HELLO，会直接报 "unknown command 'HELLO'"，导致整个 Redis 后端开箱不可用
# （见 FINDINGS 2.4）。RESP2 对新老服务端都兼容。
DEFAULT_PROTOCOL = 2

# 允许通过环境变量覆盖协议版本（如确需 RESP3：NEOTASK_REDIS_PROTOCOL=3）
_PROTOCOL_ENV = "NEOTASK_REDIS_PROTOCOL"


def resolve_protocol(protocol: Optional[int] = None) -> int:
    """解析应使用的 RESP 协议版本。

    优先级：显式入参 > 环境变量 ``NEOTASK_REDIS_PROTOCOL`` > 默认 RESP2。
    """
    if protocol is not None:
        return protocol
    raw = os.getenv(_PROTOCOL_ENV)
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return DEFAULT_PROTOCOL


def from_url(redis_url: str, *, protocol: Optional[int] = None, **kwargs: Any):
    """基于 URL 创建 ``redis.asyncio.ConnectionPool``，默认使用 RESP2。

    - 显式传入 ``protocol`` 时优先采用；否则读取环境变量，最终回退 RESP2。
    - 旧版 redis-py（<5.0）不支持 ``protocol`` 关键字参数，捕获 ``TypeError``
      后自动降级为不传该参数，避免在低版本依赖上直接报错。
    """
    from redis.asyncio import ConnectionPool

    proto = resolve_protocol(protocol)
    try:
        return ConnectionPool.from_url(redis_url, protocol=proto, **kwargs)
    except TypeError:
        # redis-py < 5.0 不支持 protocol 关键字参数
        return ConnectionPool.from_url(redis_url, **kwargs)


# ========== 跨事件循环安全客户端 ==========
#
# 背景：redis-py 的 asyncio 连接池内部持有 ``asyncio.Lock`` 等与事件循环绑定的原语，
# 连接池在某个 loop 中首次使用后，就无法再从另一个 loop 访问，否则会抛
# ``RuntimeError: <asyncio.locks.Lock ...> is bound to a different event loop``。
# NeoTask 同时存在两种 loop：
#   1. ``TaskPool`` 内部守护线程中的事件循环（所有内部组件共享）；
#   2. 调用方自己的事件循环（``asyncio.run`` / pytest-asyncio / 用户 loop）。
# 若同一个 ``redis.Redis`` 客户端被两边共用，就会触发上述错误。
#
# 解决方案：进程内维护一个专用事件循环线程，所有 Redis 命令都通过
# ``run_coroutine_threadsafe`` 投递到该 loop 执行。客户端代理只负责转发，
# 从而与调用方 loop 完全解耦；``close()`` 也在专用 loop 上完成资源释放。


# 专用事件循环由 neotask.utils.dedicated_loop 统一提供（Redis 与 SQLite 共享同一 loop）

# 代理工厂签名：接收真实客户端，返回需要等待的协程
_ClientFactory = Callable[[Any], Awaitable[Any]]


class LoopBoundPipeline:
    """代理 ``redis.asyncio.client.Pipeline``。

    命令先记录在本地（纯 Python 操作，不触碰连接池），
    ``execute()`` 时把整批命令投递到专用 loop 统一执行。
    """

    def __init__(
            self,
            owner: "LoopBoundRedisClient",
            transaction: bool = True,
            shard_hint: Optional[str] = None,
    ) -> None:
        self._owner = owner
        self._transaction = transaction
        self._shard_hint = shard_hint
        self._commands: List[Tuple[str, Tuple[Any, ...], dict]] = []

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)

        def _enqueue(*args: Any, **kwargs: Any):
            self._commands.append((name, args, kwargs))
            return self

        return _enqueue

    def reset(self) -> "LoopBoundPipeline":
        """清空已排队的命令（与 redis-py Pipeline.reset 语义一致）。"""
        self._commands.clear()
        return self

    async def execute(self, raise_on_error: bool = True):
        """在专用 loop 中构建真实 pipeline 并执行全部命令。"""
        commands = list(self._commands)
        self._commands.clear()

        transaction = self._transaction
        shard_hint = self._shard_hint

        async def _factory(client: Any) -> Any:
            pipe = client.pipeline(transaction=transaction, shard_hint=shard_hint)
            for name, args, kwargs in commands:
                getattr(pipe, name)(*args, **kwargs)
            return await pipe.execute(raise_on_error=raise_on_error)

        return await self._owner._execute(_factory)


class LoopBoundScript:
    """代理 ``redis.asyncio.client.Script``。

    真实脚本对象在专用 loop 中被注册和执行，这里只保存 Lua 源码。
    """

    def __init__(self, owner: "LoopBoundRedisClient", script: str) -> None:
        self._owner = owner
        self._script = script

    async def __call__(self, keys: Optional[List[str]] = None, args: Optional[List[Any]] = None):
        script_source = self._script

        async def _factory(client: Any) -> Any:
            script = client.register_script(script_source)
            return await script(keys=keys or [], args=args or [])

        return await self._owner._execute(_factory)


class LoopBoundRedisClient:
    """绑定到专用事件循环的异步 Redis 客户端代理。

    用法与 ``redis.asyncio.Redis`` 基本一致：

    >>> client = LoopBoundRedisClient("redis://127.0.0.1:6379/0", decode_responses=True)
    >>> await client.set("k", "v")          # 任意 loop 中都可安全调用
    >>> await client.get("k")
    >>> pipe = client.pipeline()            # 与真实 Pipeline 用法一致
    >>> pipe.sadd("s", "a"); await pipe.execute()
    >>> script = client.register_script(lua)
    >>> await script(keys=["k"], args=[1])
    >>> await client.close()

    说明：未显式实现的方法通过 ``__getattr__`` 动态转发，
    因此 ``get/set/hset/zadd/scan/...`` 等命令无需逐个声明。
    """

    def __init__(
            self,
            redis_url: str,
            *,
            protocol: Optional[int] = None,
            **pool_kwargs: Any,
    ) -> None:
        self._redis_url = redis_url
        self._protocol = protocol
        self._pool_kwargs = pool_kwargs
        self._client: Optional[Any] = None
        self._bound_loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def closed(self) -> bool:
        """底层客户端是否已关闭。"""
        return self._client is None

    async def _ensure_client(self) -> Any:
        """在专用 loop 内惰性创建连接池与客户端。"""
        if self._client is None:
            from redis.asyncio import Redis

            pool = from_url(self._redis_url, protocol=self._protocol, **self._pool_kwargs)
            self._client = Redis(connection_pool=pool)
        return self._client

    async def _execute(self, factory: _ClientFactory) -> Any:
        """把 ``factory(client)`` 投递到专用 loop 执行，并等待结果。"""
        loop = get_shared_loop()

        # 专用 loop 曾被重建（守护线程异常退出）时，旧客户端及其连接池内部持有的
        # asyncio 原语仍绑定在已关闭的 loop 上，继续复用会报跨 loop 错误。
        # 这里直接丢弃旧客户端，交由 ``_ensure_client`` 在新 loop 中重新建池。
        if self._client is not None and self._bound_loop is not loop:
            self._client = None
        self._bound_loop = loop

        async def _runner() -> Any:
            client = await self._ensure_client()
            return await factory(client)

        return await run_on_loop(_runner, loop=loop)

    def __getattr__(self, name: str):
        """动态转发任意 Redis 命令方法（如 get/set/hset/zadd/scan/...）。"""
        if name.startswith("_"):
            raise AttributeError(name)

        async def _command(*args: Any, **kwargs: Any) -> Any:
            async def _factory(client: Any) -> Any:
                return await getattr(client, name)(*args, **kwargs)

            return await self._execute(_factory)

        return _command

    def pipeline(self, transaction: bool = True, shard_hint: Optional[str] = None) -> LoopBoundPipeline:
        """创建 loop 安全的 pipeline 代理。"""
        return LoopBoundPipeline(self, transaction=transaction, shard_hint=shard_hint)

    def register_script(self, script: str) -> LoopBoundScript:
        """创建 loop 安全的 Lua 脚本代理。"""
        return LoopBoundScript(self, script)

    async def close(self) -> None:
        """关闭底层客户端与连接池（在专用 loop 中执行）。

        关闭后代理仍可再次使用：下次命令会重新建连（与 redis-py 语义一致）。
        """
        client = self._client
        loop = self._bound_loop
        self._client = None
        self._bound_loop = None
        if client is None:
            return

        async def _close_client() -> None:
            closer = getattr(client, "aclose", None)
            if closer is not None:
                await closer()
            else:  # pragma: no cover - 兼容 redis-py < 5.0
                await client.close()

        # loop 已不可用时直接放弃：守护线程退出后连接随之释放
        if loop is None or loop.is_closed() or not loop.is_running():
            return

        await run_on_loop(_close_client, loop=loop)

    async def aclose(self, close_connection_pool: Optional[bool] = None) -> None:
        """``close()`` 的别名，兼容 redis-py 5.x 命名。"""
        await self.close()


def create_client(redis_url: str, *, protocol: Optional[int] = None, **pool_kwargs: Any) -> LoopBoundRedisClient:
    """创建跨事件循环安全的异步 Redis 客户端。

    Args:
        redis_url: Redis 连接 URL。
        protocol: RESP 协议版本，缺省读取环境变量或 RESP2。
        **pool_kwargs: 透传给 ``ConnectionPool.from_url``（如 ``max_connections``、
            ``decode_responses``）。

    Returns:
        可在任意事件循环中安全调用的客户端代理。
    """
    return LoopBoundRedisClient(redis_url, protocol=protocol, **pool_kwargs)
