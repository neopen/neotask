"""
@FileName: sqlite_compat.py
@Description: SQLite 异步连接兼容助手 - 把 aiosqlite 连接绑定到专用事件循环，
             避免 TaskPool 内部 loop 与调用方 loop 共用同一连接时报跨 loop 错误。
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Optional, Union

import aiosqlite

from neotask.utils.dedicated_loop import get_shared_loop, run_on_loop

# 数据库路径类型（与 aiosqlite.connect 保持一致）
DatabasePath = Union[str, Path]


class LoopBoundCursor:
    """代理 ``aiosqlite.Cursor``。

    ``fetchone`` / ``fetchall`` 等在专用事件循环中执行；
    ``description`` / ``rowcount`` 等只读属性直接暴露（执行完成后不再变化）。
    """

    def __init__(self, owner: "LoopBoundSQLiteConnection", cursor: Any) -> None:
        self._owner = owner
        self._cursor = cursor

    async def fetchone(self) -> Optional[Any]:
        return await self._owner._dispatch(self._cursor.fetchone)

    async def fetchmany(self, size: Optional[int] = None) -> Iterable[Any]:
        if size is None:
            return await self._owner._dispatch(self._cursor.fetchmany)
        return await self._owner._dispatch(lambda: self._cursor.fetchmany(size))

    async def fetchall(self) -> Iterable[Any]:
        return await self._owner._dispatch(self._cursor.fetchall)

    async def close(self) -> None:
        await self._owner._dispatch(self._cursor.close)

    @property
    def description(self) -> Any:
        """列描述信息（供 ``dict(zip(...))`` 使用）"""
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        """受影响的行数"""
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> Optional[int]:
        """最后插入行的 ID"""
        return self._cursor.lastrowid

    async def __aenter__(self) -> "LoopBoundCursor":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


class LoopBoundSQLiteConnection:
    """绑定到专用事件循环的 ``aiosqlite.Connection`` 代理。

    用法与 ``aiosqlite.Connection`` 基本一致：

    >>> conn = create_connection("tasks.db")
    >>> cursor = await conn.execute("SELECT 1")
    >>> row = await cursor.fetchone()
    >>> await conn.commit()
    >>> await conn.close()

    背景：``aiosqlite`` 的每次调用都会在“当前 loop”上创建 Future 并由后台线程回填，
    连接对象本身在旧版本 / 部分使用方式下会与创建它的 loop 耦合。NeoTask 中
    ``TaskPool`` 内部守护 loop 与调用方 loop 并存，因此把所有 SQL 操作统一投递到
    共享专用 loop，彻底消除跨 loop 访问。
    """

    def __init__(
            self,
            database: DatabasePath,
            *,
            row_factory: Optional[Callable[..., Any]] = None,
            **connect_kwargs: Any,
    ) -> None:
        self._database = database
        self._row_factory = row_factory
        self._connect_kwargs = connect_kwargs
        self._conn: Optional[aiosqlite.Connection] = None
        self._bound_loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def closed(self) -> bool:
        """底层连接是否已关闭。"""
        return self._conn is None

    async def _ensure_connection(self) -> aiosqlite.Connection:
        """在专用 loop 内惰性建立连接。

        ``aiosqlite.connect`` 是异步的，多个并发命令可能同时走到这里；建立完成后
        再检查一次，若已有其它协程先完成建连，则关闭本次多出来的连接，避免出现
        两个连接同时写同一个库文件。
        """
        if self._conn is None:
            conn = await aiosqlite.connect(self._database, **self._connect_kwargs)
            if self._conn is not None:  # 并发情况下已有连接，丢弃重复的
                await conn.close()
            else:
                if self._row_factory is not None:
                    conn.row_factory = self._row_factory
                self._conn = conn
        return self._conn

    async def _run(self, factory: Callable[[aiosqlite.Connection], Awaitable[Any]]) -> Any:
        """把 ``factory(conn)`` 投递到专用 loop 执行。"""
        loop = get_shared_loop()
        self._bound_loop = loop

        async def _runner() -> Any:
            conn = await self._ensure_connection()
            return await factory(conn)

        return await run_on_loop(_runner, loop=loop)

    async def _dispatch(self, fn: Callable[[], Awaitable[Any]]) -> Any:
        """在连接所属的专用 loop 中执行一个零参协程工厂（供 Cursor 代理复用）。"""
        loop = self._bound_loop or get_shared_loop()
        return await run_on_loop(fn, loop=loop)

    async def execute(self, sql: str, parameters: Optional[Iterable[Any]] = None) -> LoopBoundCursor:
        """执行 SQL 并返回 loop 安全的游标代理。"""

        async def _factory(conn: aiosqlite.Connection) -> LoopBoundCursor:
            if parameters is None:
                cursor = await conn.execute(sql)
            else:
                cursor = await conn.execute(sql, parameters)
            return LoopBoundCursor(self, cursor)

        return await self._run(_factory)

    async def executemany(
            self,
            sql: str,
            parameters: Iterable[Iterable[Any]],
    ) -> LoopBoundCursor:
        """批量执行 SQL。"""

        async def _factory(conn: aiosqlite.Connection) -> LoopBoundCursor:
            cursor = await conn.executemany(sql, parameters)
            return LoopBoundCursor(self, cursor)

        return await self._run(_factory)

    async def executescript(self, sql_script: str) -> LoopBoundCursor:
        """执行 SQL 脚本。"""

        async def _factory(conn: aiosqlite.Connection) -> LoopBoundCursor:
            cursor = await conn.executescript(sql_script)
            return LoopBoundCursor(self, cursor)

        return await self._run(_factory)

    async def commit(self) -> None:
        """提交事务。"""

        async def _factory(conn: aiosqlite.Connection) -> None:
            await conn.commit()

        await self._run(_factory)

    async def rollback(self) -> None:
        """回滚事务。"""

        async def _factory(conn: aiosqlite.Connection) -> None:
            await conn.rollback()

        await self._run(_factory)

    async def close(self) -> None:
        """关闭连接（在专用 loop 中执行）。

        关闭后代理仍可再次使用：下次操作会自动重新建连。
        """
        conn = self._conn
        loop = self._bound_loop
        self._conn = None
        self._bound_loop = None
        if conn is None:
            return

        async def _close_connection() -> None:
            await conn.close()

        # loop 已不可用时直接放弃：守护线程退出后连接随之释放
        if loop is None or loop.is_closed() or not loop.is_running():
            return

        await run_on_loop(_close_connection, loop=loop)

    async def __aenter__(self) -> "LoopBoundSQLiteConnection":

        async def _factory(conn: aiosqlite.Connection) -> "LoopBoundSQLiteConnection":
            return self

        return await self._run(_factory)

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


def create_connection(
        database: DatabasePath,
        *,
        row_factory: Optional[Callable[..., Any]] = None,
        **connect_kwargs: Any,
) -> LoopBoundSQLiteConnection:
    """创建跨事件循环安全的异步 SQLite 连接代理。

    Args:
        database: 数据库文件路径（``str`` 或 ``Path``）。
        row_factory: 行工厂（如 ``aiosqlite.Row``），连接建立后自动设置。
        **connect_kwargs: 透传给 ``sqlite3.connect`` 的参数（如 ``timeout``）。
    """
    return LoopBoundSQLiteConnection(database, row_factory=row_factory, **connect_kwargs)
