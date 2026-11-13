"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: sqlite_compat.py
@Description: SQLite 异步连接兼容助手 - 把 aiosqlite 连接绑定到专用事件循环，
             避免 TaskPool 内部 loop 与调用方 loop 共用同一连接时报跨 loop 错误。
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional, Tuple, Union

import aiosqlite

from neotask.utils.dedicated_loop import DedicatedLoopError, get_shared_loop, run_on_loop

# 数据库路径类型（与 aiosqlite.connect 保持一致）
DatabasePath = Union[str, Path]

#: 连接级默认 PRAGMA。
#: - ``journal_mode=WAL``：允许读写并发，避免写事务阻塞读取；
#: - ``synchronous=NORMAL``：WAL 下不再每次提交都 fsync，写入吞吐提升一个量级；
#: - ``busy_timeout``：并发写遇到锁时等待而不是立刻抛 ``database is locked``。
DEFAULT_PRAGMAS: Tuple[str, ...] = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA busy_timeout=30000",
)


def _is_readonly_sql(sql: str) -> bool:
    """判断语句是否只读（只读语句走读连接）。"""
    head = sql.lstrip()[:6].upper()
    return head == "SELECT" or sql.lstrip()[:4].upper() == "WITH"


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
            pragmas: Optional[Iterable[str]] = None,
            **connect_kwargs: Any,
    ) -> None:
        self._database = database
        self._row_factory = row_factory
        self._pragmas = tuple(DEFAULT_PRAGMAS if pragmas is None else pragmas)
        self._connect_kwargs = connect_kwargs
        # 读写分离：同一进程内用两条连接。
        # - 写连接只执行 INSERT/UPDATE/DELETE，永远不会持有读事务，写操作直接
        #   申请写锁，遇到其它进程持锁时由 busy_timeout 正常排队等待；
        # - 读连接只执行 SELECT，WAL 下读不阻塞写。
        # 若读写共用一条连接，某个 SELECT 游标未关闭期间（读事务仍在），随后的
        # 写会走「读转写升级」路径，此时 SQLite 不调用 busy handler，立刻抛
        # database is locked，多进程并发下会造成任务状态写入失败。
        self._write_conn: Optional[aiosqlite.Connection] = None
        self._read_conn: Optional[aiosqlite.Connection] = None
        self._bound_loop: Optional[asyncio.AbstractEventLoop] = None
        # 写串行锁：在专用 loop 内惰性创建。所有写操作（含批量事务）共用一把锁，
        # 杜绝多个协程的 BEGIN/COMMIT 在同一写连接上交错（见 executemany_transaction）。
        self._write_lock: Optional[asyncio.Lock] = None

    def _write_lock_on_loop(self) -> asyncio.Lock:
        """返回写串行锁，必要时创建（必须在专用 loop 内调用）。

        锁绑定到专用 loop：所有写命令都投递到该 loop 执行，单线程内用 asyncio.Lock
        即可把并发写串行化。loop 重建时随连接一起丢弃（见 _run / close）。
        """
        if self._write_lock is None:
            self._write_lock = asyncio.Lock()
        return self._write_lock

    @property
    def closed(self) -> bool:
        """底层连接是否已关闭。"""
        return self._write_conn is None and self._read_conn is None

    async def _ensure_connection(self, readonly: bool = False) -> aiosqlite.Connection:
        """在专用 loop 内惰性建立读/写连接。

        ``aiosqlite.connect`` 是异步的，多个并发命令可能同时走到这里；建立完成后
        再检查一次，若已有其它协程先完成建连，则关闭本次多出来的连接，避免出现
        多条连接同时写同一个库文件。
        """
        attr = "_read_conn" if readonly else "_write_conn"
        if getattr(self, attr) is None:
            conn = await aiosqlite.connect(self._database, **self._connect_kwargs)
            if getattr(self, attr) is not None:  # 并发情况下已有连接，丢弃重复的
                await conn.close()
            else:
                if self._row_factory is not None:
                    conn.row_factory = self._row_factory
                setattr(self, attr, conn)
                # 连接建立后立即应用 PRAGMA（必须在任何业务读写之前）。
                # PRAGMA 会返回结果行，游标必须显式关闭：未关闭的语句会一直持有
                # 读事务，后续写会被当作「读转写升级」而绕过 busy_timeout。
                for pragma in self._pragmas:
                    pragma_cursor = await conn.execute(pragma)
                    await pragma_cursor.close()
                await conn.commit()
        return getattr(self, attr)

    async def _run(
            self,
            factory: Callable[[aiosqlite.Connection], Awaitable[Any]],
            readonly: bool = False,
    ) -> Any:
        """把 ``factory(conn)`` 投递到专用 loop 执行。"""
        for attempt in range(2):
            loop = get_shared_loop()

            # 专用 loop 被重建时，旧连接绑定在已关闭的 loop 上，不能继续复用；
            # 丢弃后在新的 loop 中重新建连（与 Redis 代理保持一致的防御逻辑）。
            if (self._write_conn is not None or self._read_conn is not None) \
                    and self._bound_loop is not loop:
                self._write_conn = None
                self._read_conn = None
                # 旧锁绑定在已关闭的 loop 上，随连接一起丢弃，在新 loop 重建
                self._write_lock = None
            self._bound_loop = loop

            async def _runner() -> Any:
                conn = await self._ensure_connection(readonly)
                return await factory(conn)

            try:
                return await run_on_loop(_runner, loop=loop)
            except DedicatedLoopError as exc:
                # 协程尚未开始执行时命令无副作用，换新 loop 重试一次
                if exc.started or attempt == 1:
                    raise
        raise AssertionError("unreachable")

    async def _dispatch(self, fn: Callable[[], Awaitable[Any]]) -> Any:
        """在连接所属的专用 loop 中执行一个零参协程工厂（供 Cursor 代理复用）"""
        loop = self._bound_loop
        if loop is None or loop.is_closed() or not loop.is_running():
            # 原 loop 已不可用（被重建/关闭），回退到当前共享 loop
            loop = get_shared_loop()
        return await run_on_loop(fn, loop=loop)

    async def execute(self, sql: str, parameters: Optional[Iterable[Any]] = None) -> LoopBoundCursor:
        """执行 SQL 并返回 loop 安全的游标代理。

        SELECT / WITH 走只读连接，其余语句走写连接（见 ``__init__`` 注释）。
        写语句持写串行锁执行，避免落入其它协程的批量事务窗口（BEGIN…COMMIT）。
        """
        readonly = _is_readonly_sql(sql)

        async def _factory(conn: aiosqlite.Connection) -> LoopBoundCursor:
            async def _exec() -> Any:
                if parameters is None:
                    return await conn.execute(sql)
                return await conn.execute(sql, parameters)

            if readonly:
                cursor = await _exec()
            else:
                async with self._write_lock_on_loop():
                    cursor = await _exec()
            return LoopBoundCursor(self, cursor)

        return await self._run(_factory, readonly=readonly)

    async def executemany(
            self,
            sql: str,
            parameters: Iterable[Iterable[Any]],
    ) -> LoopBoundCursor:
        """批量执行 SQL（持写串行锁）。"""

        async def _factory(conn: aiosqlite.Connection) -> LoopBoundCursor:
            async with self._write_lock_on_loop():
                cursor = await conn.executemany(sql, parameters)
                return LoopBoundCursor(self, cursor)

        return await self._run(_factory)

    async def executemany_transaction(
            self,
            sql: str,
            parameters: Iterable[Iterable[Any]],
    ) -> None:
        """单事务批量写入：``BEGIN IMMEDIATE → executemany → 关闭游标 → COMMIT``。

        整个事务作为**一次**投递在专用 loop 内执行，并全程持有写串行锁：
        其它协程的写（单条 execute / commit / 另一个批量事务）无法落入本事务
        窗口，从根本上消除共享连接上的两类竞态——
        ``cannot start a transaction within a transaction``（并发 BEGIN）与
        ``cannot commit transaction - SQL statements in progress``（COMMIT 时
        另有活跃语句）。任意一条失败整批回滚。

        注意：事务内部直接使用底层 ``conn``，不能再调用本代理的 execute/commit
        等加锁方法，否则会对非重入锁二次加锁而死锁。
        """

        async def _factory(conn: aiosqlite.Connection) -> None:
            async with self._write_lock_on_loop():
                begin_cursor = await conn.execute("BEGIN IMMEDIATE")
                await begin_cursor.close()
                try:
                    cursor = await conn.executemany(sql, parameters)
                    await cursor.close()
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

        await self._run(_factory)

    async def statements_transaction(
            self,
            items: Iterable[Tuple[str, Iterable[Any]]],
    ) -> None:
        """多语句单事务：``BEGIN IMMEDIATE → 逐条 execute → COMMIT``。

        与 :meth:`executemany_transaction` 的区别是每条语句可以不同
        （如「写 checkpoint 分表 + 更新任务标记」）。原子性约定与实现纪律相同：

        - 整个事务作为**一次**投递在专用 loop 内执行，并全程持有写串行锁；
        - 事务体内直接使用底层 ``conn``，不得回调本代理的 execute/commit
          等加锁方法，否则会对非重入锁二次加锁而死锁；
        - 任一语句失败整体 ``rollback`` 并原样抛出。

        空列表视为 no-op。
        """
        statements = list(items)
        if not statements:
            return

        async def _factory(conn: aiosqlite.Connection) -> None:
            async with self._write_lock_on_loop():
                begin_cursor = await conn.execute("BEGIN IMMEDIATE")
                await begin_cursor.close()
                try:
                    for sql, parameters in statements:
                        cursor = await conn.execute(sql, parameters)
                        await cursor.close()
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

        await self._run(_factory)

    async def executescript(self, sql_script: str) -> LoopBoundCursor:
        """执行 SQL 脚本。"""

        async def _factory(conn: aiosqlite.Connection) -> LoopBoundCursor:
            cursor = await conn.executescript(sql_script)
            return LoopBoundCursor(self, cursor)

        return await self._run(_factory)

    async def commit(self) -> None:
        """提交事务（自动提交模式下为安全 no-op，仅作用于写连接）。

        持写串行锁：即便调用方把 execute 与 commit 拆成两次调用，COMMIT 也不会
        落在其它协程的批量事务窗口内（自动提交模式下本调用通常为 no-op）。
        """

        async def _factory(conn: aiosqlite.Connection) -> None:
            async with self._write_lock_on_loop():
                await conn.commit()

        if self._write_conn is not None:
            await self._run(_factory)

    async def rollback(self) -> None:
        """回滚事务（仅作用于写连接，持写串行锁）。"""

        async def _factory(conn: aiosqlite.Connection) -> None:
            async with self._write_lock_on_loop():
                await conn.rollback()

        if self._write_conn is not None:
            await self._run(_factory)

    async def close(self) -> None:
        """关闭读/写连接（在专用 loop 中执行）。

        关闭后代理仍可再次使用：下次操作会自动重新建连。
        """
        write_conn, read_conn = self._write_conn, self._read_conn
        loop = self._bound_loop
        self._write_conn = None
        self._read_conn = None
        self._bound_loop = None
        self._write_lock = None

        # loop 已不可用时直接放弃：守护线程退出后连接随之释放
        if loop is None or loop.is_closed() or not loop.is_running():
            return

        for conn in (write_conn, read_conn):
            if conn is None:
                continue

            async def _close_connection(target=conn) -> None:
                await target.close()

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
        pragmas: Optional[Iterable[str]] = None,
        **connect_kwargs: Any,
) -> LoopBoundSQLiteConnection:
    """创建跨事件循环安全的异步 SQLite 连接代理。

    Args:
        database: 数据库文件路径（``str`` 或 ``Path``）。
        row_factory: 行工厂（如 ``aiosqlite.Row``），连接建立后自动设置。
        pragmas: 建连后执行的 PRAGMA，默认使用 :data:`DEFAULT_PRAGMAS`。
        **connect_kwargs: 透传给 ``sqlite3.connect`` 的参数（如 ``timeout``）。
    """
    # 使用自动提交模式（isolation_level=None）：每条语句各自成事务。
    # 多个协程共享同一条连接时，若使用隐式事务，一个协程的 commit() 可能落在
    # 另一个协程的语句执行中间，触发 "cannot commit transaction - SQL statements
    # in progress"；自动提交彻底消除了这种跨协程事务边界竞争。
    connect_kwargs.setdefault("isolation_level", None)
    return LoopBoundSQLiteConnection(
        database,
        row_factory=row_factory,
        pragmas=pragmas,
        **connect_kwargs,
    )


#: 同进程内按数据库路径共享的连接与引用计数。
_SHARED_CONNECTIONS: Dict[str, LoopBoundSQLiteConnection] = {}
_SHARED_REFS: Dict[str, int] = {}
_SHARED_LOCK = threading.Lock()


def _connection_key(database: DatabasePath) -> str:
    """把数据库路径归一化，作为共享连接的键。"""
    return str(Path(database).expanduser().resolve())


def acquire_shared_connection(
        database: DatabasePath,
        *,
        row_factory: Optional[Callable[..., Any]] = None,
        pragmas: Optional[Iterable[str]] = None,
        **connect_kwargs: Any,
) -> LoopBoundSQLiteConnection:
    """获取（必要时创建）同路径的共享连接，引用计数 +1。

    TaskRepository / QueueRepository / PeriodicStore 访问同一个库文件时共用一条
    连接，避免多个连接互抢写锁而出现 ``database is locked``，同时让并发写入
    自然合并到更少的提交批次。
    """
    key = _connection_key(database)
    with _SHARED_LOCK:
        conn = _SHARED_CONNECTIONS.get(key)
        if conn is None:
            conn = create_connection(
                database,
                row_factory=row_factory,
                pragmas=pragmas,
                **connect_kwargs,
            )
            _SHARED_CONNECTIONS[key] = conn
        _SHARED_REFS[key] = _SHARED_REFS.get(key, 0) + 1
        return conn


async def release_shared_connection(connection: LoopBoundSQLiteConnection) -> None:
    """释放共享连接，引用计数归零时真正关闭。

    非共享连接（由 :func:`create_connection` 直接创建）同样会被关闭，方便调用方
    统一使用。
    """
    should_close = True
    with _SHARED_LOCK:
        for key, candidate in _SHARED_CONNECTIONS.items():
            if candidate is connection:
                refs = _SHARED_REFS[key] - 1
                if refs > 0:
                    _SHARED_REFS[key] = refs
                    should_close = False
                else:
                    _SHARED_CONNECTIONS.pop(key, None)
                    _SHARED_REFS.pop(key, None)
                break

    if should_close:
        await connection.close()
