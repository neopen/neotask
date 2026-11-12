"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: test_sqlite_transactions.py
@Description: statements_transaction 多语句事务原语测试 - 原子提交/回滚/并发交错
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio

import pytest

from neotask.utils.sqlite_compat import create_connection


async def _scalar(conn, sql, parameters=()):
    cursor = await conn.execute(sql, parameters)
    try:
        row = await cursor.fetchone()
    finally:
        await cursor.close()
    return row[0] if row else None


@pytest.fixture
async def conn(tmp_path):
    connection = create_connection(str(tmp_path / "txn.db"))
    await connection.execute(
        "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v INTEGER NOT NULL)"
    )
    await connection.execute(
        "CREATE TABLE IF NOT EXISTS log (id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT)"
    )
    await connection.execute("INSERT INTO kv VALUES (?, ?)", ("counter", 0))
    await connection.commit()
    try:
        yield connection
    finally:
        await connection.close()


async def test_statements_transaction_commits_all(conn):
    """多条不同 SQL 在同一事务内一起提交"""
    await conn.statements_transaction(
        [
            ("INSERT INTO log (note) VALUES (?)", ("checkpoint",)),
            ("UPDATE kv SET v = v + 1 WHERE k = ?", ("counter",)),
        ]
    )

    assert await _scalar(conn, "SELECT COUNT(*) FROM log") == 1
    assert await _scalar(conn, "SELECT v FROM kv WHERE k = 'counter'") == 1


async def test_statements_transaction_rolls_back_on_failure(conn):
    """任一语句失败时，前面已执行的语句同样回滚"""
    with pytest.raises(Exception, match="missing_table"):
        await conn.statements_transaction(
            [
                ("INSERT INTO log (note) VALUES (?)", ("will-rollback",)),
                ("UPDATE missing_table SET v = 1", ()),
            ]
        )

    assert await _scalar(conn, "SELECT COUNT(*) FROM log") == 0
    assert await _scalar(conn, "SELECT v FROM kv WHERE k = 'counter'") == 0


async def test_statements_transaction_empty_is_noop(conn):
    await conn.statements_transaction([])
    assert await _scalar(conn, "SELECT v FROM kv WHERE k = 'counter'") == 0


async def test_statements_transaction_interleaves_with_single_writes(conn):
    """多语句事务与单条写入交错执行：总量守恒且不出现嵌套事务报错"""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS account (id TEXT PRIMARY KEY, balance INTEGER NOT NULL)"
    )
    await conn.execute("INSERT INTO account VALUES ('a', 1000), ('b', 1000)")
    await conn.commit()

    async def transfer() -> None:
        await conn.statements_transaction(
            [
                ("UPDATE account SET balance = balance - 1 WHERE id = 'a'", ()),
                ("UPDATE account SET balance = balance + 1 WHERE id = 'b'", ()),
            ]
        )

    async def single_write(n: int) -> None:
        await conn.execute("INSERT INTO log (note) VALUES (?)", (f"w{n}",))
        await conn.commit()

    tasks = [transfer() for _ in range(50)] + [single_write(i) for i in range(50)]
    await asyncio.gather(*tasks)

    total = await _scalar(conn, "SELECT SUM(balance) FROM account")
    assert total == 2000
    assert await _scalar(conn, "SELECT COUNT(*) FROM log") == 50
    assert await _scalar(conn, "SELECT balance FROM account WHERE id = 'a'") == 950
    assert await _scalar(conn, "SELECT balance FROM account WHERE id = 'b'") == 1050


async def test_statements_transaction_no_deadlock_within_timeout(conn):
    """事务体只用底层连接：连续多个事务不会因写锁非重入而自锁"""
    for i in range(20):
        await asyncio.wait_for(
            conn.statements_transaction(
                [
                    ("INSERT INTO log (note) VALUES (?)", (f"n{i}",)),
                    ("UPDATE kv SET v = v + 1 WHERE k = 'counter'", ()),
                ]
            ),
            timeout=5,
        )

    assert await _scalar(conn, "SELECT v FROM kv WHERE k = 'counter'") == 20
