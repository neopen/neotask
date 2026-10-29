"""
@FileName: migrate_redis_keys.py
@Description: 为存量 Redis 键补充统一根前缀 neotask:（生产环境迁移脚本）
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/10/29

背景
----
新版本把所有 Redis 键统一收敛到 ``neotask:`` 根前缀下：

    旧键                              新键
    task:<task_id>                    neotask:task:<task_id>
    status:<status>                   neotask:status:<status>
    queue:priority                    neotask:queue:priority
    queue:delayed                     neotask:queue:delayed
    queue:priority:paused             neotask:queue:priority:paused
    queue:priority:disabled           neotask:queue:priority:disabled
    lock:<key>                        neotask:lock:<key>

周期任务、节点注册表、死信队列在旧版本中已经以 ``neotask:`` 开头，无需迁移。

用法
----
    # 1) 预演（只统计，不修改任何键）
    python scripts/migrate_redis_keys.py --redis-url redis://127.0.0.1:6379/0

    # 2) 执行迁移（默认遇到同名新键时跳过，绝不覆盖）
    python scripts/migrate_redis_keys.py --redis-url redis://127.0.0.1:6379/0 --apply

    # 3) 冲突键需要覆盖时显式开启
    python scripts/migrate_redis_keys.py --redis-url redis://127.0.0.1:6379/0 --apply --overwrite

    # 只迁移部分命名空间
    python scripts/migrate_redis_keys.py --redis-url ... --apply --namespaces task: status:

    # 自定义根前缀（默认 neotask:）
    python scripts/migrate_redis_keys.py --redis-url ... --prefix myapp: --apply

注意
----
1. 务必在**停机维护窗口**执行：迁移过程中仍有旧版本进程在跑时，
   旧进程释放锁/删除任务会写入旧键名，造成迁移后残留。
2. 使用 ``RENAME``/``RENAMENX`` 迁移，键的 TTL 会被完整保留。
3. 脚本幂等：重复执行只会处理仍以旧命名空间开头的键。
4. 默认跳过“目标键已存在”的冲突（``--overwrite`` 才会覆盖），
   执行结束后会复查旧键残留，有残留时以退出码 1 结束。
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Iterable, List, Optional

try:
    import redis
except ImportError:  # pragma: no cover - 运行环境缺少 redis 客户端时的兜底
    sys.exit("缺少 redis 客户端，请先执行：pip install redis")

#: 与 neotask.common.constants.REDIS_KEY_PREFIX 保持一致
DEFAULT_PREFIX = "neotask:"

#: 旧版本未加根前缀的命名空间（周期任务/节点/死信队列已带前缀，无需迁移）
DEFAULT_NAMESPACES = ("task:", "status:", "queue:", "lock:")

#: 预演时每个命名空间最多展示的示例条数
SAMPLE_LIMIT = 5


@dataclass
class NamespaceStats:
    """单个命名空间的迁移统计"""

    namespace: str
    scanned: int = 0
    renamed: int = 0
    skipped: int = 0
    errors: int = 0
    samples: List[str] = field(default_factory=list)


def mask_url(url: str) -> str:
    """隐藏 URL 中的密码，便于安全打印"""
    if "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    credentials, host = rest.rsplit("@", 1)
    if ":" in credentials:
        user = credentials.split(":", 1)[0]
        return f"{scheme}://{user}:***@{host}"
    return f"{scheme}://***@{host}"


def iter_keys(client: "redis.Redis", pattern: str, count: int) -> Iterable[str]:
    """使用 SCAN 遍历匹配的键（避免 KEYS 阻塞 Redis）"""
    cursor = 0
    while True:
        cursor, keys = client.scan(cursor=cursor, match=pattern, count=count)
        for key in keys:
            yield key
        if cursor == 0:
            return


def count_keys(client: "redis.Redis", pattern: str, count: int) -> int:
    """统计匹配键数量"""
    return sum(1 for _ in iter_keys(client, pattern, count))


def _exists_many(client: "redis.Redis", keys: List[str]) -> List[int]:
    """批量判断键是否存在"""
    pipe = client.pipeline(transaction=False)
    for key in keys:
        pipe.exists(key)
    return pipe.execute()


def _process_batch(
    client: "redis.Redis",
    batch: List[str],
    prefix: str,
    overwrite: bool,
    dry_run: bool,
    stats: NamespaceStats,
) -> None:
    """处理一批键：预演时统计冲突，执行时批量 RENAME"""
    targets = [f"{prefix}{key}" for key in batch]

    if dry_run:
        collisions = _exists_many(client, targets)
        for key, target, exists in zip(batch, targets, collisions):
            if exists and not overwrite:
                stats.skipped += 1
                continue
            stats.renamed += 1
            if len(stats.samples) < SAMPLE_LIMIT:
                stats.samples.append(f"{key} -> {target}")
        return

    pipe = client.pipeline(transaction=False)
    for key, target in zip(batch, targets):
        if overwrite:
            pipe.rename(key, target)
        else:
            # RENAMENX：目标已存在时返回 0，源键保持不变
            pipe.renamenx(key, target)
    results = pipe.execute(raise_on_error=False)

    for key, target, result in zip(batch, targets, results):
        if isinstance(result, Exception):
            # 常见原因：SCAN 之后键过期/被删除
            stats.errors += 1
            continue
        if not result:
            stats.skipped += 1
            continue
        stats.renamed += 1
        if len(stats.samples) < SAMPLE_LIMIT:
            stats.samples.append(f"{key} -> {target}")


def migrate_namespace(
    client: "redis.Redis",
    namespace: str,
    prefix: str,
    batch_size: int,
    overwrite: bool,
    dry_run: bool,
) -> NamespaceStats:
    """迁移一个命名空间下的所有键"""
    stats = NamespaceStats(namespace=namespace)
    pattern = f"{namespace}*"
    batch: List[str] = []

    for key in iter_keys(client, pattern, batch_size):
        stats.scanned += 1
        batch.append(key)
        if len(batch) >= batch_size:
            _process_batch(client, batch, prefix, overwrite, dry_run, stats)
            batch.clear()

    if batch:
        _process_batch(client, batch, prefix, overwrite, dry_run, stats)
    return stats


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="为存量 Redis 键补充统一根前缀（默认 neotask:）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "示例：\n"
            "  python scripts/migrate_redis_keys.py --redis-url redis://127.0.0.1:6379/0\n"
            "  python scripts/migrate_redis_keys.py --redis-url redis://127.0.0.1:6379/0 --apply\n"
        ),
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("NEOTASK_REDIS_URL") or os.environ.get("REDIS_URL"),
        help="Redis 连接 URL（默认读取 NEOTASK_REDIS_URL / REDIS_URL）",
    )
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="统一根前缀，默认 neotask:")
    parser.add_argument(
        "--namespaces",
        nargs="+",
        default=list(DEFAULT_NAMESPACES),
        help="要迁移的旧命名空间，默认：task: status: queue: lock:",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="SCAN/批量 RENAME 的批次大小")
    parser.add_argument("--apply", action="store_true", help="真正执行迁移（默认仅预演）")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="目标键已存在时覆盖（默认跳过并计入冲突）",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if not args.redis_url:
        print("错误：未提供 --redis-url（也可设置 NEOTASK_REDIS_URL / REDIS_URL）", file=sys.stderr)
        return 2
    if not args.prefix:
        print("错误：--prefix 不能为空", file=sys.stderr)
        return 2
    if args.batch_size < 1:
        print("错误：--batch-size 必须为正整数", file=sys.stderr)
        return 2

    namespaces: List[str] = []
    for namespace in args.namespaces:
        if namespace.startswith(args.prefix):
            print(f"警告：命名空间 {namespace!r} 已带根前缀，跳过", file=sys.stderr)
            continue
        namespaces.append(namespace)
    if not namespaces:
        print("错误：没有可迁移的命名空间", file=sys.stderr)
        return 2

    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    try:
        client.ping()
    except Exception as exc:
        print(f"错误：无法连接 Redis {mask_url(args.redis_url)}：{exc}", file=sys.stderr)
        return 2

    dry_run = not args.apply
    mode = "预演（不会修改任何键）" if dry_run else "执行迁移"
    print(f"Redis   : {mask_url(args.redis_url)}")
    print(f"根前缀  : {args.prefix}")
    print(f"命名空间: {', '.join(namespaces)}")
    print(f"模式    : {mode}{'（冲突键覆盖）' if args.overwrite else ''}")
    print("-" * 78)

    started = time.time()
    all_stats: List[NamespaceStats] = []
    for namespace in namespaces:
        stats = migrate_namespace(
            client,
            namespace=namespace,
            prefix=args.prefix,
            batch_size=args.batch_size,
            overwrite=args.overwrite,
            dry_run=dry_run,
        )
        all_stats.append(stats)
        print(
            f"{namespace:<14} 扫描 {stats.scanned:>8}  "
            f"迁移 {stats.renamed:>8}  冲突 {stats.skipped:>6}  错误 {stats.errors:>4}"
        )
        for sample in stats.samples:
            print(f"    - {sample}")

    elapsed = time.time() - started
    print("-" * 78)
    total_scanned = sum(s.scanned for s in all_stats)
    total_renamed = sum(s.renamed for s in all_stats)
    total_skipped = sum(s.skipped for s in all_stats)
    total_errors = sum(s.errors for s in all_stats)
    print(
        f"合计：扫描 {total_scanned}，{'将迁移' if dry_run else '已迁移'} {total_renamed}，"
        f"冲突 {total_skipped}，错误 {total_errors}，耗时 {elapsed:.2f}s"
    )

    if dry_run:
        print("\n预演完成，未做任何修改。确认无误后加 --apply 执行迁移。")
        client.close()
        return 0

    # 执行后复查：旧命名空间应已清空
    leftovers = 0
    for namespace in namespaces:
        remaining = count_keys(client, f"{namespace}*", args.batch_size)
        leftovers += remaining
        if remaining:
            print(f"警告：命名空间 {namespace} 仍有 {remaining} 个旧键（多为冲突跳过或迁移期间新增）")
    client.close()

    if total_errors or total_skipped or leftovers:
        print(
            "\n迁移完成但存在未处理的键：请检查冲突（可用 --overwrite 覆盖）或迁移期间仍在运行的旧版本进程。",
            file=sys.stderr,
        )
        return 1

    print("\n迁移完成：旧命名空间已全部清理，无冲突与错误。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
