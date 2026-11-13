"""
Copyright (c) 2025 neopen
Licensed under the MIT License.
See LICENSE File For Details.

@FileName: 18_cost_tracking.py
@Description: 成本采集 — 执行器内上报 token/调用次数/费用，按任务与批次聚合
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/11/12
"""

import asyncio

from neotask import TaskPool, TaskPoolConfig, report_cost


async def call_llm(data: dict) -> dict:
    """模拟两次模型调用：执行器内用 report_cost 累加用量"""
    await asyncio.sleep(0.1)
    tokens_in, tokens_out = data["tokens_in"], data["tokens_out"]

    # 第一次调用
    report_cost(tokens_in=tokens_in, tokens_out=tokens_out, api_calls=1,
                estimated_cost=0.002, currency="USD")
    # 第二次调用：同一任务的成本会累加
    await asyncio.sleep(0.05)
    report_cost(tokens_in=tokens_in // 2, tokens_out=tokens_out, api_calls=1,
                estimated_cost=0.001, currency="USD")

    return {"answer": "42", "tokens": tokens_in + tokens_out}


def main():
    with TaskPool(
            executor=call_llm,
            config=TaskPoolConfig(worker_concurrency=2, max_retries=0),
    ) as pool:
        # 1. 提交两条任务，成本由执行器内部上报
        print("1. 提交任务（执行器内 report_cost）")
        ids = [
            pool.submit({"tokens_in": 1000, "tokens_out": 200}),
            pool.submit({"tokens_in": 500, "tokens_out": 900}),
        ]
        for task_id in ids:
            print(f"  {task_id}")

        for task_id in ids:
            pool.wait_for_result(task_id, timeout=10)

        # 2. 单任务成本明细
        print("\n2. 单任务成本明细")
        info = pool.get_result(ids[0])
        print(f"  estimated_cost: {info['estimated_cost']}")
        print(f"  cost_tracking : {info['cost_tracking']}")

        # 3. 任务结束后仍可追加成本（如事后回传的账单）
        print("\n3. 终态任务追加成本（外部上报）")
        pool.report_cost(ids[0], api_calls=1, estimated_cost=0.0005, currency="USD")
        info = pool.get_result(ids[0])
        print(f"  追加后 estimated_cost: {info['estimated_cost']}")
        print(f"  累计 api_calls: {info['cost_tracking']['api_calls']}")

        # 4. 聚合：全部任务 vs 指定任务子集
        print("\n4. 成本聚合")
        total = pool.get_cost_summary()
        print(f"  全部任务: {total}")
        subset = pool.get_cost_summary(task_ids=ids[:1])
        print(f"  仅 {ids[0]}: estimated_cost={subset['estimated_cost']}, "
              f"tokens_in={subset['tokens_in']}")

        # 5. 混用币种会被标记为 MIXED
        print("\n5. 多币种任务")
        mixed_id = pool.submit({"tokens_in": 100, "tokens_out": 100})
        pool.wait_for_result(mixed_id, timeout=10)
        pool.report_cost(mixed_id, estimated_cost=1.0, currency="EUR")
        mixed = pool.get_cost_summary(task_ids=[mixed_id])
        print(f"  currency={mixed['currency']}, estimated_cost={mixed['estimated_cost']}")


if __name__ == "__main__":
    main()
