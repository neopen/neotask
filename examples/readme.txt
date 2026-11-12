NeoTask 示例索引（examples/）

运行方式：
    python examples/00_quick_start.py
    python examples/99_all_features.py

文件                     主题            说明
00_quick_start.py        快速入门        最少代码跑通第一个任务
01_simple.py             基础使用        提交多个任务并用 wait_all 等待结果
02_context_manager.py    上下文管理器    使用 with 自动 start/shutdown
03_priority.py           优先级队列      CRITICAL/HIGH/NORMAL/LOW 出队顺序（确定性演示）
04_events.py             事件回调        created/started/progress/completed/failed/cancelled
05_cron_tasks.py         Cron 任务       表达式解析、下次执行时间、注册与取消
06_delayed_tasks.py      延时任务        延时/定点执行、取消与精度说明（约 4 秒）
07_batch.py              批量任务        submit_batch + wait_all + 吞吐量统计
08_periodic.py           周期管理        暂停/恢复/取消、max_runs 上限（约 8 秒）
09_retry_and_cancel.py   重试和取消      自动重试、手动重试（retry）、排队任务取消
10_task_query.py         任务查询        状态/结果/完整信息/统计/健康状态
11_sqlite_persistence.py SQLite 持久化   进程重启后仍能查到历史任务
12_timeout.py            任务超时        task_timeout 超时失败与错误信息
13_benchmark.py          性能自测        用自己的 executor 对比并发度下的吞吐/延迟
14_event_backpressure.py EventBus 背压   有界队列丢弃策略、慢回调监控与隔离（约 2 秒）
15_low_jitter_delay.py   低抖动延时      延迟队列 vs 时间轮的触发精度对比（约 3 秒）
16_custom_task_id.py     外部任务 ID     业务自持 task_id、task_exists 与幂等重提交
17_business_failure.py   业务失败上报    report_failure 登记原因、跳过自动重试、事件携带 reason
18_cost_tracking.py      成本采集        report_cost 上报用量、单任务明细与 get_cost_summary 聚合
19_parent_child.py       父子任务        parent_task_id 关联、get_children/get_parent 查询与汇总
99_all_features.py       完整演示        即时+优先级+批量+事件+进度+统计+定时（约 7 秒）

说明：
- 示例默认使用 memory 存储；需要持久化时参考 11_sqlite_persistence.py。
- 周期/Cron 任务在 memory 模式下重启会丢失，生产环境请使用 sqlite 或 redis。
- 延时任务按截止时间提升、入队事件即时唤醒 Worker，触发开销通常在几十毫秒内（精度对比见 15_low_jitter_delay.py）。
- 存储迁移工具（memory → SQLite/Redis、SQLite → Redis）见 scripts/migrate_*.py。
