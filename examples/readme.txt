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
99_all_features.py       完整演示        即时+优先级+批量+事件+进度+统计+定时（约 7 秒）

说明：
- 示例默认使用 memory 存储；需要持久化时参考 11_sqlite_persistence.py。
- 周期/Cron 任务在 memory 模式下重启会丢失，生产环境请使用 sqlite 或 redis。
- 延时任务的实际触发时间约为目标时间 +0.1~0.8 秒（延迟队列 0.1s 扫描 + Worker 预取退避）。
