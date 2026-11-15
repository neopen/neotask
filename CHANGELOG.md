# Changelog

所有重要的项目更改都将记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。


## [1.2.0](https://github.com/neopen/neotask/compare/v1.1.1...v1.2.0) (2026-11-15)


### ✨ Features (新功能)

* **api:** 定稿外部 task_id 幂等语义并补齐 TaskScheduler 异步存在性检查 ([ee0d77c](https://github.com/neopen/neotask/commit/ee0d77ce38fc6782b67a08ecc7c3385e1c0f264e))
* **api:** 新增 submit_many 批量提交 ([2af981b](https://github.com/neopen/neotask/commit/2af981ba0e6baeeaa39de671f82a49aef21d5af6))
* **batch:** 批量提交支持全有或全无与 partial_on_error ([22c13d2](https://github.com/neopen/neotask/commit/22c13d243e094a6162fcbc3e5a13d1dd89d2f22a))
* **checkpoint:** CheckpointRepository 三后端实现与独立工厂 ([ca4c629](https://github.com/neopen/neotask/commit/ca4c629b656e475065ec008943e314199e24fcee))
* **checkpoint:** 配置装配、双形态 API 与断点续跑恢复路径 ([cb4ae87](https://github.com/neopen/neotask/commit/cb4ae877cf5c29e1e2e53e9d6159228babfb8e53))
* **cleanup:** 终态任务按 TTL 周期清理，避免存储无限增长 ([1c67cd5](https://github.com/neopen/neotask/commit/1c67cd569fb87c74f09b136a62d22a283bc91512))
* **cli:** 补齐 neotask 命令行入口 ([38e878f](https://github.com/neopen/neotask/commit/38e878f100d26270f3105dbbd7b33a1b08ef1242))
* **common:** 日志快捷函数支持 *args/**kwargs 透传 ([62a2248](https://github.com/neopen/neotask/commit/62a2248ddae66251772d27c0d1829199dcb38c47))
* **context:** 新增 Memory-only 本地 Blackboard 上下文 ([d448497](https://github.com/neopen/neotask/commit/d448497cb1ba42fae216275fff947556d86aaae8))
* **core:** 新增业务级失败上报 report_failure，定稿 FAILED 收尾语义 ([70ffe29](https://github.com/neopen/neotask/commit/70ffe299fe5a4685ef782346bf93155089353ff1))
* **cost,parent:** 成本采集、耗时字段与父子任务公开 API ([758b108](https://github.com/neopen/neotask/commit/758b10800e48f9fd1a5a51968c207a5dabd6f3ca))
* **event:** EventBus 增加有界队列背压与慢回调监控 ([4cab61d](https://github.com/neopen/neotask/commit/4cab61d76bd57e5f36135c74a33d11d848dce35b))
* **event:** TaskEvent 信封稳定化，补齐 timestamp/node_id/schema_version ([b41ba69](https://github.com/neopen/neotask/commit/b41ba691c4f3224d7b493fa2cef58fa18cb9c3cf))
* **event:** 新增 EventType 事件名常量注册表并替换散写字符串 ([1606202](https://github.com/neopen/neotask/commit/1606202c61f3f643fe15959e0846da9d7e002cee))
* **model,storage:** Task 元数据字段与三后端新列 ([c139b08](https://github.com/neopen/neotask/commit/c139b081545efc395deddf49c880b7edcbf6f67d))
* **redis:** 统一所有 Redis 键的 neotask: 根前缀 ([e3e6c0c](https://github.com/neopen/neotask/commit/e3e6c0c311047446600eabd7d300b4e88fac273b))
* **scheduler:** 实现周期任务持久化，enable_persistence 生效并支持跨重启恢复 ([36690bf](https://github.com/neopen/neotask/commit/36690bfa7302ac4f86352692d50eb6ae6e455d14))
* **scheduler:** 实现周期任务持久化，enable_persistence 生效并支持跨重启恢复 ([534b824](https://github.com/neopen/neotask/commit/534b824391095c9544e43fccf8115e08da8856dc))
* **scripts:** migrate_task_metadata 存量回填与索引重建 ([8a9f1b2](https://github.com/neopen/neotask/commit/8a9f1b2314fc8a01ea17f4ddc5a02c82c426a811))
* **shutdown:** 增加延迟任务 drain 与停机摘要 ([95dbae7](https://github.com/neopen/neotask/commit/95dbae70278876135f94ff4c1d2ad1a288336319))
* **storage:** 优化 SQLite 高并发写入并打通 Redis 多进程主存储 ([1882ac3](https://github.com/neopen/neotask/commit/1882ac3bdec0903b677abc6a136ea3e8762031e7))
* **storage:** 新增 statements_transaction 多语句事务原语 ([c61fc0f](https://github.com/neopen/neotask/commit/c61fc0fc4080e82ced0baa6076bbde5c5b16423f))
* 支持 neotask --benchmark 并输出基准参数信息 ([293e3bc](https://github.com/neopen/neotask/commit/293e3bcc899fed4bf982eae039aa3ccbad3b9f43))


### 🐛 Bug Fixes (问题修复)

* **cli:** 非 Windows 平台不再向 stdout 输出颜色初始化提示 ([d7ff7fa](https://github.com/neopen/neotask/commit/d7ff7fad5dbf4be273a1275cefdfe1ec9d9abad0))
* **compat:** 事件循环原语延迟到 start() 创建，修复 3.8 线程内构造 TaskPool 报错 ([ca75de9](https://github.com/neopen/neotask/commit/ca75de9812ce9ce532def245b9f04a7aa6a27740))
* **core:** 修复优雅关闭排空/强制关闭异常并兼容旧版 Redis ([6056d54](https://github.com/neopen/neotask/commit/6056d54c6729a733d0a326bd8d389a248470738f))
* **core:** 兼容字符串执行器类型、修正分片语义并优化跨节点等待 ([7848c87](https://github.com/neopen/neotask/commit/7848c87a8ce3781d6d126e8281b4978ddf4d21e4))
* **core:** 消除 asyncio 原语跨事件循环导致的等待丢失与挂起 ([d32964e](https://github.com/neopen/neotask/commit/d32964ea4b7c74921736c0ec7438c7e49ddbf7ba))
* **event:** 背压配置接入 TaskPool/TaskScheduler/TaskEngine 主 API ([d6271ca](https://github.com/neopen/neotask/commit/d6271ca60a2030b353525e760a89e6928f5cf680))
* **event:** 阻塞等待队列替代 wait_for 轮询，修复取消被吞导致的收尾卡死 ([11244b0](https://github.com/neopen/neotask/commit/11244b0f3402edc8c76e60e5483db3a386869f03))
* **io-loop:** 修复专用 I/O 循环死亡后命令永久挂起 ([f858851](https://github.com/neopen/neotask/commit/f8588519fe7feedfc0631663511a849405fb83f1))
* **queue:** 删除重复的 re_enqueue 死代码定义 ([8291ae0](https://github.com/neopen/neotask/commit/8291ae0f51f2a0e9a36ea5d23832c81d4de40e79))
* **redis:** 将 Redis 异步客户端统一到专用事件循环 ([a67fcd8](https://github.com/neopen/neotask/commit/a67fcd8ffe911ae9553983d0edd0b5cd4a4734a7))
* **redis:** 让 enable_heartbeat/enable_reclaimer 开关真正生效 ([c545d72](https://github.com/neopen/neotask/commit/c545d72b784640a9371db11c68b5e61203b27233))
* **scheduler:** 修复暂停态非空队列忙等死锁，恢复后即时唤醒 ([fe2bea0](https://github.com/neopen/neotask/commit/fe2bea0ad440f8272d6e06bc1bd3c2c006c5b537))
* **sqlite:** SQLite 统一到专用事件循环并修复测试资源泄漏 ([9cb0e78](https://github.com/neopen/neotask/commit/9cb0e78f29d60d45037266f7ad40a2eb25168305))
* **sqlite:** 共享写连接串行化，消除并发批量提交竞态 ([fb4e954](https://github.com/neopen/neotask/commit/fb4e954092c6ca8ec3ca2a0a08fec1b0f3f2933e))
* **stats:** 按状态计数替代实体聚合，修复万级任务统计截断 ([430a529](https://github.com/neopen/neotask/commit/430a5297fbe0b70ebcb858d67c99091ecbf3276b))
* **storage:** 修复 SQLite 多进程下任务丢失与 database is locked ([1bea020](https://github.com/neopen/neotask/commit/1bea0203ee78dcb94531a8cdf2df058b6d2aae8b))
* **utils:** 专用事件循环重建时重置 Redis/SQLite 客户端代理 ([70ed7e6](https://github.com/neopen/neotask/commit/70ed7e6e9271e2a99bd8cb9fcc1a141a4f87bf43))
* v1.0 本地能力与稳定性（停机/批量语义、日志统一、存储迁移与压测） ([#38](https://github.com/neopen/neotask/issues/38)) ([920dde4](https://github.com/neopen/neotask/commit/920dde41edb397fea6d46cc395d4c37c65dfe314))
* v1.1.1 稳定性与升级体验补丁 ([#44](https://github.com/neopen/neotask/issues/44)) ([1180660](https://github.com/neopen/neotask/commit/1180660c73981a35b1d280481ef07a609d077724))
* **worker:** 任务结束后立即清理运行表 ([23288f2](https://github.com/neopen/neotask/commit/23288f2e8dea3f475ba8861cdf2b9d5584decea7))
* **worker:** 逐任务调度，消除运行表随队列深度膨胀的 O(N²) 开销 ([c6cfc16](https://github.com/neopen/neotask/commit/c6cfc1656f408bb6424b9d7b9f01779c3ce51ff0))
* 事件驱动调度、EventBus 背压与万级任务性能可靠性提升（v1.0.8） ([#40](https://github.com/neopen/neotask/issues/40)) ([c486c02](https://github.com/neopen/neotask/commit/c486c02981dba2074f11377ea90e3c445dc8ed41))
* 优化完善示例并修复 Cron 别名与优雅停机回灌问题 ([c07c2e3](https://github.com/neopen/neotask/commit/c07c2e3d95f56065fcb805e251dbed6bd3f9df21))
* 修复手动重试被跳过与可选依赖 PyYAML 缺失导致的导入失败 ([546290c](https://github.com/neopen/neotask/commit/546290c33b62b589cf5d20d846271783b2dbf25d))
* 修复跨事件循环等待、手动重试、Cron 别名与优雅停机问题 ([#34](https://github.com/neopen/neotask/issues/34)) ([9d85201](https://github.com/neopen/neotask/commit/9d852015191fe1ae1803e5ad51da576fbc1c7fc6))
* 修复重启挂死、事件循环线程泄漏与取消不中断执行 ([c5b975e](https://github.com/neopen/neotask/commit/c5b975e9fcae48817fbfe9b4545cd9b05f4dc3f7))
* 合并Dev ([#18](https://github.com/neopen/neotask/issues/18)) ([74996d5](https://github.com/neopen/neotask/commit/74996d5a067b3e0275a54992b14efe969ab30fde))
* 合并修复 ([a07a445](https://github.com/neopen/neotask/commit/a07a445ba1967e311f897b5e15783179c9943b4a))
* 支持进度回调，从“有订阅无发布”到端到端可用；周期任务支持持久化，默认开启 + 明确降级告警；其他相关文档和测试优化。 ([d61986a](https://github.com/neopen/neotask/commit/d61986a53a945ce7e0eef408d107422932beb087))


### ⚡ Performance Improvements (性能优化)

* **lifecycle:** 修复缓存淘汰 O(n²) 回退并发布 1.0.6 ([7edc45b](https://github.com/neopen/neotask/commit/7edc45b7b2e53b1940ee0dd351a7f51e7d4368e8))
* **lifecycle:** 新增 running_persist_delay 降低执行侧写入 ([3393b77](https://github.com/neopen/neotask/commit/3393b774f25a2f037e40a3e906dd344d7435fff3))
* **scheduler:** 入队事件唤醒替代空转退避，修复延时抖动与时间轮漂移 ([b6aee91](https://github.com/neopen/neotask/commit/b6aee910e6aa517e3a9aae0d5137368eac0a2d76))
* **sqlite:** 为队列出队补充复合索引 ([ca22d8a](https://github.com/neopen/neotask/commit/ca22d8af7f2f1c0dd582fbbf0263a637eb676188))
* **submit:** 提交路径合并往返，Redis 用单条 Lua 原子创建并入队 ([c34c503](https://github.com/neopen/neotask/commit/c34c503210009b588d960ed8c5f5d5f99a173022))
* **worker:** 预取改为需求事件驱动，解除吞吐上限 ([72af039](https://github.com/neopen/neotask/commit/72af0396d3672d0da86922a28bd35bb4bfc60e3e))
* 存储并发、批量提交的性能优化与提交路径优化 ([#36](https://github.com/neopen/neotask/issues/36)) ([2fa8f6b](https://github.com/neopen/neotask/commit/2fa8f6b05fe226f62eda5228e8a3fc63c9b0ba17))


### ♻️ Code Refactoring (代码重构)

* **logger:** 事件/锁/监控模块统一使用 common.logger ([b9ea49f](https://github.com/neopen/neotask/commit/b9ea49ff48b306a5039f5fa59d290fd7cc97c710))
* **storage:** 仓储基类补充 close 并移除 hasattr 兜底 ([0fd7378](https://github.com/neopen/neotask/commit/0fd73782510748abca6c727e0617111df4d8bb97))


### 📝 Documentation (文档更新)

* **architecture:** 配置参考补充事件总线背压字段 ([ccd9919](https://github.com/neopen/neotask/commit/ccd99197aa497de02ae5e8f14b805cc26fdee077))
* **contract:** 冻结执行面线协议并改写路线图为控制面/执行面定位 ([ae95075](https://github.com/neopen/neotask/commit/ae95075be4d3505b984b1e6c450180aa684bc2d9))
* **examples:** 批次 3 Checkpoint 与 Blackboard 示例、索引导航与文档同步 ([a0cc31e](https://github.com/neopen/neotask/commit/a0cc31ed8fabd90243af40f0737cca42f8bd7ada))
* **examples:** 新增 memory → Redis 存储迁移示例 ([d602348](https://github.com/neopen/neotask/commit/d602348bd646b286d4ce45a98590e7018f3768fb))
* **examples:** 新增 memory → SQLite 存储迁移示例 ([1bddf27](https://github.com/neopen/neotask/commit/1bddf27e6b9b1a88415b995f97bb5e6489a14875))
* **examples:** 新增 SQLite → Redis 存储迁移示例 ([15ad0d1](https://github.com/neopen/neotask/commit/15ad0d1c65ca8a71ed37f657fc373b83a5dc6b6c))
* **examples:** 补充 EventBus 背压与低抖动延时示例，更新过时精度说明 ([8f3a9c3](https://github.com/neopen/neotask/commit/8f3a9c3dd28b7893c83bef4b87ba0007f0c10abd))
* **readme:** 双语路线图与架构图同步 v1.1 已交付与执行面定位 ([a6c865e](https://github.com/neopen/neotask/commit/a6c865e43e7b300bb1dd8511d5ef330735fae6c0))
* **report:** 2026-11-11 真实环境集成测试报告与三项修复回归验证 ([0f491f3](https://github.com/neopen/neotask/commit/0f491f3a47b3caeecb5b48960f89f70c276d1089))
* **report:** 标记 7.4 已修复，问题清单全部闭环 ([d5ad78b](https://github.com/neopen/neotask/commit/d5ad78bce22c07a8817201332daf5e5d583153c9))
* **report:** 补充最终压力复测、distributed/chaos 与全量门禁结果 ([f1cc7e7](https://github.com/neopen/neotask/commit/f1cc7e7565d681eb1cb68d51f09a22c9fae26420))
* 修复文档 ([#17](https://github.com/neopen/neotask/issues/17)) ([f36be26](https://github.com/neopen/neotask/commit/f36be26e86f9314dabdde5e794ea3f559c3fc172))
* 批次 2 文档、示例索引与设计进度同步 ([e5fc224](https://github.com/neopen/neotask/commit/e5fc224ab42953f8a6e6579f0e6b499e7f062929))
* 文档优化 ([24758f6](https://github.com/neopen/neotask/commit/24758f632306065a72392874f1a1acf9a73570aa))
* 文档优化 ([c208b6c](https://github.com/neopen/neotask/commit/c208b6ce2be4bac5d152ff9363d99ead46e75a3c))
* 文档信息变更 ([1321d1e](https://github.com/neopen/neotask/commit/1321d1e106ee274fe4d9e75c48477e4cbeb3c530))
* 文档内容补充 ([16022a8](https://github.com/neopen/neotask/commit/16022a83c56bb60d67cb6b2bdd822748b4ab74e7))
* 文档对齐 v1.0.3 实现并修正过期内容 ([255181f](https://github.com/neopen/neotask/commit/255181fe43c30dd19510d8d8f2f6a99b4392bac4))
* 新增三存储极限操作性能压测章节 ([1635c72](https://github.com/neopen/neotask/commit/1635c72eded42b45e8b22f9bdf3ffcfc7361e3df))
* 新增版本演进规划文档 ([645cb4d](https://github.com/neopen/neotask/commit/645cb4df5fcd816b5762f0d768236a9c496a60f0))
* 更新 README 架构图、路线图与 v1.1 能力说明 ([15674af](https://github.com/neopen/neotask/commit/15674af7061c87d24d390477bd2b45f7d8df94da))
* 更新批量提交与停机语义说明 ([3d54a7b](https://github.com/neopen/neotask/commit/3d54a7bdbcc8fd50a1df563c56ab614a2025fa6b))
* 补充 A/B 性能对照与 2026-11-10 集成测试报告 ([0442406](https://github.com/neopen/neotask/commit/04424060de4926091703e20d3992a2726f1de4bc))
* 记录 10-29 剩余既有问题修复与全量回归结果 ([e4fa886](https://github.com/neopen/neotask/commit/e4fa886edd2791b036eef52ab545dbf2c70a5b15))


### 💄 Styles (代码样式)

* Dev ([#26](https://github.com/neopen/neotask/issues/26)) ([cd2cd8a](https://github.com/neopen/neotask/commit/cd2cd8aeaa9b01a5c17e2db445f3673e80674dbb))
* 修复工作流配置 ([3a61955](https://github.com/neopen/neotask/commit/3a619550e721adb4cae1f9c4fe35513f69b86ffa))
* 修改测试及数据路径，优化测试脚本 ([4414375](https://github.com/neopen/neotask/commit/4414375faf20c07e81cee73ba523c9e9a789361c))
* 合并 ([#24](https://github.com/neopen/neotask/issues/24)) ([fa6654a](https://github.com/neopen/neotask/commit/fa6654ab4a5509a2c1c2c4362d130d3e000c1356))
* 工作流配置 ([96afd3a](https://github.com/neopen/neotask/commit/96afd3a8690c2e11cfb9fd3f42303e749eabebcd))
* 工作流配置修复 ([064dcce](https://github.com/neopen/neotask/commit/064dcce87f5bc5ae119aeb139ddcd1768906389c))
* 标准注释格式，添加 Licensed 和 Copyright ([b020eb6](https://github.com/neopen/neotask/commit/b020eb6974763c023478ec3758ccbef9f0598812))
* 格式 ([#32](https://github.com/neopen/neotask/issues/32)) ([fd3f019](https://github.com/neopen/neotask/commit/fd3f019c0844acdc1d983a7e4ee015ef741393ac))
* 添加依赖 ([6a2b829](https://github.com/neopen/neotask/commit/6a2b8296f6d25640521e7c11a651ed462794646f))


### 🔧 Other Changes (其他变更)

* **deps:** redis 依赖增加 &lt;6 上界 ([01cf39b](https://github.com/neopen/neotask/commit/01cf39ba1fa0702f5ea658d195099e8c3f43e26c))
* **logger:** 移除误留的外项目 GitHub 头行 ([9e23720](https://github.com/neopen/neotask/commit/9e23720e4c94da0adf41c5bc38c99a1bd460d55d))
* **main:** release 1.0.3 ([#25](https://github.com/neopen/neotask/issues/25)) ([36690bf](https://github.com/neopen/neotask/commit/36690bfa7302ac4f86352692d50eb6ae6e455d14))
* **main:** release 1.0.4 ([#27](https://github.com/neopen/neotask/issues/27)) ([6da3ee0](https://github.com/neopen/neotask/commit/6da3ee004eea2f403fcd98c4445c2da3ff20fb61))
* **main:** release 1.0.5 ([#35](https://github.com/neopen/neotask/issues/35)) ([ec8d571](https://github.com/neopen/neotask/commit/ec8d571afb2f71933817939cbcde123e537deb84))
* **main:** release 1.0.6 ([#37](https://github.com/neopen/neotask/issues/37)) ([8cd9dde](https://github.com/neopen/neotask/commit/8cd9dde61b77694fed191a71e30b82caca33a50c))
* **main:** release 1.0.7 ([#39](https://github.com/neopen/neotask/issues/39)) ([c1ee5fa](https://github.com/neopen/neotask/commit/c1ee5fa173675dec46d1ade81b24c6ece512bce8))
* **main:** release 1.0.8 ([#41](https://github.com/neopen/neotask/issues/41)) ([edac82b](https://github.com/neopen/neotask/commit/edac82b75f9f1eae34edb1521b986c375433e050))
* **main:** release 1.1.0 ([4c95f60](https://github.com/neopen/neotask/commit/4c95f60dc3c0e0196eefa612ab5eaeec0f720acd))
* **main:** release 1.1.1 ([#45](https://github.com/neopen/neotask/issues/45)) ([013382d](https://github.com/neopen/neotask/commit/013382d0db19238a5c2017502a62a93b4b67c3cf))
* **merge:** 合入 feat_1.1 基准测试能力 ([1c4ef4b](https://github.com/neopen/neotask/commit/1c4ef4b8f79e088868084facad02487465d31154))
* **merge:** 合入 main 分支 1.0.7 发布 ([f88d1ec](https://github.com/neopen/neotask/commit/f88d1ece8a9b56563550a3831c8e9df0bc7459b9))
* **merge:** 合并 dev 的 1.1.0 本地能力组件到 main ([09b5872](https://github.com/neopen/neotask/commit/09b587212007fe8142fe75bcb7822a467fcc9554))
* **merge:** 合并 main（v1.0.8）到 dev，保留 1.0.8 版本基线 ([5ed9988](https://github.com/neopen/neotask/commit/5ed9988c57d46508fa585bd5470aa25a43449fb6))
* **packaging:** 统一发布元数据并收敛依赖与工具链 ([ac3481a](https://github.com/neopen/neotask/commit/ac3481a7acc97a3d916a954fac4971da23189703))
* **release:** 同步版本到已发布的 1.0.4 并标注待发布 1.0.5 ([38738ee](https://github.com/neopen/neotask/commit/38738ee16c78f7c5ba6db3f09ef38bd85ce93a4a))
* **scripts:** 迁移脚本从 examples 移入 scripts 目录 ([1f0634c](https://github.com/neopen/neotask/commit/1f0634c552ebb7bc966be80f323aa84d3c66893c))
* 其他 ([1a3efa3](https://github.com/neopen/neotask/commit/1a3efa327b1368f1b490103d19e0e1ac674353ad))

## [1.1.1](https://github.com/neopen/neotask/compare/v1.1.0...v1.1.1) (2026-11-15)


### 🐛 Bug Fixes (问题修复)

* v1.1.1 稳定性与升级体验补丁 ([#44](https://github.com/neopen/neotask/issues/44)) ([1180660](https://github.com/neopen/neotask/commit/1180660c73981a35b1d280481ef07a609d077724))

## [1.1.0](https://github.com/neopen/neotask/compare/v1.0.8...v1.1.0) (2026-11-13)


### ✨ Features (新功能)

* **api:** 定稿外部 task_id 幂等语义并补齐 TaskScheduler 异步存在性检查 ([ee0d77c](https://github.com/neopen/neotask/commit/ee0d77ce38fc6782b67a08ecc7c3385e1c0f264e))
* **checkpoint:** CheckpointRepository 三后端实现与独立工厂 ([ca4c629](https://github.com/neopen/neotask/commit/ca4c629b656e475065ec008943e314199e24fcee))
* **checkpoint:** 配置装配、双形态 API 与断点续跑恢复路径 ([cb4ae87](https://github.com/neopen/neotask/commit/cb4ae877cf5c29e1e2e53e9d6159228babfb8e53))
* **context:** 新增 Memory-only 本地 Blackboard 上下文 ([d448497](https://github.com/neopen/neotask/commit/d448497cb1ba42fae216275fff947556d86aaae8))
* **core:** 新增业务级失败上报 report_failure，定稿 FAILED 收尾语义 ([70ffe29](https://github.com/neopen/neotask/commit/70ffe299fe5a4685ef782346bf93155089353ff1))
* **cost,parent:** 成本采集、耗时字段与父子任务公开 API ([758b108](https://github.com/neopen/neotask/commit/758b10800e48f9fd1a5a51968c207a5dabd6f3ca))
* **event:** TaskEvent 信封稳定化，补齐 timestamp/node_id/schema_version ([b41ba69](https://github.com/neopen/neotask/commit/b41ba691c4f3224d7b493fa2cef58fa18cb9c3cf))
* **event:** 新增 EventType 事件名常量注册表并替换散写字符串 ([1606202](https://github.com/neopen/neotask/commit/1606202c61f3f643fe15959e0846da9d7e002cee))
* **model,storage:** Task 元数据字段与三后端新列 ([c139b08](https://github.com/neopen/neotask/commit/c139b081545efc395deddf49c880b7edcbf6f67d))
* **scripts:** migrate_task_metadata 存量回填与索引重建 ([8a9f1b2](https://github.com/neopen/neotask/commit/8a9f1b2314fc8a01ea17f4ddc5a02c82c426a811))
* **storage:** 新增 statements_transaction 多语句事务原语 ([c61fc0f](https://github.com/neopen/neotask/commit/c61fc0fc4080e82ced0baa6076bbde5c5b16423f))


### 📝 Documentation (文档更新)

* **contract:** 冻结执行面线协议并改写路线图为控制面/执行面定位 ([ae95075](https://github.com/neopen/neotask/commit/ae95075be4d3505b984b1e6c450180aa684bc2d9))
* **examples:** 批次 3 Checkpoint 与 Blackboard 示例、索引导航与文档同步 ([a0cc31e](https://github.com/neopen/neotask/commit/a0cc31ed8fabd90243af40f0737cca42f8bd7ada))
* **readme:** 双语路线图与架构图同步 v1.1 已交付与执行面定位 ([a6c865e](https://github.com/neopen/neotask/commit/a6c865e43e7b300bb1dd8511d5ef330735fae6c0))
* 批次 2 文档、示例索引与设计进度同步 ([e5fc224](https://github.com/neopen/neotask/commit/e5fc224ab42953f8a6e6579f0e6b499e7f062929))


### 🔧 Other Changes (其他变更)

* **merge:** 合并 dev 的 1.1.0 本地能力组件到 main ([09b5872](https://github.com/neopen/neotask/commit/09b587212007fe8142fe75bcb7822a467fcc9554))
* **merge:** 合并 main（v1.0.8）到 dev，保留 1.0.8 版本基线 ([5ed9988](https://github.com/neopen/neotask/commit/5ed9988c57d46508fa585bd5470aa25a43449fb6))

## [1.0.8](https://github.com/neopen/neotask/compare/v1.0.7...v1.0.8) (2026-11-11)


### 🐛 Bug Fixes (问题修复)

* 事件驱动调度、EventBus 背压与万级任务性能可靠性提升（v1.0.8） ([#40](https://github.com/neopen/neotask/issues/40)) ([c486c02](https://github.com/neopen/neotask/commit/c486c02981dba2074f11377ea90e3c445dc8ed41))

## [1.0.7](https://github.com/neopen/neotask/compare/v1.0.6...v1.0.7) (2026-11-10)


### 🐛 Bug Fixes (问题修复)

* v1.0 本地能力与稳定性（停机/批量语义、日志统一、存储迁移与压测） ([#38](https://github.com/neopen/neotask/issues/38)) ([920dde4](https://github.com/neopen/neotask/commit/920dde41edb397fea6d46cc395d4c37c65dfe314))

## [1.0.6](https://github.com/neopen/neotask/compare/v1.0.5...v1.0.6) (2026-11-08)


### ⚡ Performance Improvements (性能优化)

* 存储并发、批量提交的性能优化与提交路径优化 ([#36](https://github.com/neopen/neotask/issues/36)) ([2fa8f6b](https://github.com/neopen/neotask/commit/2fa8f6b05fe226f62eda5228e8a3fc63c9b0ba17))

## [1.0.5](https://github.com/neopen/neotask/compare/v1.0.4...v1.0.5) (2026-10-29)


### 🐛 Bug Fixes (问题修复)

* 修复跨事件循环等待、手动重试、Cron 别名与优雅停机问题 ([#34](https://github.com/neopen/neotask/issues/34)) ([9d85201](https://github.com/neopen/neotask/commit/9d852015191fe1ae1803e5ad51da576fbc1c7fc6))


### 💄 Styles (代码样式)

* 格式 ([#32](https://github.com/neopen/neotask/issues/32)) ([fd3f019](https://github.com/neopen/neotask/commit/fd3f019c0844acdc1d983a7e4ee015ef741393ac))

## [Unreleased]

> 待发布版本：**1.0.5**（GitHub Release 与版本号由 release-please 在 Release PR 中生成）

### 新增（2026-10-29）

- **轻量基准入口**：新增 `neotask.benchmark`，可用 `python -m neotask.benchmark`（或统一 CLI `neotask --benchmark` / `python -m neotask --benchmark`）跑一轮简化基准；支持 `--executor 模块:函数` / `--executor 文件.py:函数`（兼容 Windows 绝对路径）、`--tasks`、`--concurrency`、`--warmup`、`--payload`、`--json`，并关闭健康检查/指标/Reporter，测量端到端延迟（avg/p50/p95/p99）与吞吐；报告与 JSON 输出包含任务数、并发度、预热、超时、预取、存储、执行器方式与数据模板等基本参数；同步执行器自动走线程池（`executor_mode="thread"`）避免阻塞事件循环；代码中可调用 `run_benchmark` / `run_benchmark_async` 返回 `BenchmarkResult`

### 修复（2026-10-29）

- **跨事件循环 asyncio 原语**：Redis/SQLite 访问统一经 `utils/dedicated_loop.py` 的专用事件循环执行，新增 `LoopBoundRedisClient` / `LoopBoundSQLiteConnection` 代理；连接池仍然复用，专用 loop 重建时自动丢弃旧客户端并重新建池
- **跨节点等待**：`LifecycleManager.wait_for_task` 改为“Future 通知 + 0.2s 时间片仓储回查”，不再等满 `timeout`，并同步刷新本地缓存
- **执行器类型**：`ExecutorFactory.create` 与 `TaskPoolConfig.executor_type` 兼容 `"async" / "thread" / "process" / "class" / "auto"` 字符串，未知值抛 `ValueError`
- **分片语义**：`ConsistentHashSharder` 支持 md5 / sha1 / sha256 / sha3_256；`ModuloSharder` 改用稳定 md5 哈希（不再受 `PYTHONHASHSEED` 影响）；`RangeSharder` 明确“尾部溢出归末分片、其余返回 -1”
- **测试与回归**：修复过期测试资产、时序敏感断言与共享 Redis 队列污染，全量 `pytest tests` 恢复为 625 passed / 6 skipped / 0 failed
- **可选依赖守卫**：`common/logger.py` 中 PyYAML 改为可选导入，未安装时回退内置日志默认值，避免干净安装（wheel 未装 PyYAML）时 `import neotask` 直接失败
- **手动重试**：`TaskPool.retry()` 先将 failed / cancelled 任务重置为 PENDING（`Task.reset_for_retry()` + `LifecycleManager.reset_for_retry()`）再重新入队，修复重试后 Worker 因状态不是 PENDING 而跳过执行的问题；已在队列中的 PENDING 任务不再重复入队
- **Cron 别名**：`@hourly` / `@daily` / `@weekly` 等预定义别名统一在 `CronParser.parse` / `is_valid` / `describe` 中归一化为标准 5 字段表达式，修复“`is_valid` 返回 True 但 `submit_cron` 抛 `Invalid cron expression`”的口径不一致
- **优雅停机**：`WorkerPool.stop()` 先停预取循环、让 Worker 消费完本地队列再退出，避免停机瞬间本地残留任务无人消费而白等满 `timeout`；`TaskPrefetcher.drain_back()` 改用新增的 `QueueScheduler.re_enqueue()` 绕过停机时的入队禁用，未执行任务不再变成孤儿
- **队列日志**：`MemoryQueueRepository.remove` 在优先级队列未命中时降级为 `debug`，不再对“随后命中延迟队列”的正常流程输出误导性 WARNING

### 文档（2026-10-29）

- `docs/ARCHITECTURE.md` 对齐 1.0.5：更新当前能力、路线图、配置默认值、项目结构（新增 `benchmark.py`），并新增“事件循环隔离与存储客户端”“分布式与高可用”章节
- 修正 README/README_zh 中 `submit_delayed/interval/cron` 的参数名、覆盖率命令与重复段落
- README/README_zh 新增“轻量性能自测”小节，说明 `python -m neotask.benchmark`、自定义执行器、`--json`、`--warmup` 与指标口径
- 补齐 1.0.4 发布记录（release-please 生成的条目）
- 重写 `examples/` 示例并逐个实际运行验证：修正优先级演示的确定性（pause/resume）、批量任务改用 `submit_batch`、延时/Cron/周期/超时的输出与口径，新增 `11_sqlite_persistence.py`、`12_timeout.py`，同步更新 `examples/readme.txt`

### 计划中 (v1.5 / v2.0)
- **任务编排（v1.5）**
  - DAG 工作流引擎（`WorkflowEngine`）
  - 条件分支执行
  - 并行任务编排
- **企业级功能（v2.0）**
  - 独立 Web UI 监控面板（多进程形态）
  - Prometheus 指标拉取端点
  - 多租户隔离支持

> v0.4/v0.5/v1.0 的能力（Redis 共享队列、分布式锁、预取、看门狗续期、超时检测、故障自动恢复）均已发布，记录见下。

## [1.0.4](https://github.com/neopen/neotask/compare/v1.0.3...v1.0.4) (2026-10-04)

### 💄 Styles (代码样式)

* Dev ([#26](https://github.com/neopen/neotask/issues/26)) ([cd2cd8a](https://github.com/neopen/neotask/commit/cd2cd8aeaa9b01a5c17e2db445f3673e80674dbb))

## [1.0.3](https://github.com/neopen/neotask/compare/v1.0.2...v1.0.3) (2026-10-03)


### 🐛 Bug Fixes (问题修复)

* 合并Dev ([#18](https://github.com/neopen/neotask/issues/18)) ([74996d5](https://github.com/neopen/neotask/commit/74996d5a067b3e0275a54992b14efe969ab30fde))


### 📝 Documentation (文档更新)

* 修复文档 ([#17](https://github.com/neopen/neotask/issues/17)) ([f36be26](https://github.com/neopen/neotask/commit/f36be26e86f9314dabdde5e794ea3f559c3fc172))


### 💄 Styles (代码样式)

* 合并 ([#24](https://github.com/neopen/neotask/issues/24)) ([fa6654a](https://github.com/neopen/neotask/commit/fa6654ab4a5509a2c1c2c4362d130d3e000c1356))

---

## [1.0.2] - 2026-09-12

### 修复

- **分布式链路缺陷修复**：Redis 延迟队列迁移回优先级队列时丢失优先级的问题——`queue:delayed` 只存 task_id 到到期时间的映射、不带优先级字段，迁移时统一按 `TaskPriority.NORMAL` 入队
- **延时队列回调静默失败**：`DelayedQueue` 中回调异常被 `except Exception: pass` 吞掉，改为记录错误日志
- **Python 3.8 运行期阻塞点**：`core/future.py` 的 `self._futures: dict[str, TaskFuture]` 是会在运行期求值的属性注解，PEP 585 泛型在 3.8 抛 `TypeError`；改回 `typing.Dict`

### 变更

- **依赖收敛**：`pip install neotask` 的强制依赖由 4 个（aiosqlite/redis/psutil/croniter）收敛为 2 个（`aiosqlite>=0.19.0`、`croniter>=1.0.0`）；`redis`、`psutil` 移回可选 extra，源码中原有 import 守卫（`HAS_REDIS`、psutil 惰性导入）保证缺失时仍可 `import neotask`
- **最低 Python 版本统一为 ≥ 3.8**：`requires-python`、classifiers、black/mypy/ruff 的 target-version 全部对齐 3.8
- **版本号三方对齐**：`pyproject.toml` / `[tool.bumpversion]` / `neotask.__version__` 统一为 `1.0.2`（此前 1.0.0/1.0.1/1.0.2 混用）
- **CI 矩阵**：`.github/workflows/ci.yml` 改为 3.8 / 3.10 / 3.12 三版本矩阵，并新增 `import neotask` 冒烟测试作为最低版本保护线

### 文档

- 依赖与版本口径统一到 1.0.2 / Python ≥ 3.8，覆盖 `README.md`、`README_zh.md`、`docs/`、示例与系列文章
- 移除 README 中尚未实现的 `WorkflowEngine` / `engine.submit_workflow` 等 API 条目与「DAG 工作流」特性宣传，改由路线图 timeline 的 `v1.5` 段承载
- 将「零依赖部署」表述修正为「无外部服务依赖」，并如实标注 2 个基础依赖

---

## [1.0.1] - 2026-08-20

### 修复

- 状态守门、回收判据、心跳契约等行为修复

---

## [1.0.0] - 2026-05-11

### 新增

- 死信队列（`DeadLetterQueue`，Redis 模式默认启用，保留 7 天，支持 `replay`）

---

## [0.5.0] - 2026-05-10

### 变更

- 该 tag 无文件级变化（预取机制与批量操作已在此前版本落地）

---

## [0.4.1] - 2026-05-08

### 移除

- 删除 `distributed/elector`（Redis 主节点选举存活 7 天后弃用）

---

## [0.4.0] - 2026-05-01

### 新增

- `distributed` 包：`node`（节点管理与心跳）、`coordinator`、`elector`、`sharding`
- Redis 共享队列与分布式锁

---

## [0.3.0] - 2026-04-22

### 新增功能

#### 定时任务调度器 (`TaskScheduler`)
- **延时任务** (`submit_delayed`) - 支持指定延迟秒数后执行
- **定时任务** (`submit_at`) - 支持指定具体时间点执行
- **周期任务** (`submit_interval`) - 支持固定间隔重复执行
- **Cron 任务** (`submit_cron`) - 支持标准 5 字段 Cron 表达式
- 任务暂停/恢复 (`pause_periodic` / `resume_periodic`)
- 任务取消 (`cancel_periodic`)
- 周期任务列表查询 (`get_periodic_tasks`)
- 优雅关闭支持

#### 延迟队列 (`DelayedQueue`)
- 独立的时间堆实现
- 自动到期触发机制
- 任务取消支持
- 批量调度支持
- 统计信息收集

#### Cron 表达式解析器 (`CronParser`)
- 标准 5 字段 Cron 语法支持（分钟、小时、日、月、周）
- 特殊字符支持：`*`（任意）、`,`（列表）、`-`（范围）、`/`（步长）
- 预定义表达式：`@yearly`、`@monthly`、`@weekly`、`@daily`、`@hourly`
- 获取下次执行时间 (`next`)
- 获取上次执行时间 (`previous`)
- 批量获取多次执行时间 (`get_next_n`)
- 表达式验证 (`is_valid`)
- 中文描述生成 (`describe`)

#### 时间轮 (`TimeWheel`)
- 高性能定时器实现
- 多轮调度支持（大延迟任务）
- 槽位数量可配置
- 滴答间隔可配置
- 任务取消支持

#### 周期任务管理器 (`PeriodicTaskManager`)
- 周期任务完整生命周期管理
- 固定间隔和 Cron 两种调度模式
- 最大执行次数限制 (`max_runs`)
- 时间范围限制 (`start_at` / `end_at`)
- 错过执行策略 (`MissedExecutionPolicy`)
  - `IGNORE` - 忽略错过的执行
  - `RUN_ONCE` - 只执行一次（不追赶）
  - `CATCH_UP` - 追赶所有错过的执行
  - `SKIP` - 跳过错过的执行
- 任务更新 (`update_task`)
- 任务暂停/恢复 (`pause` / `resume`)
- 任务删除 (`delete_task`)
- 标签过滤和分页查询 (`list_tasks`)
- 执行历史记录 (`get_execution_history`)

#### 配置增强
- 新增 `SchedulerConfig` 配置类
  - `scan_interval` - 调度循环扫描间隔
  - `enable_periodic_manager` - 是否启用周期任务管理器
  - `enable_time_wheel` - 是否启用时间轮
  - `time_wheel_slots` - 时间轮槽位数
  - `time_wheel_tick` - 时间轮滴答间隔
  - `default_max_runs` - 默认最大执行次数
  - `default_missed_policy` - 默认错过执行策略
- 预设配置工厂方法
  - `high_performance()` - 高性能配置（启用时间轮）
  - `lightweight()` - 轻量级配置（禁用周期管理器）

### 改进
- 优化 `TaskScheduler` 与 `TaskPool` 的委托关系
- 完善事件循环处理，支持混合线程/异步场景
- 增强 Cron 表达式中月末(`L`)和星期(`#`)支持
- 改进延迟队列的调度精度
- 统一配置类命名和结构
- 完善异常处理和错误信息

### 文档
- 新增定时任务使用示例
  - `examples/05_cron_tasks.py` - Cron 任务示例
  - `examples/06_delayed_tasks.py` - 延时任务示例
  - `examples/08_periodic.py` - 周期任务示例
- 更新 README 添加 TaskScheduler API 说明
- 补充架构演进路线图
- 新增 API 参考文档

### 测试
- 新增 Cron 表达式解析器单元测试 (`test_cron_parser.py`)
- 新增延迟队列功能测试 (`test_delayed_queue.py`)
- 新增时间轮单元测试 (`test_time_wheel.py`)
- 新增周期任务管理器测试 (`test_periodic_manager.py`)
- 新增 TaskScheduler 集成测试 (`test_task_scheduler.py`)
- 测试覆盖率提升至 85%+

### 修复
- 修复事件循环嵌套导致的 `RuntimeError`
- 修复 `_periodic_tasks` 缺失问题
- 修复 `_execute_periodic_task` 参数传递错误
- 修复调度线程中事件循环处理

---

## [0.2.0] - 2026-04-15

### 新增功能
- **事件总线系统** (`EventBus`)
  - 支持任务生命周期事件监听
  - 内置事件类型：created, started, completed, failed, cancelled
  - 异步事件处理器注册
  - 全局事件订阅支持
  
- **指标收集器** (`MetricsCollector`)
  - 实时任务统计（总数、成功数、失败数、取消数）
  - 执行时间统计（平均值、P95）
  - 排队时间统计
  - Worker 池状态监控
  - 队列长度跟踪
  
- **健康检查** (`HealthChecker` / `SystemHealthChecker`)
  - 系统资源监控（CPU、内存、磁盘）
  - Worker 存活检测
  - 存储后端可用性检查
  - 队列健康状态检查
  - 健康状态聚合 (`HEALTHY` / `DEGRADED` / `UNHEALTHY`)

- **指标报告器**
  - 控制台输出 (`ConsoleReporter`)
  - 文件日志 (`FileReporter`)
  - Prometheus 导出 (`PrometheusReporter`)
  - 报告管理器 (`ReporterManager`)

- **分布式锁** (`LockFactory`)
  - 内存锁实现 (`MemoryLock`)
  - Redis 分布式锁 (`RedisLock`)
  - 看门狗自动续期机制 (`WatchDog`)
  - 锁管理器 (`LockManager`)
  - 上下文管理器支持 (`async with lock.lock()`)

### 改进
- 重构 Worker 池管理逻辑
- 优化优先级队列性能
- 增强 SQLite 存储的事务处理
- 改进日志格式和输出
- 完善类型注解覆盖

### 文档
- 新增事件回调使用示例 (`examples/04_events.py`)
- 补充监控和可观测性文档
- 更新架构图展示新组件
- 新增配置说明文档

### 修复
- 修复任务取消时的资源泄漏问题
- 修复并发场景下的竞态条件
- 修复 SQLite 连接未正确关闭的问题
- 修复指标收集器线程安全问题

---

## [0.1.0] - 2026-04-08

### 初始发布

#### 核心任务池 (`TaskPool`)
- 异步任务提交和执行
- 优先级队列支持（0: CRITICAL, 1: HIGH, 2: NORMAL, 3: LOW）
- 多 Worker 并发处理
- 任务状态追踪（PENDING, RUNNING, SUCCESS, FAILED, CANCELLED, SCHEDULED）
- 任务结果存储和查询
- 任务取消支持

#### 存储后端
- **内存存储** (`MemoryTaskRepository` / `MemoryQueueRepository`)
  - 适合开发和测试环境
  - 零配置，高性能
- **SQLite 存储** (`SQLiteTaskRepository` / `SQLiteQueueRepository`)
  - 单机持久化
  - 支持事务
  - 自动创建索引
- **Redis 存储** (`RedisTaskRepository` / `RedisQueueRepository`)
  - 分布式共享队列
  - 连接池支持
  - Lua 脚本原子操作

#### 任务执行器
- **异步执行器** (`AsyncExecutor`) - 直接执行异步函数
- **线程执行器** (`ThreadExecutor`) - 线程池执行同步函数
- **进程执行器** (`ProcessExecutor`) - 进程池执行 CPU 密集型任务
- **回调执行器** (`CallbackExecutor`) - 包装普通函数
- 执行器工厂 (`ExecutorFactory`) - 自动选择执行器类型

#### Worker 管理
- 动态 Worker 池 (`WorkerPool`)
- 任务预取机制 (`prefetch_size`)
- Worker 监督者 (`WorkerSupervisor`)
  - 健康检查
  - 自动重启故障 Worker
  - 失败率监控
- 重试机制（可配置次数和延迟）

#### 队列系统
- **优先级队列** (`PriorityQueue`) - 基于堆实现
- **延迟队列** (`DelayedQueue`) - 基于时间堆
- **队列调度器** (`QueueScheduler`) - 整合两种队列
- 暂停/恢复支持
- 队列清空等待

#### 辅助功能
- Future 异步结果等待 (`TaskFuture` / `FutureManager`)
- 任务上下文 (`TaskContext`) - 链路追踪
- 事件总线基础框架
- 异常体系 (`TaskSchedulerError` 系列)

#### Web UI（可选）
- FastAPI 后端服务
- 实时任务监控面板
- WebSocket 实时推送
- 任务控制操作（取消、重试）

#### 配置类
- `StorageConfig` - 存储配置
- `LockConfig` - 锁配置
- `WorkerConfig` - Worker 配置
- `QueueConfig` - 队列配置
- `ExecutorConfig` - 执行器配置
- `WebUIConfig` - Web UI 配置
- `TaskPoolConfig` - TaskPool 配置
- `TaskConfig` - 统一配置（兼容旧版）

### 示例代码
- `examples/00_quick_start.py` - 快速入门
- `examples/01_simple.py` - 基础用法
- `examples/02_context_manager.py` - 上下文管理器
- `examples/03_priority.py` - 优先级调度
- `examples/04_events.py` - 事件回调
- `examples/05_cron_tasks.py` - Cron 任务
- `examples/06_delayed_tasks.py` - 延时任务
- `examples/07_batch.py` - 批量任务处理
- `examples/08_periodic.py` - 周期任务
- `examples/09_retry_and_cancel.py` - 重试和取消
- `examples/10_task_query.py` - 任务查询
- `examples/99_all_features.py` - 全功能演示

---

## 版本说明

### 版本号规则
- **主版本号** (MAJOR)：不兼容的 API 修改
- **次版本号** (MINOR)：向下兼容的功能性新增
- **修订号** (PATCH)：向下兼容的问题修正

### 符号说明
- `新增` - 新功能或组件
- `改进` - 现有功能的优化
- `修复` - Bug 修复
- `变更` - 破坏性变更（会在升级指南中详细说明）
- `废弃` - 即将移除的功能
- `移除` - 已移除的功能
- `安全` - 安全相关的修复

---

## 升级指南

### 从 1.0.1 升级到 1.0.2

#### 依赖变更（唯一需要动作的地方）

`redis`、`psutil` 不再是强制依赖。如果你的项目此前依赖 `pip install neotask` 顺带装上它们，升级后需显式声明：

```bash
# 用到 Redis 存储 / 分布式锁 / 去中心化
pip install "neotask[redis]"

# 用到系统指标采集（get_stats()["metrics"]["system"]、健康检查 system 项）
pip install "neotask[monitor]"

# 或一次装全
pip install "neotask[full]"
```

#### 最低 Python 版本

`requires-python` 由 `>=3.9` 降为 `>=3.8`，对已有环境无影响；Python 3.8 用户此前会被 pip 拒绝安装，现在可用。

#### API 变更

无破坏性变更，完全向后兼容。

---

### 从 0.2.x 升级到 0.3.0

#### 新增依赖
如果使用定时任务功能，无需额外依赖。Cron 解析器为纯 Python 实现。

#### API 变更
无破坏性变更，完全向后兼容。

#### 迁移步骤

```python
# 1. 导入新的调度器
from neotask import TaskScheduler

# 2. 创建调度器实例（与 TaskPool 类似）
scheduler = TaskScheduler(executor=your_executor)

# 3. 使用新的定时任务方法
# 延时任务
scheduler.submit_delayed(data, delay_seconds=60)

# 周期任务
scheduler.submit_interval(data, interval_seconds=300)

# Cron 任务
scheduler.submit_cron(data, "0 9 * * *")

# 4. 周期任务管理
scheduler.pause_periodic(task_id)
scheduler.resume_periodic(task_id)
scheduler.cancel_periodic(task_id)

# 5. 查询周期任务
tasks = scheduler.get_periodic_tasks()
task = scheduler.get_periodic_task(task_id)
```



#### 配置迁移

```python
# 旧配置（0.2.x）- 无变化
config = TaskPoolConfig(storage_type="memory")

# 新配置（0.3.x）- 可选使用 SchedulerConfig
from neotask.models.config import SchedulerConfig

scheduler_config = SchedulerConfig(
    scan_interval=1.0,
    enable_periodic_manager=True,
    enable_time_wheel=False
)
scheduler = TaskScheduler(executor=executor, config=scheduler_config)
```



### 从 0.1.x 升级到 0.2.0

#### 新增依赖

如果使用 Redis 分布式锁，需要安装：

```bash
pip install neotask[redis]
```



#### API 变更

无破坏性变更，事件总线和监控组件为可选功能。

#### 迁移步骤

```python
# 1. 注册事件处理器（可选）
pool.on_completed(async def on_complete(event): ...)

# 2. 启用指标收集（可选）
from neotask.monitor import MetricsCollector
collector = MetricsCollector()

# 3. 使用分布式锁（可选）
from neotask.lock import LockFactory
lock = LockFactory.create_redis("redis://localhost:6379")
```



------

## 贡献者

感谢以下贡献者对项目的支持：

- **neopen** - 项目创始人和维护者
- 欢迎更多开发者参与贡献！

------

**注意**: 对于更详细的提交历史，请查看 [GitHub Releases](https://github.com/neopen/neotask/releases) 或 Git 提交记录。
