# BatchQueue 完整合同

<span class="manual-label">参考 · BatchQueue 完整行为与边界</span>

本手册对应当前未发布的 BatchQueue 根包，资格目标为 Node.js 22/24、Redis 7.2。不包含 CLI、框架专用适配器、独立 Worker/Coordinator、job mapper 或旧数据迁移。

## 1. 安装与生命周期

在源码目录执行 `npm ci`、`npm pack`，按 pack 输出文件名安装实际根包，例如 `npm install /absolute/path/queuebit-0.0.5.tgz`。0.0.5 是保留的历史元数据，不是新 v2 版本；在选择发布版本前包保持 private。ESM/CommonJS 的运行时根导出仅 createBatchQueue、QueuebitError；NodeNext import/require 和 Bundler 分别解析类型。仅允许根出口与 queuebit/package.json。

创建 createBatchQueue({namespace,redis})，在 await queue.ready() 前 queue.define(contract,handlers)，随后使用 task.start/get/cancel 和 queue.operator。导入/构造没有 I/O。首次 ready 打开自有连接并登记不可变定义，失败显式报错；重复 ready/close 共享进行中的 Promise，关闭后不能重用。消费者须保持运行；start 是接收而非完成。应用关闭时 await close 并检查 remainingExecutions/remainingCallbacks/timedOut。

runtime.mode 为默认 all、producer、consumer。producer 登记定义并维护，但不调用 handler；consumer 不能 start。consumer/all 的 handlers 必须匹配声明事件。定义绑定 name/version/事件集合/有效策略，不绑定函数源码；不兼容业务实现应更换 version，同版本不同策略不能互换。

## 2. Run 与 Batch

`queue.define<Q,S>({name,version,events,policy?},{execute,onBatchSettled?,onSuccess?,onFailure?})` 返回类型化 Task。start({query,idempotencyKey?}) 返回 {runId,created}；get(runId) 返回不可变 Run 快照或 null；cancel(runId) 返回以 found 区分的结果。query 全 Run 冻结，state 初始 null。每个 Run 同时只有一个有效租约 Batch 串行推进，不同 Run 可并发。

execute 必须且只能返回本次 ctx.next(state?) 或 ctx.end()。它们是绑定上下文的不透明控制值，不是根 helper 或任意 {kind:...}。throw/reject 消耗有限业务预算；错误/重复 control 即使捕获也成为终态合同失败。next 提交 state 并重置下一 Batch 预算；上下文有 page、batchId、attempt、AbortSignal。收到 abort 后协作停止；Redis fencing 保护库内提交，不回滚外部副作用。

## 3. 输入与幂等

仅接受有限 JSON 基元、稠密数组和普通数据对象，不支持 Date/BigInt/undefined/NaN/Infinity/函数/访问器/循环/symbol key。配置严格拒绝未知、null、显式 undefined。编码后 query≤256KiB、state≤64KiB、原始错误 envelope≤32KiB。合法 JSON 字符串保留 UTF-16；业务幂等键必须合法 Unicode、≤256 UTF-8 bytes。空键与未传不同，不 trim/normalize 文本。

同 task/key 且 canonical query/有效策略完全相同时，在保留期间返回已有 Run；输入不同给 IDEMPOTENCY_CONFLICT。不是永久去重。外部 sink 应使用持久业务键；执行和回调均为至少一次，Redis 异步复制故障切换仍可能丢已确认写，不能保证 exactly-once。

## 4. 默认值与上限

| 分组 | 默认值 |
|---|---|
| 本地 runtime | all；concurrency4；callbackConcurrency4；closeGraceMs30000 |
| 执行策略 | attempts3；timeoutMs30000；full-jitter base1000/max30000 ms |
| 回调策略 | attempts10；timeoutMs30000；full-jitter base1000/max60000 ms |
| 租约 | leaseMs30000；heartbeatMs10000；pollMs1000；recoveryLimit20 |
| 保留 | Run最短7d；delivered Event最短7d；死信绝对30d |
| 容量 | nonterminal10000；Runs20000；objects100000；unfinishedEvents20000；definitions10000；members1000；总逻辑512MiB |
| 维护 | batchSize100；maxBatchesPerTick10；timeBudgetMs50 |

同 namespace 的 protocol（callback/lease/retention/limits/maintenance）须一致，本地 runtime/并发与每 Task 的执行策略分开。逻辑计费含预付结算/Event 空间，不等于 Redis RSS。高低水位限制新 start，已预付任务、控制与维护仍可排空。CAPACITY_EXCEEDED 显式拒绝，不能换 schema 绕过；runtime member 有独立有限分区。

## 5. 连接与故障边界

redis 三选一：{mode:'direct',host,port,username?,password?,database?,tls?}；{mode:'url',url,tls?}；{mode:'sentinel',name,seeds,nodeAuth?,sentinelAuth?,nodeTls?,sentinelTls?,database?,addressMap?}。tls.ca/cert/key 是 PEM 字符串，可设 servername；URL TLS 必须 rediss。证书始终校验，无关闭校验开关。数据节点与 Sentinel 的认证/TLS 分开；生产至少三 Sentinel，置于独立故障域，单机测试不证明跨故障域高可用。

Redis 要求7.2+、可写 primary、noeviction、匹配 schema/protocol。隔离 prefix 为 qb:batch:v1:{namespace}:，不消费或迁移旧 key。namespace 是组织隔离而非恶意租户安全边界。preflight 对孤儿/不确定存储可明确拒绝；不要只删 meta 或手工重建索引压掉错误。

连接/命令重试共用单操作有限 deadline（≤10s），offline queue 关闭。OUTCOME_UNKNOWN 表示写可能已提交，应按返回的 runId/commandId 核对并重用原请求，不换 ID 隐藏不确定性。传输重试不会自行重跑业务 handler；客户端 fencing 不能消除 Redis 切换的历史回退。

## 6. 持久回调与 replay

声明 batchSettled/success/failure 并提供匹配 handler。Event 的 query/state/error/timestamp 为原结算不可变快照，回调返回值忽略。回调有独立 attempt、租约和物理并发；失败不重跑 execute，也不反转成功 Run。正常事件在同 Run 按创建顺序投递，retry 阻后序；delivered/dead_letter 推进 cursor；late replay 不回退 cursor，且与正常投递共享 Run Event 锁。

首次死信固定 firstDeadAt、独立列表序号、E=firstDeadAt+30d。replay 复用 Event/父 query，开启新代并重置投递次数、消耗 unfinished 容量，不延长 E。E 前第一次跨 E 的 grant 永久冻结 replayDrainDeadline。到 E 不允许新 claim/renew/recover/retry；只有原有效 attempt 可在冻结 deadline 和自身 timeout 前结算。replay 可重复外部副作用，应按 Event ID 或持久业务键去重。

## 7. Operator SDK

queue.operator 下有 runs.getMetadata/list/pause/resume/cancel、deadLetters.get/list/replay、health.snapshot()、capacity.snapshot()、本地 metrics.snapshot()，没有 HTTP server。Run 控制输入 {runId,expectedRevision,reason,commandId}；replay 输入 {eventId,expectedRevision,reason,commandId}，使用 Event revision。结果区分 applied/noop/not_found，not_found 不虚构 revision。父 Run 共用最多32条/24h command 历史，同 ID 不同全文操作/目标/文本在 CAS 前即冲突；完整编码回执≤2KiB，过窗不承诺永久去重。

list 为 live，不是快照：默认50/max200，cursor15分钟固定到期，绑定初始 upper sequence/filter，不纳入首屏后新建项。稀疏页不足 limit 仍可能有 nextCursor；稳定坏索引报错，不伪造空末页。死信 list 使用首次死信序号而非 Run 序号，排除过期/成功 replay；get 可查看保留至GC的 replay metadata。metadata 不带业务 payload。health 可 degraded/unavailable；本地 metrics 不是精确全局历史，有限可丢弃 telemetry 不回滚业务。

## 8. 保留与关闭

无永久 tombstone 或自动整 namespace purge。未完成正常 Event 和合法 frozen drain 保护依赖。GC 先释放 Event 引用与费用，再回收满足期限的终态 Run/query、幂等及无引用定义。close 停新领取、有限允许在途收敛，随后撤权；未退出 Promise 仍占物理槽，应检查关闭结果并修复不协作适配器。ready/close 从不清旧 Redis 数据。

## 9. 应用例子与运维

仓库 examples/batch-v2/receipt-task.ts 注入不可变快照仓储和持久幂等收据 sink。每页最多100个严格递增 ID，ctx.next({afterId}) 推进，完成回调通知。不能用可变数据上的时间戳替代快照隔离。测试把原 TypeScript 文件在 fresh consumer 编译，注入写后失败并实际使用 Redis；请自行提供真实数据库/服务适配器，测试数组不是生产存储。

REVISION_CONFLICT 后先读取最新 metadata 再决定新操作；SCHEMA_MISMATCH/INDEX_INCONSISTENT/STORAGE_INCONSISTENT 应停止并检查精确 namespace，不静默reset。容量/死信增长时检查消费者、卡住的 handler 和保留。pause 不撤销合法在途租约，cancel 不能撤销已经发出的外部写；replay 是可能重复副作用的运维决定。

## 10. 资格与发布状态

本地资格脚本覆盖模型/真实Redis故障、独立安装与类型、TLS、三Sentinel真实切换、示例和双语文档，并在Node22/24验证。资格测试在独立目录安装实际根 tarball，不合成 package 元数据，也不使用仓库路径别名。本地通过不意味着 npm 已发布；新版本与发布仍是独立决定，生命周期方法不会清理旧 Redis 数据。
