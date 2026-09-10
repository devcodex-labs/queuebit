# Redis 模型与原子不变式

<span class="manual-label">维护者资料 · 内部存储合同，不是公开命令 API</span>

## 用户边界

使用者通过 BatchQueue 操作，不直接改 key。内部前缀为 qb:batch:v1:{namespace}:，不迁移/消费旧前缀。启动仅用 MATCH 限定精确新namespace 的有限 SCAN 检测孤儿状态，没有旧key扫描。

## 概念 keyspace

namespace metadata绑定schema/protocol，definition绑定任务身份。Run持有不可变query和已提交state；索引支持有限live列表和到期/租约处理。Event保存不可变结算快照、独立投递状态和保护父数据的引用。runtime member有独立上限，不是持久业务身份。

## 必须原子的转换

静态Lua所有key均通过KEYS显式传入，租约/token/revision约束claim/renew/settlement。部分对象或索引不一致时拒绝，不能变成可执行工作。容量预付后续结算/Event空间，拒绝不安全新接收时仍允许合法排空。

## Canonical input

存储前验证JSON，规范身份保留允许字符串/数字/数组/对象的精确语义。query/state/error有编码上限。业务键采用合法Unicode，不trim/normalize。

## 保留与不可删除状态

未完成正常回调和合法冻结replay排空保护依赖。死信期限锚定firstDeadAt，不随replay延长。GC先去除计费和引用，再回收符合条件的Run/query/幂等/definition。没有永久墓碑或自动整namespace清理。

不能把 FLUSHDB、FLUSHALL 或手工删除metadata/index当库修复步骤。生命周期方法不清旧数据，恢复须走明确运维决策。

## 验证矩阵

在真实Redis验证CAS/token竞态、不确定回复、部分索引、有限内存、容量、Event/Run关联、期限排空与清理。根包测试另预置旧key，确认零旧key命令且新key有实际流量。

## 下一步

[运行时生命周期](worker-lifecycle.md) · [验证](development-contract.md)
