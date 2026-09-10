# 故障恢复

<span class="manual-label">运行指南 · 保留身份和业务证据后再行动</span>

## 先记住三条规则

改变状态前先保存 Run/Event 身份和当前状态。外部效果为至少一次。不确定回复不代表零写入，不能删除 namespace 来消除错误。

## 按现象分流

| 现象 | 检查 | 恢复 |
|---|---|---|
| 不推进 | ready、匹配定义/成员、health/backlog | 恢复兼容消费者和连接 |
| handler失败 | task.get错误、state、业务审计 | 修复适配器，有限重试从持久状态继续 |
| 执行超时 | AbortSignal、残留物理槽 | I/O有界且协作退出；取消不回滚 |
| revision冲突 | 当前Run/Event元数据 | 按最新revision重做决定 |
| 命令结果未知 | 原commandId和完整请求 | 读状态，适当时重试相同请求 |
| 容量压力 | 预留字节、未完成事件、卡住的handler | 修复或排空，不改计数器 |
| 存储不一致 | 精确namespace/schema/index证据 | 停不安全工作，恢复已验证状态 |

## Redis 不可用或网络分区

参见[Redis 中断语义](distributed-semantics.md)。不确定期间不要反复创建新身份；切换后对账持久业务存储和可能丢失的已确认写。

## 继续或替换业务工作

只有当前状态允许时才 pause/resume。失败/取消终态不会因回调 replay 变成新执行。需重做业务时，应明确选择正确不可变快照创建新 Run，并保留外部稳定业务键；没有旧 failed-job 迁移或 recovery-run API。

## 恢复回调投递

<span id="sc-replay"></span>

先读取 queue.operator.deadLetters.get(eventId)，确认 handler、原业务快照和外部幂等仍可安全重试，使用当前 Event revision：

```ts
const replay = await queue.operator.deadLetters.replay({
  eventId,
  expectedRevision: event.revision,
  reason: 'Provider restored; original event remains safe to retry',
  commandId: recoveryCommandId,
});
```

eventId、非 null 的 event 和持久 recoveryCommandId 来自经过认证的事故工具。replay 复用原 Event，不重跑业务页。首次死信将 deadLetterExpiresAt 固定为30天，后续 replay 不延长。到期后只有原本有效的尝试可在冻结 replayDrainDeadline 和自身 timeout 之前结算，不允许新 claim/retry/renewal。

## 事故结束条件

分别确认 Run 和回调结果，对账业务重复/缺失效果，观察容量和 health 稳定，并记录接受的 Redis 丢失窗口。一次请求成功不等于事故已解决。

## 下一步

[运维流程](operations.md) · [完整回调合同](batch-v2.md)
