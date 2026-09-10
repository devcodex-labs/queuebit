# 状态和错误怎么读

<span class="manual-label">参考 · 分开看执行、投递和传输结果</span>

## Run 状态

pending 等待执行，running 有当前租约工作，retrying 等待业务重试，blocked 暂不能推进，pausing 表示有暂停请求且仍有在途执行，paused 不领取下一页。终态为 success、failed、cancelled。get 返回 null 表示没有匹配的保留 Run，不是虚构成功。

## Event 状态

pending、retrying、delivering、delivered、dead_letter 描述回调投递。Run 成功不意味着回调全部完成；回调失败不重做执行。replay 在同一不可变 Event 上开启新投递代，不延长首次死信期限。

## 错误长什么样

QueuebitError 提供 code、operation、可选 runId/eventId/commandId，以及 retryable、outcomeKnown。业务 handler 失败保存为有上限的错误 envelope。不确定写结果与是否可重试是不同维度。

## 按现象处理

| 错误 | 安全的第一步 |
|---|---|
| CONFIG_INVALID / JSON_INVALID / PAYLOAD_TOO_LARGE | 按边界修正输入 |
| QUEUE_NOT_READY / QUEUE_CLOSED / MODE_OPERATION_NOT_ALLOWED | 使用已 ready、模式正确的 Queue |
| TASK_IDENTITY_MISMATCH / DEFINITION_HASH_CONFLICT | 部署匹配不可变定义 |
| IDEMPOTENCY_CONFLICT / COMMAND_CONFLICT | 比较完整原请求，不用同ID执行不同动作 |
| REVISION_CONFLICT | 读最新元数据再决策 |
| OUTCOME_UNKNOWN / CONNECTION_UNAVAILABLE | 保留身份，核对可能已发生的写 |
| CAPACITY_EXCEEDED | 查积压、成员和容量，允许有限排空 |
| CURSOR_INVALID / CURSOR_EXPIRED | 按原意图重新开始有限 live 列表 |
| INDEX_INCONSISTENT / STORAGE_INCONSISTENT / SCHEMA_MISMATCH / NAMESPACE_ORPHANED | 停止不安全工作并调查，不手工重置 |
| LEASE_LOST / HANDLER_CONTRACT_INVALID | 查过期尝试或非法控制值 |
| RESOURCE_CLEANUP_FAILED | 检查自有连接清理和进程状态 |

这是常见处置分组，完整错误码以安装包中的 QueuebitErrorCode 联合类型为准。

## 下一步

[恢复操作](failure-runbooks.md) · [运维控制](operations.md)
