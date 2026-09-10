# Redis 断了怎么办：恢复边界

<span class="manual-label">运行指南 · 区分连接中断、不确定回复和历史丢失</span>

## 先判断是哪一种

连接不可用、命令回复不确定、Redis 历史确认丢失，是三种不同情况。retryable 不代表操作没有写入。

## Redis 临时中断时

连接与命令重试共用有限 deadline，离线队列关闭。OUTCOME_UNKNOWN 表示可能已提交；保留原 query、幂等键或 commandId，查看对应 Run/Event 后再决定重试。换身份可能重复业务工作。

## Sentinel 切换时

同一个 Queue 可以发现新主节点。租约在当前 Redis 历史内阻止旧提交，但异步复制可能丢已确认写。Sentinel 和客户端成功回复都不构成业务恰好一次保证；丢失窗口后需对账持久业务存储。

## Run blocked 时怎么恢复

检查 reason、当前元数据及兼容消费者成员。没有匹配活跃消费者的定义无法安全执行。恢复正确合同、版本和 protocol，在状态允许时使用带 revision 的控制操作；不能手工改 Run hash 或索引。

## 确认 Redis 状态丢失时

评估精确 namespace 和业务审计期间停止新接收。按既定 RPO/RTO 恢复，或业务对账后明确创建新 Run。Queuebit 没有旧数据迁移或自动破坏性重置。外部幂等的保留应长于队列身份窗口。

## 下一步

[故障恢复](failure-runbooks.md) · [生产部署](production-deployment.md)
