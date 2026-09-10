# 先用起来，再理解 Queuebit

<span class="manual-label">快速开始 · 从业务任务逐步理解</span>

## 开始前只需要 5 个词

| 术语 | 对应用的含义 |
|---|---|
| Queue | 某个 Redis namespace 中已配置的进程参与者 |
| Task | 版本化合同，以及消费者中的处理函数 |
| Run | 一次接收的不可变 query 与持久进度 |
| Batch | 一次有限执行，成功后推进一页 |
| Event | 结算时生成的不可变通知，独立重试投递 |

## 从任务开始

跟随[收据快照路径](quick-start.md)：固定数据集、读取小页、业务幂等写入，再用 ctx.next 提交游标；空输入用 ctx.end 结束。

## 按需学习

需要分离请求接收与后台服务时看[运行模式](distributed-workers.md)；离开本地 Redis 时看[连接配置](configuration-recipes.md)；需要暂停、查询和恢复投递时看[运维控制](operations.md)。

## 常见误解

start 不等待完成。成功 Run 仍可能有未完成或死信回调。租约保护 Redis 提交，不保护外部副作用。超时或取消信号无法强制中断任意 JavaScript。query 去重依赖身份保留，不是永久业务审计。

## 下一步

[开始使用](quick-start.md) · [分页业务记录](batch-runs.md) · [完整合同](batch-v2.md)
