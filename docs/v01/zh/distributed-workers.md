# 多个消费者怎么一起跑

<span class="manual-label">任务指南 · 扩容、崩溃接手与滚动更新</span>

<span id="sc-scale"></span>

## 先看你要做什么

单个 runtime.mode 为 all 的进程可接收并执行任务。请求服务用 producer、后台服务用 consumer 可以分离生命周期和扩容。producer 只注册合同，不执行 handler；consumer/all 必须提供与事件声明匹配的 handler。consumer 不能 start。

## 最小部署方式

共享 Redis、namespace、protocol，以及任务 name/version/events/有效执行策略。在 ready 前 define。库不替你加载业务模块或启动进程，没有独立 coordinator/scheduler 角色。

## 并发怎么算

每进程默认4执行物理槽、4回调物理槽。N个同配置消费者的名义本地槽之和分别为N×4，不是全局限速。应测量数据库/外部服务容量和事件循环延迟。同 Run 串行推进页，不同 Run 可并发。

## 消费者挂了会怎样

租约恢复可以重跑同一页，旧 token 的迟到提交会拒绝。外部效果仍可能重复，必须使用持久业务键。超时 abort 后继续运行的 handler，在实际退出前仍占物理槽；接管逻辑租约不会释放它的 CPU 或连接。

## 扩容步骤

以相同不可变合同启动进程，await ready，确认成员和 health 后接入流量。protocol 不一致显式失败，不要只改一个进程的共享容量或回调策略。

## 滚动更新与排空

新任务版本的消费者先于生产者上线，保留仍有待执行工作的旧任务版本消费者。这是 BatchQueue 内任务版本运维，不是兼容已移除的旧 API。关闭时先停请求接收，再 await queue.close；检查 remainingExecutions、remainingCallbacks、timedOut。是否进一步终止进程由服务管理者决定。

## 下一步

[配置选择](configuration-recipes.md) · [业务幂等](idempotency-patterns.md)
