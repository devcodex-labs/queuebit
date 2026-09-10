# 业务幂等：重复执行也只产生一次结果

<span class="manual-label">任务指南 · 外部写入的持久保护</span>

<span id="sc-idempotency"></span>

## 先记住一句话

执行和回调至少一次；崩溃或回复丢失会重复外部已成功的工作。Run 身份保留不是永久业务去重系统。

## key 应该怎么生成

选择持久业务操作身份。收据用 `JSON.stringify(['receipt', snapshotId, row.id])` 避免分隔符冲突，跨页重试和替代 Run 保持相同。不要用随机 attempt ID。Event ID 在 replay 中稳定；若多个 Event 代表同一业务操作，则使用相应业务键。

## 做法一：外部服务幂等

每次重试传相同 key，不确定回复后查询服务状态。服务的幂等保留期限须覆盖恢复窗口。

## 做法二：数据库事务

唯一操作键与业务状态变更在同一持久事务内完成。进程内 Set、或者写之前单独查询“是否见过”，都不构成持久幂等。

## 做法三：事务 Outbox

业务写和 outbox 行一起提交，随后结合目标服务的持久幂等投递。仅有 outbox 并不能保证非幂等远端副作用不重复。

## 队列接收身份

task.start({query,idempotencyKey}) 在 task/key 和 canonical query/有效策略一致时返回保留 Run；输入变化报 IDEMPOTENCY_CONFLICT。key≤256 UTF-8 bytes、合法 Unicode，空键与未传不同，不trim/normalize；对应身份回收后不再去重。

## 验收方式

执行一页，在第一次外部写成功后强制失败，再从持久 cursor 重试；核对每条只有一个持久业务效果。203条收据测试证明适配器合同，其内存替身不证明生产数据库/服务的持久性，需要你自己的故障演练。

## 下一步

[收据分页](batch-runs.md) · [不确定结果恢复](failure-runbooks.md)
