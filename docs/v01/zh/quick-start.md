# 快速开始：处理收据快照

<span class="manual-label">快速开始 · 从固定输入到验证结果的真实批处理</span>

## 1. 安装 Queuebit，准备 Redis

使用 Node.js22+，以及可连接、noeviction 的 Redis7.2+。当前源码未发布，在源码目录打包，再安装到应用：

```bash
# Source checkout
npm ci
npm pack
# Your application (use the filename printed by pack)
npm install /absolute/path/to/queuebit-0.0.5.tgz
```

保留的0.0.5文件名是历史元数据，不是本次选择的新 v2 发布版本。

## 2. 准备业务快照

启动前创建持久收据快照，固定成员和payload；可变记录上的时间戳不够。将 [receipt-task.ts](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/receipt-task.ts) 复制到应用。

实现它的两份适配器合同：仓储按严格递增 id > afterId 每次最多100条读取；sink 的 putOnce 将持久唯一键与业务写原子结合，completeOnce 同样去重完成效果。这是你的真实数据库/服务接入，不是 Queuebit 提供的存储。

## 3. 在服务中只注册一次任务

```ts
import { createBatchQueue } from 'queuebit';
import { defineReceiptTask } from './receipt-task.js';
import { receiptRepository, receiptSink } from './your-application-adapters.js';

const queue = createBatchQueue({
  namespace: 'receipt-service',
  redis: { mode: 'direct', host: '127.0.0.1', port: 6379 },
});
const task = defineReceiptTask(queue, receiptRepository, receiptSink);
await queue.ready();
```

适配器模块就是第2步实现。import/构造/define不连接；ready() 才启动参与者。实例应在启动时创建一次，不在每个HTTP请求里创建。

## 4. 接收快照任务

```ts
const { runId, created } = await task.start({
  query: { snapshotId: 'snapshot-2026-09' },
  idempotencyKey: 'receipt-snapshot-2026-09',
});
console.log({ runId, created });
```

使用业务服务实际创建的snapshotId。认证和租户范围在服务端确定，不能只相信请求输入。task.start 仅表示接收，消费者进程要保持运行。

## 5. 确认结果

```ts
const current = await task.get(runId);
// Repeat in your monitoring path until terminal:
// current?.status === 'success'
// current?.callbacks.delivered === 1 for this success-callback example

// From the application shutdown hook, not immediately after admission:
const closed = await queue.close();
console.log(closed.timedOut, closed.remainingExecutions, closed.remainingCallbacks);
```

通过 task.get(runId) 在监控路径重复查询终态；此示例成功时 status=success、callbacks.delivered=1。queue.close 应在应用关闭钩子执行，不是接收后立即执行。

任务每次读100条，以稳定业务键写入，ctx.next({afterId})推进，后续读空时结束。执行和回调至少一次。示例验证203条记录并强制一次写后失败，检查安全重跑；其中内存测试替身不能替代持久生产适配器。

ready失败先查Redis/地址/protocol；工作不推进查匹配消费者与Run错误；外部回复丢失后用原业务键对账或重试，不能假定零效果。

## 下一步

[理解分页流程](batch-runs.md) · [选择Redis配置](configuration-recipes.md) · [安全恢复](failure-runbooks.md)
