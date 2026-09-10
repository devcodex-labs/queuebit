# 配置 Redis 和消费者

<span class="manual-label">运行指南 · 选择满足需求的最小连接方案</span>

## 先判断你的场景

本地单 Redis 用 direct；托管端点用 URL；平台提供单主复制发现时用 Sentinel。所有生产者/消费者的 namespace protocol 必须一致；本地并发可根据下游容量独立调整。

## 最小本地配置

```ts
import { createBatchQueue } from 'queuebit';
const queue = createBatchQueue({
  namespace: 'receipt-service',
  redis: { mode: 'direct', host: '127.0.0.1', port: 6379 },
});
```

在 await queue.ready() 前定义任务。库没有配置文件加载器或环境变量自动替换，应用通过自己的配置体系提供实际值。

## 托管 Redis 与 TLS

```ts
const redis = {
  mode: 'url' as const,
  url: 'rediss://redis.example.internal:6380/0',
  tls: { ca: trustedCaPem, servername: 'redis.example.internal' },
};
```

trustedCaPem 是服务提供方 CA 的 PEM 字符串；需要认证时在 URL 提供实际凭据。主机名必须匹配证书。CA 或地址错误应修配置，不能关闭校验。

## Sentinel

```ts
const redis = {
  mode: 'sentinel' as const,
  name: 'receipt-primary',
  seeds: [
    { host: 'sentinel-a.internal', port: 26379 },
    { host: 'sentinel-b.internal', port: 26379 },
    { host: 'sentinel-c.internal', port: 26379 },
  ],
  nodeAuth: { username: redisUser, password: redisPassword },
  sentinelAuth: { username: discoveryUser, password: discoveryPassword },
  database: 0,
};
```

四个凭据变量来自应用配置。相应通道使用 TLS 时补充 nodeTls/sentinelTls；广播的 host:port 需映射时使用 addressMap。发现权限和数据权限分开；Sentinel 不能消除异步复制丢失。

## 调整工作并发，不随意改变共享语义

本地执行/回调默认各4槽，从较低值起步，观察下游延迟、CPU 和事件循环。处理函数必须响应 abort 并最终退出。Task policy 覆盖 defaults；回调、租约、保留、容量和维护属于共享 protocol，配置不一致会使 ready 失败，不会静默采用某个进程的值。

## 启动前验证

要求 Node.js22+、Redis7.2+、可写主节点和 noeviction。未知/null/显式undefined配置同步报 CONFIG_INVALID；不要向未 ready 的 Queue 路由请求。参见[默认值和边界](cli-and-config.md)、[部署指南](production-deployment.md)。
