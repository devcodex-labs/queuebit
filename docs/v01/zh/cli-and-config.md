# 配置字段字典

<span class="manual-label">参考 · 类型、默认值、约束和作用域</span>

先看[配置场景](configuration-recipes.md)。配置直接传入 `createBatchQueue`，没有 CLI 加载器、自动环境读取或框架覆盖配置。

## 常用字段先看这里

| 字段 | 默认 | 含义 |
|---|---|---|
| namespace | 必填 | ASCII 字母/数字开头，其后允许字母、数字、点、下划线、连字符；最多128 UTF-8 bytes |
| redis | 必填 | direct、URL 或 Sentinel |
| runtime.mode | all | producer、consumer 或两者 |
| runtime.concurrency | 4 | 执行物理槽，1–1024 |
| runtime.callbackConcurrency | 4 | 回调物理槽，1–1024 |
| runtime.closeGraceMs | 30000 | 本地排空宽限，0–86400000 ms |
| defaults | 下表执行策略 | 任务执行默认值 |
| protocol | 下表 | 同 namespace 所有进程必须一致 |
| telemetry.sink | 无 | 有限、可丢弃的本地观测 |

## 命名与静态校验

未知字段、显式 undefined、null 配置值和访问器属性均报 CONFIG_INVALID；可选项请省略。任务名称与版本使用同样的安全标识符规则。配置不能为 null，不意味着 JSON 业务数据不能使用 null。

## 执行与回调策略

执行默认 attempts3、timeoutMs30000、full-jitter backoff baseMs1000/maxMs30000；回调默认 attempts10、timeoutMs30000、baseMs1000/maxMs60000。任务 policy 按字段覆盖执行 defaults；回调策略属于共享 protocol。attempts1–1000，timeout1–86400000ms，backoff base1–3600000ms，max≤86400000ms 且 max≥base；只接受 `jitter: 'full'`。

## Protocol 与保留

| 分组 | 默认及约束 |
|---|---|
| lease | leaseMs30000（最小3000）；heartbeatMs10000（最小1000，≤lease/3）；pollMs1000（1–1000）；recoveryLimit20（1–1000） |
| retention | runMs7d、deliveredEventMs7d（7–365d）；deadLetterMs 固定30d |
| limits | nonterminalRunMax10000、runMax20000、objectMax100000、unfinishedEventMax20000、definitionMax10000、memberMax1000、totalBytes512MiB |
| maintenance | batchSize100、maxBatchesPerTick10、timeBudgetMs50；正值可下调不可上调 |

容量值是上限，仅在相互依赖预算合法时可降低。逻辑字节包含预付结算/Event 空间，不等于 Redis RSS。保留期限表示可以进入有限 GC，不保证某个精确时刻立即删除。

## Redis 连接

direct：host/port、username/password、database、tls。URL：redis/rediss URL 和可选 tls；使用 TLS 时必须 rediss。Sentinel：name/seeds，独立 nodeAuth/sentinelAuth、nodeTls/sentinelTls，以及 database/addressMap。database 默认0；seeds1–32地址；addressMap 最多256项 host:port 映射。tls 的 ca/cert/key 是 PEM 字符串，servername 可选，不提供关闭证书校验的选项。

## Payload 上限

query≤256KiB、state≤64KiB、原始编码错误 envelope≤32KiB。业务幂等键≤256 UTF-8 bytes 且必须合法 Unicode；空键不同于未传。详见[完整 JSON 合同](batch-v2.md)。

## 下一步

[状态与错误](failure-modes.md) · [运维控制](operations.md)
