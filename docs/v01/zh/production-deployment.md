# 生产上线怎么部署

<span class="manual-label">运行指南 · 应用托管与单主 Redis</span>

<span id="sc-redis"></span>

## 先按环境选路径

使用 Node.js `>=22`、Redis `>=7.2`。当前接口未发布，应安装本地实际根包，不用历史 npm 版本对应新手册。

## Redis 必须满足

可写单主、noeviction、充足容量，以及符合 RPO/RTO 的持久化和备份。限制应用对目标部署的访问。TLS 校验CA/主机名，实际凭据由应用选定的配置体系管理。namespace 是组织隔离，不是恶意租户授权边界。

ready 在写入命名空间元数据前通过 `INFO memory` 核实 `maxmemory_policy:noeviction`。策略不同或不可观察时返回 `CONFIG_INVALID`；Queuebit 不修改服务端配置。后台运行时重新校验重连客户端时也会检查。这是一次预检，不是持续强制配置；运维必须在整个部署期间保持 noeviction。

Sentinel 建议至少三个独立故障域和适当 quorum，数据与发现认证/TLS 分开。异步复制可能丢已确认写，客户端租约无法修复历史回退。不支持 Redis Cluster。

## 进程怎么部署

producer 接收工作、注册不带handler的合同；consumer 执行业务页和回调；all 两者皆可。所有参与者使用同一 protocol 和匹配不可变任务合同。仓储和 sink 是部署方的真实依赖，不是库内置数据库实现。

## 启动顺序

1. 准备 Redis 与持久不可变业务快照仓储。
2. 启动已注册 handler 的消费者并 await ready。
3. 检查兼容成员和 health。
4. 启动 producer/请求接收，使用服务端确定的身份。
5. 同时观察 Run、回调结果和业务审计。

库不安装 CLI 守护进程、不额外启动 worker，也不绑定全局关闭信号。

## 容器与服务管理

沿用正常进程管理器。为 closeGraceMs 和有限 I/O 留出关闭时间，并检查 close 结果。进程存活不代表 Queue ready。保持长期参与者，不按每个请求重建连接。

## 配置版本与滚动更新

共享 protocol 必须一致，本地并发可不同。先部署新任务版本的消费者，再让 producer 接收该版本工作；仍有工作依赖的版本保留至排空。handler 行为不兼容时需要新version，即使函数名未变。

## 上线前验收

在自己的环境验证真实数据库/服务幂等、启动失败、TLS/认证拒绝、进程崩溃、不确定回复、切换、回调 replay、容量压力和自有服务清理。单机 Sentinel 夹具验证客户端行为，不证明跨故障域可用性或磁盘持久性。

## 下一步

[连接配置](configuration-recipes.md) · [中断边界](distributed-semantics.md)
