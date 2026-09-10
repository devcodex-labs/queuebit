# 我的环境能不能用

<span class="manual-label">参考 · 运行环境与任务范围</span>

## 一分钟判断

- Node.js `>=22`；资格验证覆盖 Node22/24。
- Redis `>=7.2`，可写单主，noeviction。
- 单实例或托管单主，可由 Sentinel 发现。
- ESM/CommonJS，NodeNext/Bundler 条件类型声明。
- 当前接口未发布，应安装本地根 tarball，不能替换成 npm 历史版本。

## 适合什么场景

有限快照处理、导出、收据发送和受控回填。应用负责持久输入和外部幂等写。不同 Run 可并发，同一 Run 只串行推进一个正常页游标。

## 不适合什么场景

Redis Cluster、其他存储后端、无限流/CDC、cron、DAG、优先级、全局限速或内置管理界面。namespace 不是恶意租户安全隔离。

## 连接安全

TLS 校验 CA 和主机名。Sentinel 发现与数据节点的认证/TLS 分开。生产建议三 Sentinel 分布在独立故障域；单机测试不能证明跨故障域可用性。异步复制可能在切换时丢已确认写。

## 兼容边界

不提供旧 API 别名、旧 Redis 迁移、CLI、独立 worker/coordinator 包入口或框架专用适配器。`docs/v01` 只是保留 URL；生命周期方法不会读取或删除旧 Redis key。

## 下一步

[第一个批处理](quick-start.md) · [选择 Redis 配置](configuration-recipes.md)
