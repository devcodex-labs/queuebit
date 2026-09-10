# 开发合同与验收路线

<span class="manual-label">维护者资料 · 验证要求，不是用户接入前置</span>

## 真相源顺序

公开 BatchQueue 类型和[完整行为合同](batch-v2.md) 定义消费者边界。根入口是唯一包 API，内部模块构建仅供测试。不能通过意外导出恢复已移除的 CLI 或框架适配器。

## 本地验证

使用 Node22 或 Node24、Redis7.2 和 OpenSSL。Linux 默认调用 redis-server，可用 `QUEUEBIT_BATCH_REDIS_BINARY` 指定夹具二进制。Windows 夹具使用 WSL Ubuntu-24.04 和隔离 Redis7.2.16 路径；运行前检查 harness 默认路径或提供二进制覆盖。测试自行拥有随机端口和临时数据目录，缺少前置条件时失败。

```bash
npm ci
npm --prefix website ci
npm run typecheck
npm test
```

npm test 执行完整必测核心、实际根包独立安装、TLS、三 Sentinel 切换、应用示例和文档验证。缺少夹具或 required 项 skip 不算通过。可独立运行 test:batch:consumers、test:batch:tls、test:batch:sentinel、test:batch:docs，各自包含构建前置；根 prepack 也先构建。

## 哪些证据有效

保存命令、exit code、Node/Redis 版本和真实包 hash。独立消费者不使用链接或仓库路径别名，验证 ESM/CJS、NodeNext/Bundler 并拒绝旧子路径。示例编译实际任务模块，以203条记录测试写后失败；旧 key 隔离在自有 Redis 预置旧值，完整观察命令并验证新前缀正向对照。

## 本地文档站

npm run docs:validate 检查场景、构建和本地链接。docs:preview 在127.0.0.1:4180展示生成页面；docs:dev 使用4181；docs:edit 在4182热编辑。需检查真实中英文页面、导航和移动端无障碍，不能只看 Markdown。

## 不能替代真实验证

合成暂存 manifest、静默环境 skip、仅 mock 的切换、源码别名消费者或放宽包内容断言都不成立。同机 Sentinel 夹具不证明跨故障域 HA 或磁盘持久性；不能为了测试停止用户已有服务。

## 发布边界

PR、main 和 tag 工作流只验证根包，不发布。选择新版本前保持 private=true 和历史0.0.5元数据。Git操作、tag和npm发布由发布负责人另行决定；本地全绿不代表远端CI已跑或版本已发布。
