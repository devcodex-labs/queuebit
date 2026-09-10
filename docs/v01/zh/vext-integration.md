# 在框架中托管 Queuebit

<span class="manual-label">参考 · 使用普通 API，不提供专用适配器</span>

<span id="sc-hosting"></span>

原框架适配器已移除。保留此URL不是兼容层；使用普通createBatchQueue API，结合框架原生启动、依赖注入和关闭能力。

## 自行管理生命周期

应用启动时创建一个长期producer/all参与者，注册合同并await ready，只向路由暴露必要Task操作。部署/扩容不同则独立托管consumer。关闭时先停接收，再await queue.close；没有按请求连接工厂或隐藏守护进程。

## 接收前认证

验证请求，在服务端确定租户和快照所有权。只向task.start传有限JSON，稳定业务幂等键要包含操作范围。namespace不能替代授权；Queuebit不规定HTTP状态映射或框架路由schema。

## 显式处理结果

接收成功后返回runId，不等待业务全部完成。按应用合同映射输入错误、身份冲突、存储不可用和容量压力。不确定回复后保留原身份、查看Run状态，不能静默创建替代请求。

## 部署匹配消费者

消费者共享namespace/protocol，注册相同任务version/events/policy和真实handler。行为不兼容需新version。[收据模块](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/receipt-task.ts)由应用注入仓储/sink，因此不绑定特定框架。

## 下一步

[快速开始](quick-start.md) · [扩容消费者](distributed-workers.md) · [生产部署](production-deployment.md)
