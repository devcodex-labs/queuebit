---
pageType: home
hero:
  name: queuebit
  text: Node.js 持久批处理
  tagline: 将固定业务快照分页执行，在 Redis 保存恢复进度，并安全投递回调。
  actions:
    - theme: brand
      text: 开始第一个批处理
      link: /zh/quick-start.html
    - theme: alt
      text: 分页处理业务记录
      link: /zh/batch-runs.html
    - theme: alt
      text: 了解任务与快照
      link: /zh/concepts.html
features:
  - title: 从真实快照开始
    details: 安装本地根包、连接 Redis、注册收据任务、注入持久适配器并查看 Run 结果。
    link: /zh/quick-start.html
  - title: 有限工作使用统一任务
    details: 接收有限 query，由托管消费者从已提交状态逐页推进。
    link: /zh/job-recipes.html
  - title: 随任务需求逐步学习
    details: 理解有限重试、协作超时、业务幂等和运维控制。
    link: /zh/concepts.html
  - title: 分页有界，进度可恢复
    details: 冻结输入成员与数据，幂等写入，每页完成后提交 keyset 游标。
    link: /zh/batch-runs.html
  - title: 扩容独立消费者
    details: 共享 namespace 和不可变合同，按下游容量配置本地执行与回调槽。
    link: /zh/distributed-workers.html
  - title: 框架负责自身生命周期
    details: 应用负责认证、生命周期和托管，不需要框架专用适配器。
    link: /zh/vext-integration.html
  - title: 运维知识按需查阅
    details: 部署、容量、告警与故障恢复位于运行恢复分区，不挤占首次使用路径。
    link: /zh/failure-runbooks.html
---

<span class="manual-label">首页 · BatchQueue 用户手册</span>

> **发布状态：** 本手册对应当前未发布的 BatchQueue 源码。请安装本地根 tarball，npm 历史版本不提供该接口；保留 v01 页面地址不表示兼容旧 API。
