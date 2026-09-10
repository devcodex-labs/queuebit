# 参考

<span class="manual-label">参考 · 第一次成功后按需精确查询</span>

## 按需求查找

| 需求 | 页面 |
|---|---|
| 第一个真实业务批处理 | [快速开始](quick-start.md) |
| 公开方法和类型 | [API快查](target-api.md) |
| 完整行为与边界 | [Batch完整合同](batch-v2.md) |
| 配置默认值 | [字段字典](cli-and-config.md) |
| 支持环境 | [兼容范围](compatibility.md) |
| 判断执行和投递失败 | [状态与错误](failure-modes.md) |
| 控制任务或重放回调 | [运维](operations.md) |
| 放进框架服务 | [框架托管](vext-integration.md) |
| 从旧CLI链接进入 | [运维SDK，不提供CLI](cli-reference.md) |

## 公开命名快览

createBatchQueue创建Queue，define返回Task，start接收Run，ctx.next/end结算Batch，声明的handler投递Event。QueuebitError提供稳定错误/结果字段；只有根入口和package元数据可以导入。

## 版本状态

手册对应未发布源码，历史npm版本不匹配。保留v01地址便于查找，不兼容已移除的API或数据。

## 维护者入口

[架构](architecture.md) · [Redis模型](redis-model.md) · [生命周期](worker-lifecycle.md) · [验证](development-contract.md)
