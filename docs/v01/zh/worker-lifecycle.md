# 运行时生命周期与归属

<span class="manual-label">维护者资料 · 租约、物理槽和清理</span>

## 共通阶段

导入/构造/define没有网络I/O。ready拥有连接、protocol检查、definition/member注册和模式循环。重复ready/close共享在途结果；关闭后的Queue不能重用。导入时不注册全局信号或创建worker进程。

## 执行和回调归属

每个attempt拥有受约束租约和本地物理槽。失去逻辑租约撤销提交权限，但未退出JavaScript仍占物理槽。Redis约束阻止迟到状态提交，不撤销外部效果。回调有独立槽/预算并消费不可变Event快照。

## 时间推进

有限轮询和维护推进到期工作、恢复过期权限、回收符合保留条件的对象。producer也维护但不执行业务handler。不存在独立公开scheduler/coordinator进程。

## Replay边界

正常回调在delivered/dead_letter时推进序列；late replay不回退序列，与正常投递共享父Run的Event锁。第一次跨固定期限的有效租约冻结replayDrainDeadline；到期后只允许原本有效的尝试在冻结边界内结算。

## 连接策略

命令共用有限deadline，离线队列关闭，不确定写显式保留。清理只处理参与者/夹具拥有的连接和进程。close停新领取，在closeGraceMs内排空后撤销剩余权限，返回残留执行/回调数量。

## 必测故障窗口

进程死亡、claim/settle回复丢失、事件循环停顿、不协作handler、ready中close、旧token竞态、回调重试/replay/到期，以及夹具清理。分发验证使用实际根包而非内部测试构建。

## 下一步

[存储模型](redis-model.md) · [验证](development-contract.md)
