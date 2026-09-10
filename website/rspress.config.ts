import { defineConfig } from '@rspress/core';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const currentDir = path.dirname(fileURLToPath(import.meta.url));

const englishSidebar = [
  { text: 'Start', items: [
    { text: 'First batch', link: '/quick-start' },
    { text: 'Choose the right task', link: '/concepts' },
    { text: 'Run a bounded task', link: '/job-recipes' }
  ] },
  { text: 'Tasks', items: [
    { text: 'Page business records', link: '/batch-runs' },
    { text: 'Prevent duplicate effects', link: '/idempotency-patterns' },
    { text: 'Scale consumers', link: '/distributed-workers' }
  ] },
  { text: 'Operations', items: [
    { text: 'Choose Redis settings', link: '/configuration-recipes' },
    { text: 'Deploy the service', link: '/production-deployment' },
    { text: 'Inspect and control runs', link: '/operations' },
    { text: 'Recover from failures', link: '/failure-runbooks' },
    { text: 'Handle Redis outages', link: '/distributed-semantics' }
  ] },
  { text: 'Reference', items: [
    { text: 'Supported environment', link: '/compatibility' },
    { text: 'API lookup', link: '/target-api' },
    { text: 'Complete Batch contract', link: '/batch-v2' },
    { text: 'Configuration defaults', link: '/cli-and-config' },
    { text: 'States and errors', link: '/failure-modes' },
    { text: 'Operator SDK, no CLI', link: '/cli-reference' },
    { text: 'Framework hosting', link: '/vext-integration' }
  ] },
  { text: 'Maintainer', items: [
    { text: 'Module boundaries', link: '/architecture' },
    { text: 'Storage invariants', link: '/redis-model' },
    { text: 'Runtime lifecycle', link: '/worker-lifecycle' },
    { text: 'Qualification and release', link: '/development-contract' }
  ] }
];

const chineseSidebar = [
  { text: '开始使用', items: [
    { text: '开始第一个批处理', link: '/zh/quick-start' },
    { text: '理解任务与快照', link: '/zh/concepts' },
    { text: '执行有限任务', link: '/zh/job-recipes' }
  ] },
  { text: '常见任务', items: [
    { text: '分页处理业务记录', link: '/zh/batch-runs' },
    { text: '避免重复业务写入', link: '/zh/idempotency-patterns' },
    { text: '扩容消费者', link: '/zh/distributed-workers' }
  ] },
  { text: '运行与恢复', items: [
    { text: '选择 Redis 配置', link: '/zh/configuration-recipes' },
    { text: '部署服务', link: '/zh/production-deployment' },
    { text: '检查和控制任务', link: '/zh/operations' },
    { text: '故障恢复', link: '/zh/failure-runbooks' },
    { text: '处理 Redis 中断', link: '/zh/distributed-semantics' }
  ] },
  { text: '参考查询', items: [
    { text: '支持环境', link: '/zh/compatibility' },
    { text: 'API 快查', link: '/zh/target-api' },
    { text: 'Batch 完整合同', link: '/zh/batch-v2' },
    { text: '配置默认值', link: '/zh/cli-and-config' },
    { text: '状态与错误', link: '/zh/failure-modes' },
    { text: '使用运维 SDK（无 CLI）', link: '/zh/cli-reference' },
    { text: '框架托管（无专用适配器）', link: '/zh/vext-integration' }
  ] },
  { text: '维护者资料', items: [
    { text: '模块边界', link: '/zh/architecture' },
    { text: '存储不变量', link: '/zh/redis-model' },
    { text: '运行时生命周期', link: '/zh/worker-lifecycle' },
    { text: '验证与发布边界', link: '/zh/development-contract' }
  ] }
];

const englishNav = [
  { text: 'Quick Start', link: '/quick-start' },
  { text: 'Capabilities', link: '/batch-runs' },
  { text: 'Production', link: '/production-deployment' },
  { text: 'Reference', link: '/reference' }
];

const chineseNav = [
  { text: '快速开始', link: '/zh/quick-start' },
  { text: '按需能力', link: '/zh/batch-runs' },
  { text: '生产运维', link: '/zh/production-deployment' },
  { text: '参考', link: '/zh/reference' }
];

export default defineConfig({
  root: path.join(currentDir, '..', 'docs', 'v01'),
  base: '/queuebit/',
  lang: 'en',
  title: 'queuebit',
  logoText: 'queuebit',
  icon: '/favicon.svg',
  globalStyles: path.join(currentDir, 'styles', 'queuebit.css'),
  globalUIComponents: [path.join(currentDir, 'components', 'A11yLabels.tsx')],
  description: 'Durable batch processing user manual for queuebit.',
  outDir: 'dist',
  locales: [
    {
      lang: 'en',
      label: 'English',
      title: 'queuebit',
      description: 'Durable batch processing user manual.'
    },
    {
      lang: 'zh',
      label: '简体中文',
      title: 'queuebit',
      description: '基于 Redis 的持久批处理用户手册。'
    }
  ],
  markdown: {
    link: {
      checkDeadLinks: false
    }
  },
  search: {
    codeBlocks: true
  },
  languageParity: {
    enabled: true
  },
  themeConfig: {
    nav: englishNav,
    locales: [
      {
        lang: 'en',
        label: 'English',
        title: 'queuebit',
        description: 'Durable batch processing user manual.',
        nav: englishNav,
        footer: {
          message: 'Released under the Apache-2.0 License.'
        },
        sidebar: {
          '/': englishSidebar
        }
      },
      {
        lang: 'zh',
        label: '简体中文',
        title: 'queuebit',
        description: '基于 Redis 的持久批处理用户手册。',
        nav: chineseNav,
        footer: {
          message: '基于 Apache-2.0 许可证发布。'
        },
        sidebar: {
          '/zh/': chineseSidebar
        }
      }
    ],
    sidebar: {
      '/': englishSidebar,
      '/zh/': chineseSidebar
    },
    socialLinks: [
      {
        icon: 'github',
        mode: 'link',
        content: 'https://github.com/devcodex-labs/queuebit'
      }
    ],
    footer: {
      message: 'Released under the Apache-2.0 License.'
    }
  }
});
