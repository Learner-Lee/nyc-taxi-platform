# NYC 出租车智能运营平台

企业级大数据全栈项目 | NYC Yellow Taxi 2022-2024 | 约 3-5 亿条记录

## 快速导航

- [项目总览](docs/PROJECT_OVERVIEW.md) — 架构图、业务背景、简历/面试价值
- [资源规划](docs/RESOURCE_PLAN.md) — 内存预算、分组启停、OOM 应急
- [学习路径](docs/LEARNING_PATH.md) — 阶段依赖、核心概念、延伸阅读

## 启动

```bash
# 启动核心服务（常驻）
docker compose -f docker/docker-compose.core.yml up -d

# 启动服务层（按需）
docker compose -f docker/docker-compose.serving.yml up -d
```

## 当前进度

- [x] STAGE 00: 文档体系搭建
- [ ] STAGE 01: 环境搭建与 HDFS 入门
- [ ] STAGE 02: 数据接入与 ODS 层
- ...
